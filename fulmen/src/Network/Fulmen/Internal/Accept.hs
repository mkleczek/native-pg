{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

-- | Accepting without waiting: the listening socket's own calls, which never wait. The acceptor blocks on the
-- socket's event while they find no connection, as a link's receive does, so that the wait is the arrow's and not
-- GHC's IO manager's, which the network package's own accept waits in.
module Network.Fulmen.Internal.Accept
  ( acceptor,
    listening,
  )
where

import Control.Arrow (ArrowChoice, first, returnA, (>>>), (^>>), (|||))
import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.Bits ((.&.), (.|.))
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CInt (..))
import Foreign.Ptr (Ptr, nullPtr)
import Network.Socket (Socket, SocketOption (NoDelay), mkSocket, setSocketOption, withFdSocket)
import qualified Network.Socket as Socket
import Network.Streaming.Classes (Block (..), Effect (..), Event (..), GiveWay (..))
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Descriptors (fresh)
import Network.Streaming.Mode (Instrumented (bump), ModeOf)
import System.Posix.Internals (c_fcntl_read, c_fcntl_write, const_f_getfl, const_f_setfl, o_NONBLOCK, setNonBlockingFD)
import System.Posix.Types (Fd (..))

-- | Makes the listening socket one that doesn't block, which it may already be.
listening :: Socket -> IO ()
listening sock = withFdSocket sock (`setNonBlockingFD` True)

-- | The next connection, if one is waiting: a socket that doesn't block, and that doesn't hold back a small write.
-- macOS has no @accept4@, so the socket is made to not block after it is accepted.
tryAccept :: forall m. (Instrumented m) => Socket -> IO (Maybe Socket)
tryAccept listener = withFdSocket listener attempt
  where
    attempt fd =
      c_accept fd nullPtr nullPtr >>= \case
        -1 ->
          Errno.getErrno >>= \errno ->
            if
              | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> Nothing <$ bump @m AcceptsEmpty
              -- A connection that was reset before it was accepted, or a call that a signal interrupted: the next.
              | errno == Errno.eINTR || errno == Errno.eCONNABORTED || errno == Errno.ePROTO -> attempt fd
              | otherwise -> Errno.throwErrno "accept"
        accepted -> bump @m Accepts *> (Just <$> adopt accepted)
    adopt fd = do
      nonBlocking @m fd
      sock <- mkSocket fd
      fresh sock
      -- A socket that isn't a TCP one has no such option.
      _ <- try (setSocketOption sock NoDelay 1) :: IO (Either SomeException ())
      pure sock

-- | Makes the descriptor one that doesn't block, as 'setNonBlockingFD' does, and counts the calls it makes: it reads the
-- flags, and writes them if the descriptor doesn't have the flag, which a socket accepted from one that doesn't block has on
-- some systems.
nonBlocking :: forall m. (Instrumented m) => CInt -> IO ()
nonBlocking fd = do
  flags <- Errno.throwErrnoIfMinus1Retry "fcntl" (c_fcntl_read fd const_f_getfl)
  bump @m Fcntls
  when (flags .&. o_NONBLOCK == 0) $ do
    Errno.throwErrnoIfMinus1Retry_ "fcntl" (c_fcntl_write fd const_f_setfl (fromIntegral (flags .|. o_NONBLOCK)))
    bump @m Fcntls

foreign import ccall unsafe "accept"
  c_accept :: CInt -> Ptr () -> Ptr () -> IO CInt

-- | The next connection of the listening socket: it tries, and while there is none, blocks until the socket is
-- readable and tries again. The part gives way before it tries, as a receive does, so that a listener that always has
-- a connection doesn't keep the others of its core from running.
acceptor :: forall arr. (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Instrumented (ModeOf arr)) => arr Socket Socket
acceptor = giveWay >>> loop
  where
    loop = now attempt >>> ((first block >>> snd ^>> loop) ||| returnA)
    attempt listener =
      tryAccept @(ModeOf arr) listener >>= \case
        Just accepted -> pure (Right accepted)
        Nothing -> (\fd -> Left (Readable (Fd fd), listener)) <$> Socket.unsafeFdSocket listener
