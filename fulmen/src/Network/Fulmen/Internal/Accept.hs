{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Accepting without waiting: the listening socket's own calls, which never wait. The acceptor blocks on the
-- socket's event while they find no connection, as a link's receive does, so that the wait is the arrow's and not
-- GHC's IO manager's, which the network package's own accept waits in.
module Network.Fulmen.Internal.Accept
  ( acceptor,
    listening,
  )
where

import Control.Arrow (ArrowChoice, (>>>))
import Control.Exception (SomeException, try)
#if !defined(linux_HOST_OS)
import Control.Monad (when)
#endif
import Data.Bits
  ( (.|.),
#if !defined(linux_HOST_OS)
    (.&.),
#endif
  )
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CInt (..))
import Foreign.Ptr (Ptr, nullPtr)
import Network.Socket (Socket, SocketOption (NoDelay), mkSocket, setSocketOption, withFdSocket)
import qualified Network.Socket as Socket
import Network.Streaming.Classes (Annotate, Block (..), Effect, Event (..), GiveWay (..), giving, taking)
import Network.Streaming.Combinators (pattern Again, pattern Finish, consume, iterateWith, step)
import Network.Streaming.Counters (Counter (..))
import Network.Streaming.Internal.Descriptors (fresh)
import Network.Streaming.Mode (Instrumented (bump), ModeOf)
import System.Posix.Internals
  ( setNonBlockingFD,
#if !defined(linux_HOST_OS)
    c_fcntl_read,
    c_fcntl_write,
    const_f_getfl,
    const_f_setfl,
    o_NONBLOCK,
#endif
  )
import System.Posix.Types (Fd (..))

-- | Makes the listening socket one that doesn't block, which it may already be.
listening :: Socket -> IO ()
listening sock = withFdSocket sock (`setNonBlockingFD` True)

-- | The next connection, if one is waiting: a socket that doesn't block, and that doesn't hold back a small write. Where
-- there is @accept4@, the socket is made so by the call, which also has it close on exec. macOS has none, so the socket is made
-- to not block after it is accepted; one that is accepted from a listener that doesn't block already doesn't, on macOS.
tryAccept :: forall m. (Instrumented m) => Socket -> IO (Maybe Socket)
tryAccept listener = withFdSocket listener attempt
  where
    attempt fd =
      acceptFrom fd >>= \case
        -1 ->
          Errno.getErrno >>= \errno ->
            if
              | errno == Errno.eAGAIN || errno == Errno.eWOULDBLOCK -> Nothing <$ bump @m AcceptsEmpty
              -- A connection that was reset before it was accepted, or a call that a signal interrupted: the next.
              | errno == Errno.eINTR || errno == Errno.eCONNABORTED || errno == Errno.ePROTO -> attempt fd
              | otherwise -> Errno.throwErrno "accept"
        accepted -> bump @m Accepts *> (Just <$> adopt accepted)
    adopt fd = do
#if !defined(linux_HOST_OS)
      nonBlocking @m fd
#endif
      sock <- mkSocket fd
      fresh sock
      -- A socket that isn't a TCP one has no such option.
      _ <- try (setSocketOption sock NoDelay 1) :: IO (Either SomeException ())
      pure sock

#if defined(linux_HOST_OS)
acceptFrom :: CInt -> IO CInt
acceptFrom fd = c_accept4 fd nullPtr nullPtr (sockNonblock .|. sockCloexec)

foreign import ccall unsafe "accept4"
  c_accept4 :: CInt -> Ptr () -> Ptr () -> CInt -> IO CInt

foreign import capi "sys/socket.h value SOCK_NONBLOCK"
  sockNonblock :: CInt

foreign import capi "sys/socket.h value SOCK_CLOEXEC"
  sockCloexec :: CInt
#else
acceptFrom :: CInt -> IO CInt
acceptFrom fd = c_accept fd nullPtr nullPtr

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
#endif

-- | The next connection of the listening socket: it tries, and while there is none, blocks until the socket is
-- readable and tries again. The part gives way before it tries, as a receive does, so that a listener that always has
-- a connection doesn't keep the others of its core from running.
acceptor :: forall arr. (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Block arr, Instrumented (ModeOf arr)) => arr Socket Socket
acceptor =
  giveWay >>> iterateWith (giving "accepted?" (step "try accept" attempt)) (taking "none waiting" (consume block))
  where
    attempt listener =
      tryAccept @(ModeOf arr) listener >>= \case
        Just accepted -> pure (Finish accepted)
        Nothing -> (\fd -> Again (Readable (Fd fd), listener)) <$> Socket.unsafeFdSocket listener
