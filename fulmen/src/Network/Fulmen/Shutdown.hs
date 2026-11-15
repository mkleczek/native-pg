{-# LANGUAGE CApiFFI #-}

-- | What starts a graceful shutdown: an event of the scheduler's poller, which the settings of a server name. A signal is
-- one: its handler is C code that writes to a pipe, and the event is that the pipe is readable, so it reaches the scheduler
-- at once, and no handler of GHC's runtime is involved.
module Network.Fulmen.Shutdown
  ( terminating,
    terminatedBy,
  )
where

import Foreign.C.Error (throwErrnoIfMinus1)
import Foreign.C.Types (CInt (..))
import Foreign.Marshal.Array (withArrayLen)
import Foreign.Ptr (Ptr)
import Network.Streaming.Classes (Event (..))
import System.Posix.Types (Fd (..))

foreign import capi "signal.h value SIGINT" sigInt :: CInt

foreign import capi "signal.h value SIGTERM" sigTerm :: CInt

foreign import ccall unsafe "fulmen_signals"
  c_signals :: Ptr CInt -> CInt -> IO CInt

foreign import ccall unsafe "fulmen_last_signal"
  c_lastSignal :: IO CInt

-- | The event that fires when the process is sent SIGTERM or SIGINT, which are handled from now on. It stays fired.
terminating :: IO Event
terminating =
  withArrayLen [sigTerm, sigInt] $ \count numbers ->
    Readable . Fd <$> throwErrnoIfMinus1 "fulmen_signals" (c_signals numbers (fromIntegral count))

-- | The number of the signal that came last, if one did.
terminatedBy :: IO (Maybe Int)
terminatedBy = (\number -> if number == 0 then Nothing else Just (fromIntegral number)) <$> c_lastSignal
