-- | An HTTP/1.1 server that is a fixed network of parts, made at startup, per core:
-- each core has as many workers as its share of the connection cap, and an acceptor
-- hands the connections that it accepts to free workers as remote tickets. A worker
-- serves its connection with a request direction, which parses the head and hands
-- the body on piece by piece, and an answer direction, which runs the handler; a
-- connection that waits for its next request holds no buffer.
--
-- The handler is a program for any arrow that has what 'Runtime' bundles. It
-- sends its own answer, so a request is one lexical scope: whatever the handler
-- acquires, the loans of the call included, is released when it returns, after
-- 'finish'.
--
-- Under 'Network.Streaming.Delimited.runCores' each core runs the program with its own 'Cores', made from the one
-- that 'newCores' gives by 'onCore': the first core runs the acceptor. Under the other runners there is one core.
module Network.Fulmen
  ( -- * Serving
    Settings (..),
    defaultSettings,
    Cores,
    newCores,
    onCore,
    serve,

    -- * A call
    Call,
    loans,
    module Network.Fulmen.Request,
    module Network.Fulmen.Response,
  )
where

import Control.Arrow (Arrow, arr)
import Network.Fulmen.Internal (Call (..), Cores (..), Settings (..), Shared (..))
import Network.Fulmen.Internal.Date (newDate)
import Network.Fulmen.Internal.Serve (serve)
import Network.Fulmen.Request
import Network.Fulmen.Response
import Network.Socket (Socket)
import Network.Streaming.BufferPool (BufferPool)
import Network.Streaming.Classes (secs)
import Network.Streaming.Loans (Loans)
import Network.Streaming.Pool (newPool)

-- | A server of one worker for each core, with a line limit of 16 KiB, a head limit
-- of 64 KiB, no limit on bodies, and a silent client given 30 seconds.
defaultSettings :: Socket -> BufferPool -> Settings
defaultSettings socket pool = Settings socket pool 1 16384 65536 Nothing (Just (secs 30))

-- | The cores that a server runs on, from how many there are: what they share, and the first core's own value.
newCores :: Int -> IO Cores
newCores _ = Cores 0 <$> (Shared <$> newPool [] <*> newDate)

-- | The value of the core of this number, for the program that runs on it. The first core runs the acceptor.
onCore :: Cores -> Int -> Cores
onCore (Cores _ shared) core = Cores core shared

-- | The loans of the call, which native-pg takes, so that the replies of the
-- database and the response share a buffer.
loans :: (Arrow arr) => arr Call Loans
loans = arr (\(Call callLoans _ _) -> callLoans)
