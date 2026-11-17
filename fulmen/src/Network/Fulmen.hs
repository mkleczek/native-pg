{-# LANGUAGE DataKinds #-}

-- | An HTTP/1.1 server whose connections are served by parts that are spawned as the connections are accepted: each
-- core spawns a connection worker for each connection that the acceptor gives it, up to its share of the connection cap,
-- and the acceptor, which waits until some core has room, accepts as many connections as the cores that wait have room
-- for, in one step that never waits, and gives each core its share, so that the cores have as many connections as one
-- another. A connection worker serves its connection with a request direction, which parses the
-- head and hands the body on piece by piece, and an answer direction, which runs the handler; a connection that waits
-- for its next request holds no buffer.
--
-- The handler is a program for any arrow that has what 'Runtime' bundles. It
-- sends its own answer, so a request is one lexical scope: whatever the handler
-- acquires, the loans of the call included, is released when it returns, after
-- 'finish'.
--
-- The listening socket is not a part of the program's settings: a 'Listen' is how the acceptor gets it when the server
-- starts, and gives it back when it ends. What the cores share is made where the program runs: under
-- 'Network.Streaming.Delimited.runCores' each core runs the program with its own 'Cores', made from the one that 'newCores' gives
-- by 'onCore', which has the core's own pool of request buffers, and the first core runs the acceptor. Under the other runners
-- there is one core.
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
    module Network.Fulmen.Listen,
    module Network.Fulmen.Request,
    module Network.Fulmen.Response,
    module Network.Fulmen.Shutdown,
  )
where

import Control.Arrow (Arrow, arr)
import Control.Monad (replicateM)
import Data.IORef (newIORef)
import qualified Data.IntMap.Strict as IntMap
import Data.Maybe (fromMaybe)
import Network.Fulmen.Internal (Call (..), Cores (..), Count (..), Settings (..), Shared (..))
import Network.Fulmen.Internal.Date (newPulse)
import Network.Fulmen.Internal.Serve (serve)
import Network.Fulmen.Listen
import Network.Fulmen.Request
import Network.Fulmen.Response
import Network.Fulmen.Shutdown
import Network.Streaming.BufferPool (BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes (secs)
import Network.Streaming.Loans (Loans)
import Network.Streaming.Pool (newPool)
import Network.Streaming.PositiveInt (mkPositiveInt, positiveInt, positiveIntValue)

-- | A server of one connection worker at a time for each core, with a pool of 16 buffers of 16 KiB for requests in
-- flight, 2 of them in reserve, and 2 for setup, a line limit and a head limit of 50 KiB, which are Warp's default for
-- the whole head, no limit on bodies, a silent client given 30 seconds, and a connection that waits for its next request
-- 30 seconds, as Warp's timeout is.
defaultSettings :: Settings
defaultSettings =
  Settings
    (BufferPoolSettings (positiveInt @16384) (positiveInt @16) (positiveInt @2) (positiveInt @2))
    1
    51200
    51200
    Nothing
    (Just (secs 30))
    (Just (secs 30))
    Nothing
    (Just (secs 30))

-- | The cores that a server runs on, from its settings and how many there are: what they share, the pool of the cores
-- that wait for a connection, the count of each core's connection workers, a pool of request buffers for each core that
-- has its share of the ordinary buffers that the settings size, and the first core's own value. The cores are numbered
-- from 0, and a core that is not counted here can't run the server.
newCores :: Settings -> Int -> IO Cores
newCores settings cores = do
  first <- newBufferPool share
  others <- replicateM (max 1 cores - 1) (newBufferPool share)
  Cores 0
    <$> ( Shared
            <$> newPool []
            <*> (IntMap.fromList <$> traverse (\core -> (core,) <$> newIORef (Count 0 Nothing)) [0 .. max 1 cores - 1])
            <*> newPulse
            <*> pure first
            <*> pure (IntMap.fromList (zip [0 ..] (first : others)))
        )
  where
    share = coreShare (max 1 cores) (bufferPool settings)

-- | The settings of the pool of one of that many cores: its share of the ordinary buffers, rounded up, and the reserve and the
-- setup buffers as the settings have them.
coreShare :: Int -> BufferPoolSettings -> BufferPoolSettings
coreShare cores settings = settings {ordinaryBufferCount = fromMaybe (ordinaryBufferCount settings) (mkPositiveInt ((ordinary + toInteger cores - 1) `div` toInteger cores))}
  where
    ordinary = toInteger (positiveIntValue (ordinaryBufferCount settings))

-- | The value of the core of this number, for the program that runs on it: its own pool of request buffers. The first core
-- runs the acceptor.
onCore :: Cores -> Int -> Cores
onCore (Cores _ shared) core = Cores core shared {sharedBuffers = IntMap.findWithDefault (sharedBuffers shared) core (sharedCorePools shared)}

-- | The loans of the call, which native-pg takes, so that the replies of the
-- database and the response share a buffer.
loans :: (Arrow arr) => arr Call Loans
loans = arr (\(Call callLoans _ _) -> callLoans)
