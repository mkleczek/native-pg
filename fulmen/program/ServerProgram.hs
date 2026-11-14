{-# LANGUAGE DataKinds #-}

-- | The server's program, built at the 'Diagram' arrow, which runs nothing: it is drawn by @fulmen-diagram@ and linted
-- by it and by the lint test.
module ServerProgram (withServer) where

import qualified Network.Fulmen as Fulmen
import qualified Network.Socket as Socket
import Network.Streaming.BufferPool (BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes (Annotate (..), Effect (..), Label (..))
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.PositiveInt (positiveInt)

-- | Gives the program of the server to the action. The listener and the pool are the settings' own, and are never used:
-- nothing runs. The server has four workers on each core, so that the pages show a count of them.
withServer :: (Diagram Fulmen.Cores () -> IO a) -> IO a
withServer use = do
  listener <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
  pool <- newBufferPool (BufferPoolSettings (positiveInt @16384) (positiveInt @3) (positiveInt @1) (positiveInt @1))
  result <- use (server ((Fulmen.defaultSettings listener pool) {Fulmen.connectionsPerCore = 4}))
  Socket.close listener
  pure result

-- | The server with a handler that does nothing.
server :: Fulmen.Settings -> Diagram Fulmen.Cores ()
server settings = Fulmen.serve settings handler
  where
    handler = named (Label "handler" "the application's handler") (now (const (pure ())))
