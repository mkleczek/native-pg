-- | The server's program, built at the 'Diagram' arrow, which runs nothing: it is drawn by @fulmen-diagram@ and linted
-- by it and by the lint test.
module ServerProgram (server) where

import qualified Network.Fulmen as Fulmen
import Network.Streaming.Classes (Annotate (..), Effect (..), Label (..))
import Network.Streaming.Diagram (Diagram)
import System.Posix.Types (Fd (..))

-- | The server, which listens on a socket that it is started with, and has four workers on each core, so that the pages
-- show a count of them. It is built without a socket and without a pool, and nothing runs.
server :: Diagram Fulmen.Cores ()
server = Fulmen.serve Fulmen.defaultSettings {Fulmen.connectionsPerCore = 4} (Fulmen.inherited (Fd 3)) handler
  where
    handler = named (Label "handler" "the application's handler") (now (const (pure ())))
