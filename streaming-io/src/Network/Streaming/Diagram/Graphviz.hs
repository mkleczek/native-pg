-- | Prints a graph of "Network.Streaming.Diagram.Graph" as Graphviz DOT.
module Network.Streaming.Diagram.Graphviz (dot) where

import Control.Exception (throw)
import Network.Streaming.Diagram.Graph (Graph)
import Network.Streaming.NotImplemented (NotImplemented (..))

-- | The DOT of the graph.
dot :: Graph -> String
dot _ = throw (NotImplemented "the DOT of a graph")
