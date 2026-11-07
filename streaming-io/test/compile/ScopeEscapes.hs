-- | A scope can't be returned from the region that owns it.
module ScopeEscapes (escapes) where

import Network.Streaming.Scope

escapes :: IO ()
escapes = do
  _ <- withScope pure
  pure ()
