-- | A resource that's tagged with its scope can't be returned from it.
module OwnedEscapes (escapes) where

import Network.Streaming.Scope

escapes :: IO ()
escapes = do
  _ <- withScope $ \scope -> acquireIn scope (pure ()) (\_ _ -> pure ())
  pure ()
