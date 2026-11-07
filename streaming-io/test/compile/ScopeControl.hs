-- | What a scope allows: a plain value may leave it, and a resource's
-- contents are taken out by its library.
module ScopeControl (control) where

import Network.Streaming.Scope

control :: IO Int
control = withScope $ \scope -> do
  resource <- acquireIn scope (pure 1) (\_ _ -> pure ())
  pure (owned resource)
