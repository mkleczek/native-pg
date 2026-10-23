{-# LANGUAGE MagicHash #-}

-- | Retain a finalizer anchor through ownership and its exception cleanup.
module Database.PostgreSQL.Internal.PoolLifetime (withLifetime) where

import GHC.Exts (keepAlive#)
import GHC.IO (IO (..))

withLifetime :: anchor -> IO a -> IO a
withLifetime anchor (IO action) = IO $ \state -> keepAlive# anchor state action
