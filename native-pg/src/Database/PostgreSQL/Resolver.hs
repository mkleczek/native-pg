-- | The resolver of host names, which a program makes once when it starts and puts in the settings of its connections.
module Database.PostgreSQL.Resolver
  ( Resolver,
    newResolver,
  )
where

import Database.PostgreSQL.Internal.Cares (Resolver, defaultResolverOptions, newResolverWith)

-- | A resolver with the system's configuration. It reads the configuration here, and not when a name is looked up, so it
-- is made where a program starts, in IO, and then used by every core.
newResolver :: IO Resolver
newResolver = newResolverWith defaultResolverOptions
