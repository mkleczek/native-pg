-- | The resolver of host names. It is made once, in IO, when a program starts, and every lookup of a name on any
-- core uses it. Nothing is made yet: host names are looked up as they were.
module Database.PostgreSQL.Internal.Cares
  ( Resolver,
    ResolverOptions (..),
    defaultResolverOptions,
    newResolverWith,
  )
where

-- | What looks names up.
data Resolver = Resolver

-- | How a resolver is made: where its configuration comes from.
data ResolverOptions = ResolverOptions
  { -- | The name servers as a comma-separated list of addresses, each with a port if it isn't 53, in place of the
    -- system's.
    resolverServers :: Maybe String,
    -- | The milliseconds that each try of a query waits for an answer, in place of the system's.
    resolverTimeout :: Maybe Int,
    -- | How many times a query is tried, in place of the system's.
    resolverTries :: Maybe Int,
    -- | The hosts file, in place of the system's.
    resolverHostsFile :: Maybe FilePath,
    -- | Whether a name that isn't found is looked up with each of the system's search domains added.
    resolverSearchDomains :: Bool
  }

-- | The system's configuration.
defaultResolverOptions :: ResolverOptions
defaultResolverOptions = ResolverOptions Nothing Nothing Nothing Nothing True

-- | A resolver that is made as the options say.
newResolverWith :: ResolverOptions -> IO Resolver
newResolverWith _ = pure Resolver
