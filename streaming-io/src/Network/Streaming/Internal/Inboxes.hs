{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | What the address end of an inbox is made of, for the instances of 'Network.Streaming.Classes.Inboxes'. The package
-- doesn't expose this module: a program that could make an 'Address' of its own would choose its reach.
module Network.Streaming.Internal.Inboxes
  ( Address,
  )
where

import GHC.TypeLits (Symbol)
import Network.Streaming.Internal.Tickets (Reach (..))

-- | The end of an inbox that others post to, which has the inbox's name. An address of reach @'Here s@ mentions the region
-- @s@, so that it can't leave the core that made the inbox; an address of reach @'Anywhere@ mentions no core, and can be
-- kept in structures that cores share.
data family Address (name :: Symbol) (r :: Reach) a

-- | An address of an inbox of this core.
data instance Address name ('Here _s) a

-- | An address of an inbox of any core.
data instance Address name 'Anywhere a
