{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Anonymous records for the parts of a program's input: a value that has its name in its type, and the fields of the
-- pairs that '&&&' nests to the right, so that @is "name" a &&& is "size" b &&& is "kept" c@ makes a record that is read
-- by name, as @r.name@ with @OverloadedRecordDot@ or as @field \@"name"@, and not by its position. No record type is
-- declared for it, and a name that the record doesn't have, or has twice, is a type error that says so.
--
-- A field is read where it is used, inside 'Network.Streaming.Classes.now' or a step that takes the record, so that
-- nothing is built of the field but the read: a record that is projected into a new pair has the thunks of the fields
-- again.
module Network.Streaming.Named
  ( (:=) (..),
    is,
  )
where

import Control.Arrow (Arrow)
import Control.Exception (throw)
import Data.Kind (Constraint)
import Data.Proxy (Proxy (..))
import GHC.Records (HasField (..))
import GHC.TypeLits (ErrorMessage (..), KnownSymbol, Symbol, TypeError, symbolVal)
import Network.Streaming.Annotate (Annotate)
import Network.Streaming.NotImplemented (NotImplemented (..))

-- | A value with its name in its type: one field of an anonymous record.
newtype (name :: Symbol) := a = Is a

-- | The arrow's result, as the field of the name, which the wire out of the arrow is called in diagrams.
is :: forall name -> forall arr a b. (Arrow arr, Annotate arr, KnownSymbol name) => arr a b -> arr a (name := b)
is name _ = unwritten ("is " <> symbolVal (Proxy @name))

-- | The last field of a record that nests to the right, as '&&&' makes it. What the field is read as is its own type,
-- so that which instance reads a field never depends on the type that it is read at.
instance (b ~ a) => HasField name (name := a) b where
  getField = unwritten "the last field"

-- | The first field, when it has the name. The name is a type error if the rest of the record has it too.
instance {-# OVERLAPPING #-} (b ~ a, Once name rest) => HasField name (name := a, rest) b where
  getField = unwritten "the first field"

-- | A later field, found in the rest.
instance (HasField name rest b) => HasField name (other := a, rest) b where
  getField = unwritten "a later field"

-- | The last field, when it has another name: the record has no field of the name.
instance {-# OVERLAPPABLE #-} (b ~ TypeError (Missing name)) => HasField name (other := a) b where
  getField = unwritten "a field that is not there"

-- | What is said of a name that no field of the record has.
type Missing name = 'Text "The record has no field " ':<>: 'ShowType name

-- | What is said of a name that two fields of the record have.
type Twice name = 'Text "The record has more than one field named " ':<>: 'ShowType name

-- | The name is not the name of a field of the record.
type family Once (name :: Symbol) record :: Constraint where
  Once name (name := a, rest) = TypeError (Twice name)
  Once name (name := a) = TypeError (Twice name)
  Once name (other := a, rest) = Once name rest
  Once name rest = ()

-- | What the stubs fail with, until they are written.
unwritten :: String -> a
unwritten = throw . NotImplemented . ("Network.Streaming.Named: " <>)
