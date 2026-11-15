{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Anonymous records for the parts of a program's input: a value that has its name in its type, and the pairs that
-- nest to the right, so that a record is made by its names and its fields are read, put in front and changed by name,
-- and not by position. No record type is declared for it.
--
-- A record is made by @newRecord ["name", "size"] \<$\> p \<*\> q@, with a step for each name, or by
-- @is "name" p &&& is "size" q@, or from a record that is there, by @adding "kept" r@. A step reads the fields that
-- it needs with 'HasField', and takes any record that has them: @(HasField "size" r Int) => arr r Bool@. A field is
-- read as @r.size@ with @OverloadedRecordDot@, or by @field "size"@. @at "size" p@ runs an arrow on a field and puts
-- the result in its place. A name that the record doesn't have is a type error that says so, and so is a name that
-- 'newRecord' is given twice. A field that is put in front of a record that has a field of its name shadows the older
-- one, as a @let@ does, which is what lets a step that is written for any record add a field.
--
-- The labels of the wires in diagrams come from the arrow combinators 'is', 'adding', 'at' and 'field', and from no
-- other: 'newRecord' is applied with @\<$\>@, and the diagram's @fmap@ drops the function, so that it can't label
-- anything.
--
-- A field is read where it is used, inside 'Network.Streaming.Classes.now' or a step that takes the record, so that
-- nothing is built of the field but the read: a record that is projected into a new pair has the thunks of the fields
-- again.
module Network.Streaming.Named
  ( (:=) (..),
    is,
    Build,
    newRecord,
    SetField (..),
    at,
    adding,
    field,
  )
where

import Control.Arrow (Arrow, arr, first, (&&&), (>>^), (^>>))
import Data.Kind (Constraint)
import Data.Proxy (Proxy (..))
import GHC.Records (HasField (..))
import GHC.TypeLits (ErrorMessage (..), KnownSymbol, Symbol, TypeError, symbolVal)
import Network.Streaming.Annotate (Annotate, giving, inout)
import Network.Streaming.Plumbing (alongside)

-- | A value with its name in its type: one field of an anonymous record.
newtype (name :: Symbol) := a = Is a

-- | The arrow's result, as the field of the name, which the wire out of the arrow is called in diagrams.
is :: forall name -> forall arr a b. (Arrow arr, Annotate arr, KnownSymbol name) => arr a b -> arr a (name := b)
is name p = (p `giving` symbolVal (Proxy @name)) >>^ Is
{-# INLINE is #-}

-- * Reading

-- | The last field of a record that nests to the right, as '&&&' makes it. What the field is read as is its own type,
-- so that which instance reads a field never depends on the type that it is read at.
instance (b ~ a) => HasField name (name := a) b where
  getField (Is a) = a
  {-# INLINE getField #-}

-- | The first field, when it has the name: a newer field shadows the older ones of its name.
instance {-# OVERLAPPING #-} (b ~ a) => HasField name (name := a, rest) b where
  getField (Is a, _) = a
  {-# INLINE getField #-}

-- | A later field, found in the rest.
instance (HasField name rest b) => HasField name (other := a, rest) b where
  getField (_, rest) = getField @name rest
  {-# INLINE getField #-}

-- | The last field, when it has another name: the record has no field of the name.
instance {-# OVERLAPPABLE #-} (b ~ TypeError (Missing name)) => HasField name (other := a) b where
  getField _ = error "Network.Streaming.Named: a field that the record doesn't have is a type error"

-- | What is said of a name that no field of the record has.
type Missing name = 'Text "The record has no field " ':<>: 'ShowType name

-- | What is said of a name that two fields have.
type Twice name = 'Text "The record has more than one field named " ':<>: 'ShowType name

-- | The field of a record, whose output wire is named after the field in diagrams.
field :: forall name -> forall r a arr. (HasField name r a, KnownSymbol name, Arrow arr, Annotate arr) => arr r a
field name = arr (getField @name) `giving` symbolVal (Proxy @name)
{-# INLINE field #-}

-- * Making

-- | Builds a record of the names from one argument per name, in CPS.
class Build (names :: [Symbol]) record k f | names record k -> f, names f -> record k where
  build :: (record -> k) -> f

instance (f ~ (a -> k), record ~ (name := a)) => Build '[name] record k f where
  build done = done . Is
  {-# INLINE build #-}

instance
  (f ~ (a -> g), record ~ (name := a, rest), Build (next ': names) rest k g) =>
  Build (name ': next ': names) record k f
  where
  build done a = build @(next ': names) (\rest -> done (Is a, rest))
  {-# INLINE build #-}

-- | The names are all different.
type family Distinct (names :: [Symbol]) :: Constraint where
  Distinct '[] = ()
  Distinct (name ': names) = (Absent name names, Distinct names)

-- | The name is not one of the names.
type family Absent (name :: Symbol) (names :: [Symbol]) :: Constraint where
  Absent name '[] = ()
  Absent name (name ': names) = TypeError (Twice name)
  Absent name (other ': names) = Absent name names

-- | A record of the names, made from one argument for each: the arrows of an applicative style, or the values of a
-- plain function. The names are in the record in the order that they are given, so that @newRecord ["a", "b"] x y@ is
-- the record that @is "a" p &&& is "b" q@ makes, and a name that is given twice is a type error.
newRecord :: forall names -> forall record f. (Distinct names, Build names record record f) => f
newRecord names = buildRecord @names
{-# INLINE newRecord #-}

-- | The record that the arguments of 'build' make, as it is.
buildRecord :: forall names record f. (Build names record record f) => f
buildRecord = build @names @record @record id
{-# INLINE buildRecord #-}

-- * Changing

-- | Replaces a field, possibly with a value of another type.
class SetField (name :: Symbol) r b r' | name r b -> r' where
  setField :: b -> r -> r'

-- | The last field, which has the name.
instance (r' ~ (name := b)) => SetField name (name := a) b r' where
  setField b _ = Is b
  {-# INLINE setField #-}

-- | The first field, which has the name: the newer one, as 'getField' reads it.
instance {-# OVERLAPPING #-} (r' ~ (name := b, rest)) => SetField name (name := a, rest) b r' where
  setField b (_, rest) = (Is b, rest)
  {-# INLINE setField #-}

-- | A later field, found in the rest.
instance (SetField name rest b rest', r' ~ (other := a, rest')) => SetField name (other := a, rest) b r' where
  setField b (a, rest) = (,) a $! setField @name b rest
  {-# INLINE setField #-}

-- | Runs the arrow on a field of the record, and puts its result in the field's place, with the rest of the record as
-- it was. Both wires of the arrow are named after the field in diagrams.
at ::
  forall name ->
  forall arr a b r r'.
  (Arrow arr, Annotate arr, KnownSymbol name, HasField name r a, SetField name r b r') =>
  arr a b ->
  arr r r'
at name p = (getField @name &&& id) ^>> first (inout label p label) >>^ uncurry (setField @name)
  where
    label = symbolVal (Proxy @name)
{-# INLINE at #-}

-- | The arrow's result, as a field of the name in front of the input, which is the rest of the record. The wire out of
-- the arrow is called the name in diagrams.
adding :: forall name -> forall arr r b. (Arrow arr, Annotate arr, KnownSymbol name) => arr r b -> arr r (name := b, r)
adding name p = alongside (is name p)
{-# INLINE adding #-}
