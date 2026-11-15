{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Anonymous records and sums for the parts of a program's input: a value that has its name in its type, and the pairs
-- that nest to the right, so that a record is made by its names and its fields are read, put in front and changed by
-- name, and not by position. No record type is declared for it.
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
-- A sum is the dual: its cases have their names in its type, as @Either ("refused" := x) (Either ("good" := y) ("quit"
-- := z))@, which nests to the right as '|||' takes it apart. @inj "refused" x@ puts a value in the case of a name, and the
-- type of the sum comes from the branches that take it apart, so that a producer needs no signature. @decide "how did it
-- end?" (cases "head")@ gives each branch its case's value in front of the record that the sum is a field of, and a chain
-- of branches, @"refused" ?> p ||| "good" ?> q ||| "quit" ?> r@, takes the record apart: each branch reads its case as a
-- field named after the case, and the rest of the record by name, and names its wire in diagrams with the case. A
-- classification that is made where the record is read, @casesBy "status" classify@, does the same. @_Case "refused" p@
-- runs an arrow on the one case of a plain sum, which can change its type, and passes the others through. A name that the
-- choice doesn't have is a type error that says so.
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
    Inj,
    inj,
    Distribute,
    cases,
    casesBy,
    (?>),
    Case (..),
    _Case,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, left, right, (&&&), (>>^), (^>>))
import Data.Kind (Constraint)
import Data.Proxy (Proxy (..))
import GHC.Records (HasField (..))
import GHC.TypeLits (ErrorMessage (..), KnownSymbol, Symbol, TypeError, symbolVal)
import Network.Streaming.Annotate (Annotate, giving, inout, taking)
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

-- * Choosing

-- | What is said of a name that no case of the choice has.
type NoCase name = 'Text "The choice has no case " ':<>: 'ShowType name

-- | Puts a value in the case of the name, in a sum that nests to the right, as '|||' takes it apart.
class Inj (name :: Symbol) a s where
  injWith :: a -> s

-- | The last case, which has the name.
instance (a ~ b) => Inj name a (name := b) where
  injWith = Is
  {-# INLINE injWith #-}

-- | The first case, when it has the name.
instance {-# OVERLAPPING #-} (a ~ b) => Inj name a (Either (name := b) rest) where
  injWith = Left . Is
  {-# INLINE injWith #-}

-- | A later case, found in the rest.
instance (Inj name a rest) => Inj name a (Either other rest) where
  injWith = Right . injWith @name
  {-# INLINE injWith #-}

-- | The last case, when it has another name: the choice has no case of the name.
instance {-# OVERLAPPABLE #-} (TypeError (NoCase name)) => Inj name a (other := b) where
  injWith _ = error "Network.Streaming.Named: a case that the choice doesn't have is a type error"

-- | The value in the case of the name. The type of the sum comes from the branches that take it apart, so that a match
-- of @inj "refused" x@ and the like needs no signature, and a name that the choice doesn't have is a type error.
inj :: forall name -> forall a s. (Inj name a s) => a -> s
inj name = injWith @name
{-# INLINE inj #-}

-- | Puts the case of a choice in front of the record that the choice is a field of, for the branch of the case: the value of
-- the case is a field named after the case, and the rest of the record is as it was. The sum and the choice that it
-- makes determine each other once the record is known, so that the type of a sum flows back from its branches to the
-- code that makes it.
class Distribute s r d | s r -> d, d r -> s where
  distribute :: s -> r -> d

-- | The last case: its value, in front of the record.
instance (r' ~ r) => Distribute (name := x) r (name := (name := x, r')) where
  distribute (Is x) r = Is (Is x, r)
  {-# INLINE distribute #-}

-- | The first case, and the rest of them.
instance (r' ~ r, Distribute rest r drest) => Distribute (Either (name := x) rest) r (Either (name := (name := x, r')) drest) where
  distribute (Left (Is x)) r = Left (Is (Is x, r))
  distribute (Right rest) r = Right (distribute rest r)
  {-# INLINE distribute #-}

-- | The sum that is the field of the name, with each case's value in front of the record, for the branches of a chain of
-- '?>'. A pure function, for 'Network.Streaming.Plumbing.decide', which gives the decision the question that labels it in
-- diagrams.
cases :: forall name -> forall r s d. (HasField name r s, Distribute s r d) => r -> d
cases name r = distribute (getField @name r) r
{-# INLINE cases #-}

-- | The same for a field that holds a value that the function puts in a case: a classification, such as the outcome of a
-- handler.
casesBy :: forall name -> forall r a s d. (HasField name r a, Distribute s r d) => (a -> s) -> r -> d
casesBy name classify r = distribute (classify (getField @name r)) r
{-# INLINE casesBy #-}

-- '?>' binds tighter than '|||', so that a chain of branches needs no parentheses.
infixr 6 ?>

-- | The branch for the case of the name: it takes the case's value in front of the record, as a field of that name, and
-- the wire into it is called the name in diagrams. A chain of them, @"a" ?> p ||| "b" ?> q ||| "c" ?> r@, takes apart what
-- 'cases' gives.
(?>) :: forall name -> forall arr x r b. (Arrow arr, Annotate arr, KnownSymbol name) => arr (name := x, r) b -> arr (name := (name := x, r)) b
(?>) name p = taking (symbolVal (Proxy @name)) ((\(Is a) -> a) ^>> p)
{-# INLINE (?>) #-}

-- | Runs an arrow on the case of the name of a choice, with a change of its type if the arrow has one.
class Case (name :: Symbol) s a b t | name s -> a, name s b -> t where
  caseOf :: forall arr. (ArrowChoice arr) => arr a b -> arr s t

-- | The last case, which has the name.
instance (a ~ x, t ~ (name := b)) => Case name (name := x) a b t where
  caseOf p = (\(Is a) -> a) ^>> p >>^ Is
  {-# INLINE caseOf #-}

-- | The first case, when it has the name: the rest passes through.
instance {-# OVERLAPPING #-} (a ~ x, t ~ Either (name := b) rest) => Case name (Either (name := x) rest) a b t where
  caseOf p = left ((\(Is a) -> a) ^>> p >>^ Is)
  {-# INLINE caseOf #-}

-- | A later case, found in the rest: the first case passes through.
instance (Case name rest a b rest', t ~ Either other rest') => Case name (Either other rest) a b t where
  caseOf = right . caseOf @name
  {-# INLINE caseOf #-}

-- | The last case, when it has another name: the choice has no case of the name.
instance {-# OVERLAPPABLE #-} (a ~ TypeError (NoCase name), t ~ TypeError (NoCase name)) => Case name (other := x) a b t where
  caseOf _ = error "Network.Streaming.Named: a case that the choice doesn't have is a type error"

-- | The prism of the case of the name: runs the arrow on the case, with a change of its type if the arrow has one, and
-- passes the other cases through without running it. The wire into the arrow is called the name in diagrams.
_Case :: forall name -> forall arr s t a b. (ArrowChoice arr, Annotate arr, KnownSymbol name, Case name s a b t) => arr a b -> arr s t
_Case name p = caseOf @name (taking (symbolVal (Proxy @name)) p)
{-# INLINE _Case #-}
