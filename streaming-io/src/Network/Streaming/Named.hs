{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Records and sums whose fields and cases have their names in their types, so that a program makes, reads and changes
-- them by name and not by position. No record type is declared for them.
--
-- A record is a pair that nests to the right, a pair for each field. It is made by @newRecord ["name", "size"] \<$\> p
-- \<*\> q@, or by @is "name" p &&& is "size" q@, or from a record that is there by putting a field in front of it with
-- @adding "kept" p@. A field is read as @r.size@ with @OverloadedRecordDot@, or by @field "size"@, and @at "size" p@
-- runs an arrow on the field and puts the result in its place. A step that reads some of the fields takes any record
-- that has them: @(HasField "size" r Int) => arr r Bool@. A field that is put in front of one of its name shadows it,
-- as a @let@ does. A name that the record doesn't have, and a name that 'newRecord' is given twice, are type errors
-- that say so.
--
-- A sum has the names of its cases in its type, as @Either ("refused" := x) (Either ("good" := y) ("quit" := z))@,
-- which nests to the right as '|||' takes it apart. @inj "refused" x@ makes it, and its type comes from the branches
-- that take it apart, so that a producer needs no signature. @decide "how did it end?" (cases "head")@ takes the sum
-- that is a field of the record apart, and a chain of branches, @"refused" ?> p ||| "good" ?> q ||| "quit" ?> r@, gives
-- each branch the value of its case, as a field named after the case, in front of the record. @casesBy "status"
-- classify@ does the same for a value that a function puts in a case. @_Case "refused" p@ runs an arrow on one case of
-- a plain sum. A name that the choice doesn't have, and a name that two of its cases have, are type errors that say so.
--
-- The names of wires in diagrams come from 'is', 'adding', 'at', 'field' and '?>'. 'newRecord' is applied with @\<$\>@,
-- which a diagram draws without the function, so that it names nothing.
--
-- A field is read where it is used, inside 'Network.Streaming.Classes.now' or a step that takes the record. A record
-- that is projected into a new pair gets a thunk for each field again.
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
    CaseNames,
    DistinctCases,
    cases,
    casesBy,
    (?>),
    Case,
    _Case,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, left, right, (&&&), (>>^), (^>>))
import Data.Kind (Constraint)
import Data.Proxy (Proxy (..))
import GHC.Records (HasField (..))
import GHC.TypeLits (ErrorMessage (..), KnownSymbol, Symbol, TypeError, symbolVal)
import Network.Streaming.Annotate (Annotate, (-|), (|-))
import Network.Streaming.Plumbing (alongside)

-- | A value with its name in its type: one field of an anonymous record.
newtype (name :: Symbol) := a = Is a

-- | The arrow's result as a field of the name, which also names the wire out of the arrow in diagrams.
is :: forall name -> forall arr a b. (Arrow arr, Annotate arr, KnownSymbol name) => arr a b -> arr a (name := b)
is name p = p -| symbolVal (Proxy @name) >>^ Is
{-# INLINE is #-}

-- * Reading

-- | The only field of a record, when it has the name: it is read at its own type.
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
field name = arr (getField @name) -| symbolVal (Proxy @name)
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

-- | A record with a field for each of the names, in the order given, made from one argument for each: arrows, as in
-- @newRecord ["a", "b"] \<$\> p \<*\> q@, or plain values. A name that is given twice is a type error.
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
at name p = (getField @name &&& id) ^>> first (label |- p -| label) >>^ uncurry (setField @name)
  where
    label = symbolVal (Proxy @name)
{-# INLINE at #-}

-- | Puts the arrow's result in front of its input as a field of the name. The wire out of the arrow has the name in
-- diagrams.
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

-- | Puts the value in the case of the name. The type of the sum comes from the branches that take it apart, so that a
-- producer needs no signature.
inj :: forall name -> forall a s. (Inj name a s) => a -> s
inj name = injWith @name
{-# INLINE inj #-}

-- | Puts the value of a case of a sum in front of the record, as a field named after the case, for the branch of that
-- case. The sum and the result determine each other, so that the type of the sum comes from the branches.
class Distribute s r d | s r -> d, d r -> s where
  distribute :: s -> r -> d

-- | The last case: its value, in front of the record.
instance (r' ~ r) => Distribute (name := x) r (name := (name := x, r')) where
  distribute (Is x) r = Is (Is x, r)
  {-# INLINE distribute #-}

-- | The first case, and the rest of them.
instance
  (r' ~ r, Distribute rest r drest) =>
  Distribute (Either (name := x) rest) r (Either (name := (name := x, r')) drest)
  where
  distribute (Left (Is x)) r = Left (Is (Is x, r))
  distribute (Right rest) r = Right (distribute rest r)
  {-# INLINE distribute #-}

-- | What is said of a name that two cases of a choice have.
type TwiceCase name = 'Text "The choice has more than one case named " ':<>: 'ShowType name

-- | The names of the cases of a sum that nests to the right, as '|||' takes it apart.
type family CaseNames s :: [Symbol] where
  CaseNames (Either (name := x) rest) = name ': CaseNames rest
  CaseNames (name := x) = '[name]

-- | The names are all different.
type family DistinctCases (names :: [Symbol]) :: Constraint where
  DistinctCases '[] = ()
  DistinctCases (name ': names) = (AbsentCase name names, DistinctCases names)

-- | The name is not one of the names.
type family AbsentCase (name :: Symbol) (names :: [Symbol]) :: Constraint where
  AbsentCase name '[] = ()
  AbsentCase name (name ': names) = TypeError (TwiceCase name)
  AbsentCase name (other ': names) = AbsentCase name names

-- | Takes the sum that is the field of the name apart for a chain of '?>': the value of its case goes in front of the
-- record. It is the function that 'Network.Streaming.Plumbing.decide' takes, so that the decision has its question in
-- diagrams. Two cases of one name are a type error.
cases ::
  forall name ->
  forall r s d.
  (HasField name r s, Distribute s r d, DistinctCases (CaseNames s)) =>
  r ->
  d
cases name r = distribute (getField @name r) r
{-# INLINE cases #-}

-- | The same, with a function that puts the value of the field in a case, as a classification does.
casesBy ::
  forall name ->
  forall r a s d.
  (HasField name r a, Distribute s r d, DistinctCases (CaseNames s)) =>
  (a -> s) ->
  r ->
  d
casesBy name classify r = distribute (classify (getField @name r)) r
{-# INLINE casesBy #-}

-- '?>' binds tighter than '|||', so that a chain of branches needs no parentheses.
infixr 6 ?>

-- | The branch for the case of the name: it takes the value of the case in front of the record, as a field of that
-- name, and the wire into it has the name in diagrams. A chain of them takes apart what 'cases' gives.
(?>) ::
  forall name ->
  forall arr x r b.
  (Arrow arr, Annotate arr, KnownSymbol name) =>
  arr (name := x, r) b ->
  arr (name := (name := x, r)) b
(?>) name p = (\(Is a) -> a) ^>> (symbolVal (Proxy @name) |- p)
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
instance
  {-# OVERLAPPABLE #-}
  (a ~ TypeError (NoCase name), t ~ TypeError (NoCase name)) =>
  Case name (other := x) a b t
  where
  caseOf _ = error "Network.Streaming.Named: a case that the choice doesn't have is a type error"

-- | The prism of the case of the name: runs the arrow on that case, which the arrow may give another type, and passes
-- the other cases through. The wire into the arrow has the name in diagrams.
_Case ::
  forall name ->
  forall arr s t a b.
  (ArrowChoice arr, Annotate arr, KnownSymbol name, Case name s a b t) =>
  arr a b ->
  arr s t
_Case name p = caseOf @name (symbolVal (Proxy @name) |- p)
{-# INLINE _Case #-}
