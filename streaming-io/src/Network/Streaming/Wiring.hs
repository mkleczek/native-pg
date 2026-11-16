{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Steps wired by name, in a syntax like the one of @proc@ notation, as a library: the steps are arrows that are built
-- once, and the names are in types.
--
-- A flow is a chain of lines that '>>>' joins:
--
-- > result
-- >   "left"
-- >   ( "socket" <: accept -< ["listener"]
-- >       >>> "left" <: handTo -< ["to" =: "recipient", "value" =: "socket"]
-- >       >>> unlessTaken close -< ["value" =: "left"]
-- >   )
--
-- The record of a flow is the record that it is run on, with a field in front of it for each step that has a name. A
-- name in the flow is a field of that record: an input of the flow, or the name of an earlier step.
--
-- * @"out" <: p -< bindings@ runs the step @p@ and keeps what it gives in the record of the flow under @"out"@. A line
--   without @"out" <:@ keeps nothing: the record goes on as it was.
-- * @p -< bindings@ makes the input of the step @p@ from the record of the flow. The input of a step is a record that
--   has a field for each binding, in the order written, and the step takes it as it takes any record, by the names of
--   the fields that it reads, as in @(HasField "i" r Int) => arr r Bool@. The left side of a binding is a port of the
--   step, which is a field of its input. The right side is a name in the flow: an input of the flow, the name of an
--   earlier step, or @"step.port"@ for the field @port@ of what a step gave, or a longer path such as @"a.b.c"@. A
--   bare name, @"cores"@, binds the port of that name to the name in the flow that is the same, and a binding that has
--   two sides is written @"port" =: "name"@.
-- * A name that is bound to several ports, and a step whose ports are bound to several producers, are only bindings.
--
-- A step that takes a plain value for its input, and not a record, reads it from a port when it is made one that does:
-- @field "loans" >>> borrow@ takes the port @"loans"@.
--
-- A choice is a line of a flow too:
--
-- > "kept" <: choice "how did the head end?" "head"
-- >   (    "stream ended" ?> quit    -< []
-- >    ||| "refused"      ?> refuse  -< ["refused", "conn"]
-- >    ||| "good"         ?> answered -< ["good", "conn", "loans", "loan", "progress"] )
--
-- It decides on the sum that the flow has under the name @"head"@, and runs the branch of the case that it is. A branch,
-- @"case" ?> p -< bindings@, binds the names of the flow, and the value of its case under the name of the case, and the
-- input of its step has only the ports that it binds. The branches give the same type, which is what @"kept"@ has. The
-- errors of '-<' are those of a branch, and a case that the sum doesn't have is the error of "Network.Streaming.Named".
--
-- A list of names on the left of '<:' takes the output of the step apart, as a pattern does:
--
-- > ["socket", "peer"]             <: acceptFrom -< ["listener"]
-- > ["socket" =: "client", "peer"] <: acceptFrom -< ["listener"]
-- > ["kept", "joined"]             <: group AwaitAll workers -< ["cores"]
--
-- A record, whose fields are @name := a@, is taken apart by the names of its fields: the list may name some of them, the
-- others are not kept, and @"field" =: "name"@ keeps a field under another name. A pair, or pairs that nest to the right,
-- is taken apart by position: the list has a name for each part, and @"_"@ keeps none. A record is told from a pair by
-- its first part, which is a field. Each name of the list joins the flow as the name of a step does, in front of the
-- record, in the order of the list, and it is the name of an earlier step to the lines after it. There are no nested
-- patterns: a path, @"out.x"@, reaches deeper.
--
-- These are type errors that say what they are: a name that the flow doesn't have, an output that a step doesn't give,
-- a port that is not bound, a port that is bound twice, a step whose name is one that the flow has already, and, for a
-- list of names, a field that the output doesn't have, too few or too many names for a pair, a name that the list has
-- twice, a field that is named for a pair, and an output that is neither a record nor a pair.
--
-- In diagrams the box of a step that has names has them for its title, comma-separated, and each name that a step binds
-- is an edge into it from where the name comes from: the step that gives it, or where the flow is entered for a name of
-- the flow's input. An edge says the names that it has, as in @x → i@ for the port @i@ bound to the field @x@ of a step, and the
-- names of one producer that a step binds share one edge. A step that binds nothing from the step before it has an edge
-- from it that says only the order. A choice is drawn in the box of its line, with its question, and the edge from it to
-- a branch says its case, which is where the value of the case comes from.
module Network.Streaming.Wiring
  ( Flow,
    Line,
    Lined,
    (-<),
    (<:),
    Outs,
    choice,
    type (=:),
    flow,
    result,
    Inputs,
  )
where

import Control.Arrow (Arrow, (>>>), (>>^))
import qualified Control.Arrow as Arrow
import Control.Category (Category (..))
import Control.Exception (throw)
import Data.Kind (Constraint, Type)
import Data.Proxy (Proxy (..))
import GHC.Records (HasField (..))
import GHC.TypeLits
  ( AppendSymbol,
    CmpChar,
    ConsSymbol,
    ErrorMessage (..),
    KnownSymbol,
    Symbol,
    TypeError,
    UnconsSymbol,
    symbolVal,
  )
import Network.Streaming.Annotate (Annotate (..), Binding (..), (-|))
import Network.Streaming.Named (CaseNames, DistinctCases, Distribute, (:=) (..), cases)
import Network.Streaming.NotImplemented (NotImplemented (..))
import Network.Streaming.Plumbing (alongside)
import Prelude hiding (id, (.))

-- * Flows

-- | A flow: an arrow from the record that the flow reads its names from to that record with the outputs of the steps
-- that were named in front of it.
newtype Flow arr x y = Flow (arr x y)

instance (Category arr) => Category (Flow arr) where
  id = Flow id
  {-# INLINE id #-}
  Flow after . Flow before = Flow (after . before)
  {-# INLINE (.) #-}

-- | A step with the input that its bindings make, which is told, by what it is used as, what to do with its output.
data Line arr x o = Line [Binding] (arr x o)

-- | The record of the flow that a line is a part of. It is a type family, and not only the functional dependency of
-- 'Lined', because GHC solves equalities before it looks for instances, and the names of the flow are to be looked for
-- in a record that is known.
type family FlowIn (t :: Type) :: Type where
  FlowIn (Flow arr x y) = x
  FlowIn (Line arr x o) = x
  FlowIn (arr x o) = x

-- | What a line can be: a step that keeps nothing, which is a flow whose record goes on as it was, a step that is given
-- a name by '<:', or a step that is an arrow of its own, as the branch of a choice is. The first two are incoherent so
-- that an arrow whose type is that of a signature, which GHC cannot tell from a flow, is the third.
class Lined t arr x o where
  line :: [Binding] -> arr x o -> t

-- | A step that gives nothing to keep: the record goes on as it was.
instance {-# INCOHERENT #-} (Arrow arr, Annotate arr, arr' ~ arr, x' ~ x, y ~ x) => Lined (Flow arr' x' y) arr x o where
  line texts p = Flow (alongside (flowStep Nothing texts p) >>^ snd)
  {-# INLINE line #-}

-- | A step whose output is to be named.
instance {-# INCOHERENT #-} (arr' ~ arr, x' ~ x, o' ~ o) => Lined (Line arr' x' o') arr x o where
  line = Line
  {-# INLINE line #-}

-- | A step that is an arrow of its own, as a branch of a choice is: it takes the record that its input is made from.
instance (Annotate arr, arr' ~ arr, x' ~ x, o' ~ o) => Lined (arr' x' o') arr x o where
  line = flowStep Nothing
  {-# INLINE line #-}

-- | The flow as an arrow: it takes the record that its names are read from, and gives that record with the outputs of
-- the steps that have names in front of it.
flow :: (Annotate arr) => Flow arr x y -> arr x y
flow (Flow p) = flowScope p
{-# INLINE flow #-}

-- | The flow as an arrow that gives what its step of the name gave.
result :: forall name -> forall arr x y a. (Arrow arr, Annotate arr, HasField name y a) => Flow arr x y -> arr x a
result name (Flow p) = flowScope p >>^ getField @name
{-# INLINE result #-}

-- * Lines

-- '-<' binds tighter than '<:', '>>>' and '?>', so that a line, and a branch of a choice, need no parentheses.
infix 7 -<

-- | The step, with its input made from the flow's record by the bindings. What it is used as says what it does with its
-- output: it keeps none if it is a line of the flow, names it if it is the operand of '<:', and gives it if it is the
-- operand of '?>'.
(-<) ::
  forall arr i o x t.
  (Arrow arr) =>
  arr i o ->
  forall (bindings :: [Symbol]) ->
  (x ~ FlowIn t, Lined t arr x o, Unique (Ports bindings), Describe bindings, Bind bindings x i) =>
  t
(-<) p bindings = line (describe @bindings) (Arrow.arr (bind @bindings @x) >>> p)
{-# INLINE (-<) #-}

infixr 2 <:

-- | The line, whose output is kept in the record of the flow under the name, or taken apart into the names of a list, in
-- front of the rest. The flow doesn't have the names already.
(<:) ::
  forall k.
  forall (out :: k) ->
  forall arr x o y.
  (Arrow arr, Annotate arr, Outs out x o y) =>
  Line arr x o ->
  Flow arr x y
(<:) out bound = outcome @out bound
{-# INLINE (<:) #-}

-- | What a step gives the flow under what is on the left of '<:': a name keeps the whole output, and a list of names
-- takes it apart.
class Outs out x o y | out x o -> y where
  -- | The flow that runs the step, with the names of what it gave in front of the record.
  outcome :: (Arrow arr, Annotate arr) => Line arr x o -> Flow arr x y

-- | A name: the output is kept as it is.
instance (KnownSymbol name, Fresh name x, y ~ (name := o, x)) => Outs (name :: Symbol) x o y where
  outcome (Line texts p) =
    Flow (alongside (flowStep (Just (symbolVal (Proxy @name))) texts p) >>^ \(o, x) -> (Is o, x))
  {-# INLINE outcome #-}

-- | A list of names: the output is taken apart, and each name is a part of it.
instance
  (Pattern (ShapeOf o) entries o x y, Distinct (Given entries), AllFresh (Given entries) x) =>
  Outs (entries :: [Symbol]) x o y
  where
  outcome _ = throw (NotImplemented "<:")
  {-# INLINE outcome #-}

-- | A choice that is a line of the flow: it decides on the sum that the flow has under the name of the field, as
-- @decide question (cases field)@ does, and runs the branch of the case that it is. A branch is a step of its own,
-- @"case" ?> p -< bindings@, which reads the names of the flow and the value of its case under the name of the case.
choice ::
  forall t arr x s d o.
  String ->
  forall field ->
  ( Arrow arr,
    Annotate arr,
    KnownSymbol field,
    x ~ FlowIn t,
    Lined t arr x o,
    HasField field x s,
    Distribute s x d,
    DistinctCases (CaseNames s)
  ) =>
  arr d o ->
  t
choice question field branches =
  line [Binding name [name]] (Arrow.arr (cases field :: x -> d) -| question >>> branches)
  where
    name = symbolVal (Proxy @field)
{-# INLINE choice #-}

-- * Patterns

-- | How an output is taken apart by a list of names.
data Shape
  = -- | By the names of its fields: a record, which is a pair of fields that nests to the right.
    Fields
  | -- | By position: a pair, or pairs that nest to the right.
    Positions
  | -- | Not at all.
    Opaque

-- | How the output is taken apart: a record is told from a pair by its first part, which is a field.
type family ShapeOf (o :: Type) :: Shape where
  ShapeOf (name := a, rest) = 'Fields
  ShapeOf (name := a) = 'Fields
  ShapeOf (a, b) = 'Positions
  ShapeOf o = 'Opaque

-- | Whether a value that is a part of a pair is a pair that goes on.
type family Continues (o :: Type) :: Bool where
  Continues o = ContinuesAs (ShapeOf o)

-- | Whether a value that is taken apart by position goes on.
type family ContinuesAs (shape :: Shape) :: Bool where
  ContinuesAs 'Positions = 'True
  ContinuesAs shape = 'False

-- | The names in the flow that a list of entries gives, in order: the entries that are not @"_"@.
type family Given (entries :: [Symbol]) :: [Symbol] where
  Given '[] = '[]
  Given (entry ': entries) = Keep (Source entry) (Given entries)

-- | The name in front of the others, unless it is @"_"@.
type family Keep (name :: Symbol) (more :: [Symbol]) :: [Symbol] where
  Keep "_" more = more
  Keep name more = name ': more

-- | What is said of a name that a list has twice.
type Twice name = 'Text "The pattern has the name " ':<>: 'ShowType name ':<>: 'Text " twice"

-- | No name is twice in the list.
type family Distinct (more :: [Symbol]) :: Constraint where
  Distinct '[] = ()
  Distinct (name ': rest) = (NotTwice name rest, Distinct rest)

-- | The name is not one of the names.
type family NotTwice (name :: Symbol) (more :: [Symbol]) :: Constraint where
  NotTwice name '[] = ()
  NotTwice name (name ': rest) = TypeError (Twice name)
  NotTwice name (other ': rest) = NotTwice name rest

-- | The flow doesn't have any of the names.
type family AllFresh (more :: [Symbol]) (r :: Type) :: Constraint where
  AllFresh '[] r = ()
  AllFresh (name ': rest) r = (Fresh name r, AllFresh rest r)

-- | What is said of an output that a list of names can't take apart.
type Whole = 'Text "The output of the step is neither a record nor a pair, so a list of names can't take it apart"

-- | What is said of a list that has fewer names than the parts of a pair.
type TooFew = 'Text "The pattern has too few names for the output of the step"

-- | What is said of a list that has more names than the parts of a pair.
type TooMany = 'Text "The pattern has too many names for the output of the step"

-- | What is said of a field that is named for a pair, which has none.
type NotFields entry =
  'Text "The output of the step is a pair, taken apart by position: "
    ':<>: 'ShowType (Port entry)
    ':<>: 'Text " =: "
    ':<>: 'ShowType (Source entry)
    ':<>: 'Text " names a field, and a pair has none"

-- | An entry for a pair is a name, and not a field and a name.
type family ByPosition (entry :: Symbol) :: Constraint where
  ByPosition entry = ByPositionOf (Break '=' entry) entry

-- | The entry, from a split at its first @=@.
type family ByPositionOf (split :: (Symbol, Maybe Symbol)) (entry :: Symbol) :: Constraint where
  ByPositionOf '(before, 'Nothing) entry = ()
  ByPositionOf '(before, 'Just after) entry = TypeError (NotFields entry)

-- | The record of the flow after a step whose output is taken apart by a list of entries: in front of it the names that
-- the entries give, in the order of the list. How the output is taken apart is its shape.
class Pattern (shape :: Shape) (entries :: [Symbol]) (o :: Type) (x :: Type) (y :: Type) | shape entries o x -> y

-- | A record: each entry is a field of it, and the fields that the list doesn't have are not kept.
instance Pattern 'Fields '[] o x x

instance
  (Dig 'InPattern (Port entry) '[] o a, Pattern 'Fields entries o x rest, Place (Source entry) a rest y) =>
  Pattern 'Fields (entry ': entries) o x y

-- | A pair, or pairs that nest to the right: each entry is a part of it.
instance (Spread 'True entries o x y) => Pattern 'Positions entries o x y

instance (y ~ TypeError Whole) => Pattern 'Opaque entries o x y

-- | The record of the flow after a value is taken apart by position: a value that is a pair, when the first of its
-- parts is the first of the entries, and the rest of the value is the rest of them.
class Spread (pair :: Bool) (entries :: [Symbol]) (o :: Type) (x :: Type) (y :: Type) | pair entries o x -> y

-- | The last entry is the last part, which is a value that goes on no further.
instance (ByPosition entry, Place (Source entry) o x y) => Spread 'False '[entry] o x y

-- | More entries than parts.
instance (y ~ TypeError TooMany) => Spread 'False (entry ': next ': entries) o x y

-- | A pair whose rest is left without an entry.
instance (y ~ TypeError TooFew) => Spread 'True '[entry] o x y

instance
  (ByPosition entry, Spread (Continues b) (next ': entries) b x rest, Place (Source entry) a rest y) =>
  Spread 'True (entry ': next ': entries) (a, b) x y

-- | The name of a part, in front of the record of the flow: unless it is @"_"@, which keeps nothing.
class Place (name :: Symbol) (a :: Type) (rest :: Type) (y :: Type) | name a rest -> y

instance {-# OVERLAPPING #-} Place "_" a rest rest

instance (y ~ (name := a, rest)) => Place name a rest y

-- * Bindings

-- | What a binding is written as when it has both sides: the port, and the name in the flow.
type family (port :: Symbol) =: (name :: Symbol) :: Symbol where
  port =: name = AppendSymbol port (AppendSymbol "=" name)

-- | A split of a symbol at the first of a character: what comes before it, and what comes after, if it was there.
type family Break (c :: Char) (s :: Symbol) :: (Symbol, Maybe Symbol) where
  Break c s = BreakFrom c "" (UnconsSymbol s)

-- | The split, from a symbol that is taken apart, with what came before the character so far.
type family BreakFrom (c :: Char) (before :: Symbol) (rest :: Maybe (Char, Symbol)) :: (Symbol, Maybe Symbol) where
  BreakFrom c before 'Nothing = '(before, 'Nothing)
  BreakFrom c before ('Just '(h, t)) = BreakAt (CmpChar h c) c before h t

-- | The split, at a character that is compared with the one that is looked for.
type family
  BreakAt (o :: Ordering) (c :: Char) (before :: Symbol) (h :: Char) (t :: Symbol) ::
    (Symbol, Maybe Symbol)
  where
  BreakAt 'EQ c before h t = '(before, 'Just t)
  BreakAt o c before h t = BreakFrom c (AppendSymbol before (ConsSymbol h "")) (UnconsSymbol t)

-- | The port of a binding: what is before the @=@, or the whole of a bare name.
type family Port (binding :: Symbol) :: Symbol where
  Port binding = Before (Break '=' binding)

-- | The name in the flow that a binding has: what is after the @=@, or the whole of a bare name.
type family Source (binding :: Symbol) :: Symbol where
  Source binding = After (Break '=' binding) binding

-- | The first half of a split.
type family Before (split :: (Symbol, Maybe Symbol)) :: Symbol where
  Before '(before, rest) = before

-- | The second half of a split, or the symbol it is made from when the character wasn't there.
type family After (split :: (Symbol, Maybe Symbol)) (whole :: Symbol) :: Symbol where
  After '(before, 'Nothing) whole = whole
  After '(before, 'Just after) whole = after

-- | The names of a path, which is separated by dots: the name in the flow, then the fields of what it is.
type family Path (source :: Symbol) :: [Symbol] where
  Path source = PathOf (Break '.' source)

-- | The path, from a split at its first dot.
type family PathOf (split :: (Symbol, Maybe Symbol)) :: [Symbol] where
  PathOf '(name, 'Nothing) = '[name]
  PathOf '(name, 'Just rest) = name ': Path rest

-- | The ports of the bindings, in order.
type family Ports (bindings :: [Symbol]) :: [Symbol] where
  Ports '[] = '[]
  Ports (binding ': bindings) = Port binding ': Ports bindings

-- | What is said of a port that two bindings have.
type BoundTwice port = 'Text "Input " ':<>: 'ShowType port ':<>: 'Text " of the step is bound twice"

-- | No port has two bindings.
type family Unique (ports :: [Symbol]) :: Constraint where
  Unique '[] = ()
  Unique (port ': ports) = (NotIn port ports, Unique ports)

-- | The port is not one of the ports.
type family NotIn (port :: Symbol) (ports :: [Symbol]) :: Constraint where
  NotIn port '[] = ()
  NotIn port (port ': ports) = TypeError (BoundTwice port)
  NotIn port (other ': ports) = NotIn port ports

-- | The bindings, for diagrams: the port of each, and the path that it reads.
class Describe (bindings :: [Symbol]) where
  describe :: [Binding]

instance Describe '[] where
  describe = []
  {-# INLINE describe #-}

instance (KnownSymbol (Port binding), Names (Path (Source binding)), Describe bindings) => Describe (binding ': bindings) where
  describe = Binding (symbolVal (Proxy @(Port binding))) (names @(Path (Source binding))) : describe @bindings
  {-# INLINE describe #-}

-- | The names of a path, as text.
class Names (path :: [Symbol]) where
  names :: [String]

instance Names '[] where
  names = []
  {-# INLINE names #-}

instance (KnownSymbol name, Names path) => Names (name ': path) where
  names = symbolVal (Proxy @name) : names @path
  {-# INLINE names #-}

-- | The record that a step takes as its input, whose fields are its ports. A port that is not bound is a type error.
newtype Inputs r = Inputs r

-- | Reads a port of the step: the value that was bound to it.
instance (Dig 'Unbound name '[] r a) => HasField name (Inputs r) a where
  getField (Inputs r) = dig @'Unbound @name @'[] r
  {-# INLINE getField #-}

-- | The input of a step made from the bindings, by the record of the flow that the bindings are read from.
class Bind (bindings :: [Symbol]) x i | bindings x -> i where
  bind :: x -> i

-- | A step that has no bindings takes nothing.
instance Bind '[] x () where
  bind _ = ()
  {-# INLINE bind #-}

instance (Record (binding ': bindings) x r) => Bind (binding ': bindings) x (Inputs r) where
  bind = Inputs . record @(binding ': bindings)
  {-# INLINE bind #-}

-- | The fields of the input of a step: a pair for each binding but the last, which is a field alone.
class Record (bindings :: [Symbol]) x r | bindings x -> r where
  record :: x -> r

instance (port ~ Port binding, Fetch (Path (Source binding)) x a) => Record '[binding] x (port := a) where
  record x = Is (fetch @(Path (Source binding)) x)
  {-# INLINE record #-}

instance
  (port ~ Port binding, Fetch (Path (Source binding)) x a, Record (next ': bindings) x rest) =>
  Record (binding ': next ': bindings) x (port := a, rest)
  where
  record x = (Is (fetch @(Path (Source binding)) x), record @(next ': bindings) x)
  {-# INLINE record #-}

-- | The value that a path of the flow has: the name in the flow, then a field of each value on the way.
class Fetch (path :: [Symbol]) x a | path x -> a where
  fetch :: x -> a

instance (Dig 'InFlow name fields x a) => Fetch (name ': fields) x a where
  fetch = dig @'InFlow @name @fields
  {-# INLINE fetch #-}

-- * Names

-- | Where a name is looked for, which says what is said when it is not there.
data Where
  = -- | In the record of the flow.
    InFlow
  | -- | In the output of the step that has the name.
    InOutput Symbol
  | -- | Among the ports of the input of a step.
    Unbound
  | -- | Among the fields of the output of a step that a list of names takes apart.
    InPattern

-- | What is said of a name that isn't there.
type family Absence (place :: Where) (name :: Symbol) :: ErrorMessage where
  Absence 'InFlow name = 'Text "The flow has no name " ':<>: 'ShowType name
  Absence ('InOutput step) name =
    'Text "The step " ':<>: 'ShowType step ':<>: 'Text " has no output " ':<>: 'ShowType name
  Absence 'Unbound name = 'Text "Input " ':<>: 'ShowType name ':<>: 'Text " of the step isn't bound"
  Absence 'InPattern name = 'Text "The output of the step has no field " ':<>: 'ShowType name

-- | Where the fields of the value that has the name are looked for: in the output of the step of that name.
type family Below (place :: Where) (name :: Symbol) :: Where where
  Below 'InFlow name = 'InOutput name
  Below ('InOutput step) name = 'InOutput (AppendSymbol step (AppendSymbol "." name))

-- | The value of the field of the name in a record that nests to the right, as 'getField' reads it, and then of the
-- fields of the path that are left, in that value. A record that has no field of the name is the error that the place
-- says. The instance that finds the field gives the type of its value, so that what is left is looked for in a type
-- that is known.
class Dig (place :: Where) (name :: Symbol) (fields :: [Symbol]) r a | place name fields r -> a where
  dig :: r -> a

-- | The first field, when it has the name: the newest field of a name is the one that is read.
instance {-# OVERLAPPING #-} (Onward place name fields v a) => Dig place name fields (name := v, rest) a where
  dig (Is v, _) = onward @place @name @fields v
  {-# INLINE dig #-}

-- | A later field, found in the rest.
instance (Dig place name fields rest a) => Dig place name fields (other := b, rest) a where
  dig (_, rest) = dig @place @name @fields rest
  {-# INLINE dig #-}

-- | The last field, when it has the name.
instance {-# OVERLAPPING #-} (Onward place name fields v a) => Dig place name fields (name := v) a where
  dig (Is v) = onward @place @name @fields v
  {-# INLINE dig #-}

-- | The last field, when it has another name: the record has no field of the name.
instance
  {-# OVERLAPPABLE #-}
  (a ~ TypeError (Absence place name)) =>
  Dig place name fields (other := b) a
  where
  dig _ = error "Network.Streaming.Wiring: a name that the flow doesn't have is a type error"

-- | A record that is not made of the fields of this module: the record that a flow is run on, which is any record that
-- has the names that the steps read, or a value of a type of its own that a step gave. A field of it is read by its own
-- 'HasField'. The instance is chosen at once for a record that is not known yet, which is read all the same, but a
-- name that it doesn't have is then not said as it is here.
instance {-# INCOHERENT #-} (HasField name r v, Onward place name fields v a) => Dig place name fields r a where
  dig r = onward @place @name @fields (getField @name r)
  {-# INLINE dig #-}

-- | What is left of a path, from the value of the name that was found: the value itself if nothing is left, and else
-- the next field of that value.
class Onward (place :: Where) (name :: Symbol) (fields :: [Symbol]) v a | place name fields v -> a where
  onward :: v -> a

instance (a ~ v) => Onward place name '[] v a where
  onward v = v
  {-# INLINE onward #-}

instance (Dig (Below place name) field fields v a) => Onward place name (field ': fields) v a where
  onward = dig @(Below place name) @field @fields
  {-# INLINE onward #-}

-- | What is said of a step whose name the flow has already.
type Repeated name = 'Text "The flow already has the name " ':<>: 'ShowType name

-- | The record of the flow doesn't have the name yet.
class Fresh (name :: Symbol) (r :: Type)

instance {-# OVERLAPPING #-} (TypeError (Repeated name)) => Fresh name (name := a, rest)

instance (Fresh name rest) => Fresh name (other := a, rest)

instance {-# OVERLAPPING #-} (TypeError (Repeated name)) => Fresh name (name := a)

instance {-# OVERLAPPABLE #-} Fresh name (other := a)

-- | A record that isn't made of fields of this module: its fields aren't known.
instance {-# INCOHERENT #-} Fresh name r
