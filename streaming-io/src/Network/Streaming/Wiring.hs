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
-- >   (    "stream ended"                   ?> quit    -< []
-- >    ||| "refused" =: "status"            ?> refuse  -< ["status", "conn"]
-- >    ||| "good" =: ["parsed", "consumed"] ?> answered -< ["parsed", "consumed", "conn", "loans", "loan", "progress"] )
--
-- It decides on the sum that the flow has under the name @"head"@, and runs the branch of the case that it is. A branch
-- is @"case" ?> p -< bindings@, which selects the case and binds nothing from its value, or @"case" =: pattern ?> p -<
-- bindings@, which takes the value of the case apart by the pattern, as is told below, and the names of the pattern join
-- the names of the flow that the branch sees, in front of them. The input of the step of a branch has only the ports that
-- it binds. The branches give the same type, which is what @"kept"@ has. The errors of '-<' are those of a branch, and a
-- case that the sum doesn't have is the error of "Network.Streaming.Named".
--
-- The left of '<:' is a pattern for the output of the step, as @R { socket = S { fd, addr }, peer }@ is in Haskell:
--
-- > "socket"                                  <: accept         -< ["listener"]
-- > "conn" @: ["socket", "peer"]              <: acceptFrom     -< ["listener"]
-- > ["socket", "peer"]                        <: acceptFrom     -< ["listener"]
-- > ["socket" =: ["fd", "addr"], "peer"]      <: acceptFrom     -< ["listener"]
-- > ["socket" =: "s" @: ["fd", "addr"], "peer"] <: acceptFrom   -< ["listener"]
-- > ["kept", "joined"]                        <: group AwaitAll workers -< ["cores"]
--
-- A name keeps the output as it is. @"name" \@: [patterns]@ keeps it whole under @name@ and takes it apart, and @"_" \@:
-- [patterns]@ takes it apart and keeps nothing whole. A list takes the output apart: a record, whose fields are @name := a@,
-- by the names of its fields, and a pair, or pairs that nest to the right, by position. A record is told from a pair by
-- its first part, which is a field. In a list for a record, @"field"@ is the field of that name, which is kept under it,
-- and @"field" =: pattern@ is the field, taken by the pattern: a name keeps it under that name, a list takes it apart, and
-- @name \@: [patterns]@ does both. The fields that the list doesn't name are not kept. In a list for a pair, each pattern
-- is for a part, @"_"@ keeps none, and the last pattern is for what is left, whole: on @(a, (b, c))@, @["a", "bc"]@ gives
-- @bc = (b, c)@, and @["a", "b", "c"]@ takes it apart. Patterns nest to any depth. Each name of a pattern joins the flow
-- as the name of a step does, in front of the record, the whole before its parts, and it is the name of an earlier step
-- to the lines after it. A pattern is not for the right of '-<', whose bindings read the names of the flow: a path,
-- @"out.x"@, reaches into the output of a step there.
--
-- These are type errors that say what they are: a name that the flow doesn't have, an output that a step doesn't give,
-- a port that is not bound, a port that is bound twice, a step whose name is one that the flow has already, a binding that
-- is a pattern, and, for a pattern, a field that the output doesn't have, too many names for a pair, a name that the
-- pattern has twice, a field that is named for a pair, a field for the output of a step, and a value that is neither a
-- record nor a pair that a list takes apart. An error in a nested pattern says where it is, as in @in "socket": the
-- output of the step has no field "w"@.
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
    type (@:),
    flow,
    result,
    Inputs,
  )
where

import Control.Arrow (Arrow, (>>>), (>>^))
import qualified Control.Arrow as Arrow
import Control.Category (Category (..))
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
import Network.Streaming.Internal.Patterns
  ( AllFresh,
    Dig (..),
    Distinct,
    Entry (..),
    Given,
    Level (..),
    Parse,
    Parses,
    Root,
    ShapeOf,
    Take (..),
    Where (..),
    type (=:),
    type (@:),
    (:=) (..),
  )
import Network.Streaming.Named (CaseNames, DistinctCases, Distribute, cases)
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
  line texts p = Flow (alongside (flowStep [] texts p) >>^ snd)
  {-# INLINE line #-}

-- | A step whose output is to be named.
instance {-# INCOHERENT #-} (arr' ~ arr, x' ~ x, o' ~ o) => Lined (Line arr' x' o') arr x o where
  line = Line
  {-# INLINE line #-}

-- | A step that is an arrow of its own, as a branch of a choice is: it takes the record that its input is made from.
instance (Annotate arr, arr' ~ arr, x' ~ x, o' ~ o) => Lined (arr' x' o') arr x o where
  line = flowStep []
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
  (x ~ FlowIn t, Lined t arr x o, Unique (Ports bindings), Plain bindings, Describe bindings, Bind bindings x i) =>
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

-- | A name, or a name that is kept and taken apart: the output is kept as it is, and its parts too if the pattern has
-- them.
instance
  ( NoField (Parse out),
    Names (Given '[Parse out]),
    Distinct (Given '[Parse out]),
    AllFresh (Given '[Parse out]) x,
    Take Root (Parse out) o x y
  ) =>
  Outs (out :: Symbol) x o y
  where
  outcome (Line texts p) =
    Flow (alongside (flowStep (names @(Given '[Parse out])) texts p) >>^ \(o, x) -> takeEntry @Root @(Parse out) o x)
  {-# INLINE outcome #-}

-- | A list of patterns: the output is taken apart, and each name is a part of it.
instance
  ( Level (ShapeOf o) Root (Parses entries) o x y,
    Names (Given (Parses entries)),
    Distinct (Given (Parses entries)),
    AllFresh (Given (Parses entries)) x
  ) =>
  Outs (entries :: [Symbol]) x o y
  where
  outcome (Line texts p) =
    Flow (alongside (flowStep (names @(Given (Parses entries))) texts p) >>^ \(o, x) -> level @(ShapeOf o) @Root @(Parses entries) o x)
  {-# INLINE outcome #-}

-- | The pattern on the left of '<:' is not for a field: it is the output of the step that is taken.
type family NoField (entry :: Entry) :: Constraint where
  NoField ('Entry 'Nothing name parts) = ()
  NoField ('Entry ('Just field) name parts) =
    TypeError ('Text "The output of a step is not a field: " ':<>: 'ShowType field ':<>: 'Text " =: has nothing to take a field of")

-- | A choice that is a line of the flow: it decides on the sum that the flow has under the name of the field, as
-- @decide question (cases field)@ does, and runs the branch of the case that it is. A branch is a step of its own,
-- @"case" ?> p -< bindings@, which reads the names of the flow, and the names that the pattern of its case gives, if it
-- has one: @"case" =: pattern ?> p -< bindings@.
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

-- * Bindings

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

-- | What is said of a binding that is a pattern: a list of names, and @\@:@, take a value apart, which is for the left of
-- '<:'.
type Pattern_ binding =
  'Text "A binding reads a name of the flow, and not a pattern: "
    ':<>: 'ShowType binding
    ':<>: 'Text " takes a value apart, which is for the left of <:"

-- | No binding is a pattern: a list of names, and @\@:@, are for the left of '<:'.
type family Plain (bindings :: [Symbol]) :: Constraint where
  Plain '[] = ()
  Plain (binding ': bindings) = (Reads (Break '(' binding) binding, Plain bindings)

-- | The binding, from a split at its first parenthesis, which every list of names and every @\@:@ has.
type family Reads (split :: (Symbol, Maybe Symbol)) (binding :: Symbol) :: Constraint where
  Reads '(before, 'Nothing) binding = ()
  Reads '(before, 'Just after) binding = TypeError (Pattern_ binding)

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
