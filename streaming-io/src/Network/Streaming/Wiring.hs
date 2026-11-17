{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
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
-- * @p -< bindings@ makes the input of the step @p@ from the record of the flow. What is on the right of @-<@ is a name, a
--   list or a tuple, as what is on the left of '<:' is a name, which keeps the whole output, or a list, which takes it
--   apart. A name in the flow is an input of the flow, the name of an earlier step, or a path: @"step.port"@ is the field
--   @port@ of what a step gave, and @"a.b.c"@ is a field of a field.
-- * @p -< "name"@ makes the input of the step @p@ the value that the name has in the flow, whole, as @borrow -< "loans"@
--   gives @borrow@ the loans. A path works too: @p -< "head.parsed"@ gives the field @parsed@ of what @head@ is.
-- * @p -< ["a", "b"]@ makes the input of the step @p@ a record that has a field for each binding, in the order written, and
--   the step takes it as it takes any record, by the names of the fields that it reads, as in @(HasField "i" r Int) => arr
--   r Bool@. The left side of a binding is a port of the step, which is a field of its input. The right side is a name in
--   the flow. A bare name, @"cores"@, binds the port of that name to the name in the flow that is the same, and a binding
--   that has two sides is written @"port" =: "name"@. The empty list, @[]@, gives the step @()@.
-- * @p -< ("a", "b")@ makes the input of the step @p@ the pair of the values of the names, for a step that takes a pair.
--   Three names, @("a", "b", "c")@, give pairs that nest to the right, @(a, (b, c))@, which is what a chain of @&&&@ makes
--   and what the left of '<:' takes apart. Each name is a name of the flow, or a path.
-- * A name and a tuple have no ports: @"port" =: "name"@ in either is a type error, as a pattern is.
-- * A name that is bound to several ports, and a step whose ports are bound to several producers, are only bindings.
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
-- is a pattern, a port in a name or a tuple, and, for a pattern, a field that the output doesn't have, too many names for a
-- pair, a name that the pattern has twice, a field that is named for a pair, a field for the output of a step, and a value
-- that is neither a record nor a pair that a list takes apart. An error in a nested pattern says where it is, as in
-- @in "socket": the output of the step has no field "w"@.
--
-- In diagrams each name is a box, nested as its pattern nests. The box of a step is titled with the name that keeps its output
-- whole, if it has one, and holds the step and a box for each name of its pattern: a small node for a name, and a box titled
-- with its name, or else its field, for a part that is taken apart. A branch's pattern gives boxes beside the branch's step, in
-- the box of the choice. An edge into a step starts at the exact box of each name that it binds, and says nothing of the name,
-- which the box says, but the port if it is another name, as @→ i@, and the field of a path into a whole, as @x → i@. A name
-- of the flow's input is an edge from where the flow is entered, which says the names, and the names of one producer that a
-- step binds share one edge. A step that binds nothing from the step before it has a dotted edge from it, which says only the
-- order, in grey. A name that a step takes whole, and the names of a tuple, are drawn as the same names are in a list. A
-- choice is drawn in the box of its line, with its question, and the edge from it to a branch says its case.
module Network.Streaming.Wiring
  ( Flow,
    Line,
    Lined,
    (-<),
    (<:),
    Outs,
    releasedBy,
    onExit,
    inside,
    inside_,
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
import Network.Streaming.Annotate (Annotate (..), Binding (..), Part (..), (-|))
import Network.Streaming.Classes (Exit, Scope)
import Network.Streaming.Internal.Patterns
  ( AllFresh,
    Described (..),
    DescribedList (..),
    Dig (..),
    Distinct,
    Entry (..),
    Fresh,
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
import Network.Streaming.NotImplemented (NotImplemented (..))
import Network.Streaming.Plumbing (alongside)
import Prelude hiding (id, (.))

-- * Flows

-- | A flow: what it does with the record that the flow reads its names from, to give that record with the outputs of the
-- steps that were named in front of it, followed by an arrow that is given what the lines after it are.
newtype Flow arr x y = Flow (forall z. arr y z -> arr x z)

instance Category (Flow arr) where
  id = Flow (\rest -> rest)
  {-# INLINE id #-}
  Flow after . Flow before = Flow (\rest -> before (after rest))
  {-# INLINE (.) #-}

-- | What a line is besides its step: a step that is only run, a step that is released when the rest of the flow is done, which
-- has the type of the record that its release reads, and a wrapper that the rest of the flow is the body of.
data How = Plainly | Releasing Type | Wrapped

-- | A step with the input that its bindings make, which is told, by what it is used as, what to do with its output. A line
-- that is released after the rest of the flow has the step that releases it, with the input that its own bindings make, and
-- a line that is a wrapper has the function that wraps the rest of the flow.
data Line arr x o (how :: How) where
  Line :: [Binding] -> arr x o -> Line arr x o 'Plainly
  Held :: (Scope arr) => [Binding] -> arr x o -> [Binding] -> arr w () -> Line arr x o ('Releasing w)
  Inside :: [Binding] -> (x -> i) -> (forall z. arr (o, x) z -> arr (i, x) z) -> Line arr x o 'Wrapped

-- | The record of the flow that a line is a part of. It is a type family, and not only the functional dependency of
-- 'Lined', because GHC solves equalities before it looks for instances, and the names of the flow are to be looked for
-- in a record that is known.
type family FlowIn (t :: Type) :: Type where
  FlowIn (Flow arr x y) = x
  FlowIn (Line arr x o how) = x
  FlowIn (arr x o) = x

-- | What a line can be: a step that keeps nothing, which is a flow whose record goes on as it was, a step that is given
-- a name by '<:', or a step that is an arrow of its own, as the branch of a choice is. The first two are incoherent so
-- that an arrow whose type is that of a signature, which GHC cannot tell from a flow, is the third.
class Lined t arr x o where
  line :: [Binding] -> arr x o -> t

-- | A step that gives nothing to keep: the record goes on as it was.
instance {-# INCOHERENT #-} (Arrow arr, Annotate arr, arr' ~ arr, x' ~ x, y ~ x) => Lined (Flow arr' x' y) arr x o where
  line texts p = Flow (\rest -> (alongside (flowStep Nothing texts p) >>^ snd) >>> rest)
  {-# INLINE line #-}

-- | A step whose output is to be named.
instance {-# INCOHERENT #-} (arr' ~ arr, x' ~ x, o' ~ o, how ~ 'Plainly) => Lined (Line arr' x' o' how) arr x o where
  line = Line
  {-# INLINE line #-}

-- | A step that is an arrow of its own, as a branch of a choice is: it takes the record that its input is made from.
instance (Annotate arr, arr' ~ arr, x' ~ x, o' ~ o) => Lined (arr' x' o') arr x o where
  line = flowStep Nothing
  {-# INLINE line #-}

-- | The flow as an arrow: it takes the record that its names are read from, and gives that record with the outputs of
-- the steps that have names in front of it.
flow :: (Category arr, Annotate arr) => Flow arr x y -> arr x y
flow (Flow p) = flowScope Nothing (p id)
{-# INLINE flow #-}

-- | The flow as an arrow that gives what its step of the name gave.
result :: forall name -> forall arr x y a. (Arrow arr, Annotate arr, KnownSymbol name, HasField name y a) => Flow arr x y -> arr x a
result name (Flow p) = flowScope (Just (symbolVal (Proxy @name))) (p id) >>^ getField @name
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
  forall k.
  forall (bindings :: k) ->
  (x ~ FlowIn t, Lined t arr x o, Describe bindings, Bind bindings x i) =>
  t
(-<) p bindings = line (describe @bindings) (Arrow.arr (bind @bindings @x) >>> p)
{-# INLINE (-<) #-}

infixr 2 <:

-- | The line, whose output is kept in the record of the flow under the name, or taken apart into the names of a list, in
-- front of the rest. The flow doesn't have the names already.
(<:) ::
  forall arr x o how y k.
  forall (out :: k) ->
  (Arrow arr, Annotate arr, Outs out x o y, Closing how y) =>
  Line arr x o how ->
  Flow arr x y
(<:) out bound = case bound of
  Line {} -> outcome @out bound
  Held {} -> unwritten "a line that is released"
  Inside {} -> unwritten "inside"
{-# INLINE (<:) #-}

-- | What a line that is not a plain step needs of the record of the flow after it: a release reads that record, and the exit,
-- which the flow doesn't have already.
type family Closing (how :: How) (y :: Type) :: Constraint where
  Closing 'Plainly y = ()
  Closing ('Releasing w) y = (w ~ ("exit" := Exit, y), Fresh "exit" y)
  Closing 'Wrapped y = ()

-- | What a step gives the flow under what is on the left of '<:': a name keeps the whole output, and a list of names
-- takes it apart.
class Outs out x o y | out x o -> y where
  -- | The flow that runs the step, with the names of what it gave in front of the record.
  outcome :: (Arrow arr, Annotate arr) => Line arr x o 'Plainly -> Flow arr x y

-- | A name, or a name that is kept and taken apart: the output is kept as it is, and its parts too if the pattern has
-- them.
instance
  ( NoField (Parse out),
    Described (Parse out),
    Distinct (Given '[Parse out]),
    AllFresh (Given '[Parse out]) x,
    Take Root (Parse out) o x y
  ) =>
  Outs (out :: Symbol) x o y
  where
  outcome (Line texts p) =
    Flow
      ( \rest ->
          (alongside (flowStep (Just (describeEntry @(Parse out))) texts p) >>^ \(o, x) -> takeEntry @Root @(Parse out) o x) >>> rest
      )
  {-# INLINE outcome #-}

-- | A list of patterns: the output is taken apart, and each name is a part of it.
instance
  ( Level (ShapeOf o) Root (Parses entries) o x y,
    DescribedList (Parses entries),
    Distinct (Given (Parses entries)),
    AllFresh (Given (Parses entries)) x
  ) =>
  Outs (entries :: [Symbol]) x o y
  where
  outcome (Line texts p) =
    Flow
      ( \rest ->
          ( alongside (flowStep (Just (Part Nothing Nothing (Just (listOf @(Parses entries))))) texts p)
              >>^ \(o, x) -> level @(ShapeOf o) @Root @(Parses entries) o x
          )
            >>> rest
      )
  {-# INLINE outcome #-}

-- | The pattern on the left of '<:' is not for a field: it is the output of the step that is taken.
type family NoField (entry :: Entry) :: Constraint where
  NoField ('Entry 'Nothing name parts) = ()
  NoField ('Entry ('Just field) name parts) =
    TypeError ('Text "The output of a step is not a field: " ':<>: 'ShowType field ':<>: 'Text " =: has nothing to take a field of")

infix 6 `releasedBy`

-- | What an acquire and its release can be: a line that '<:' names, or a line of the flow that keeps nothing.
class Released t arr x r w where
  -- | The step that acquires, which the rest of the flow follows, and the step that releases it however that ends, which
  -- reads the record of the flow after the line, and the exit.
  releasedBy :: Line arr x r 'Plainly -> Line arr w () 'Plainly -> t

-- | A line that '<:' names: the acquire, and the release, which reads what the record has when the line is done.
instance (Scope arr, arr' ~ arr, x' ~ x, r' ~ r, how ~ 'Releasing w) => Released (Line arr' x' r' how) arr x r w where
  releasedBy _ _ = Held [] (unwritten "releasedBy") [] (unwritten "releasedBy")

-- | A line that keeps nothing: the release reads the record of the flow, and the exit.
instance
  (Arrow arr, Annotate arr, Scope arr, arr' ~ arr, x' ~ x, y ~ x, w ~ ("exit" := Exit, x), Fresh "exit" x) =>
  Released (Flow arr' x' y) arr x r w
  where
  releasedBy acquire release = (<:) "_" (releasedBy acquire release :: Line arr x r ('Releasing w))
  {-# INLINE releasedBy #-}

-- | A line that is a release only, for what the flow has already: the rest of the flow is its body, and the release reads
-- the record of the flow, and the exit.
onExit :: (Arrow arr, Annotate arr, Scope arr, w ~ ("exit" := Exit, x), Fresh "exit" x) => Line arr w () 'Plainly -> Flow arr x x
onExit = releasedBy (Line [] (Arrow.arr (const ())))
{-# INLINE onExit #-}

-- | A line that has a combinator for a bracket take the rest of the flow as its body: the bindings make the input of the
-- wrapper, and the rest of the flow sees what the wrapper gives the body under the names of the line.
inside ::
  forall arr x i o.
  (forall z. arr (o, x) z -> arr (i, x) z) ->
  forall k.
  forall (bindings :: k) ->
  (Describe bindings, Bind bindings x i) =>
  Line arr x o 'Wrapped
inside _ _ = Inside [] (unwritten "inside") (unwritten "inside")

-- | A line that has a combinator that wraps a step take the rest of the flow as its body, when the combinator gives the body
-- what it takes, as @shield@ does.
inside_ :: (forall z. arr x z -> arr x z) -> Flow arr x x
inside_ _ = unwritten "inside_"

-- | What the stubs of the lines that release and wrap fail with, until the code is there.
unwritten :: String -> a
unwritten operation = throw (NotImplemented operation)

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
class Describe bindings where
  describe :: [Binding]

instance Describe ('[] :: [k]) where
  describe = []
  {-# INLINE describe #-}

instance
  (KnownSymbol (Port binding), Names (Path (Source binding)), Describe bindings) =>
  Describe ((binding :: Symbol) ': bindings)
  where
  describe = Binding (symbolVal (Proxy @(Port binding))) (names @(Path (Source binding))) : describe @bindings
  {-# INLINE describe #-}

-- | A name has the binding that it has in a list of that name.
instance (Describe '[name]) => Describe (name :: Symbol) where
  describe = describe @'[name]
  {-# INLINE describe #-}

-- | Each name of a tuple has the binding that it has in a list.
instance (Describe '[first, second]) => Describe ('(first, second) :: (Symbol, Symbol)) where
  describe = describe @'[first, second]
  {-# INLINE describe #-}

instance (Describe '[first, second, third]) => Describe ('(first, second, third) :: (Symbol, Symbol, Symbol)) where
  describe = describe @'[first, second, third]
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

-- | The input of a step made from the bindings, by the record of the flow that the bindings are read from: the bindings
-- are a name, a list of bindings or a tuple of names.
class Bind bindings x i | bindings x -> i where
  bind :: x -> i

-- | A step that has no bindings takes nothing.
instance Bind ('[] :: [k]) x () where
  bind _ = ()
  {-# INLINE bind #-}

-- | A list of bindings: the step takes a record that has a field for each.
instance
  (Unique (Ports (binding ': bindings)), Plain (binding ': bindings), Record (binding ': bindings) x r) =>
  Bind ((binding :: Symbol) ': bindings) x (Inputs r)
  where
  bind = Inputs . record @(binding ': bindings)
  {-# INLINE bind #-}

-- | A name: the step takes the value that the name has in the flow, whole.
instance (Plain '[name], NoPort (Break '=' name), Fetch (Path name) x i) => Bind (name :: Symbol) x i where
  bind = fetch @(Path name)
  {-# INLINE bind #-}

-- | A tuple of names: the step takes the pair of their values, and pairs that nest to the right for three.
instance (Bind first x a, Bind second x b) => Bind ('(first, second) :: (Symbol, Symbol)) x (a, b) where
  bind x = (bind @first x, bind @second x)
  {-# INLINE bind #-}

instance
  (Bind first x a, Bind second x b, Bind third x c) =>
  Bind ('(first, second, third) :: (Symbol, Symbol, Symbol)) x (a, (b, c))
  where
  bind x = (bind @first x, (bind @second x, bind @third x))
  {-# INLINE bind #-}

-- | What is said of a name that has a port, which a step that takes its input whole has none of.
type family NoPort (split :: (Symbol, Maybe Symbol)) :: Constraint where
  NoPort '(before, 'Nothing) = ()
  NoPort '(before, 'Just after) =
    TypeError
      ( 'Text "A step that takes its input whole has no port: "
          ':<>: 'ShowType before
          ':<>: 'Text " =: "
          ':<>: 'ShowType after
          ':<>: 'Text " binds a port, which a list of bindings has"
      )

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
