{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | The records whose fields have names in their types, and the patterns that take a value apart into names. They are one
-- module because "Network.Streaming.Named", whose branches take the value of a case apart, and
-- "Network.Streaming.Wiring", whose lines take the output of a step apart, have the same patterns.
--
-- A pattern is a name, which keeps the value as it is, or a list of patterns, which takes it apart. It is written
-- with two operators, which build the text that the library reads back:
--
-- * @"name" \@: [patterns]@ keeps the value under @name@ and takes it apart. @"_" \@: [patterns]@ keeps nothing whole.
-- * @"field" =: pattern@ is the part of a record that has the field, taken by the pattern, which is a name, a list of
--   patterns that takes the part apart, or @name \@: [patterns]@, which does both. A choice's branch has the case where
--   a record has the field.
--
-- A record is taken apart by the names of its fields, and a pair, or pairs that nest to the right, by position, in
-- which the last pattern takes what is left whole. A record is told from a pair by its first part, which is a field.
module Network.Streaming.Internal.Patterns
  ( -- * Fields
    (:=) (..),

    -- * Writing patterns
    type (=:),
    type (@:),

    -- * Names in a record
    Where (..),
    Dig (..),
    Onward (..),
    Fresh,

    -- * Reading patterns
    Entry (..),
    Parse,
    Parses,
    Site (..),
    Root,
    Shape (..),
    ShapeOf,
    Level (..),
    Take (..),
    Given,
    Distinct,
    AllFresh,
    CaseOf,
    Branch (..),
    (?>),
  )
where

import Control.Arrow (Arrow, (^>>))
import Data.Kind (Constraint, Type)
import Data.Proxy (Proxy (..))
import GHC.Records (HasField (..))
import GHC.TypeLits
  ( AppendSymbol,
    ConsSymbol,
    ErrorMessage (..),
    KnownSymbol,
    Symbol,
    TypeError,
    UnconsSymbol,
    symbolVal,
  )
import Network.Streaming.Annotate (Annotate, (|-))

-- * Fields

-- | A value with its name in its type: one field of an anonymous record.
newtype (name :: Symbol) := a = Is a

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

-- * Writing patterns

-- | The text of a list of patterns: the texts of the patterns, in parentheses, separated by commas.
type family Parts (patterns :: [Symbol]) :: Symbol where
  Parts '[] = "()"
  Parts (pattern ': patterns) = AppendSymbol "(" (AppendSymbol pattern (PartsAfter patterns))

-- | The text of the patterns of a list after its first, which ends the parentheses.
type family PartsAfter (patterns :: [Symbol]) :: Symbol where
  PartsAfter '[] = ")"
  PartsAfter (pattern ': patterns) = AppendSymbol "," (AppendSymbol pattern (PartsAfter patterns))

-- | The part of a record that has the field, or the case of a choice, taken by the pattern: a name or a pattern that is
-- written with '@:' is the text after the @=@, and a list is the text of its patterns. In bindings, which are on the right
-- of '<:' and have no patterns, it is the port and the name in the flow.
type (=:) :: forall k. Symbol -> k -> Symbol
type family field =: rhs where
  field =: (name :: Symbol) = AppendSymbol field (AppendSymbol "=" name)
  field =: (patterns :: [Symbol]) = AppendSymbol field (AppendSymbol "=" (Parts patterns))

-- | The value kept under the name and taken apart by the patterns, as @name\@(patterns)@ is written.
type (@:) :: Symbol -> [Symbol] -> Symbol
type family name @: patterns where
  name @: patterns = AppendSymbol name (AppendSymbol "@" (Parts patterns))

infixr 9 @:

infixr 8 =:

-- * Reading patterns

-- | A pattern as it is read from its text: the field or the case that it is for, if it has one, the name that the whole
-- is kept under, which is @"_"@ if it is not kept, and the patterns that the whole is taken apart by, if it is.
data Entry = Entry (Maybe Symbol) Symbol (Maybe [Entry])

-- | Whether a character has a meaning in the text of a pattern, and so ends a name.
type family Delimiter (c :: Char) :: Bool where
  Delimiter '=' = 'True
  Delimiter '@' = 'True
  Delimiter '(' = 'True
  Delimiter ')' = 'True
  Delimiter ',' = 'True
  Delimiter c = 'False

-- | The name at the start of a text, which ends at a character that has a meaning, and the text that is left, which
-- starts with that character.
type family NameFrom (before :: Symbol) (rest :: Maybe (Char, Symbol)) :: (Symbol, Symbol) where
  NameFrom before 'Nothing = '(before, "")
  NameFrom before ('Just '(c, more)) = NameAt (Delimiter c) before c more

-- | The name, from a character that ends it or goes on with it.
type family NameAt (ends :: Bool) (before :: Symbol) (c :: Char) (more :: Symbol) :: (Symbol, Symbol) where
  NameAt 'True before c more = '(before, ConsSymbol c more)
  NameAt 'False before c more = NameFrom (AppendSymbol before (ConsSymbol c "")) (UnconsSymbol more)

-- | What is said of a text that is not the text of a pattern.
type Unreadable text = 'Text "The pattern " ':<>: 'ShowType text ':<>: 'Text " can't be read"

-- | The pattern at the start of a text, and the text that is left.
type family EntryFrom (text :: Symbol) :: (Entry, Symbol) where
  EntryFrom text = EntryAfter text (NameFrom "" (UnconsSymbol text))

-- | The pattern, from the name that it starts with.
type family EntryAfter (text :: Symbol) (named :: (Symbol, Symbol)) :: (Entry, Symbol) where
  EntryAfter text '(name, rest) = EntryAt text name (UnconsSymbol rest)

-- | The pattern, from the character after its name.
type family EntryAt (text :: Symbol) (name :: Symbol) (rest :: Maybe (Char, Symbol)) :: (Entry, Symbol) where
  EntryAt text name 'Nothing = '( 'Entry 'Nothing name 'Nothing, "")
  EntryAt text name ('Just '( '=', more)) = ForField name (RhsFrom more)
  EntryAt text name ('Just '( '@', more)) = KeptAs text name (UnconsSymbol more)
  EntryAt text name ('Just '(c, more)) = '( 'Entry 'Nothing name 'Nothing, ConsSymbol c more)

-- | The pattern that a field or a case is taken by.
type family ForField (field :: Symbol) (rhs :: (Entry, Symbol)) :: (Entry, Symbol) where
  ForField field '( 'Entry 'Nothing name parts, rest) = '( 'Entry ('Just field) name parts, rest)
  ForField field '( 'Entry ('Just other) name parts, rest) = TypeError ('Text "A pattern for the field " ':<>: 'ShowType field ':<>: 'Text " has a field of its own, " ':<>: 'ShowType other)

-- | What is on the right of a @=@: a list of patterns, or a pattern with no field of its own.
type family RhsFrom (text :: Symbol) :: (Entry, Symbol) where
  RhsFrom text = RhsAt text (UnconsSymbol text)

-- | The right of a @=@, from its first character.
type family RhsAt (text :: Symbol) (first :: Maybe (Char, Symbol)) :: (Entry, Symbol) where
  RhsAt text ('Just '( '(', more)) = TakenApart (ListFrom more)
  RhsAt text first = EntryFrom text

-- | The pattern that keeps nothing whole and takes a value apart.
type family TakenApart (parts :: ([Entry], Symbol)) :: (Entry, Symbol) where
  TakenApart '(entries, rest) = '( 'Entry 'Nothing "_" ('Just entries), rest)

-- | The pattern that keeps a value under a name and takes it apart, from the character after its @\@@.
type family KeptAs (text :: Symbol) (name :: Symbol) (after :: Maybe (Char, Symbol)) :: (Entry, Symbol) where
  KeptAs text name ('Just '( '(', more)) = KeepsParts name (ListFrom more)
  KeptAs text name after = TypeError (Unreadable text)

-- | The pattern, from the patterns that it takes the value apart by.
type family KeepsParts (name :: Symbol) (parts :: ([Entry], Symbol)) :: (Entry, Symbol) where
  KeepsParts name '(entries, rest) = '( 'Entry 'Nothing name ('Just entries), rest)

-- | The patterns of a list in parentheses, whose opening has been read, and the text that is left after its closing.
type family ListFrom (text :: Symbol) :: ([Entry], Symbol) where
  ListFrom text = ListAt text (UnconsSymbol text)

-- | The list, from its first character: it is empty if that closes it.
type family ListAt (text :: Symbol) (first :: Maybe (Char, Symbol)) :: ([Entry], Symbol) where
  ListAt text ('Just '( ')', more)) = '( '[], more)
  ListAt text first = ListAfter text (EntryFrom text)

-- | The list, from its first pattern.
type family ListAfter (text :: Symbol) (parsed :: (Entry, Symbol)) :: ([Entry], Symbol) where
  ListAfter text '(entry, rest) = ListSeparated text entry (UnconsSymbol rest)

-- | The list, from the character after its first pattern: a comma goes on to the next, and a parenthesis closes it.
type family ListSeparated (text :: Symbol) (entry :: Entry) (rest :: Maybe (Char, Symbol)) :: ([Entry], Symbol) where
  ListSeparated text entry ('Just '( ',', more)) = Prepended entry (ListFrom more)
  ListSeparated text entry ('Just '( ')', more)) = '( '[entry], more)
  ListSeparated text entry rest = TypeError (Unreadable text)

-- | The list with a pattern in front.
type family Prepended (entry :: Entry) (list :: ([Entry], Symbol)) :: ([Entry], Symbol) where
  Prepended entry '(entries, rest) = '(entry ': entries, rest)

-- | The pattern that a text is.
type family Parse (text :: Symbol) :: Entry where
  Parse text = WholeEntry text (EntryFrom text)

-- | The pattern, if the text has nothing left after it.
type family WholeEntry (text :: Symbol) (parsed :: (Entry, Symbol)) :: Entry where
  WholeEntry text '(entry, "") = entry
  WholeEntry text '(entry, rest) = TypeError (Unreadable text)

-- | The patterns that the texts of a list are.
type family Parses (texts :: [Symbol]) :: [Entry] where
  Parses '[] = '[]
  Parses (text ': texts) = Parse text ': Parses texts

-- * Names

-- | The names in the flow that the patterns give, in order, followed by the names: a name comes before the names of the
-- parts that it is taken apart into, and @"_"@ is no name.
type family GivenBefore (entries :: [Entry]) (more :: [Symbol]) :: [Symbol] where
  GivenBefore '[] more = more
  GivenBefore ('Entry field name parts ': entries) more = Keep name (GivenInside parts (GivenBefore entries more))

-- | The names of the parts that a value is taken apart into, if it is, before the names.
type family GivenInside (parts :: Maybe [Entry]) (more :: [Symbol]) :: [Symbol] where
  GivenInside 'Nothing more = more
  GivenInside ('Just entries) more = GivenBefore entries more

-- | The names that the patterns give.
type Given entries = GivenBefore entries '[]

-- | The name in front of the others, unless it is @"_"@.
type family Keep (name :: Symbol) (more :: [Symbol]) :: [Symbol] where
  Keep "_" more = more
  Keep name more = name ': more

-- | What is said of a name that a pattern has twice.
type Twice name = 'Text "The pattern has the name " ':<>: 'ShowType name ':<>: 'Text " twice"

-- | No name is twice in the list.
type family Distinct (names :: [Symbol]) :: Constraint where
  Distinct '[] = ()
  Distinct (name ': rest) = (NotTwice name rest, Distinct rest)

-- | The name is not one of the names.
type family NotTwice (name :: Symbol) (names :: [Symbol]) :: Constraint where
  NotTwice name '[] = ()
  NotTwice name (name ': rest) = TypeError (Twice name)
  NotTwice name (other ': rest) = NotTwice name rest

-- | The flow doesn't have any of the names.
type family AllFresh (names :: [Symbol]) (r :: Type) :: Constraint where
  AllFresh '[] r = ()
  AllFresh (name ': rest) r = (Fresh name r, AllFresh rest r)

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

-- | Where a name is looked for, which says what is said when it is not there.
data Where
  = -- | In the record of the flow.
    InFlow
  | -- | In the output of the step that has the name.
    InOutput Symbol
  | -- | Among the ports of the input of a step.
    Unbound
  | -- | Among the fields of a value that a pattern takes apart.
    InPattern Site

-- | Where in a pattern a value is taken apart, for what is said of it: what the value is called, and the way to it from
-- the whole that the pattern started at, as @"socket.fd"@, which is empty at the start.
data Site = Site Symbol Symbol

-- | The start of a pattern for the output of a step.
type Root = 'Site "output of the step" ""

-- | What is said of a name that isn't there.
type family Absence (place :: Where) (name :: Symbol) :: ErrorMessage where
  Absence 'InFlow name = 'Text "The flow has no name " ':<>: 'ShowType name
  Absence ('InOutput step) name =
    'Text "The step " ':<>: 'ShowType step ':<>: 'Text " has no output " ':<>: 'ShowType name
  Absence 'Unbound name = 'Text "Input " ':<>: 'ShowType name ':<>: 'Text " of the step isn't bound"
  Absence ('InPattern site) name = Here site ':<>: 'Text "The " ':<>: Called site ':<>: 'Text " has no field " ':<>: 'ShowType name

-- | Where in a pattern the error is, if it is not at its start.
type family Here (site :: Site) :: ErrorMessage where
  Here ('Site called "") = 'Text ""
  Here ('Site called at) = 'Text "in " ':<>: 'ShowType at ':<>: 'Text ": "

-- | What the value is called.
type family Called (site :: Site) :: ErrorMessage where
  Called ('Site called at) = 'Text called

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

-- * Taking a value apart

-- | How a value is taken apart by a list of patterns.
data Shape
  = -- | By the names of its fields: a record, which is a pair of fields that nests to the right.
    Fields
  | -- | By position: a pair, or pairs that nest to the right.
    Positions
  | -- | Not at all.
    Opaque

-- | How the value is taken apart: a record is told from a pair by its first part, which is a field.
type family ShapeOf (o :: Type) :: Shape where
  ShapeOf (name := a, rest) = 'Fields
  ShapeOf (name := a) = 'Fields
  ShapeOf (a, b) = 'Positions
  ShapeOf o = 'Opaque

-- | Whether a value that is the rest of a pair is a pair that goes on.
type family Continues (o :: Type) :: Bool where
  Continues o = ContinuesAs (ShapeOf o)

-- | Whether a value that is taken apart by position goes on.
type family ContinuesAs (shape :: Shape) :: Bool where
  ContinuesAs 'Positions = 'True
  ContinuesAs shape = 'False

-- | The way to a part from the site that it is taken apart at: the field or case that it is for, or else its name.
type family Deeper (site :: Site) (entry :: Entry) :: Site where
  Deeper ('Site called at) ('Entry field name parts) = 'Site called (Along at (LabelOf field name))

-- | The label of a part: its field or case if it has one, and else its name.
type family LabelOf (field :: Maybe Symbol) (name :: Symbol) :: Symbol where
  LabelOf ('Just field) name = field
  LabelOf 'Nothing name = name

-- | The way to a part, which goes on from the way to the whole.
type family Along (at :: Symbol) (label :: Symbol) :: Symbol where
  Along "" label = label
  Along at label = AppendSymbol at (AppendSymbol "." label)

-- | What is said of a value that a list of patterns can't take apart.
type Whole site =
  Here site
    ':<>: 'Text "The "
    ':<>: Called site
    ':<>: 'Text " is neither a record nor a pair, so a list of names can't take it apart"

-- | What is said of a list that has more patterns than the parts of a pair.
type TooMany site =
  Here site ':<>: 'Text "The pattern has too many names for the " ':<>: Called site

-- | What is said of a field that is named for a pair, which has none.
type NotFields site field name =
  Here site
    ':<>: 'Text "The "
    ':<>: Called site
    ':<>: 'Text " is a pair, taken apart by position: "
    ':<>: 'ShowType field
    ':<>: 'Text " =: "
    ':<>: 'ShowType name
    ':<>: 'Text " names a field, and a pair has none"

-- | A pattern for a pair is a name, and not a field and a name.
type family ByPosition (site :: Site) (entry :: Entry) :: Constraint where
  ByPosition site ('Entry 'Nothing name parts) = ()
  ByPosition site ('Entry ('Just field) name parts) = TypeError (NotFields site field name)

-- | The field of a record that a pattern is for: the field that it names, and else its own name.
type family FieldOf (entry :: Entry) :: Symbol where
  FieldOf ('Entry ('Just field) name parts) = field
  FieldOf ('Entry 'Nothing name parts) = name

-- | The record has a field of the name, which is an error that is reported on its own: a pattern that takes a field apart may
-- not use it, so that the error of 'Dig', which is the type of the field, would not be seen.
type family Present (site :: Site) (name :: Symbol) (o :: Type) :: Constraint where
  Present site name (name := a, rest) = ()
  Present site name (name := a) = ()
  Present site name (other := a, rest) = Present site name rest
  Present site name (other := a) = TypeError (Absence ('InPattern site) name)

-- | A list of patterns taken from a value of a shape: the names that they give, in front of the record of the flow, in
-- the order of the list.
class Level (shape :: Shape) (site :: Site) (entries :: [Entry]) (o :: Type) (x :: Type) (y :: Type) | shape site entries o x -> y where
  -- | The record of the flow after the value is taken apart: the names in front of the record that it had.
  level :: o -> x -> y

-- | A record: each pattern is for a field of it, and the fields that the list doesn't have are not kept.
instance Level 'Fields site '[] o x x where
  level _ x = x
  {-# INLINE level #-}

instance
  ( Present site (FieldOf entry) o,
    Dig ('InPattern site) (FieldOf entry) '[] o v,
    Level 'Fields site entries o x rest,
    Take site entry v rest y
  ) =>
  Level 'Fields site (entry ': entries) o x y
  where
  level o x = takeEntry @site @entry (dig @('InPattern site) @(FieldOf entry) @'[] o) (level @'Fields @site @entries o x)
  {-# INLINE level #-}

-- | A pair, or pairs that nest to the right: each pattern is for a part of it.
instance (Spread 'True site entries o x y) => Level 'Positions site entries o x y where
  level = spread @'True @site @entries
  {-# INLINE level #-}

instance (TypeError (Whole site), y ~ x) => Level 'Opaque site entries o x y where
  level _ _ = error "Network.Streaming.Wiring: a list of names for an output that has no parts is a type error"

-- | The record of the flow after a value is taken apart by position: a value that is a pair, when the first of its
-- parts is the first of the patterns, and the rest of the value is the rest of them. The last pattern is the rest of the
-- value, whole.
class Spread (pair :: Bool) (site :: Site) (entries :: [Entry]) (o :: Type) (x :: Type) (y :: Type) | pair site entries o x -> y where
  -- | The record of the flow after the value: the names in front of the record that it had.
  spread :: o -> x -> y

instance Spread pair site '[] o x x where
  spread _ x = x
  {-# INLINE spread #-}

-- | The last pattern is for the last part, which is a value that goes on no further, or for what is left of a pair, whole.
instance (ByPosition site entry, Take site entry o x y) => Spread pair site '[entry] o x y where
  spread = takeEntry @site @entry
  {-# INLINE spread #-}

-- | More patterns than parts.
instance (TypeError (TooMany site), y ~ x) => Spread 'False site (entry ': next ': entries) o x y where
  spread _ _ = error "Network.Streaming.Wiring: a list that has too many names is a type error"

instance
  (ByPosition site entry, Spread (Continues b) site (next ': entries) b x rest, Take site entry a rest y) =>
  Spread 'True site (entry ': next ': entries) (a, b) x y
  where
  spread (a, b) x = takeEntry @site @entry a (spread @(Continues b) @site @(next ': entries) b x)
  {-# INLINE spread #-}

-- | The names that one pattern gives for a value, in front of the record of the flow: the name that keeps the value
-- whole, if it has one, and then the names of its parts, if it takes it apart.
class Take (site :: Site) (entry :: Entry) (v :: Type) (rest :: Type) (y :: Type) | site entry v rest -> y where
  -- | The record of the flow with the names of the pattern in front of it.
  takeEntry :: v -> rest -> y

-- | A pattern that keeps the value under a name, or drops it, and takes it apart no further.
instance (Place name v rest y) => Take site ('Entry field name 'Nothing) v rest y where
  takeEntry = putFirst @name
  {-# INLINE takeEntry #-}

-- | A pattern that takes the value apart by a list of patterns, and keeps it whole too if it has a name.
instance
  (Level (ShapeOf v) (Deeper site ('Entry field name ('Just entries))) entries v rest inner, Place name v inner y) =>
  Take site ('Entry field name ('Just entries)) v rest y
  where
  takeEntry v rest = putFirst @name v (level @(ShapeOf v) @(Deeper site ('Entry field name ('Just entries))) @entries v rest)
  {-# INLINE takeEntry #-}

-- | The name of a part, in front of the record of the flow: unless it is @"_"@, which keeps nothing.
class Place (name :: Symbol) (a :: Type) (rest :: Type) (y :: Type) | name a rest -> y where
  putFirst :: a -> rest -> y

instance {-# OVERLAPPING #-} Place "_" a rest rest where
  putFirst _ rest = rest
  {-# INLINE putFirst #-}

instance (y ~ (name := a, rest)) => Place name a rest y where
  putFirst a rest = (Is a, rest)
  {-# INLINE putFirst #-}

-- * Branches

-- | The case that a branch is for: the case that its pattern is for, and else its name.
type family CaseOf (label :: Symbol) :: Symbol where
  CaseOf label = CaseNamed (Parse label)

-- | The case of a pattern.
type family CaseNamed (entry :: Entry) :: Symbol where
  CaseNamed ('Entry ('Just case_) name parts) = case_
  CaseNamed ('Entry 'Nothing name 'Nothing) = name
  CaseNamed ('Entry 'Nothing name ('Just parts)) =
    TypeError ('Text "A branch is for a case, which is written with =: before its pattern, and not with @: alone")

-- | The record that a branch sees for the value of its case, and the record of the choice: nothing from the case if the
-- label is a case alone, and else the names of the pattern in front of the record.
class Branch (label :: Symbol) (v :: Type) (r :: Type) (y :: Type) | label v r -> y where
  -- | The record that the branch sees.
  branch :: v -> r -> y

instance (Selected (Parse label) v r y) => Branch label v r y where
  branch = selected @(Parse label)
  {-# INLINE branch #-}

-- | What a branch sees, from the pattern of its label.
class Selected (entry :: Entry) (v :: Type) (r :: Type) (y :: Type) | entry v r -> y where
  selected :: v -> r -> y

-- | A case alone: the value of the case is not in the record.
instance Selected ('Entry 'Nothing name 'Nothing) v r r where
  selected _ r = r
  {-# INLINE selected #-}

-- | A case and a pattern: the names of the pattern join the record, in front of it.
instance
  ( Distinct (Given '[ 'Entry ('Just case_) name parts]),
    AllFresh (Given '[ 'Entry ('Just case_) name parts]) r,
    Take ('Site "value of the case" "") ('Entry ('Just case_) name parts) v r y
  ) =>
  Selected ('Entry ('Just case_) name parts) v r y
  where
  selected = takeEntry @('Site "value of the case" "") @('Entry ('Just case_) name parts)
  {-# INLINE selected #-}

infixr 6 ?>

-- | The branch for the case that the label is for. It takes the value of its case apart by the pattern of the label, if it
-- has one, and its names join the record, in front of it. The wire into it has the name of the case in diagrams. A chain of
-- them takes apart what 'Network.Streaming.Named.cases' gives.
(?>) ::
  forall (label :: Symbol) ->
  forall arr v r y b.
  (Arrow arr, Annotate arr, KnownSymbol (CaseOf label), Branch label v r y) =>
  arr y b ->
  arr (CaseOf label := (CaseOf label := v, r)) b
(?>) label p = (\(Is (Is v, r)) -> branch @label v r) ^>> (symbolVal (Proxy @(CaseOf label)) |- p)
{-# INLINE (?>) #-}
