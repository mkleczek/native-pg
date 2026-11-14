-- | The annotations of a program for the diagrams that "Network.Streaming.Diagram" draws of it and the lint that
-- "Network.Streaming.Lint" makes of it: a title for a box, names for the wires, the ends of exchanges, and the checks of
-- the lint that a part allows. They are in a module of their own so that "Network.Streaming.Plumbing", which the classes
-- use, can use them. Every arrow but the diagram returns the arrow it is given, so that an annotation costs nothing when
-- the program runs.
module Network.Streaming.Annotate
  ( Label (..),
    Side (..),
    Check (..),
    Annotate (..),
    box,
    taking,
    giving,
    asking,
    answering,
  )
where

import GHC.Stack (HasCallStack)

-- | A box's title and a sentence about what it does, for diagrams.
data Label = Label {labelTitle :: String, labelComment :: String}

-- | Which end of an exchange an arrow is.
data Side = Asking | Answering
  deriving (Eq)

-- | What the lint of "Network.Streaming.Lint" looks for in the structure of a program.
data Check
  = -- | The acquire of a scope can wait, and a cancellation there skips its release.
    WaitInAcquire
  | -- | A loop has a way round that passes no wait, so that on one core it never yields.
    LoopWithoutWait
  | -- | A deadline or a race is over a part that can't wait, so the deadline can never fire, and the race is decided at once.
    DeadlineWithoutWait
  | -- | A shield is around a wait, which holds the cancellation off for as long as the wait lasts.
    ShieldAroundWait
  | -- | An inbox has posts and no collects anywhere in the program, or collects and no posts: nothing pairs by its name.
    DeadInbox
  | -- | The same name is opened at two sites of the program, so that its posts and collects can't be told apart.
    InboxOpenedTwice
  deriving (Eq, Show, Enum, Bounded)

-- | Annotations for the diagrams that "Network.Streaming.Diagram" draws of a program, and for its lint. Every other arrow
-- returns the arrow it is given, so that an annotation costs nothing when the program runs.
class Annotate arr where
  -- | The arrow as a box of its own in diagrams. Every runtime returns the arrow unchanged.
  named :: (HasCallStack) => Label -> arr a b -> arr a b
  named _ step = step
  {-# INLINE named #-}

  -- | The arrow, with names on its input and output wires in diagrams. Every runtime returns the arrow unchanged.
  wires :: Maybe String -> Maybe String -> arr a b -> arr a b
  wires _ _ step = step
  {-# INLINE wires #-}

  -- | The arrow as one end of an exchange of the named value, for diagrams. Every runtime returns the arrow unchanged.
  exchange :: Side -> String -> arr a b -> arr a b
  exchange _ _ step = step
  {-# INLINE exchange #-}

  -- | The arrow, where the lint doesn't report the check, for the reason given: the part means what the check finds.
  -- Every runtime returns the arrow unchanged.
  allowing :: Check -> String -> arr a b -> arr a b
  allowing _ _ step = step
  {-# INLINE allowing #-}

-- | The arrow as a box in diagrams, named by its title alone.
box :: (HasCallStack, Annotate arr) => String -> arr a b -> arr a b
box title = named (Label title "")
{-# INLINE box #-}

-- | The arrow, with a name for its input wire in diagrams.
taking :: (Annotate arr) => String -> arr a b -> arr a b
taking input = wires (Just input) Nothing
{-# INLINE taking #-}

-- | The arrow, with a name for its output wire in diagrams.
giving :: (Annotate arr) => String -> arr a b -> arr a b
giving output = wires Nothing (Just output)
{-# INLINE giving #-}

-- | The arrow as the asking end of an exchange of the named value in diagrams: the end that makes a ticket and waits.
asking :: (Annotate arr) => String -> arr a b -> arr a b
asking = exchange Asking
{-# INLINE asking #-}

-- | The arrow as the answering end of an exchange of the named value in diagrams: the end that fulfils the promise.
answering :: (Annotate arr) => String -> arr a b -> arr a b
answering = exchange Answering
{-# INLINE answering #-}
