-- | The annotations of a program for the diagrams that "Network.Streaming.Diagram" draws of it: a title for a box,
-- names for the wires, and the ends of exchanges. They are in a module of their own so that
-- "Network.Streaming.Plumbing", which the classes use, can use them. Every arrow but the diagram returns the arrow it
-- is given, so that an annotation costs nothing when the program runs.
module Network.Streaming.Annotate
  ( Label (..),
    Side (..),
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

-- | Annotations for the diagrams that "Network.Streaming.Diagram" draws of a program. Every other arrow returns the arrow
-- it is given, so that an annotation costs nothing when the program runs.
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

-- | The arrow as the answering end of an exchange of the named value in diagrams: the end that gives the value.
answering :: (Annotate arr) => String -> arr a b -> arr a b
answering = exchange Answering
{-# INLINE answering #-}
