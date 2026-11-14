{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | An arrow that runs nothing: a program built at 'Diagram' is a description of its own structure, which
-- "Network.Streaming.Diagram.Mermaid" draws as flowcharts. A step that is an action, a wait or a ticket is a leaf, and
-- the combinators of the program, the 'Control.Arrow' ones and the capabilities of the classes, are the nodes above it.
-- The annotations of 'Annotate' are nodes too.
--
-- Everything here is lazy in its arrows, because a loop is a tree that contains itself: @iterateWith@ and @foreverA@ tie
-- the knot, and a constructor or an instance method that looked at its arrows while it built the tree would never
-- return. The tree is not normalised when it is built: the renderer does that.
module Network.Streaming.Diagram
  ( Diagram (..),
    Tree (..),
    Shape (..),
    DiagramCore,
    unnamedAction,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..))
import Control.Category (Category (..))
import Data.Maybe (listToMaybe)
import GHC.Stack (CallStack, SrcLoc, callStack, getCallStack)
import Network.Streaming.Annotate (Annotate (..), Side)
import Network.Streaming.Classes
import Network.Streaming.Mode (Mode (..), ModeOf)
import Prelude hiding (id, (.))

-- | A program as a description of its structure: the input and the output are only types.
newtype Diagram a b = Diagram Tree

-- | What a program is made of.
data Tree
  = -- | A step that is only a function, which draws no node.
    Pure
  | -- | One step after the other.
    Seq Tree Tree
  | -- | A step on the first of a pair, which passes the second on.
    First Tree
  | -- | A step on the second of a pair, which passes the first on.
    Second Tree
  | -- | Two steps, each on a half of a pair: @***@.
    Par Tree Tree
  | -- | Two steps on the same input: @&&&@.
    Fanout Tree Tree
  | -- | Two steps on the same input, whose results are combined: @<*>@.
    Ap Tree Tree
  | -- | A step on a 'Left', which passes a 'Right' on.
    Left' Tree
  | -- | A step on a 'Right', which passes a 'Left' on.
    Right' Tree
  | -- | A step for each side of an 'Either': @+++@.
    Sum Tree Tree
  | -- | A step for each side of an 'Either', which ends in the same place: @|||@.
    Choice Tree Tree
  | -- | Parts that run at once.
    Together [Tree]
  | -- | A step that is one node: how it is called, and how it is drawn.
    Leaf String Shape
  | -- | A scope: what it acquires, what releases it, and its body.
    Scope Tree Tree Tree
  | -- | A body whose failure is a value.
    Try Tree
  | -- | A body that cancellations from outside don't reach.
    Shield Tree
  | -- | Two steps that run until the first ends.
    Race Tree Tree
  | -- | A body with a deadline.
    Within Duration Tree
  | -- | A box of its own: its label, the place in the code that named it, and its body.
    Named Label (Maybe SrcLoc) Tree
  | -- | A body with names for its input and output wires.
    Wires (Maybe String) (Maybe String) Tree
  | -- | One end of an exchange of the named value: which end, and the step that is it.
    End Side String Tree

-- | How a leaf is drawn.
data Shape
  = -- | An action that never waits.
    Action
  | -- | A wait for a ticket to be given its value.
    Wait
  | -- | A block until an event fires.
    Blocking
  | -- | A point where the part gives way to the others.
    Yielding
  | -- | A value given to another part.
    Handover
  | -- | A step that is none of those.
    Round

-- | The core of the tickets that are of the diagram's own reach. No part runs on it: nothing runs.
data DiagramCore

-- | What an action is called in a diagram until the program names it.
unnamedAction :: String
unnamedAction = "IO"

-- | A step that is a leaf.
leaf :: String -> Shape -> Diagram a b
leaf text shape = Diagram (Leaf text shape)

instance Category Diagram where
  id = Diagram Pure
  Diagram after . Diagram before = Diagram (Seq before after)

-- | Every combinator is defined, so that the tree has the structure that the program wrote: the defaults would define
-- most of them by 'first' and hide it.
instance Arrow Diagram where
  arr _ = Diagram Pure
  first (Diagram step) = Diagram (First step)
  second (Diagram step) = Diagram (Second step)
  Diagram one *** Diagram two = Diagram (Par one two)
  Diagram one &&& Diagram two = Diagram (Fanout one two)

instance ArrowChoice Diagram where
  left (Diagram step) = Diagram (Left' step)
  right (Diagram step) = Diagram (Right' step)
  Diagram one +++ Diagram two = Diagram (Sum one two)
  Diagram one ||| Diagram two = Diagram (Choice one two)

instance Functor (Diagram a) where
  fmap _ (Diagram step) = Diagram (Seq step Pure)

instance Applicative (Diagram a) where
  pure _ = Diagram Pure
  Diagram function <*> Diagram argument = Diagram (Ap function argument)

instance ArrowApplicative Diagram

instance Effect Diagram where
  now _ = leaf unnamedAction Action

instance Block Diagram where
  block = leaf "block" Blocking

instance GiveWay Diagram where
  giveWay = leaf "give way" Yielding

instance Time Diagram where
  clock = leaf "clock" Round

instance Scope Diagram where
  scope (Diagram acquire) (Diagram release) (Diagram body) = Diagram (Scope acquire release body)
  tryA (Diagram body) = Diagram (Try body)
  shield (Diagram body) = Diagram (Shield body)

instance Race Diagram where
  race (Diagram one) (Diagram two) = Diagram (Race one two)

instance Within Diagram where
  within duration (Diagram body) = Diagram (Within duration body)

instance Concurrent Diagram where
  together parts = Diagram (Together [part | Diagram part <- parts])

-- | The box records where the code asked for it: the outermost call of the stack, which is the one in the program, and
-- not the helpers that the box was called through.
instance Annotate Diagram where
  named label (Diagram body) = Diagram (Named label (outermost callStack) body)
  wires input output (Diagram body) = Diagram (Wires input output body)
  exchange side name (Diagram body) = Diagram (End side name body)

-- | The place of the outermost call in the stack.
outermost :: CallStack -> Maybe SrcLoc
outermost = fmap snd . listToMaybe . reverse . getCallStack

-- | The tickets of a diagram are never made: there are two instances, for the two reaches, so that a program that
-- uses either is built.
instance Exchange Diagram ('Here DiagramCore) where
  data Waiting Diagram ('Here DiagramCore) a
  ticket = leaf "ticket" Round
  await = leaf "await" Wait
  give = leaf "give" Handover
  abandon = leaf "abandon" Round

instance Exchange Diagram 'Anywhere where
  data Waiting Diagram 'Anywhere a
  ticket = leaf "ticket" Round
  await = leaf "await" Wait
  give = leaf "give" Handover
  abandon = leaf "abandon" Round

type instance Local Diagram = 'Here DiagramCore

-- | A diagram counts nothing.
type instance ModeOf Diagram = 'Release
