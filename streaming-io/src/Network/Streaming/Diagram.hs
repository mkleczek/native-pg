{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | An arrow that runs nothing: a program built at 'Diagram' is a description of its own structure, which
-- "Network.Streaming.Diagram.Mermaid" draws as flowcharts. A step that is an action, a wait or a ticket is a leaf, and
-- the combinators of the program, the 'Control.Arrow' ones and the capabilities of the classes, are the nodes above it.
-- The annotations of 'Annotate' are nodes too. The tree and the walks over it are in "Network.Streaming.Structure",
-- which the lint of "Network.Streaming.Lint" uses as well.
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
import Data.Proxy (Proxy (..))
import GHC.Stack (CallStack, SrcLoc, callStack, getCallStack)
import GHC.TypeLits (KnownSymbol, symbolVal)
import Network.Streaming.Annotate (Annotate (..))
import Network.Streaming.Classes
import Network.Streaming.Concurrent (Concurrent (Group, group, spawn))
import Network.Streaming.Mode (Mode (..), ModeOf)
import Network.Streaming.Structure (Shape (..), Tree (..), unnamedAction)
import Prelude hiding (id, (.))

-- | A program as a description of its structure: the input and the output are only types.
newtype Diagram a b = Diagram Tree

-- | The core of the tickets that are of the diagram's own reach. No part runs on it: nothing runs.
data DiagramCore

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
  timeoutA (Diagram body) = Diagram (TimeoutA body)

instance Concurrent Diagram where
  data Group Diagram s j
  together parts = Diagram (Together [part | Diagram part <- parts])
  group _ (Diagram body) = Diagram (Grouped body)
  spawn (Diagram part) = Diagram (Spawned part)

-- | The box records where the code asked for it: the outermost call of the stack, which is the one in the program, and
-- not the helpers that the box was called through.
instance Annotate Diagram where
  named label (Diagram body) = Diagram (Named label (outermost callStack) body)
  wires input output (Diagram body) = Diagram (Wires input output body)
  exchange side name (Diagram body) = Diagram (End side name body)
  flowStep name bindings (Diagram body) = Diagram (FlowStep name bindings body)
  allowing check reason (Diagram body) = Diagram (Allowing check reason body)

-- | The place of the outermost call in the stack.
outermost :: CallStack -> Maybe SrcLoc
outermost = fmap snd . listToMaybe . reverse . getCallStack

-- | The tickets of a diagram are never made: there are two instances, for the two reaches, so that a program that
-- uses either is built.
instance Exchange Diagram ('Here DiagramCore) where
  data Future Diagram ('Here DiagramCore) a
  ticket = leaf "ticket" Round
  await = leaf "await" Wait
  fulfil = leaf "fulfil" Handover
  abandon = leaf "abandon" Round

instance Exchange Diagram 'Anywhere where
  data Future Diagram 'Anywhere a
  ticket = leaf "ticket" Round
  await = leaf "await" Wait
  fulfil = leaf "fulfil" Handover
  abandon = leaf "abandon" Round

-- | The inboxes of a diagram are never made either, and have the two reaches for the same reason. Each step is a leaf that
-- has the inbox's name in its text and in its shape: a post is a handover, and a collect is a wait.
instance Inboxes Diagram ('Here DiagramCore) where
  data Inbox Diagram name ('Here DiagramCore) a
  inbox :: forall name a. (KnownSymbol name) => Diagram () (Inbox Diagram name ('Here DiagramCore) a, Address name ('Here DiagramCore) a)
  inbox = atInbox @name "open inbox " OpenInbox Nothing
  collect :: forall name a. (KnownSymbol name) => Diagram (Inbox Diagram name ('Here DiagramCore) a) a
  collect = atInbox @name "collect " CollectFrom (Just envelope)
  post :: forall name a. (KnownSymbol name) => Diagram (Address name ('Here DiagramCore) a, a) Bool
  post = atInbox @name "post " PostTo (Just envelope)
  closeInbox :: forall name a. (KnownSymbol name) => Diagram (Inbox Diagram name ('Here DiagramCore) a) (Maybe a)
  closeInbox = atInbox @name "close inbox " CloseInbox Nothing

instance Inboxes Diagram 'Anywhere where
  data Inbox Diagram name 'Anywhere a
  inbox :: forall name a. (KnownSymbol name) => Diagram () (Inbox Diagram name 'Anywhere a, Address name 'Anywhere a)
  inbox = atInbox @name "open inbox " OpenInbox Nothing
  collect :: forall name a. (KnownSymbol name) => Diagram (Inbox Diagram name 'Anywhere a) a
  collect = atInbox @name "collect " CollectFrom (Just envelope)
  post :: forall name a. (KnownSymbol name) => Diagram (Address name 'Anywhere a, a) Bool
  post = atInbox @name "post " PostTo (Just envelope)
  closeInbox :: forall name a. (KnownSymbol name) => Diagram (Inbox Diagram name 'Anywhere a) (Maybe a)
  closeInbox = atInbox @name "close inbox " CloseInbox Nothing

-- | A leaf for a step at the inbox of the name: its text is the step, then the sign of a message if it has one, then the
-- name, and its shape says which step and which inbox.
atInbox :: forall name a b. (KnownSymbol name) => String -> (String -> Shape) -> Maybe String -> Diagram a b
atInbox step shape sign = leaf (step ++ concat sign ++ symbolVal (Proxy @name)) (shape (symbolVal (Proxy @name)))

-- | The sign of a message, which is an envelope (U+2709), and a space.
envelope :: String
envelope = "\x2709 "

type instance Local Diagram = 'Here DiagramCore

-- | A diagram counts nothing.
type instance ModeOf Diagram = 'Release
