{-# LANGUAGE LambdaCase #-}

-- | What a program built at "Network.Streaming.Diagram" is made of, and the walks over it that the charts of
-- "Network.Streaming.Diagram.Mermaid" and the lint of "Network.Streaming.Lint" share.
--
-- The tree is cyclic: a loop is a tree that contains itself. A walk remembers the nodes on its way down by their stable
-- names, and a node that it meets again on its own way down is a back edge, whose target is a loop head. A node that is
-- shared, and is not on its own way down, is not a loop: each walk says whether it meets it again.
module Network.Streaming.Structure
  ( Tree (..),
    Shape (..),
    unnamedAction,
    opensInbox,
    readable,
    children,
    identify,
    loopHeads,
    Met (..),
    Walked (..),
    walk,
    reach,
  )
where

import Control.Applicative ((<|>))
import Control.Exception (evaluate)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import GHC.Stack (SrcLoc)
import Network.Streaming.Annotate (Check, Side)
import Network.Streaming.Classes (Duration (..), Label)
import System.Mem.StableName (StableName, hashStableName, makeStableName)

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
  | -- | A group: the body, which spawns parts while it runs.
    Grouped Tree
  | -- | A part that is started on its own, while the part that spawns it goes on.
    Spawned Tree
  | -- | A step that is one node: how it is called, and how it is drawn.
    Leaf String Shape
  | -- | A scope: what it acquires, what releases it, and its body.
    Scope Tree Tree Tree
  | -- | A body whose failure is a value.
    Try Tree
  | -- | A body that cancellations from outside don't reach.
    Shield Tree
  | -- | A body that cancellations from outside reach only after a period.
    Grace Duration Tree
  | -- | Two steps that run until the first ends.
    Race Tree Tree
  | -- | A body with a deadline.
    Within Duration Tree
  | -- | A body with a deadline that its input gives.
    TimeoutA Tree
  | -- | A box of its own: its label, the place in the code that named it, and its body.
    Named Label (Maybe SrcLoc) Tree
  | -- | A body with names for its input and output wires.
    Wires (Maybe String) (Maybe String) Tree
  | -- | One end of an exchange of the named value: which end, and the step that is it.
    End Side String Tree
  | -- | A body where the lint doesn't report a check: which, the reason, and the body.
    Allowing Check String Tree

-- | How a leaf is drawn.
data Shape
  = -- | An action that never waits.
    Action
  | -- | A wait for the promise of a ticket to be fulfilled.
    Wait
  | -- | A block until an event fires.
    Blocking
  | -- | A point where the part gives way to the others.
    Yielding
  | -- | A promise fulfilled with a value for another part.
    Handover
  | -- | A post to the inbox of the name: a handover, which waits while the inbox is full.
    PostTo String
  | -- | A collect from the inbox of the name: a wait.
    CollectFrom String
  | -- | The opening of the inbox of the name.
    OpenInbox String
  | -- | The closing of the inbox of the name.
    CloseInbox String
  | -- | A step that is none of those.
    Round

-- | What an action is called in a diagram until the program names it.
unnamedAction :: String
unnamedAction = "IO"

-- | The name of the inbox that a tree opens, if it is the step, or what a chain of steps is made of, that opens it, within
-- as many nodes as the fuel.
opensInbox :: Int -> Tree -> Maybe String
opensInbox fuel step
  | fuel <= 0 = Nothing
  | otherwise = case step of
      Leaf _ (OpenInbox name) -> Just name
      Seq one two -> opensInbox (fuel - 1) one <|> opensInbox (fuel - 1) two
      First inner -> opensInbox (fuel - 1) inner
      Second inner -> opensInbox (fuel - 1) inner
      Wires _ _ inner -> opensInbox (fuel - 1) inner
      End _ _ inner -> opensInbox (fuel - 1) inner
      Allowing _ _ inner -> opensInbox (fuel - 1) inner
      _ -> Nothing

-- | A length of time in the largest unit that has it exactly.
readable :: Duration -> String
readable (Duration count) = case [show (count `div` size) ++ " " ++ unit | (size, unit) <- units, divides size] of
  text : _ -> text
  [] -> show count ++ " ns"
  where
    units = [(1000000000, "s"), (1000000, "ms"), (1000, "us"), (1, "ns")]
    divides size = count `mod` size == 0

-- | The trees that a node is made of.
children :: Tree -> [Tree]
children = \case
  Pure -> []
  Seq one two -> [one, two]
  First inner -> [inner]
  Second inner -> [inner]
  Par one two -> [one, two]
  Fanout one two -> [one, two]
  Ap one two -> [one, two]
  Left' inner -> [inner]
  Right' inner -> [inner]
  Sum one two -> [one, two]
  Choice one two -> [one, two]
  Together parts -> parts
  Grouped inner -> [inner]
  Spawned inner -> [inner]
  Leaf _ _ -> []
  Scope acquire release body -> [acquire, release, body]
  Try inner -> [inner]
  Shield inner -> [inner]
  Grace _ inner -> [inner]
  Race one two -> [one, two]
  Within _ inner -> [inner]
  TimeoutA inner -> [inner]
  Named _ _ inner -> [inner]
  Wires _ _ inner -> [inner]
  End _ _ inner -> [inner]
  Allowing _ _ inner -> [inner]

-- | The node evaluated, and its stable name: a node is evaluated before its name is made, so that the thunk of a node and
-- its value are one.
identify :: Tree -> IO (StableName Tree, Tree)
identify step = do
  tree <- evaluate step
  name <- makeStableName tree
  pure (name, tree)

-- | The nodes below a tree that are met again on their own way down, which are the targets of back edges, from the nodes
-- already on the way down, which are the path. The walk doesn't go into a node that the test holds for, and meets a node
-- that is shared as often as the way down leads to it.
loopHeads :: (Tree -> Bool) -> [StableName Tree] -> Tree -> IO [StableName Tree]
loopHeads boundary path step = do
  tree <- evaluate step
  if boundary tree
    then pure []
    else do
      name <- makeStableName tree
      if name `elem` path
        then pure [name]
        else concat <$> traverse (loopHeads boundary (name : path)) (children tree)

-- | A node that a walk met: the node, the context that it was met in, and the nodes above it on the way down, the nearest
-- first.
data Met context = Met
  { metTree :: Tree,
    metContext :: context,
    metAbove :: [Tree]
  }

-- | What a walk met: each node, in the order they were first met, and the nodes that a back edge goes to, as they were
-- first met.
data Walked context = Walked
  { walkedNodes :: [Met context],
    walkedHeads :: [Met context]
  }

-- | Walks the tree down from its root, into the boxes too, and meets each node once for each context it is in. The context
-- of a node is made by the function from the context above it, and is the node's own too: an annotation sets what holds
-- below it, and at it. A node that is on its own way down is not walked into again: it is a loop head.
walk :: (Eq context) => context -> (Tree -> context -> context) -> Tree -> IO (Walked context)
walk start enter root = do
  seen <- newIORef IntMap.empty
  nodes <- newIORef []
  heads <- newIORef []
  let visit stack above context step = do
        (name, tree) <- identify step
        case lookup name stack of
          Just met -> modifyIORef' heads (\found -> if any ((== name) . fst) found then found else (name, met) : found)
          Nothing -> do
            let inside = enter tree context
            known <- remember seen name inside
            if known
              then pure ()
              else do
                let met = Met tree inside above
                modifyIORef' nodes (met :)
                mapM_ (visit ((name, met) : stack) (tree : above) inside) (children tree)
  visit [] [] start root
  Walked <$> (reverse <$> readIORef nodes) <*> (reverse . fmap snd <$> readIORef heads)
  where
    -- Whether the node was met in the context before, noting it if not.
    remember :: (Eq context) => IORef (IntMap [(StableName Tree, context)]) -> StableName Tree -> context -> IO Bool
    remember seen name context = do
      found <- IntMap.findWithDefault [] (hashStableName name) <$> readIORef seen
      if (name, context) `elem` found
        then pure True
        else False <$ modifyIORef' seen (IntMap.insertWith (++) (hashStableName name) [(name, context)])

-- | The way from the tree to the first node below it, or it, that the test holds for: the nodes on it, in order, into the
-- boxes too, but not into a part that a spawn starts, which runs on its own. Each node is looked at once, so a loop is no
-- trouble.
reach :: (Tree -> Bool) -> Tree -> IO (Maybe [Tree])
reach found root = do
  seen <- newIORef IntMap.empty
  let visit step = do
        (name, tree) <- identify step
        known <- elem name . IntMap.findWithDefault [] (hashStableName name) <$> readIORef seen
        if known
          then pure Nothing
          else do
            modifyIORef' seen (IntMap.insertWith (++) (hashStableName name) [name])
            if found tree
              then pure (Just [tree])
              else fmap (tree :) <$> firstOf (case tree of Spawned _ -> []; _ -> children tree)
      firstOf = \case
        [] -> pure Nothing
        step : others -> visit step >>= maybe (firstOf others) (pure . Just)
  visit root
