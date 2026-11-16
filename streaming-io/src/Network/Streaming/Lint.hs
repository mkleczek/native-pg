{-# LANGUAGE LambdaCase #-}

-- | A lint over the structure of a program built at "Network.Streaming.Diagram". It sees what a reader of one function
-- can't: the structure across function boundaries, after helpers such as @withTicket@ and @receive@ have expanded, and
-- where the runtime can cancel a part. It suggests changes and never rewrites anything.
--
-- A part is cancelled at its waits only: the leaves @block@, @await@ and @giveWay@, and @fulfil@, which yields in the
-- arrows that run every part on one thread. An action that never waits, @now@, is never interrupted. A part of the tree
-- can wait if a wait is reachable inside it, through the boxes, wires, exchanges, scopes, parts that run together, races
-- and deadlines.
--
-- The inboxes are checked across the whole program, by their names: a name that has posts and no collects, or collects and
-- no posts, anywhere, and a name that is opened at two sites.
--
-- A part allows a check with 'allowing', for a reason, and a finding inside it is listed apart from the others.
module Network.Streaming.Lint
  ( Check (..),
    Finding (..),
    Allowed (..),
    Report (..),
    lint,
    renderReport,
  )
where

import Control.Applicative ((<|>))
import Data.Foldable (asum)
import Data.Function (on)
import Data.List (intercalate, nub, nubBy)
import Data.Maybe (isNothing, listToMaybe)
import GHC.Stack (SrcLoc (..))
import Network.Streaming.Annotate (Check (..))
import Network.Streaming.Classes (Label (..))
import Network.Streaming.Diagram (Diagram (..))
import Network.Streaming.Structure
import System.Mem.StableName (StableName)

-- | What the lint found in a part of the program, and where.
data Finding = Finding
  { findingCheck :: Check,
    -- | The titles of the boxes on the way down from the top of the program to the part.
    findingPath :: [String],
    -- | The place in the code of the nearest box that has one.
    findingSource :: Maybe SrcLoc,
    -- | The wait or the construct that the finding is about.
    findingWhat :: String,
    -- | What is wrong, and what to do instead.
    findingMessage :: String
  }
  deriving (Eq)

-- | A finding in a part that allows its check, with the reason that the part gives.
data Allowed = Allowed
  { allowedFinding :: Finding,
    allowedReason :: String
  }

-- | What the lint came to: the findings, and the findings that a part allows, each in the order that the walk met them.
data Report = Report
  { reportFindings :: [Finding],
    reportAllowed :: [Allowed]
  }

-- | The checks that the parts above a node allow, and the node does, each with its reason.
type Allowances = [(Check, String)]

-- | A finding, with the allowances that hold where it is.
data Found = Found Finding Allowances

-- | Lints the program: every part of it, into the boxes too, once for each set of allowances it is in.
lint :: Diagram a b -> IO Report
lint (Diagram root) = do
  Walked nodes heads <- walk [] allow root
  structural <- concat <$> traverse atNode nodes
  loops <- concat <$> traverse atHead heads
  inboxes <- atInboxes nodes
  pure (settle (structural ++ loops ++ inboxes))
  where
    allow = \case
      Allowing check reason _ -> \held -> if (check, reason) `elem` held then held else held ++ [(check, reason)]
      _ -> id

-- | The findings, split by whether the part they are in allows them. The same finding, with the same reason, is one.
settle :: [Found] -> Report
settle results =
  Report [finding | (finding, Nothing) <- judged] [Allowed finding reason | (finding, Just reason) <- judged]
  where
    judged = nubBy (\(one, why) (other, otherWhy) -> one == other && why == otherWhy) (judge <$> results)
    judge (Found finding held) = (finding, lookup (findingCheck finding) held)

-- * The checks of a node

-- | The checks that look at one node of the tree: a scope, a deadline, a race and a shield.
atNode :: Met Allowances -> IO [Found]
atNode met@(Met tree _ _) = case tree of
  Scope acquire _ _ -> do
    way <- waitIn acquire
    pure
      [ found met WaitInAcquire ("waits at " ++ waitAt waiting) $
          "The acquire of this scope can wait, and a cancellation there skips its release: "
            ++ "acquire in a step that never waits, and wait in the body, or have the waiting step undo itself."
        | Just waiting <- [way]
      ]
  Within duration body -> deadline ("within " ++ readable duration) body
  TimeoutA body -> deadline "within the duration of its input" body
  Race one two -> do
    stuck <- traverse (fmap isNothing . waitIn) [one, two]
    pure
      [ found met DeadlineWithoutWait ("race: " ++ sides) $
          "A side of this race can't wait, so the race is decided at once: "
            ++ "race parts that wait, and run the part that doesn't wait on its own."
        | Just sides <- [stuckSides stuck]
      ]
  Shield body -> do
    way <- waitIn body
    pure
      [ found met ShieldAroundWait ("waits at " ++ waitAt waiting) $
          "This shield holds cancellation off for as long as the wait lasts: "
            ++ "shield only the step that must not be cancelled, or bound the wait with a deadline inside the shield."
        | Just waiting <- [way]
      ]
  _ -> pure []
  where
    deadline what body = do
      way <- waitIn body
      pure
        [ found met DeadlineWithoutWait what $
            "The body of this deadline can't wait, so the deadline can never fire: "
              ++ "drop the deadline, or put the wait that it is meant to bound inside it."
          | isNothing way
        ]
    stuckSides = \case
      [True, True] -> Just "neither side can wait"
      [True, False] -> Just "the first side can't wait"
      [False, True] -> Just "the second side can't wait"
      _ -> Nothing

-- | The way to the first wait that the tree can reach, if it can wait.
waitIn :: Tree -> IO (Maybe [Tree])
waitIn = reach isWait

-- | Whether the node is a wait: a leaf where a part is cancelled, or gives way.
isWait :: Tree -> Bool
isWait = \case
  Leaf _ shape -> case shape of
    Wait -> True
    Blocking -> True
    Yielding -> True
    Handover -> True
    PostTo _ -> True
    CollectFrom _ -> True
    Action -> False
    Round -> False
    OpenInbox _ -> False
    CloseInbox _ -> False
  _ -> False

-- | The wait at the end of a way to it, and the nearest box that it is in.
waitAt :: [Tree] -> String
waitAt way = case (reverse way, [title | Named (Label title _) _ _ <- reverse way]) of
  (Leaf text _ : _, title : _) -> text ++ " in " ++ title
  (Leaf text _ : _, []) -> text
  _ -> "a wait"

-- | A finding at the node: where it is in the boxes, and in the code.
found :: Met Allowances -> Check -> String -> String -> Found
found (Met tree held above) check what message = Found (Finding check path source what message) held
  where
    boxes = [(title, place) | Named (Label title _) place _ <- tree : above]
    path = reverse (fst <$> boxes)
    source = listToMaybe [place | (_, Just place) <- boxes]

-- * Inboxes

-- | The findings about the inboxes of the whole program, which are paired by name: the posts of a name that nothing collects,
-- the collects of a name that nothing posts to, and the sites that open a name that another site opens.
atInboxes :: [Met Allowances] -> IO [Found]
atInboxes nodes = do
  sites <- traverse (\(name, met) -> (\(site, _) -> (name, site, met)) <$> identify (metTree met)) openers
  pure (dead ++ opened sites)
  where
    posts = [(name, met) | met@(Met (Leaf _ (PostTo name)) _ _) <- nodes]
    collects = [(name, met) | met@(Met (Leaf _ (CollectFrom name)) _ _) <- nodes]
    openers = [(name, met) | met@(Met (Scope acquire _ _) _ _) <- nodes, Just name <- [opensInbox 16 acquire]]
    dead =
      [ found met DeadInbox ("posts to " ++ name) $
          "Nothing in the program collects from the inbox " ++ show name ++ ", so what is posted to it stays there: "
            ++ "collect from it, or drop the posts."
        | (name, met) <- posts,
          name `notElem` fmap fst collects
      ]
        ++ [ found met DeadInbox ("collects from " ++ name) $
               "Nothing in the program posts to the inbox " ++ show name ++ ", so this collect waits for ever: "
                 ++ "post to it, or drop the collect."
             | (name, met) <- collects,
               name `notElem` fmap fst posts
           ]
    -- A site is a scope, which parts that are replicated share, so that they are one site: its stable name is the same.
    opened sites =
      [ found met InboxOpenedTwice ("opens " ++ name ++ " again") $
          "The inbox " ++ show name ++ " is opened at another site too, so its posts and collects can't be told apart: "
            ++ "give each inbox a name of its own."
        | name <- nub [name | (name, _, _) <- sites],
          (_, _, met) <- drop 1 (nubBy ((==) `on` stable) [site | site@(other, _, _) <- sites, other == name])
      ]
    stable (_, name, _) = name

-- * Loops

-- | The finding for a loop, if some way round it from its head back to its head passes no wait.
atHead :: Met Allowances -> IO [Found]
atHead met@(Met tree _ _) =
  wayRound tree >>= \case
    Nothing -> pure []
    Just steps ->
      pure . pure $
        found met LoopWithoutWait ("a way round without a wait: " ++ stepsText steps) $
          "A way round this loop passes no wait, so on one thread it never gives way and keeps the core: "
            ++ "put a wait or a give way on every way round."
  where
    stepsText = \case
      [] -> "no named step"
      steps -> intercalate " > " steps

-- | What a part does on the ways through it that pass no wait, from its start, when it is part of a loop: a way to the
-- loop's head, which is a way round, and a way out of the part, with the steps that each passes.
data Ways = Ways
  { wayBack :: Maybe [String],
    wayOut :: Maybe [String]
  }

-- | A way round a loop from its head back to its head that passes no wait, as the steps that it passes.
wayRound :: Tree -> IO (Maybe [String])
wayRound top = do
  (name, tree) <- identify top
  wayBack <$> below name [name] tree

-- | The ways through a node of the loop whose head is the first node. The head is a way round. A node that is on the way
-- down already, and is not the head, is a loop inside the loop, entered again, which has no way on: the way out of an
-- inner loop is the one that its first visit finds.
waysOf :: StableName Tree -> [StableName Tree] -> Tree -> IO Ways
waysOf top path step = identify step >>= uncurry reached
  where
    reached name tree
      | name == top = pure (Ways (Just []) Nothing)
      | name `elem` path = pure (Ways Nothing Nothing)
      | otherwise = below top (name : path) tree

-- | The ways through a node of the loop, from the ways through the parts it is made of.
below :: StableName Tree -> [StableName Tree] -> Tree -> IO Ways
below top path tree = combine tree <$> traverse (waysOf top path) (children tree)

-- | The ways through a node, from the ways through its parts.
--
-- * @Seq@, @First@, @Second@, @Par@, @Fanout@ and @Ap@ run all their parts, one after the other, so a wait in a part
--   before the way round counts, and a part after it is not reached.
-- * @Choice@ and @Sum@ split the ways: the ways of either branch. @Left'@ and @Right'@ have a way that passes the part.
-- * @Race@ runs its sides at once and ends with the first: a way through either side is a way through it.
-- * @Together@ runs its parts at once, each a way of its own: a part that loops without a wait keeps the core, whatever
--   the other parts wait for. The way out is a way out of every part.
-- * A scope runs its acquire, its body and its release.
-- * A @Grouped@ runs its body. A @Spawned@ part runs on its own, as a part of its own that the lint judges by itself: the step that
--   starts it is a way through that waits for nothing, whatever the part waits for.
combine :: Tree -> [Ways] -> Ways
combine tree inner = case tree of
  Pure -> Ways Nothing (Just [])
  Leaf text _ -> Ways Nothing (if isWait tree then Nothing else Just [text | text /= unnamedAction])
  Seq {} -> inOrder inner
  First {} -> inOrder inner
  Second {} -> inOrder inner
  Par {} -> inOrder inner
  Fanout {} -> inOrder inner
  Ap {} -> inOrder inner
  Try {} -> inOrder inner
  Shield {} -> inOrder inner
  Within {} -> inOrder inner
  TimeoutA {} -> inOrder inner
  Wires {} -> inOrder inner
  End {} -> inOrder inner
  FlowScope {} -> inOrder inner
  FlowStep {} -> inOrder inner
  FlowCase {} -> inOrder inner
  Allowing {} -> inOrder inner
  Named (Label title _) _ _ -> case inner of
    [only] -> Ways ((title :) <$> wayBack only) ((title :) <$> wayOut only)
    _ -> inOrder inner
  Scope {} -> case inner of
    [acquire, release, body] -> inOrder [acquire, body, release]
    _ -> inOrder inner
  Left' {} -> passing inner
  Right' {} -> passing inner
  Sum {} -> eitherOf inner
  Choice {} -> eitherOf inner
  Race {} -> eitherOf inner
  Together {} -> Ways (asum (wayBack <$> inner)) (concat <$> traverse wayOut inner)
  Grouped {} -> inOrder inner
  Spawned {} -> Ways Nothing (Just ["spawn"])

-- | The ways through parts that run one after the other: a way round in a part, or a way out of the parts before it and a
-- way round in the part after.
inOrder :: [Ways] -> Ways
inOrder = foldr follow (Ways Nothing (Just []))
  where
    follow first rest =
      Ways
        (wayBack first <|> ((++) <$> wayOut first <*> wayBack rest))
        ((++) <$> wayOut first <*> wayOut rest)

-- | The ways through a part that a step may pass by.
passing :: [Ways] -> Ways
passing inner = Ways (asum (wayBack <$> inner)) (asum (wayOut <$> inner) <|> Just [])

-- | The ways through any one of the parts.
eitherOf :: [Ways] -> Ways
eitherOf inner = Ways (asum (wayBack <$> inner)) (asum (wayOut <$> inner))

-- * The report as text

-- | The report as lines of text: each finding with its place, what it is about and what to do, then each allowed finding
-- with its reason, and the counts.
renderReport :: Report -> String
renderReport (Report findings allowed) =
  unlines $
    concatMap describe findings
      ++ ["allowed:" | not (null allowed)]
      ++ concatMap describeAllowed allowed
      ++ [show (length findings) ++ " findings, " ++ show (length allowed) ++ " allowed"]
  where
    describe finding =
      [ show (findingCheck finding) ++ ": " ++ located finding,
        "  " ++ findingWhat finding,
        "  " ++ findingMessage finding
      ]
    describeAllowed (Allowed finding reason) =
      [ show (findingCheck finding) ++ ": " ++ located finding,
        "  " ++ findingWhat finding,
        "  allowed because " ++ reason
      ]
    located finding =
      (if null (findingPath finding) then "the program" else intercalate " > " (findingPath finding))
        ++ maybe "" place (findingSource finding)
    place source = " (" ++ srcLocFile source ++ ":" ++ show (srcLocStartLine source) ++ ")"
