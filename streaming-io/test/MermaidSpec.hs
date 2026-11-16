{-# LANGUAGE DataKinds #-}

-- | The charts of "Network.Streaming.Diagram.Mermaid": small programs, built at the diagram arrow, whose charts have what
-- they should.
module MermaidSpec (spec) where

import Control.Arrow ((&&&), (>>>), (>>^), (^>>), (|||), returnA)
import Data.Char (isAlphaNum)
import Data.List (isInfixOf, isPrefixOf, nub)
import Network.Streaming.Classes
import Network.Streaming.Combinators (_Just, box, decide, done, step, tap, withInbox)
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (..))
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.Diagram.Mermaid (Page (..), pages)
import Network.Streaming.Named (adding, at, cases, field, is, (:=), (?>))
import Network.Streaming.Plumbing (each, prism)
import Skeletest

-- | A step that never waits.
quick :: (Effect arr) => arr a ()
quick = now (const (pure ()))

-- | A scope that opens the inbox "jobs", for a body that is given the inbox and its address.
owning :: Diagram ((Inbox Diagram "jobs" 'Anywhere (), Address "jobs" 'Anywhere ()), ()) () -> Diagram () ()
owning = withInbox @"jobs" @'Anywhere (quick :: Diagram ((), ()) ())

-- | A step that posts to the inbox, and a step that collects from it.
poster :: Diagram (Address "jobs" 'Anywhere ()) ()
poster = (,()) ^>> post >>> quick

collector :: Diagram (Inbox Diagram "jobs" 'Anywhere ()) ()
collector = collect >>> quick

-- | A body that runs the post and the collect side by side.
beside :: (Diagram (Address "jobs" 'Anywhere ()) (), Diagram (Inbox Diagram "jobs" 'Anywhere ()) ()) -> Diagram ((Inbox Diagram "jobs" 'Anywhere (), Address "jobs" 'Anywhere ()), ()) ()
beside (posting, collecting) = fst ^>> ((snd ^>> posting) &&& (fst ^>> collecting) >>> done)

-- | The posts and the collects as parts that run together, which are lanes.
together' :: Diagram ((Inbox Diagram "jobs" 'Anywhere (), Address "jobs" 'Anywhere ()), ()) ()
together' = fst ^>> together [snd ^>> poster, fst ^>> collector]

-- | What is drawn in front of the label of a step that never waits: a gear (U+2699) as a plain glyph, not a coloured
-- emoji (U+FE0E), and a space.
gear :: String
gear = "\x2699\xFE0E "

spec :: Spec
spec = describe "the chart" $ do
  it "has a store for the inbox, with an edge from its post and an edge to its collect, in the style of a message" $ do
    chart <- chartOf (owning (beside (poster, collector)))
    case storeIn chart of
      Nothing -> failTest "the chart has no store"
      Just store -> do
        declarations chart (into chart store) `shouldBe` ["post"]
        declarations chart (outOf chart store) `shouldBe` ["collect"]
        any ("linkStyle" `isPrefixOf`) (stripped chart) `shouldBe` True

  it "has the edges at the boxes that the post and the collect are in" $ do
    chart <- chartOf (owning (beside (box "producer" (poster >>> quick), box "consumer" (collector >>> quick))))
    case storeIn chart of
      Nothing -> failTest "the chart has no store"
      Just store ->
        (declaredAs "producer" chart (into chart store), declaredAs "consumer" chart (outOf chart store)) `shouldBe` (True, True)

  it "has the edges at the lanes that the post and the collect are in, when they run together" $ do
    chart <- chartOf (owning together')
    case storeIn chart of
      Nothing -> failTest "the chart has no store"
      Just store -> do
        (length (into chart store), length (outOf chart store)) `shouldBe` (1, 1)
        [any (("subgraph " ++ lane ++ " ") `isPrefixOf`) (stripped chart) | lane <- into chart store ++ outOf chart store]
          `shouldBe` [True, True]

  it "has a group for a deadline that the input gives, which says so" $ do
    chart <- chartOf (timeoutA (quick >>> quick) :: Diagram (Duration, ()) (Maybe ()))
    any ("within the duration of the input" `isInfixOf`) chart `shouldBe` True

  it "has no store, and no edge of a message, for a program with no inbox" $ do
    chart <- chartOf (quick >>> quick :: Diagram () ())
    (storeIn chart, any ("-.->" `isInfixOf`) (stripped chart)) `shouldBe` (Nothing, False)

  it "has the name of each field that is made on the wire out of its step" $ do
    chart <- chartOf ((is "name" quick &&& is "size" quick) >>> quick :: Diagram () ())
    let labelled = [name | name <- ["name", "size"], any (("|\"" ++ name ++ "\"|") `isInfixOf`) (stripped chart)]
    labelled `shouldBe` ["name", "size"]

  it "has the loop of each: the step goes round, the decision asks for another focus, and the loop is named" $ do
    chart <- chartOf (each working :: Diagram [Int] [()])
    (wiresNamed "each" chart, wiresNamed "again" chart, wiresNamed "finish" chart) `shouldBe` (1, 1, 1)
    any (("{\"another focus?\"}" `isInfixOf`)) chart `shouldBe` True
    let worked = nodesDeclared (gear ++ "working") chart
    length worked `shouldBe` 1
    [from | (from, _) <- edgesOf chart, from `elem` worked, reaches chart from from] `shouldBe` worked

  it "has the branches of a prism: the arrow is on the branch of the focus, and the other case goes past it" $ do
    chart <- chartOf (prism Just (maybe (Left Nothing) Right) working >>> afterwards :: Diagram (Maybe Int) ())
    case (nodesDeclared (gear ++ "working") chart, nodesDeclared (gear ++ "afterwards") chart) of
      ([inside], [after]) -> do
        targetsOf "Right" chart `shouldBe` [inside]
        targetsOf "Left" chart `shouldBe` [after]
        ((inside, after) `elem` edgesOf chart) `shouldBe` True
      found -> failTest ("the chart has the steps " <> show found)

  it "has the branches of _Just named Just and Nothing, with the arrow on the branch of Just" $ do
    chart <- chartOf (_Just working >>> afterwards :: Diagram (Maybe Int) ())
    (wiresNamed "Just" chart, wiresNamed "Nothing" chart) `shouldBe` (1, 1)
    (wiresNamed "Left" chart, wiresNamed "Right" chart) `shouldBe` (0, 0)
    case (nodesDeclared (gear ++ "working") chart, nodesDeclared (gear ++ "afterwards") chart) of
      ([inside], [after]) -> do
        targetsOf "Just" chart `shouldBe` [inside]
        targetsOf "Nothing" chart `shouldBe` [after]
        ((inside, after) `elem` edgesOf chart) `shouldBe` True
      found -> failTest ("the chart has the steps " <> show found)

  it "has the question of the decision and the names of the cases on the branches of a chain of ?>" $ do
    let program ::
          Diagram ("head" := Either ("stream ended" := ()) (Either ("refused" := Int) ("good" := Bool))) ()
        program =
          decide "how did the head end?" (cases "head")
            >>> ("stream ended" ?> quick ||| "refused" ?> quick ||| "good" ?> quick)
    chart <- chartOf program
    [wiresNamed name chart | name <- ["stream ended", "refused", "good"]] `shouldBe` [1, 1, 1]
    any ("{\"how did the head end?\"}" `isInfixOf`) chart `shouldBe` True
    length (nub (concat [sourcesOf name chart | name <- ["stream ended", "refused", "good"]])) `shouldBe` 1

  it "has the names of both wires of an arrow that |- names before it and -| after it" $ do
    chart <- chartOf ("in" |- working -| "out" >>> quick :: Diagram Int ())
    (wiresNamed "in" chart, wiresNamed "out" chart) `shouldBe` (1, 1)

  it "has the name that -| gives a wire at a junction, where the next arrow names it too: a wire is labelled once" $ do
    chart <- chartOf (working -| "a" >>> "b" |- quick :: Diagram Int ())
    (wiresNamed "a" chart, wiresNamed "b" chart) `shouldBe` (1, 0)

  it "has the names that |- gives to the branches of a choice, with no parentheses around the branches" $ do
    chart <- chartOf (("yes" |- working ||| "no" |- working) :: Diagram (Either Int Int) ())
    (wiresNamed "yes" chart, wiresNamed "no" chart) `shouldBe` (1, 1)

  it "has the name that |- gives to the arrow that a function applied to arguments makes, with no parentheses" $ do
    chart <- chartOf ("x" |- step "working" (const (pure ())) >>> quick :: Diagram Int ())
    wiresNamed "x" chart `shouldBe` 1

  it "has the names of the wires in a pipeline that mixes the operators with ^>> and >>^" $ do
    chart <- chartOf ((+ 1) ^>> "in" |- working -| "out" >>> quick :: Diagram Int ())
    (wiresNamed "in" chart, wiresNamed "out" chart) `shouldBe` (1, 1)
    ended <- chartOf ("in" |- working -| "out" >>^ const (Just ()) :: Diagram Int (Maybe ()))
    (wiresNamed "in" ended, wiresNamed "out" ended) `shouldBe` (1, 1)

  it "has the name of the field that adding puts in front of a record, on the wire out of its step" $ do
    chart <- chartOf (adding "size" quick >>> quick :: Diagram () ())
    wiresNamed "size" chart `shouldBe` 1

  it "has the name of the field that at runs its arrow on, on the wire into its step and out of it" $ do
    chart <- chartOf (at "size" quick >>> quick :: Diagram ("size" := Int, "kept" := Bool) ())
    wiresNamed "size" chart `shouldBe` 2

  it "has the name of the field that field reads, on the wire out of it" $ do
    chart <- chartOf (field "size" >>> quick :: Diagram ("size" := Int, "kept" := Bool) ())
    wiresNamed "size" chart `shouldBe` 1

  it "has a gear in front of the label of a step that never waits and has no name" $ do
    chart <- chartOf (quick :: Diagram () ())
    length (nodesDeclared (gear ++ "IO") chart) `shouldBe` 1

  it "has a gear in front of the name of a step that never waits and has one" $ do
    chart <- chartOf (working >>> quick)
    length (nodesDeclared (gear ++ "working") chart) `shouldBe` 1

  it "has no gear in front of a block, a wait, a box or a decision, nor on the box's own page" $ do
    blocked <- chartOf (block :: Diagram Event ())
    waited <- chartOf (await :: Diagram (Future Diagram 'Anywhere ()) ())
    boxed <- chartsOf (block >>> box "outer" (decide "inside?" Left >>> (returnA ||| returnA)) :: Diagram Event ())
    decided <- chartOf (decide "which?" (\n -> if n > (0 :: Int) then Left () else Right ()) >>> (returnA ||| returnA))
    let drawnAs shape chart = length [line | line <- stripped chart, shape `isInfixOf` line]
    case boxed of
      [outside, inside] -> do
        let drawn =
              [ drawnAs "hourglass" blocked,
                drawnAs "delay" waited,
                drawnAs "[[\"outer\"]]" outside,
                drawnAs "{\"which?\"}" decided
              ]
        drawn `shouldBe` [1, 1, 1, 1]
        drawnAs "{\"inside?\"}" inside `shouldBe` 1
      found -> failTest ("the box has " <> show (length found) <> " pages, and not its own and the page that has it")
    any (gear `isInfixOf`) (concat ([blocked, waited, decided] <> boxed)) `shouldBe` False

  it "has the gears of the steps of a box's body on the box's own page, and none in front of the box" $ do
    boxed <- chartsOf (block >>> box "outer" (quick >>> quick) :: Diagram Event ())
    case boxed of
      [outside, inside] -> do
        any (gear `isInfixOf`) outside `shouldBe` False
        length (nodesDeclared (gear ++ "IO") inside) `shouldBe` 2
        length (filter (gear `isInfixOf`) inside) `shouldBe` 2
      found -> failTest ("the box has " <> show (length found) <> " pages, and not its own and the page that has it")

  it "has a step that watches, drawn as a wait, and a step that hands over, drawn as a handover" $ do
    chart <- chartOf (watching >>> handing >>> quick :: Diagram () ())
    any (\line -> "shape: delay" `isInfixOf` line && "\"watch\"" `isInfixOf` line) chart `shouldBe` True
    any (\line -> "shape: lean-r" `isInfixOf` line && "\"hand\"" `isInfixOf` line) chart `shouldBe` True

  it "has a box for a group, which has its body" $ do
    chart <- chartOf (group AwaitAll spawnsWorking :: Diagram Int ((), ()))
    case subgraphTitled "group" chart of
      Nothing -> failTest "the chart has no box for the group"
      Just (_, inside) -> length (nodesDeclared (gear ++ "collected") inside) `shouldBe` 1

  it "has a box labelled spawn for a spawn, which holds the part, has an edge into it from the step before, and has no edge out of the part" $ do
    chart <- chartOf (group AwaitAll spawnsWorking :: Diagram Int ((), ()))
    case subgraphTitled "spawn" chart of
      Nothing -> failTest "the chart has no box for the spawn"
      Just (spawned, inside) -> do
        case (nodesDeclared (gear ++ "working") inside, nodesDeclared (gear ++ "collected") chart) of
          ([part], [collected]) -> do
            ((collected, spawned) `elem` edgesOf chart) `shouldBe` True
            [to | (from, to) <- edgesOf chart, from == part] `shouldBe` []
          found -> failTest ("the chart has the steps " <> show found)
        length (nodesDeclared (gear ++ "working") chart) `shouldBe` 1

-- | A step that watches a ticket, and a step that hands over to it.
watching :: Diagram () ()
watching = const undefined ^>> untilParked @_ @'Anywhere @()

handing :: Diagram () ()
handing = const undefined ^>> handWaiting @_ @'Anywhere @() >>> quick

-- | A body that collects, and spawns the part that works.
spawnsWorking :: Diagram (Group Diagram s AwaitAll, Int) ()
spawnsWorking = tap (snd ^>> step "collected" (const (pure ()))) >>> spawn working

-- | The node and the lines inside the subgraph of the title, if the chart has one: the lines that are indented further
-- than its first line, up to its end.
subgraphTitled :: String -> [String] -> Maybe (String, [String])
subgraphTitled title chart = case break opens chart of
  (_, []) -> Nothing
  (_, opening : rest) -> Just (words opening !! 1, takeWhile (\line -> indentOf line > indentOf opening) rest)
  where
    opens line = "subgraph " `isPrefixOf` dropWhile (== ' ') line && ("[\"" ++ title ++ "\"]") `isInfixOf` line
    indentOf = length . takeWhile (== ' ')

-- | The chart of the top page of the program, as lines.
chartOf :: Diagram a b -> IO [String]
chartOf program = do
  found <- pages program
  pure $ case found of
    page : _ -> lines (pageMermaid page)
    [] -> []

-- | The charts of all the pages of the program, as lines: the top page, and then the page of each box.
chartsOf :: Diagram a b -> IO [[String]]
chartsOf program = fmap (lines . pageMermaid) <$> pages program

-- | A step that is a node of the chart of its own, of the name.
working :: Diagram Int ()
working = step "working" (const (pure ()))

-- | The step after it.
afterwards :: Diagram (Maybe ()) ()
afterwards = step "afterwards" (const (pure ()))

-- | The edges of the chart, from a node to a node, whatever their labels.
edgesOf :: [String] -> [(String, String)]
edgesOf chart =
  [ (takeWhile isAlphaNum line, last (words line))
  | line <- stripped chart,
    " --> " `isInfixOf` line || " -->|" `isInfixOf` line
  ]

-- | The nodes that an edge of the label leaves.
sourcesOf :: String -> [String] -> [String]
sourcesOf label chart =
  [takeWhile isAlphaNum line | line <- stripped chart, ("-->|\"" ++ label ++ "\"|") `isInfixOf` line]

-- | The nodes that an edge of the label goes to.
targetsOf :: String -> [String] -> [String]
targetsOf label chart = [last (words line) | line <- stripped chart, ("-->|\"" ++ label ++ "\"|") `isInfixOf` line]

-- | The nodes that are declared with the text, as a step of its own.
nodesDeclared :: String -> [String] -> [String]
nodesDeclared text chart =
  [takeWhile isAlphaNum line | line <- stripped chart, ("[\"" ++ text ++ "\"]") `isInfixOf` line]

-- | Whether the edges lead from the node to the target, in one step or more.
reaches :: [String] -> String -> String -> Bool
reaches chart from target = go [] [from]
  where
    edges = edgesOf chart
    go _ [] = False
    go seen (node : rest)
      | node `elem` seen = go seen rest
      | otherwise =
          let onward = [to | (source, to) <- edges, source == node]
           in target `elem` onward || go (node : seen) (onward ++ rest)

-- | How many wires of the chart are labelled with the name.
wiresNamed :: String -> [String] -> Int
wiresNamed name = length . filter (("|\"" ++ name ++ "\"|") `isInfixOf`) . stripped

-- | The lines without their indent.
stripped :: [String] -> [String]
stripped = fmap (dropWhile (== ' '))

-- | The node that is declared as a store, which is a cylinder with the name of the inbox.
storeIn :: [String] -> Maybe String
storeIn chart = case [takeWhile isAlphaNum line | line <- stripped chart, "[(\"jobs\")]" `isInfixOf` line] of
  node : _ -> Just node
  [] -> Nothing

-- | The nodes that have an edge of a message to the node, and the nodes that it has an edge of a message to.
into, outOf :: [String] -> String -> [String]
into chart node = [from | [from, "-.->", to] <- words <$> stripped chart, to == node]
outOf chart node = [to | [from, "-.->", to] <- words <$> stripped chart, from == node]

-- | The text that declares each of the nodes, which has what the step is called.
declarations :: [String] -> [String] -> [String]
declarations chart nodes =
  [ if "post" `isInfixOf` text then "post" else if "collect" `isInfixOf` text then "collect" else text
    | node <- nodes,
      text <- take 1 [line | line <- stripped chart, takeWhile isAlphaNum line == node, not ("-->" `isInfixOf` line), not ("-.->" `isInfixOf` line)]
  ]

-- | Whether each of the nodes is declared with the title.
declaredAs :: String -> [String] -> [String] -> Bool
declaredAs title chart nodes = not (null nodes) && and [any (\line -> takeWhile isAlphaNum line == node && title `isInfixOf` line) (stripped chart) | node <- nodes]
