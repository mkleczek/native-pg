{-# LANGUAGE DataKinds #-}

-- | The charts of "Network.Streaming.Diagram.Mermaid": small programs, built at the diagram arrow, whose charts have what
-- they should.
module MermaidSpec (spec) where

import Control.Arrow ((&&&), (>>>), (^>>), (|||))
import Data.Char (isAlphaNum)
import Data.List (isInfixOf, isPrefixOf, nub)
import Network.Streaming.Classes
import Network.Streaming.Combinators (_Just, box, decide, done, each, step, withInbox)
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.Diagram.Mermaid (Page (..), pages)
import Network.Streaming.Named (adding, at, cases, field, is, (:=), (?>))
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

-- | The stub of ?> fails when the program is built, until it is written, and the mark comes off with it.
stubbed :: Spec -> Spec
stubbed = xfail "Network.Streaming.Named has stubs: ?> is not written yet"

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
    let worked = nodesDeclared "working" chart
    length worked `shouldBe` 1
    [from | (from, _) <- edgesOf chart, from `elem` worked, reaches chart from from] `shouldBe` worked

  it "has the branches of a prism: the arrow is on the branch of the focus, and the other case goes past it" $ do
    chart <- chartOf (_Just working >>> afterwards :: Diagram (Maybe Int) ())
    case (nodesDeclared "working" chart, nodesDeclared "afterwards" chart) of
      ([inside], [after]) -> do
        targetsOf "Right" chart `shouldBe` [inside]
        targetsOf "Left" chart `shouldBe` [after]
        ((inside, after) `elem` edgesOf chart) `shouldBe` True
      found -> failTest ("the chart has the steps " <> show found)

  stubbed $ it "has the question of the decision and the names of the cases on the branches of a chain of ?>" $ do
    chart <- chartOf (decide "how did the head end?" (cases "head") >>> ("stream ended" ?> quick ||| "refused" ?> quick ||| "good" ?> quick) :: Diagram ("head" := Either ("stream ended" := ()) (Either ("refused" := Int) ("good" := Bool))) ())
    [wiresNamed name chart | name <- ["stream ended", "refused", "good"]] `shouldBe` [1, 1, 1]
    any ("{\"how did the head end?\"}" `isInfixOf`) chart `shouldBe` True
    length (nub (concat [sourcesOf name chart | name <- ["stream ended", "refused", "good"]])) `shouldBe` 1

  it "has the name of the field that adding puts in front of a record, on the wire out of its step" $ do
    chart <- chartOf (adding "size" quick >>> quick :: Diagram () ())
    wiresNamed "size" chart `shouldBe` 1

  it "has the name of the field that at runs its arrow on, on the wire into its step and out of it" $ do
    chart <- chartOf (at "size" quick >>> quick :: Diagram ("size" := Int, "kept" := Bool) ())
    wiresNamed "size" chart `shouldBe` 2

  it "has the name of the field that field reads, on the wire out of it" $ do
    chart <- chartOf (field "size" >>> quick :: Diagram ("size" := Int, "kept" := Bool) ())
    wiresNamed "size" chart `shouldBe` 1

-- | The chart of the top page of the program, as lines.
chartOf :: Diagram a b -> IO [String]
chartOf program = do
  found <- pages program
  pure $ case found of
    page : _ -> lines (pageMermaid page)
    [] -> []

-- | A step that is a node of the chart of its own, of the name.
working :: Diagram Int ()
working = step "working" (const (pure ()))

-- | The step after it.
afterwards :: Diagram (Maybe ()) ()
afterwards = step "afterwards" (const (pure ()))

-- | The edges of the chart, from a node to a node, whatever their labels.
edgesOf :: [String] -> [(String, String)]
edgesOf chart = [(takeWhile isAlphaNum line, last (words line)) | line <- stripped chart, " --> " `isInfixOf` line || " -->|" `isInfixOf` line]

-- | The nodes that an edge of the label leaves.
sourcesOf :: String -> [String] -> [String]
sourcesOf label chart = [takeWhile isAlphaNum line | line <- stripped chart, ("-->|\"" ++ label ++ "\"|") `isInfixOf` line]

-- | The nodes that an edge of the label goes to.
targetsOf :: String -> [String] -> [String]
targetsOf label chart = [last (words line) | line <- stripped chart, ("-->|\"" ++ label ++ "\"|") `isInfixOf` line]

-- | The nodes that are declared with the text, as a step of its own.
nodesDeclared :: String -> [String] -> [String]
nodesDeclared text chart = [takeWhile isAlphaNum line | line <- stripped chart, ("[\"" ++ text ++ "\"]") `isInfixOf` line]

-- | Whether the edges lead from the node to the target, in one step or more.
reaches :: [String] -> String -> String -> Bool
reaches chart from target = go [] [from]
  where
    edges = edgesOf chart
    go _ [] = False
    go seen (node : rest)
      | node `elem` seen = go seen rest
      | otherwise = let onward = [to | (source, to) <- edges, source == node] in target `elem` onward || go (node : seen) (onward ++ rest)

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
