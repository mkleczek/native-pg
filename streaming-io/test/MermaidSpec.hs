{-# LANGUAGE DataKinds #-}

-- | The charts of "Network.Streaming.Diagram.Mermaid": small programs, built at the diagram arrow, whose charts have what
-- they should.
module MermaidSpec (spec) where

import Control.Arrow ((&&&), (>>>), (^>>))
import Data.Char (isAlphaNum)
import Data.List (isInfixOf, isPrefixOf)
import Network.Streaming.Classes
import Network.Streaming.Combinators (box, done, withInbox)
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.Diagram.Mermaid (Page (..), pages)
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

spec :: Spec
spec = describe "the chart of an inbox" $ do
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

  xfail "withinIn is not there yet" $ it "has a group for a deadline that the input gives, which says so" $ do
    chart <- chartOf (withinIn (quick >>> quick) :: Diagram (Duration, ()) (Maybe ()))
    any ("within the duration of the input" `isInfixOf`) chart `shouldBe` True

  it "has no store, and no edge of a message, for a program with no inbox" $ do
    chart <- chartOf (quick >>> quick :: Diagram () ())
    (storeIn chart, any ("-.->" `isInfixOf`) (stripped chart)) `shouldBe` (Nothing, False)

-- | The chart of the top page of the program, as lines.
chartOf :: Diagram a b -> IO [String]
chartOf program = do
  found <- pages program
  pure $ case found of
    page : _ -> lines (pageMermaid page)
    [] -> []

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
