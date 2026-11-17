{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExplicitNamespaces #-}

-- | The DOT of "Network.Streaming.Diagram.Graphviz": small programs, built at the diagram arrow, whose graphs have the pins and
-- the edges between pins that they should. A node is read from its line, which has its class, and the cells of its pins,
-- which have the ports that the edges name.
module GraphvizSpec (spec) where

import Control.Arrow ((&&&), (>>>), (^>>), (|||))
import Data.Char (isAlphaNum)
import Data.List (isInfixOf, isPrefixOf, sort, stripPrefix)
import Data.Maybe (fromMaybe)
import GHC.Records (HasField, getField)
import Network.Streaming.Classes
import Network.Streaming.Combinators (box, done, step, tap, withInbox)
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (..))
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.Diagram.Graph (Sheet (..), sheets)
import Network.Streaming.Diagram.Graphviz (dot)
import Network.Streaming.Named (inj, newRecord, (:=), (?>))
import Network.Streaming.Plumbing (each)
import Network.Streaming.Wiring (choice, result, type (=:), type (@:), (-<), (<:))
import Skeletest

-- | A step that never waits.
quick :: (Effect arr) => arr a ()
quick = now (const (pure ()))

spec :: Spec
spec = describe "the DOT of a graph" $ do
  it "has a node for a step, with a pin for each port that reads a name and one for each name that it gives, and an edge from each name to the ports that read it" $ do
    found <- dotOf fanning
    -- The name "i" of the input is read by the first step and by the last, so it leaves the input twice.
    edgesNamed found
      `shouldBe` sort
        [ ("flow input", "i", "alpha", "i", ""),
          ("flow input", "i", "gamma", "i", ""),
          ("alpha", "a", "beta", "a", ""),
          ("beta", "b", "gamma", "b", ""),
          ("gamma", "c", "flow result", "c", ""),
          ("flow result", "", "end", "", "")
        ]
    pinsOf "gamma" found `shouldBe` ["i_i", "i_b", "o_c"]
    pinsOf "flow input" found `shouldBe` ["o_i"]
    pinsOf "flow result" found `shouldBe` ["i_c"]

  it "has the edges of a flow that follows one node from that node, which has the names of the input for its pins" $ do
    found <- dotOf (tap quick >>> fanning)
    edgesNamed found
      `shouldBe` sort
        [ ("start", "", "IO", "", ""),
          ("IO", "i", "alpha", "i", ""),
          ("IO", "i", "gamma", "i", ""),
          ("alpha", "a", "beta", "a", ""),
          ("beta", "b", "gamma", "b", ""),
          ("gamma", "c", "flow result", "c", ""),
          ("flow result", "", "end", "", "")
        ]

  it "has the output pins of a name that is kept and taken apart nested as its pattern nests, each part with the field it is for, and no pin for a part that is dropped" $ do
    found <- dotOf nestedNames
    pinsOf "accept" found `shouldBe` ["i_i", "i_j", "o_conn", "o_conn_fd", "o_conn_addr", "o_conn_peer"]
    -- A part that is taken apart and not kept is titled with its field, in a cell that holds the pins of its parts.
    any (\line -> "accept" `isInfixOf` line && "POINT-SIZE=\"8\">socket<" `isInfixOf` line) found `shouldBe` True

  it "has the pins of a list of names side by side with no title, a pin for a name under another name, and none for a field that the list leaves out" $ do
    found <- dotOf dividing
    pinsOf "divide" found `shouldBe` ["i_i", "i_j", "o_q", "o_r"]
    any (\line -> "divide" `isInfixOf` line && "POINT-SIZE=\"8\"" `isInfixOf` line) found `shouldBe` False
    renamed' <- dotOf renaming
    pinsOf "make" renamed' `shouldBe` ["i_i", "i_j", "o_left", "o_z"]

  it "has the edges from the exact pin that a step reads: a part, a whole, a part of a whole by a path, and the port that is another name is the label of its pin" $ do
    found <- dotOf nestedNames
    edgesNamed found
      `shouldBe` sort
        [ ("flow input", "i", "accept", "i", ""),
          ("flow input", "j", "accept", "j", ""),
          ("accept", "conn_fd", "tune", "n", ""),
          ("accept", "conn_peer", "serve", "a", ""),
          ("accept", "conn_peer", "serve", "b", ""),
          ("accept", "conn_addr", "serve", "c", ""),
          ("tune", "tuned", "serve", "d", ""),
          ("serve", "c", "flow result", "c", ""),
          ("flow result", "", "end", "", "")
        ]
    -- An input read under another port is an input pin of that port.
    ported <- dotOf ports
    edgesNamed ported
      `shouldBe` sort
        [ ("flow input", "i", "pair", "i", ""),
          ("flow input", "j", "sum", "k", ""),
          ("flow input", "j", "sum", "j", ""),
          ("pair", "first", "sum", "n", "x"),
          ("pair", "first", "sum", "m", "y"),
          ("sum", "last", "flow result", "last", ""),
          ("flow result", "", "end", "", "")
        ]

  it "has a decision with a pin for each case, in yellow, which holds the pins of the pattern of the case: a name, a list, a name that is kept and taken apart, and none" $ do
    found <- dotOf choosing
    pinsOf "how big?" found
      `shouldBe` [ "i_size",
                   "o_small",
                   "o_small_s",
                   "o_big",
                   "o_big_x",
                   "o_big_y",
                   "o_huge",
                   "o_huge_h",
                   "o_huge_h_lo",
                   "o_huge_h_hi",
                   "o_none"
                 ]
    any (\line -> "how big?" `isInfixOf` line && "#fff8c5" `isInfixOf` line) found `shouldBe` True
    edgesNamed found
      `shouldBe` sort
        [ ("flow input", "i", "size up", "i", ""),
          ("size up", "size", "how big?", "size", ""),
          ("how big?", "small_s", "take", "n", ""),
          ("how big?", "big_x", "add", "n", ""),
          ("how big?", "big_y", "add", "m", ""),
          ("how big?", "huge_h_lo", "span", "n", ""),
          ("how big?", "huge_h_hi", "span", "m", ""),
          ("flow input", "j", "span", "j", ""),
          -- A case that no step reads a pin of has an edge to its branch, and the other cases have none.
          ("how big?", "none", "zero", "", ""),
          ("take", "picked", "flow result", "picked", ""),
          ("add", "picked", "flow result", "picked", ""),
          ("span", "picked", "flow result", "picked", ""),
          ("zero", "picked", "flow result", "picked", ""),
          ("flow result", "", "IO", "", ""),
          ("IO", "", "end", "", "")
        ]

  it "has the cases of a decision that no flow makes as pins too, each of the name that the edge to its branch had" $ do
    found <- dotOf choosingOut
    pinsOf "choose" found `shouldBe` ["o_yes", "o_no"]
    edgesNamed found
      `shouldBe` sort
        [ ("start", "", "choose", "", ""),
          ("choose", "yes", "first", "", ""),
          ("choose", "no", "second", "", ""),
          ("first", "", "IO", "", ""),
          ("second", "", "IO", "", ""),
          ("IO", "", "end", "", "")
        ]

  it "has a dotted edge from the step before to a step that binds nothing from it, and none to a step that does" $ do
    found <- dotOf quiet
    orderEdges found `shouldBe` [("note", "beta")]
    -- The edge into "note" is of the name that it reads, and has no label.
    ("alpha", "a", "note", "n", "") `elem` edgesNamed found `shouldBe` True

  it "has the name that |- gives the wire into an arrow for an input pin of it, and the name that -| gives its wire out for an output pin" $ do
    found <- dotOf (("in" |- step "working" (const (pure ())) -| "out") >>> quick :: Diagram Int ())
    edgesNamed found
      `shouldBe` sort
        [ ("start", "", "working", "in", ""),
          ("working", "out", "IO", "", ""),
          ("IO", "", "end", "", "")
        ]

  it "has the box of a name that has a page of its own link to the page, and a border that a step that is an action has not" $ do
    found <- dotOf (quick >>> box "outer" (quick >>> quick) :: Diagram () ())
    let outer = [line | line <- found, "class=\"box\"" `isInfixOf` line]
    length outer `shouldBe` 1
    any ("HREF=\"#outer\"" `isInfixOf`) outer `shouldBe` True
    any ("BORDER=\"2\"" `isInfixOf`) outer `shouldBe` True

  it "has a back edge for a loop, which goes round to a junction" $ do
    found <- dotOf (each working :: Diagram [Int] [()])
    let ids = [node | node <- nodeIds found, "working" `elem` [name | (n, name) <- nodesNamed found, n == node]]
    case ids of
      [worked] -> reaches found worked worked `shouldBe` True
      other -> failTest ("the graph has the steps " <> show other)

  it "has a cluster for a scope, which holds its steps and the store of its inbox, and a cluster for a group and one for a spawn" $ do
    scoped <- dotOf (owning (beside (poster, collector)))
    case clusterTitled "scope" scoped of
      Nothing -> failTest "the graph has no cluster for the scope"
      Just inside -> any ("class=\"store\"" `isInfixOf`) inside `shouldBe` True
    grouped <- dotOf (group AwaitAll spawnsWorking :: Diagram Int ((), ()))
    case (clusterTitled "group" grouped, clusterTitled "spawn" grouped) of
      (Just inside, Just spawned) -> do
        any ("collected" `isInfixOf`) inside `shouldBe` True
        any ("working" `isInfixOf`) spawned `shouldBe` True
      found -> failTest ("the graph has the clusters " <> show (fmap length <$> [fst found, snd found]))

  it "has a cluster for a fast lane, which holds its steps" $ do
    found <- dotOf (fastLane (box "working" quick) :: Diagram () ())
    case clusterTitled "fast lane" found of
      Nothing -> failTest "the graph has no cluster for the fast lane"
      Just inside -> any ("working" `isInfixOf`) inside `shouldBe` True

  it "has the edges of the messages of an inbox in the style of a message, from the post to the store and from the store to the collect" $ do
    found <- dotOf (owning (beside (poster, collector)))
    messagesOf found `shouldBe` sort [("post \x2709 jobs", "jobs"), ("jobs", "collect \x2709 jobs")]

  it "has no node for a name that Mermaid draws as a node, which is a pin" $ do
    found <- dotOf nestedNames
    [name | (_, name) <- nodesNamed found, name `elem` ["fd", "addr", "peer", "conn", "socket"]] `shouldBe` []

  it "has the same graph for a flow with the plumbing as without it" $ do
    found <- sheets fanning
    case found of
      sheet : _ -> dot (sheetPlumbing sheet) `shouldBe` dot (sheetFlow sheet)
      [] -> failTest "the program has no page"

  it "has the names of a flow that is a step of another flow apart from the names of the other flow" $ do
    found <- dotOf nesting
    -- The inner flow reads its own "n", which the outer step binds: it is read from the pin of "first" by the input of the inner
    -- flow, which is a node of its own, and the step "tail" reads it from there.
    edgesNamed found
      `shouldBe` sort
        [ ("flow input", "i", "first", "i", ""),
          ("first", "n", "flow input", "", ""),
          ("flow input", "n", "tail", "n", ""),
          ("tail", "m", "flow result", "m", ""),
          ("tail", "t", "flow result", "t", ""),
          ("flow result", "", "end", "", "")
        ]

-- | A flow of three steps over two inputs, where the first and the last read the same name of the input, and the one between
-- them reads the first.
fanning :: Diagram ("i" := Int, "j" := Int) Int
fanning =
  result
    "c"
    ( "a" <: step "alpha" (\r -> pure (getField @"i" r + 1)) -< ["i"]
        >>> "b" <: step "beta" (\r -> pure (getField @"a" r + 1)) -< ["a"]
        >>> "c" <: step "gamma" (\r -> pure (getField @"i" r + getField @"b" r)) -< ["i", "b"]
    )

-- | A flow whose last step binds the fields of the output of the first by a path, an input under another port, and the
-- same input under its own.
ports :: Diagram ("i" := Int, "j" := Int) Int
ports =
  result
    "last"
    ( "first" <: step "pair" (\r -> pure (newRecord ["x", "y"] (getField @"i" r) (getField @"i" r + 1))) -< ["i"]
        >>> "last"
          <: step "sum" (\r -> pure (getField @"n" r + getField @"m" r + getField @"k" r + getField @"j" r))
          -< ["n" =: "first.x", "m" =: "first.y", "k" =: "j", "j"]
    )

-- | A flow where the step that gives nothing reads the step before it, and the step after it reads nothing of it.
quiet :: Diagram ("i" := Int, "j" := Int) Int
quiet =
  result
    "b"
    ( "a" <: step "alpha" (\r -> pure (getField @"i" r + 1)) -< ["i"]
        >>> step "note" (\r -> pure (getField @"n" r `seq` ())) -< ["n" =: "a"]
        >>> "b" <: step "beta" (\r -> pure (getField @"j" r + 1)) -< ["j"]
    )

-- | A flow whose first step gives a pair that a list of names takes apart.
dividing :: Diagram ("i" := Int, "j" := Int) Int
dividing =
  result
    "c"
    ( ["q", "r"] <: step "divide" (\r -> pure (getField @"i" r `div` getField @"j" r, getField @"i" r `mod` getField @"j" r)) -< ["i", "j"]
        >>> "s" <: step "plus" (\r -> pure (getField @"n" r + 1)) -< ["n" =: "q"]
        >>> "c" <: step "mix" (\r -> pure (getField @"q" r + getField @"r" r + getField @"s" r)) -< ["q", "r", "s"]
    )

-- | A flow whose first step gives a record of three fields, of which a list keeps two, one under another name.
renaming :: Diagram ("i" := Int, "j" := Int) Int
renaming =
  result
    "c"
    ( ["x" =: "left", "z"] <: step "make" (\r -> pure (newRecord ["x", "y", "z"] (getField @"i" r) (0 :: Int) (getField @"j" r))) -< ["i", "j"]
        >>> "c" <: step "use" (\r -> pure (getField @"left" r + getField @"z" r)) -< ["left", "z"]
    )

-- | A flow whose first step gives a record that a name keeps whole and takes apart, a field of it apart again, and the steps
-- after it read a part, a whole by a path, and a name of a step.
nestedNames :: Diagram ("i" := Int, "j" := Int) Int
nestedNames =
  result
    "c"
    ( "conn" @: ["socket" =: ["fd", "addr"], "peer"]
        <: step "accept" (\r -> pure (newRecord ["socket", "peer"] (newRecord ["fd", "addr"] (getField @"i" r) (0 :: Int)) (getField @"j" r))) -< ["i", "j"]
        >>> "tuned" <: step "tune" (\r -> pure (getField @"n" r + 1)) -< ["n" =: "fd"]
        >>> "c"
          <: step "serve" (\r -> pure (getField @"a" r + getField @"b" r + getField @"c" r + getField @"d" r))
          -< ["a" =: "conn.peer", "b" =: "peer", "c" =: "addr", "d" =: "tuned"]
    )

-- | A flow whose last line is a choice of four cases, which has a name: the branches take the value of their case apart by a
-- name, by a list for a pair, by a name that is kept and takes a record apart, and not at all.
choosing :: Diagram ("i" := Int, "j" := Int) ()
choosing =
  result
    "picked"
    ( "size" <: step "size up" (\r -> pure (sizeOf (getField @"i" r))) -< ["i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" =: "s" ?> step "take" (\r -> pure (getField @"n" r)) -< ["n" =: "s"]
                ||| "big" =: ["x", "y"] ?> step "add" (\r -> pure (getField @"n" r + getField @"m" r)) -< ["n" =: "x", "m" =: "y"]
                ||| "huge" =: "h" @: ["lo", "hi"]
                  ?> step "span" (\r -> pure (getField @"n" r + getField @"m" r + getField @"j" r))
                  -< ["n" =: "lo", "m" =: "h.hi", "j"]
                ||| "none" ?> step "zero" (const (pure 0)) -< []
            )
    )
    >>> quick
  where
    sizeOf :: Int -> Either ("small" := Int) (Either ("big" := (Int, Int)) (Either ("huge" := ("lo" := Int, "hi" := Int)) ("none" := ())))
    sizeOf n
      | n < 10 = inj "small" n
      | n < 100 = inj "big" (n `div` 10, n `mod` 10)
      | n < 1000 = inj "huge" (newRecord ["lo", "hi"] (n `div` 100) (n `mod` 100))
      | otherwise = inj "none" ()

-- | A choice that no flow makes, whose branches are named by what goes into them.
choosingOut :: Diagram (Either Int Int) ()
choosingOut = ("yes" |- step "first" (const (pure ())) ||| "no" |- step "second" (const (pure ()))) >>> quick

-- | A flow whose second step is a flow of its own, which reads the port "n" of its step.
nesting :: Diagram ("i" := Int, "j" := Int) Int
nesting =
  result
    "m"
    ( "n" <: step "first" (\r -> pure (getField @"i" r + 1)) -< ["i"]
        >>> "m" <: inner -< ["n"]
    )

-- | A flow over any record that has the name "n", whose step reads it.
inner :: (HasField "n" r Int) => Diagram r Int
inner = result "t" ("t" <: step "tail" (\r -> pure (getField @"n" r + 1)) -< ["n"])

-- | A step that is a node of the graph of its own, of the name.
working :: Diagram Int ()
working = step "working" (const (pure ()))

-- | A body that collects, and spawns the part that works.
spawnsWorking :: Diagram (Group Diagram s AwaitAll, Int) ()
spawnsWorking = tap (snd ^>> step "collected" (const (pure ()))) >>> spawn working

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

-- | The DOT of the graph of the top page of the program, as lines.
dotOf :: Diagram a b -> IO [String]
dotOf program = do
  found <- sheets program
  pure $ case found of
    sheet : _ -> lines (dot (sheetFlow sheet))
    [] -> []

-- | The lines without their indent.
stripped :: [String] -> [String]
stripped = fmap (dropWhile (== ' '))

-- | The lines that are nodes, each with its node: the lines that have a class in the brackets that follow the node.
nodeLines :: [String] -> [(String, String)]
nodeLines found = [(takeWhile isAlphaNum line, line) | line <- stripped found, (takeWhile isAlphaNum line ++ " [class=\"") `isPrefixOf` line]

-- | The nodes of the graph.
nodeIds :: [String] -> [String]
nodeIds = fmap fst . nodeLines

-- | What a node is called: the first line of its body, without the gear, or its class if it has no body.
nodesNamed :: [String] -> [(String, String)]
nodesNamed found = [(node, nameOfNode line) | (node, line) <- nodeLines found]

-- | The name of a node, from its line: the text of its body, if it has one, and else its class, which is "start", "end",
-- "junction", or the label of a store.
nameOfNode :: String -> String
nameOfNode line = case breakAfter "POINT-SIZE=\"12\">" line of
  Just rest -> withoutGear (fst (breakText "</FONT>" rest))
  Nothing -> case breakAfter "class=\"store\"" line of
    Just _ -> maybe "store" (fst . breakText "\"") (breakAfter "label=\"" line)
    Nothing -> maybe "" (fst . breakText "\"") (breakAfter "class=\"" line)
  where
    withoutGear text = fromMaybe text (stripPrefix "\x2699\xFE0E " text)

-- | What is after the first place that the marker is, if it is there.
breakAfter :: String -> String -> Maybe String
breakAfter marker text = case breakText marker text of
  (_, []) -> Nothing
  (_, rest) -> Just (drop (length marker) rest)

-- | The text before the first place where the marker is, and what is from it on.
breakText :: String -> String -> (String, String)
breakText marker = go []
  where
    go before rest
      | marker `isPrefixOf` rest = (reverse before, rest)
      | otherwise = case rest of
          [] -> (reverse before, [])
          c : more -> go (c : before) more

-- | The ports of the cells of a node, in the order that they are written, but not the body.
pinsOf :: String -> [String] -> [String]
pinsOf name found = case [line | (node, line) <- nodeLines found, any ((== name) . snd) [(node, nameOfNode line)]] of
  line : _ -> ports' line
  [] -> []
  where
    ports' line = case breakAfter "PORT=\"" line of
      Nothing -> []
      Just rest ->
        let (port, more) = breakText "\"" rest
         in [port | port /= "body"] ++ ports' (drop 1 more)

-- | An edge: the node and the port that it leaves, the node and the port that it enters, and the rest of the line.
data Seen = Seen String String String String String

-- | The edges of the graph, as the lines that have an arrow.
edgesSeen :: [String] -> [Seen]
edgesSeen found = [seen | line <- stripped found, Just seen <- [seenOf line]]
  where
    seenOf line = case breakText " -> " line of
      (_, []) -> Nothing
      (from, rest) ->
        let (to, attrs) = breakText " [" (takeWhile (/= ';') (drop 4 rest))
            (fromNode, fromPort) = split from
            (toNode, toPort) = split to
         in Just (Seen fromNode fromPort toNode toPort attrs)
    split end' = case breakText ":" end' of
      (node, []) -> (node, "")
      (node, rest) -> (node, takeWhile (/= ':') (drop 1 rest))

-- | The edges of the graph, each as the names of its two nodes and the pins that it joins, which are named without the mark of
-- an input and of an output, and its label, which is empty if it has none.
edgesNamed :: [String] -> [(String, String, String, String, String)]
edgesNamed found =
  sort
    [ (nameOf from, pin "o_" fromPort, nameOf to, pin "i_" toPort, labelOf attrs)
      | Seen from fromPort to toPort attrs <- edgesSeen found
    ]
  where
    names = nodesNamed found
    nameOf node = fromMaybe node (lookup node names)
    pin mark port = fromMaybe port (stripPrefix mark port)
    labelOf attrs = maybe "" (fst . breakText "\"") (breakAfter "label=\"" attrs)

-- | The edges that are dotted, each as the names of its two nodes.
orderEdges :: [String] -> [(String, String)]
orderEdges found =
  sort
    [ (nameOf from, nameOf to)
      | Seen from _ to _ attrs <- edgesSeen found,
        "style=dotted" `isInfixOf` attrs
    ]
  where
    names = nodesNamed found
    nameOf node = fromMaybe node (lookup node names)

-- | The edges that are in the style of a message, which is orange, each as the names of its two nodes.
messagesOf :: [String] -> [(String, String)]
messagesOf found =
  sort
    [ (nameOf from, nameOf to)
      | Seen from _ to _ attrs <- edgesSeen found,
        "color=\"#d08c3c\"" `isInfixOf` attrs
    ]
  where
    names = nodesNamed found
    nameOf node = fromMaybe node (lookup node names)

-- | Whether the edges lead from the node to the target, in one step or more.
reaches :: [String] -> String -> String -> Bool
reaches found from target = go [] [from]
  where
    edges = [(a, b) | Seen a _ b _ _ <- edgesSeen found]
    go _ [] = False
    go seen (node : rest)
      | node `elem` seen = go seen rest
      | otherwise =
          let onward = [to | (source, to) <- edges, source == node]
           in target `elem` onward || go (node : seen) (onward ++ rest)

-- | The lines inside the cluster of the title, if the graph has one: the lines that are indented further than its first line,
-- up to its end.
clusterTitled :: String -> [String] -> Maybe [String]
clusterTitled title found = go found
  where
    go (opening : labelled : rest)
      | "subgraph " `isPrefixOf` dropWhile (== ' ') opening && ("label=\"" ++ title ++ "\"") `isPrefixOf` dropWhile (== ' ') labelled =
          Just (takeWhile (\line -> indentOf line > indentOf opening) rest)
    go (_ : rest) = go rest
    go [] = Nothing
    indentOf = length . takeWhile (== ' ')
