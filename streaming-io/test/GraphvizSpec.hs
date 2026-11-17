{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExplicitNamespaces #-}

-- | The DOT of "Network.Streaming.Diagram.Graphviz": small programs, built at the diagram arrow, whose graphs have the clusters,
-- the pins and the edges between pins that they should. A step is a faint wrapper cluster that holds its body, which is the box
-- of the step, and a node for each of its pins outside the box, and a name that is taken apart is a cluster in it, so a graph is
-- read from the lines of its nodes, which have their class, and from the lines of its clusters, which have what is inside them
-- indented. A flow is a cluster that is a box, which holds a group of its input ports above its steps and a group of its output
-- ports under them, and a diamond is where the branches of a decision that feed the same pin meet.
module GraphvizSpec (spec) where

import Control.Arrow ((&&&), (>>>), (^>>), (|||))
import Data.Char (isSpace)
import Data.Foldable (for_)
import Data.List (isInfixOf, isPrefixOf, sort, stripPrefix, tails)
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

-- | A case that the printer fails as long as it draws the input and the result of a flow as elements, and not as a box with its ports.
unwritten :: Spec -> Spec
unwritten = xfail "the DOT of a graph has a flow as a box with its ports, and a diamond where branches rejoin, only when the printer draws them so"

spec :: Spec
spec = describe "the DOT of a graph" $ do
  unwritten $ it "has a cluster for a step, which holds its body and a node for each port that reads a name and for each name that it gives, and an edge from each name to the ports that read it" $ do
    found <- dotOf fanning
    -- The name "i" of the input is read by the first step and by the last, so it leaves the input port twice.
    edgesNamed found
      `shouldBe` sort
        [ ("ports in", "i", "alpha", "i", ""),
          ("ports in", "i", "gamma", "i", ""),
          ("alpha", "a", "beta", "a", ""),
          ("beta", "b", "gamma", "b", ""),
          ("gamma", "c", "ports out", "c", ""),
          ("ports out", "c", "end", "", "")
        ]
    pinsOf "in" "gamma" found `shouldBe` ["i", "b"]
    pinsOf "out" "gamma" found `shouldBe` ["c"]

  unwritten $ it "has a flow as a cluster that is a box with the background of the canvas and a light border, which holds a group of its input ports at the top and a group of its output ports at the bottom, each a faint wrapper with no title that holds a pin for each port" $ do
    found <- dotOf fanning
    case flowsOf found of
      [(attrs, inside)] -> do
        isFlowBox attrs `shouldBe` True
        case portGroupsOf inside of
          [(inAttrs, ins), (outAttrs, outs)] -> do
            isWrapper "#f6f8fa" "#d8dee4" inAttrs `shouldBe` True
            isWrapper "#f6f8fa" "#d8dee4" outAttrs `shouldBe` True
            (labelOf inAttrs, labelOf outAttrs) `shouldBe` ("", "")
            -- The input ports are input pins, and the output ports are output pins.
            ins `shouldBe` [("in", "i")]
            outs `shouldBe` [("out", "c")]
            -- The group of the input ports is the first cluster in the box, and the group of the output ports the last.
            let classes = [clusterClassOf attrs' | (_, attrs', _) <- clusterLines inside]
            (take 1 classes, drop (length classes - 1) classes) `shouldBe` (["ports"], ["ports"])
          other -> failTest ("the box has the groups of ports " <> show (length other))
      other -> failTest ("the graph has the boxes of flows " <> show (length other))

  unwritten $ it "has no element for the input of a flow and none for its result, and no circle for the start of a page that a flow starts: the ports of its box are the entry and the exit" $ do
    found <- dotOf fanning
    [name | (_, line) <- nodeLines found, let name = nameOfNode line, name `elem` ["flow input", "flow result"]] `shouldBe` []
    [line | (_, line) <- nodeLines found, classOf line == "start"] `shouldBe` []
    -- Nothing leaves a start: the first edge is from a port.
    [from | (from, _, _, _, _) <- edgesNamed found, from `elem` ["start", "flow input"]] `shouldBe` []

  unwritten $ it "has an edge from the node that a flow follows to its input ports, which a start circle that comes before it points at that node" $ do
    found <- dotOf (tap quick >>> fanning)
    edgesNamed found
      `shouldBe` sort
        [ ("start", "", "IO", "", ""),
          ("IO", "", "ports in", "i", ""),
          ("ports in", "i", "alpha", "i", ""),
          ("ports in", "i", "gamma", "i", ""),
          ("alpha", "a", "beta", "a", ""),
          ("beta", "b", "gamma", "b", ""),
          ("gamma", "c", "ports out", "c", ""),
          ("ports out", "c", "end", "", "")
        ]

  it "has the output pins of a name that is kept and taken apart in clusters nested as its pattern nests, titled with the name of the whole and the field of a part that is taken apart, and no pin for a part that is dropped" $ do
    found <- dotOf nestedNames
    outPathsOf "accept" found `shouldBe` [["conn", "socket", "fd"], ["conn", "socket", "addr"], ["conn", "peer"]]
    titlesIn "accept" found `shouldBe` ["conn", "socket"]
    -- A list of names that has "_" for a part gives no pin for it.
    dropped <- dotOf dropping
    outPathsOf "triple" dropped `shouldBe` [["x"], ["z"]]

  it "has the pins of a list of names side by side in no cluster, a pin for a name under another name, the pins of a part that no name keeps in no cluster either, and no pin for a field that the list leaves out" $ do
    found <- dotOf dividing
    outPathsOf "divide" found `shouldBe` [["q"], ["r"]]
    titlesIn "divide" found `shouldBe` []
    renamed' <- dotOf renaming
    outPathsOf "make" renamed' `shouldBe` [["left"], ["z"]]
    -- The pins of a part that no name keeps are those of the list itself, and a part that a name keeps is a cluster titled with it.
    positioned' <- dotOf positioned
    titlesIn "pairs" positioned' `shouldBe` ["both"]
    outPathsOf "pairs" positioned' `shouldBe` [["x"], ["y"], ["both", "sum"], ["both", "prod"]]

  unwritten $ it "has the edges from the exact pin that a step reads: a part, a whole, a part of a whole by a path, and the port that is another name is the label of its pin" $ do
    found <- dotOf nestedNames
    edgesNamed found
      `shouldBe` sort
        [ ("ports in", "i", "accept", "i", ""),
          ("ports in", "j", "accept", "j", ""),
          ("accept", "fd", "tune", "n", ""),
          ("accept", "peer", "serve", "a", ""),
          ("accept", "peer", "serve", "b", ""),
          ("accept", "addr", "serve", "c", ""),
          ("tune", "tuned", "serve", "d", ""),
          ("serve", "c", "ports out", "c", ""),
          ("ports out", "c", "end", "", "")
        ]
    -- An input read under another port is an input pin of that port.
    ported <- dotOf ports
    edgesNamed ported
      `shouldBe` sort
        [ ("ports in", "i", "pair", "i", ""),
          ("ports in", "j", "sum", "k", ""),
          ("ports in", "j", "sum", "j", ""),
          ("pair", "first", "sum", "n", "x"),
          ("pair", "first", "sum", "m", "y"),
          ("sum", "last", "ports out", "last", ""),
          ("ports out", "last", "end", "", "")
        ]

  unwritten $ it "has a decision as a cluster with a yellow node for a case, or a yellow cluster for a case that holds the pins of its pattern: a name, a list, a name that is kept and taken apart, and none" $ do
    found <- dotOf choosing
    pinsOf "in" "how big?" found `shouldBe` ["size"]
    outPathsOf "how big?" found
      `shouldBe` [["small", "s"], ["big", "x"], ["big", "y"], ["huge", "h", "lo"], ["huge", "h", "hi"], ["none"]]
    titlesIn "how big?" found `shouldBe` ["small", "big", "huge", "h"]
    -- The cases are yellow, and so is the case that has no pattern, which is a node: the pins of the patterns are blue.
    yellowIn "how big?" found `shouldBe` ["small", "big", "huge", "none"]
    edgesNamed found
      `shouldBe` sort
        [ ("ports in", "i", "size up", "i", ""),
          ("size up", "size", "how big?", "size", ""),
          ("how big?", "s", "take", "n", ""),
          ("how big?", "x", "add", "n", ""),
          ("how big?", "y", "add", "m", ""),
          ("how big?", "lo", "span", "n", ""),
          ("how big?", "hi", "span", "m", ""),
          ("ports in", "j", "span", "j", ""),
          -- A case that no step reads a pin of has an edge to its branch, and the other cases have none.
          ("how big?", "none", "zero", "", ""),
          -- The four branches give "picked", which the flow gives: their edges end at one diamond, which has an edge to the port.
          ("take", "picked", "merge", "", ""),
          ("add", "picked", "merge", "", ""),
          ("span", "picked", "merge", "", ""),
          ("zero", "picked", "merge", "", ""),
          ("merge", "", "ports out", "picked", ""),
          ("ports out", "picked", "IO", "", ""),
          ("IO", "", "end", "", "")
        ]

  it "has the cases of a decision that no flow makes as yellow nodes too, each of the name that the edge to its branch had" $ do
    found <- dotOf choosingOut
    outPathsOf "choose" found `shouldBe` [["yes"], ["no"]]
    yellowIn "choose" found `shouldBe` ["yes", "no"]
    edgesNamed found
      `shouldBe` sort
        [ ("start", "", "choose", "", ""),
          ("choose", "yes", "first", "", ""),
          ("choose", "no", "second", "", ""),
          ("first", "", "IO", "", ""),
          ("second", "", "IO", "", ""),
          ("IO", "", "end", "", "")
        ]

  unwritten $ it "has a diamond before the pin that two or more branches of a decision feed, which the edges of the branches end at, and one edge from the diamond to the pin, for the input pin of a step and for the output port of a flow" $ do
    found <- dotOf rejoining
    case [line | (_, line) <- nodeLines found, classOf line == "merge"] of
      [line] -> isDiamond line `shouldBe` True
      other -> failTest ("the graph has the diamonds " <> show (length other))
    -- The branches "take" and "add" give "picked", which "finish" reads: the diamond is before its pin "p".
    edgesNamed found
      `shouldBe` sort
        [ ("ports in", "i", "size up", "i", ""),
          ("ports in", "i", "take", "i", ""),
          ("ports in", "i", "add", "i", ""),
          ("size up", "size", "how big?", "size", ""),
          ("how big?", "small", "take", "", ""),
          ("how big?", "big", "add", "", ""),
          ("take", "picked", "merge", "", ""),
          ("add", "picked", "merge", "", ""),
          ("merge", "", "finish", "p", ""),
          ("finish", "r", "ports out", "r", ""),
          ("ports out", "r", "end", "", "")
        ]
    -- The four branches of "choosing" give the name that the flow gives: the diamond is before its output port, and there is no other.
    chosen <- dotOf choosing
    case [line | (_, line) <- nodeLines chosen, classOf line == "merge"] of
      [line] -> isDiamond line `shouldBe` True
      other -> failTest ("the graph has the diamonds " <> show (length other))
    [(from, to) | (from, _, to, pin, _) <- edgesNamed chosen, to == "ports out" || from == "merge", pin == "picked"] `shouldBe` [("merge", "ports out")]

  it "has no diamond where the branches of a decision end at the end circle or at a step that has no pins, nor where one branch feeds a pin, nor in a flow that has no decision" $ do
    for_ [("choosingOut", dotOf choosingOut), ("choosingEnded", dotOf choosingEnded), ("fanning", dotOf fanning), ("nestedNames", dotOf nestedNames), ("ports", dotOf ports), ("nesting", dotOf nesting), ("quiet", dotOf quiet)] $ \(name, drawn) -> do
      found <- drawn
      [(name, node) | (node, line) <- nodeLines found, classOf line == "merge"] `shouldBe` []
    -- The two branches of "choosingEnded" end at the end circle, by an edge each.
    ended <- dotOf choosingEnded
    length [() | (_, _, "end", _, _) <- edgesNamed ended] `shouldBe` 2

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

  it "has the box of a name that has a page of its own link to the page, and a border that a step that is an action has not, as a node, or as the box in a wrapper that links to the page too" $ do
    found <- dotOf (quick >>> box "outer" (quick >>> quick) :: Diagram () ())
    let outer = [line | line <- found, "class=\"box\"" `isInfixOf` line]
    length outer `shouldBe` 1
    any ("href=\"#outer\"" `isInfixOf`) outer `shouldBe` True
    any ("penwidth=2" `isInfixOf`) outer `shouldBe` True
    -- A box that has pins is a heavy box that links, in a wrapper that is as faint as that of an action and links too.
    boxed' <- dotOf boxed
    isBox "#dde3ea" "#24292f" 2 (bodyOf "outer" boxed') `shouldBe` True
    ("href=\"#outer\"" `isInfixOf` bodyOf "outer" boxed') `shouldBe` True
    isWrapper "#f6f8fa" "#d8dee4" (wrapperOf "outer" boxed') `shouldBe` True
    ("href=\"#outer\"" `isInfixOf` wrapperOf "outer" boxed') `shouldBe` True
    -- The action beside it has the thin border and no link.
    isBox "#dde3ea" "#57606a" 1 (bodyOf "after" boxed') `shouldBe` True
    ("href=" `isInfixOf` bodyOf "after" boxed') `shouldBe` False

  it "has the body of a step that has pins as a filled rounded box in the colours of a step, in a cluster that is a faint wrapper around the box and its pins" $ do
    found <- dotOf fanning
    isBox "#dde3ea" "#57606a" 1 (bodyOf "alpha" found) `shouldBe` True
    isWrapper "#f6f8fa" "#d8dee4" (wrapperOf "alpha" found) `shouldBe` True
    -- The pins are nodes of the wrapper that are not the box, so they sit outside it: the box is the node of the body only.
    pinsOf "in" "alpha" found `shouldBe` ["i"]
    ("class=\"in\"" `isInfixOf` bodyOf "alpha" found) `shouldBe` False

  it "has the body of a decision as a yellow box in a wrapper of a paler yellow, which are not the colours of a step" $ do
    found <- dotOf choosing
    isBox "#fff1a8" "#9a6700" 1 (bodyOf "how big?" found) `shouldBe` True
    isWrapper "#fffbea" "#f0e2a8" (wrapperOf "how big?" found) `shouldBe` True

  it "has an edge into or out of a step that ends at a pin or at its box, and none at its wrapper, in a flow whose edges say only the order, whose cases have no pin that is read, and whose result leaves" $ do
    for_ [("quiet", dotOf quiet), ("choosing", dotOf choosing), ("fanning", dotOf fanning), ("boxed", dotOf boxed), ("nesting", dotOf nesting)] $ \(name, drawn) -> do
      found <- drawn
      let nodes = fmap fst (nodeLines found)
          wrappers = [node | (node, attrs, _) <- clusterLines found, clusterClassOf attrs `elem` ["step", "box", "decision", "input", "result"]]
      -- Every edge is drawn between nodes, which are pins or bodies: the box of a step, not a cluster that wraps it.
      [(name, end') | Seen from to _ <- edgesSeen found, end' <- [from, to], end' `notElem` nodes] `shouldBe` []
      -- No edge is clipped at the cluster of a step, which would end it at the wrapper, outside the pins.
      [(name, wrapper) | line <- stripped found, " -> " `isInfixOf` line, wrapper <- wrappers, ("\"cluster_" ++ wrapper ++ "\"") `isInfixOf` line] `shouldBe` []

  it "has a step that has no pins as a rounded box of its own, with no wrapper" $ do
    found <- dotOf (each working :: Diagram [Int] [()])
    case [(node, line) | (node, line) <- nodeLines found, nameOfNode line == "working"] of
      [(node, line)] -> do
        isBox "#dde3ea" "#57606a" 1 line `shouldBe` True
        ("margin=\"0.12,0.07\"" `isInfixOf` line) `shouldBe` True
        [node' | (node', _, _) <- clusterLines found, node' == node] `shouldBe` []
      other -> failTest ("the graph has the steps " <> show other)

  it "has a back edge for a loop, which goes round to a junction" $ do
    found <- dotOf (each working :: Diagram [Int] [()])
    let ids = [node | (node, line) <- nodeLines found, nameOfNode line == "working"]
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

  it "has the edges of the messages of an inbox in the style of a message, from the post to the store and from the store to the collect" $ do
    found <- dotOf (owning (beside (poster, collector)))
    messagesOf found `shouldBe` sort [("post \x2709 jobs", "jobs"), ("jobs", "collect \x2709 jobs")]

  it "has no node for a name that is kept and taken apart, which is the title of a cluster, and none for a selector" $ do
    found <- dotOf nestedNames
    [name | (_, line) <- nodeLines found, let name = nameOfNode line, name `elem` ["conn", "socket"]] `shouldBe` []
    titlesIn "accept" found `shouldBe` ["conn", "socket"]

  it "has the same graph for a flow with the plumbing as without it" $ do
    found <- sheets fanning
    case found of
      sheet : _ -> dot (sheetPlumbing sheet) `shouldBe` dot (sheetFlow sheet)
      [] -> failTest "the program has no page"

  unwritten $ it "has the names of a flow that is a step of another flow apart from the names of the other flow, and its box inside the box of the other with ports of its own" $ do
    found <- dotOf nesting
    -- The inner flow reads its own "n", which the outer step binds: it is read from the pin of "first" by the input port of the inner
    -- flow, which is in its own group, and the step "tail" reads it from there.
    edgesNamed found
      `shouldBe` sort
        [ ("ports in", "i", "first", "i", ""),
          ("first", "n", "ports in", "n", ""),
          ("ports in", "n", "tail", "n", ""),
          ("tail", "m", "ports out", "m", ""),
          ("tail", "t", "ports out", "t", ""),
          ("ports out", "m", "end", "", "")
        ]
    case flowsOf found of
      [(outerAttrs, outerLines), (innerAttrs, innerLines)] -> do
        isFlowBox outerAttrs `shouldBe` True
        isFlowBox innerAttrs `shouldBe` True
        -- The box of the inner flow is inside the box of the outer flow, and each has its own two groups.
        (innerAttrs `elem` outerLines) `shouldBe` True
        [kinds | (_, kinds) <- portGroupsOf innerLines] `shouldBe` [[("in", "n")], [("out", "t")]]
        [kinds | (_, kinds) <- portGroupsOf outerLines, kinds `notElem` [kinds' | (_, kinds') <- portGroupsOf innerLines]] `shouldBe` [[("in", "i")], [("out", "m")]]
      other -> failTest ("the graph has the boxes of flows " <> show (length other))

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

-- | A flow whose first step gives three values, of which a list keeps the first and the last.
dropping :: Diagram ("i" := Int, "j" := Int) Int
dropping =
  result
    "c"
    ( ["x", "_", "z"] <: step "triple" (\r -> pure (getField @"i" r, (getField @"j" r, getField @"i" r + getField @"j" r))) -< ["i", "j"]
        >>> "c" <: step "use" (\r -> pure (getField @"x" r + getField @"z" r)) -< ["x", "z"]
    )

-- | A flow whose first step gives a pair of pairs, which a list takes apart by position: no name keeps the first pair, and the
-- second is kept under a name.
positioned :: Diagram ("i" := Int, "j" := Int) Int
positioned =
  result
    "c"
    ( ["_" @: ["x", "y"], "both" @: ["sum", "prod"]]
        <: step "pairs" (\r -> pure ((getField @"i" r, getField @"j" r), (getField @"i" r + getField @"j" r, getField @"i" r * getField @"j" r))) -< ["i", "j"]
        >>> "c" <: step "use" (\r -> pure (getField @"x" r + getField @"sum" r)) -< ["x", "sum"]
    )

-- | A flow whose first step is a box that has a page of its own, with the action that reads its output after it.
boxed :: Diagram ("i" := Int, "j" := Int) Int
boxed =
  result
    "r"
    ( "a" <: box "outer" (step "inside" (\r -> pure (getField @"i" r + 1))) -< ["i"]
        >>> "r" <: step "after" (\r -> pure (getField @"a" r + 1)) -< ["a"]
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

-- | A flow whose choice gives a name by each of its two branches, which a step after the choice reads.
rejoining :: Diagram ("i" := Int, "j" := Int) Int
rejoining =
  result
    "r"
    ( "size" <: step "size up" (\r -> pure (sizeOf (getField @"i" r))) -< ["i"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" ?> step "take" (\r -> pure (getField @"i" r)) -< ["i"]
                ||| "big" ?> step "add" (\r -> pure (getField @"i" r + 1)) -< ["i"]
            )
        >>> "r" <: step "finish" (\r -> pure (getField @"p" r + 1)) -< ["p" =: "picked"]
    )
  where
    sizeOf :: Int -> Either ("small" := ()) ("big" := ())
    sizeOf n
      | n < 10 = inj "small" ()
      | otherwise = inj "big" ()

-- | A choice that no flow makes, whose branches are named by what goes into them.
choosingOut :: Diagram (Either Int Int) ()
choosingOut = ("yes" |- step "first" (const (pure ())) ||| "no" |- step "second" (const (pure ()))) >>> quick

-- | A choice that no flow makes, whose branches end the page.
choosingEnded :: Diagram (Either Int Int) ()
choosingEnded = "yes" |- step "first" (const (pure ())) ||| "no" |- step "second" (const (pure ()))

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

-- | How far a line is indented.
indentOf :: String -> Int
indentOf = length . takeWhile (== ' ')

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

-- | The lines that are nodes, each with its node: the lines that have a class in the brackets that follow the node.
nodeLines :: [String] -> [(String, String)]
nodeLines found = [(node, line) | line <- stripped found, let node = takeWhile (not . isSpace) line, (node ++ " [class=\"") `isPrefixOf` line]

-- | The class of a node, from its line.
classOf :: String -> String
classOf line = maybe "" (fst . breakText "\"") (breakAfter "[class=\"" line)

-- | The class of a cluster, from the line of its attributes.
clusterClassOf :: String -> String
clusterClassOf = maybe "" (fst . breakText "\"") . breakAfter "class=\""

-- | Whether the line is of a pin: a node of the class of an input pin, or of an output pin.
isPin :: String -> Bool
isPin line = classOf line `elem` ["in", "out"]

-- | The label of a node or of a cluster, when it is a text in quotes.
labelOf :: String -> String
labelOf = maybe "" (fst . breakText "\"") . breakAfter "label=\""

-- | What a node is called: the first line of its body, without the gear, the label of a pin or of a store, or its class if it has
-- none, which is "start", "end", "junction" or "bar".
nameOfNode :: String -> String
nameOfNode line
  | isPin line = labelOf line
  | otherwise = case breakAfter "POINT-SIZE=\"12\">" line of
      Just rest -> withoutGear (fst (breakText "</FONT>" rest))
      Nothing | classOf line == "store" -> labelOf line
      Nothing -> classOf line
  where
    withoutGear text = fromMaybe text (stripPrefix "\x2699\xFE0E " text)

-- | The clusters of the graph, in order, those in clusters too: each with its node, the line of its attributes, and the lines in
-- it, which are those that are indented further than its first line.
clusterLines :: [String] -> [(String, String, [String])]
clusterLines found =
  [ (takeWhile (/= ' ') node, attrs, inside)
    | opening : more@(attrs : _) <- tails found,
      Just node <- [stripPrefix "subgraph cluster_" (dropWhile (== ' ') opening)],
      let inside = takeWhile (\line -> indentOf line > indentOf opening) more
  ]

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

-- | The lines of the cluster of a step: the step is the node of the name that is not a pin, and its cluster is of its node.
stepLines :: String -> [String] -> [String]
stepLines name found = case [node | (node, line) <- nodeLines found, not (isPin line), nameOfNode line == name] of
  node : _ -> concat (take 1 [inside | (node', _, inside) <- clusterLines found, node' == node])
  [] -> []

-- | The line of the body of a step, which is the node of the name that is not a pin, or nothing if there is none.
bodyOf :: String -> [String] -> String
bodyOf name found = concat (take 1 [line | (_, line) <- nodeLines found, not (isPin line), nameOfNode line == name])

-- | The line of the attributes of the cluster of a step, which is the cluster of its body, or nothing if it has none.
wrapperOf :: String -> [String] -> String
wrapperOf name found = case [node | (node, line) <- nodeLines found, not (isPin line), nameOfNode line == name] of
  node : _ -> concat (take 1 [attrs | (node', attrs, _) <- clusterLines found, node' == node])
  [] -> ""

-- | Whether the line of a body is a filled rounded box with the fill, the colour of the border and the width of it.
isBox :: String -> String -> Int -> String -> Bool
isBox fill line' width body =
  all
    (`isInfixOf` body)
    ["shape=box", "style=\"rounded,filled\"", ", fillcolor=\"" ++ fill ++ "\"", ", color=\"" ++ line' ++ "\"", ", penwidth=" ++ show width ++ ","]

-- | Whether the line of the attributes of a cluster is a wrapper with the fill and the colour of the border: a thin border, and a
-- margin that is close to what is in it.
isWrapper :: String -> String -> String -> Bool
isWrapper fill line' attrs =
  all (`isInfixOf` attrs) ["; fillcolor=\"" ++ fill ++ "\"", "; color=\"" ++ line' ++ "\"", "; penwidth=1;", "; margin=6;"]

-- | The labels of the pins of a step that are of the class, in the order that they are written.
pinsOf :: String -> String -> [String] -> [String]
pinsOf klass name found = [labelOf line | (_, line) <- nodeLines (stepLines name found), classOf line == klass]

-- | The titles of the clusters in the cluster of a step, in order: the name of a whole that is kept and taken apart, the field of a
-- part that is taken apart, and the case of a decision.
titlesIn :: String -> [String] -> [String]
titlesIn name found = [labelOf attrs | (_, attrs, _) <- clusterLines (drop 1 (stepLines name found))]

-- | The output pins of a step, each as the titles of the clusters that hold it, which have a title, and then its own label.
outPathsOf :: String -> [String] -> [[String]]
outPathsOf name found = go [] (drop 1 (stepLines name found))
  where
    go _ [] = []
    go titles (line : rest)
      | Just _ <- stripPrefix "subgraph cluster_" (dropWhile (== ' ') line) =
          let inside = takeWhile (\following -> indentOf following > indentOf line) rest
              title = labelOf (concat (take 1 inside))
           in go (titles ++ [title | not (null title)]) (drop 1 inside) ++ go titles (drop (length inside) rest)
      | (node, pin) <- (takeWhile (not . isSpace) (dropWhile (== ' ') line), dropWhile (== ' ') line),
        (node ++ " [class=\"out\"") `isPrefixOf` pin =
          (titles ++ [labelOf pin]) : go titles rest
      | otherwise = go titles rest

-- | The cases of a decision that are yellow: the clusters of a case that has a pattern and the nodes of a case that has none, in
-- order. The pins of the patterns are blue.
yellowIn :: String -> [String] -> [String]
yellowIn name found = [labelOf line | line <- stripped (drop 1 (stepLines name found)), "#fff8c5" `isInfixOf` line, "label=\"" `isInfixOf` line]

-- | An edge: the node that it leaves, the node that it enters, and the rest of the line.
data Seen = Seen String String String

-- | The edges of the graph that are drawn, as the lines that have an arrow: the edges inside a step that hold its pins to its body
-- are not.
edgesSeen :: [String] -> [Seen]
edgesSeen found = [seen | line <- stripped found, not ("style=invis" `isInfixOf` line), Just seen <- [seenOf line]]
  where
    seenOf line = case breakText " -> " line of
      (_, []) -> Nothing
      (from, rest) ->
        let (to, attrs) = breakText " [" (takeWhile (/= ';') (drop 4 rest))
         in Just (Seen from to attrs)

-- | The edges of the graph, each as the names of its two steps and the pins that it joins, which are the labels of their nodes, and
-- its label, which is empty if it has none. An end that is a node that is not a pin has no pin. A pin that has no step is a port of
-- a flow, which is "ports in" if it is an input pin and "ports out" if it is an output pin, and the diamond is "merge".
edgesNamed :: [String] -> [(String, String, String, String, String)]
edgesNamed found =
  sort
    [ (stepOf from, pinOf from, stepOf to, pinOf to, labelOf' attrs)
      | Seen from to attrs <- edgesSeen found
    ]
  where
    nodes = nodeLines found
    stepOf end' = case lookup (stepNodeOf end') [(node, nameOfNode line) | (node, line) <- nodes, not (isPin line)] of
      Just name -> name
      Nothing -> case lookup end' [(node, classOf line) | (node, line) <- nodes, isPin line] of
        Just "in" -> "ports in"
        Just "out" -> "ports out"
        _ -> end'
    pinOf end' = maybe "" labelOf (lookup end' [(node, line) | (node, line) <- nodes, isPin line])
    labelOf' attrs = maybe "" (fst . breakText "\"") (breakAfter "label=\"" attrs)

-- | The clusters that are the box of a flow, in order, those in boxes too: each with the line of its attributes and the lines in it.
flowsOf :: [String] -> [(String, [String])]
flowsOf found = [(attrs, inside) | (_, attrs, inside) <- clusterLines found, clusterClassOf attrs == "flow"]

-- | The groups of ports in the lines, in order: each with the line of its attributes, and the class and the label of each pin in it.
portGroupsOf :: [String] -> [(String, [(String, String)])]
portGroupsOf found =
  [ (attrs, [(classOf line, labelOf line) | (_, line) <- nodeLines inside])
    | (_, attrs, inside) <- clusterLines found,
      clusterClassOf attrs == "ports"
  ]

-- | Whether the line of the attributes of a cluster is the box of a flow: it has the background of the canvas, and a light border.
isFlowBox :: String -> Bool
isFlowBox attrs = all (`isInfixOf` attrs) ["style=\"filled,rounded\"", "; fillcolor=\"white\"", "; color=\"#afb8c1\"", "; penwidth=1;"]

-- | Whether the line of a node is a diamond that has no label, small and of the colours of a case.
isDiamond :: String -> Bool
isDiamond line =
  all
    (`isInfixOf` line)
    ["shape=diamond", "style=filled", "fillcolor=\"#fff8c5\"", "color=\"#d4a72c\"", "width=0.2", "height=0.2", "fixedsize=true", "label=\"\""]

-- | The edges that are dotted, each as the names of its two nodes.
orderEdges :: [String] -> [(String, String)]
orderEdges found = sort [(nameOf from, nameOf to) | Seen from to attrs <- edgesSeen found, "style=dotted" `isInfixOf` attrs]
  where
    names = [(node, nameOfNode line) | (node, line) <- nodeLines found]
    nameOf node = fromMaybe node (lookup node names)

-- | The edges that are in the style of a message, which is orange, each as the names of its two nodes.
messagesOf :: [String] -> [(String, String)]
messagesOf found = sort [(nameOf from, nameOf to) | Seen from to attrs <- edgesSeen found, "color=\"#d08c3c\"" `isInfixOf` attrs]
  where
    names = [(node, nameOfNode line) | (node, line) <- nodeLines found]
    nameOf node = fromMaybe node (lookup node names)

-- | The node of a step that an end of an edge is at: the node of a pin has the node of its step in front of its own.
stepNodeOf :: String -> String
stepNodeOf = fst . breakText "__"

-- | Whether the edges lead from the node to the target, in one step or more, from a pin to its step too.
reaches :: [String] -> String -> String -> Bool
reaches found from target = go [] [from]
  where
    edges = [(stepNodeOf a, stepNodeOf b) | Seen a b _ <- edgesSeen found]
    go _ [] = False
    go seen (node : rest)
      | node `elem` seen = go seen rest
      | otherwise =
          let onward = [to | (source, to) <- edges, source == node]
           in target `elem` onward || go (node : seen) (onward ++ rest)
