{-# LANGUAGE LambdaCase #-}

-- | Prints a graph of "Network.Streaming.Diagram.Graph" as Graphviz DOT.
--
-- A step that has pins is a faint wrapper cluster that holds its body, which is the box of the step, and a node for each of its
-- pins, so that Graphviz orders the pins to cut the crossings of the edges as it orders the steps: the input pins are on a rank
-- above the box and the output pins on a rank below it, outside it, each held there by an invisible edge. An output pin that is
-- kept and taken apart is a cluster in the wrapper, titled with its name, that holds the pins of its parts. An edge goes from an
-- output pin to the input pins that read it, so most edges say nothing: the pins say it, and an edge that has no pin to be at is
-- drawn at the box, never at the wrapper. A decision is a box in yellow in a wrapper of a paler yellow, with its cases as yellow
-- nodes, or as yellow clusters that hold the pins of their patterns. What Mermaid draws as a node or a box for a name is not
-- drawn: it is a pin. The box of a named step, which has a page of its own, and its wrapper link to the page, and the box has a
-- heavier border than a step that is an action.
--
-- A flow is a box of the colour of the canvas with a light border, which holds its steps, a group of its input ports at the top
-- and a group of its output ports at the bottom: each a faint wrapper that holds a pin for each port, side by side. The ports are
-- the entry and the exit of the flow, so a page that a flow starts has no start circle, and what follows the flow follows its
-- output port. A diamond is where the branches of a decision that give the same name meet: the edges that end at a pin from two
-- or more places end at a diamond before the pin instead, which has one edge to the pin.
module Network.Streaming.Diagram.Graphviz (dot) where

import Control.Applicative ((<|>))
import Data.List (intercalate, isInfixOf, isPrefixOf, nub, partition, sort)
import Data.Maybe (fromMaybe, isJust, isNothing)
import Network.Streaming.Diagram.Graph

-- * The look

-- | The fill and the border of the body of a step, of a box that has a page, of a decision.
bodyFill, bodyLine, boxLine, decisionFill, decisionLine :: String
bodyFill = "#dde3ea"
bodyLine = "#57606a"
boxLine = "#24292f"
decisionFill = "#fff1a8"
decisionLine = "#9a6700"

-- | The fill and the border of the wrapper of a step, and of the wrapper of a decision.
wrapperFill, wrapperLine, decisionWrapperFill, decisionWrapperLine :: String
wrapperFill = "#f6f8fa"
wrapperLine = "#d8dee4"
decisionWrapperFill = "#fffbea"
decisionWrapperLine = "#f0e2a8"

-- | The fill and the border of an input pin, an output pin, and a case.
inFill, inLine, outFill, outLine, caseFill, caseLine :: String
inFill = "white"
inLine = "#8c959f"
outFill = "#ddf4ff"
outLine = "#54aeff"
caseFill = "#fff8c5"
caseLine = "#d4a72c"

-- | The border of the box of a flow, which has the colour of the canvas for its fill.
flowBorder :: String
flowBorder = "#afb8c1"

-- | The colour of the nodes and edges that are only marks.
markFill, orderLine, messageLine :: String
markFill = "#57606a"
orderLine = "#8c959f"
messageLine = "#d08c3c"

-- | How many ranks an edge spans: the ranks are close, so that the pins of a step are close to its body, and the steps are
-- apart by the ranks between them.
edgeRanks :: Int
edgeRanks = 4

-- * The graph

-- | What the printing of a graph knows of the graph.
data Env = Env
  { -- | The elements that are drawn.
    envElements :: [(Node, Element)],
    -- | The clusters that are drawn, with the nodes that they hold.
    envClusters :: [(Node, [Node])],
    -- | The results of flows, and the names that they read.
    envResults :: [(Node, Maybe String)],
    -- | The cases of decisions that a step reads a pin of: the decision, the case, and the node of the step.
    envCovered :: [(Node, String, Node)],
    -- | The starts of the chart that a flow follows, which are not drawn: the ports of the flow are the entry.
    envStarts :: [Node],
    -- | The input pins that edges from two or more places end at, each with the node of the diamond that they end at instead.
    envMerges :: [(Node, Node)]
  }

-- | The DOT of the graph.
dot :: Graph -> String
dot (Graph items) =
  unlines $
    [ "digraph G {",
      "  rankdir=TB;",
      "  compound=true;",
      "  newrank=true;",
      "  nodesep=0.1;",
      "  ranksep=0.06;",
      -- A longer search for the order of the nodes in a rank cuts the crossings of the largest page.
      "  mclimit=10;",
      "  bgcolor=\"white\";",
      "  fontname=\"Helvetica\";",
      "  node [shape=plaintext, fontname=\"Helvetica\"];",
      "  edge [arrowsize=0.6, color=\"" ++ bodyLine ++ "\", fontname=\"Helvetica\", fontsize=10];"
    ]
      ++ concatMap (itemLines env {envMerges = merges} "  ") items
      ++ fmap arcLine (merged merges arcs)
      ++ ["}"]
  where
    elements = elementsOf items
    env =
      Env
        elements
        (clustersOf items)
        [(node, name) | Result node name _ <- everything items]
        [ (decision, case', target)
          | Wire edge' <- everything items,
            Joined joins <- [edgeSits edge'],
            Join (Just (Seat decision (case' : _ : _))) to _ <- joins,
            let target = maybe (edgeTo edge') (\(Seat node _) -> node) to
        ]
        [ edgeFrom edge'
          | Wire edge' <- everything items,
            edgeStyle edge' == Entry,
            Just element <- [lookup (edgeFrom edge') elements],
            elementForm element == Start
        ]
        []
    arcs = nub (concatMap (arcsOf env) (everything items))
    merges = mergesOf arcs

-- | The items, those in groups too, in order.
everything :: [Item] -> [Item]
everything = concatMap $ \case
  Group node kind title inner -> Group node kind title [] : everything inner
  item -> [item]

-- | The elements that are drawn, which are those that are not the way that Mermaid draws a name.
elementsOf :: [Item] -> [(Node, Element)]
elementsOf items = [(node, element) | Node node element <- everything items, not (isName (elementForm element))]

-- | Whether the form is a node that Mermaid draws a name as.
isName :: Form -> Bool
isName = \case
  Name _ -> True
  _ -> False

-- | The clusters that are drawn, each with the nodes in it, and in the clusters in it, in order.
clustersOf :: [Item] -> [(Node, [Node])]
clustersOf = concatMap $ \case
  Group node kind _ inner
    | kind == Naming -> clustersOf inner
    | otherwise -> (node, nodesIn inner) : clustersOf inner
  _ -> []

-- | The nodes that are drawn in the items, in the groups too, in order.
nodesIn :: [Item] -> [Node]
nodesIn = concatMap $ \case
  Node node element | not (isName (elementForm element)) -> [node]
  Result node _ _ -> [node]
  Group _ _ _ inner -> nodesIn inner
  _ -> []

-- | The text of a string as a quoted DOT string.
quoted :: String -> String
quoted text = "\"" ++ concatMap escape text ++ "\""
  where
    escape = \case
      '"' -> "\\\""
      '\\' -> "\\\\"
      '\n' -> "\\n"
      c -> [c]

-- | The text as HTML, for a label.
html :: String -> String
html = concatMap $ \case
  '&' -> "&amp;"
  '<' -> "&lt;"
  '>' -> "&gt;"
  '"' -> "&quot;"
  c -> [c]

-- * The items

-- | The lines of an item, indented: a node, and a cluster with what is in it. The edges are written after them.
itemLines :: Env -> String -> Item -> [String]
itemLines env indent = \case
  Node node element
    | node `elem` envStarts env -> []
    | otherwise -> [indent ++ line | line <- elementLines node element ++ mergeLines env node]
  Result node name _ -> [indent ++ line | line <- portLines node "out" [fromMaybe " " name] ++ mergeLines env node]
  Wire _ -> []
  Group node kind title inner -> case kind of
    Naming -> concatMap (itemLines env indent) inner
    Flowing ->
      [ indent ++ "subgraph cluster_" ++ node ++ " {",
        indent ++ "  class=\"flow\"; style=\"filled,rounded\"; fillcolor=\"white\"; color=" ++ quoted flowBorder ++ "; penwidth=1; margin=8; label=\"\";"
      ]
        ++ concatMap (itemLines env (indent ++ "  ")) (flowOrder inner)
        ++ [indent ++ "}"]
    Plain -> cluster node title "style=\"rounded\"; color=\"#8c959f\"" False inner
    Container -> cluster node "\x2016 together" "style=\"dashed,rounded\"; color=\"#7c6fd0\"; penwidth=2" True (reverse inner)
    Lane -> cluster node title "style=\"filled,rounded\"; fillcolor=\"#7c6fd01a\"; color=\"#7c6fd066\"" False inner
  where
    cluster node title look sideBySide inner =
      [indent ++ "subgraph cluster_" ++ node ++ " {", indent ++ "  label=" ++ quoted title ++ "; labeljust=l; fontsize=11; " ++ look ++ ";"]
        ++ concatMap (itemLines env (indent ++ "  ")) inner
        ++ [indent ++ "  { rank=same; " ++ unwords [topOf env first ++ ";" | first <- firsts] ++ " }" | sideBySide, length firsts > 1]
        ++ [indent ++ "}"]
      where
        -- The lanes of a container are side by side: the first node of each is on the same rank.
        firsts = [first | Group _ _ _ lane <- inner, first : _ <- [nodesIn lane]]

-- | The lines of an element: a block, a box, a mark, or the group of the input ports of a flow, which is its entry.
elementLines :: Node -> Element -> [String]
elementLines node element = case (elementForm element, bodied element) of
  (Entered, _) -> portLines node "in" (inPorts element)
  (_, Just found) -> blockLines node found (inNames element) (elementOut element) (isDecision (elementForm element))
  (_, Nothing) -> maybe [] pure (markLine node element)
  where
    isDecision = \case
      Decision _ -> True
      _ -> False

-- | The node of an element that is a mark.
markLine :: Node -> Element -> Maybe String
markLine node element = case elementForm element of
  Start -> Just (mark "start" "shape=circle, width=0.2, fixedsize=true")
  Finish -> Just (mark "end" "shape=circle, peripheries=2, width=0.15, fixedsize=true, penwidth=1.5")
  Junction -> Just (mark "junction" "shape=circle, width=0.12, fixedsize=true")
  Bar _ -> Just (mark "bar" "shape=box, width=0.7, height=0.06, fixedsize=true")
  Store name -> Just (node ++ " [class=\"store\", shape=cylinder, style=filled, fillcolor=\"#fff4e5\", color=\"" ++ messageLine ++ "\", label=" ++ quoted name ++ "];")
  _ -> Nothing
  where
    mark klass look =
      node ++ " [class=\"" ++ klass ++ "\", " ++ look ++ ", style=filled, fillcolor=\"" ++ markFill ++ "\", color=\"" ++ markFill ++ "\", label=\"\"];"

-- * Bodies

-- | What the body of a step is drawn as: its class, its fill, its border and the width of it, its link, its text and the words
-- under it.
data Bodied = Bodied
  { bodiedClass :: String,
    bodiedFill :: String,
    bodiedLine :: String,
    bodiedWidth :: Int,
    bodiedLink :: Maybe String,
    bodiedText :: String,
    bodiedNotes :: [String]
  }

-- | The body of an element that has one, which is every element that is not a mark.
bodied :: Element -> Maybe Bodied
bodied element = case elementForm element of
  Step look text -> Just (Bodied "step" bodyFill bodyLine 1 Nothing (stepText look text) (caption look))
  Titled text -> Just (Bodied "box" bodyFill boxLine 2 (elementLink element) text [])
  Awaits text -> Just (Bodied "box" bodyFill boxLine 2 (elementLink element) text [])
  Decision question -> Just (Bodied "decision" decisionFill decisionLine 1 Nothing (fromMaybe "choose" question) [])
  _ -> Nothing

-- | The text of a step: a gear in front of what never waits.
stepText :: Look -> String -> String
stepText look text = case look of
  LookAction -> "\x2699\xFE0E " ++ text
  _ -> text

-- | The words that tell how a step that waits, blocks, gives way or hands over is told from one that is none of those.
caption :: Look -> [String]
caption = \case
  LookWait -> ["waits"]
  LookBlocking -> ["blocks"]
  LookYielding -> ["gives way"]
  LookHandover -> ["hands over"]
  _ -> []

-- | The text of the body: the title in a larger font, and under it the comment, wrapped, and the words that say how it waits.
bodyLabel :: Bodied -> String
bodyLabel found =
  "<<FONT POINT-SIZE=\"12\">" ++ html title ++ "</FONT>"
    ++ concat ["<BR/><FONT POINT-SIZE=\"9\" COLOR=\"#57606a\">" ++ html piece ++ "</FONT>" | piece <- bodiedNotes found ++ wrapped 38 comment]
    ++ ">"
  where
    (title, rest) = break (== '\n') (bodiedText found)
    comment = unwords (words (drop 1 rest))

-- | The words, broken into lines of at most the width, if each word fits.
wrapped :: Int -> String -> [String]
wrapped width = go [] . words
  where
    go [] [] = []
    go line [] = [unwords (reverse line)]
    go line (word : more)
      | null line = go [word] more
      | length (unwords (reverse (word : line))) <= width = go (word : line) more
      | otherwise = unwords (reverse line) : go [word] more

-- | The link of a body, as the attribute of a node or a cluster, with the separator in front of it.
linkAttr :: String -> Bodied -> String
linkAttr separator found = maybe "" (\anchor -> separator ++ "href=" ++ quoted ("#" ++ anchor)) (bodiedLink found)

-- | The node of a body that is a box: rounded and filled, of the class and with the margin.
boxNode :: Node -> String -> String -> Bodied -> String
boxNode node klass margin found =
  node ++ " [class=" ++ quoted klass ++ ", shape=box, style=\"rounded,filled\", fillcolor=" ++ quoted (bodiedFill found)
    ++ ", color=" ++ quoted (bodiedLine found) ++ ", penwidth=" ++ show (bodiedWidth found) ++ ", height=0.1, margin=" ++ quoted margin ++ ", label=" ++ bodyLabel found
    ++ linkAttr ", " found ++ "];"

-- * Blocks

-- | The names of the input pins of an element, each once.
inNames :: Element -> [String]
inNames = nub . elementIn

-- | The names of the ports of the entry of a flow, each once, which are the names that its steps read and the names that an outer
-- flow binds, or a blank one if it has none, so that the entry is a pin. They are in alphabetical order, from which Graphviz orders
-- the ports side by side as it cuts the crossings.
inPorts :: Element -> [String]
inPorts element = case sort (nub (elementIn element ++ [name | Just name <- pinName <$> elementOut element])) of
  [] -> [" "]
  names -> names

-- | The group of the ports of a flow, which is a faint wrapper with no title that holds a pin for each port side by side: input
-- pins for its input ports, which are the entry, and output pins for its output ports, which are the exit.
portLines :: Node -> String -> [String] -> [String]
portLines node klass names =
  [ "subgraph cluster_" ++ node ++ " {",
    "  class=\"ports\"; style=\"filled,rounded\"; fillcolor=" ++ quoted wrapperFill ++ "; color=" ++ quoted wrapperLine ++ "; penwidth=1; margin=6; label=\"\";"
  ]
    ++ ["  " ++ port (inPinId node [i]) name | (i, name) <- zip [0 :: Int ..] names]
    ++ ["  { rank=same; " ++ unwords [inPinId node [i] ++ ";" | i <- [0 .. length names - 1]] ++ " }" | length names > 1]
    ++ ["}"]
  where
    port = if klass == "in" then \ident -> pinNode ident klass "filled" inFill inLine else \ident -> pinNode ident klass "rounded,filled" outFill outLine

-- | The items of a flow in the order that they are drawn: its entry at the top, its steps, and its result at the bottom.
flowOrder :: [Item] -> [Item]
flowOrder inner = entries ++ others ++ results
  where
    (entries, rest) = partition (\case Node _ element -> elementForm element == Entered; _ -> False) inner
    (results, others) = partition (\case Result {} -> True; _ -> False) rest

-- | The diamonds of the pins of a node that edges from two or more places end at, which are declared where the pins are.
mergeLines :: Env -> Node -> [String]
mergeLines env node =
  [ diamond ++ " [class=\"merge\", shape=diamond, style=filled, fillcolor=" ++ quoted caseFill ++ ", color=" ++ quoted caseLine
      ++ ", penwidth=1, width=0.2, height=0.2, fixedsize=true, label=\"\"];"
    | (pin, diamond) <- envMerges env,
      (node ++ "__i_") `isPrefixOf` pin
  ]

-- | The node of the input pin, and of the output pin, that the indices lead to: the position of the pin among the pins of the
-- step, and then in the pins that it holds, if it is taken apart.
inPinId, outPinId :: Node -> [Int] -> Node
inPinId node ixs = node ++ "__i" ++ concatMap (\i -> "_" ++ show i) ixs
outPinId node ixs = node ++ "__o" ++ concatMap (\i -> "_" ++ show i) ixs

-- | Whether the element is a block: a body that has pins, which is a cluster.
blockOf :: Element -> Bool
blockOf element = isJust (bodied element) && not (null (elementIn element) && null (elementOut element))

-- | The node that an edge to the top of a node is drawn at: the first input pin of a block, the first port of the entry of a flow, its
-- output port if it is the result of one, or else the node.
topOf :: Env -> Node -> Node
topOf env node = case (lookup node (envElements env), lookup node (envResults env)) of
  (Just element, _) | elementForm element == Entered || (blockOf element && not (null (inNames element))) -> inPinId node [0]
  (_, Just _) -> inPinId node [0]
  _ -> node

-- | A body with its pins: a faint wrapper cluster, with the body as a box in it, the input pins above the box and the output pins
-- below it, outside it, on ranks of their own, held there by invisible edges. A body that has no pins is a box and no cluster.
blockLines :: Node -> Bodied -> [String] -> [Pin] -> Bool -> [String]
blockLines node found names pins asCase
  | null names && null pins = [boxNode node (bodiedClass found) "0.12,0.07" found]
  | otherwise =
      [ "subgraph cluster_" ++ node ++ " {",
        "  class=" ++ quoted (bodiedClass found) ++ "; style=\"filled,rounded\"; fillcolor=" ++ quoted wrapFill ++ "; color=" ++ quoted wrapLine
          ++ "; penwidth=1; margin=6; label=\"\"" ++ linkAttr "; " found ++ ";"
      ]
        ++ ["  " ++ pinNode (inPinId node [i]) "in" "filled" inFill inLine name | (i, name) <- zip [0 :: Int ..] names]
        ++ ["  " ++ boxNode node "body" "0.12,0.06" found]
        ++ concat [fmap ("  " ++) (outPinLines node asCase [k] pin) | (k, pin) <- zip [0 :: Int ..] pins]
        ++ ["  { rank=same; " ++ unwords [inPinId node [i] ++ ";" | i <- [0 .. length names - 1]] ++ " }" | length names > 1]
        ++ ["  " ++ inPinId node [i] ++ " -> " ++ node ++ " [style=invis, weight=100];" | i <- [0 .. length names - 1]]
        ++ ["  " ++ node ++ " -> " ++ outPinId node leaf ++ " [style=invis, weight=100];" | (k, pin) <- zip [0 :: Int ..] pins, leaf <- leavesOf [k] pin]
        ++ ["}"]
  where
    (wrapFill, wrapLine) = if asCase then (decisionWrapperFill, decisionWrapperLine) else (wrapperFill, wrapperLine)

-- | The node of a pin, which has the name of the pin for its label.
pinNode :: Node -> String -> String -> String -> String -> String -> String
pinNode ident klass style fill line name =
  ident ++ " [class=" ++ quoted klass ++ ", shape=box, style=" ++ quoted style ++ ", fillcolor=" ++ quoted fill ++ ", color=" ++ quoted line
    ++ ", fontsize=9, width=0, height=0.17, margin=\"0.06,0.02\", label=" ++ quoted name ++ "];"

-- | The indices of the leaves of a pin, whose own indices are given.
leavesOf :: [Int] -> Pin -> [[Int]]
leavesOf ixs pin = case pinParts pin of
  [] -> [ixs]
  parts -> concat [leavesOf (ixs ++ [k]) part | (k, part) <- zip [0 ..] parts]

-- | The lines of an output pin, whose indices are given: a node, or a cluster, titled with its name, that holds the pins of its
-- parts. A pin of a decision is a case: yellow, and the pins that it holds are the pins of its pattern.
outPinLines :: Node -> Bool -> [Int] -> Pin -> [String]
outPinLines node asCase ixs pin
  | null (pinParts pin) = [pinNode (outPinId node ixs) "out" "rounded,filled" fill line (if null title then " " else title)]
  | otherwise =
      [ "subgraph cluster_" ++ outPinId node ixs ++ " {",
        "  label=" ++ quoted title ++ "; fontsize=8; style=\"filled,rounded\"; fillcolor=" ++ quoted fill ++ "; color=" ++ quoted line ++ "; margin=4;"
      ]
        ++ concat [fmap ("  " ++) (outPinLines node False (ixs ++ [k]) part) | (k, part) <- zip [0 ..] (pinParts pin)]
        ++ ["}"]
  where
    (fill, line) = if asCase then (caseFill, caseLine) else (outFill, outLine)
    title = fromMaybe "" (pinName pin <|> pinField pin)

-- | The output pins that have a name, each with the names on the way to it from the top, and its indices.
namedPins :: Element -> [([String], [Int], Pin)]
namedPins element = go [] [] (elementOut element)
  where
    go names ixs pins =
      concat
        [ [(names', ixs', pin) | isJust (pinName pin)] ++ go names' ixs' (pinParts pin)
          | (k, pin) <- zip [0 ..] pins,
            let names' = names ++ [name | Just name <- [pinName pin]]
                ixs' = ixs ++ [k]
        ]

-- * The edges

-- | Where an edge is drawn at: the node, and the cluster if the end is a cluster that is not the wrapper of a step, which the edge
-- is drawn at a node of.
data Place = Place Node (Maybe Node)
  deriving (Eq)

-- | The place of a node that is an element or a result, or else of a cluster, which is at its first node.
placeOf :: Env -> Bool -> Node -> Maybe Place
placeOf env leaving node = case (lookup node (envElements env), lookup node (envResults env)) of
  (Just element, _) | elementForm element == Entered -> Just (Place (inPinId node [0]) Nothing)
  (Just _, _) -> Just (Place node Nothing)
  (_, Just _) -> Just (resultPlace node)
  _ -> case lookup node (envClusters env) of
    Just (first : _) -> Just (Place (if leaving then first else topOf env first) (Just node))
    _ -> Nothing

-- | The place that a seat is: the node of the pin that its names lead to, which is at its cluster if the pin is taken apart, and
-- the body if it has no pin.
seatPlace :: Env -> Bool -> Seat -> Maybe Place
seatPlace env leaving (Seat node path) = case (lookup node (envElements env), path) of
  (Just element, _) | elementForm element == Entered -> Just (Place (inPinId node [portOf element path]) Nothing)
  (Just element, _ : _) ->
    Just $
      if leaving
        then case pinOf element of
          Just (ixs, pin)
            | null (pinParts pin) -> Place (outPinId node ixs) Nothing
            | otherwise -> Place (outPinId node (firstLeaf ixs pin)) (Just (outPinId node ixs))
          Nothing -> Place node Nothing
        else case path of
          [name] | Just i <- lookup name (zip (inNames element) [0 :: Int ..]) -> Place (inPinId node [i]) Nothing
          _ -> Place node Nothing
  (Just _, []) -> Just (Place node Nothing)
  (Nothing, _) -> placeOf env leaving node
  where
    -- The pin that has exactly those names, or else the one whose last name is the last of them.
    pinOf element = case (lookup path found, path) of
      (Just pin, _) -> Just pin
      (Nothing, []) -> Nothing
      (Nothing, _) -> lookup (last path) [(last names, pin) | (names, pin) <- found, not (null names)]
      where
        found = [(names, (ixs, pin)) | (names, ixs, pin) <- namedPins element]
    firstLeaf ixs pin = case pinParts pin of
      [] -> ixs
      part : _ -> firstLeaf (ixs ++ [0]) part

-- | The position of the port that the names lead to among the ports of the entry of a flow, which is the first if no name does.
portOf :: Element -> [String] -> Int
portOf element path = case path of
  name : _ -> fromMaybe 0 (lookup name (zip (inPorts element) [0 ..]))
  [] -> 0

-- | The place at the result of a flow, which is its output port.
resultPlace :: Node -> Place
resultPlace node = Place (inPinId node [0]) Nothing

-- | The look of an edge of the style, with its label.
edgeAttrs :: Style -> Maybe String -> [String] -> String
edgeAttrs style label extra =
  case parts of
    [] -> ""
    _ -> " [" ++ intercalate ", " parts ++ "]"
  where
    parts =
      (case style of
          Solid -> []
          Entry -> []
          Dashed -> ["style=dashed"]
          Message -> ["style=dashed", "color=\"" ++ messageLine ++ "\"", "penwidth=2", "fontcolor=\"#9a6700\""]
          Order -> ["style=dotted", "color=\"" ++ orderLine ++ "\""])
        ++ ["label=" ++ quoted text | Just text <- [label]]
        ++ extra

-- | An edge as it is drawn: its style and its label, the places that it joins, how many ranks it spans, and whether it holds its ends
-- together, as the edge from a diamond to its pin does.
data Arc = Arc Style (Maybe String) Place Place Int Bool
  deriving (Eq)

-- | An edge between two places, which spans as many ranks as the others do.
joined :: Style -> Maybe String -> Place -> Place -> Arc
joined style label from to = Arc style label from to edgeRanks False

-- | The line of an edge.
arcLine :: Arc -> String
arcLine (Arc style label (Place tailNode tailCluster) (Place headNode headCluster) ranks heavy) =
  "  " ++ tailNode ++ " -> " ++ headNode
    ++ edgeAttrs
      style
      label
      ( ["minlen=" ++ show ranks]
          ++ ["weight=100" | heavy]
          ++ ["ltail=" ++ quoted ("cluster_" ++ c) | Just c <- [tailCluster]]
          ++ ["lhead=" ++ quoted ("cluster_" ++ c) | Just c <- [headCluster]]
      )
    ++ ";"

-- | The input pins that edges from two or more places end at, each with the diamond that they end at instead. No name is given by
-- two places but by the ways out of a decision, so these are the pins that the branches of a decision that give the same name feed.
mergesOf :: [Arc] -> [(Node, Node)]
mergesOf arcs = zip pins ["merge" ++ show k | k <- [1 :: Int ..]]
  where
    pins = nub [pin | Arc Solid _ _ (Place pin Nothing) _ _ <- arcs, "__i_" `isInfixOf` pin, length (nub (fed pin)) > 1]
    fed pin = [from | Arc Solid _ from (Place found Nothing) _ _ <- arcs, found == pin]

-- | The edges, with those that end at a pin that merges ending at its diamond, and one edge from the diamond to the pin after the last
-- of them.
merged :: [(Node, Node)] -> [Arc] -> [Arc]
merged merges arcs = concat (zipWith viaDiamond [0 :: Int ..] arcs)
  where
    viaDiamond k arc@(Arc style label from (Place pin cluster) ranks heavy) = case lookup pin merges of
      Just diamond
        | style == Solid && isNothing cluster ->
            Arc style label from (Place diamond Nothing) (ranks - 1) heavy
              : [Arc Solid Nothing (Place diamond Nothing) (Place pin Nothing) 1 True | k == lastInto pin]
      _ -> [arc]
    lastInto pin = maximum [k | (k, Arc Solid _ _ (Place found Nothing) _ _) <- zip [0 :: Int ..] arcs, found == pin]

-- | The edges of an item of the graph: one between the pins that it joins, for each of them, or else one between its nodes,
-- and the edge that leaves a flow is drawn from the result of the flow. An edge that has an end that is only the way that
-- Mermaid draws a name, and no pin to be at, is not drawn, and nor is the edge from a case of a decision to a step that reads
-- a pin of the case: the edge from the pin says it. The edge from a start to the entry of a flow is not drawn: the start is not.
arcsOf :: Env -> Item -> [Arc]
arcsOf env = \case
  Wire edge' | edgeStyle edge' == Entry && edgeFrom edge' `elem` envStarts env -> []
  Wire edge' -> case edgeVia edge' of
    Just result | isJust (lookup result (envResults env)) -> viaLines edge' result
    _ -> plainLines edge'
  Result node _ seats -> resultLines node seats
  _ -> []
  where
    plainLines edge' = case edgeSits edge' of
      Joined joins -> concat [joinLines edge' join' | join' <- joins]
      _ -> [joined (edgeStyle edge') (edgeLabel edge') from to | Just from <- [placeOf env True (edgeFrom edge')], Just to <- [placeOf env False (edgeTo edge')]]
    joinLines edge' (Join from to note) =
      [ joined (edgeStyle edge') note source target
        | not (covered edge' from to),
          Just source <- [maybe (placeOf env True (edgeFrom edge')) (seatPlace env True) from],
          Just target <- [maybe (placeOf env False (edgeTo edge')) (seatPlace env False) to]
      ]
    covered edge' from to = case from of
      Just (Seat decision [case']) -> (decision, case', maybe (edgeTo edge') (\(Seat node _) -> node) to) `elem` envCovered env
      _ -> False
    viaLines edge' result =
      [joined (edgeStyle edge') Nothing source target | Just source <- [placeOf env True result], Just target <- [targetOf edge']]
    targetOf edge' = case edgeSits edge' of
      Joined joins | seat : _ <- [seat | Join _ (Just seat) _ <- joins] -> seatPlace env False seat
      _ -> placeOf env False (edgeTo edge')
    resultLines node seats =
      [ joined Solid Nothing source (resultPlace node)
        | seat <- seats,
          Just source <- [seatPlace env True seat]
      ]
