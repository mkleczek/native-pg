{-# LANGUAGE LambdaCase #-}

-- | Prints a graph of "Network.Streaming.Diagram.Graph" as Graphviz DOT.
--
-- A step is one node, a table of three rows: its input pins on top, its body, and its output pins below, which are nested as
-- the pattern that takes the output apart nests. An edge goes from an output pin to the input pins that read it, so most
-- edges say nothing: the pins say it. A decision is a body in yellow, with its cases as yellow pins below it, which hold the
-- pins of their patterns. What Mermaid draws as a node or a box for a name is not drawn: it is a pin. The box of a named
-- step, which has a page of its own, links to the page, and has a heavier border than a step that is an action.
module Network.Streaming.Diagram.Graphviz (dot) where

import Control.Applicative ((<|>))
import Data.Char (isAlphaNum, isAscii)
import Data.List (intercalate, nub)
import Data.Maybe (fromMaybe, isJust)
import Network.Streaming.Diagram.Graph

-- * The look

-- | The fill and the border of the body of a step, of a box that has a page, of a decision.
bodyFill, bodyLine, boxLine, decisionFill, decisionLine :: String
bodyFill = "#dde3ea"
bodyLine = "#57606a"
boxLine = "#24292f"
decisionFill = "#fff1a8"
decisionLine = "#9a6700"

-- | The fill and the border of an input pin, an output pin, and a case.
inFill, inLine, outFill, outLine, caseFill, caseLine :: String
inFill = "white"
inLine = "#8c959f"
outFill = "#ddf4ff"
outLine = "#54aeff"
caseFill = "#fff8c5"
caseLine = "#d4a72c"

-- | The colour of the nodes and edges that are only marks.
markFill, orderLine, messageLine :: String
markFill = "#57606a"
orderLine = "#8c959f"
messageLine = "#d08c3c"

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
    envCovered :: [(Node, String, Node)]
  }

-- | The DOT of the graph.
dot :: Graph -> String
dot (Graph items) =
  unlines $
    [ "digraph G {",
      "  rankdir=TB;",
      "  compound=true;",
      "  newrank=true;",
      "  nodesep=0.35;",
      "  ranksep=0.45;",
      "  bgcolor=\"white\";",
      "  fontname=\"Helvetica\";",
      "  node [shape=plaintext, fontname=\"Helvetica\"];",
      "  edge [arrowsize=0.6, color=\"" ++ bodyLine ++ "\", fontname=\"Helvetica\", fontsize=10];"
    ]
      ++ concatMap (itemLines env "  ") items
      ++ nub (concatMap (edgeLines env) (everything items))
      ++ ["}"]
  where
    env =
      Env
        (elementsOf items)
        (clustersOf items)
        [(node, name) | Result node name _ <- everything items]
        [ (decision, case', target)
          | Wire edge' <- everything items,
            Joined joins <- [edgeSits edge'],
            Join (Just (Seat decision (case' : _ : _))) to _ <- joins,
            let target = maybe (edgeTo edge') (\(Seat node _) -> node) to
        ]

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

-- | A name as the text of an identifier.
sanitize :: String -> String
sanitize = fmap (\c -> if isAscii c && isAlphaNum c then c else '_')

-- * The items

-- | The lines of an item, indented: a node, and a cluster with what is in it. The edges are written after them.
itemLines :: Env -> String -> Item -> [String]
itemLines env indent = \case
  Node node element -> [indent ++ line | Just line <- [elementLine node element]]
  Result node name _ -> [indent ++ resultLine node name]
  Wire _ -> []
  Group node kind title inner -> case kind of
    Naming -> concatMap (itemLines env indent) inner
    Plain -> cluster node title "style=\"rounded\"; color=\"#8c959f\"" False inner
    Container -> cluster node "\x2016 together" "style=\"dashed,rounded\"; color=\"#7c6fd0\"; penwidth=2" True (reverse inner)
    Lane -> cluster node title "style=\"filled,rounded\"; fillcolor=\"#7c6fd01a\"; color=\"#7c6fd066\"" False inner
  where
    cluster node title look sideBySide inner =
      [indent ++ "subgraph cluster_" ++ node ++ " {", indent ++ "  label=" ++ quoted title ++ "; labeljust=l; fontsize=11; " ++ look ++ ";"]
        ++ concatMap (itemLines env (indent ++ "  ")) inner
        ++ [indent ++ "  { rank=same; " ++ unwords [first ++ ";" | first <- firsts] ++ " }" | sideBySide, length firsts > 1]
        ++ [indent ++ "}"]
      where
        -- The lanes of a container are side by side: the first node of each is on the same rank.
        firsts = [first | Group _ _ _ lane <- inner, first : _ <- [nodesIn lane]]

-- | The node of an element: a table of its pins and its body, or a mark.
elementLine :: Node -> Element -> Maybe String
elementLine node element = case elementForm element of
  Name _ -> Nothing
  Start
    | null (elementOut element) -> Just (mark "start" "shape=circle, width=0.2, fixedsize=true")
    | otherwise -> Just (tabled "input" (body bodyFill bodyLine 1 Nothing "flow input" []))
  Finish -> Just (mark "end" "shape=circle, peripheries=2, width=0.15, fixedsize=true, penwidth=1.5")
  Junction -> Just (mark "junction" "shape=circle, width=0.12, fixedsize=true")
  Entered -> Just (tabled "input" (body bodyFill bodyLine 1 Nothing "flow input" []))
  Bar _ -> Just (mark "bar" "shape=box, width=0.7, height=0.06, fixedsize=true")
  Store name -> Just (node ++ " [class=\"store\", shape=cylinder, style=filled, fillcolor=\"#fff4e5\", color=\"" ++ messageLine ++ "\", label=" ++ quoted name ++ "];")
  Step look text -> Just (tabled "step" (body bodyFill bodyLine 1 Nothing (stepText look text) (caption look)))
  Titled text -> Just (tabled "box" (body bodyFill boxLine 2 (elementLink element) text []))
  Awaits text -> Just (tabled "box" (body bodyFill boxLine 2 (elementLink element) text []))
  Decision question -> Just (tabled "decision" (body decisionFill decisionLine 1 Nothing (fromMaybe "choose" question) []))
  where
    mark klass look =
      node ++ " [class=\"" ++ klass ++ "\", " ++ look ++ ", style=filled, fillcolor=\"" ++ markFill ++ "\", color=\"" ++ markFill ++ "\", label=\"\"];"
    tabled klass bodyCell =
      node ++ " [class=\"" ++ klass ++ "\", label=<" ++ table (inRow element) bodyCell (outRow element) ++ ">];"

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

-- | The result of a flow: a body, with an input pin for the name that it reads.
resultLine :: Node -> Maybe String -> String
resultLine node name =
  node ++ " [class=\"result\", label=<" ++ table pins (body bodyFill bodyLine 1 Nothing "flow result" []) "" ++ ">];"
  where
    pins = case name of
      Just wanted -> inCells [wanted]
      Nothing -> ""

-- * Tables

-- | The three rows of an element, each one a table of its own so that the cells of a row don't line up with those of another.
table :: String -> String -> String -> String
table above middle below =
  "<TABLE BORDER=\"0\" CELLSPACING=\"0\" CELLPADDING=\"0\">"
    ++ row above
    ++ "<TR>" ++ middle ++ "</TR>"
    ++ row below
    ++ "</TABLE>"
  where
    row cells
      | null cells = ""
      | otherwise = "<TR><TD BORDER=\"0\"><TABLE BORDER=\"0\" CELLSPACING=\"2\" CELLPADDING=\"0\"><TR>" ++ cells ++ "</TR></TABLE></TD></TR>"

-- | The body of an element: the cell with its text, which has the title on a line of its own in a larger font, and the
-- comment under it, wrapped, and the words that say how it waits.
body :: String -> String -> Int -> Maybe String -> String -> [String] -> String
body fill line width link text notes =
  "<TD PORT=\"body\" BGCOLOR=\"" ++ fill ++ "\" COLOR=\"" ++ line ++ "\" BORDER=\"" ++ show width ++ "\" CELLPADDING=\"6\" STYLE=\"rounded\""
    ++ maybe "" (\anchor -> " HREF=\"#" ++ html anchor ++ "\"") link
    ++ ">"
    ++ "<FONT POINT-SIZE=\"12\">" ++ html title ++ "</FONT>"
    ++ concat ["<BR/><FONT POINT-SIZE=\"9\" COLOR=\"#57606a\">" ++ html piece ++ "</FONT>" | piece <- notes ++ wrapped 38 comment]
    ++ "</TD>"
  where
    (title, rest) = break (== '\n') text
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

-- * Pins

-- | The ports of the input pins of an element, each with the name of its pin.
inPorts :: Element -> [(String, String)]
inPorts element = uniqued [("i_" ++ sanitize name, name) | name <- nub (elementIn element)]

-- | The ports of the output pins of an element, each with the names on the way to the pin from the top.
outPorts :: Element -> [([String], String)]
outPorts element = uniqued (go [] (elementOut element))
  where
    go path pins =
      concat
        [ [(path', "o_" ++ intercalate "_" (sanitize <$> path')) | isJust (pinName pin)] ++ go path' (pinParts pin)
          | pin <- pins,
            let path' = path ++ [name | Just name <- [pinName pin]]
        ]

-- | The ports that are named alike are numbered, so that each is its own.
uniqued :: [(a, String)] -> [(a, String)]
uniqued ports = [(key, if count == 1 then port else port ++ "_" ++ show n) | (index, (key, port)) <- numbered, let (n, count) = counts index port]
  where
    numbered = zip [0 :: Int ..] ports
    counts index port =
      ( length [() | (other, (_, same)) <- numbered, other <= index, same == port],
        length [() | (_, (_, same)) <- numbered, same == port]
      )

-- | The port of the pin that the names lead to: the one that has exactly those names, or else the one whose last name is the
-- last of them.
outPortOf :: Element -> [String] -> Maybe String
outPortOf element path = case (lookup path ports, path) of
  (Just port, _) -> Just port
  (Nothing, []) -> Nothing
  (Nothing, _) -> lookup (last path) [(last found, port) | (found, port) <- ports, not (null found)]
  where
    ports = outPorts element

-- | The port of the input pin of the name.
inPortOf :: Element -> String -> Maybe String
inPortOf element name = lookup name [(pin, port) | (port, pin) <- inPorts element]

-- | The cells of the input pins, in a row.
inRow :: Element -> String
inRow element = concat [inCell port name | (port, name) <- inPorts element]

-- | The cells of the input pins of the names, which have no ports.
inCells :: [String] -> String
inCells names = concat [inCell ("i_" ++ sanitize name) name | name <- names]

-- | The cell of an input pin.
inCell :: String -> String -> String
inCell port name =
  "<TD PORT=\"" ++ port ++ "\" BGCOLOR=\"" ++ inFill ++ "\" COLOR=\"" ++ inLine ++ "\" BORDER=\"1\" CELLPADDING=\"2\"><FONT POINT-SIZE=\"9\">"
    ++ html name
    ++ "</FONT></TD>"

-- | The cells of the output pins in a row, which are the cases of a decision and the pins of what a step gives.
outRow :: Element -> String
outRow element = concat (cells [] (elementOut element) (isDecision (elementForm element)))
  where
    ports = outPorts element
    isDecision = \case
      Decision _ -> True
      _ -> False
    cells path pins asCase =
      [ cell path' pin asCase
        | pin <- pins,
          let path' = path ++ [name | Just name <- [pinName pin]]
      ]
    cell path pin asCase =
      "<TD" ++ portAttr ++ " BGCOLOR=\"" ++ fill ++ "\" COLOR=\"" ++ line ++ "\" BORDER=\"1\" CELLPADDING=\"" ++ (if null (pinParts pin) then "2" else "1") ++ "\" STYLE=\"rounded\">"
        ++ inside
        ++ "</TD>"
      where
        (fill, line) = if asCase then (caseFill, caseLine) else (outFill, outLine)
        portAttr = case (pinName pin, lookup path ports) of
          (Just _, Just port) -> " PORT=\"" ++ port ++ "\""
          _ -> ""
        title = fromMaybe "" (pinName pin <|> pinField pin)
        inside
          | null (pinParts pin) = "<FONT POINT-SIZE=\"9\">" ++ html (if null title then " " else title) ++ "</FONT>"
          | otherwise =
              "<TABLE BORDER=\"0\" CELLSPACING=\"2\" CELLPADDING=\"0\">"
                ++ (if null title then "" else "<TR><TD COLSPAN=\"" ++ show (length (pinParts pin)) ++ "\" BORDER=\"0\" CELLPADDING=\"0\"><FONT POINT-SIZE=\"8\">" ++ html title ++ "</FONT></TD></TR>")
                ++ "<TR>"
                ++ concat (cells path (pinParts pin) False)
                ++ "</TR></TABLE>"

-- * The edges

-- | Where an edge is drawn at: the node, the port, and the cluster if the end is a cluster, which the edge is drawn at a
-- node of.
data Place = Place Node (Maybe String) (Maybe Node)
  deriving (Eq)

-- | The place of a node that is an element or a cluster, which has no port.
placeOf :: Env -> Node -> Maybe Place
placeOf env node
  | isJust (lookup node (envElements env)) || isJust (lookup node (envResults env)) = Just (Place node Nothing Nothing)
  | otherwise = case lookup node (envClusters env) of
      Just (first : _) -> Just (Place first Nothing (Just node))
      _ -> Nothing

-- | The place that a seat is: the pin of the element that its names lead to, and the body if it has none.
seatPlace :: Env -> Bool -> Seat -> Maybe Place
seatPlace env leaving (Seat node path) = case (lookup node (envElements env), path) of
  (Just element, _ : _) ->
    let port
          | leaving = outPortOf element path
          | otherwise = case path of
              [name] -> inPortOf element name
              _ -> Nothing
     in Just (Place node port Nothing)
  (Just _, []) -> Just (Place node Nothing Nothing)
  (Nothing, _) -> placeOf env node

-- | The text of the end of an edge: the node, the port at the side that the edge leaves from or enters at, and the cluster.
end :: Bool -> Place -> (String, [String])
end leaving (Place node port cluster) =
  ( node ++ maybe "" (\p -> ":" ++ p ++ (if leaving then ":s" else ":n")) port,
    [(if leaving then "ltail=" else "lhead=") ++ quoted ("cluster_" ++ c) | Just c <- [cluster]]
  )

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
          Dashed -> ["style=dashed"]
          Message -> ["style=dashed", "color=\"" ++ messageLine ++ "\"", "penwidth=2", "fontcolor=\"#9a6700\""]
          Order -> ["style=dotted", "color=\"" ++ orderLine ++ "\""])
        ++ ["label=" ++ quoted text | Just text <- [label]]
        ++ extra

-- | The line of an edge between two places.
joined :: Style -> Maybe String -> Place -> Place -> String
joined style label from to = "  " ++ tailEnd ++ " -> " ++ headEnd ++ edgeAttrs style label (tailExtra ++ headExtra) ++ ";"
  where
    (tailEnd, tailExtra) = end True from
    (headEnd, headExtra) = end False to

-- | The lines of an edge of the graph: one between the pins that it joins, for each of them, or else one between its nodes,
-- and the edge that leaves a flow is drawn from the result of the flow. An edge that has an end that is only the way that
-- Mermaid draws a name, and no pin to be at, is not drawn, and nor is the edge from a case of a decision to a step that reads
-- a pin of the case: the edge from the pin says it.
edgeLines :: Env -> Item -> [String]
edgeLines env = \case
  Wire edge' -> case edgeVia edge' of
    Just result | isJust (lookup result (envResults env)) -> viaLines edge' result
    _ -> plainLines edge'
  Result node name seats -> resultLines node name seats
  _ -> []
  where
    plainLines edge' = case edgeSits edge' of
      Joined joins -> concat [joinLines edge' join' | join' <- joins]
      _ -> [joined (edgeStyle edge') (edgeLabel edge') from to | Just from <- [placeOf env (edgeFrom edge')], Just to <- [placeOf env (edgeTo edge')]]
    joinLines edge' (Join from to note) =
      [ joined (edgeStyle edge') note source target
        | not (covered edge' from to),
          Just source <- [maybe (placeOf env (edgeFrom edge')) (seatPlace env True) from],
          Just target <- [maybe (placeOf env (edgeTo edge')) (seatPlace env False) to]
      ]
    covered edge' from to = case from of
      Just (Seat decision [case']) -> (decision, case', maybe (edgeTo edge') (\(Seat node _) -> node) to) `elem` envCovered env
      _ -> False
    viaLines edge' result =
      [joined (edgeStyle edge') Nothing (Place result Nothing Nothing) target | Just target <- [targetOf edge']]
    targetOf edge' = case edgeSits edge' of
      Joined joins | seat : _ <- [seat | Join _ (Just seat) _ <- joins] -> seatPlace env False seat
      _ -> placeOf env (edgeTo edge')
    resultLines node name seats =
      [ joined Solid Nothing source (Place node (name >>= \wanted -> Just ("i_" ++ sanitize wanted)) Nothing)
        | seat <- seats,
          Just source <- [seatPlace env True seat]
      ]
