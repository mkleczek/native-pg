{-# LANGUAGE LambdaCase #-}

-- | Draws a program of the "Network.Streaming.Diagram" arrow as Mermaid flowcharts: one chart for each named box. A
-- named box is a single node in the chart of the box it is in, which links to the chart of its own. The charts are
-- printed from the graphs of "Network.Streaming.Diagram.Graph", which has the walk of the tree: Mermaid has no pins, so a
-- name that Graphviz draws as a pin is a node here, or a box that holds the nodes of its parts, and the text of an edge is
-- its label.
module Network.Streaming.Diagram.Mermaid
  ( Page (..),
    pages,
    Pair (..),
    pairs,
    html,
  )
where

import Control.Exception (evaluate)
import Control.Monad (unless)
import Data.Foldable (traverse_)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.List (intercalate, nub)
import Data.Maybe (fromMaybe, isNothing)
import GHC.Stack (SrcLoc (..))
import Network.Streaming.Annotate (Side (..))
import Network.Streaming.Classes (Label (..))
import Network.Streaming.Diagram
import Network.Streaming.Diagram.Graph
import Network.Streaming.Structure (children)
import System.Mem.StableName (makeStableName)

-- | A chart of its own, for a named box.
data Page = Page
  { pageTitle :: String,
    pageComment :: String,
    -- | The place in the code that named the box.
    pageSource :: Maybe SrcLoc,
    -- | The name of the page in links, and of its file.
    pageAnchor :: String,
    -- | The anchor of the page whose chart has the box that this page is of, where it was first met.
    pageParent :: Maybe String,
    -- | The chart, in Mermaid's flowchart syntax, without the plumbing: the steps on a part of a value are drawn alone.
    pageMermaid :: String,
    -- | The chart with the plumbing: a step on a part of a value has a split, a join, and an edge past it.
    pagePlumbing :: String,
    -- | How many actions in the chart have no name, and are drawn as "IO".
    pageUnnamed :: Int,
    -- | How many decisions in the chart have no question, and are drawn as "choose".
    pageUnquestioned :: Int,
    -- | How many pairs of neighbouring steps were drawn as one step.
    pageFolds :: Int
  }

-- | An exchange of a named value: the boxes that its answering ends are in, and the boxes that its asking ends are in,
-- each as the titles of the boxes on the way from the top of the program.
data Pair = Pair
  { pairName :: String,
    pairAnswering :: [[String]],
    pairAsking :: [[String]]
  }

-- | The ends of the exchanges of a program, paired by name, in the order that their names are first met. An end is met
-- once, in the first box that has it: the program is walked as it is, with each node visited once.
pairs :: Diagram a b -> IO [Pair]
pairs (Diagram root) = do
  seen <- newIORef []
  found <- newIORef []
  let visit path step = do
        tree <- evaluate step
        name <- makeStableName tree
        known <- elem name <$> readIORef seen
        unless known $ do
          modifyIORef' seen (name :)
          case tree of
            End side exchanged _ -> modifyIORef' found (++ [(side, exchanged, path)])
            _ -> pure ()
          let inside = case tree of
                Named (Label title _) _ _ -> path ++ [title]
                End Asking exchanged _ -> path ++ ["awaits " ++ exchanged]
                _ -> path
          traverse_ (visit inside) (children tree)
  visit [] root
  ends <- readIORef found
  pure
    [ Pair exchanged (pathsOf Answering exchanged ends) (pathsOf Asking exchanged ends)
      | exchanged <- nub [exchanged | (_, exchanged, _) <- ends]
    ]
  where
    pathsOf side exchanged ends = [path | (found, other, path) <- ends, found == side, other == exchanged]


-- * The pages

-- | The pages of a program, as Mermaid charts: the page of its top box first, and then those of the boxes in it, in the
-- order they are met. A program that is not a box is a page of its own, named "diagram".
pages :: Diagram a b -> IO [Page]
pages program = fmap page <$> sheets program
  where
    page sheet =
      Page
        (sheetTitle sheet)
        (sheetComment sheet)
        (sheetSource sheet)
        (sheetAnchor sheet)
        (sheetParent sheet)
        (chart (sheetFlow sheet))
        (chart (sheetPlumbing sheet))
        (sheetUnnamed sheet)
        (sheetUnquestioned sheet)
        (sheetFolds sheet)

-- * The text of a chart

-- | A line of the text of a chart, with the style of its edge if it is an edge.
data Line = Line
  { lineEdge :: Maybe Style,
    lineText :: String
  }

-- | The chart of a graph.
chart :: Graph -> String
chart (Graph items) =
  unlines ("flowchart TD" : fmap ("    " ++) (fmap lineText body ++ clicks items ++ classes items ++ linkStyles body))
  where
    (written, outside) = layout items
    body = laidLines "" written ++ fmap (linkLined "") outside

-- | The lines of what is laid out, indented.
laidLines :: String -> [Laid] -> [Line]
laidLines indent = concatMap $ \case
  Declared node form -> [Line Nothing (indent ++ describe node form)]
  Boxed node kind title inner ->
    let opening = indent ++ "subgraph " ++ node ++ " [" ++ quoted title ++ "]"
        direction = [indent ++ "  direction TB" | kind == Lane]
     in fmap (Line Nothing) ([opening] ++ direction) ++ laidLines (indent ++ "  ") inner ++ [Line Nothing (indent ++ "end")]
  Linked joint -> [linkLined indent joint]

-- | The text that declares a node, from what it is.
describe :: Node -> Form -> String
describe node = \case
  Start -> node ++ "@{ shape: sm-circ }"
  Finish -> node ++ "@{ shape: fr-circ }"
  Junction -> junction
  Entered -> junction
  Bar title -> node ++ "@{ shape: fork, label: " ++ labelOf title ++ " }"
  Step look text -> case look of
    LookAction -> node ++ "[" ++ labelOf (gear ++ text) ++ "]"
    LookWait -> node ++ "@{ shape: delay, label: " ++ labelOf text ++ " }"
    LookBlocking -> node ++ "@{ shape: hourglass }"
    LookYielding -> node ++ "@{ shape: flip-tri, label: " ++ labelOf text ++ " }"
    LookHandover -> node ++ "@{ shape: lean-r, label: " ++ labelOf text ++ " }"
    LookRound -> node ++ "(" ++ labelOf text ++ ")"
  Titled text -> node ++ "[[" ++ labelOf text ++ "]]"
  Awaits text -> node ++ "@{ shape: notch-rect, label: " ++ labelOf text ++ " }"
  Store name -> node ++ "[(" ++ labelOf name ++ ")]"
  Decision question -> node ++ "{" ++ labelOf (fromMaybe "choose" question) ++ "}"
  Name name -> node ++ "([" ++ labelOf name ++ "])"
  where
    junction = node ++ "@{ shape: f-circ }"

-- | The sign of a step that never waits, which is a gear (U+2699) kept a plain glyph by U+FE0E, and a space.
gear :: String
gear = "\x2699\xFE0E "

-- | A label as Mermaid reads it: in quotes, with the characters that it takes for markup escaped. The first line is a
-- title, and the rest a comment, which is drawn on a line of its own, smaller.
labelOf :: String -> String
labelOf text = "\"" ++ escaped title ++ comment' ++ "\""
  where
    (title, rest) = break (== '\n') text
    comment' = case drop 1 rest of
      "" -> ""
      comment -> "<br/><small>" ++ escaped comment ++ "</small>"

-- | The text, with the characters that Mermaid takes for markup in a label written as it has them.
escaped :: String -> String
escaped = concatMap $ \case
  '"' -> "#quot;"
  '<' -> "#lt;"
  '>' -> "#gt;"
  c -> [c]

-- | A label in quotes, which is only text.
quoted :: String -> String
quoted text = "\"" ++ escaped text ++ "\""

-- | The line of an edge.
linkLine :: Link -> String
linkLine (Link from to label style) = from ++ arrow ++ to
  where
    arrow = case label of
      Nothing -> case style of Solid -> " --> "; _ -> " -.-> "
      Just text -> case style of Solid -> " -->|" ++ quoted text ++ "| "; _ -> " -.->|" ++ quoted text ++ "| "

-- | The line of an edge, indented.
linkLined :: String -> Link -> Line
linkLined indent joint@(Link _ _ _ style) = Line (Just style) (indent ++ linkLine joint)

-- | The style of the edges of messages and of the edges that say only the order, which are known by their numbers: Mermaid
-- numbers the edges in the order of the text. The order edges are dotted, in grey and thin, as an edge of data never is.
linkStyles :: [Line] -> [String]
linkStyles body =
  [ "linkStyle " ++ intercalate "," (show <$> numbers) ++ " " ++ look
    | (wanted, look) <-
        [ (messageStyle, "stroke:#d08c3c,stroke-width:2px,stroke-dasharray:3 3"),
          (orderStyle, "stroke:#8c959f,stroke-width:1px,stroke-dasharray:2 4")
        ],
      let numbers = [number | (number, style) <- zip [0 :: Int ..] [style | Just style <- lineEdge <$> body], wanted style],
      not (null numbers)
  ]
  where
    messageStyle = \case Message -> True; _ -> False
    orderStyle = \case Order -> True; _ -> False

-- | The lines of the links of the boxes in the items, which the chart ends with.
clicks :: [Item] -> [String]
clicks = concatMap $ \case
  Node node (Element _ (Just anchor) _ _) -> ["click " ++ node ++ " href " ++ quoted ('#' : anchor)]
  Group _ _ _ inner -> clicks inner
  _ -> []

-- | The definitions of the styles of the containers and the lanes, and the groups that have them, if there are any.
classes :: [Item] -> [String]
classes items = case groupsOf Container items of
  [] -> []
  containers ->
    [ "classDef together fill:none,stroke:#7c6fd0,stroke-width:2px,stroke-dasharray:6 4",
      "classDef lane fill:#7c6fd01a,stroke:#7c6fd066",
      "class " ++ intercalate "," containers ++ " together",
      "class " ++ intercalate "," (groupsOf Lane items) ++ " lane"
    ]

-- | The groups of the kind that are in the items, in groups too.
groupsOf :: Kind -> [Item] -> [Node]
groupsOf kind items = concat [[node | found == kind] ++ groupsOf kind inner | Group node found _ inner <- items]

-- * The document

-- | A document of one section for each page, with a tree of the pages to find them in, and Mermaid to draw them. Each
-- page has its chart without the plumbing and with it, and a switch at the top shows one or the other. The title is the
-- document's.
html :: String -> [Page] -> String
html title docPages =
  unlines
    [ "<!doctype html>",
      "<html lang=\"en\">",
      "<head>",
      "<meta charset=\"utf-8\">",
      "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
      "<title>" ++ htmlText title ++ "</title>",
      "<style>",
      stylesheet,
      "</style>",
      "</head>",
      "<body>",
      "<nav>",
      "<h1>" ++ htmlText title ++ "</h1>",
      "<label class=\"switch\"><input type=\"checkbox\" id=\"plumbing\"> show plumbing</label>",
      navigation docPages,
      "</nav>",
      "<main>",
      concatMap section docPages,
      "</main>",
      "<script src=\"https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.min.js\"></script>",
      "<script>",
      "mermaid.initialize({ startOnLoad: false, securityLevel: 'loose',",
      "  theme: window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'default' });",
      "// The charts are drawn while all are visible, as Mermaid draws a chart of no size in a box that is hidden.",
      "const plumbing = document.getElementById('plumbing');",
      "const show = () => document.body.classList.toggle('plumbing', plumbing.checked);",
      "plumbing.addEventListener('change', show);",
      "mermaid.run({ querySelector: '.mermaid', suppressErrors: true })",
      "  .then(() => { document.body.classList.add('ready'); show(); });",
      "</script>",
      "</body>",
      "</html>"
    ]

-- | The page's section: its heading, which is the anchor, its comment, where it is in the code, a link to its parent, and
-- its chart.
section :: Page -> String
section page =
  unlines
    [ "<section>",
      "<h2 id=\"" ++ htmlText (pageAnchor page) ++ "\">" ++ htmlText (pageTitle page) ++ "</h2>",
      paragraph "comment" (pageComment page),
      maybe "" (paragraph "source" . sourceText) (pageSource page),
      maybe "" (\parent -> "<p class=\"up\">" ++ link parent ("up to " ++ parent) ++ "</p>") (pageParent page),
      charts page,
      "</section>"
    ]

-- | The charts of the page: the one that has no plumbing, and the one that has it, which is left out if it is the same.
charts :: Page -> String
charts page
  | pageMermaid page == pagePlumbing page = chartOf "" (pageMermaid page)
  | otherwise = chartOf "view-default" (pageMermaid page) ++ chartOf "view-plumbing" (pagePlumbing page)
  where
    chartOf kind text = "<pre class=\"mermaid " ++ kind ++ "\">\n" ++ htmlText text ++ "</pre>\n"

-- | A paragraph of the text, of a kind of the style's, which is nothing for no text.
paragraph :: String -> String -> String
paragraph kind body = if null body then "" else "<p class=\"" ++ kind ++ "\">" ++ htmlText body ++ "</p>"

-- | Where in the code: the file and the line.
sourceText :: SrcLoc -> String
sourceText place = srcLocFile place ++ ":" ++ show (srcLocStartLine place)

-- | A link to the section of the page with the anchor.
link :: String -> String -> String
link anchor body = "<a href=\"#" ++ htmlText anchor ++ "\">" ++ htmlText body ++ "</a>"

-- | The pages as nested lists: a page under the page where it was first met.
navigation :: [Page] -> String
navigation docPages = list (filter (isNothing . pageParent) docPages)
  where
    list [] = ""
    list found = "<ul>" ++ concatMap item found ++ "</ul>"
    item page =
      "<li>"
        ++ link (pageAnchor page) (pageTitle page)
        ++ list (filter ((== Just (pageAnchor page)) . pageParent) docPages)
        ++ "</li>"

-- | The text, written as HTML.
htmlText :: String -> String
htmlText = concatMap $ \case
  '&' -> "&amp;"
  '<' -> "&lt;"
  '>' -> "&gt;"
  '"' -> "&quot;"
  c -> [c]

-- | The style of the document, which follows the light or dark setting of the reader.
stylesheet :: String
stylesheet =
  unlines
    [ ":root { color-scheme: light dark; }",
      "body { margin: 0; display: flex; font: 15px/1.5 system-ui, sans-serif; background: #ffffff; color: #1b1f24; }",
      "nav { position: sticky; top: 0; align-self: flex-start; box-sizing: border-box; height: 100vh; min-width: 15rem;",
      "  overflow: auto; padding: 1rem 1.25rem; border-right: 1px solid #d0d7de; }",
      "nav h1 { font-size: 1.1rem; }",
      "nav ul { list-style: none; margin: 0; padding-left: 1rem; }",
      "nav > ul { padding-left: 0; }",
      "main { flex: 1; min-width: 0; padding: 0 2rem 4rem; }",
      "section { padding-top: 1rem; margin-bottom: 1.5rem; border-bottom: 1px solid #d0d7de; }",
      "a { color: #0969da; }",
      ".comment { margin: 0.25rem 0; }",
      ".source, .up { margin: 0.25rem 0; font-size: 0.85em; color: #57606a; }",
      "pre.mermaid { overflow-x: auto; }",
      ".switch { display: block; margin-bottom: 0.75rem; }",
      "body.ready .view-plumbing, body.ready.plumbing .view-default { display: none; }",
      "body.ready.plumbing .view-plumbing { display: block; }",
      "@media (max-width: 800px) {",
      "  body { display: block; }",
      "  nav { position: static; height: auto; border-right: 0; }",
      "  main { padding: 0 1rem 2rem; }",
      "}",
      "@media (prefers-color-scheme: dark) {",
      "  body { background: #0d1117; color: #e6edf3; }",
      "  nav, section { border-color: #30363d; }",
      "  a { color: #58a6ff; }",
      "  .source, .up { color: #8b949e; }",
      "}"
    ]
