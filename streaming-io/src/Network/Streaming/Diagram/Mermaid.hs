{-# LANGUAGE LambdaCase #-}

-- | Draws a program of the "Network.Streaming.Diagram" arrow as Mermaid flowcharts: one chart for each named box. A
-- named box is a single node in the chart of the box it is in, which links to the chart of its own.
--
-- The tree is drawn as it is met. A loop is a tree that contains itself, so the nodes on the way down from the top of a
-- chart are remembered, by their stable names: a node that is met again on its own way down is a back edge, an edge to
-- a junction that is the entry of that node. A node that is shared, and is not on its own way down, is drawn again, with
-- nodes of its own, as often as it is met.
module Network.Streaming.Diagram.Mermaid
  ( Page (..),
    pages,
    Pair (..),
    pairs,
    html,
  )
where

import Control.Applicative ((<|>))
import Control.Exception (evaluate)
import Control.Monad (foldM, forM_, join, unless, when)
import Data.Char (isAlphaNum, toLower)
import Data.Foldable (traverse_)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (find, intercalate, nub, partition)
import Data.Maybe (fromMaybe, isJust, isNothing)
import GHC.Stack (SrcLoc (..))
import Network.Streaming.Annotate (Side (..))
import Network.Streaming.Classes (Label (..))
import Network.Streaming.Diagram
import Network.Streaming.Structure (children, loopHeads, opensInbox, readable)
import System.Mem.StableName (StableName, makeStableName)

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

-- | A node of a chart, by its name in the chart's text.
type Node = String

-- | How an edge is drawn: the edge of a message is dashed too, and is the only one that has a style of its own.
data Style = Solid | Dashed | Message

-- | A way out of a fragment: the node that an edge leaves from, with the label and style of that edge, which are set
-- once the next step is known.
data Outlet = Outlet
  { outletNode :: Node,
    -- | The text that the way out has of its own, such as the answer of a decision.
    outletLabel :: Maybe String,
    -- | The name of the wire that goes out, which is the text of the edge if the way out has none of its own.
    outletWire :: Maybe String,
    -- | The name that a pure step after the node gave the wire. A decision that the wire goes into is asked it as its
    -- question, and any other node has it as the text of the edge, if there is no other.
    outletThen :: Maybe String,
    outletStyle :: Style,
    -- | Whether the node is a junction that joins branches.
    outletJoins :: Bool
  }

-- | What a part of the tree is drawn as: a wire with no node, and the name of its output wire if it has one, or an entry
-- node, whether that node is a junction that splits branches, and the ways out.
data Core
  = Bare (Maybe String)
  | Nodes Node Bool [Outlet]

-- | A part of the tree, drawn: the name of its input wire, and what it is drawn as.
data Fragment = Fragment
  { fragmentInput :: Maybe String,
    fragmentCore :: Core
  }

-- | What a chart is made of, in the order it was made.
data Item
  = Declare Node String
  | -- | A decision, which is drawn with the question that it is asked once the chart is made.
    Decision Node
  | Edge Node Node (Maybe String) Style
  | Group Node Kind String [Item]
  | Click Node String

-- | What a group is drawn as.
data Kind
  = -- | A box around steps, which has the title of what they are in.
    Plain
  | -- | The box around the lanes of the parts that run together.
    Container
  | -- | One part, drawn top to bottom, beside the other lanes of its container.
    Lane
  deriving (Eq)

-- | What a step does at an inbox that an edge is drawn for: opening it, which is the store, posting, and collecting.
data Use = Opens | Posts | Collects
  deriving (Eq)

-- | One end of an exchange of a value, or a step at an inbox, each of which has a name.
data Mark = Mark Side String | Uses Use String
  deriving (Eq)

-- | A node of the chart, with the ends of exchanges that are in the part of the program that it draws.
data Placed = Placed
  { placedNode :: Node,
    placedMarks :: [Mark]
  }

-- | A line of the text of a chart, with the style of its edge if it is an edge.
data Line = Line
  { lineEdge :: Maybe Style,
    lineText :: String
  }

-- | The nodes on the way down from the top of a chart to the node that is drawn, each with the junction that is its
-- entry, if it is the target of a back edge.
type Path = [(StableName Tree, Maybe Node)]

-- | What the drawing of all the pages shares.
newtype World = World (IORef Registry)

-- | The pages in the order they were met, and the boxes that are drawn as the page of another box.
data Registry = Registry
  { registrySlots :: [Slot],
    registryAliases :: [(StableName Tree, String)]
  }

-- | A page that is drawn or being drawn, which a box has by its stable name.
data Slot = Slot
  { slotName :: StableName Tree,
    slotPage :: Page,
    slotDrawn :: Bool,
    -- | Whether a box of the page was met while it was drawn: its anchor is in a chart already, so it can't go.
    slotMet :: Bool
  }

-- | What a chart shows: only the flow of the program, or the plumbing that passes values by as well.
data View = Flow | Plumbing

-- | What a chart is drawn with.
data Ctx = Ctx
  { ctxWorld :: World,
    ctxAnchor :: String,
    ctxView :: View,
    -- | The nodes of the chart that are the targets of back edges.
    ctxHeads :: [StableName Tree],
    ctxCount :: IORef Int,
    ctxItems :: IORef [Item],
    -- | The junctions that are one with the junction after them.
    ctxAliases :: IORef [(Node, Node)],
    -- | The junctions that a back edge goes to.
    ctxTargets :: IORef [Node],
    -- | The decisions, each with the question that it is asked, which is the name of the wire that goes into it.
    ctxQuestions :: IORef [(Node, Maybe String)],
    -- | How many actions without a name have been drawn.
    ctxUnnamed :: IORef Int,
    -- | The nodes that have the ends of exchanges.
    ctxPlaced :: IORef [Placed],
    -- | How many pairs of neighbouring steps have been drawn as one.
    ctxFolds :: IORef Int
  }

-- | What the drawing of a chart came to: its text, and how many of its nodes say nothing of what they mean.
data Drawing = Drawing
  { drawingText :: String,
    drawingUnnamed :: Int,
    drawingUnquestioned :: Int,
    drawingFolds :: Int
  }

-- | A page to draw: the box it is of, and what the chart starts from.
data Subject = Subject
  { subjectName :: StableName Tree,
    subjectLabel :: Label,
    subjectLocation :: Maybe SrcLoc,
    subjectPath :: Path,
    subjectBody :: Tree
  }

-- | The charts of a program: the page of its top box first, and then those of the boxes in it, in the order they are
-- met. A program that is not a box is a page of its own, named "diagram".
pages :: Diagram a b -> IO [Page]
pages (Diagram root) = do
  registry <- newIORef (Registry [] [])
  tree <- evaluate root
  name <- makeStableName tree
  let world = World registry
      subject = case tree of
        Named label location body -> Subject name label location [(name, Nothing)] body
        _ -> Subject name (Label "diagram" "") Nothing [] tree
  _ <- pageOf world Nothing subject
  fmap slotPage . registrySlots <$> readIORef registry

-- | The anchor of the page of a box, which is drawn if the box has none yet.
pageOf :: World -> Maybe String -> Subject -> IO String
pageOf world parent subject = do
  known <- lookupPage world (subjectName subject)
  case known of
    Just anchor -> pure anchor
    Nothing -> do
      anchor <- openPage world parent subject
      drawing <- drawPage world anchor Flow subject
      plumbing <- drawPage world anchor Plumbing subject
      closePage world subject anchor drawing plumbing

-- | The anchor of the page that a box has, and a note that it was met.
lookupPage :: World -> StableName Tree -> IO (Maybe String)
lookupPage (World registry) name = atomicModifyIORef' registry $ \current ->
  case lookup name (registryAliases current) of
    Just anchor -> (current, Just anchor)
    Nothing -> case find ((== name) . slotName) (registrySlots current) of
      Just slot -> (current {registrySlots = met slot <$> registrySlots current}, Just (pageAnchor (slotPage slot)))
      Nothing -> (current, Nothing)
  where
    met slot other = if slotName other == slotName slot then other {slotMet = True} else other

-- | Gives the box a page that is being drawn, with an anchor of its own.
openPage :: World -> Maybe String -> Subject -> IO String
openPage (World registry) parent subject = atomicModifyIORef' registry $ \current ->
  let Label title comment = subjectLabel subject
      taken = pageAnchor . slotPage <$> registrySlots current
      anchor = unique taken (slug title)
      page = Page title comment (subjectLocation subject) anchor parent "" "" 0 0 0
      slot = Slot (subjectName subject) page False False
   in (current {registrySlots = registrySlots current ++ [slot]}, anchor)

-- | The page is drawn. If a page that is drawn has the same title and chart, and no box of this one was met while it was
-- drawn, they are the same page: the anchor is the other's.
closePage :: World -> Subject -> String -> Drawing -> Drawing -> IO String
closePage (World registry) subject anchor drawing plumbing = atomicModifyIORef' registry $ \current ->
  let slots = registrySlots current
      text = drawingText drawing
      mine slot = pageAnchor (slotPage slot) == anchor
      same slot =
        slotDrawn slot
          && pageTitle (slotPage slot) == title
          && pageMermaid (slotPage slot) == text
          && pagePlumbing (slotPage slot) == drawingText plumbing
      Label title _ = subjectLabel subject
      drawn slot =
        if mine slot
          then
            slot
              { slotPage =
                  (slotPage slot)
                    { pageMermaid = text,
                      pagePlumbing = drawingText plumbing,
                      pageUnnamed = drawingUnnamed drawing,
                      pageUnquestioned = drawingUnquestioned drawing,
                      pageFolds = drawingFolds plumbing
                    },
                slotDrawn = True
              }
          else slot
   in case (find same slots, find mine slots) of
        (Just twin, Just slot)
          | not (slotMet slot) ->
              ( current
                  { registrySlots = filter (not . mine) slots,
                    registryAliases = (subjectName subject, pageAnchor (slotPage twin)) : registryAliases current
                  },
                pageAnchor (slotPage twin)
              )
        _ -> (current {registrySlots = drawn <$> slots}, anchor)

-- | The chart in the view, drawn from the body of a box, and how many of its nodes have no name or no question.
drawPage :: World -> String -> View -> Subject -> IO Drawing
drawPage world anchor view subject = do
  heads <- loopHeads boxed (fst <$> subjectPath subject) (subjectBody subject)
  ctx <-
    Ctx world anchor view heads
      <$> newIORef 0
      <*> newIORef []
      <*> newIORef []
      <*> newIORef []
      <*> newIORef []
      <*> newIORef 0
      <*> newIORef []
      <*> newIORef 0
  start <- declare ctx (\node -> node ++ "@{ shape: sm-circ }")
  fragment <- render ctx (subjectPath subject) (subjectBody subject)
  bounded ctx start (outputOf (subjectBody subject)) fragment
  drawn <- reverse <$> readIORef (ctxItems ctx)
  placed <- reverse <$> readIORef (ctxPlaced ctx)
  let items = drawn ++ messages (ancestry [] drawn) placed
  aliases <- readIORef (ctxAliases ctx)
  questions <- readIORef (ctxQuestions ctx)
  unnamed <- readIORef (ctxUnnamed ctx)
  folds <- readIORef (ctxFolds ctx)
  pure (Drawing (chart aliases questions items) unnamed (length (filter (isNothing . snd) questions)) folds)

-- | The name of the output wire of a body, if the body names it.
outputOf :: Tree -> Maybe String
outputOf = \case
  Wires _ output _ -> output
  _ -> Nothing

-- | The edge from the start to the entry of the fragment, and the edges from its ways out to the end. The end is not
-- there if there is no way out.
bounded :: Ctx -> Node -> Maybe String -> Fragment -> IO ()
bounded ctx start output (Fragment input core) = case core of
  Bare out -> do
    end <- declare ctx (\node -> node ++ "@{ shape: fr-circ }")
    edge ctx start end (input <|> out) Solid
  Nodes entry _ outlets -> do
    wire <- into ctx entry input
    edge ctx start entry wire Solid
    unless (null outlets) $ do
      end <- declare ctx (\node -> node ++ "@{ shape: fr-circ }")
      forM_ outlets $ \outlet -> edge ctx (outletNode outlet) end (exitName outlet) (outletStyle outlet)
  where
    -- The way out's own label, such as the side of a decision, and the name of the output wire.
    exitName outlet = case (outletText outlet, output) of
      (Just pending, Just out) | pending /= out -> Just (pending ++ ": " ++ out)
      (pending, out) -> pending <|> out

-- | The anchor for a title: its words, in lower case, joined by dashes, and numbered if another page has it.
slug :: String -> String
slug title = case words (fmap (\c -> if isAlphaNum c then toLower c else ' ') title) of
  [] -> "page"
  ws -> intercalate "-" ws

-- | The anchor, or the first of the numbered anchors after it that is not taken.
unique :: [String] -> String -> String
unique taken anchor = fromMaybe anchor (find (`notElem` taken) candidates)
  where
    candidates = anchor : [anchor ++ "-" ++ show n | n <- [2 :: Int ..]]

-- * Cycles

-- | The nodes that have a chart of their own. The walk that finds the nodes of a chart that are met again on their own
-- way down, which is the walk that 'render' makes without drawing, doesn't go into them.
boxed :: Tree -> Bool
boxed = \case
  Named {} -> True
  End Asking _ _ -> True
  _ -> False

-- * Drawing

-- | A fragment that is a wire only.
bare :: Fragment
bare = Fragment Nothing (Bare Nothing)

-- | A fragment of one node, which is its entry and its way out.
single :: Node -> Fragment
single node = Fragment Nothing (Nodes node False [plain node])

-- | A way out of the node that has no text and no name.
plain :: Node -> Outlet
plain node = Outlet node Nothing Nothing Nothing Solid False

-- | The text of the edge from a way out, if the edge is not into a decision: its own, or else the name of its wire.
outletText :: Outlet -> Maybe String
outletText outlet = outletLabel outlet <|> outletWire outlet <|> outletThen outlet

-- | A new name for a node of the chart.
fresh :: Ctx -> IO Node
fresh ctx = atomicModifyIORef' (ctxCount ctx) (\count -> (count + 1, 'n' : show (count + 1)))

-- | Adds an item to the chart.
emit :: Ctx -> Item -> IO ()
emit ctx item = modifyIORef' (ctxItems ctx) (item :)

-- | Adds a node to the chart, from the text that declares it.
declare :: Ctx -> (Node -> String) -> IO Node
declare ctx text = do
  node <- fresh ctx
  node <$ emit ctx (Declare node (text node))

-- | Adds an edge to the chart.
edge :: Ctx -> Node -> Node -> Maybe String -> Style -> IO ()
edge ctx from to label style = emit ctx (Edge from to label style)

-- | Adds the edge from a way out to the node.
leave :: Ctx -> Node -> Outlet -> IO ()
leave ctx to outlet = edge ctx (outletNode outlet) to (outletText outlet) (outletStyle outlet)

-- | The items that the action adds to the chart, which it leaves out of it.
collect :: Ctx -> IO a -> IO (a, [Item])
collect ctx action = do
  outer <- readIORef (ctxItems ctx)
  writeIORef (ctxItems ctx) []
  result <- action
  inner <- readIORef (ctxItems ctx)
  writeIORef (ctxItems ctx) outer
  pure (result, reverse inner)

-- | The node is a junction, which has no text.
junction :: Node -> String
junction node = node ++ "@{ shape: f-circ }"

-- | The node is a bar that forks or joins parts, which the title says.
bar :: String -> Node -> String
bar title node = node ++ "@{ shape: fork, label: " ++ labelOf title ++ " }"

-- | The text of a node that is a leaf.
leafText :: Shape -> String -> Node -> String
leafText shape text node = case shape of
  Action -> node ++ "[" ++ labelOf (gear ++ text) ++ "]"
  Wait -> node ++ "@{ shape: delay, label: " ++ labelOf text ++ " }"
  Blocking -> node ++ "@{ shape: hourglass }"
  Yielding -> node ++ "@{ shape: flip-tri, label: " ++ labelOf text ++ " }"
  Handover -> node ++ "@{ shape: lean-r, label: " ++ labelOf text ++ " }"
  Round -> node ++ "(" ++ labelOf text ++ ")"
  -- A step at an inbox is drawn as the step it is: a post hands over, a collect waits.
  PostTo _ -> leafText Handover text node
  CollectFrom _ -> leafText Wait text node
  OpenInbox _ -> leafText Round text node
  CloseInbox _ -> leafText Round text node

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

-- | How a box is called when it is a node: its title, and under it its comment.
boxText :: Label -> String
boxText (Label title comment) = if null comment then title else title ++ "\n" ++ comment

-- | Draws a part of the tree. A node that is on the path is a loop, drawn as a way in to the node's junction and no way
-- out, and a node that a loop goes back to gets that junction, if a loop goes back to it.
render :: Ctx -> Path -> Tree -> IO Fragment
render ctx path step = do
  tree <- evaluate step
  case tree of
    Pure -> pure bare
    Named {} -> drawStep ctx path tree
    End Asking _ _ -> drawStep ctx path tree
    _ -> do
      name <- makeStableName tree
      case lookup name path of
        Just (Just target) -> do
          modifyIORef' (ctxTargets ctx) (target :)
          pure (Fragment Nothing (Nodes target False []))
        -- The walk that found the heads met every node that is met again, so this is not a node that is drawn.
        Just Nothing -> pure bare
        Nothing
          | name `elem` ctxHeads ctx -> do
              target <- fresh ctx
              inner <- drawStep ctx ((name, Just target) : path) tree
              headed ctx target inner
          | otherwise -> drawStep ctx ((name, Nothing) : path) tree

-- | The fragment, entered at its junction if a back edge went to it.
headed :: Ctx -> Node -> Fragment -> IO Fragment
headed ctx target inner = do
  targets <- readIORef (ctxTargets ctx)
  if target `notElem` targets
    then pure inner
    else do
      emit ctx (Declare target (junction target))
      case fragmentCore inner of
        Bare _ -> pure (Fragment (fragmentInput inner) (Nodes target False [plain target]))
        Nodes entry _ outlets -> do
          edge ctx target entry Nothing Solid
          pure (Fragment (fragmentInput inner) (Nodes target False outlets))

-- | Draws a node of the tree that is not a pure step, from what it is made of.
drawStep :: Ctx -> Path -> Tree -> IO Fragment
drawStep ctx path = \case
  Pure -> pure bare
  Seq one two -> do
    (chain, count) <- case ctxView ctx of
      Flow -> pure ([], 0)
      Plumbing -> folded <$> ((++) <$> members ctx one <*> members ctx two)
    if count == 0
      then do
        before <- render ctx path one
        after <- render ctx path two
        sequential ctx before after
      else do
        modifyIORef' (ctxFolds ctx) (+ count)
        render ctx path (foldr1 Seq (memberTree <$> chain))
  First inner -> render ctx path inner >>= partial ctx
  Second inner -> render ctx path inner >>= partial ctx
  Par one two -> fanned ctx path (\case Par a b -> Just (a, b); _ -> Nothing) one two
  Fanout one two -> fanned ctx path (\case Fanout a b -> Just (a, b); _ -> Nothing) one two
  Ap function argument -> applied ctx function argument >>= traverse (render ctx path) >>= branched ctx
  Left' inner -> do
    taken <- render ctx path inner
    decided ctx [Branch "Left" taken, Branch "Right" bare]
  Right' inner -> do
    taken <- render ctx path inner
    decided ctx [Branch "Left" bare, Branch "Right" taken]
  Sum one two -> chosen ctx path (\case Sum a b -> Just (a, b); _ -> Nothing) one two
  Choice one two -> chosen ctx path (\case Choice a b -> Just (a, b); _ -> Nothing) one two
  Together parts -> laned ctx path parts
  Grouped inner -> grouped ctx "group" (render ctx path inner)
  Spawned inner -> spawned ctx path inner
  Race one two -> traverse (render ctx path) [one, two] >>= barred ctx "race"
  Leaf text how -> do
    when (text == unnamedAction) $ modifyIORef' (ctxUnnamed ctx) (+ 1)
    node <- declare ctx (leafText how text)
    placing ctx node (leafMarks how)
    pure (single node)
  Scope acquire release body -> scoped ctx path acquire release body
  Try inner -> grouped ctx "try" (render ctx path inner)
  Shield inner -> grouped ctx "shield" (render ctx path inner)
  Within duration inner -> grouped ctx ("within " ++ readable duration) (render ctx path inner)
  TimeoutA inner -> grouped ctx "within the duration of the input" (render ctx path inner)
  Wires given output (FlowStep name bindings inner) ->
    wired Nothing output <$> flowStepped ctx path given name bindings inner
  Wires input output inner -> wired input output <$> render ctx path inner
  FlowStep name bindings inner -> flowStepped ctx path Nothing name bindings inner
  Allowing _ _ inner -> render ctx path inner
  tree@(Named label location body) -> named ctx path tree label location body
  tree@(End Asking exchanged body) -> awaiting ctx tree exchanged body
  End Answering exchanged body -> answered ctx path exchanged body

-- | The way out, with the name of its wire if it has none.
labelled :: Maybe String -> Outlet -> Outlet
labelled wire outlet = outlet {outletWire = outletWire outlet <|> wire}

-- | The way out of a node that a pure step follows, with the name of the wire that the step takes if it has none, and
-- the name that the step gives the wire.
followed :: Maybe String -> Maybe String -> Outlet -> Outlet
followed taken given outlet =
  outlet {outletWire = outletWire outlet <|> taken, outletThen = given <|> outletThen outlet}

-- | The fragment with the names of its wires, where it has none.
wired :: Maybe String -> Maybe String -> Fragment -> Fragment
wired input output (Fragment inner core) = Fragment (inner <|> input) $ case core of
  Bare out -> Bare (out <|> output)
  Nodes entry splits outlets -> Nodes entry splits (labelled output <$> outlets)

-- | A step of a flow: a box that has the name of the step for its title, if it has one. The edge into it says what its
-- inputs are bound to, in place of any name that the step's own wire has, and after the name that the wire into it was
-- given, if it was, as the case of a branch is.
flowStepped :: Ctx -> Path -> Maybe String -> Maybe String -> [String] -> Tree -> IO Fragment
flowStepped ctx path given name bindings inner = do
  Fragment input core <- maybe id (grouped ctx) name (render ctx path inner)
  pure (Fragment (onEdge input) core)
  where
    bound = if null bindings then Nothing else Just (intercalate ", " bindings)
    onEdge input = case (given, bound) of
      (Just caseName, Just inputs) -> Just (caseName ++ ": " ++ inputs)
      _ -> bound <|> input <|> given

-- | One fragment after the other, with the edges labelled by the way out, or else by the input wire of the second.
sequential :: Ctx -> Fragment -> Fragment -> IO Fragment
sequential ctx (Fragment input1 core1) (Fragment input2 core2) = case (core1, core2) of
  (Bare out1, Bare out2) -> pure (Fragment (input1 <|> out1 <|> input2) (Bare (out2 <|> out1)))
  (Bare out1, Nodes entry2 _ _) -> do
    wire <- into ctx entry2 out1
    pure (Fragment (wire <|> input2 <|> input1) core2)
  (Nodes entry splits outlets, Bare out2) ->
    pure (Fragment input1 (Nodes entry splits (followed input2 out2 <$> outlets)))
  (Nodes entry splits outlets, Nodes entry2 splits2 outlets2) -> do
    connect ctx outlets input2 entry2 splits2
    pure (Fragment input1 (Nodes entry splits outlets2))

-- | Draws the edges from the ways out to the entry, unless a join and a split that follows it can be one junction.
connect :: Ctx -> [Outlet] -> Maybe String -> Node -> Bool -> IO ()
connect ctx outlets label entry splits = case outlets of
  [ Outlet
      { outletNode = joined,
        outletLabel = Nothing,
        outletWire = Nothing,
        outletThen = Nothing,
        outletStyle = Solid,
        outletJoins = True
      }
    ]
    | splits && isNothing label -> modifyIORef' (ctxAliases ctx) ((entry, joined) :)
  _ -> forM_ outlets $ \outlet -> do
    later <- into ctx entry (outletThen outlet)
    wire <- into ctx entry (outletWire outlet <|> label)
    edge ctx (outletNode outlet) entry (outletLabel outlet <|> wire <|> later) (outletStyle outlet)

-- | A step on a part of a value: with the plumbing it is a split, the step, and a join, with a dashed edge for the part
-- that the step leaves, and without it only the step, as what passes by is no flow of the program.
partial :: Ctx -> Fragment -> IO Fragment
partial ctx inner = case ctxView ctx of
  Flow -> pure inner
  Plumbing -> bypassed ctx inner

-- | A step on a part of a value, drawn with the plumbing: a split, the step, and a join, with a dashed edge for the
-- part that the step leaves.
bypassed :: Ctx -> Fragment -> IO Fragment
bypassed ctx inner = case fragmentCore inner of
  Bare _ -> pure inner
  Nodes entry _ outlets -> do
    split <- declare ctx junction
    joined <- declare ctx junction
    wire <- into ctx entry (fragmentInput inner)
    edge ctx split entry wire Solid
    traverse_ (leave ctx joined) outlets
    edge ctx split joined Nothing Dashed
    pure (Fragment Nothing (Nodes split True [(plain joined) {outletJoins = True}]))

-- | A step of a chain of @>>>@, with whether a loop goes back to it.
data Member = Member
  { memberLoops :: Bool,
    memberTree :: Tree
  }

-- | The steps of a chain of @>>>@, in order: the chain ends at a node that is not one, and at a node that a loop goes
-- back to.
members :: Ctx -> Tree -> IO [Member]
members ctx step = do
  tree <- evaluate step
  name <- makeStableName tree
  let loops = name `elem` ctxHeads ctx
  case tree of
    Seq one two | not loops -> (++) <$> members ctx one <*> members ctx two
    _ -> pure [Member loops tree]

-- | The steps with each pair of neighbours that the arrow laws make one step made one, and how many pairs that was. It
-- is for the view with the plumbing, whose steps on a part of a value it joins: without it they are only their steps.
-- Nothing is folded across a step that is not a plain 'First' or 'Second', such as a pure step or a box, or one that a
-- loop goes back to. The new step may be folded with the next, so that one pass is all there is to do.
folded :: [Member] -> ([Member], Int)
folded = go [] 0
  where
    go done count = \case
      [] -> (reverse done, count)
      next : rest -> case done of
        previous : before | Just one <- fuse previous next -> go (one : before) (count + 1) rest
        _ -> go (next : done) count rest

-- | The step that two neighbours are: @first f >>> first g@ is @first (f >>> g)@, and likewise for 'Second'. A 'First'
-- and a 'Second' are not made one, as side by side they would hide that the effects of the first run before the second.
fuse :: Member -> Member -> Maybe Member
fuse one two
  | memberLoops one || memberLoops two = Nothing
  | otherwise = fmap (Member False) $ case (memberTree one, memberTree two) of
      (First f, First g) -> Just (First (Seq f g))
      (Second f, Second g) -> Just (Second (Seq f g))
      _ -> Nothing

-- | One operand of a chain, with the path of 'Left' and 'Right' that leads to it from the top of the chain.
data Operand = Operand [String] Tree

-- | The operands of a chain of one operator, in order, with the path to each: the chain ends at a node that is not the
-- operator, and at a node that a loop goes back to.
operands :: Ctx -> (Tree -> Maybe (Tree, Tree)) -> Tree -> Tree -> IO [Operand]
operands ctx split one two = (++) <$> go ["Left"] one <*> go ["Right"] two
  where
    go at step = do
      tree <- evaluate step
      name <- makeStableName tree
      case split tree of
        Just (a, b) | name `notElem` ctxHeads ctx -> (++) <$> go (at ++ ["Left"]) a <*> go (at ++ ["Right"]) b
        _ -> pure [Operand at tree]

-- | The operands of a chain of @<*>@ that nests to the left, where a step with a pure step after it, as @<$>@ makes it,
-- is the step, and a pure step alone is no operand.
applied :: Ctx -> Tree -> Tree -> IO [Tree]
applied ctx function argument = (++ [argument]) <$> go function
  where
    go step = do
      tree <- evaluate step
      name <- makeStableName tree
      case tree of
        Ap a b | name `notElem` ctxHeads ctx -> (++ [b]) <$> go a
        Seq inner after | name `notElem` ctxHeads ctx -> do
          ended <- evaluate after
          case ended of
            Pure -> pure [inner]
            _ -> pure [tree]
        Pure -> pure []
        _ -> pure [tree]

-- | The steps of a chain that runs on the same input or on parts of it, side by side.
fanned :: Ctx -> Path -> (Tree -> Maybe (Tree, Tree)) -> Tree -> Tree -> IO Fragment
fanned ctx path split one two = operands ctx split one two >>= traverse (render ctx path . operandStep) >>= branched ctx
  where
    operandStep (Operand _ step) = step

-- | The branches, between a junction that splits and a junction that joins.
branched :: Ctx -> [Fragment] -> IO Fragment
branched ctx = \case
  [] -> pure bare
  [only] -> pure only
  branches -> fork ctx junction Nothing branches

-- | A part, which is met as often as it is a part of a chain of parts that run together.
data Same = Same
  { sameName :: StableName Tree,
    sameTree :: Tree,
    sameCount :: Int
  }

-- | A part drawn: how many parts it stands for, what it is drawn as, and the items of its lane.
data Drawn = Drawn
  { drawnCount :: Int,
    drawnFragment :: Fragment,
    drawnItems :: [Item],
    -- | The items as text, with the nodes named by their order, which is the same for parts that are drawn alike.
    drawnText :: String
  }

-- | The parts that are one value, each once, in the order that they are first met, with how many times it is met.
sameValue :: [Tree] -> IO [Same]
sameValue = foldM meet []
  where
    meet found step = do
      tree <- evaluate step
      name <- makeStableName tree
      pure $ case break ((== name) . sameName) found of
        (before, twin : after) -> before ++ twin {sameCount = sameCount twin + 1} : after
        _ -> found ++ [Same name tree 1]

-- | The part, drawn in a lane of its own.
drawPart :: Ctx -> Path -> Same -> IO Drawn
drawPart ctx path same = do
  (fragment, items) <- collect ctx (render ctx path (sameTree same))
  Drawn (sameCount same) fragment items <$> alike ctx items

-- | The text of the items, with each node that they declare named by its order, so that parts that are drawn alike have
-- the same text.
alike :: Ctx -> [Item] -> IO String
alike ctx items = do
  aliases <- readIORef (ctxAliases ctx)
  questions <- readIORef (ctxQuestions ctx)
  let tidied = tidy aliases questions items
      numbered = zip (declared tidied) (("m" ++) . show <$> [1 :: Int ..])
      (written, outside) = layout "" (renamed (\node -> fromMaybe node (lookup node numbered)) tidied)
  pure (unlines (fmap lineText written ++ fmap edgeLine outside))

-- | The items with their nodes renamed. A declaration of a node starts with the name of the node.
renamed :: (Node -> Node) -> [Item] -> [Item]
renamed rename = fmap $ \case
  Declare node text -> Declare (rename node) (rename node ++ drop (length node) text)
  Decision node -> Decision (rename node)
  Edge from to label style -> Edge (rename from) (rename to) label style
  Group node kind title inner -> Group (rename node) kind title (renamed rename inner)
  Click node anchor -> Click (rename node) anchor

-- | The parts that are drawn alike as one, which stands for all of them, where the first of them is.
merged :: [Drawn] -> [Drawn]
merged = foldl' merge []
  where
    merge found part = case break (alikeTo part) found of
      (before, twin : after) -> before ++ twin {drawnCount = drawnCount twin + drawnCount part} : after
      _ -> found ++ [part]
    alikeTo part other = not (null (drawnItems part)) && drawnText other == drawnText part

-- | The parts that run together, each in a lane of its own, side by side in a container. The parts that are one value,
-- or are drawn alike, are one lane, which is titled by how many they are if that is more than one. Nothing leaves a
-- lane, as the parts have no result, and a part that is a loop back to a node outside the container has the container
-- for an edge. If no part has a lane there is nothing to put in a container, and the parts are between junctions.
laned :: Ctx -> Path -> [Tree] -> IO Fragment
laned ctx path parts = do
  drawn <- merged <$> (sameValue parts >>= traverse (drawPart ctx path))
  case [part | part <- drawn, not (null (drawnItems part))] of
    [] -> branched ctx (drawnFragment <$> drawn)
    lanes -> do
      container <- fresh ctx
      -- Mermaid places the lanes in the opposite order to their declarations, so the first part is the leftmost.
      groups <- traverse lane (reverse lanes)
      -- U+2016, the double bar of parallel.
      emit ctx (Group container Container "\x2016 together" groups)
      forM_ [entry | Drawn _ (Fragment _ (Nodes entry _ _)) [] _ <- drawn] $ \entry ->
        edge ctx container entry Nothing Solid
      pure (Fragment Nothing (Nodes container False [plain container]))
  where
    lane part = (\node -> Group node Lane (title part) (drawnItems part)) <$> fresh ctx
    -- U+00D7, the multiplication sign.
    title part = if drawnCount part > 1 then '\xd7' : ' ' : show (drawnCount part) else " "

-- | The branches, between bars that fork and join, which the title is on.
barred :: Ctx -> String -> [Fragment] -> IO Fragment
barred ctx title = \case
  [] -> pure bare
  [only] -> pure only
  branches -> fork ctx (bar title) (Just title) branches

-- | The branches side by side, between a node that splits and a node that joins, and a wire alone goes from one to the
-- other. With no title the nodes are junctions, and with a title they are bars, which Mermaid draws without a label, so
-- the edges from the split carry the title, unless the branch has the name of a wire to put there.
fork :: Ctx -> (Node -> String) -> Maybe String -> [Fragment] -> IO Fragment
fork ctx text title branches = do
  split <- declare ctx text
  joined <- declare ctx text
  forM_ branches $ \branch -> case fragmentCore branch of
    Bare out -> edge ctx split joined (out <|> fragmentInput branch <|> title) Solid
    Nodes entry _ outlets -> do
      wire <- into ctx entry (fragmentInput branch)
      edge ctx split entry (wire <|> title) Solid
      traverse_ (leave ctx joined) outlets
  pure (Fragment Nothing (Nodes split junctions [(plain joined) {outletJoins = junctions}]))
  where
    junctions = isNothing title

-- | A branch of a decision, with the name of the edge to it.
data Branch = Branch String Fragment

-- | A chain of one operator as one decision, with the path of 'Left' and 'Right' to each operand as its name.
chosen :: Ctx -> Path -> (Tree -> Maybe (Tree, Tree)) -> Tree -> Tree -> IO Fragment
chosen ctx path split one two = do
  found <- operands ctx split one two
  traverse (\(Operand at step) -> Branch (unwords at) <$> render ctx path step) found >>= decided ctx

-- | A decision with an edge to each branch and no join: the ways out of the branches are the ways out of the fragment.
-- It is asked the name of the wire that goes into it, as its question, if the wire has one.
decided :: Ctx -> [Branch] -> IO Fragment
decided ctx branches = do
  decision <- fresh ctx
  emit ctx (Decision decision)
  modifyIORef' (ctxQuestions ctx) ((decision, Nothing) :)
  outlets <- concat <$> traverse (follow decision) branches
  pure (Fragment Nothing (Nodes decision False outlets))
  where
    -- The edge is the answer: the name of the wire that goes into the branch, or else the name of the branch.
    follow decision (Branch name branch) = case fragmentCore branch of
      Bare out -> pure [(plain decision) {outletLabel = Just (answer name branch), outletThen = out}]
      Nodes entry _ outlets -> do
        edge ctx decision entry (Just (answer name branch)) Solid
        pure outlets
    answer name branch = fromMaybe name (fragmentInput branch)

-- | The text of an edge into a node, given the name of the wire that goes into it. A decision that has no question is
-- asked the name of its wire, and the edge into it then has no text, as the same name is not said twice.
into :: Ctx -> Node -> Maybe String -> IO (Maybe String)
into ctx entry wire = do
  questions <- readIORef (ctxQuestions ctx)
  case (lookup entry questions, wire) of
    (Just Nothing, Just name) -> Nothing <$ writeIORef (ctxQuestions ctx) (asked name <$> questions)
    (Just (Just question), Just name) | question == name -> pure Nothing
    _ -> pure wire
  where
    asked name (node, question) = if node == entry then (node, Just name) else (node, question)

-- | The fragment that the action draws, with its nodes in a group of its own, which has no node if the fragment has none.
grouped :: Ctx -> String -> IO Fragment -> IO Fragment
grouped ctx title draw = do
  (fragment, items) <- collect ctx draw
  case fragmentCore fragment of
    Bare _ -> pure fragment
    Nodes {} -> do
      node <- fresh ctx
      fragment <$ emit ctx (Group node Plain title items)

-- | A spawn: the part in a box labelled "spawn", which the input of the spawn goes into and the part that spawns it goes on from,
-- as it goes on at once. No edge leaves the part: it runs on its own.
spawned :: Ctx -> Path -> Tree -> IO Fragment
spawned ctx path inner = do
  (fragment, items) <- collect ctx (render ctx path inner)
  case fragmentCore fragment of
    Bare _ -> single <$> declare ctx (leafText Round "spawn")
    Nodes {} -> do
      node <- fresh ctx
      emit ctx (Group node Plain "spawn" items)
      pure (Fragment (fragmentInput fragment) (Nodes node False [plain node]))

-- | A scope as a group: the body after the acquire, and the release after it by a dashed edge named for the exit, from a
-- junction if the acquire has no node.
scoped :: Ctx -> Path -> Tree -> Tree -> Tree -> IO Fragment
scoped ctx path acquire release body = grouped ctx "scope" $ do
  -- A scope that opens an inbox draws it as a store, a cylinder, which the posts and the collects of its name have edges to.
  forM_ (opensInbox 16 acquire) $ \name -> do
    store <- declare ctx (\node -> node ++ "[(" ++ labelOf name ++ ")]")
    placing ctx store [Uses Opens name]
  acquired <- render ctx path acquire
  running <- render ctx path body
  released <- render ctx path release
  start <- case fragmentCore acquired of
    Bare out -> do
      node <- declare ctx junction
      pure (Fragment (fragmentInput acquired) (Nodes node False [(plain node) {outletThen = out}]))
    Nodes {} -> pure acquired
  _ <- sequential ctx (onExit start) released
  sequential ctx start running
  where
    onExit (Fragment input core) = Fragment input $ case core of
      Nodes entry splits outlets -> Nodes entry splits (dashed <$> outlets)
      Bare out -> Bare out
    dashed outlet = outlet {outletLabel = Just "on exit", outletStyle = Dashed}

-- | A named box: its only step, called by its name, or else a node that links to the page of the box.
named :: Ctx -> Path -> Tree -> Label -> Maybe SrcLoc -> Tree -> IO Fragment
named ctx path tree label location body = case oneStep 8 (boxText label) body of
  Just step -> render ctx path step
  Nothing -> do
    name <- makeStableName tree
    anchor <- pageOf (ctxWorld ctx) (Just (ctxAnchor ctx)) (Subject name label location [(name, Nothing)] body)
    node <- declare ctx (\n -> n ++ "[[" ++ labelOf (boxText label) ++ "]]")
    endsOf body >>= placing ctx node
    linked ctx node anchor body

-- | A node that links to the page of a box, which has the names of the wires of the body, if it has them.
linked :: Ctx -> Node -> String -> Tree -> IO Fragment
linked ctx node anchor body = do
  emit ctx (Click node anchor)
  pure $ case body of
    Wires input output _ -> Fragment input (Nodes node False [(plain node) {outletWire = output}])
    _ -> single node

-- | The asking end of an exchange: a node of its own, with the name of what it awaits, and under it the steps that hand
-- the ticket out and dispose of a value that comes late, where they are named. It links to the page of the mechanics.
awaiting :: Ctx -> Tree -> String -> Tree -> IO Fragment
awaiting ctx tree exchanged body = do
  name <- makeStableName tree
  anchor <- pageOf (ctxWorld ctx) (Just (ctxAnchor ctx)) (Subject name label Nothing [(name, Nothing)] body)
  node <- declare ctx (\n -> n ++ "@{ shape: notch-rect, label: " ++ labelOf (title ++ detail) ++ " }")
  endsOf body >>= placing ctx node . (Mark Asking exchanged :)
  linked ctx node anchor body
  where
    title = envelope ++ "awaits " ++ exchanged
    label = Label ("awaits " ++ exchanged) comment
    comment = "Hands out a ticket for " ++ exchanged ++ ", waits for its promise to be fulfilled, and abandons it otherwise."
    detail = maybe "" ("\n" ++) (steps body)

-- | The sign of an exchange, which is an envelope (U+2709), and a space.
envelope :: String
envelope = "\x2709 "

-- | The step as the answering end of an exchange: drawn as it is, with the name of the exchange on a line of its own in
-- its label if it is one node.
answered :: Ctx -> Path -> String -> Tree -> IO Fragment
answered ctx path exchanged body = do
  fragment <- render ctx path (marked (envelope ++ exchanged) body)
  marks <- endsOf body
  case fragmentCore fragment of
    Nodes entry _ _ -> placing ctx entry (Mark Answering exchanged : marks)
    Bare _ -> pure ()
  pure fragment

-- | The step with a line added to its label, if it is a leaf, or a box that is drawn as one.
marked :: String -> Tree -> Tree
marked line = \case
  Leaf text how -> Leaf (text ++ "\n" ++ line) how
  Named (Label title comment) location body
    | isJust (oneStep 8 "" body) -> Named (Label title (unwords (filter (not . null) [comment, line]))) location body
  Wires input output inner -> Wires input output (marked line inner)
  Allowing check reason inner -> Allowing check reason (marked line inner)
  step -> step

-- | The names of the steps of a ticket's scope, where the scope is in the tree: the ones that hand the ticket out, and
-- the ones that dispose of a value that comes late.
steps :: Tree -> Maybe String
steps body = do
  (acquire, release) <- scopeIn 16 body
  case (titles 16 acquire, titles 16 release) of
    ([], []) -> Nothing
    (handOut, dispose) ->
      Just . intercalate " \xb7 " $
        [intercalate ", " handOut | not (null handOut)] ++ ["late: " ++ intercalate ", " dispose | not (null dispose)]

-- | What a scope acquires and releases, if it is the tree or what a chain of steps is made of, within as many nodes as
-- the fuel.
scopeIn :: Int -> Tree -> Maybe (Tree, Tree)
scopeIn fuel step
  | fuel <= 0 = Nothing
  | otherwise = case step of
      Scope acquire release _ -> Just (acquire, release)
      Seq one two -> scopeIn (fuel - 1) one <|> scopeIn (fuel - 1) two
      Wires _ _ inner -> scopeIn (fuel - 1) inner
      End _ _ inner -> scopeIn (fuel - 1) inner
      Allowing _ _ inner -> scopeIn (fuel - 1) inner
      _ -> Nothing

-- | The titles of the boxes in the tree, each once, within as many nodes as the fuel: it doesn't go into a box.
titles :: Int -> Tree -> [String]
titles fuel step
  | fuel <= 0 = []
  | otherwise = case step of
      Named (Label title _) _ _ -> [title]
      _ -> nub (concatMap (titles (fuel - 1)) (children step))

-- | The marks of a step at an inbox: a post and a collect have edges of their own to the store of the name.
leafMarks :: Shape -> [Mark]
leafMarks = \case
  PostTo name -> [Uses Posts name]
  CollectFrom name -> [Uses Collects name]
  _ -> []

-- | The marks that a box has for the steps in it, which the opening of an inbox is one of: the box stands for the store, on
-- the page of the box whose own page has no store of that name.
liftedMarks :: Shape -> [Mark]
liftedMarks = \case
  OpenInbox name -> [Uses Opens name]
  how -> leafMarks how

-- | The ends of exchanges and the steps at inboxes that are in the tree, each once, which it walks as it is, into the boxes
-- too.
endsOf :: Tree -> IO [Mark]
endsOf root = do
  seen <- newIORef []
  found <- newIORef []
  let visit step = do
        tree <- evaluate step
        name <- makeStableName tree
        known <- elem name <$> readIORef seen
        unless known $ do
          modifyIORef' seen (name :)
          case tree of
            End side exchanged _ -> modifyIORef' found (\marks -> nub (marks ++ [Mark side exchanged]))
            Leaf _ how -> modifyIORef' found (\marks -> nub (marks ++ liftedMarks how))
            _ -> pure ()
          traverse_ visit (children tree)
  visit root
  readIORef found

-- | Notes that the node has the ends of exchanges, if there are any.
placing :: Ctx -> Node -> [Mark] -> IO ()
placing ctx node marks = unless (null marks) (modifyIORef' (ctxPlaced ctx) (Placed node marks :))

-- | The groups that each node of the items is in, from the outermost.
ancestry :: [(Node, Kind)] -> [Item] -> [(Node, [(Node, Kind)])]
ancestry around items =
  concat
    [ case item of
        Declare node _ -> [(node, around)]
        Group node kind _ inner -> ancestry (around ++ [(node, kind)]) inner
        _ -> []
      | item <- items
    ]

-- | The edges of the messages of exchanges: from a node that has the answering end to a node that has the asking end of
-- an exchange of the same name, for each pair of such nodes. An end in a lane is joined at the lane, as an edge to a
-- node in it would make Mermaid forget the lane's direction, unless the other end is in the lane too. The edges of the
-- messages of inboxes are drawn the same way, from the posts to the store of an inbox, and from the store to the collects.
messages :: [(Node, [(Node, Kind)])] -> [Placed] -> [Item]
messages around placed =
  [ Edge (joined from to) (joined to from) (Just (envelope ++ exchanged)) Message
    | answer <- placed,
      Mark Answering exchanged <- placedMarks answer,
      ask <- placed,
      let from = placedNode answer
          to = placedNode ask,
      from /= to,
      Mark Asking awaited <- placedMarks ask,
      awaited == exchanged
  ]
    ++ stores
  where
    groupsAround node = fromMaybe [] (lookup node around)
    joined node other =
      let mine = groupsAround node
          shared = length (takeWhile id (zipWith (==) (fst <$> mine) (fst <$> groupsAround other)))
       in case [group | (group, Lane) <- drop shared mine] of
            group : _ -> group
            [] -> node
    -- The posts to an inbox have an edge to its store, and the store has an edge to the collects from it, which are lifted to
    -- boxes and lanes as the messages are. A node that the chart doesn't have, as one of a part that was drawn as another, has none.
    stores =
      concat
        [ [Edge (joined user store) (joined store user) Nothing Message | user <- using Posts name, user /= store]
            ++ [Edge (joined store user) (joined user store) Nothing Message | user <- using Collects name, user /= store]
          | (store, name) <- nub [(placedNode opening, name) | opening <- placed, Uses Opens name <- placedMarks opening],
            store `elem` fmap fst around
        ]
    using use name =
      nub
        [ placedNode user
          | user <- placed,
            Uses found called <- placedMarks user,
            found == use,
            called == name,
            placedNode user `elem` fmap fst around
        ]

-- | The step that a body is, called by the text, if it is a leaf or a function with wires and pure steps about it, within
-- as many nodes as the fuel, so that a loop of pure steps ends.
oneStep :: Int -> String -> Tree -> Maybe Tree
oneStep fuel text step
  | fuel <= 0 = Nothing
  | otherwise = case step of
      Leaf _ how -> Just (Leaf text how)
      Pure -> Just (Leaf text Round)
      Wires input output inner -> Wires input output <$> oneStep (fuel - 1) text inner
      Allowing _ _ inner -> oneStep (fuel - 1) text inner
      Seq Pure inner -> oneStep (fuel - 1) text inner
      Seq inner Pure -> oneStep (fuel - 1) text inner
      _ -> Nothing

-- * The text of a chart

-- | The chart, from its items, with the junctions that are one named alike.
chart :: [(Node, Node)] -> [(Node, Maybe String)] -> [Item] -> String
chart aliases questions items =
  unlines ("flowchart TD" : fmap ("    " ++) (fmap lineText body ++ clicks items' ++ classes items' ++ linkStyles body))
  where
    items' = tidy aliases questions items
    (written, outside) = layout "" items'
    body = written ++ fmap (edgeLined "") outside

-- | The items with the nodes that are named by their junction renamed, and the declarations of those left out, and the
-- decisions declared with their questions, or with "choose" if they have none.
tidy :: [(Node, Node)] -> [(Node, Maybe String)] -> [Item] -> [Item]
tidy aliases questions = concatMap $ \case
  Declare node _ | node `elem` fmap fst aliases -> []
  Decision node -> [Declare node (node ++ "{" ++ labelOf (fromMaybe "choose" (join (lookup node questions))) ++ "}")]
  Edge from to label style -> [Edge (resolve from) (resolve to) label style]
  Group node kind title inner -> [Group node kind title (tidy aliases questions inner)]
  item -> [item]
  where
    resolve node = maybe node resolve (lookup node aliases)

-- | The nodes that the items declare, in groups too.
declared :: [Item] -> [Node]
declared items =
  concat
    [ case item of
        Declare node _ -> [node]
        Group node _ _ inner -> node : declared inner
        _ -> []
      | item <- items
    ]

-- | The lines of the items, indented, and the edges among them that are not between nodes that they declare. A node that
-- an edge names inside a group is a node of the group, so an edge to a node outside it is written outside.
layout :: String -> [Item] -> ([Line], [Item])
layout indent items = (concat (fmap fst parts) ++ fmap (edgeLined indent) kept, escapedEdges)
  where
    own = declared items
    parts = fmap place items
    place = \case
      Declare _ text -> ([Line Nothing (indent ++ text)], [])
      Group node kind title inner ->
        let (inside, up) = layout (indent ++ "  ") inner
            opening = indent ++ "subgraph " ++ node ++ " [" ++ quoted title ++ "]"
            direction = [indent ++ "  direction TB" | kind == Lane]
         in (fmap (Line Nothing) ([opening] ++ direction) ++ inside ++ [Line Nothing (indent ++ "end")], up)
      _ -> ([], [])
    (kept, escapedEdges) = partition (between own) ([item | item@Edge {} <- items] ++ concatMap snd parts)
    between nodes = \case
      Edge from to _ _ -> from `elem` nodes && to `elem` nodes
      _ -> True

-- | The line of an edge.
edgeLine :: Item -> String
edgeLine = \case
  Edge from to label style -> from ++ arrow style label ++ to
  _ -> ""
  where
    arrow style = \case
      Nothing -> case style of Solid -> " --> "; _ -> " -.-> "
      Just text -> case style of Solid -> " -->|" ++ quoted text ++ "| "; _ -> " -.->|" ++ quoted text ++ "| "

-- | The line of an edge, indented.
edgeLined :: String -> Item -> Line
edgeLined indent item = Line (case item of Edge _ _ _ style -> Just style; _ -> Nothing) (indent ++ edgeLine item)

-- | The style of the edges of messages, which are known by their numbers: Mermaid numbers the edges in the order of the
-- text.
linkStyles :: [Line] -> [String]
linkStyles body = case [number | (number, Message) <- zip [0 :: Int ..] [style | Just style <- lineEdge <$> body]] of
  [] -> []
  numbers ->
    ["linkStyle " ++ intercalate "," (show <$> numbers) ++ " stroke:#d08c3c,stroke-width:2px,stroke-dasharray:3 3"]

-- | The lines of the links of the boxes in the items, which the chart ends with.
clicks :: [Item] -> [String]
clicks = concatMap $ \case
  Click node anchor -> ["click " ++ node ++ " href " ++ quoted ('#' : anchor)]
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
