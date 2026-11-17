{-# LANGUAGE LambdaCase #-}

-- | The graph that the diagram of a program is drawn from, and the walk of the program's tree that makes it: one graph for
-- each named box. A named box is a single element in the graph of the box it is in, which links to the graph of its own.
-- "Network.Streaming.Diagram.Mermaid" and "Network.Streaming.Diagram.Graphviz" print the graph; neither walks the tree.
--
-- A graph is a list of items: elements, the clusters that hold elements, and the edges between them. An element has the pins
-- that its edges end at: its input pins, which are the ports that read a name, and its output pins, which are the names that
-- it gives, nested as the pattern that takes its output apart nests, or the cases of a decision. Mermaid has no pins, and
-- draws a name as a node, or as a box that holds the nodes of its parts, so the graph has those as well, as elements and
-- clusters that are marked as the way Mermaid draws a name, and an edge from one of them says in its joins where it is from
-- on pins.
--
-- The tree is drawn as it is met. A loop is a tree that contains itself, so the nodes on the way down from the top of a
-- chart are remembered, by their stable names: a node that is met again on its own way down is a back edge, an edge to
-- a junction that is the entry of that node. A node that is shared, and is not on its own way down, is drawn again, with
-- nodes of its own, as often as it is met.
module Network.Streaming.Diagram.Graph
  ( Node,
    Style (..),
    Look (..),
    Form (..),
    Pin (..),
    Seat (..),
    Join (..),
    Sits (..),
    Edge (..),
    Kind (..),
    Element (..),
    Item (..),
    Graph (..),
    Sheet (..),
    sheets,
    Laid (..),
    Link (..),
    layout,
    hostsOut,
    hostsIn,
    declared,
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
import Network.Streaming.Annotate (Binding (..), Part (..), Side (..))
import Network.Streaming.Classes (Label (..))
import Network.Streaming.Diagram
import Network.Streaming.Structure (children, loopHeads, opensInbox, readable)
import System.Mem.StableName (StableName, makeStableName)

-- * The graph

-- | A node of a graph, by its name in the text of the charts.
type Node = String

-- | How an edge is drawn: the edge of a message is dashed too, and so is the edge that says only the order of the lines of a
-- flow, and they are the ones that have a style of their own. The edge into the entry of a flow from the node that the flow
-- follows is solid, and Mermaid draws no edge for it: its entry is one with that node.
data Style = Solid | Dashed | Message | Order | Entry
  deriving (Eq)

-- | How a step that is a leaf is drawn.
data Look
  = -- | A step that never waits.
    LookAction
  | -- | A wait.
    LookWait
  | -- | A block.
    LookBlocking
  | -- | A point where the part gives way.
    LookYielding
  | -- | A handover.
    LookHandover
  | -- | A step that is none of those.
    LookRound
  deriving (Eq)

-- | What an element is.
data Form
  = -- | The start of the chart.
    Start
  | -- | The end of the chart.
    Finish
  | -- | A junction, which has no text.
    Junction
  | -- | The entry of a flow: the names of its input that the flow reads, and the ports that an outer flow binds, are its pins.
    -- Mermaid draws a junction, unless the node that the flow follows is one with it.
    Entered
  | -- | A bar that forks or joins parts, which the title is said on the edges of.
    Bar String
  | -- | A leaf: how it is drawn, and its text.
    Step Look String
  | -- | A box that has a page of its own: the text of its title, and under it its comment.
    Titled String
  | -- | The asking end of an exchange: its text.
    Awaits String
  | -- | The store of an inbox, by the name of the inbox.
    Store String
  | -- | A decision, with the question that it is asked, which is "choose" if it has none. Its cases are its output pins.
    Decision (Maybe String)
  | -- | How Mermaid draws a name of a pattern that has no parts: a small node. Graphviz draws the name as a pin.
    Name String
  deriving (Eq)

-- | A pin of the output of an element: the field or case that it is for, if it is for one, the name that it has, if it has one,
-- and the pins of the parts that it is taken apart into. The title that it is drawn with is its name, or else its field.
data Pin = Pin {pinField :: Maybe String, pinName :: Maybe String, pinParts :: [Pin]}
  deriving (Eq)

-- | A place on an element where an edge is joined: the element, and the names of the pins on the way to the pin from the
-- top, which are those that have a name. No names is the body of the element. The pin of a name of the input of a flow is
-- an output pin of the element that the flow is entered at, and the pin of a port is an input pin.
data Seat = Seat Node [String]
  deriving (Eq)

-- | An edge between two pins, where a part of the edge that is drawn as one is more: the seat that it leaves from, if it
-- leaves from a pin, the seat that it enters at, if it enters at a pin, and what it says that no pin does.
data Join = Join (Maybe Seat) (Maybe Seat) (Maybe String)
  deriving (Eq)

-- | Where the label of an edge sits on pins, which Graphviz draws, and Mermaid doesn't have.
data Sits
  = -- | Nowhere: it is text on the edge.
    Loose
  | -- | On a pin of the element that the edge leaves, which has the label for its name: an output, or a case.
    OnOut
  | -- | On an input pin of the element that the edge enters, which has the label for its name.
    OnIn
  | -- | The edge is these edges between pins, and the label is what they say.
    Joined [Join]
  deriving (Eq)

-- | An edge, as Mermaid draws it: between the nodes, which may be clusters, with the label. Graphviz draws it between the
-- pins that its 'Sits' says, and through the result of a flow, if the edge leaves the flow.
data Edge = Edge
  { edgeFrom :: Node,
    edgeTo :: Node,
    edgeLabel :: Maybe String,
    edgeStyle :: Style,
    edgeSits :: Sits,
    -- | The result of the flow that the edge leaves, if it does.
    edgeVia :: Maybe Node
  }
  deriving (Eq)

-- | What a cluster is drawn as.
data Kind
  = -- | A box around steps, which has the title of what they are in.
    Plain
  | -- | The box that Mermaid draws around the nodes of a name, and Graphviz draws no box for: its nodes are pins.
    Naming
  | -- | The box of a flow, around its steps and its entry and its result. Graphviz draws it as a box that has the ports of the flow,
    -- and Mermaid draws no box.
    Flowing
  | -- | The box around the lanes of the parts that run together.
    Container
  | -- | One part, drawn top to bottom, beside the other lanes of its container.
    Lane
  deriving (Eq)

-- | A node of the graph: what it is, the anchor of the page that it links to, if it does, and its pins.
data Element = Element
  { elementForm :: Form,
    elementLink :: Maybe String,
    elementIn :: [String],
    elementOut :: [Pin]
  }
  deriving (Eq)

-- | What a graph is made of, in the order it was made.
data Item
  = Node Node Element
  | Group Node Kind String [Item]
  | Wire Edge
  | -- | The result of a flow, which Mermaid doesn't draw: the name that it reads, and the seats of what it is read from.
    Result Node (Maybe String) [Seat]
  deriving (Eq)

-- | The items of a chart.
newtype Graph = Graph {graphItems :: [Item]}

-- | A page: a box, with the graph of its body as the flow of the program and as the plumbing that passes values by as well.
data Sheet = Sheet
  { sheetTitle :: String,
    sheetComment :: String,
    -- | The place in the code that named the box.
    sheetSource :: Maybe SrcLoc,
    -- | The name of the page in links, and of its file.
    sheetAnchor :: String,
    -- | The anchor of the page whose graph has the box that this page is of, where it was first met.
    sheetParent :: Maybe String,
    -- | The graph without the plumbing: the steps on a part of a value are drawn alone.
    sheetFlow :: Graph,
    -- | The graph with the plumbing: a step on a part of a value has a split, a join, and an edge past it.
    sheetPlumbing :: Graph,
    -- | How many actions in the graph have no name, and are drawn as "IO".
    sheetUnnamed :: Int,
    -- | How many decisions in the graph have no question, and are drawn as "choose".
    sheetUnquestioned :: Int,
    -- | How many pairs of neighbouring steps were drawn as one step.
    sheetFolds :: Int
  }

-- * The walk

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
    outletJoins :: Bool,
    -- | Whether the own label of the way out is the case of a decision, which is a pin of it.
    outletCase :: Bool,
    -- | The result of the flow that the way out is the way out of, if it is.
    outletVia :: Maybe Node
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

-- | What a step does at an inbox that an edge is drawn for: opening it, which is the store, posting, and collecting.
data Use = Opens | Posts | Collects
  deriving (Eq)

-- | One end of an exchange of a value, or a step at an inbox, each of which has a name.
data Mark = Mark Side String | Uses Use String
  deriving (Eq)

-- | A node of the chart, with the ends of exchanges that are in the part of the program that it draws.
data Marked = Marked
  { markedNode :: Node,
    markedMarks :: [Mark]
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
    slotSheet :: Sheet,
    slotDrawn :: Bool,
    -- | Whether a box of the page was met while it was drawn: its anchor is in a chart already, so it can't go.
    slotMet :: Bool
  }

-- | What a chart shows: only the flow of the program, or the plumbing that passes values by as well.
data View = Flow | Plumbing

-- | A name of a flow that a step gave, with what it is drawn as.
data Given = Given
  { givenName :: String,
    givenNode :: Maybe Node,
    -- | The names of the pins on the way to its pin from the top.
    givenPath :: [String],
    -- | Where its pin is: the nodes that give it, each with the case of the decision that it is, if it is one.
    givenAt :: [(Node, Maybe String)],
    -- | The pins of what the step gave, which the name is one of.
    givenPins :: [Pin]
  }

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
    ctxMarked :: IORef [Marked],
    -- | How many pairs of neighbouring steps have been drawn as one.
    ctxFolds :: IORef Int,
    -- | The junctions that flows are entered at.
    ctxInputs :: IORef [Node],
    -- | The names that the step of a flow that was drawn last gave, each with the node of its box, if it has one.
    ctxGiven :: IORef [Given],
    -- | The pins that the elements of the chart give.
    ctxOuts :: IORef [(Node, [Pin])],
    -- | The patterns of the cases of the choices: the site of the choice, the case, and the pattern.
    ctxCases :: IORef [(Node, String, Part)],
    -- | How many sites of choices have been made.
    ctxSites :: IORef Int,
    -- | The flow that the step being drawn is a step of, if it is one.
    ctxFlow :: Maybe FlowEnv,
    -- | The site of the choice that the branch being drawn is a branch of, which the decision will be.
    ctxSite :: Maybe Node
  }

-- | A name of a flow, with the node of its box: a small node for a name that is not taken apart, and the group of a box that
-- holds the parts of a name that is. It has none if its step drew no node. The line of the flow that gave it is its
-- number, which is 0 for the input of the flow, and no line for the names of a branch.
data Box = Box
  { boxName :: String,
    boxNode :: Maybe Node,
    boxLine :: Int,
    -- | The names of the pins on the way to its pin from the top.
    boxPath :: [String],
    -- | Where its pin is: the nodes that give it, each with the case of the decision that it is, if it is one.
    boxAt :: [(Node, Maybe String)],
    -- | The pins of what the step that gave it gave, which it is one of.
    boxPins :: [Pin]
  }
  deriving (Eq)

-- | Where the names of a flow come from, where one of its steps is drawn.
data FlowEnv = FlowEnv
  { -- | The ways out of the node that the flow is entered at, which gives the names of its input.
    flowInput :: [Outlet],
    -- | The names that the flow has, in the order that they were given.
    flowBoxes :: [Box]
  }

-- | What the drawing of a chart came to: its graph, and how many of its nodes say nothing of what they mean.
data Drawing = Drawing
  { drawingGraph :: Graph,
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

-- * The pages

-- | The pages of a program: the page of its top box first, and then those of the boxes in it, in the order they are
-- met. A program that is not a box is a page of its own, named "diagram".
sheets :: Diagram a b -> IO [Sheet]
sheets (Diagram root) = do
  registry <- newIORef (Registry [] [])
  tree <- evaluate root
  name <- makeStableName tree
  let world = World registry
      subject = case tree of
        Named label location body -> Subject name label location [(name, Nothing)] body
        _ -> Subject name (Label "diagram" "") Nothing [] tree
  _ <- pageOf world Nothing subject
  fmap slotSheet . registrySlots <$> readIORef registry

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
      Just slot -> (current {registrySlots = met slot <$> registrySlots current}, Just (sheetAnchor (slotSheet slot)))
      Nothing -> (current, Nothing)
  where
    met slot other = if slotName other == slotName slot then other {slotMet = True} else other

-- | Gives the box a page that is being drawn, with an anchor of its own.
openPage :: World -> Maybe String -> Subject -> IO String
openPage (World registry) parent subject = atomicModifyIORef' registry $ \current ->
  let Label title comment = subjectLabel subject
      taken = sheetAnchor . slotSheet <$> registrySlots current
      anchor = unique taken (slug title)
      sheet = Sheet title comment (subjectLocation subject) anchor parent (Graph []) (Graph []) 0 0 0
      slot = Slot (subjectName subject) sheet False False
   in (current {registrySlots = registrySlots current ++ [slot]}, anchor)

-- | The page is drawn. If a page that is drawn has the same title and graphs, and no box of this one was met while it was
-- drawn, they are the same page: the anchor is the other's.
closePage :: World -> Subject -> String -> Drawing -> Drawing -> IO String
closePage (World registry) subject anchor drawing plumbing = atomicModifyIORef' registry $ \current ->
  let slots = registrySlots current
      mine slot = sheetAnchor (slotSheet slot) == anchor
      same slot =
        slotDrawn slot
          && sheetTitle (slotSheet slot) == title
          && graphItems (sheetFlow (slotSheet slot)) == graphItems (drawingGraph drawing)
          && graphItems (sheetPlumbing (slotSheet slot)) == graphItems (drawingGraph plumbing)
      Label title _ = subjectLabel subject
      drawn slot =
        if mine slot
          then
            slot
              { slotSheet =
                  (slotSheet slot)
                    { sheetFlow = drawingGraph drawing,
                      sheetPlumbing = drawingGraph plumbing,
                      sheetUnnamed = drawingUnnamed drawing,
                      sheetUnquestioned = drawingUnquestioned drawing,
                      sheetFolds = drawingFolds plumbing
                    },
                slotDrawn = True
              }
          else slot
   in case (find same slots, find mine slots) of
        (Just twin, Just slot)
          | not (slotMet slot) ->
              ( current
                  { registrySlots = filter (not . mine) slots,
                    registryAliases = (subjectName subject, sheetAnchor (slotSheet twin)) : registryAliases current
                  },
                sheetAnchor (slotSheet twin)
              )
        _ -> (current {registrySlots = drawn <$> slots}, anchor)

-- | The graph in the view, drawn from the body of a box, and how many of its nodes have no name or no question.
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
      <*> newIORef []
      <*> newIORef []
      <*> newIORef []
      <*> newIORef []
      <*> newIORef 0
      <*> pure Nothing
      <*> pure Nothing
  start <- declare ctx Start
  fragment <- render ctx (subjectPath subject) (subjectBody subject)
  bounded ctx start (outputOf (subjectBody subject)) fragment
  drawn <- reverse <$> readIORef (ctxItems ctx)
  ended <- reverse <$> readIORef (ctxMarked ctx)
  let items = drawn ++ messages (ancestry [] drawn) ended
  aliases <- readIORef (ctxAliases ctx)
  questions <- readIORef (ctxQuestions ctx)
  outs <- reverse <$> readIORef (ctxOuts ctx)
  unnamed <- readIORef (ctxUnnamed ctx)
  folds <- readIORef (ctxFolds ctx)
  pure (Drawing (Graph (finish aliases questions outs items)) unnamed (length (filter (isNothing . snd) questions)) folds)

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
    end <- declare ctx Finish
    edge ctx start end (input <|> out) Solid
  Nodes entry _ outlets -> do
    wire <- into ctx entry input
    inputs <- readIORef (ctxInputs ctx)
    -- A flow that the chart starts with is entered at the start, which its edge says.
    edgeAs ctx (if isJust wire then OnIn else Loose) start entry wire (if entry `elem` inputs && isNothing wire then Entry else Solid)
    unless (null outlets) $ do
      end <- declare ctx Finish
      forM_ outlets $ \outlet -> leaveAs ctx outlet end (exitName outlet) (exitSits outlet)
  where
    -- The way out's own label, such as the side of a decision, and the name of the output wire.
    exitName outlet = case (outletText outlet, output) of
      (Just pending, Just out) | pending /= out -> Just (pending ++ ": " ++ out)
      (pending, out) -> pending <|> out
    -- A name that the way out has is on its pin, unless it is said with the name of the output wire.
    exitSits outlet = case (outletText outlet, output) of
      (Just pending, Just out) | pending /= out -> Loose
      _ -> outletSits outlet

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
plain node = Outlet node Nothing Nothing Nothing Solid False False Nothing

-- | The text of the edge from a way out, if the edge is not into a decision: its own, or else the name of its wire.
outletText :: Outlet -> Maybe String
outletText outlet = outletLabel outlet <|> outletWire outlet <|> outletThen outlet

-- | Where the text of the edge from a way out sits: on a pin of the node, if it is the case of a decision or the name of a
-- wire, and nowhere if it is the text that a scope gives the edge to its release.
outletSits :: Outlet -> Sits
outletSits outlet = case outletLabel outlet of
  Just _ | not (outletCase outlet) -> Loose
  _ -> OnOut

-- | A new name for a node of the chart.
fresh :: Ctx -> IO Node
fresh ctx = atomicModifyIORef' (ctxCount ctx) (\count -> (count + 1, 'n' : show (count + 1)))

-- | Adds an item to the chart.
emit :: Ctx -> Item -> IO ()
emit ctx item = modifyIORef' (ctxItems ctx) (item :)

-- | Adds an element of the form to the chart, and links it to the page of the anchor if it has one.
declaring :: Ctx -> Form -> Maybe String -> IO Node
declaring ctx form link = do
  node <- fresh ctx
  node <$ emit ctx (Node node (Element form link [] []))

-- | Adds an element of the form to the chart.
declare :: Ctx -> Form -> IO Node
declare ctx form = declaring ctx form Nothing

-- | Adds an edge to the chart, whose text sits where the sits say.
edgeAs :: Ctx -> Sits -> Node -> Node -> Maybe String -> Style -> IO ()
edgeAs ctx sits from to label style = emit ctx (Wire (Edge from to label style sits Nothing))

-- | Adds an edge to the chart, whose text is on it.
edge :: Ctx -> Node -> Node -> Maybe String -> Style -> IO ()
edge ctx = edgeAs ctx Loose

-- | Adds the edge from a way out to the node, with the text and the sits that it is given.
leaveAs :: Ctx -> Outlet -> Node -> Maybe String -> Sits -> IO ()
leaveAs ctx outlet to label sits =
  emit ctx (Wire (Edge (outletNode outlet) to label (outletStyle outlet) sits (outletVia outlet)))

-- | Adds the edge from a way out to the node.
leave :: Ctx -> Node -> Outlet -> IO ()
leave ctx to outlet = leaveAs ctx outlet to (outletText outlet) (outletSits outlet)

-- | The items that the action adds to the chart, which it leaves out of it.
collect :: Ctx -> IO a -> IO (a, [Item])
collect ctx action = do
  outer <- readIORef (ctxItems ctx)
  writeIORef (ctxItems ctx) []
  result <- action
  inner <- readIORef (ctxItems ctx)
  writeIORef (ctxItems ctx) outer
  pure (result, reverse inner)

-- | How a leaf is drawn: a post hands over, a collect waits, and the opening and the closing of an inbox are round.
look :: Shape -> Look
look = \case
  Action -> LookAction
  Wait -> LookWait
  Blocking -> LookBlocking
  Yielding -> LookYielding
  Handover -> LookHandover
  Round -> LookRound
  PostTo _ -> LookHandover
  CollectFrom _ -> LookWait
  OpenInbox _ -> LookRound
  CloseInbox _ -> LookRound

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
      emit ctx (Node target (Element Junction Nothing [] []))
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
    decided ctx Nothing [Branch "Left" taken, Branch "Right" bare]
  Right' inner -> do
    taken <- render ctx path inner
    decided ctx Nothing [Branch "Left" bare, Branch "Right" taken]
  Sum one two -> chosen ctx path (\case Sum a b -> Just (a, b); _ -> Nothing) one two
  Choice one two -> chosen ctx path (\case Choice a b -> Just (a, b); _ -> Nothing) one two
  Together parts -> laned ctx path parts
  Grouped inner -> grouped ctx "group" (render ctx path inner)
  Spawned inner -> spawned ctx path inner
  Race one two -> traverse (render ctx path) [one, two] >>= barred ctx "race"
  Leaf text how -> do
    when (text == unnamedAction) $ modifyIORef' (ctxUnnamed ctx) (+ 1)
    node <- declare ctx (Step (look how) text)
    placing ctx node (leafMarks how)
    pure (single node)
  Scope acquire release body -> scoped ctx path acquire release body
  Try inner -> grouped ctx "try" (render ctx path inner)
  Shield inner -> grouped ctx "shield" (render ctx path inner)
  Grace duration inner -> grouped ctx ("grace " ++ readable duration) (render ctx path inner)
  FastLane inner -> grouped ctx "fast lane" (render ctx path inner)
  Within duration inner -> grouped ctx ("within " ++ readable duration) (render ctx path inner)
  TimeoutA inner -> grouped ctx "within the duration of the input" (render ctx path inner)
  Wires given output (FlowStep part bindings inner) ->
    wired Nothing output <$> flowStepped ctx path given part bindings inner
  Wires given output (FlowCase part inner) -> wired Nothing output <$> flowCased ctx path given part inner
  Wires input output inner -> wired input output <$> render ctx path inner
  FlowScope result inner -> flowScoped ctx path result inner
  FlowStep part bindings inner -> flowStepped ctx path Nothing part bindings inner
  FlowCase part inner -> flowCased ctx path Nothing part inner
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

-- | A whole flow: its steps in order, each with an edge from the box of each name that it binds, and a step that binds nothing
-- from the step before it has an edge from it that says only the order. The flow is a box that holds its steps, its entry, which
-- Graphviz draws as the group of its input ports and Mermaid as a junction, and its result. The entry is one with the start of
-- the chart if the flow is what the chart starts with, and with the one node that the flow follows if that has nothing else to
-- say, as the edge into it of the style 'Entry' says, so that Mermaid draws a junction only when the flow follows more than that.
flowScoped :: Ctx -> Path -> Maybe String -> Tree -> IO Fragment
flowScoped ctx path result inner = do
  lined <- flowLines inner
  known <- readIORef (ctxInputs ctx)
  let number = show (length known + 1)
      entry = 'i' : number
      entered = [plain entry]
  modifyIORef' (ctxInputs ctx) (entry :)
  -- The names that the step drawn last gave are of this flow: a flow in one of its steps keeps its own.
  saved <- readIORef (ctxGiven ctx)
  ((env, (_, outlets), drawn, _), items) <-
    collect ctx (foldM (flowItem ctx path) (FlowEnv entered [], (Just 0, entered), False, 0) lined)
  writeIORef (ctxGiven ctx) saved
  if drawn
    then do
      -- The result is drawn by Graphviz, which draws the edges out of the flow from it, and Mermaid has no node for it.
      let bodies = [Seat (outletNode outlet) [] | outlet <- outlets]
          seats = case result of
            Nothing -> bodies
            Just wanted -> case find ((== wanted) . boxName) (reverse (flowBoxes env)) of
              Just box | not (null (boxAt box)) -> seatsOf box []
              Just _ -> bodies
              Nothing -> [Seat entry [wanted]]
          exit = 'r' : number
      -- The box of the flow holds its steps, its entry and its result.
      emit ctx (Group ('f' : number) Flowing "" (items ++ [Node entry (Element Entered Nothing [] []), Result exit result seats]))
      pure (Fragment Nothing (Nodes entry True [outlet {outletVia = Just exit} | outlet <- outlets]))
    else bare <$ traverse_ (emit ctx) items

-- | A line of a flow: a step of it, or a scope that the rest of the flow is the body of.
data FlowLine
  = -- | A step of the flow.
    OneStep Tree
  | -- | A scope: the lines of its acquire, of its body, and of its release.
    InScope [FlowLine] [FlowLine] [FlowLine]

-- | The lines of a flow, in order: the nodes of its chain that are steps of it, which it doesn't go into, and a scope with
-- the lines of its parts.
flowLines :: Tree -> IO [FlowLine]
flowLines step = do
  tree <- evaluate step
  case tree of
    FlowStep {} -> pure [OneStep tree]
    Scope acquire release body -> do
      acquired <- flowLines acquire
      inner <- flowLines body
      released <- flowLines release
      pure [InScope acquired inner released]
    _ -> concat <$> traverse flowLines (children tree)

-- | What a flow has when its lines are drawn: the names that it has, the number of the line before and the ways out of it,
-- whether a node was drawn, and how many lines there are.
type FlowState = (FlowEnv, (Maybe Int, [Outlet]), Bool, Int)

-- | A line of a flow, drawn. A scope is a group that has the lines of its acquire, and the lines after them, which are its
-- body, and the lines of its release, which hang on the dashed edge from the acquire named for the exit. The release reads
-- the names that the acquire gave, and the exit, which the edge says, so it has no box of its own.
flowItem :: Ctx -> Path -> FlowState -> FlowLine -> IO FlowState
flowItem ctx path (env, before, drawn, count) = \case
  OneStep step -> do
    (env', before', drawn') <- flowLine ctx path (env, before, drawn) (count + 1, step)
    pure (env', before', drawn', count + 1)
  InScope acquires inner releases -> do
    (state, items) <- collect ctx $ do
      acquired@(afterAcquire, (_, starts), _, _) <- foldM (flowItem ctx path) (env, before, drawn, count) acquires
      body <- foldM (flowItem ctx path) acquired inner
      let reading = afterAcquire {flowBoxes = flowBoxes afterAcquire ++ [Box "exit" Nothing 0 [] [] []]}
      forM_ [step | OneStep step <- releases] $ \step -> do
        released <- render (ctx {ctxFlow = Just reading}) path step
        sequential ctx (Fragment Nothing (Nodes "" False [outlet {outletLabel = Just "on exit", outletStyle = Dashed} | outlet <- starts])) released
      pure body
    let (env', before', drawn', count') = state
        any' = not (null (declared items))
    when any' $ do
      node <- fresh ctx
      emit ctx (Group node Plain "scope" items)
    pure (env', before', drawn' || any', count')

-- | A step of a flow, drawn with the names that the flow has before it, which the steps after it have its names among, if it
-- has any. The step before it, which is the one before that if it draws no node, has an edge to it that says only the order if
-- the step binds nothing from it. The steps are numbered from 1, and the input is the line 0.
flowLine :: Ctx -> Path -> (FlowEnv, (Maybe Int, [Outlet]), Bool) -> (Int, Tree) -> IO (FlowEnv, (Maybe Int, [Outlet]), Bool)
flowLine ctx path (env, before, drawn) (number, step) = case step of
  FlowStep part bindings _ -> do
    writeIORef (ctxGiven ctx) []
    Fragment _ core <- render (ctx {ctxFlow = Just env}) path step
    given <- readIORef (ctxGiven ctx)
    let outlets = case core of
          Nodes _ _ out -> out
          Bare _ -> []
        later =
          env
            { flowBoxes =
                flowBoxes env ++ [Box (givenName g) (givenNode g) number (givenPath g) (givenAt g) (givenPins g) | g <- given]
            }
    case core of
      Nodes entry _ _ -> do
        unless (any ((== fst before) . Just . lineOf env) bindings) $
          forM_ (snd before) $ \outlet ->
            emit ctx (Wire (Edge (outletNode outlet) entry (outletLabel outlet) Order (outletSits outlet) (outletVia outlet)))
        pure (later, (if isJust part then Just number else Nothing, outlets), True)
      Bare _ -> pure (later, before, drawn)
  _ -> pure (env, before, drawn)

-- | A step of a flow: its nodes, in a box that has the whole that it gives for its title, if it has a pattern, with a box for
-- each name that the pattern takes its output apart into beside them, and an edge into it from the box of each name that it
-- binds. The wire into it that a branch of a choice was given the case for is the case, which the edge from the decision says.
-- The names are pins of the ways out of the step, and of the decision, for the ways that are the cases of one.
flowStepped :: Ctx -> Path -> Maybe String -> Maybe Part -> [Binding] -> Tree -> IO Fragment
flowStepped ctx path given part bindings inner = do
  (Fragment input core, registered) <- case part of
    Nothing -> (\fragment -> (fragment, [])) <$> render ctx path inner
    Just whole -> do
      ((fragment, boxes), items) <- collect ctx $ do
        fragment <- render ctx path inner
        boxes <- case fragmentCore fragment of
          Nodes {} -> boxParts ctx [name | Just name <- [partName whole]] whole
          Bare _ -> pure []
        pure (fragment, boxes)
      case fragmentCore fragment of
        Bare _ -> pure (fragment, [(name, Nothing, [name]) | name <- namesOf whole])
        Nodes {} -> do
          group <- fresh ctx
          emit ctx (Group group Naming (fromMaybe " " (partName whole)) items)
          pure (fragment, [(name, Just group, [name]) | Just name <- [partName whole]] ++ [(name, Just node, path') | (name, node, path') <- boxes])
  case (ctxFlow ctx, core) of
    (Just env, Nodes entry _ _) -> drawBindings ctx env entry bindings
    _ -> pure ()
  let pins = maybe [] pinsOf part
      outlets = case core of
        Nodes _ _ out -> out
        Bare _ -> []
      at = nub [(outletNode outlet, if outletCase outlet then Just (fromMaybe "" (outletLabel outlet)) else Nothing) | outlet <- outlets]
  -- A way out that is the case of a decision has no pin for the name: it is the case.
  forM_ [node | (node, Nothing) <- at] $ \node -> unless (null pins) $ modifyIORef' (ctxOuts ctx) ((node, pins) :)
  modifyIORef' (ctxGiven ctx) (++ [Given name node path' at pins | (name, node, path') <- registered])
  pure (Fragment (given <|> input) core)

-- | A branch of a choice of a flow whose case is taken apart by a pattern: a box for each name of the pattern beside the
-- branch, which the steps of the branch bind the names of. A branch that is not in a flow has no boxes. The pattern is
-- the pins of the case, which is a pin of the decision that the choice makes.
flowCased :: Ctx -> Path -> Maybe String -> Part -> Tree -> IO Fragment
flowCased ctx path given part inner = do
  forM_ ((,) <$> ctxSite ctx <*> given) $ \(site, case') -> modifyIORef' (ctxCases ctx) ((site, case', part) :)
  case ctxFlow ctx of
    Nothing -> render ctx path (Wires given Nothing inner)
    Just env -> do
      let prefix = [case' | Just case' <- [given]]
      boxes <- case (partName part, partParts part) of
        (_, Nothing) -> boxPart ctx prefix part
        (Just _, Just _) -> boxPart ctx prefix part
        (Nothing, Just _) -> boxParts ctx prefix part
      let at = [(site, Nothing) | Just site <- [ctxSite ctx], isJust given]
          pins = pinsOf part {partField = Nothing}
          env' = env {flowBoxes = flowBoxes env ++ [Box name (Just node) (-1) path' at pins | (name, node, path') <- boxes]}
      render (ctx {ctxFlow = Just env'}) path (Wires given Nothing inner)

-- | The names that a pattern keeps, in the order of the pattern: a name before the names of its parts.
namesOf :: Part -> [String]
namesOf part = [name | Just name <- [partName part]] ++ concatMap namesOf (fromMaybe [] (partParts part))

-- | The pins that a pattern gives: a pin for a name that is not taken apart, a pin that holds the pins of its parts for a part
-- that is, which is titled with its name, or else its field, and none for a part that is dropped. The parts of a part that has
-- neither a name nor a field are the pins of the pattern itself.
pinsOf :: Part -> [Pin]
pinsOf (Part field name parts) = case parts of
  Just inner@(_ : _)
    | isJust field || isJust name -> [Pin field name (concatMap pinsOf inner)]
    | otherwise -> concatMap pinsOf inner
  _ -> [Pin field name [] | isJust name]

-- | The boxes of the parts that a pattern takes its value apart into, each name that is kept with the node of its box and the
-- names of the pins on the way to its pin, which start with the prefix.
boxParts :: Ctx -> [String] -> Part -> IO [(String, Node, [String])]
boxParts ctx prefix whole = concat <$> traverse (boxPart ctx prefix) (fromMaybe [] (partParts whole))

-- | The box of a part: a small node for a name that is not taken apart, and a box with the title of the part, which is its
-- name, or else its field, that holds the boxes of its parts for a name that is.
boxPart :: Ctx -> [String] -> Part -> IO [(String, Node, [String])]
boxPart ctx prefix part = case partParts part of
  Just parts@(_ : _) -> do
    let inside = prefix ++ [name | Just name <- [partName part]]
    (inner, items) <- collect ctx (concat <$> traverse (boxPart ctx inside) parts)
    group <- fresh ctx
    emit ctx (Group group Naming (fromMaybe (fromMaybe " " (partField part)) (partName part)) items)
    pure ([(name, group, inside) | Just name <- [partName part]] ++ inner)
  _ -> case partName part of
    Just name -> do
      node <- declare ctx (Name name)
      pure [(name, node, prefix ++ [name])]
    Nothing -> pure []

-- | The box of a name that a binding reads, if the name is one that a step or a branch of the flow gave, and not an input.
boxOf :: FlowEnv -> Binding -> Maybe Box
boxOf env binding = case bindingPath binding of
  name : _ -> find ((== name) . boxName) (reverse (flowBoxes env))
  [] -> Nothing

-- | The line of the flow that a binding reads from: 0 for the input.
lineOf :: FlowEnv -> Binding -> Int
lineOf env = maybe 0 boxLine . boxOf env

-- | What an edge from the entry of the flow says of a binding: the name that it reads, with the port after it if the port is
-- another name.
sayingInput :: Binding -> String
sayingInput (Binding port path)
  | source == port = source
  | otherwise = source ++ " \x2192 " ++ port
  where
    source = intercalate "." path

-- | What an edge from a box says of a binding: nothing if the box is the name that the port has, as the box says it, and else
-- the port, and the field of a path into what the box has, with the port after it if the port is another name.
sayingBox :: Binding -> Maybe String
sayingBox (Binding port path) = case path of
  [name] | name == port -> Nothing
  [_] -> Just ("\x2192 " ++ port)
  _ : fields@(_ : _) ->
    let source = intercalate "." fields
     in Just (if source == port then source else source ++ " \x2192 " ++ port)
  [] -> Nothing

-- | The edges into the entry of a step of a flow: one from the node that the flow is entered at for the names of its input,
-- and one from the box of each name that is read, which says the names of that box that are read, in the order that they are
-- bound.
drawBindings :: Ctx -> FlowEnv -> Node -> [Binding] -> IO ()
drawBindings ctx env entry bindings = do
  case [binding | binding <- bindings, isNothing (boxOf env binding)] of
    [] -> pure ()
    inputs -> do
      said <- into ctx entry (Just (intercalate ", " (sayingInput <$> inputs)))
      forM_ (flowInput env) $ \outlet ->
        emit ctx . Wire $
          Edge
            (outletNode outlet)
            entry
            (withOwn (outletLabel outlet) said)
            (outletStyle outlet)
            (Joined [Join (Just (Seat (outletNode outlet) [source])) (Just (Seat entry [bindingPort binding])) fields | binding <- inputs, let (source, fields) = splitPath binding])
            (outletVia outlet)
  forM_ (nub [box | binding <- bindings, Just box <- [boxOf env binding]]) $ \box -> do
    let mine = [binding | binding <- bindings, boxOf env binding == Just box]
        texts = [text | binding <- mine, Just text <- [sayingBox binding]]
        joins = [Join (Just seat) (Just (Seat entry [bindingPort binding])) note | binding <- mine, (seat, note) <- sources box binding]
    said <- into ctx entry (if null texts then Nothing else Just (intercalate ", " texts))
    forM_ (boxNode box) $ \node -> edgeAs ctx (Joined joins) node entry said Solid
  where
    -- The label that a way out has of its own, such as the answer of a decision, is before the names.
    withOwn (Just own) (Just text) = Just (own ++ ": " ++ text)
    withOwn own text = own <|> text

-- | The name that a binding reads, and the fields of it that the path goes on to, if it does.
splitPath :: Binding -> (String, Maybe String)
splitPath binding = case bindingPath binding of
  name : fields -> (name, if null fields then Nothing else Just (intercalate "." fields))
  [] -> ("", Nothing)

-- | The seats of the pins of a name of a box, and the pins on the way to a part of it that the fields lead to.
seatsOf :: Box -> [String] -> [Seat]
seatsOf box extra =
  [ case from of
      Just case' -> Seat node [case']
      Nothing -> Seat node (boxPath box ++ extra)
    | (node, from) <- boxAt box
  ]

-- | Where a binding reads from in a box, and what its path says that the pins don't: the seats of the pin that the fields
-- of the path lead to, if the pins of the box have the parts, and the fields that are left.
sources :: Box -> Binding -> [(Seat, Maybe String)]
sources box binding = [(seat, note) | seat <- seatsOf box extra]
  where
    fields = drop 1 (bindingPath binding)
    (extra, rest) = case locate (boxName box) (boxPins box) of
      Just pin -> reach pin [] fields
      Nothing -> ([], fields)
    note = if null rest then Nothing else Just (intercalate "." rest)

-- | The pin of the name, among the pins and the pins of their parts.
locate :: String -> [Pin] -> Maybe Pin
locate name pins = find ((== Just name) . pinName) (every pins)
  where
    every = concatMap (\pin -> pin : every (pinParts pin))

-- | The names of the pins that the fields lead to from the pin, and the fields that are left over: a part that has no name
-- has no pin, and its field is left if no part with a name follows.
reach :: Pin -> [String] -> [String] -> ([String], [String])
reach _ pending [] = ([], reverse pending)
reach pin pending fields@(field : more) =
  case [part | part <- pinParts pin, pinField part == Just field || pinName part == Just field] of
    part : _ -> case pinName part of
      Just name -> let (names, rest) = reach part [] more in (name : names, rest)
      Nothing -> reach part (field : pending) more
    [] -> ([], reverse pending ++ fields)

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

-- | Draws the edges from the ways out to the entry, unless a join and a split that follows it can be one junction. The edge to the
-- entry of a flow from one node that has nothing else to say is the way out, which the flow is entered at, says so.
connect :: Ctx -> [Outlet] -> Maybe String -> Node -> Bool -> IO ()
connect ctx outlets label entry splits = do
  inputs <- readIORef (ctxInputs ctx)
  case outlets of
    [ outlet@Outlet
        { outletNode = before,
          outletLabel = Nothing,
          outletWire = Nothing,
          outletThen = Nothing,
          outletStyle = Solid,
          outletJoins = joins
        }
      ]
      | isNothing label && entry `elem` inputs -> emit ctx (Wire (Edge before entry Nothing Entry Loose (outletVia outlet)))
      | isNothing label && splits && joins -> modifyIORef' (ctxAliases ctx) ((entry, before) :)
    _ -> forM_ outlets $ \outlet -> do
      later <- into ctx entry (outletThen outlet)
      wire <- into ctx entry (outletWire outlet <|> label)
      let sits
            | isJust (outletLabel outlet) = outletSits outlet
            | isJust wire = if isJust (outletWire outlet) then OnOut else OnIn
            | otherwise = OnOut
      leaveAs ctx outlet entry (outletLabel outlet <|> wire <|> later) sits

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
    split <- declare ctx Junction
    joined <- declare ctx Junction
    wire <- into ctx entry (fragmentInput inner)
    edgeAs ctx (if isJust wire then OnIn else Loose) split entry wire Solid
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
  branches -> fork ctx Junction Nothing branches

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
    -- | The items as they are laid out, with the nodes named by their order, which is the same for parts that are drawn alike.
    drawnLaid :: ([Laid], [Link])
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

-- | The items as they are laid out in a chart, with each node that they declare named by its order, so that parts that are
-- drawn alike are laid out alike.
alike :: Ctx -> [Item] -> IO ([Laid], [Link])
alike ctx items = do
  aliases <- readIORef (ctxAliases ctx)
  questions <- readIORef (ctxQuestions ctx)
  let tidied = viewed (tidy aliases questions items)
      numbered = zip (declared tidied) (("m" ++) . show <$> [1 :: Int ..])
  pure (layout (renamed (\node -> fromMaybe node (lookup node numbered)) tidied))

-- | The items with their nodes renamed.
renamed :: (Node -> Node) -> [Item] -> [Item]
renamed rename = fmap $ \case
  Node node element -> Node (rename node) element
  Wire edge' -> Wire edge' {edgeFrom = rename (edgeFrom edge'), edgeTo = rename (edgeTo edge')}
  Group node kind title inner -> Group (rename node) kind title (renamed rename inner)
  item@Result {} -> item

-- | The parts that are drawn alike as one, which stands for all of them, where the first of them is.
merged :: [Drawn] -> [Drawn]
merged = foldl' merge []
  where
    merge found part = case break (alikeTo part) found of
      (before, twin : after) -> before ++ twin {drawnCount = drawnCount twin + drawnCount part} : after
      _ -> found ++ [part]
    alikeTo part other = not (null (drawnItems part)) && drawnLaid other == drawnLaid part

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
  branches -> fork ctx (Bar title) (Just title) branches

-- | The branches side by side, between a node that splits and a node that joins, and a wire alone goes from one to the
-- other. With no title the nodes are junctions, and with a title they are bars, which Mermaid draws without a label, so
-- the edges from the split carry the title, unless the branch has the name of a wire to put there.
fork :: Ctx -> Form -> Maybe String -> [Fragment] -> IO Fragment
fork ctx form title branches = do
  split <- declare ctx form
  joined <- declare ctx form
  forM_ branches $ \branch -> case fragmentCore branch of
    Bare out -> edge ctx split joined (out <|> fragmentInput branch <|> title) Solid
    Nodes entry _ outlets -> do
      wire <- into ctx entry (fragmentInput branch)
      edgeAs ctx (if isJust wire then OnIn else Loose) split entry (wire <|> title) Solid
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
  site <- atomicModifyIORef' (ctxSites ctx) (\count -> (count + 1, 'c' : show (count + 1)))
  traverse (\(Operand at step) -> Branch (unwords at) <$> render (ctx {ctxSite = Just site}) path step) found
    >>= decided ctx (Just site)

-- | A decision with an edge to each branch and no join: the ways out of the branches are the ways out of the fragment.
-- It is asked the name of the wire that goes into it, as its question, if the wire has one. Its cases are its pins, which hold
-- the pins of the pattern of the case, where the branch of the case has one: the choice's site, which the names of the
-- patterns are seated at, is the decision.
decided :: Ctx -> Maybe Node -> [Branch] -> IO Fragment
decided ctx site branches = do
  decision <- fresh ctx
  emit ctx (Node decision (Element (Decision Nothing) Nothing [] []))
  modifyIORef' (ctxQuestions ctx) ((decision, Nothing) :)
  outlets <- concat <$> traverse (follow decision) branches
  patterns <- readIORef (ctxCases ctx)
  let casePin (Branch name branch) =
        let case' = answer name branch
            inside = [pinsOf part {partField = Nothing} | Just at <- [site], (found, called, part) <- patterns, found == at, called == case']
         in Pin (Just case') (Just case') (concat (take 1 inside))
  modifyIORef' (ctxOuts ctx) ((decision, nub (casePin <$> branches)) :)
  forM_ site $ \at -> modifyIORef' (ctxAliases ctx) ((at, decision) :)
  pure (Fragment Nothing (Nodes decision False outlets))
  where
    -- The edge is the answer: the name of the wire that goes into the branch, or else the name of the branch.
    follow decision (Branch name branch) = case fragmentCore branch of
      Bare out -> pure [(plain decision) {outletLabel = Just (answer name branch), outletThen = out, outletCase = True}]
      Nodes entry _ outlets -> do
        edgeAs ctx OnOut decision entry (Just (answer name branch)) Solid
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
    Bare _ -> single <$> declare ctx (Step LookRound "spawn")
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
    store <- declare ctx (Store name)
    placing ctx store [Uses Opens name]
  acquired <- render ctx path acquire
  running <- render ctx path body
  released <- render ctx path release
  start <- case fragmentCore acquired of
    Bare out -> do
      node <- declare ctx Junction
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
    node <- declaring ctx (Titled (boxText label)) (Just anchor)
    endsOf body >>= placing ctx node
    pure (linked node body)

-- | A node that links to the page of a box, which has the names of the wires of the body, if it has them.
linked :: Node -> Tree -> Fragment
linked node body = case body of
  Wires input output _ -> Fragment input (Nodes node False [(plain node) {outletWire = output}])
  _ -> single node

-- | The asking end of an exchange: a node of its own, with the name of what it awaits, and under it the steps that hand
-- the ticket out and dispose of a value that comes late, where they are named. It links to the page of the mechanics.
awaiting :: Ctx -> Tree -> String -> Tree -> IO Fragment
awaiting ctx tree exchanged body = do
  name <- makeStableName tree
  anchor <- pageOf (ctxWorld ctx) (Just (ctxAnchor ctx)) (Subject name label Nothing [(name, Nothing)] body)
  node <- declaring ctx (Awaits (title ++ detail)) (Just anchor)
  endsOf body >>= placing ctx node . (Mark Asking exchanged :)
  pure (linked node body)
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
placing ctx node marks = unless (null marks) (modifyIORef' (ctxMarked ctx) (Marked node marks :))

-- | The groups that each node of the items is in, from the outermost.
ancestry :: [(Node, Kind)] -> [Item] -> [(Node, [(Node, Kind)])]
ancestry around items =
  concat
    [ case item of
        Node node _ -> [(node, around)]
        Group node kind _ inner -> ancestry (around ++ [(node, kind)]) inner
        _ -> []
      | item <- items
    ]

-- | The edges of the messages of exchanges: from a node that has the answering end to a node that has the asking end of
-- an exchange of the same name, for each pair of such nodes. An end in a lane is joined at the lane, as an edge to a
-- node in it would make Mermaid forget the lane's direction, unless the other end is in the lane too. The edges of the
-- messages of inboxes are drawn the same way, from the posts to the store of an inbox, and from the store to the collects.
messages :: [(Node, [(Node, Kind)])] -> [Marked] -> [Item]
messages around placed =
  [ message from to (Just (envelope ++ exchanged))
    | answer <- placed,
      Mark Answering exchanged <- markedMarks answer,
      ask <- placed,
      let from = markedNode answer
          to = markedNode ask,
      from /= to,
      Mark Asking awaited <- markedMarks ask,
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
    -- Mermaid draws the edge at the lanes that the nodes are in, and Graphviz at the nodes.
    message from to label =
      Wire (Edge (joined from to) (joined to from) label Message (Joined [Join (Just (Seat from [])) (Just (Seat to [])) label]) Nothing)
    -- The posts to an inbox have an edge to its store, and the store has an edge to the collects from it, which are lifted to
    -- boxes and lanes as the messages are. A node that the chart doesn't have, as one of a part that was drawn as another, has none.
    stores =
      concat
        [ [message user store Nothing | user <- using Posts name, user /= store]
            ++ [message store user Nothing | user <- using Collects name, user /= store]
          | (store, name) <- nub [(markedNode opening, name) | opening <- placed, Uses Opens name <- markedMarks opening],
            store `elem` fmap fst around
        ]
    using use name =
      nub
        [ markedNode user
          | user <- placed,
            Uses found called <- markedMarks user,
            found == use,
            called == name,
            markedNode user `elem` fmap fst around
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

-- * The graph of a chart

-- | An edge as it is laid out in the text of a chart: only what Mermaid draws of it.
data Link = Link Node Node (Maybe String) Style
  deriving (Eq)

-- | What a chart is made of at one level, in the order that it is written: the nodes, the boxes with what is in them, and
-- then the edges between the nodes of the level.
data Laid
  = Declared Node Form
  | Boxed Node Kind String [Laid]
  | Linked Link
  deriving (Eq)

-- | The nodes that the items declare, in groups too.
declared :: [Item] -> [Node]
declared items =
  concat
    [ case item of
        Node node _ -> [node]
        Group node _ _ inner -> node : declared inner
        _ -> []
      | item <- items
    ]

-- | The items as Mermaid draws them: the box of a flow is no box, and the entry of a flow that follows a node is that node, as the
-- edge into the entry says, which is not drawn.
viewed :: [Item] -> [Item]
viewed items = concatMap seen items
  where
    entries = [(edgeTo edge', edgeFrom edge') | Wire edge' <- flat items, edgeStyle edge' == Entry]
    seen = \case
      Node node _ | node `elem` fmap fst entries -> []
      Wire edge'
        | edgeStyle edge' == Entry -> []
        | otherwise -> [Wire edge' {edgeFrom = followed' entries (edgeFrom edge'), edgeTo = followed' entries (edgeTo edge')}]
      Group _ Flowing _ inner -> concatMap seen inner
      Group node kind title inner -> [Group node kind title (concatMap seen inner)]
      item -> [item]

-- | The items as a chart is written: the nodes and the boxes in order, and the edges among them that are not between nodes
-- that they declare. A node that an edge names inside a group is a node of the group, so an edge to a node outside it is
-- written outside.
layout :: [Item] -> ([Laid], [Link])
layout = laidOut . viewed

-- | The items as they are written, which are those that 'viewed' leaves.
laidOut :: [Item] -> ([Laid], [Link])
laidOut items = (concat (fmap fst parts) ++ fmap Linked kept, escapedEdges)
  where
    own = declared items
    parts = fmap place items
    place = \case
      Node node element -> ([Declared node (elementForm element)], [])
      Group node kind title inner ->
        let (inside, up) = laidOut inner
         in ([Boxed node kind title inside], up)
      _ -> ([], [])
    (kept, escapedEdges) = partition (between own) ([Link (edgeFrom e) (edgeTo e) (edgeLabel e) (edgeStyle e) | Wire e <- items] ++ concatMap snd parts)
    between nodes (Link from to _ _) = from `elem` nodes && to `elem` nodes

-- | The node that the junctions that are one with the junction after them lead to.
followed' :: [(Node, Node)] -> Node -> Node
followed' aliases node = maybe node (followed' aliases) (lookup node aliases)

-- | The items with the nodes that are named by their junction renamed, and the declarations of those left out, and the
-- decisions given their questions, which are "choose" if they have none.
tidy :: [(Node, Node)] -> [(Node, Maybe String)] -> [Item] -> [Item]
tidy aliases questions = concatMap $ \case
  Node node _ | node `elem` fmap fst aliases -> []
  Node node element@(Element (Decision _) _ _ _) -> [Node node element {elementForm = Decision (join (lookup node questions))}]
  Wire edge' ->
    [ Wire
        edge'
          { edgeFrom = resolve (edgeFrom edge'),
            edgeTo = resolve (edgeTo edge'),
            edgeVia = resolve <$> edgeVia edge',
            edgeSits = case edgeSits edge' of
              Joined joins -> Joined [Join (resolveSeat <$> from) (resolveSeat <$> to) note | Join from to note <- joins]
              other -> other
          }
    ]
  Group node kind title inner -> [Group node kind title (tidy aliases questions inner)]
  Result node name seats -> [Result node name (resolveSeat <$> seats)]
  item -> [item]
  where
    resolve = followed' aliases
    resolveSeat (Seat node path) = Seat (resolve node) path

-- | Whether an element of the form has output pins that edges leave from.
hostsOut :: Form -> Bool
hostsOut = \case
  Entered -> True
  Step {} -> True
  Titled _ -> True
  Awaits _ -> True
  Decision _ -> True
  _ -> False

-- | Whether an element of the form has input pins that edges enter at.
hostsIn :: Form -> Bool
hostsIn = \case
  Entered -> True
  Step {} -> True
  Titled _ -> True
  Awaits _ -> True
  Decision _ -> True
  _ -> False

-- | The items that are not groups, and those of the groups, in order.
flat :: [Item] -> [Item]
flat = concatMap $ \case
  Group _ _ _ inner -> flat inner
  item -> [item]

-- | Where the text of each edge sits is what the pins of its ends can have: an edge that leaves an element that has no pins
-- has its text on it, and so does an edge that enters one, and a seat on an element that has no pins is the body.
seated :: [Item] -> [Item]
seated items = fmap fix items
  where
    forms = [(node, elementForm element) | Node node element <- flat items]
    leaves node = maybe False hostsOut (lookup node forms)
    enters node = maybe False hostsIn (lookup node forms)
    -- The entry of a flow has a port for each name that is bound, and none for a label on an edge.
    entered node = lookup node forms == Just Entered
    fix = \case
      Wire edge' -> Wire edge' {edgeSits = sitting edge'}
      Group node kind title inner -> Group node kind title (fmap fix inner)
      Result node name seats -> Result node name [if leaves at || null path then seat else Seat at [] | seat@(Seat at path) <- seats]
      item -> item
    sitting edge' = case (edgeSits edge', edgeLabel edge') of
      (OnOut, Just label) | leaves (edgeFrom edge') -> Joined [Join (Just (Seat (edgeFrom edge') [label])) Nothing Nothing]
      (OnIn, Just label) | enters (edgeTo edge') && not (entered (edgeTo edge')) -> Joined [Join Nothing (Just (Seat (edgeTo edge') [label])) Nothing]
      (Joined joins, _) -> Joined [Join (keep leaves from) (keep enters to) note | Join from to note <- joins]
      _ -> Loose
    keep hosted = \case
      Just seat@(Seat at path) | null path || hosted at -> Just seat
      _ -> Nothing

-- | The elements given their pins: the pins that the walk made, and a pin for each name that an edge is seated at.
pinned :: [(Node, [Pin])] -> [Item] -> [Item]
pinned made items = fmap put items
  where
    -- An edge that leaves a flow is drawn from its result, and has no pin of its own.
    wires = [edge' | Wire edge' <- flat items, isNothing (edgeVia edge')]
    joins = [join' | edge' <- wires, Joined js <- [edgeSits edge'], join' <- js]
    outSeats = [seat | Join (Just seat) _ _ <- joins] ++ concat [seats | Result _ _ seats <- flat items]
    inSeats = [seat | Join _ (Just seat) _ <- joins]
    put = \case
      Node node element ->
        Node
          node
          element
            { elementIn = if hostsIn (elementForm element) then nub [port | Seat at [port] <- inSeats, at == node] else [],
              elementOut =
                if hostsOut (elementForm element)
                  then foldl' (\pins (Seat _ path) -> ensure path pins) (concat [pins | (at, pins) <- made, at == node]) [seat | seat@(Seat at path) <- outSeats, at == node, not (null path)]
                  else []
            }
      Group node kind title inner -> Group node kind title (fmap put inner)
      item -> item
    ensure path pins
      | reaches path pins = pins
      | otherwise = pins ++ [Pin Nothing (Just (last path)) []]

-- | Whether the names lead to a pin from the pins: a pin that has no name is passed through.
reaches :: [String] -> [Pin] -> Bool
reaches [] _ = True
reaches (name : rest) pins = any (\pin -> pinName pin == Just name && reaches rest (pinParts pin)) (addressable pins)
  where
    addressable = concatMap (\pin -> if isNothing (pinName pin) then addressable (pinParts pin) else [pin])

-- | The graph of a chart: its items with the junctions that are one named alike, the decisions asked their questions, the
-- seats of the edges settled and the elements given their pins.
finish :: [(Node, Node)] -> [(Node, Maybe String)] -> [(Node, [Pin])] -> [Item] -> [Item]
finish aliases questions made =
  pinned [(followed' aliases node, pins) | (node, pins) <- made] . seated . tidy aliases questions
