{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExplicitNamespaces #-}

-- | The charts of "Network.Streaming.Diagram.Mermaid": small programs, built at the diagram arrow, whose charts have what
-- they should.
module MermaidSpec (spec) where

import Control.Arrow ((&&&), (>>>), (>>^), (^>>), (|||), returnA)
import Data.Char (isAlphaNum)
import Data.List (isInfixOf, isPrefixOf, nub, sort, stripPrefix)
import Data.Maybe (fromMaybe)
import GHC.Records (HasField, getField)
import Network.Streaming.Classes
import Network.Streaming.Combinators (_Just, box, decide, done, step, tap, withInbox)
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (..))
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.Diagram.Mermaid (Page (..), pages)
import Network.Streaming.Named (adding, at, cases, field, inj, is, newRecord, (:=), (?>))
import Network.Streaming.Plumbing (each, prism)
import Network.Streaming.Wiring (choice, result, type (=:), type (@:), (-<), (<:))
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

-- | The code that draws a name or a tuple on the right of '-<' is not written yet: the cases that need it are expected to
-- fail, and the marks come off when the code is there.
stubbed :: Spec -> Spec
stubbed = xfail "Network.Streaming.Wiring has a stub: a name or a tuple on the right of -< is not written yet"

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

  it "has a group for a grace, which says its period" $ do
    chart <- chartOf (grace (secs 30) (quick >>> quick) :: Diagram () ())
    any ("grace 30 s" `isInfixOf`) chart `shouldBe` True

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

  it "has the name of a step of a flow for the title of its box" $ do
    chart <- chartOf wired
    case (subgraphTitled "first" chart, subgraphTitled "last" chart) of
      (Just (_, before), Just (_, after)) ->
        (length (nodesDeclared (gear ++ "pair") before), length (nodesDeclared (gear ++ "sum") after)) `shouldBe` (1, 1)
      found -> failTest ("the chart has the boxes " <> show (fmap fst <$> [fst found, snd found]))
    -- The step that gives nothing is no box of its own.
    subgraphTitled "note" chart `shouldBe` Nothing

  it "has the question of a choice in the box of its line" $ do
    chart <- chartOf choosing
    case subgraphTitled "picked" chart of
      Just (_, inside) -> any ("{\"how big?\"}" `isInfixOf`) inside `shouldBe` True
      Nothing -> failTest "the chart has no box for the line of the choice"

  it "has an edge into a step of a flow from the box of each name that it binds, and from the start for the names of the input, past the steps between" $ do
    chart <- chartOf fanning
    -- The name "i" is read by the first step and the last, so it leaves the start twice.
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "alpha", "i"),
          ("start", "gamma", "i"),
          ("a", "beta", ""),
          ("b", "gamma", ""),
          ("gamma", "end", "")
        ]

  it "has on an edge from a box that is a whole the field of a path alone and a port that is another name after it, and the names of one producer on one edge" $ do
    chart <- chartOf ports
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "pair", "i"),
          ("start", "sum", "j \x2192 k, j"),
          ("first", "sum", "x \x2192 n, y \x2192 m"),
          ("sum", "end", "")
        ]

  it "has an edge that is dotted from the step before to a step that binds nothing from it, and none to a step that does" $ do
    chart <- chartOf quiet
    -- "note" gives nothing, so "beta" cannot read it: its edge says only the order. "note" reads "alpha": its edge is of the name.
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "alpha", "i"),
          ("start", "beta", "j"),
          ("a", "note", "\x2192 n"),
          ("beta", "end", "")
        ]
    orderEdges chart `shouldBe` [("note", "beta")]
    -- The order edge has a style of its own: it is dotted and drawn in grey, as a data edge never is.
    any (\line -> "linkStyle" `isPrefixOf` line && "stroke:#8c959f" `isInfixOf` line) (stripped chart) `shouldBe` True

  it "has a box for each name of a pattern of a choice's branch beside its step, and an edge to the step from the exact box that it reads, with the case on the edge from the decision" $ do
    chart <- chartOf choosing
    case subgraphTitled "picked" chart of
      Just (_, inside) -> do
        [length (boxesDeclared name inside) | name <- ["s", "x", "y", "lo", "hi"]] `shouldBe` [1, 1, 1, 1, 1]
        -- A name that is kept and taken apart is a box of its own, which has the names of its parts.
        case subgraphTitled "h" inside of
          Just (_, parts) -> [length (boxesDeclared name parts) | name <- ["lo", "hi", "s"]] `shouldBe` [1, 1, 0]
          Nothing -> failTest "the choice has no box for the name that is kept and taken apart"
        -- A case that has no pattern has no box.
        subgraphTitled "none" inside `shouldBe` Nothing
      Nothing -> failTest "the chart has no box for the line of the choice"
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "size up", "i"),
          ("size", "how big?", ""),
          ("how big?", "take", "small"),
          ("how big?", "add", "big"),
          ("how big?", "span", "huge"),
          ("how big?", "zero", "none"),
          ("s", "take", "\x2192 n"),
          ("x", "add", "\x2192 n"),
          ("y", "add", "\x2192 m"),
          ("lo", "span", "\x2192 n"),
          ("h", "span", "hi \x2192 m"),
          ("start", "span", "j"),
          ("take", "IO", ""),
          ("add", "IO", ""),
          ("span", "IO", ""),
          ("zero", "IO", ""),
          ("IO", "end", "")
        ]

  it "has the names of a flow that is a step of another flow apart from the names of the other flow" $ do
    chart <- chartOf nesting
    -- The inner flow reads its own "n", which the outer step binds: it is drawn from the box of "n" to the start of the inner
    -- flow, a junction, and not from the step "first" of the outer flow.
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "first", "i"),
          ("n", "junction", ""),
          ("junction", "tail", "n"),
          ("tail", "end", "")
        ]

  it "has a box for each name of a pattern, nested as the pattern nests, with the step in the box of its whole" $ do
    chart <- chartOf nestedNames
    case subgraphTitled "conn" chart of
      Nothing -> failTest "the chart has no box for the whole"
      Just (_, inside) -> do
        length (nodesDeclared (gear ++ "accept") inside) `shouldBe` 1
        -- "socket" is taken apart and not kept: a box with the titles of its parts, which are small nodes.
        case subgraphTitled "socket" inside of
          Just (_, parts) -> ([length (boxesDeclared name parts) | name <- ["fd", "addr", "peer"]]) `shouldBe` [1, 1, 0]
          Nothing -> failTest "the whole has no box for the field that is taken apart"
        length (boxesDeclared "peer" inside) `shouldBe` 1
    -- A step that gives a name of its own is a box beside it, and not in the box of the whole.
    (subgraphTitled "tuned" chart == Nothing, subgraphTitled "c" chart == Nothing) `shouldBe` (False, False)

  it "has an edge from the box of the exact name that a step reads: a part, a whole, and a part of a whole by a path, with the port that is another name" $ do
    chart <- chartOf nestedNames
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "accept", "i, j"),
          ("fd", "tune", "\x2192 n"),
          ("conn", "serve", "peer \x2192 a"),
          ("peer", "serve", "\x2192 b"),
          ("addr", "serve", "\x2192 c"),
          ("tuned", "serve", "\x2192 d"),
          ("serve", "end", "")
        ]
    orderEdges chart `shouldBe` []

  it "has a box that has no title for the step and the names of a list, and no box for a field that the list leaves out" $ do
    chart <- chartOf dividing
    case subgraphTitled " " chart of
      Just (_, inside) -> do
        length (nodesDeclared (gear ++ "divide") inside) `shouldBe` 1
        [length (boxesDeclared name inside) | name <- ["q", "r"]] `shouldBe` [1, 1]
      Nothing -> failTest "the chart has no box for the step that gives the names q and r"
    -- A field under another name is a box of that name, and a field that the list leaves out has none.
    chart' <- chartOf renaming
    ([length (boxesDeclared name chart') | name <- ["left", "z", "x", "y"]]) `shouldBe` [1, 1, 0, 0]

  it "has an edge from the box of each name of a list to each step that reads it" $ do
    chart <- chartOf dividing
    -- "q" and "r" are boxes beside "divide": "mix" reads both, and "plus" reads "q" under its port.
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "divide", "i, j"),
          ("q", "plus", "\x2192 n"),
          ("q", "mix", ""),
          ("r", "mix", ""),
          ("s", "mix", ""),
          ("mix", "end", "")
        ]
    chart' <- chartOf renaming
    edgesNamed chart'
      `shouldBe` sort
        [ ("start", "make", "i, j"),
          ("left", "use", ""),
          ("z", "use", ""),
          ("use", "end", "")
        ]

  stubbed $ it "has the chart of a step that takes a name whole, and of a step that takes a tuple of names, as of the same steps that bind the names in lists" $ do
    listedChart <- chartOf listed
    -- "gamma" reads "i" from the start, and "a" and "b" from their boxes.
    edgesNamed listedChart
      `shouldBe` sort
        [ ("start", "alpha", "i"),
          ("start", "beta", "j"),
          ("start", "gamma", "i"),
          ("a", "beta", ""),
          ("a", "gamma", ""),
          ("b", "gamma", ""),
          ("gamma", "end", "")
        ]
    chart <- chartOf wholes
    chart `shouldBe` listedChart

  it "has the same edges for a flow with the plumbing as without it" $ do
    found <- pages fanning
    case found of
      page : _ -> pagePlumbing page `shouldBe` pageMermaid page
      [] -> failTest "the program has no page"

  it "has the edges that a flow has from the start from the step before it instead, when that is the one node that leads into it" $ do
    chart <- chartOf (tap quick >>> fanning)
    edgesNamed chart
      `shouldBe` sort
        [ ("start", "IO", ""),
          ("IO", "alpha", "i"),
          ("IO", "gamma", "i"),
          ("a", "beta", ""),
          ("b", "gamma", ""),
          ("gamma", "end", "")
        ]

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

-- | A flow of three steps over two inputs: the first and the last have names, and the one between them gives nothing.
wired :: Diagram ("i" := Int, "j" := Int) ()
wired =
  result
    "last"
    ( "first" <: step "pair" (\r -> pure (newRecord ["x", "y"] (getField @"i" r + 1) (getField @"j" r))) -< ["i"]
        >>> step "note" (\r -> pure (getField @"n" r `seq` ())) -< ["n" =: "first.x"]
        >>> "last" <: step "sum" (\r -> pure (getField @"n" r + getField @"j" r)) -< ["n" =: "first.x", "j"]
    )
    >>> quick

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

-- | A flow whose first step gives a pair that a list of names takes apart: the step after it reads one of the names, and
-- the last reads both and the name of the step between.
dividing :: Diagram ("i" := Int, "j" := Int) Int
dividing =
  result
    "c"
    ( ["q", "r"] <: step "divide" (\r -> pure (getField @"i" r `div` getField @"j" r, getField @"i" r `mod` getField @"j" r)) -< ["i", "j"]
        >>> "s" <: step "plus" (\r -> pure (getField @"n" r + 1)) -< ["n" =: "q"]
        >>> "c" <: step "mix" (\r -> pure (getField @"q" r + getField @"r" r + getField @"s" r)) -< ["q", "r", "s"]
    )

-- | A flow whose first step gives a record of three fields, of which a list keeps two, one under another name, and the last
-- step reads both.
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

-- | A flow whose steps take a name whole, a pair of names and a triple of names.
wholes :: Diagram ("i" := Int, "j" := Int) Int
wholes =
  result
    "c"
    ( "a" <: step "alpha" (\n -> pure (n + 1)) -< "i"
        >>> "b" <: step "beta" (\(n, m) -> pure (n + m)) -< ("a", "j")
        >>> "c" <: step "gamma" (\(n, (m, k)) -> pure (n + m + k)) -< ("i", "a", "b")
    )

-- | The same flow with the names in lists, whose steps read the ports.
listed :: Diagram ("i" := Int, "j" := Int) Int
listed =
  result
    "c"
    ( "a" <: step "alpha" (\r -> pure (getField @"i" r + 1)) -< ["i"]
        >>> "b" <: step "beta" (\r -> pure (getField @"a" r + getField @"j" r)) -< ["a", "j"]
        >>> "c" <: step "gamma" (\r -> pure (getField @"i" r + getField @"a" r + getField @"b" r)) -< ["i", "a", "b"]
    )

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

-- | The edges of the chart, each as the names of its two nodes and its label, which is empty if it has none.
edgesNamed :: [String] -> [(String, String, String)]
edgesNamed chart = sort [(nameOf chart from, nameOf chart to, label) | line <- stripped chart, Just (from, to, label) <- [edgeOf line]]

-- | The edges of the chart that say only the order, each as the names of its two nodes: the dotted ones that have no label.
orderEdges :: [String] -> [(String, String)]
orderEdges chart = sort [(nameOf chart from, nameOf chart to) | [from, "-.->", to] <- words <$> stripped chart]

-- | The nodes that are declared as a box that has only a name, a small node, which is a stadium.
boxesDeclared :: String -> [String] -> [String]
boxesDeclared name chart = [takeWhile isAlphaNum line | line <- stripped chart, ("([\"" ++ name ++ "\"])") `isInfixOf` line]

-- | The nodes and the label of a line that is an edge, with a solid arrow.
edgeOf :: String -> Maybe (String, String, String)
edgeOf line = case break (== ' ') line of
  (from, ' ' : rest) | not (null from), all isAlphaNum from -> case stripPrefix "-->" rest of
    Just (' ' : to) | all isAlphaNum to -> Just (from, to, "")
    Just ('|' : '"' : labelled) -> case breakText "\"| " labelled of
      (label, after) | not (null after), to <- drop 3 after, all isAlphaNum to -> Just (from, to, label)
      _ -> Nothing
    _ -> Nothing
  _ -> Nothing

-- | The text before the first place where the marker is, and what is from it on.
breakText :: String -> String -> (String, String)
breakText marker text = go [] text
  where
    go before rest
      | marker `isPrefixOf` rest = (reverse before, rest)
      | otherwise = case rest of
          [] -> (reverse before, [])
          c : more -> go (c : before) more

-- | What a node of the chart is called: "start" and "end" for the circles, "junction" for a junction, the title of a box that
-- is a subgraph, and else the text of its box, without the gear and without the comment under it.
nameOf :: [String] -> String -> String
nameOf chart node = case [line | line <- stripped chart, takeWhile isAlphaNum line == node, not (any (`isInfixOf` line) ["-->", "subgraph"])] of
  line : _
    | "sm-circ" `isInfixOf` line -> "start"
    | "fr-circ" `isInfixOf` line -> "end"
    | "f-circ" `isInfixOf` line -> "junction"
    | otherwise -> withoutGear (fst (breakText "<br/>" (fst (breakText "\"" (drop 1 (dropWhile (/= '"') line))))))
  [] -> case [line | line <- stripped chart, ("subgraph " ++ node ++ " ") `isPrefixOf` line] of
    line : _ -> fst (breakText "\"" (drop 1 (dropWhile (/= '"') line)))
    [] -> node
  where
    withoutGear text = fromMaybe text (stripPrefix gear text)

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
