{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | Steps wired by name: a flow of lines, each of which makes the input of its step from the names of the flow, runs in
-- each arrow that runs programs and gives what its steps give, a choice is a line of a flow whose branches are lines, and
-- a list of names takes the output of a step apart. That a name that the flow doesn't have, an output that a step doesn't
-- give, a port that is not bound or is bound twice, and a name that the flow has already don't compile, in a branch of a
-- choice too, and that a list of names that doesn't fit the output of its step doesn't, is checked by
-- @test/check-wiring.sh@.
module WiringSpec (spec) where

import Control.Arrow (arr, (>>>), (>>^), (|||))
import Control.Monad (forM_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Named
import Network.Streaming.Threads
import Network.Streaming.Wiring
import Skeletest

-- | The arrow of a step that can't wait.
type Program a = forall arr. (Steps arr) => arr () a

-- | A way to run a program: an arrow's.
data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners =
  [ Runner "threads" (\program -> runThreads program ()),
    Runner "delimited" (\program -> fst <$> runDelimited program ()),
    Runner "compiled" (\program -> fst <$> runCompiled program ()),
    Runner "loop" (\program -> fst <$> runLoop program ())
  ]

spec :: Spec
spec = describe "steps wired by name" $ forM_ runners $ \(Runner name run) -> describe name $ do
  it "wires three steps by bare names and by bindings of a port to a name, and gives what the last gave" $
    run (inputs >>> threeSteps) `shouldReturn` 18

  it "gives the record of the flow, with a field in front of it for each step that has a name" $
    run
      ( inputs
          >>> flow ("sum" <: adder -< ["a", "b"] >>> "double" <: adder -< ["a" =: "sum", "b" =: "sum"])
          >>^ \r -> (r.double, r.sum, r.a, r.b)
      )
      `shouldReturn` (14, 7, 3, 4)

  it "binds one name to several ports of a step, and to the ports of several steps" $
    run (inputs >>> fanOut) `shouldReturn` 42

  it "feeds the ports of a step from several producers" $
    run (inputs >>> fanIn) `shouldReturn` 19

  it "reads a field of the record that a step gave, by a path, and a field of a field, by a longer one" $ do
    run (inputs >>> fromRecord) `shouldReturn` 7
    run (inputs >>> fromNested) `shouldReturn` (44, 0)

  it "goes on with the record as it was after a step that gives nothing, which runs once, in its place" $ do
    noted <- newLog
    run (inputs >>> quiet noted) `shouldReturn` 11
    readLog noted `shouldReturn` ["sum 7", "total 11"]

  it "runs each step once, from the first to the last, as the lines are in order" $ do
    noted <- newLog
    run (inputs >>> ordered noted) `shouldReturn` 18
    readLog noted `shouldReturn` ["first", "second", "third"]

  it "runs the branch of the case that a choice finds, which binds the names that it reads, and gives what it gave" $ do
    -- The value of the case and an input of the flow, the value of the case and a step of the flow, and nothing.
    run (sized 3 >>> picked) `shouldReturn` 12
    run (sized 20 >>> picked) `shouldReturn` 44
    run (sized 500 >>> picked) `shouldReturn` 99

  it "runs the branch of a choice once, and the other branches not at all" $ do
    noted <- newLog
    run (sized 20 >>> chosen noted >>^ \r -> (r.a, r.b)) `shouldReturn` (20, 4)
    readLog noted `shouldReturn` ["big 20"]

  it "goes on with the record as it was after a choice that has no name" $ do
    noted <- newLog
    run (sized 3 >>> chosen noted >>^ \r -> (sizeName r.size, r.a, r.b)) `shouldReturn` ("small", 3, 4)
    readLog noted `shouldReturn` ["small 3"]

  it "takes the record that a step gave apart by the names of its fields, and keeps only the fields that the list names" $ do
    run (inputs >>> flow (["sum", "prod"] <: splitter -< ["a", "b"]) >>^ \r -> (r.sum, r.prod, r.a, r.b))
      `shouldReturn` (7, 12, 3, 4)

  it "keeps a field of the record that a step gave under another name, the others of the list under their own" $
    run (inputs >>> flow (["sum" =: "total", "diff"] <: splitter -< ["a", "b"]) >>^ \r -> (r.total, r.diff))
      `shouldReturn` (7, -1)

  it "takes a pair apart by position, and pairs that nest to the right, and drops the parts that are named \"_\"" $ do
    run (sized 17 >>> flow (["q", "r"] <: divider -< ["a", "b"]) >>^ \r -> (r.q, r.r)) `shouldReturn` (4, 1)
    run (inputs >>> flow (["first", "_", "last"] <: tripler -< ["a", "b"]) >>^ \r -> (r.first, r.last))
      `shouldReturn` (3, 7)

  it "reads the names that a list gives in the steps after it, and in the branches of a choice" $ do
    run (sized 17 >>> divided) `shouldReturn` 5
    run (sized 3 >>> rounded) `shouldReturn` 3
    run (sized 20 >>> rounded) `shouldReturn` 20
    run (sized 500 >>> rounded) `shouldReturn` 99

  it "runs a step once for the names that it gives, and goes on with the names in front of the record" $ do
    noted <- newLog
    run (inputs >>> tallied noted) `shouldReturn` 7
    readLog noted `shouldReturn` ["ran"]

  it "reads the names of a record that is not known, for a flow that is a step of its own" $ do
    run (inputs >>> anyRecord) `shouldReturn` 14
    run (newRecord ["extra", "b", "a"] <$> arr (const 'x') <*> arr (const 5) <*> arr (const 1) >>> anyRecord)
      `shouldReturn` 12

-- | The record that the flows are run on.
inputs :: (Steps arr) => arr () ("a" := Int, "b" := Int)
inputs = newRecord ["a", "b"] <$> arr (const 3) <*> arr (const 4)

-- | A step that reads its ports "a" and "b", and gives their sum.
adder :: (Effect arr, HasField "a" r Int, HasField "b" r Int) => arr r Int
adder = now (\r -> pure (r.a + r.b))

-- | A step that reads its ports "a" and "b", and gives their product.
multiplier :: (Effect arr, HasField "a" r Int, HasField "b" r Int) => arr r Int
multiplier = now (\r -> pure (r.a * r.b))

-- | A step that reads its port "n", and gives a record of two outputs: the number, and the one after it.
pairer :: (Effect arr, HasField "n" r Int) => arr r ("this" := Int, "next" := Int)
pairer = now (\r -> pure (newRecord ["this", "next"] r.n (r.n + 1)))

-- | A step that reads its port "n", and gives a record that has a record in a field, which has a record in a field.
nester :: (Effect arr, HasField "n" r Int) => arr r ("inner" := ("leaf" := Int, "twin" := Int), "other" := Int)
nester = now (\r -> pure (newRecord ["inner", "other"] (newRecord ["leaf", "twin"] (r.n * 10) r.n) 0))

-- | A step that reads its ports "a" and "b", and gives a record of three fields: their sum, difference and product.
splitter :: (Effect arr, HasField "a" r Int, HasField "b" r Int) => arr r ("sum" := Int, ("diff" := Int, "prod" := Int))
splitter = now (\r -> pure (newRecord ["sum", "diff", "prod"] (r.a + r.b) (r.a - r.b) (r.a * r.b)))

-- | A step that reads its ports "a" and "b", and gives a pair: the quotient and the remainder.
divider :: (Effect arr, HasField "a" r Int, HasField "b" r Int) => arr r (Int, Int)
divider = now (\r -> pure (r.a `div` r.b, r.a `mod` r.b))

-- | A step that reads its ports "a" and "b", and gives pairs that nest to the right: "a", "b" and their sum.
tripler :: (Effect arr, HasField "a" r Int, HasField "b" r Int) => arr r (Int, (Int, Int))
tripler = now (\r -> pure (r.a, (r.b, r.a + r.b)))

-- | A step that notes that it ran, reads its ports "a" and "b", and gives them as a pair.
tallying :: (Effect arr, HasField "a" r Int, HasField "b" r Int) => Log -> arr r (Int, Int)
tallying noted = now (\r -> atomicModifyIORef' noted (\entries -> ("ran" : entries, ())) >> pure (r.a, r.b))

-- | A step that reads its port "v", and notes it with the label.
noting :: (Effect arr, HasField "v" r Int) => Log -> String -> arr r ()
noting noted label = now (\r -> atomicModifyIORef' noted (\entries -> ((label <> " " <> show r.v) : entries, ())))

-- | A step that reads its port "n", and gives its size.
sizer :: (Effect arr, HasField "n" r Int) => arr r Size
sizer = now (\r -> pure (if r.n < 10 then inj "small" r.n else if r.n < 100 then inj "big" r.n else inj "huge" r.n))

-- | A step that reads no port, and gives a number.
constant :: (Effect arr) => Int -> arr r Int
constant n = now (const (pure n))

-- | A step that notes its label and reads its ports "a" and "b", and gives their sum.
counting :: (Effect arr, HasField "a" r Int, HasField "b" r Int) => Log -> String -> arr r Int
counting noted label = now (\r -> atomicModifyIORef' noted (\entries -> (label : entries, ())) >> pure (r.a + r.b))

-- | Three steps: the bare names "a" and "b" of the inputs, the names of earlier steps, and a port bound to a name.
threeSteps :: (Steps arr) => arr ("a" := Int, "b" := Int) Int
threeSteps =
  result
    "total"
    ( "sum" <: adder -< ["a", "b"]
        >>> "double" <: adder -< ["a" =: "sum", "b" =: "sum"]
        >>> "total" <: adder -< ["a" =: "double", "b"]
    )

-- | The name "a" bound to both ports of a step, and the name of a step bound to a port of two steps.
fanOut :: (Steps arr) => arr ("a" := Int, "b" := Int) Int
fanOut =
  result
    "fanned"
    ( "self" <: adder -< ["a", "b" =: "a"]
        >>> "twice" <: multiplier -< ["a" =: "self", "b" =: "self"]
        >>> "fanned" <: adder -< ["a" =: "self", "b" =: "twice"]
    )

-- | A step whose ports are two different producers.
fanIn :: (Steps arr) => arr ("a" := Int, "b" := Int) Int
fanIn =
  result
    "joined"
    ( "product" <: multiplier -< ["a", "b"]
        >>> "sum" <: adder -< ["a", "b"]
        >>> "joined" <: adder -< ["a" =: "product", "b" =: "sum"]
    )

-- | Ports bound to the fields of the record that a step gave.
fromRecord :: (Steps arr) => arr ("a" := Int, "b" := Int) Int
fromRecord =
  result
    "total"
    ( "parts" <: pairer -< ["n" =: "a"]
        >>> "total" <: adder -< ["a" =: "parts.next", "b" =: "parts.this"]
    )

-- | Ports bound to a field of a field of the record that a step gave, and to a field of it.
fromNested :: (Steps arr) => arr ("a" := Int, "b" := Int) (Int, Int)
fromNested =
  result
    "both"
    ( "nested" <: nester -< ["n" =: "b"]
        >>> "leaves" <: adder -< ["a" =: "nested.inner.leaf", "b" =: "nested.inner.twin"]
        >>> "other" <: adder -< ["a" =: "nested.other", "b" =: "nested.other"]
        >>> "both" <: pairing -< ["a" =: "leaves", "b" =: "other"]
    )
  where
    pairing = now (\r -> pure (r.a, r.b))

-- | Steps that give nothing between steps that do: the record goes on as it was.
quiet :: (Steps arr) => Log -> arr ("a" := Int, "b" := Int) Int
quiet noted =
  result
    "total"
    ( "sum" <: adder -< ["a", "b"]
        >>> noting noted "sum" -< ["v" =: "sum"]
        >>> "total" <: adder -< ["a" =: "sum", "b"]
        >>> noting noted "total" -< ["v" =: "total"]
    )

-- | Three steps that note that they ran.
ordered :: (Steps arr) => Log -> arr ("a" := Int, "b" := Int) Int
ordered noted =
  result
    "third"
    ( "first" <: counting noted "first" -< ["a", "b"]
        >>> "second" <: counting noted "second" -< ["a" =: "first", "b"]
        >>> "third" <: counting noted "third" -< ["a" =: "second", "b" =: "first"]
    )

-- | The sum that 'sizer' gives, which a choice takes apart.
type Size = Either ("small" := Int) (Either ("big" := Int) ("huge" := Int))

-- | The case that a size is in.
sizeName :: Size -> String
sizeName = either (const "small") (either (const "big") (const "huge"))

-- | The record that the flows of a choice are run on: the number that is sized, and another.
sized :: (Steps arr) => Int -> arr () ("a" := Int, "b" := Int)
sized n = newRecord ["a", "b"] <$> arr (const n) <*> arr (const 4)

-- | A choice by the size of "a", whose branches bind the value of their case and an input of the flow, the value of
-- their case and a step of the flow, and nothing, and whose result is named.
picked :: (Steps arr) => arr ("a" := Int, "b" := Int) Int
picked =
  result
    "picked"
    ( "size" <: sizer -< ["n" =: "a"]
        >>> "sum" <: adder -< ["a", "b"]
        >>> "picked"
          <: choice
            "how big?"
            "size"
            ( "small" ?> multiplier -< ["a" =: "small", "b"]
                ||| "big" ?> adder -< ["a" =: "big", "b" =: "sum"]
                ||| "huge" ?> constant 99 -< []
            )
    )

-- | A choice whose branches note that they ran, with no name for its result.
chosen :: (Steps arr) => Log -> arr ("a" := Int, "b" := Int) ("size" := Size, ("a" := Int, "b" := Int))
chosen noted =
  flow
    ( "size" <: sizer -< ["n" =: "a"]
        >>> choice
          "how big?"
          "size"
          ( "small" ?> noting noted "small" -< ["v" =: "small"]
              ||| "big" ?> noting noted "big" -< ["v" =: "big"]
              ||| "huge" ?> noting noted "huge" -< ["v" =: "huge"]
          )
    )

-- | The parts of a pair, which a list of names takes apart, read by the step after it.
divided :: (Steps arr) => arr ("a" := Int, "b" := Int) Int
divided =
  result
    "total"
    ( ["q", "r"] <: divider -< ["a", "b"]
        >>> "total" <: adder -< ["a" =: "q", "b" =: "r"]
    )

-- | The parts of a pair read by the branches of a choice, with a name that the flow has apart from the pattern.
rounded :: (Steps arr) => arr ("a" := Int, "b" := Int) Int
rounded =
  result
    "rounded"
    ( ["q", "r"] <: divider -< ["a", "b"]
        >>> "size" <: sizer -< ["n" =: "a"]
        >>> "rounded"
          <: choice
            "how big?"
            "size"
            ( "small" ?> adder -< ["a" =: "small", "b" =: "q"]
                ||| "big" ?> adder -< ["a" =: "big", "b" =: "r"]
                ||| "huge" ?> constant 99 -< []
            )
    )

-- | A step that gives a pair, which a list of names takes apart: the flow runs it once.
tallied :: (Steps arr) => Log -> arr ("a" := Int, "b" := Int) Int
tallied noted =
  result
    "total"
    ( ["x", "y"] <: tallying noted -< ["a", "b"]
        >>> "total" <: adder -< ["a" =: "x", "b" =: "y"]
    )

-- | A flow over any record that has the names that its steps read, which is a step of its own.
anyRecord :: (Steps arr, HasField "a" r Int, HasField "b" r Int) => arr r Int
anyRecord =
  result
    "twice"
    ( "sum" <: adder -< ["a", "b"]
        >>> "twice" <: adder -< ["a" =: "sum", "b" =: "sum"]
    )

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef
