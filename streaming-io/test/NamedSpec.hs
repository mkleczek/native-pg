{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | Fields named in their type: a record that '&&&' makes of steps that 'is' names is read by name, in each arrow that
-- runs programs. The same programs, written once for any arrow that has what a step that can't wait has, are run by
-- each. That a name that the record doesn't have, or has twice, doesn't compile is checked by @test/check-named.sh@.
module NamedSpec (spec) where

import Control.Arrow (arr, (&&&), (>>>), (>>^))
import Control.Monad (forM_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Network.Streaming.Classes
import Network.Streaming.Combinators (field)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Named
import Network.Streaming.Threads
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
spec = describe "fields named in their type" $ forM_ runners $ \(Runner name run) -> describe name $ do
  it "reads the first, a middle and the last field of a record of four that &&& makes" $
    run (record >>^ \r -> (r.name, r.size, r.kept, r.count)) `shouldReturn` ("pipe", 3, True, 4)

  it "reads the only field of a record of one" $
    run (is "only" (arr (const 'x')) >>^ \r -> r.only) `shouldReturn` 'x'

  it "reads a field by field @\"size\", whose output is that field" $ do
    run (record >>> field @"size") `shouldReturn` 3
    run (record >>> field @"name") `shouldReturn` "pipe"
    run (record >>> field @"count") `shouldReturn` 4

  it "reads a field inside a step, where it is used" $
    run (record >>> now (\r -> pure (r.size + r.count))) `shouldReturn` 7

  it "reads the fields of a record that has a field added in front of it" $
    run ((is "extra" (arr (const 'e')) &&& record) >>^ \r -> (r.extra, r.name, r.count))
      `shouldReturn` ('e', "pipe", 4)

  it "runs the step of each field once, from the first to the last, and the names add no step" $ do
    noted <- newLog
    total <-
      run
        ( is "first" (noting noted "first" 1)
            &&& is "second" (noting noted "second" 2)
            &&& is "third" (noting noted "third" 3)
            >>> now (\r -> pure (r.first + r.second + r.third))
        )
    total `shouldBe` 6
    readLog noted `shouldReturn` ["first", "second", "third"]

-- | A record of four fields, each made by a step of its own.
record :: (Steps arr) => arr () ("name" := String, ("size" := Int, ("kept" := Bool, "count" := Int)))
record =
  is "name" (arr (const "pipe"))
    &&& is "size" (arr (const 3))
    &&& is "kept" (arr (const True))
    &&& is "count" (arr (const 4))

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

-- | A step that notes its name, and gives the number.
noting :: (Effect arr) => Log -> String -> Int -> arr () Int
noting noted entry number = now (\() -> atomicModifyIORef' noted (\entries -> (entry : entries, ())) >> pure number)
