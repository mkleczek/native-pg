{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | Records of fields named in their type: a record that 'newRecord' or '&&&' makes of steps is read by name, grown by
-- 'adding' and changed by 'at', and a step that names the fields it reads takes any record that has them, in each arrow
-- that runs programs. The same programs, written once for any arrow that has what a step that can't wait has, are run
-- by each. That a name that the record doesn't have, or that 'newRecord' has twice, doesn't compile is checked by
-- @test/check-named.sh@.
module NamedSpec (spec) where

import Control.Arrow (arr, (&&&), (>>>), (>>^))
import Control.Monad (forM_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import GHC.Records (HasField)
import Network.Streaming.Classes
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

-- | The stubs of newRecord, adding, at and field fail with 'NotImplemented': every case that needs one is expected to
-- fail, and the mark comes off when the code is there.
stubbed :: Spec -> Spec
stubbed = xfail "Network.Streaming.Named has stubs: newRecord, adding, at and field are not written yet"

spec :: Spec
spec = describe "fields named in their type" $ do
  stubbed $ it "makes a record from the values of a plain function, by the names in the order that they are given" $ do
    let r = newRecord ["name", "size", "kept"] "pipe" (3 :: Int) True
    (r.name, r.size, r.kept) `shouldBe` ("pipe", 3, True)

  forM_ runners $ \(Runner name run) -> describe name $ do
    it "reads the first, a middle and the last field of a record of four that &&& makes" $
      run (record >>^ \r -> (r.name, r.size, r.kept, r.count)) `shouldReturn` ("pipe", 3, True, 4)

    it "reads the only field of a record of one" $
      run (is "only" (arr (const 'x')) >>^ \r -> r.only) `shouldReturn` 'x'

    stubbed $ it "reads a field by field \"size\", whose output is that field" $ do
      run (record >>> field "size") `shouldReturn` 3
      run (record >>> field "name") `shouldReturn` "pipe"
      run (record >>> field "count") `shouldReturn` 4

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

    stubbed $ it "makes a record of three arrows with newRecord, <$> and <*>, and reads each field back" $
      run (three >>^ \r -> (r.name, r.size, r.kept)) `shouldReturn` ("pipe", 3, True)

    stubbed $ it "makes a record of four arrows with newRecord, <$> and <*>, and reads each field back" $
      run (four >>^ \r -> (r.name, r.size, r.kept, r.count)) `shouldReturn` ("pipe", 3, True, 4)

    stubbed $ it "runs the arrows that newRecord is applied to once each, from the first to the last" $ do
      noted <- newLog
      total <-
        run
          ( newRecord ["first", "second", "third"]
              <$> noting noted "first" 1
              <*> noting noted "second" 2
              <*> noting noted "third" 3
              >>> now (\r -> pure (r.first + r.second + r.third))
          )
      total `shouldBe` 6
      readLog noted `shouldReturn` ["first", "second", "third"]

    stubbed $ it "puts a field in front of a record, and keeps the rest of it" $
      run
        ( record >>> adding "extra" (arr (const 'e')) >>^ \(Is extra, rest) ->
            (extra, rest.name, rest.size, rest.kept, rest.count)
        )
        `shouldReturn` ('e', "pipe", 3, True, 4)

    stubbed $ it "gives a field that it adds the arrow's result, which reads the record" $
      run (record >>> adding "total" (now (\r -> pure (r.size + r.count))) >>^ \r -> (r.total, r.size))
        `shouldReturn` (7, 3)

    stubbed $ it "reads the newer of two fields of a name, which shadows the older, of whatever type" $
      run (record >>> adding "size" (field "name") >>^ \r -> (r.size, r.count)) `shouldReturn` ("pipe", 4)

    stubbed $ it "runs an arrow on the first field and puts the result in its place, of another type" $
      run (record >>> at "name" (arr length) >>^ \r -> (r.name, r.size, r.kept, r.count)) `shouldReturn` (4, 3, True, 4)

    stubbed $ it "runs an arrow on a middle field and puts the result in its place, of another type" $
      run (record >>> at "size" (arr show) >>^ \r -> (r.name, r.size, r.kept, r.count))
        `shouldReturn` ("pipe", "3", True, 4)

    stubbed $ it "runs an arrow on the last field and puts the result in its place, of another type" $
      run (record >>> at "count" (arr even) >>^ \r -> (r.name, r.size, r.kept, r.count))
        `shouldReturn` ("pipe", 3, True, True)

    stubbed $ it "runs an arrow on the only field of a record of one" $
      run (is "only" (arr (const 'x')) >>> at "only" (arr fromEnum) >>^ \r -> r.only) `shouldReturn` 120

    stubbed $ it "runs an arrow on a field of a record that is a field, with at \"outer\" . at \"inner\"" $
      run (nested >>> (at "outer" . at "inner") (arr show) >>^ \r -> (r.label, r.outer.inner, r.outer.kept))
        `shouldReturn` ("pipe", "3", True)

    stubbed $ it "runs a step that asks for two fields on records of different shapes" $ do
      run (record >>> sized) `shouldReturn` "pipe3"
      run (four >>> sized) `shouldReturn` "pipe3"
      run (record >>> adding "size" (arr (const 5)) >>> sized) `shouldReturn` "pipe5"
      run (is "size" (arr (const 8)) &&& is "name" (arr (const "tube")) >>> sized) `shouldReturn` "tube8"
      run (newRecord ["size", "name"] <$> arr (const 6) <*> arr (const "bar") >>> sized) `shouldReturn` "bar6"

-- | A record of four fields, each made by a step of its own.
record :: (Steps arr) => arr () ("name" := String, ("size" := Int, ("kept" := Bool, "count" := Int)))
record =
  is "name" (arr (const "pipe"))
    &&& is "size" (arr (const 3))
    &&& is "kept" (arr (const True))
    &&& is "count" (arr (const 4))

-- | The same record of four, which newRecord makes, with the same arrows.
four :: (Steps arr) => arr () ("name" := String, ("size" := Int, ("kept" := Bool, "count" := Int)))
four =
  newRecord ["name", "size", "kept", "count"]
    <$> arr (const "pipe")
    <*> arr (const 3)
    <*> arr (const True)
    <*> arr (const 4)

-- | A record of three fields, which newRecord makes.
three :: (Steps arr) => arr () ("name" := String, ("size" := Int, "kept" := Bool))
three = newRecord ["name", "size", "kept"] <$> arr (const "pipe") <*> arr (const 3) <*> arr (const True)

-- | A record that has a record in one of its fields.
nested :: (Steps arr) => arr () ("label" := String, "outer" := ("inner" := Int, "kept" := Bool))
nested = is "label" (arr (const "pipe")) &&& is "outer" (is "inner" (arr (const 3)) &&& is "kept" (arr (const True)))

-- | A step that asks for the two fields that it reads, and so takes any record that has them.
sized :: (Steps arr, HasField "name" r String, HasField "size" r Int) => arr r String
sized = now (\r -> pure (r.name <> show r.size))

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

-- | A step that notes its name, and gives the number.
noting :: (Effect arr) => Log -> String -> Int -> arr () Int
noting noted entry number = now (\() -> atomicModifyIORef' noted (\entries -> (entry : entries, ())) >> pure number)
