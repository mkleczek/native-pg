{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | Records whose fields are named in their type, and sums whose cases are: a record is made by 'newRecord' or '&&&',
-- read by name, grown by 'adding' and changed by 'at', and a step that names the fields it reads takes any record that
-- has them. A sum is made by 'inj', taken apart by 'cases' or 'casesBy' and a chain of '?>', and changed in one case by
-- '_Case'. The cases run in each arrow that runs programs. That a name that a record or a choice doesn't have, or has
-- twice, doesn't compile is checked by @test/check-named.sh@.
module NamedSpec (spec) where

import Control.Arrow (arr, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Monad (forM_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Combinators (decide, tap)
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
spec = describe "fields named in their type" $ do
  it "makes a record from the values of a plain function, by the names in the order that they are given" $ do
    let r = newRecord ["name", "size", "kept"] "pipe" (3 :: Int) True
    (r.name, r.size, r.kept) `shouldBe` ("pipe", 3, True)

  forM_ runners $ \(Runner name run) -> describe name $ do
    it "reads the first, a middle and the last field of a record of four that &&& makes" $
      run (record >>^ \r -> (r.name, r.size, r.kept, r.count)) `shouldReturn` ("pipe", 3, True, 4)

    it "reads the only field of a record of one" $
      run (is "only" (arr (const 'x')) >>^ \r -> r.only) `shouldReturn` 'x'

    it "reads a field by field \"size\", whose output is that field" $ do
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

    it "makes a record of three arrows with newRecord, <$> and <*>, and reads each field back" $
      run (three >>^ \r -> (r.name, r.size, r.kept)) `shouldReturn` ("pipe", 3, True)

    it "makes a record of four arrows with newRecord, <$> and <*>, and reads each field back" $
      run (four >>^ \r -> (r.name, r.size, r.kept, r.count)) `shouldReturn` ("pipe", 3, True, 4)

    it "runs the arrows that newRecord is applied to once each, from the first to the last" $ do
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

    it "puts a field in front of a record, and keeps the rest of it" $
      run
        ( record >>> adding "extra" (arr (const 'e')) >>^ \(Is extra, rest) ->
            (extra, rest.name, rest.size, rest.kept, rest.count)
        )
        `shouldReturn` ('e', "pipe", 3, True, 4)

    it "gives a field that it adds the arrow's result, which reads the record" $
      run (record >>> adding "total" (now (\r -> pure (r.size + r.count))) >>^ \r -> (r.total, r.size))
        `shouldReturn` (7, 3)

    it "reads the newer of two fields of a name, which shadows the older, of whatever type" $
      run (record >>> adding "size" (field "name") >>^ \r -> (r.size, r.count)) `shouldReturn` ("pipe", 4)

    it "runs an arrow on the newer of two fields of a name and leaves the older as it was" $ do
      run (record >>> adding "size" (field "name") >>> at "size" (arr length) >>^ \r@(_, older) -> (r.size, older.size))
        `shouldReturn` (4, 3)
      run
        ( record >>> adding "size" (field "name") >>> adding "extra" (arr (const 'e')) >>> at "size" (arr length)
            >>^ \(_, r@(_, older)) -> (r.size, older.size)
        )
        `shouldReturn` (4, 3)

    it "runs an arrow on the first field and puts the result in its place, of another type" $
      run (record >>> at "name" (arr length) >>^ \r -> (r.name, r.size, r.kept, r.count)) `shouldReturn` (4, 3, True, 4)

    it "runs an arrow on a middle field and puts the result in its place, of another type" $
      run (record >>> at "size" (arr show) >>^ \r -> (r.name, r.size, r.kept, r.count))
        `shouldReturn` ("pipe", "3", True, 4)

    it "runs an arrow on the last field and puts the result in its place, of another type" $
      run (record >>> at "count" (arr even) >>^ \r -> (r.name, r.size, r.kept, r.count))
        `shouldReturn` ("pipe", 3, True, True)

    it "runs an arrow on the only field of a record of one" $
      run (is "only" (arr (const 'x')) >>> at "only" (arr fromEnum) >>^ \r -> r.only) `shouldReturn` 120

    it "runs an arrow on a field of a record that is a field, with at \"outer\" . at \"inner\"" $
      run (nested >>> (at "outer" . at "inner") (arr show) >>^ \r -> (r.label, r.outer.inner, r.outer.kept))
        `shouldReturn` ("pipe", "3", True)

    it "runs a step that asks for two fields on records of different shapes" $ do
      run (record >>> sized) `shouldReturn` "pipe3"
      run (four >>> sized) `shouldReturn` "pipe3"
      run (record >>> adding "size" (arr (const 5)) >>> sized) `shouldReturn` "pipe5"
      run (is "size" (arr (const 8)) &&& is "name" (arr (const "tube")) >>> sized) `shouldReturn` "tube8"
      run (newRecord ["size", "name"] <$> arr (const 6) <*> arr (const "bar") >>> sized) `shouldReturn` "bar6"

    it "hands each case of a choice of three to its branch, with its value, the rest of the record and a record" $ do
      forM_ [(0, "ended GET /", "stream ended"), (404, "GET / refused 404", "refused"), (1000, "GET / good head 1000", "good")] $ \(number, expected, taken) -> do
        noted <- newLog
        run (const number ^>> heads noted) `shouldReturn` expected
        readLog noted `shouldReturn` [taken]

    it "hands each case of a choice of four to its branch, with its value and the rest of the record" $ do
      forM_ [(5, "GET / tiny 5", "tiny"), (50, "GET / small 50", "small"), (500, "GET / medium 500", "medium"), (5000, "GET / large 5000", "large")] $ \(number, expected, taken) -> do
        noted <- newLog
        run (const number ^>> sizes noted) `shouldReturn` expected
        readLog noted `shouldReturn` [taken]

    it "hands each case of a classification of a field to its branch, with casesBy" $ do
      forM_ [(200, "GET / ok 200", "ok"), (404, "GET / missing 404", "missing"), (503, "GET / broken 503", "broken")] $ \(status, expected, taken) -> do
        noted <- newLog
        run (const status ^>> classified noted) `shouldReturn` expected
        readLog noted `shouldReturn` [taken]

    it "runs the arrow on the case of the name of a plain sum, with another type, and passes the other cases through" $ do
      noted <- newLog
      run (const (inj "failed" "boom" :: Outcome) ^>> _Case "failed" (tap (noting' noted "failed") >>^ length) >>^ showOutcome)
        `shouldReturn` "failed 4"
      readLog noted `shouldReturn` ["failed \"boom\""]
      passed <- newLog
      run (const (inj "done" 7 :: Outcome) ^>> _Case "failed" (tap (noting' passed "failed") >>^ length) >>^ showOutcome)
        `shouldReturn` "done 7"
      readLog passed `shouldReturn` []

    it "runs the arrow on the first case of a plain sum, and on the last, with _Case" $ do
      run (const (inj "done" 7 :: Outcome) ^>> _Case "done" (arr show) >>^ showOutcome) `shouldReturn` "done \"7\""
      run (const (inj "failed" "boom" :: Outcome) ^>> _Case "failed" (arr (const True)) >>^ showOutcome) `shouldReturn` "failed True"

    it "composes with at: at \"outcome\" . _Case \"failed\" runs the arrow on the case of a field that is a sum" $ do
      noted <- newLog
      run (withOutcome (inj "failed" "boom") >>> (at "outcome" . _Case "failed") (tap (noting' noted "failed") >>^ length) >>^ \r -> (showOutcome r.outcome, r.kept))
        `shouldReturn` ("failed 4", True)
      readLog noted `shouldReturn` ["failed \"boom\""]
      passed <- newLog
      run (withOutcome (inj "done" 7) >>> (at "outcome" . _Case "failed") (tap (noting' passed "failed") >>^ length) >>^ \r -> (showOutcome r.outcome, r.kept))
        `shouldReturn` ("done 7", True)
      readLog passed `shouldReturn` []

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

-- | How the head of the request ended, which the producer makes by naming the case, and a chain of branches takes apart.
-- Neither the producer nor the branches has a signature: the type of the choice comes from the branches. Each branch reads
-- its case, the request and, for the good head, a record.
heads :: (Steps arr) => Log -> arr Int String
heads noted =
  newRecord ["req", "number"] "GET /"
    ^>> adding "head" (arr ended)
    >>> decide "how did the head end?" (cases "head")
    >>> ( "stream ended" ?> branch noted "stream ended" (\r -> "ended " <> r.req)
            ||| "refused" ?> branch noted "refused" (\r -> r.req <> " refused " <> show r.refused)
            ||| "good" ?> branch noted "good" (\r -> r.req <> " good " <> r.good.parsed <> " " <> show r.good.consumed)
        )
  where
    ended r
      | r.number == 0 = inj "stream ended" ()
      | r.number < 600 = inj "refused" r.number
      | otherwise = inj "good" (newRecord ["parsed", "consumed"] "head" r.number)

-- | A choice of four cases, by the size of the number.
sizes :: (Steps arr) => Log -> arr Int String
sizes noted =
  newRecord ["req", "number"] "GET /"
    ^>> adding "size" (arr sized')
    >>> decide "how big?" (cases "size")
    >>> ( "tiny" ?> branch noted "tiny" (\r -> r.req <> " tiny " <> show r.tiny)
            ||| "small" ?> branch noted "small" (\r -> r.req <> " small " <> show r.small)
            ||| "medium" ?> branch noted "medium" (\r -> r.req <> " medium " <> show r.medium)
            ||| "large" ?> branch noted "large" (\r -> r.req <> " large " <> show r.large)
        )
  where
    sized' r
      | r.number < 10 = inj "tiny" r.number
      | r.number < 100 = inj "small" r.number
      | r.number < 1000 = inj "medium" r.number
      | otherwise = inj "large" r.number

-- | A choice that is made where the field is read: the classification of a status.
classified :: (Steps arr) => Log -> arr Int String
classified noted =
  newRecord ["req", "status"] "GET /"
    ^>> decide "how did it go?" (casesBy "status" classify)
    >>> ( "ok" ?> branch noted "ok" (\r -> r.req <> " ok " <> show r.ok)
            ||| "missing" ?> branch noted "missing" (\r -> r.req <> " missing " <> show r.missing)
            ||| "broken" ?> branch noted "broken" (\r -> r.req <> " broken " <> show r.broken)
        )
  where
    classify status
      | status < 400 = inj "ok" status
      | status < 500 = inj "missing" status
      | otherwise = inj "broken" status

-- | A choice that is done with a number or has failed with a message.
type Outcome = Either ("done" := Int) ("failed" := String)

-- | The case that a choice of two is in, and its value.
showOutcome :: (Show a, Show b) => Either (n := a) (m := b) -> String
showOutcome = either (\(Is a) -> "done " <> show a) (\(Is b) -> "failed " <> show b)

-- | A record that has a choice in one of its fields.
withOutcome :: (Steps arr) => Outcome -> arr () ("outcome" := Outcome, "kept" := Bool)
withOutcome outcome = is "outcome" (arr (const outcome)) &&& is "kept" (arr (const True))

-- | A step that notes its name, and gives the text that the function makes of its input.
branch :: (Effect arr) => Log -> String -> (a -> String) -> arr a String
branch noted entry text = now (\input -> atomicModifyIORef' noted (\entries -> (entry : entries, ())) >> pure (text input))

-- | A step that notes its name and its input, and gives nothing.
noting' :: (Effect arr, Show a) => Log -> String -> arr a ()
noting' noted entry = now (\input -> atomicModifyIORef' noted (\entries -> ((entry <> " " <> show input) : entries, ())))
