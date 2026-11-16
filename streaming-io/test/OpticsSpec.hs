{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | Prisms, traversals and folds: a prism runs an arrow on one case of a sum and passes the others through without running
-- it, and 'each', 'traversal' and 'folding' run it on every focus, in order, as a loop, in each arrow that runs programs.
-- The same programs, written once for any arrow that has what a step that can't wait has, are run by each. That 'undoing'
-- takes them only of steps that can't wait is checked by @test/check-steps.sh@.
module OpticsSpec (spec) where

import Control.Arrow (ArrowChoice, arr, (&&&), (>>>), (>>^), (^>>))
import Control.Exception (Exception, SomeException, fromException, throwIO)
import Control.Monad (forM_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Monoid (Sum (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (each, folding, prism, traversal, _1, _2, _Just, _Left, _Right)
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
spec = describe "optics" $ forM_ runners $ \(Runner name run) -> describe name $ do
  describe "prisms" $ do
    it "_Left runs the arrow on a Left, and passes a Right through without running it" $ do
      noted <- newLog
      run (const (Left 3 :: Either Int Bool) ^>> _Left (seeing noted)) `shouldReturn` Left 30
      readLog noted `shouldReturn` ["saw 3"]
      passed <- newLog
      run (const (Right True :: Either Int Bool) ^>> _Left (seeing passed)) `shouldReturn` Right True
      readLog passed `shouldReturn` []

    it "_Right runs the arrow on a Right, and passes a Left through without running it" $ do
      noted <- newLog
      run (const (Right 3 :: Either Bool Int) ^>> _Right (seeing noted)) `shouldReturn` Right 30
      readLog noted `shouldReturn` ["saw 3"]
      passed <- newLog
      run (const (Left True :: Either Bool Int) ^>> _Right (seeing passed)) `shouldReturn` Left True
      readLog passed `shouldReturn` []

    it "_Just runs the arrow on the value of a Just, and passes Nothing through without running it" $ do
      noted <- newLog
      run (const (Just 3 :: Maybe Int) ^>> _Just (seeing noted)) `shouldReturn` Just 30
      readLog noted `shouldReturn` ["saw 3"]
      passed <- newLog
      run (const (Nothing :: Maybe Int) ^>> _Just (seeing passed)) `shouldReturn` Nothing
      readLog passed `shouldReturn` []

    it "_Left, _Right and _Just run an arrow that gives another type, and what passes through keeps its type" $ do
      run (const (Left 3 :: Either Int Bool) ^>> _Left (arr show)) `shouldReturn` (Left "3" :: Either String Bool)
      run (const (Right True :: Either Int Bool) ^>> _Left (arr show)) `shouldReturn` (Right True :: Either String Bool)
      run (const (Right 3 :: Either Bool Int) ^>> _Right (arr show)) `shouldReturn` (Right "3" :: Either Bool String)
      run (const (Left True :: Either Bool Int) ^>> _Right (arr show)) `shouldReturn` (Left True :: Either Bool String)
      run (const (Just 3 :: Maybe Int) ^>> _Just (arr show)) `shouldReturn` Just "3"
      run (const (Nothing :: Maybe Int) ^>> _Just (arr show)) `shouldReturn` (Nothing :: Maybe String)

    it "a prism that the test makes runs the arrow on the case that matches, and passes the others through" $ do
      noted <- newLog
      run (const 4 ^>> halved (seeing noted)) `shouldReturn` "even, then 40"
      readLog noted `shouldReturn` ["saw 4"]
      passed <- newLog
      run (const 3 ^>> halved (seeing passed)) `shouldReturn` "odd 3"
      readLog passed `shouldReturn` []

    it "composes with the lens _1: _1 . _Left runs the arrow on the Left of the first of a pair" $ do
      noted <- newLog
      run (const (Left 3 :: Either Int Bool, "kept") ^>> (_1 . _Left) (seeing noted)) `shouldReturn` (Left 30, "kept")
      readLog noted `shouldReturn` ["saw 3"]
      passed <- newLog
      run (const (Right True :: Either Int Bool, "kept") ^>> (_1 . _Left) (seeing passed)) `shouldReturn` (Right True, "kept")
      readLog passed `shouldReturn` []

    it "composes with at: at \"x\" . _Just runs the arrow on the value of a field that is a Just" $ do
      noted <- newLog
      run (maybeField (Just 3) >>> (at "x" . _Just) (seeing noted) >>^ \r -> (r.x, r.kept)) `shouldReturn` (Just 30, True)
      readLog noted `shouldReturn` ["saw 3"]
      passed <- newLog
      run (maybeField Nothing >>> (at "x" . _Just) (seeing passed) >>^ \r -> (r.x, r.kept)) `shouldReturn` (Nothing, True)
      readLog passed `shouldReturn` []

    it "composes with the lens _2: _Just . _2 runs the arrow on the second of a pair that is in a Just" $ do
      noted <- newLog
      run (const (Just (1 :: Int, 2)) ^>> (_Just . _2) (seeing noted)) `shouldReturn` Just (1, 20)
      readLog noted `shouldReturn` ["saw 2"]
      passed <- newLog
      run (const (Nothing :: Maybe (Int, Int)) ^>> (_Just . _2) (seeing passed)) `shouldReturn` Nothing
      readLog passed `shouldReturn` []

  describe "each" $ do
    it "gives an empty list for an empty list, and doesn't run the arrow" $ do
      noted <- newLog
      run (const ([] :: [Int]) ^>> each (seeing noted)) `shouldReturn` []
      readLog noted `shouldReturn` []

    it "runs the arrow on the one element of a list of one" $ do
      noted <- newLog
      run (const [7] ^>> each (seeing noted)) `shouldReturn` [70]
      readLog noted `shouldReturn` ["saw 7"]

    it "gives the results in the order of the elements, and runs the arrow on them in that order" $ do
      noted <- newLog
      run (const [1, 2, 3, 4, 5] ^>> each (seeing noted)) `shouldReturn` [10, 20, 30, 40, 50]
      readLog noted `shouldReturn` ["saw 1", "saw 2", "saw 3", "saw 4", "saw 5"]

    it "gives results of another type than its elements, in order" $
      run (const [1, 2, 3 :: Int] ^>> each (arr show)) `shouldReturn` ["1", "2", "3"]

    it "runs the arrow on 10,000 elements, once each" $ do
      counted <- newCount
      run (const [1 .. 10000] ^>> each (counting counted)) `shouldReturn` [2 .. 10001]
      readIORef counted `shouldReturn` 10000

  describe "traversal" $ do
    it "with traverse runs the arrow on the value of a Maybe, and puts the result in its place" $ do
      noted <- newLog
      run (const (Just 4 :: Maybe Int) ^>> traversal traverse (seeing noted)) `shouldReturn` Just 40
      readLog noted `shouldReturn` ["saw 4"]
      passed <- newLog
      run (const (Nothing :: Maybe Int) ^>> traversal traverse (seeing passed)) `shouldReturn` Nothing
      readLog passed `shouldReturn` []

    it "with traverse runs the arrow on each element of a list, in order, and puts the results in their places" $ do
      noted <- newLog
      run (const [1, 2, 3] ^>> traversal traverse (seeing noted)) `shouldReturn` [10, 20, 30]
      readLog noted `shouldReturn` ["saw 1", "saw 2", "saw 3"]
      empty <- newLog
      run (const [] ^>> traversal traverse (seeing empty)) `shouldReturn` []
      readLog empty `shouldReturn` []

    it "runs the arrow on both parts of a pair, with a traversal that the test writes" $ do
      noted <- newLog
      run (const (5, 6) ^>> traversal both (seeing noted)) `shouldReturn` (50, 60)
      readLog noted `shouldReturn` ["saw 5", "saw 6"]

    it "runs the arrow on the foci of a traversal that is made of two, in the order that it visits them" $ do
      noted <- newLog
      run (const [(7, 8), (9, 10)] ^>> traversal (traverse . both) (seeing noted)) `shouldReturn` [(70, 80), (90, 100)]
      readLog noted `shouldReturn` ["saw 7", "saw 8", "saw 9", "saw 10"]

    it "gives foci of another type: in a list, in a Maybe, and in a pair that the test's traversal visits" $ do
      run (const [1, 2, 3 :: Int] ^>> traversal traverse (arr show)) `shouldReturn` ["1", "2", "3"]
      run (const (Just 4 :: Maybe Int) ^>> traversal traverse (arr show)) `shouldReturn` Just "4"
      run (const (5 :: Int, 6 :: Int) ^>> traversal both (arr show)) `shouldReturn` ("5", "6")

    it "composes with at: at \"rows\" . traversal traverse runs the arrow on each element of a field" $ do
      noted <- newLog
      run (rows >>> (at "rows" . traversal traverse) (seeing noted) >>^ \r -> (r.rows, r.name)) `shouldReturn` ([10, 20, 30], "pipe")
      readLog noted `shouldReturn` ["saw 1", "saw 2", "saw 3"]

    it "runs the arrow on 10,000 foci of a list, and on 10,000 foci of 5,000 pairs, once each" $ do
      counted <- newCount
      run (const [1 .. 10000] ^>> traversal traverse (counting counted)) `shouldReturn` [2 .. 10001]
      readIORef counted `shouldReturn` 10000
      -- The last result is read first, which a lazy putting back would have to follow a chain of thunks to.
      run (const [1 .. 10000 :: Int] ^>> traversal traverse (arr (+ 1)) >>^ last) `shouldReturn` 10001
      paired <- newCount
      run (const (pairs 5000) ^>> traversal (traverse . both) (counting paired)) `shouldReturn` fmap (\(a, b) -> (a + 1, b + 1)) (pairs 5000)
      readIORef paired `shouldReturn` 10000

  describe "folding" $ do
    it "runs the arrow on each part, in order, and combines the results with the monoid" $ do
      noted <- newLog
      run (const [1, 2, 3] ^>> folding id (seeing noted >>^ Sum)) `shouldReturn` Sum 60
      readLog noted `shouldReturn` ["saw 1", "saw 2", "saw 3"]
      shown <- newLog
      run (const [1, 2, 3] ^>> folding id (seeing shown >>^ show)) `shouldReturn` "102030"

    it "gives the empty monoid for a structure that has no part, and doesn't run the arrow" $ do
      noted <- newLog
      run (const ([] :: [Int]) ^>> folding id (seeing noted >>^ Sum)) `shouldReturn` Sum 0
      run (const (Nothing :: Maybe Int) ^>> folding id (seeing noted >>^ show)) `shouldReturn` ""
      readLog noted `shouldReturn` []

    it "folds the parts that a function gives, and 10,000 of them" $ do
      noted <- newLog
      run (const (1, 2) ^>> folding (\(a, b) -> [a, b]) (seeing noted >>^ Sum)) `shouldReturn` Sum 30
      readLog noted `shouldReturn` ["saw 1", "saw 2"]
      counted <- newCount
      run (const [1 .. 10000] ^>> folding id (counting counted >>^ Sum)) `shouldReturn` Sum 50015000
      readIORef counted `shouldReturn` 10000

  describe "a failure of the arrow" $ do
    it "on the third element reaches the caller from each, and the fourth element doesn't run it" $ do
      noted <- newLog
      result <- run (tryA (const [1 .. 5] ^>> each (failingOn 3 noted)))
      failureOf result `shouldBe` Just (Boom 3)
      readLog noted `shouldReturn` ["saw 1", "saw 2", "saw 3"]

    it "on the third focus reaches the caller from traversal, and the fourth focus doesn't run it" $ do
      noted <- newLog
      result <- run (tryA (const [1 .. 5] ^>> traversal traverse (failingOn 3 noted)))
      failureOf result `shouldBe` Just (Boom 3)
      readLog noted `shouldReturn` ["saw 1", "saw 2", "saw 3"]

    it "on the third part reaches the caller from folding, and the fourth part doesn't run it" $ do
      noted <- newLog
      result <- run (tryA (const [1 .. 5] ^>> folding id (failingOn 3 noted >>^ Sum)))
      failureOf result `shouldBe` Just (Boom 3)
      readLog noted `shouldReturn` ["saw 1", "saw 2", "saw 3"]

-- | A prism of the even numbers: the arrow runs on one, and the result is described; an odd number is the description
-- already.
halved :: (ArrowChoice arr) => arr Int Int -> arr Int String
halved = prism (\n -> "even, then " <> show n) (\n -> if even n then Right n else Left ("odd " <> show n))

-- | The two parts of a pair of one type, as a traversal.
both :: (Applicative f) => (a -> f b) -> (a, a) -> f (b, b)
both f (x, y) = (,) <$> f x <*> f y

-- | A list of pairs, which has this many.
pairs :: Int -> [(Int, Int)]
pairs count = [(2 * n - 1, 2 * n) | n <- [1 .. count]]

-- | A record with a field that is a 'Maybe'.
maybeField :: (Steps arr) => Maybe Int -> arr () ("x" := Maybe Int, "kept" := Bool)
maybeField value = is "x" (arr (const value)) &&& is "kept" (arr (const True))

-- | A record with a field that is a list.
rows :: (Steps arr) => arr () ("rows" := [Int], "name" := String)
rows = is "rows" (arr (const [1, 2, 3])) &&& is "name" (arr (const "pipe"))

-- | A failure of a step, which is told apart from another by its number.
newtype Boom = Boom Int
  deriving (Eq, Show)

instance Exception Boom

-- | The failure of the result, if it is a 'Boom'.
failureOf :: Either SomeException a -> Maybe Boom
failureOf = either fromException (const Nothing)

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

-- | Adds to the log.
note :: Log -> String -> IO ()
note noted entry = atomicModifyIORef' noted (\entries -> (entry : entries, ()))

-- | A step that notes its input, and gives it times ten.
seeing :: (Effect arr) => Log -> arr Int Int
seeing noted = now (\input -> note noted ("saw " <> show input) *> pure (input * 10))

-- | A step that notes its input, and fails with it if it is the one that the number says, and else gives it times ten.
failingOn :: (Effect arr) => Int -> Log -> arr Int Int
failingOn bad noted = now $ \input -> do
  note noted ("saw " <> show input)
  if input == bad then throwIO (Boom input) else pure (input * 10)

-- | How many times a step ran.
newCount :: IO (IORef Int)
newCount = newIORef 0

-- | A step that counts its runs, and gives its input plus one.
counting :: (Effect arr) => IORef Int -> arr Int Int
counting counted = now (\input -> atomicModifyIORef' counted (\count -> (count + 1, ())) *> pure (input + 1))
