{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module CursorsSpec (spec) where

import Control.Arrow (ArrowChoice, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (SomeException, evaluate, fromException, try)
import Control.Monad (forM_, zipWithM)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Builder (int16BE, int32BE, toLazyByteString)
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as BL
import Data.Char (ord)
import Data.Either (isRight)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.Int (Int32, Int64)
import Data.List (isPrefixOf)
import Data.Word (Word8)
import Database.PostgreSQL.Internal.Protocol.Codec.Cursor
import Database.PostgreSQL.Internal.Protocol.Codec.CursorRows
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Empty, Payload (..))
import Foreign.Storable (peekByteOff)
import Network.Directions.Classes
import Network.Directions.Compiled
import Network.Directions.Delimited
import Network.Directions.Memory
import Network.Directions.Reader
import Network.Directions.Rows (Counter, adding, counted, newCounter, rowsChecksum, rowsWire, streamWire)
import Network.Directions.Threads
import Skeletest

-- | The messages that some of the tests read: a ParseComplete, rows, and a
-- CommandComplete.
type Mixed =
  'PayloadChoice
    '[ FramedMessage '1' Empty,
       FramedMessage 'D' ('PayloadCounted16 'MaybeBytes32Atom),
       FramedMessage 'C' 'CStringAtom
     ]

spec :: Spec
spec = describe "cursors derived from the schema" $ do
  describe "the rows of a result" $ do
    it "add up to the checksum that the wire was built with, with every arrow, whatever a receive gives" $
      forM_ [maxBound, 1460, 97, 7, 1] $ \limit ->
        forM_ [16384, 64, 12] $ \capacity ->
          runAll foldRowsCursors rowsOf1000 limit capacity `shouldReturn` replicate 3 (Right (rowsChecksum 1000))

    it "are the same when the buffer holds less than a row" $
      forM_ [maxBound, 7] $ \limit ->
        runAll foldRowsCursors (rowsWire 3) limit 16 `shouldReturn` replicate 3 (Right (rowsChecksum 3))

  describe "a value larger than the buffer" $ do
    it "is read as pieces no larger than the buffer, which reassemble to the value" $
      forM_ [maxBound, 1460, 100, 7] $ \limit -> do
        results <- runPieces (streamWire field) limit 4096
        forM_ results $ \pieces -> do
          BS.concat pieces `shouldBe` field
          all ((<= 4096) . BS.length) pieces `shouldBe` True
          (length pieces > 24) `shouldBe` True

    it "is skipped, and what follows is read, however a receive gives" $
      forM_ [maxBound, 1460, 100, 7, 1] $ \limit ->
        runWith tagAfterSkipping (streamWire field) limit 64 `shouldReturn` replicate 3 (Right "SELECT 1")

    it "cannot be decoded in place, which is an error that says so" $ do
      forM_ [maxBound, 1] $ \limit -> do
        failed <- runWith (firstValue @ByteSum) (streamWire (BS.replicate 100 1)) limit 64
        all isCursorError failed `shouldBe` True
        length failed `shouldBe` 3
        fits <- runWith (firstValue @ByteSum) (streamWire (BS.replicate 100 1)) limit 128
        fits `shouldBe` replicate 3 (Right (ByteSum 100))

  describe "decoding in place" $ do
    it "gives the numbers and the bool that the row was built with, and the length of a text" $
      forM_ [maxBound, 1460, 7, 1] $ \limit -> do
        row <- runWith firstRow (rowsWire 5) limit 64
        row `shouldBe` replicate 3 (Right (0, (1, (16, (40, False)))))
        row' <- runWith firstRow (rowsWire 5) limit 16
        row' `shouldBe` row

    it "fails for a value whose size the type doesn't have" $
      forM_ [maxBound, 1] $ \limit -> do
        failed <- runWith (firstValue @Int32) (onlyRow [Just "abcde"]) limit 64
        all isCursorError failed `shouldBe` True
        right <- runWith (firstValue @Int32) (onlyRow [Just (BS.pack [0, 0, 1, 2])]) limit 64
        right `shouldBe` replicate 3 (Right 258)

    it "fails for a NULL where a value is read, and reads the NULLs that are there" $
      forM_ [maxBound, 1] $ \limit -> do
        failed <- runWith (firstValue @Int32) (onlyRow [Nothing]) limit 64
        all isCursorError failed `shouldBe` True
        sizes <- runIO allValues (onlyRow [Nothing, Just "xyz", Nothing, Just ""]) limit 64
        sizes `shouldBe` replicate 3 (Right [Nothing, Just 3, Nothing, Just 0])

  describe "what is not read" $ do
    it "is skipped: the values of a row that are not pulled, and the bytes of a value that are not taken" $
      forM_ [maxBound, 1460, 97, 7, 1] $ \limit ->
        forM_ [16384, 64] $ \capacity -> do
          runAll firstColumns rowsOf1000 limit capacity `shouldReturn` replicate 3 (Right (sum [0 .. 999]))
          runAll mixedColumns rowsOf1000 limit capacity `shouldReturn` replicate 3 (Right (sum [i + i `mod` 2 | i <- [0 .. 999]]))

    it "is skipped: the whole rows, and then the CommandComplete is read" $
      forM_ [maxBound, 1460, 7, 1] $ \limit -> do
        runWith tagAfterSkipping (rowsWire 1000) limit 64 `shouldReturn` replicate 3 (Right "SELECT 1000")
        runWith tagAfterSkipping (rowsWire 1000) limit 16384 `shouldReturn` replicate 3 (Right "SELECT 1000")

  describe "elements of a fixed size" $
    it "are read as numbers, and are skipped when they are not read, whatever a receive gives" $
      forM_ [maxBound, 1460, 7, 1] $ \limit ->
        forM_ [5, 9, 64] $ \capacity ->
          runIO fixedElements (frameOf 'k' (be16 3 <> be32 7 <> be32 8 <> be32 9)) limit capacity `shouldReturn` replicate 3 (Right [8, -1])

  describe "CommandComplete's tag" $ do
    it "is a view of the message, without its terminator, and what is after the terminator is skipped" $
      forM_ [maxBound, 1460, 7, 1] $ \limit -> do
        runIO (messagesRead 2) (completeOf "SELECT 7" <> frameOf '1' BS.empty) limit 64
          `shouldReturn` replicate 3 (Right ["command complete SELECT 7", "parse complete"])
        runIO (messagesRead 2) (frameOf 'C' "SELECT 8\0and the rest" <> frameOf '1' BS.empty) limit 64
          `shouldReturn` replicate 3 (Right ["command complete SELECT 8", "parse complete"])

    it "is an error when it doesn't fit the buffer, or has no terminator" $
      forM_ [maxBound, 1] $ \limit -> do
        tooLarge <- runIO (messagesRead 1) (completeOf (BS.replicate 200 65)) limit 64
        all isCursorError tooLarge `shouldBe` True
        unterminated <- runIO (messagesRead 1) (frameOf 'C' "SELECT") limit 64
        all isCursorError unterminated `shouldBe` True

  describe "the last message of the stream, with nothing after it" $
    it "is read without waiting for the bytes that would follow it, whatever a receive gives" $
      forM_ [maxBound, 1460, 7, 1] $ \limit ->
        forM_ [5, 9, 16, 64] $ \capacity -> do
          runIO (messagesRead 1) (frameOf '1' BS.empty) limit capacity `shouldReturn` replicate 3 (Right ["parse complete"])
          runIO (messagesRead 3) (frameOf '1' BS.empty <> frameOf '1' BS.empty <> frameOf '1' BS.empty) limit capacity `shouldReturn` replicate 3 (Right (replicate 3 "parse complete"))
          runIO (messagesRead 1) (completeOf "SELECT 0") limit (max 9 capacity) `shouldReturn` replicate 3 (Right ["command complete SELECT 0"])
          runIO (messagesRead 3) (frameOf '1' BS.empty <> dataRowOf [Just "a"] <> completeOf "SELECT 1") limit (max 9 capacity)
            `shouldReturn` replicate 3 (Right ["parse complete", "data row", "command complete SELECT 1"])

  describe "a stream that is not what its frames say" $ do
    it "is an error when a message has a tag that is not one of the alternatives" $
      forM_ [maxBound, 1] $ \limit -> do
        failed <- runIO (messagesRead 1) (frameOf 'X' "") limit 64
        all isCursorError failed `shouldBe` True

    it "is an error to read past the end of a body, or a value that reaches past it" $
      forM_ [maxBound, 1] $ \limit -> do
        pastEnd <- runIO allValues dataRowTwoValuesOneInside limit 64
        all isCursorError pastEnd `shouldBe` True
        longValue <- runIO allValues (frameOf 'D' (be16 1 <> be32 100 <> "abc")) limit 64
        all isCursorError longValue `shouldBe` True
        ok <- runIO allValues (onlyRow [Just "abc", Nothing]) limit 64
        ok `shouldBe` replicate 3 (Right [Just 3, Nothing])

    it "is an error when the stream ends inside a value" $
      forM_ [maxBound, 1] $ \limit -> do
        cut <- runIO allValues (BS.take 9 (onlyRow [Just "abcdef"])) limit 64
        any isRight cut `shouldBe` False

-- | A message whose body counts elements of four bytes.
type Fixeds = 'PayloadChoice '[FramedMessage 'k' ('PayloadCounted16 ('FixedBytesAtom 4))]

-- | A program over a reader of a memory link, for every arrow.
type Program a b = forall arr. (ArrowChoice arr, Effect arr) => Reader Memory -> IO (arr a b)

-- | Runs the program on the wire with each arrow, a reader of the capacity,
-- and a link that gives at most the limit at once: what each gave, or how it
-- failed.
runAll' :: forall a b. Program a b -> a -> ByteString -> Int -> Int -> IO [Either String b]
runAll' program input wire limit capacity =
  sequence
    [ over (`runThreads` input),
      over (\arrow -> fst <$> runCompiled arrow input),
      over (\arrow -> fst <$> runDelimited arrow input)
    ]
  where
    over :: (ArrowChoice arr, Effect arr) => (arr a b -> IO b) -> IO (Either String b)
    over run =
      outcome
        <$> try
          ( do
              memory <- newMemory wire limit
              reader <- newReader memory capacity
              program reader >>= run >>= evaluate
          )

-- | Runs a program that adds up into a counter, with each arrow.
runAll :: (forall arr. (ArrowChoice arr, Effect arr) => Reader Memory -> Counter -> arr () ()) -> ByteString -> Int -> Int -> IO [Either String Int]
runAll program wire limit capacity = do
  one <- newCounter
  two <- newCounter
  three <- newCounter
  let counters = [one, two, three]
  results <-
    sequence
      [ outcome <$> try (withReader wire limit capacity (\reader -> runThreads (program reader one) ())),
        outcome <$> try (withReader wire limit capacity (\reader -> fst <$> runCompiled (program reader two) ())),
        outcome <$> try (withReader wire limit capacity (\reader -> fst <$> runDelimited (program reader three) ()))
      ]
  zipWithM (\counter result -> either (pure . Left) (\() -> Right <$> counted counter) result) counters results

-- | How a run failed, as text: a cursor's error is told from the others.
outcome :: Either SomeException b -> Either String b
outcome = either (Left . told) Right
  where
    told failure = case fromException failure of
      Just (CursorError what) -> "cursor error: " <> what
      Nothing -> "other: " <> show failure

withReader :: ByteString -> Int -> Int -> (Reader Memory -> IO a) -> IO a
withReader wire limit capacity use = do
  memory <- newMemory wire limit
  reader <- newReader memory capacity
  use reader

-- | A program that sets itself up in IO.
runIO :: forall b. Program () b -> ByteString -> Int -> Int -> IO [Either String b]
runIO program = runAll' program ()

-- | A program that needs no input.
runWith :: forall b. (forall arr. (ArrowChoice arr, Effect arr) => Reader Memory -> arr () b) -> ByteString -> Int -> Int -> IO [Either String b]
runWith program = runAll' (pure . program) ()

isCursorError :: Either String b -> Bool
isCursorError = either (isPrefixOf "cursor error: ") (const False)

-- | The pieces of the stream's values, copied, with each arrow.
runPieces :: ByteString -> Int -> Int -> IO [[ByteString]]
runPieces wire limit capacity = do
  results <- runAll' collected () wire limit capacity
  either (failTest . show) pure (sequence results)

collected :: Program () [ByteString]
collected reader = do
  kept <- newIORef []
  pure (streamCursors reader (keeping kept) >>> now (\() -> reverse <$> readIORef kept))

keeping :: (Effect arr) => IORef [ByteString] -> arr ByteString ()
keeping kept = now (\piece' -> evaluate (BS.copy piece') >>= \owned -> modifyIORef' kept (owned :))

rowsOf1000 :: ByteString
rowsOf1000 = rowsWire 1000

field :: ByteString
field = BS.pack (take 100000 (cycle [97 .. 122]))

-- | A type decoded from all of the bytes of a value, whatever their number.
newtype ByteSum = ByteSum Int
  deriving (Eq, Show)

instance InPlace ByteSum where
  decoder = PeekWhole (\size pointer -> ByteSum . sum <$> mapM (\index -> fromIntegral <$> (peekByteOff pointer index :: IO Word8)) [0 .. size - 1])

be16, be32 :: Int -> ByteString
be16 = BL.toStrict . toLazyByteString . int16BE . fromIntegral
be32 = BL.toStrict . toLazyByteString . int32BE . fromIntegral

-- | A message: the tag, the length that counts itself, and the body.
frameOf :: Char -> ByteString -> ByteString
frameOf tag body = BS.concat [BS.singleton (fromIntegral (ord tag)), be32 (BS.length body + 4), body]

dataRowOf :: [Maybe ByteString] -> ByteString
dataRowOf values = frameOf 'D' (be16 (length values) <> mconcat (map value values))
  where
    value Nothing = be32 (-1)
    value (Just bytes') = be32 (BS.length bytes') <> bytes'

completeOf :: ByteString -> ByteString
completeOf tag = frameOf 'C' (tag <> "\0")

-- | The one DataRow of the values, and nothing after it.
onlyRow :: [Maybe ByteString] -> ByteString
onlyRow = dataRowOf

-- | A row that says it has two values, and has the bytes of one.
dataRowTwoValuesOneInside :: ByteString
dataRowTwoValuesOneInside = frameOf 'D' (be16 2 <> be32 4 <> BS.pack [0, 0, 0, 1])

-- | Of three elements of four bytes, skips the first, reads the second as a
-- number, skips the third, and finds that there is no fourth.
fixedElements :: forall arr. (ArrowChoice arr, Effect arr) => Reader Memory -> IO (arr () [Int])
fixedElements reader = do
  seen <- newIORef []
  let fixeds = messages reader :: At Fixeds Memory
      row body =
        count body
          >>> const ()
          ^>> element body
          >>> const ()
          ^>> element body
          >>> const ()
          ^>> fixed (child body)
          >>> now (\value -> modifyIORef' seen (value :))
          >>> const ()
          ^>> element body
          >>> const ()
          ^>> element body
          >>> now (\found -> modifyIORef' seen (maybe (-1) (const (-2)) found :))
          >>> now (\() -> reverse <$> readIORef seen)
  pure (message fixeds >>> (,()) ^>> cases fixeds (row :> NoCases))

-- | Reads this many messages, each as a description of what it was, and
-- reads nothing of the rows.
messagesRead :: forall arr. (ArrowChoice arr, Effect arr) => Int -> Reader Memory -> IO (arr () [String])
messagesRead total reader = do
  seen <- newIORef []
  let result = messages reader :: At Mixed Memory
      noting :: String -> IO ()
      noting description = modifyIORef' seen (description :)
      parsed _ = now (\() -> noting "parse complete")
      row _ = now (\() -> noting "data row")
      -- The tag is a view of the buffer, which is valid until the next pull.
      complete body = cstring body >>> now (\tag -> evaluate (BS.copy tag) >>= \owned -> noting ("command complete " <> BS8.unpack owned))
      step = message result >>> (,()) ^>> cases result (parsed :> row :> complete :> NoCases)
      go :: Int -> arr () [String]
      go 0 = now (\() -> reverse <$> readIORef seen)
      go left = step >>> go (left - 1)
  pure (go total)

-- | Skips whatever the stream has, until the CommandComplete, whose tag it
-- reads.
tagAfterSkipping :: forall arr link. (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr () String
tagAfterSkipping reader = loop
  where
    result = messages reader :: At ResultStream link
    loop = message result >>> (,()) ^>> cases result (row :> complete :> NoCases) >>> (loop ||| returnA)
    row _ = returnA >>^ const (Left ())
    complete body = cstring body >>> now (evaluate . BS.copy) >>^ Right . BS8.unpack
{-# INLINABLE tagAfterSkipping #-}

-- | The first value of the stream's first row, decoded in place.
firstValue :: forall a arr link. (InPlace a, ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr () a
firstValue reader = message result >>> (,()) ^>> cases result (row :> unexpected :> NoCases)
  where
    result = messages reader :: At ResultStream link
    row body = count body >>> const () ^>> column body
    unexpected _ = now (\() -> ioError (userError "the stream ended before a row"))
{-# INLINABLE firstValue #-}

-- | The values of the stream's first row: the int4, the int8, the lengths of
-- the two texts, and the bool.
firstRow :: forall arr link. (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> arr () (Int32, (Int64, (Int, (Int, Bool))))
firstRow reader = message result >>> (,()) ^>> cases result (row :> unexpected :> NoCases)
  where
    result = messages reader :: At ResultStream link
    unexpected _ = now (\() -> ioError (userError "the stream ended before a row"))
    row body =
      count body
        >>> const ()
        ^>> ( column body
                &&& ( column body
                        &&& ( (column body >>^ \(Length size) -> size)
                                &&& ((column body >>^ \(Length size) -> size) &&& column body)
                            )
                    )
            )
{-# INLINABLE firstRow #-}

-- | The sizes of all of the values of the stream's first row, with 'Nothing'
-- for a NULL, each value skipped after its size is read.
allValues :: forall arr. (ArrowChoice arr, Effect arr) => Reader Memory -> IO (arr () [Maybe Int])
allValues reader = do
  seen <- newIORef []
  let result = messages reader :: At ResultStream Memory
      unexpected _ = now (\() -> ioError (userError "the stream ended before a row"))
      row body = count body >>> const () ^>> elements body
      elements body = element body >>> (absent ^>> ((value (child body) >>> elements body) ||| now (\() -> reverse <$> readIORef seen)))
      value cursor =
        valueLength cursor
          >>> now (\size -> modifyIORef' seen ((if size < 0 then Nothing else Just size) :))
          >>> const ()
          ^>> skipValue cursor
  pure (message result >>> (,()) ^>> cases result (row :> unexpected :> NoCases))

-- | The first value of each row is added up; nothing else of the row is pulled.
firstColumns :: forall arr link. (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> Counter -> arr () ()
firstColumns reader total = loop
  where
    result = messages reader :: At ResultStream link
    loop = message result >>> (,()) ^>> cases result (row :> complete :> NoCases) >>> (loop ||| returnA)
    row body = count body >>> const () ^>> column body >>> (\value -> fromIntegral (value :: Int32)) ^>> adding total >>^ Left
    complete body = cstring body >>^ Right . const ()
{-# INLINABLE firstColumns #-}

-- | The first value, and the last value, are added up. The second value is not
-- pulled, the third is pulled by its length, and the fourth is read in a piece.
mixedColumns :: forall arr link. (ArrowChoice arr, Effect arr, Link arr link) => Reader link -> Counter -> arr () ()
mixedColumns reader total = loop
  where
    result = messages reader :: At ResultStream link
    loop = message result >>> (,()) ^>> cases result (row :> complete :> NoCases) >>> (loop ||| returnA)
    row body =
      count body
        >>> const ()
        ^>> column body
        >>> (\value -> fromIntegral (value :: Int32))
        ^>> adding total
        >>> skipped body
        >>> lengthOnly body
        >>> firstPiece body
        >>> column body
        >>> (\value -> fromEnum (value :: Bool))
        ^>> adding total
        >>^ Left
    complete body = cstring body >>^ Right . const ()
    skipped body = element body >>^ const ()
    lengthOnly body = element body >>> const () ^>> (valueLength (child body) >>^ const ())
    firstPiece body = element body >>> const () ^>> valueLength (child body) >>> const () ^>> (piece (child body) >>^ const ())
{-# INLINABLE mixedColumns #-}
