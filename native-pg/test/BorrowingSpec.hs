{-# LANGUAGE DataKinds #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module BorrowingSpec (spec) where

import Control.Exception (evaluate, try)
import Control.Monad (forM_)
import Control.Monad.Except (catchError)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Int (Int16, Int32)
import Database.PostgreSQL.Protocol.Codec.Payload (Payload (..))
import GHC.Conc (getAllocationCounter)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Database.PostgreSQL.Buffer
import Database.PostgreSQL.Protocol.Codec
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (Ptr, castPtr, minusPtr)
import Data.Word (Word8, Word16, Word32)
import Skeletest

spec :: Spec
spec = describe "caller-owned input" $ do
  it "decodes scalar boundaries through one-byte fills and compaction" $ do
    forM_ [1, 4] $ \fragment -> do
      storage <- newBuffer 4
      source <- bytesSource fragment scalarBytes (const (pure ()))
      (values, _) <- runInput source (reusableInputBuffer storage) scalarValues
      values `shouldBe` scalarExpected

  it "forces scalar values before their reusable storage is overwritten" $ do
    storage <- newBuffer 4
    source <- bytesSource 4 scalarBytes (const (pure ()))
    (values, _) <- runInput source (reusableInputBuffer storage) scalarValues
    -- Do not inspect any result until every original byte has been overwritten.
    withForeignPtr (bufferMemory storage) $ \pointer -> fillBytes pointer 85 4
    values `shouldBe` scalarExpected

  it "rejects bounded scalars before reading outside their frame" $ do
    forM_ [False, True] $ \prefilled -> forM_ scalarParsers $ \(width, parse) -> do
      storage <- newBuffer 4
      calls <- newIORef (0 :: Int)
      source <- bytesSource 4 "abcdefgh" (\_ -> modifyIORef' calls (+ 1))
      let empty = reusableInputBuffer storage
      input <- if prefilled
        then snd <$> runInput source empty (peekInputByteAt 0)
        else pure empty
      -- A full workspace must still respect a shorter enclosing frame.
      before <- readIORef calls
      result <- try @InputError (runInputUntil source maxBound input (withInputLimit (width - 1) parse))
      either show (const "accepted") result `shouldBe` "InputError \"unexpected end of a bounded part: needed " <> show width <> " bytes\""
      readIORef calls `shouldReturn` before

  it "rejects each truncated scalar without reading beyond the source" $ do
    forM_ scalarParsers $ \(width, parse) -> forM_ [0 .. width - 1] $ \available -> do
      storage <- newBuffer 4
      calls <- newIORef (0 :: Int)
      source <- bytesSource 1 (BS.replicate available 255) (\_ -> modifyIORef' calls (+ 1))
      result <- try @InputError (runInput source (reusableInputBuffer storage) parse)
      either show (const "accepted") result `shouldBe` "InputError \"unexpected end of input\""
      readIORef calls `shouldReturn` available + 1

  it "does not allocate an owned ByteString for every reusable scalar" $ do
    storage <- newBuffer 65536
    source <- bytesSource 65536 (BS.replicate 40000 1) (const (pure ()))
    -- Fill before measuring, so this isolates reads from an existing workspace.
    (_, filled) <- runInput source (reusableInputBuffer storage) (peekInputByteAt 0)
    before <- getAllocationCounter
    (total, _) <- runInput source filled (sumWords 10000 0)
    _ <- evaluate total
    after <- getAllocationCounter
    let allocated = before - after
    if allocated < 1600000
      then pure ()
      else failTest ("10000 buffered Word32 reads allocated " <> show allocated <> " bytes (limit 1600000)")

  it "compacts unread bytes in the same allocation" $ do
    storage <- newBuffer 4
    writes <- newIORef []
    source <- bytesSource 3 "abcdefghij" (\pointer -> withForeignPtr (bufferMemory storage) $ \base -> modifyIORef' writes (<> [pointer `minusPtr` base]))
    (values, _) <- runInput source (reusableInputBuffer storage) (sequence [readInputExactly 3, readInputExactly 3, readInputExactly 4])
    values `shouldBe` ["abc", "def", "ghij"]
    offsets <- readIORef writes
    all (\offset -> offset >= 0 && offset < 4) offsets `shouldBe` True

  it "rejects a contiguous request larger than storage before reading" $ do
    storage <- newBuffer 4
    readCalls <- newIORef (0 :: Int)
    source <- bytesSource 4 "abcde" (\_ -> modifyIORef' readCalls (+ 1))
    result <- inputResult (runInput source (reusableInputBuffer storage) (readInputExactly 5))
    result `shouldBe` Left "contiguous input of 5 bytes exceeds buffer capacity 4"
    readIORef readCalls `shouldReturn` 0

  it "keeps returned exact, chunk and C-string values stable across reuse" $ do
    storage <- newBuffer 4
    source <- bytesSource 4 "abc\0defghijk" (const (pure ()))
    (values, _) <- runInput source (reusableInputBuffer storage) $ do
      string <- readInputCStringBytes
      exact <- readInputExactly 4
      chunk <- readInputSome 4
      pure [string, exact, chunk]
    values `shouldBe` ["abc", "defg", "hijk"]

  it "borrows a chunk directly from the supplied storage" $ do
    storage <- newBuffer 4
    source <- bytesSource 4 "abcdefgh" (const (pure ()))
    chunks <- newIORef []
    let consume chunk = do
          offset <- BSU.unsafeUseAsCString chunk $ \pointer -> withForeignPtr (bufferMemory storage) (pure . (castPtr pointer `minusPtr`))
          copied <- evaluate (BS.copy chunk)
          modifyIORef' chunks (<> [(offset, copied)])
    _ <- runInput source (reusableInputBuffer storage) (withInputSome 8 consume *> withInputSome 8 consume)
    readIORef chunks `shouldReturn` [(0, "abcd"), (0, "efgh")]

  it "does not enter rollback handlers after reusable storage was refilled" $ do
    storage <- newBuffer 4
    calls <- newIORef (0 :: Int)
    source <- bytesSource 4 "abcdefgh" (\_ -> modifyIORef' calls (+ 1))
    let parser = catchError
          (readInputExactly 4 *> readInputExactly 4 *> readInputExactly 5)
          (const (pure "rollback handler entered"))
    inputResult (runInput source (reusableInputBuffer storage) parser)
      `shouldReturn` Left "contiguous input of 5 bytes exceeds buffer capacity 4"
    readIORef calls `shouldReturn` 2

  it "preserves rollback handlers for allocating input" $ do
    source <- bytesSource 4 "abcdefgh" (const (pure ()))
    let parser = catchError
          (readInputExactly 4 *> readInputExactly 4 *> readInputExactly 5)
          (const (pure "recovered"))
    inputResult (runInput source emptyInputBuffer parser) `shouldReturn` Right "recovered"

  it "assembles long C-string metadata into owned storage" $ do
    storage <- newBuffer 4
    let name = BS.replicate 100 120
    source <- bytesSource 4 (name <> "\0next") (const (pure ()))
    (values, _) <- runInputFramedUntil source maxBound (reusableInputBuffer storage) $ do
      name' <- withInputLimit 101 readInputCStringBytes
      next <- readInputExactly 4
      pure [name', next]
    values `shouldBe` [name, "next"]

  it "assembles bounded authentication metadata across supplied refills" $ do
    storage <- newBuffer 4
    let challenge = BS.replicate 100 99
    source <- bytesSource 4 (challenge <> "next") (const (pure ()))
    (values, _) <- runInputFramedUntil source maxBound (reusableInputBuffer storage) $ do
      value <- withInputLimit 100 readInputRemaining
      next <- readInputExactly 4
      pure [value, next]
    values `shouldBe` [challenge, "next"]

inputResult :: IO (ByteString, InputBuffer) -> IO (Either String ByteString)
inputResult action = either (\(InputError reason) -> Left reason) (Right . fst) <$> try action

bytesSource :: Int -> ByteString -> (Ptr Word8 -> IO ()) -> IO InputSource
bytesSource fragment bytes observe = do
  offset <- newIORef 0
  pure $ InputSource
    (\pointer capacity -> do
      observe pointer
      consumed <- readIORef offset
      let chunk = BS.take (min capacity fragment) (BS.drop consumed bytes)
      BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
      modifyIORef' offset (+ BS.length chunk)
      pure (BS.length chunk))
    4

-- Alternating widths leave unread bytes at the end of the four-byte workspace,
-- forcing compaction as well as exact-boundary refills.
scalarBytes :: ByteString
scalarBytes = BS.pack [99, 0, 255, 0, 0, 255, 255, 127, 255, 128, 0, 0, 0, 0, 0, 255, 255, 255, 255, 127, 255, 255, 255, 128, 0, 0, 0]

scalarValues :: Input [Integer]
scalarValues = skipInput 1 *> sequence
  [ toInteger <$> parser @Word8 @('FixedBytesAtom 1),
    toInteger <$> parser @Word8 @('FixedBytesAtom 1),
    toInteger <$> parser @Word16 @('FixedBytesAtom 2),
    toInteger <$> parser @Word16 @('FixedBytesAtom 2),
    toInteger <$> parser @Int16 @('FixedBytesAtom 2),
    toInteger <$> parser @Int16 @('FixedBytesAtom 2),
    toInteger <$> parser @Word32 @('FixedBytesAtom 4),
    toInteger <$> parser @Word32 @('FixedBytesAtom 4),
    toInteger <$> parser @Int32 @('FixedBytesAtom 4),
    toInteger <$> parser @Int32 @('FixedBytesAtom 4)
  ]

scalarExpected :: [Integer]
scalarExpected = [0, 255, 0, 65535, 32767, -32768, 0, 4294967295, 2147483647, -2147483648]

scalarParsers :: [(Int, Input Integer)]
scalarParsers =
  [ (1, toInteger <$> parser @Word8 @('FixedBytesAtom 1)),
    (2, toInteger <$> parser @Word16 @('FixedBytesAtom 2)),
    (2, toInteger <$> parser @Int16 @('FixedBytesAtom 2)),
    (4, toInteger <$> parser @Word32 @('FixedBytesAtom 4)),
    (4, toInteger <$> parser @Int32 @('FixedBytesAtom 4))
  ]

sumWords :: Int -> Word32 -> Input Word32
sumWords 0 !total = pure total
sumWords count !total = do
  value <- parser @Word32 @('FixedBytesAtom 4)
  sumWords (count - 1) (total + value)
