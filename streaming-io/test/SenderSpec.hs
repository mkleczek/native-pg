{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module SenderSpec (spec) where

import Control.Monad (forM_, void)
import qualified Data.ByteString as BS
import Data.IORef (atomicModifyIORef', newIORef)
import FakeStreams
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (plusPtr)
import GHC.Conc (listThreads)
import Network.Streaming.Buffer (bufferMemory, newBuffer)
import Network.Streaming.Link (Interest (..), Link (..), StreamLengthError (..))
import qualified Network.Streaming.Output as Output
import Network.Streaming.PositiveInt (PositiveInt, positiveInt)
import Network.Streaming.Sender
import Network.Streaming.Source (Pulled (..))
import Network.Streaming.Watch (Watch, awaitWatch)
import Skeletest
import qualified Skeletest.Predicate as P
import System.Timeout (timeout)

spec :: Spec
spec = describe "the gathering sender" $ do
  describe "a source after framing" $ do
    forM_ [("without a buffer", Nothing), ("in a supplied buffer", Just (positiveInt @16))] $ \(label, size) ->
      it ("sends the framing and a ready chunk in one call, " <> label) $ do
        link <- newFakeLink []
        source <- newFakeSource [Pulled "hello", Ended]
        left <- unsentIn size (framing <> sourceOutput 5 source <> trailer)
        (rest, blocked) <- sendSome link left
        name blocked `shouldBe` "done"
        unsentEmpty rest `shouldBe` True
        -- The trailer follows the source, which isn't pulled again before
        -- the link has taken its chunk, so it can't join the chunk's send.
        linkOffers link `shouldReturn` [[framingBytes, "hello"], [trailerBytes]]
        pullCount source `shouldReturn` 2

    it "sends the framing at once when the first pull is pending, then the chunk once the watch fires" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      left <- unsent Nothing (framing <> sourceOutput 5 source)
      (rest, blocked) <- sendSome link left
      linkOffers link `shouldReturn` [[framingBytes]]
      pullCount source `shouldReturn` 1
      -- The caller registers the watch, and sendSome registered none.
      watchCounts source `shouldReturn` (0, 0)
      let watch = onSource blocked
      timeout 50000 (awaitWatch watch) `shouldReturn` Nothing
      watchCounts source `shouldReturn` (1, 1)
      openSource source
      timeout 2000000 (awaitWatch watch) `shouldReturn` Just ()
      watchCounts source `shouldReturn` (2, 2)
      (final, finished) <- sendSome link rest
      name finished `shouldBe` "done"
      unsentEmpty final `shouldBe` True
      linkOffers link `shouldReturn` [[framingBytes], ["hello"]]

    it "sends nothing more while the source has nothing" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      left <- unsent Nothing (sourceOutput 5 source)
      (rest, blocked) <- sendSome link left
      name blocked `shouldBe` "source"
      (again, blockedAgain) <- sendSome link rest
      name blockedAgain `shouldBe` "source"
      unsentEmpty again `shouldBe` False
      linkOffers link `shouldReturn` []
      pullCount source `shouldReturn` 2

    it "waits for the link, not the source, when the link took only part of the framing" $ do
      link <- newFakeLink [2]
      source <- newClosedSource [Pulled "hello", Ended]
      left <- unsent Nothing (framing <> sourceOutput 5 source)
      (rest, blocked) <- sendSome link left
      name blocked `shouldBe` "writable"
      linkReceived link `shouldReturn` BS.take 2 framingBytes
      openSource source
      sendAll link rest
      linkReceived link `shouldReturn` framingBytes <> "hello"

    it "gathers a chunk that's ready behind the pieces before it" $ do
      link <- newFakeLink []
      source <- newFakeSource [Pulled "hello", Ended]
      let output = Output.bytes (BS.replicate 9000 65) <> framing <> sourceOutput 5 source
      sendAll link =<< unsent Nothing output
      map (map BS.length) <$> linkOffers link `shouldReturn` [[9000, 4, 5]]

    it "skips an empty chunk" $ do
      link <- newFakeLink []
      source <- newFakeSource [Pulled "", Pulled "ab", Ended]
      sendAll link =<< unsent Nothing (sourceOutput 2 source)
      linkOffers link `shouldReturn` [["ab"]]
      pullCount source `shouldReturn` 3

    it "sends one chunk per source in each call" $ do
      link <- newFakeLink []
      first <- newFakeSource [Pulled "aa", Ended]
      second <- newFakeSource [Pulled "bb", Ended]
      sendAll link =<< unsent Nothing (sourceOutput 2 first <> sourceOutput 2 second)
      linkOffers link `shouldReturn` [["aa"], ["bb"]]

    it "ends a source that declares no bytes" $ do
      link <- newFakeLink []
      source <- newFakeSource []
      (rest, blocked) <- sendSome link =<< unsent Nothing (framing <> sourceOutput 0 source <> trailer)
      name blocked `shouldBe` "done"
      unsentEmpty rest `shouldBe` True
      linkOffers link `shouldReturn` [[framingBytes, trailerBytes]]

  describe "a partial send of a held chunk" $ do
    it "keeps its tail, and pulls again only once the tail is taken" $ do
      -- The first send takes the framing and 2 bytes of the chunk.
      link <- newFakeLink [6, 0, 5]
      source <- newFakeSource [Pulled "0123456789", Pulled "abc", Ended]
      left <- unsent Nothing (framing <> sourceOutput 13 source)
      (first, firstBlocked) <- sendSome link left
      name firstBlocked `shouldBe` "writable"
      pullCount source `shouldReturn` 1
      (second, secondBlocked) <- sendSome link first
      name secondBlocked `shouldBe` "writable"
      pullCount source `shouldReturn` 1
      (third, thirdBlocked) <- sendSome link second
      name thirdBlocked `shouldBe` "writable"
      pullCount source `shouldReturn` 1
      (fourth, fourthBlocked) <- sendSome link third
      name fourthBlocked `shouldBe` "done"
      unsentEmpty fourth `shouldBe` True
      -- The chunk, its tail offered again after nothing was taken, the rest
      -- of the tail, and then the next chunk, pulled after the tail went.
      linkOffers link
        `shouldReturn` [ [framingBytes, "0123456789"],
                         ["23456789"],
                         ["23456789"],
                         ["789"],
                         ["abc"]
                       ]
      pullCount source `shouldReturn` 3
      linkReceived link `shouldReturn` framingBytes <> "0123456789abc"

    it "arrives intact through a link that takes a few bytes at a time" $
      forM_ [1, 2, 3, 4, 5, 13, 100] $ \accepts -> do
        link <- newFakeLink (repeat accepts)
        source <- newFakeSource [Pulled "0123456789", Pulled "abc", Ended]
        sendAll link =<< unsent Nothing (framing <> sourceOutput 13 source <> trailer)
        linkReceived link `shouldReturn` framingBytes <> "0123456789abc" <> trailerBytes

    it "arrives intact in small supplied buffers" $
      forM_ [1, 2, 3, 5, 64] $ \accepts ->
        forM_ [positiveInt @4, positiveInt @7, positiveInt @64] $ \capacity -> do
          buffer <- newBuffer capacity
          link <- newFakeLink (repeat accepts)
          source <- newFakeSource [Pulled "0123", Pulled "456789", Ended]
          let output =
                Output.word32 0x01020304
                  <> Output.bytes "xy"
                  <> Output.bytes "spill"
                  <> sourceOutput 10 source
                  <> Output.word32 0x05060708
                  <> Output.word16 0x090a
          sendAll link =<< unsent (Just (outputBufferFrom buffer)) output
          linkReceived link `shouldReturn` "\1\2\3\4xyspill0123456789\5\6\7\8\9\10"

  describe "declared lengths" $ do
    let sends declared script = do
          link <- newFakeLink []
          source <- newFakeSource script
          void (sendSome link =<< unsent Nothing (framing <> sourceOutput declared source))
    it "rejects a source that ends before it gave what it declared" $ do
      sends 5 [Pulled "abc", Ended]
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 5) (P.eq 3)))
      sends 5 [Ended]
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 5) (P.eq 0)))

    it "rejects a source that gives more than it declared" $ do
      sends 5 [Pulled "abcdef"]
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 5) (P.eq 6)))
      sends 5 [Pulled "abc", Pulled "def"]
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 5) (P.eq 6)))
      sends 0 [Pulled "a"]
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 0) (P.eq 1)))

    it "accepts a source that gives exactly what it declared in several chunks" $
      sends 5 [Pulled "abc", Pulled "de", Ended] `shouldReturn` ()

  describe "gather limits" $ do
    it "offers at most 16 pieces per call" $ do
      link <- newFakeLink []
      let values = replicate 40 (BS.replicate 9000 65)
      sendAll link =<< unsent Nothing (Output.outputs Output.bytes values)
      map length <$> linkOffers link `shouldReturn` [16, 16, 8]
      linkReceived link `shouldReturn` BS.concat values

    it "gathers a chunk as the sixteenth piece" $ do
      link <- newFakeLink []
      source <- newFakeSource [Pulled "hello", Ended]
      let output = Output.outputs Output.bytes (replicate 15 (BS.replicate 9000 65)) <> sourceOutput 5 source
      sendAll link =<< unsent Nothing output
      map length <$> linkOffers link `shouldReturn` [16]

    it "doesn't pull a source once 16 pieces are gathered" $ do
      link <- newFakeLink [0]
      source <- newFakeSource [Pulled "hello", Ended]
      let output = Output.outputs Output.bytes (replicate 16 (BS.replicate 9000 65)) <> sourceOutput 5 source
      (rest, blocked) <- sendSome link =<< unsent Nothing output
      name blocked `shouldBe` "writable"
      pullCount source `shouldReturn` 0
      sendAll link rest
      map length <$> linkOffers link `shouldReturn` [16, 16, 1]
      linkReceived link `shouldReturn` BS.replicate (16 * 9000) 65 <> "hello"

    it "offers at most 256 KiB per call, and holds the rest of a chunk that doesn't fit" $ do
      link <- newFakeLink []
      let chunk = BS.pack (take 300000 (cycle [1 .. 250]))
      source <- newFakeSource [Pulled chunk, Ended]
      sendAll link =<< unsent Nothing (framing <> sourceOutput 300000 source)
      offered <- linkOffers link
      map (sum . map BS.length) offered `shouldBe` [262144, 300004 - 262144]
      map length offered `shouldBe` [2, 1]
      -- The chunk was pulled once, and the source again only when all of it was taken.
      pullCount source `shouldReturn` 2
      linkReceived link `shouldReturn` framingBytes <> chunk

  describe "output continued after a direct value" $ do
    it "goes to the link in one send, from the supplied buffer after what came before" $ do
      buffer <- newBuffer (positiveInt @64)
      link <- newFakeLink []
      sendAll link =<< unsent (Just (outputBufferFrom buffer)) directValue
      linkAddresses link >>= \calls -> do
        map (map snd) calls `shouldBe` [[10, 9000, 6]]
        withForeignPtr (bufferMemory buffer) $ \start -> case calls of
          [[(first, size), _, (second, _)]] -> (first, second) `shouldBe` (start, start `plusPtr` size)
          _ -> failTest "the output did not go in one send of three pieces"

    it "arrives intact through a link that takes a few bytes at a time" $
      forM_ [1, 3, 7, 64] $ \accepts ->
        forM_ [directValue, directValue <> mconcat (replicate 40 framing)] $ \output -> do
          buffer <- newBuffer (positiveInt @64)
          link <- newFakeLink (repeat accepts)
          sendAll link =<< unsent (Just (outputBufferFrom buffer)) output
          linkReceived link `shouldReturn` Output.outputByteString output

  describe "owned streams" $ do
    it "limits gathered chunks and preserves their order across partial writes" $ do
      remaining <- newIORef (map BS.singleton [1 .. 40])
      let next = atomicModifyIORef' remaining $ \case
            [] -> ([], BS.empty)
            chunk : rest -> (rest, chunk)
      link <- newFakeLink (repeat 5)
      sendAll link =<< unsent Nothing (Output.stream (Output.ByteStream 40 next))
      linkReceived link `shouldReturn` BS.pack [1 .. 40]
      offered <- map length <$> linkOffers link
      take 1 offered `shouldBe` [16]
      all (<= 16) offered `shouldBe` True

    it "rejects a stream that gives a different length than it declared" $ do
      let streamOf declared chunks = do
            remaining <- newIORef chunks
            link <- newFakeLink []
            let next = atomicModifyIORef' remaining $ \case
                  [] -> ([], BS.empty)
                  chunk : rest -> (rest, chunk)
            void (sendSome link =<< unsent Nothing (Output.stream (Output.ByteStream declared next)))
      streamOf 5 ["abc"] `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 5) (P.eq 3)))
      streamOf 5 ["abc", "def"] `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 5) (P.eq 6)))

  describe "pushing output" $ do
    it "only flushes when nothing is unsent, and is done once it has" $ do
      link <- newFakeLink []
      (rest, blocked) <- pushOutput link =<< unsent Nothing mempty
      name blocked `shouldBe` "done"
      unsentEmpty rest `shouldBe` True
      linkOffers link `shouldReturn` []
      flushCount link `shouldReturn` 1

    it "waits for the link to be writable when nothing is unsent and records remain" $ do
      link <- newFakeLink []
      setFlushed link False
      (_, blocked) <- pushOutput link =<< unsent Nothing mempty
      name blocked `shouldBe` "writable"
      linkOffers link `shouldReturn` []

    it "is not done while what was sent isn't flushed, and is once it is" $ do
      link <- newFakeLink []
      setFlushed link False
      (rest, blocked) <- pushOutput link =<< unsent Nothing framing
      name blocked `shouldBe` "writable"
      unsentEmpty rest `shouldBe` True
      linkOffers link `shouldReturn` [[framingBytes]]
      setFlushed link True
      (_, flushed) <- pushOutput link rest
      name flushed `shouldBe` "done"
      linkOffers link `shouldReturn` [[framingBytes]]

    it "flushes once after sending, and waits for a source if the link is flushed" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      (_, blocked) <- pushOutput link =<< unsent Nothing (framing <> sourceOutput 5 source)
      name blocked `shouldBe` "source"
      flushCount link `shouldReturn` 1

    it "doesn't wait for a source before the link has flushed, and doesn't watch it" $ do
      link <- newFakeLink []
      setFlushed link False
      source <- newClosedSource [Pulled "hello", Ended]
      (rest, blocked) <- pushOutput link =<< unsent Nothing (framing <> sourceOutput 5 source)
      name blocked `shouldBe` "writable"
      watchCounts source `shouldReturn` (0, 0)
      -- Once flushed, the source is what it waits for.
      setFlushed link True
      (_, blockedAgain) <- pushOutput link rest
      name blockedAgain `shouldBe` "source"

    it "waits for the link when it took only part of what it was offered" $ do
      link <- newFakeLink [2]
      (rest, blocked) <- pushOutput link =<< unsent Nothing framing
      name blocked `shouldBe` "writable"
      unsentEmpty rest `shouldBe` False
      flushCount link `shouldReturn` 0

    it "sends everything through the waits it reports" $ do
      link <- newFakeLink (repeat 3)
      source <- newClosedSource [Pulled "0123456789", Ended]
      let go left =
            pushOutput link left >>= \case
              (_, Done) -> pure ()
              (rest, blocked) -> do
                -- The link flushes after the first wait that is for it.
                setFlushed link True
                openSource source
                awaitAny link (Interest False blocked) *> go rest
      setFlushed link False
      go =<< unsent Nothing (framing <> sourceOutput 10 source <> trailer)
      linkReceived link `shouldReturn` framingBytes <> "0123456789" <> trailerBytes

  describe "helper threads" $ do
    -- Nothing in the sender forks: its module has no threading function. This
    -- also compares the threads before and after, through a partial send of a
    -- held chunk, a wait on a source and the rest.
    it "starts none while sending, blocked on a source or blocked on the link" $ do
      let startedSince before = filter (`notElem` before) <$> listThreads
      before <- listThreads
      partialLink <- newFakeLink [6]
      held <- newFakeSource [Pulled "0123456789", Ended]
      (partial, partialBlocked) <- sendSome partialLink =<< unsent Nothing (framing <> sourceOutput 10 held)
      name partialBlocked `shouldBe` "writable"
      startedSince before `shouldReturn` []
      sendAll partialLink partial
      startedSince before `shouldReturn` []
      linkReceived partialLink `shouldReturn` framingBytes <> "0123456789"
      waitingLink <- newFakeLink []
      closed <- newClosedSource [Pulled "0123456789", Ended]
      (waiting, waitingBlocked) <- sendSome waitingLink =<< unsent Nothing (framing <> sourceOutput 10 closed)
      name waitingBlocked `shouldBe` "source"
      startedSince before `shouldReturn` []
      openSource closed
      sendAll waitingLink waiting
      startedSince before `shouldReturn` []
      linkReceived waitingLink `shouldReturn` framingBytes <> "0123456789"

-- | Four bytes of framing, and a byte after the source.
framing, trailer :: Output.Output
framing = Output.word32 0x01020304
trailer = Output.word8 9

framingBytes, trailerBytes :: BS.ByteString
framingBytes = "\1\2\3\4"
trailerBytes = "\9"

-- | Framing (10 bytes), a direct value, framing again (6 bytes).
directValue :: Output.Output
directValue =
  (Output.word32 0x01020304 <> Output.word32 0x05060708 <> Output.word16 0x090a)
    <> Output.bytes (BS.replicate 9000 65)
    <> (Output.word32 0x0b0c0d0e <> Output.word16 0x0f10)

-- | The output's parts, in a supplied buffer of this size or none.
unsentIn :: Maybe PositiveInt -> Output.Output -> IO Unsent
unsentIn Nothing output = unsent Nothing output
unsentIn (Just size) output = do
  buffer <- newBuffer size
  unsent (Just (outputBufferFrom buffer)) output

name :: Blocked -> String
name = \case
  Done -> "done"
  OnWritable -> "writable"
  OnSource _ -> "source"

-- | The watch of a blocked source.
onSource :: Blocked -> IO Watch
onSource = \case
  OnSource watch -> watch
  _ -> error "expected a wait on a source"
