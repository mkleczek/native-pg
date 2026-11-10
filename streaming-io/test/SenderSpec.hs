{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module SenderSpec (spec) where

import Control.Exception (IOException, throwIO, try)
import Control.Monad (forM_, void)
import qualified Data.ByteString as BS
import Data.Either (isLeft)
import Data.IORef (atomicModifyIORef', modifyIORef', newIORef, readIORef)
import FakeStreams
import Foreign.ForeignPtr (withForeignPtr)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Foreign.Ptr (plusPtr)
import GHC.Conc (listThreads)
import Network.Streaming.Buffer (bufferMemory, newBuffer)
import Network.Streaming.Link (Blocked (..), Interest (..), Link (..), StreamLengthError (..))
import qualified Network.Streaming.Output as Output
import Network.Streaming.PositiveInt (PositiveInt, positiveInt)
import Network.Streaming.Sender
import Network.Streaming.Source (Pulled (..))
import System.IO.Unsafe (unsafeDupablePerformIO)
import Network.Streaming.Watch (Watch (..), awaitWatch)
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
          linkReceived link `shouldReturn` staticBytes output

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

  describe "pushing output until a stall" $ do
    it "only flushes when nothing is unsent, and is done once it has" $ do
      link <- newFakeLink []
      (rest, blocked) <- sendSome link =<< unsent Nothing mempty
      name blocked `shouldBe` "done"
      unsentEmpty rest `shouldBe` True
      linkOffers link `shouldReturn` []
      flushCount link `shouldReturn` 1

    it "waits for the link to be writable when nothing is unsent and records remain" $ do
      link <- newFakeLink []
      setFlushed link False
      (_, blocked) <- sendSome link =<< unsent Nothing mempty
      name blocked `shouldBe` "writable"
      linkOffers link `shouldReturn` []

    it "is not done while what was sent isn't flushed, and is once it is" $ do
      link <- newFakeLink []
      setFlushed link False
      (rest, blocked) <- sendSome link =<< unsent Nothing framing
      name blocked `shouldBe` "writable"
      unsentEmpty rest `shouldBe` True
      linkOffers link `shouldReturn` [[framingBytes]]
      setFlushed link True
      (_, flushed) <- sendSome link rest
      name flushed `shouldBe` "done"
      linkOffers link `shouldReturn` [[framingBytes]]

    it "flushes once after sending, and waits for a source if the link is flushed" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      (_, blocked) <- sendSome link =<< unsent Nothing (framing <> sourceOutput 5 source)
      name blocked `shouldBe` "source"
      flushCount link `shouldReturn` 1

    it "doesn't wait for a source before the link has flushed, and doesn't watch it" $ do
      link <- newFakeLink []
      setFlushed link False
      source <- newClosedSource [Pulled "hello", Ended]
      (rest, blocked) <- sendSome link =<< unsent Nothing (framing <> sourceOutput 5 source)
      name blocked `shouldBe` "writable"
      watchCounts source `shouldReturn` (0, 0)
      -- Once flushed, the source is what it waits for.
      setFlushed link True
      (_, blockedAgain) <- sendSome link rest
      name blockedAgain `shouldBe` "source"

    it "waits for the link when it took only part of what it was offered" $ do
      link <- newFakeLink [2]
      (rest, blocked) <- sendSome link =<< unsent Nothing framing
      name blocked `shouldBe` "writable"
      unsentEmpty rest `shouldBe` False
      flushCount link `shouldReturn` 0

    it "sends everything through the stalls it stops at" $ do
      link <- newFakeLink (repeat 3)
      source <- newClosedSource [Pulled "0123456789", Ended]
      let go left =
            sendSome link left >>= \case
              (_, AllSent) -> pure ()
              (rest, stalled) -> do
                -- The link flushes after the first wait that is for it.
                setFlushed link True
                openSource source
                case stalled of
                  StalledOnSource register -> awaitWatch register
                  _ -> awaitAny link (Interest False OnWritable)
                go rest
      setFlushed link False
      go =<< unsent Nothing (framing <> sourceOutput 10 source <> trailer)
      linkReceived link `shouldReturn` framingBytes <> "0123456789" <> trailerBytes

  describe "pushing with stalls that wait" $ do
    let noWaits = waits (fail "unexpected wait for the link") (\_ -> fail "unexpected wait for the source")
        -- opens the source when it is waited for, so that the wait ends at once
        opening source register = openSource source *> awaitWatch register
        recording events source = waits (modifyIORef' events ("link" :)) (\register -> modifyIORef' events ("source" :) *> opening source register)

    it "waits for a source in place when the stall runs again, and pulls it again where it stopped" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      events <- newIORef ([] :: [String])
      let stalls =
            waits
              (fail "unexpected wait for the link")
              ( \register -> do
                  offered <- linkOffers link
                  modifyIORef' events (("source after " <> show offered) :)
                  openSource source *> awaitWatch register
              )
      pushAnswer link stalls =<< unsent Nothing (framing <> sourceOutput 5 source <> trailer)
      readIORef events `shouldReturn` ["source after " <> show [[framingBytes]]]
      linkOffers link `shouldReturn` [[framingBytes], ["hello"], [trailerBytes]]
      -- A pull that found nothing, the chunk and the end.
      pullCount source `shouldReturn` 3
      watchCounts source `shouldReturn` (1, 1)

    it "gives back the source and what is after it when the stall for the source stops, and goes on from there" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      sources <- newIORef (0 :: Int)
      let stalls = Stalls (pure Again) (\_ -> Stop <$ modifyIORef' sources (+ 1))
      Stopped rest _ <- pushWith link stalls =<< unsent Nothing (framing <> sourceOutput 5 source <> trailer)
      readIORef sources `shouldReturn` 1
      linkOffers link `shouldReturn` [[framingBytes]]
      pullCount source `shouldReturn` 1
      openSource source
      sendAll link rest
      linkReceived link `shouldReturn` framingBytes <> "hello" <> trailerBytes

    it "gives back what the link did not take of the pieces before a pending source when the stall for the link stops" $ do
      link <- newFakeLink [2]
      source <- newClosedSource [Pulled "hello", Ended]
      Stopped rest _ <- pushWith link (Stalls (pure Stop) (\_ -> fail "unexpected wait for the source")) =<< unsent Nothing (framing <> sourceOutput 5 source)
      linkReceived link `shouldReturn` BS.take 2 framingBytes
      openSource source
      sendAll link rest
      linkReceived link `shouldReturn` framingBytes <> "hello"

    it "gives the watch of the source it stopped on, which registers when it is run" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      Stopped _ watch <- pushWith link stopping =<< unsent Nothing (framing <> sourceOutput 5 source)
      watchCounts source `shouldReturn` (0, 0)
      watchCancel =<< watch
      watchCounts source `shouldReturn` (1, 1)
      writableWatchCount link `shouldReturn` 0

    it "gives the watch for the link to be writable when it stopped on the link, or on its flush" $ do
      partial <- newFakeLink [2]
      Stopped _ watch <- pushWith partial stopping =<< unsent Nothing framing
      writableWatchCount partial `shouldReturn` 0
      watchCancel =<< watch
      writableWatchCount partial `shouldReturn` 1
      unflushed <- newFakeLink []
      setFlushed unflushed False
      Stopped rest watchFlush <- pushWith unflushed stopping =<< unsent Nothing framing
      unsentEmpty rest `shouldBe` True
      watchCancel =<< watchFlush
      writableWatchCount unflushed `shouldReturn` 1

    it "sends the framing and a ready chunk in one call, and waits for nothing" $ do
      link <- newFakeLink []
      source <- newFakeSource [Pulled "hello", Ended]
      pushAnswer link noWaits =<< unsent Nothing (framing <> waitingOutput 5 source <> trailer)
      linkOffers link `shouldReturn` [[framingBytes, "hello"], [trailerBytes]]

    it "sends the framing before it waits for a pending source, and goes on with the chunk after the wait" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      events <- newIORef ([] :: [String])
      let stalls =
            waits
              (fail "unexpected wait for the link")
              ( \register -> do
                  offered <- linkOffers link
                  modifyIORef' events (("source after " <> show offered) :)
                  opening source register
              )
      pushAnswer link stalls =<< unsent Nothing (framing <> waitingOutput 5 source <> trailer)
      readIORef events `shouldReturn` ["source after " <> show [[framingBytes]]]
      linkOffers link `shouldReturn` [[framingBytes], ["hello"], [trailerBytes]]
      -- A pending pull, the chunk and the end.
      pullCount source `shouldReturn` 3
      watchCounts source `shouldReturn` (1, 1)

    it "waits for the link until it has taken the framing, and then for the source" $ do
      link <- newFakeLink [1, 1]
      source <- newClosedSource [Pulled "hello", Ended]
      events <- newIORef ([] :: [String])
      pushAnswer link (recording events source) =<< unsent Nothing (framing <> waitingOutput 5 source)
      readIORef events `shouldReturn` ["source", "link", "link"]
      linkReceived link `shouldReturn` framingBytes <> "hello"

    it "waits for the link to flush what it took before it waits for the source, and once more at the end" $ do
      link <- newFakeLink []
      setFlushed link False
      source <- newClosedSource [Pulled "hello", Ended]
      events <- newIORef ([] :: [String])
      let stalls =
            waits
              (modifyIORef' events ("link" :) *> setFlushed link True)
              (\register -> modifyIORef' events ("source" :) *> opening source register)
      pushAnswer link stalls =<< unsent Nothing (framing <> waitingOutput 5 source)
      readIORef events `shouldReturn` ["source", "link"]

    it "waits for the link when it took only part of a chunk, and sends all of it" $ do
      link <- newFakeLink [4, 3, 3]
      source <- newFakeSource [Pulled "0123456789", Ended]
      events <- newIORef ([] :: [String])
      pushAnswer link (recording events source) =<< unsent Nothing (framing <> waitingOutput 10 source)
      readIORef events `shouldReturn` ["link", "link", "link"]
      linkReceived link `shouldReturn` framingBytes <> "0123456789"

    it "sends everything when the link takes a few bytes at a time and the source waits" $ do
      link <- newFakeLink (repeat 3)
      source <- newClosedSource [Pulled "0123456789", Ended]
      pushAnswer link (waits (pure ()) (opening source)) =<< unsent Nothing (framing <> waitingOutput 10 source <> trailer)
      linkReceived link `shouldReturn` framingBytes <> "0123456789" <> trailerBytes

    it "only flushes when nothing is unsent" $ do
      link <- newFakeLink []
      setFlushed link False
      events <- newIORef ([] :: [String])
      pushAnswer link (waits (modifyIORef' events ("link" :) *> setFlushed link True) (\_ -> fail "unexpected wait for the source")) =<< unsent Nothing mempty
      readIORef events `shouldReturn` ["link"]
      linkOffers link `shouldReturn` []

    it "lets the failure of a wait out, after it has sent the framing" $ do
      link <- newFakeLink []
      source <- newClosedSource [Pulled "hello", Ended]
      let stalls = waits (pure ()) (\_ -> throwIO (userError "the client went away"))
      result <- try @IOException (pushAnswer link stalls =<< unsent Nothing (framing <> waitingOutput 5 source))
      isLeft result `shouldBe` True
      linkReceived link `shouldReturn` framingBytes

    it "rejects a source that gives a different length than it declared" $ do
      link <- newFakeLink []
      source <- newFakeSource [Pulled "abc", Ended]
      (pushAnswer link noWaits =<< unsent Nothing (waitingOutput 5 source))
        `shouldSatisfy` P.throws (P.con (StreamLengthError (P.eq 5) (P.eq 3)))

    it "is sent by a push that stops when its source is ready, and fails when it has to wait" $ do
      ready <- newFakeLink []
      source <- newFakeSource [Pulled "hello", Ended]
      (rest, stalled) <- sendSome ready =<< unsent Nothing (waitingOutput 5 source)
      name stalled `shouldBe` "done"
      unsentEmpty rest `shouldBe` True
      linkReceived ready `shouldReturn` "hello"
      link <- newFakeLink []
      closed <- newClosedSource [Pulled "hello", Ended]
      result <- try @IOException (void (sendSome link =<< unsent Nothing (waitingOutput 5 closed)))
      isLeft result `shouldBe` True

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
unsentIn size output = do buffer <- traverse newBuffer size; unsent (fmap outputBufferFrom buffer) output

name :: Stalled -> String
name = \case
  AllSent -> "done"
  StalledOnLink -> "writable"
  StalledOnSource _ -> "source"

-- | The registration of the watch of a stalled source.
onSource :: Stalled -> IO Watch
onSource = \case
  StalledOnSource register -> register
  _ -> error "expected a wait on a source"

-- | Stalls that wait with the actions: for the link, and for a source, with
-- the registration of its watch.
waits :: IO () -> (IO Watch -> IO ()) -> Stalls
waits writable source = Stalls (Again <$ writable) (\register -> Again <$ source register)

-- | Pushes with the stalls until all is sent and flushed.
pushAnswer :: Link link => link -> Stalls -> Unsent -> IO ()
pushAnswer link stalls left = void (pushWith link stalls left)

-- | The bytes of an output that has no streams, for fixtures.
staticBytes :: Output.Output -> BS.ByteString
staticBytes output = unsafeDupablePerformIO $ do
  parts <- (`Output.writeOutput` output) =<< mallocPlainForeignPtrBytes (Output.outputCopied output)
  pure . BS.concat $ map (either id uncopied) parts
  where
    uncopied (Output.UncopiedBytes value) = value
    uncopied _ = error "an output with streams has no byte string"
