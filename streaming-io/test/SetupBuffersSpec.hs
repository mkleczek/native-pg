{-# LANGUAGE DataKinds #-}

-- | The setup buffers of the pool, as arrows, for every arrow that runs a program: grants of several buffers at once,
-- the queue of the grants that wait, by their size, and the setup capacity. A part holds a grant, others ask for theirs,
-- and the signals and the pool's own count of those who wait say when to go on, so that the order of events is the
-- test's.
module SetupBuffersSpec (spec) where

import Control.Arrow (arr, (>>>), (>>^), (^>>))
import Control.Monad (forM, forM_)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Tuple (Solo (MkSolo))
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Storable (peek, poke)
import LoansTestSupport
import Network.Streaming.Buffer (Buffer, bufferCapacity, bufferMemory)
import Network.Streaming.BufferPool (BufferPool)
import Network.Streaming.Classes
import Network.Streaming.Combinators (sleep, together)
import Network.Streaming.Loans
import Skeletest
import System.Mem (performMajorGC)

spec :: Spec
spec = describe "the setup buffers" $ do
  forM_ runners $ \runner@(Runner name _) -> describe name $ do
    let run :: Program a -> IO a
        run = runOn runner

    describe "grants of several buffers at once" $ do
      it "leaves the buffers that remain usable when a larger grant must wait" $ do
        pool <- newTestPool 1 1 3
        run $
          withSetupBuffers @(Solo Buffer) pool $
            steps
              [ stillWaits (const () ^>> withSetupBuffers @(Buffer, Buffer, Buffer) pool (arr (const ()))),
                -- The grant that waited has given up its place, and the two buffers that remain are lent at once.
                proceeds (const () ^>> withSetupBuffers @(Buffer, Buffer) pool (arr (\((second', third), ()) -> (bufferMemory second', bufferMemory third))))
                  >>> now (uncurry shouldNotBe)
              ]

      it "gives the same five buffers again without copying them" $ do
        pool <- newTestPool 1 1 5
        written <- newIORef Nothing
        run $
          withSetupBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) pool $
            now
              ( \((a, b, c, d, e), ()) -> do
                  withForeignPtr (bufferMemory c) $ \pointer -> poke pointer 91
                  writeIORef written (Just (map bufferMemory [a, b, c, d, e]))
              )
        run $
          withSetupBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) pool $
            now
              ( \((a, b, c, d, e), ()) -> do
                  readIORef written `shouldReturn` Just (map bufferMemory [a, b, c, d, e])
                  withForeignPtr (bufferMemory c) $ \pointer -> peek pointer `shouldReturn` 91
              )

      it "refuses a grant that exceeds the setup capacity at once, and doesn't queue it" $ do
        pool <- newTestPool 1 1 2
        run $ failsWith "buffer count must fit capacity" (withSetupBuffers @(Buffer, Buffer, Buffer) pool (arr (const ())))
        setupWaiting pool `shouldReturn` 0

      it "makes buffers of the size of the pool, as many as its setup count" $ do
        pool <- newSizedPool 7 1 1 3
        run $
          withSetupBuffers @(Buffer, Buffer, Buffer) pool $
            steps
              [ now (\((a, b, c), ()) -> map bufferCapacity [a, b, c] `shouldBe` [7, 7, 7]),
                stillWaits (const () ^>> withSetupBuffers @(Solo Buffer) pool (arr (const ())))
              ]
        length <$> setupFree pool `shouldReturn` 3

    describe "the queue of grants" $ do
      it "serves grants of the same size in the order they were asked for" $ do
        pool <- newTestPool 1 1 2
        journal <- newJournal
        held <- newSignal
        go <- newSignal
        firstIn <- newSignal
        leaveFirst <- newSignal
        run $
          together
            [ withSetupBuffers @(Buffer, Buffer) pool (raise held >>> awaitSignal go),
              awaitSignal held >>> withSetupBuffers @(Buffer, Buffer) pool (note journal "first in" >>> raise firstIn >>> awaitSignal leaveFirst),
              awaitSetupWaiting pool 1 >>> withSetupBuffers @(Buffer, Buffer) pool (note journal "second in"),
              steps
                [ awaitSetupWaiting pool 2,
                  raise go,
                  awaitSignal firstIn,
                  -- A grant that comes now waits behind both.
                  stillWaits (const () ^>> withSetupBuffers @(Buffer, Buffer) pool (arr (const ()))),
                  now (\_ -> readJournal journal) >>> now (\noted -> noted `shouldBe` ["first in"]),
                  raise leaveFirst
                ]
            ]
        readJournal journal `shouldReturn` ["first in", "second in"]

      it "serves the first grant that fits, and a larger one that came earlier keeps its place" $ do
        pool <- newTestPool 1 1 2
        journal <- newJournal
        held <- newSignal
        go <- newSignal
        smallIn <- newSignal
        leaveSmall <- newSignal
        smallOut <- newSignal
        largeIn <- newSignal
        run $
          together
            [ withSetupBuffers @(Solo Buffer) pool (raise held >>> awaitSignal go),
              awaitSignal held >>> withSetupBuffers @(Buffer, Buffer) pool (note journal "large in" >>> raise largeIn),
              awaitSetupWaiting pool 1 >>> withSetupBuffers @(Solo Buffer) pool (note journal "small in" >>> raise smallIn >>> awaitSignal leaveSmall) >>> raise smallOut,
              steps
                [ awaitSignal smallIn,
                  -- The small grant did not wait for the large one: one buffer was free, and the large one needs two.
                  stillWaits (const () ^>> withSetupBuffers @(Solo Buffer) pool (arr (const ()))),
                  raise leaveSmall,
                  awaitSignal smallOut,
                  now (\_ -> readJournal journal) >>> now (\noted -> noted `shouldBe` ["small in"]),
                  raise go,
                  awaitSignal largeIn
                ]
            ]
        readJournal journal `shouldReturn` ["small in", "large in"]

      it "serves the older of the grants that fit, across sizes" $ do
        pool <- newTestPool 1 1 2
        journal <- newJournal
        held <- newSignal
        go <- newSignal
        largeIn <- newSignal
        leaveLarge <- newSignal
        smallIn <- newSignal
        run $
          together
            [ withSetupBuffers @(Buffer, Buffer) pool (raise held >>> awaitSignal go),
              awaitSignal held >>> withSetupBuffers @(Buffer, Buffer) pool (note journal "large in" >>> raise largeIn >>> awaitSignal leaveLarge),
              awaitSetupWaiting pool 1 >>> withSetupBuffers @(Solo Buffer) pool (note journal "small in" >>> raise smallIn),
              steps
                [ awaitSetupWaiting pool 2,
                  -- Both fit once the pair is back, and the large one asked first.
                  raise go,
                  awaitSignal largeIn,
                  stillWaits (awaitSignal smallIn),
                  raise leaveLarge,
                  awaitSignal smallIn
                ]
            ]
        readJournal journal `shouldReturn` ["large in", "small in"]

      it "drops a grant that was cancelled before it serves the next" $ do
        pool <- newTestPool 1 1 2
        held <- newSignal
        go <- newSignal
        journal <- newJournal
        run $
          together
            [ withSetupBuffers @(Buffer, Buffer) pool (raise held >>> awaitSignal go),
              steps
                [ awaitSignal held,
                  stillWaits (const () ^>> withSetupBuffers @(Solo Buffer) pool (note journal "cancelled in")),
                  -- The cancelled one is first in the queue of single buffers, and the pair has not been given back yet.
                  together [withSetupBuffers @(Solo Buffer) pool (note journal "second in"), awaitSetupWaiting pool 2 >>> raise go]
                ]
            ]
        readJournal journal `shouldReturn` ["second in"]
        checkSetup pool

      it "keeps the order of the grants asked for, and returns the buffers, when a queued grant is cancelled" $ do
        pool <- newTestPool 1 1 2
        journal <- newJournal
        held <- newSignal
        go <- newSignal
        firstIn <- newSignal
        leaveFirst <- newSignal
        lateIn <- newSignal
        run $
          together
            [ withSetupBuffers @(Buffer, Buffer) pool (raise held >>> awaitSignal go),
              awaitSignal held >>> withSetupBuffers @(Buffer, Buffer) pool (note journal "first in" >>> raise firstIn >>> awaitSignal leaveFirst),
              steps
                [ awaitSetupWaiting pool 1,
                  stillWaits (const () ^>> withSetupBuffers @(Buffer, Buffer) pool (note journal "cancelled in")),
                  raise go,
                  awaitSignal firstIn,
                  together
                    [ withSetupBuffers @(Buffer, Buffer) pool (note journal "late in" >>> raise lateIn),
                      -- The grant that came late waits behind the one that holds the buffers, and the cancelled one has no place.
                      awaitSetupWaiting pool 2 >>> stillWaits (awaitSignal lateIn) >>> raise leaveFirst
                    ]
                ]
            ]
        readJournal journal `shouldReturn` ["first in", "late in"]
        checkSetup pool

      it "returns every buffer when a deadline and the return that grants a waiting grant come together" $ do
        pool <- newTestPool 1 1 2
        flags <- forM [0 .. 99 :: Int] (const newSignal)
        let iteration index held =
              steps
                [ together
                    [ withSetupBuffers @(Buffer, Buffer) pool (raise held >>> const () ^>> sleep (micros (fromIntegral index * 2))),
                      awaitSignal held >>> within (micros (fromIntegral index * 2)) (withSetupBuffers @(Buffer, Buffer) pool (arr (const ()))) >>> arr (const ())
                    ],
                  proceeds (withSetupBuffers @(Buffer, Buffer) pool (arr (\((input, output), ()) -> (bufferCapacity input, bufferCapacity output))))
                    >>> now (\sizes -> sizes `shouldBe` (8, 8))
                ]
        run $ steps (zipWith iteration [0 :: Int ..] flags)
        checkSetup pool

      it "lets a part cancel queued grants of every size, and then lends all the inventory" $ do
        pool <- newTestPool 1 1 2
        held <- newSignal
        go <- newSignal
        run $
          together
            [ withSetupBuffers @(Buffer, Buffer) pool (raise held >>> awaitSignal go),
              awaitSignal held
                >>> race
                  ( together
                      [ withSetupBuffers @(Solo Buffer) pool (arr (const ())),
                        withSetupBuffers @(Buffer, Buffer) pool (arr (const ())),
                        withSetupBuffers @(Solo Buffer) pool (arr (const ()))
                      ]
                  )
                  (awaitSetupWaiting pool 3)
                >>> raise go
            ]
        forM_ [1 .. 3 :: Int] $ \_ ->
          run $ proceeds (withSetupBuffers @(Buffer, Buffer) pool (arr (\((input, output), ()) -> (bufferCapacity input, bufferCapacity output)))) `yields` (8, 8)

    describe "the scope of a grant" $ do
      it "returns the buffers when the body fails" $ do
        pool <- newTestPool 1 1 1
        run $
          steps
            [ failsWith "the body failed" (withSetupBuffers @(Solo Buffer) pool (now (\_ -> ioError (userError "the body failed")))),
              proceeds (withSetupBuffers @(Solo Buffer) pool (arr (const ())))
            ]

      it "returns the buffers when the body is cancelled" $ do
        pool <- newTestPool 1 1 1
        run $
          steps
            [ stillWaits (withSetupBuffers @(Solo Buffer) pool blockForever),
              proceeds (withSetupBuffers @(Solo Buffer) pool (arr (const ())))
            ]

      it "keeps a pool that is used after the helper that made it returned and the memory was collected" $ do
        pool <- newTestPool 1 1 1
        performMajorGC
        forM_ [1 .. 3 :: Int] $ \_ ->
          run $
            steps
              [ proceeds (withSetupBuffers @(Solo Buffer) pool (arr (const ()))),
                proceeds (request pool (arr (const ())))
              ]

    describe "setup against requests" $ do
      it "grants setup buffers while the buffers of the requests are all lent" $ do
        pool <- newTestPool 1 1 1
        primary <- newSlot
        extra <- newSlot
        run $
          request pool $
            steps
              [ borrowInto primary,
                borrowInto extra,
                proceeds (const () ^>> withSetupBuffers @(Solo Buffer) pool (arr (\(MkSolo setup, ()) -> bufferMemory setup)))
                  >>> now
                    ( \memory -> do
                        mine <- mapM memoryOf [primary, extra]
                        (memory `elem` mine) `shouldBe` False
                    ),
                giveBack extra,
                giveBack primary
              ]

      it "keeps the setup buffers and the buffers of the requests apart: a queue of requests doesn't hold up a setup" $ do
        pool <- newTestPool 2 1 1
        run $
          holding 2 pool $
            race
              (const () ^>> request pool (arr (const ())))
              (awaitRequestWaiting pool 1 >>> proceeds (const () ^>> withSetupBuffers @(Solo Buffer) pool (arr (const ()))))
              >>^ const ()

      it "lends the same setup buffer again, without taking a buffer of the requests" $ do
        pool <- newTestPool 1 1 1
        written <- newIORef Nothing
        run $ withSetupBuffers @(Solo Buffer) pool (now (\(MkSolo buffer, ()) -> writeIORef written (Just (bufferMemory buffer))))
        primary <- newSlot
        run $
          request pool $
            steps
              [ borrowInto primary,
                proceeds (const () ^>> withSetupBuffers @(Solo Buffer) pool (now (\(MkSolo buffer, ()) -> readIORef written >>= \memory -> Just (bufferMemory buffer) `shouldBe` memory))),
                giveBack primary
              ]

-- | The setup buffers are free, once each, and nobody waits for them.
checkSetup :: BufferPool -> IO ()
checkSetup pool = do
  length <$> setupFree pool `shouldReturn` 2
  setupWaiting pool `shouldReturn` 0
