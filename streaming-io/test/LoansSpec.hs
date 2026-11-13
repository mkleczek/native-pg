-- | The loans of a request, as arrows, for every arrow that runs a program. A script is a list of steps on the request's
-- loans, and several parts of a request, or several requests, wait for each other with signals and with what the pool
-- says of who waits, so that the order of events is the test's and not a matter of timing.
--
-- Every case is marked as an expected failure ('notYet') until the code is there.
module LoansSpec (spec) where

import Control.Arrow (arr, first, (&&&), (***), (>>>), (>>^), (^>>))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (fromException)
import Control.Monad (forM, forM_, void)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (nub)
import Data.Tuple (Solo)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Storable (peek, poke)
import LoansTestSupport
import Network.Streaming.Buffer (Buffer, bufferCapacity, bufferMemory)
import Network.Streaming.BufferPool (BufferPool)
import Network.Streaming.Classes
import Network.Streaming.Combinators (sleep, timesWith, together)
import Network.Streaming.Delimited (runCores)
import Network.Streaming.Loans
import Network.Streaming.Threads (runThreads)
import Skeletest
import System.IO.Error (ioeGetErrorString)
import System.Timeout (timeout)

spec :: Spec
spec = notYet $ describe "the loans of a request" $ do
  forM_ runners $ \runner@(Runner name _) -> describe name $ do
    let run :: Program a -> IO a
        run = runOn runner

    describe "the two loans" $ do
      it "lends the primary and one extra from the reserve, and refuses a third loan at once" $ do
        pool <- newTestPool 1 1 1
        primary <- newSlot
        extra <- newSlot
        run $
          request pool $
            steps
              [ borrowInto primary,
                borrowInto extra,
                differentMemory primary extra,
                failsWith "request already has two buffer loans" borrow,
                giveBack extra,
                giveBack primary
              ]

      it "lends the primary again once it is given back, and ignores a release that is stale" $ do
        pool <- newTestPool 1 1 1
        first' <- newSlot
        second' <- newSlot
        extra <- newSlot
        run $
          request pool $
            steps
              [ borrowInto first',
                giveBack first',
                borrowInto second',
                sameMemory first' second',
                -- The primary is lent as the second loan, so this release is of a loan that has ended.
                giveBack first',
                borrowInto extra,
                differentMemory extra second',
                giveBack second',
                giveBack extra
              ]

      it "gives back an extra loan promptly, for the next request to borrow, while the primary stays with its request" $ do
        pool <- newTestPool 2 1 1
        firstPrimary <- newSlot
        secondPrimary <- newSlot
        firstExtra <- newSlot
        run $
          holding 2 pool $
            steps
              [ at 0 (borrowInto firstPrimary),
                at 1 (borrowInto secondPrimary),
                at 0 (borrowInto firstExtra),
                giveBack firstExtra,
                at 1 (proceeds (borrow >>> release))
              ]

    describe "admission" $ do
      forM_ [(1, 1), (1, 3)] $ \(ordinary, reserve) ->
        it ("keeps a reserve of " <> show reserve <> " out of the admission of requests, and lends it to extra loans") $ do
          pool <- newTestPool ordinary reserve 1
          primary <- newSlot
          extra <- newSlot
          run $
            request pool $
              steps
                [ -- A request needs its primary and the whole reserve free, and the reserve is not free while one is in.
                  stillWaits (const () ^>> request pool (arr (const ()))),
                  borrowInto primary,
                  borrowInto extra,
                  differentMemory primary extra,
                  giveBack extra,
                  giveBack primary
                ]

      it "admits as many requests as the ordinary buffers, and keeps a reserve of several buffers for extra loans" $ do
        pool <- newTestPool 2 2 1
        firstPrimary <- newSlot
        secondPrimary <- newSlot
        firstExtra <- newSlot
        secondExtra <- newSlot
        run $
          holding 2 pool $
            steps
              [ at 0 (borrowInto firstPrimary),
                at 1 (borrowInto secondPrimary),
                at 0 (borrowInto firstExtra),
                at 1 (borrowInto secondExtra),
                now (\_ -> mapM memoryOf [firstPrimary, secondPrimary, firstExtra, secondExtra]) >>> now (\memories -> length (nub memories) `shouldBe` 4),
                giveBack firstExtra,
                giveBack secondExtra,
                -- Two are free again, and a request needs its primary and a reserve of two.
                stillWaits (const () ^>> request pool (arr (const ()))),
                at 0 (proceeds (borrow >>> release)),
                giveBack firstPrimary,
                giveBack secondPrimary
              ]

      it "lends buffers of the size the pool was made with, and no more than its counts" $ do
        pool <- newSizedPool 7 2 1 3
        first' <- newSlot
        second' <- newSlot
        extra <- newSlot
        run $
          holding 2 pool $
            steps
              [ at 0 (borrowInto first'),
                at 1 (borrowInto second'),
                at 0 (borrowInto extra),
                now (\_ -> mapM capacityOf [first', second', extra]) >>> now (\sizes -> sizes `shouldBe` [7, 7, 7]),
                -- The three buffers of the pool are lent: another extra loan waits, and so does another request.
                at 1 (stillWaits borrow),
                stillWaits (const () ^>> request pool (arr (const ()))),
                giveBack extra
              ]
        free <- requestFree pool
        map bufferCapacity free `shouldBe` [7, 7, 7]

    describe "pending loans" $ do
      it "gives a primary that is given back to the pending extra loan of its request, which the pool has none for" $ do
        pool <- newTestPool 2 1 1
        firstPrimary <- newSlot
        secondPrimary <- newSlot
        secondExtra <- newSlot
        pending <- newSlot
        lent <- newSignal
        run $
          holding 2 pool $
            together
              [ steps
                  [ at 1 (borrowInto secondPrimary),
                    at 1 (borrowInto secondExtra),
                    at 0 (borrowInto firstPrimary),
                    raise lent,
                    awaitRequestWaiting pool 1,
                    -- The pending loan holds the extra place, so a third is refused at once.
                    at 0 (failsWith "request already has two buffer loans" borrow),
                    giveBack firstPrimary
                  ],
                steps [awaitSignal lent, at 0 (borrowInto pending), sameMemory firstPrimary pending, giveBack pending]
              ]

      it "gives up the place of a pending extra loan that is cancelled, and a later return isn't lost to it" $ do
        pool <- newTestPool 2 1 1
        firstPrimary <- newSlot
        secondPrimary <- newSlot
        secondExtra <- newSlot
        firstExtra <- newSlot
        run $
          holding 2 pool $
            steps
              [ at 1 (borrowInto secondPrimary),
                at 1 (borrowInto secondExtra),
                at 0 (borrowInto firstPrimary),
                at 0 (stillWaits borrow),
                -- The extra place is free at once, and not only when the pool next hands a buffer over.
                at 0 (stillWaits borrow),
                giveBack secondExtra,
                -- The buffer is there for the first request.
                at 0 (proceeds (borrowInto firstExtra)),
                differentMemory firstExtra firstPrimary,
                giveBack firstExtra,
                giveBack firstPrimary
              ]

      it "serves the pending extra loans in the order they were asked for" $ do
        pool <- newTestPool 3 1 1
        aPrimary <- newSlot
        aExtra <- newSlot
        bPrimary <- newSlot
        bExtra <- newSlot
        cPrimary <- newSlot
        cExtra <- newSlot
        journal <- newJournal
        ready <- newSignal
        aGot <- newSignal
        bGot <- newSignal
        leaveA <- newSignal
        run $
          holding 3 pool $
            together
              [ steps
                  [ at 2 (borrowInto cPrimary),
                    at 2 (borrowInto cExtra),
                    raise ready,
                    awaitRequestWaiting pool 2,
                    giveBack cExtra,
                    awaitSignal aGot,
                    -- The buffer went to the request that asked first.
                    stillWaits (awaitSignal bGot),
                    raise leaveA,
                    awaitSignal bGot
                  ],
                steps [awaitSignal ready, at 0 (borrowInto aPrimary), at 0 (borrowInto aExtra), note journal "a", raise aGot, awaitSignal leaveA, giveBack aExtra],
                steps [awaitRequestWaiting pool 1, at 1 (borrowInto bPrimary), at 1 (borrowInto bExtra), note journal "b", raise bGot, giveBack bExtra]
              ]
        readJournal journal `shouldReturn` ["a", "b"]

      it "gives the reserve to a pending extra loan, though many admissions queued before it" $ do
        pool <- newTestPool 2 1 1
        aPrimary <- newSlot
        aExtra <- newSlot
        bPrimary <- newSlot
        bExtra <- newSlot
        run $
          holding 2 pool $
            race
              -- Each of these needs two buffers free, and one is at most.
              (const () ^>> together (replicate 64 (request pool (arr (const ())))))
              ( steps
                  [ at 1 (borrowInto bPrimary),
                    at 1 (borrowInto bExtra),
                    at 0 (borrowInto aPrimary),
                    awaitRequestWaiting pool 64,
                    proceeds (together [at 0 (borrowInto aExtra), awaitRequestWaiting pool 65 >>> giveBack bExtra]),
                    giveBack aExtra
                  ]
              )
              >>^ const ()

      it "fails a borrow that is still waiting when its request ends" $ do
        pool <- newTestPool 2 1 1
        aPrimary <- newSlot
        bPrimary <- newSlot
        bExtra <- newSlot
        outcome <- newEmptyMVar
        run $
          holding 2 pool $
            steps
              [ at 0 (borrowInto bPrimary),
                at 0 (borrowInto bExtra),
                at 1 (borrowInto aPrimary),
                -- A borrow that another thread runs, on loans that it keeps after the request has ended. The request that
                -- holds the buffers ends after it.
                at 1 (now (void . forkIO . runThreads (tryA borrow >>> now (putMVar outcome)))),
                awaitRequestWaiting pool 1
              ]
        result <- timeout 5000000 (takeMVar outcome)
        case result of
          Just (Left failure) -> fmap ioeGetErrorString (fromException failure) `shouldBe` Just "request buffer access is closed"
          Just (Right _) -> failTest "the borrow was given a loan after its request ended"
          Nothing -> failTest "the borrow waited for ever after its request ended"

    describe "cancellation and the end of a request" $ do
      it "returns every buffer once, whether the deadline of a pending extra loan or the return that grants it comes first" $ do
        pool <- newTestPool 2 1 1
        aPrimary <- newSlot
        bPrimary <- newSlot
        bExtra <- newSlot
        let iteration :: (Runtime arr) => Int -> arr [Loans] ()
            iteration index =
              steps
                [ at 1 (proceeds (borrowInto bExtra)),
                  -- The deadline of the borrow and the return of the buffer that it waits for come together, or one first.
                  together
                    [ at 0 (within (micros (fromIntegral index * 3)) (scope borrow (arr fst >>> release) blockForever)) >>> arr (const ()),
                      const () ^>> sleep (micros (fromIntegral index * 3)) >>> giveBack bExtra
                    ],
                  now (\_ -> requestFree pool) >>> now (\free -> length free `shouldBe` 1)
                ]
        run $ holding 2 pool $ steps ([at 0 (borrowInto aPrimary), at 1 (borrowInto bPrimary)] ++ map iteration [0 .. 99])
        length <$> requestFree pool `shouldReturn` 3

      it "returns every buffer once, whether the deadline of an extra loan or the return of the primary that it is given comes first" $ do
        pool <- newTestPool 2 1 1
        aPrimary <- newSlot
        again <- newSlot
        bPrimary <- newSlot
        bExtra <- newSlot
        let iteration :: (Runtime arr) => Int -> arr [Loans] ()
            iteration index =
              steps
                [ at 0 (borrowInto aPrimary),
                  together
                    [ at 0 (within (micros (fromIntegral index * 3)) (scope borrow (arr fst >>> release) blockForever)) >>> arr (const ()),
                      const () ^>> sleep (micros (fromIntegral index * 3)) >>> giveBack aPrimary
                    ],
                  -- The request has its primary again, and it is the same buffer, and nothing was added to the pool.
                  at 0 (proceeds (borrowInto again)),
                  sameMemory aPrimary again,
                  giveBack again,
                  now (\_ -> requestFree pool) >>> now (\free -> length free `shouldBe` 0)
                ]
        run $ holding 2 pool $ steps ([at 1 (borrowInto bPrimary), at 1 (borrowInto bExtra)] ++ map iteration [0 .. 99])

      it "expires the loans of a request when it ends: one that escaped fails, and a release of a stale loan returns nothing" $ do
        pool <- newTestPool 1 1 1
        primary <- newSlot
        extra <- newSlot
        fresh <- newSlot
        freshExtra <- newSlot
        escaped <- newIORef Nothing
        run $ request pool (steps [borrowInto primary, borrowInto extra, now (writeIORef escaped . Just)])
        run $
          now (\_ -> readIORef escaped)
            >>> now (maybe (failTest "no loans escaped") pure)
            >>> failsWith "request buffer access is closed" borrow
        run $
          request pool $
            steps
              [ borrowInto fresh,
                borrowInto freshExtra,
                giveBack primary,
                giveBack extra,
                -- Had those two returned buffers, a request could be admitted.
                stillWaits (const () ^>> request pool (arr (const ()))),
                giveBack freshExtra,
                giveBack fresh
              ]

      it "returns the buffers of a request when all of its parts have ended, and not before" $ do
        pool <- newTestPool 1 1 1
        primary <- newSlot
        extra <- newSlot
        entered <- newSignal
        hold <- newSignal
        run $
          request pool $
            together
              [ steps [borrowInto primary, borrowInto extra, raise entered, awaitSignal hold, giveBack extra, giveBack primary],
                steps [awaitSignal entered, stillWaits (const () ^>> request pool (arr (const ()))), raise hold]
              ]
        run $ proceeds (request pool (arr (const ())))

      it "gives back the loan of a part that is cancelled, for the other parts of the request to borrow" $ do
        pool <- newTestPool 1 1 1
        first' <- newSlot
        second' <- newSlot
        inChild <- newSignal
        run $
          request pool $
            steps
              [ race (scope borrow (arr fst >>> release) (raise inChild >>> blockForever)) (awaitSignal inChild) >>> arr (const ()),
                borrowInto first',
                borrowInto second',
                differentMemory first' second',
                giveBack second',
                giveBack first'
              ]

      it "returns both loans when the script of the request fails" $ do
        pool <- newTestPool 1 1 1
        run $
          steps
            [ failsWith "the script failed" (request pool (borrowTwice >>> now (\_ -> ioError (userError "the script failed")))),
              proceeds (request pool borrowTwice)
            ]

      it "returns the loans of a request that is cancelled while it holds them" $ do
        pool <- newTestPool 1 1 1
        run $ steps [stillWaits (request pool (borrowTwice >>> blockForever)), proceeds (request pool borrowTwice)]

      it "lets a part cancel a queued admission and a queued extra loan, and all the inventory is there afterwards" $ do
        pool <- newTestPool 2 1 1
        aPrimary <- newSlot
        aExtra <- newSlot
        bPrimary <- newSlot
        bExtra <- newSlot
        run $
          steps
            [ holding 2 pool $
                steps
                  [ at 1 (borrowInto bPrimary),
                    at 1 (borrowInto bExtra),
                    at 0 (borrowInto aPrimary),
                    stillWaits (const () ^>> request pool (arr (const ()))),
                    at 0 (stillWaits borrow),
                    giveBack bExtra,
                    at 0 (proceeds (borrowInto aExtra)),
                    giveBack aExtra
                  ],
              proceeds (holding 2 pool (arr (const ())))
            ]
        length <$> requestFree pool `shouldReturn` 3

    describe "storage" $ do
      it "lends the same blocks again, as they were left" $ do
        pool <- newTestPool 1 1 1
        written <- newIORef Nothing
        run $
          request pool $
            borrow
              >>> now
                ( \loan -> do
                    let buffer = loanBuffer loan
                    bufferCapacity buffer `shouldBe` 8
                    withForeignPtr (bufferMemory buffer) $ \pointer -> do
                      peek pointer `shouldReturn` 0
                      poke pointer 73
                    writeIORef written (Just (bufferMemory buffer))
                    pure loan
                )
              >>> release
        run $
          request pool $
            borrow
              >>> now
                ( \loan -> do
                    memory <- readIORef written
                    Just (bufferMemory (loanBuffer loan)) `shouldBe` memory
                    withForeignPtr (bufferMemory (loanBuffer loan)) $ \pointer -> peek pointer `shouldReturn` 73
                    pure loan
                )
              >>> release

      it "makes buffers of a byte, of the smallest counts" $ do
        pool <- newSizedPool 1 1 1 1
        run $
          request pool $
            (borrow &&& borrow)
              >>> now (\(primary, extra) -> map (bufferCapacity . loanBuffer) [primary, extra] `shouldBe` [1, 1])
              >>> arr (const ())

  describe "across cores" $ do
    it "gives the primary of a request that ends to a request that waits on another core" $ do
      ensureCores
      pool <- newTestPool 1 1 1
      journal <- newJournal
      admitted <- newSignal
      results <-
        timeout 5000000 $
          runCores 2 (pure ()) $ \() core ->
            if core == 0
              then request pool (raise admitted >>> awaitRequestWaiting pool 1 >>> note journal "first ends")
              else awaitSignal admitted >>> request pool (note journal "second is in")
      results `shouldBe` Just [(), ()]
      readJournal journal `shouldReturn` ["first ends", "second is in"]

    it "gives the buffer that a request returns to the pending extra loan of a request on another core" $ do
      ensureCores
      pool <- newTestPool 2 1 1
      journal <- newJournal
      aPrimary <- newSlot
      aExtra <- newSlot
      bPrimary <- newSlot
      bExtra <- newSlot
      aIn <- newSignal
      bHolds <- newSignal
      results <-
        timeout 5000000 $
          runCores 2 (pure ()) $ \() core ->
            if core == 0
              then
                request pool $
                  steps [awaitSignal aIn, borrowInto bPrimary, borrowInto bExtra, raise bHolds, awaitRequestWaiting pool 1, giveBack bExtra, giveBack bPrimary]
              else
                request pool $
                  steps [raise aIn, borrowInto aPrimary, awaitSignal bHolds, borrowInto aExtra, note journal "a has its extra loan", giveBack aExtra, giveBack aPrimary]
      results `shouldBe` Just [(), ()]
      readJournal journal `shouldReturn` ["a has its extra loan"]
      length <$> requestFree pool `shouldReturn` 3

  describe "the deadline of a pending extra loan, on one core, and the return that grants it, on another" $
    it "lose and duplicate no buffer, whichever comes first" $ do
      ensureCores
      pool <- newTestPool 2 1 1
      aPrimary <- newSlot
      bPrimary <- newSlot
      bExtra <- newSlot
      aIn <- newSignal
      turns <- forM [0 .. 1499 :: Int] $ \index -> (,,) index <$> newSignal <*> newSignal
      let deadline index = micros (fromIntegral (index `mod` 80))
      results <-
        timeout 60000000 $
          runCores 2 (pure ()) $ \() core ->
            if core == 0
              then
                request pool $
                  steps $
                    [borrowInto bPrimary, awaitSignal aIn]
                      ++ concat
                        [ [ proceeds (borrowInto bExtra),
                            raise started,
                            const () ^>> sleep (deadline index) >>> giveBack bExtra,
                            awaitSignal finished
                          ]
                          | (index, started, finished) <- turns
                        ]
              else
                request pool $
                  steps $
                    [borrowInto aPrimary, raise aIn]
                      ++ concat
                        [ [ awaitSignal started,
                            within (deadline index) (scope borrow (arr fst >>> release) blockForever) >>> arr (const ()),
                            raise finished
                          ]
                          | (index, started, finished) <- turns
                        ]
      results `shouldBe` Just [(), ()]
      checkInventory pool 3 1

  describe "many requests" $ do
    forM_ runners $ \runner@(Runner name _) ->
      it ("lose and duplicate no buffer, whatever fails, is cancelled or waits, with the parts of every request on the arrow " <> name) $ do
        pool <- newTestPool 6 2 3
        runOn runner (together [stress pool part | part <- [0 .. 15]])
        checkInventory pool 8 3

    it "do the same with the parts on 4 cores, which give each other the buffers" $ do
      ensureCores
      pool <- newTestPool 6 2 3
      results <- timeout 60000000 (runCores 4 (pure pool) (\shared core -> together [stress shared (core * 8 + part) | part <- [0 .. 7]]))
      results `shouldBe` Just (replicate 4 ())
      checkInventory pool 8 3

-- | What a part does in the stress: forty requests of every kind one after another on the pool. Some borrow and give
-- back, some fail, some are cancelled while they wait to be admitted or to borrow or while they hold their loans, some have
-- parts of their own that borrow, and some set up.
stress :: (Runtime arr) => BufferPool -> Int -> arr () ()
stress pool part = const part ^>> timesWith 40 turn >>> arr (const ())
  where
    turn = (\index -> (index `mod` 7, index + 1)) ^>> first (choose kinds) >>^ snd
    kinds =
      [ request pool useBoth,
        within (micros 60) (request pool useBoth) >>> arr (const ()),
        tryA (request pool ((borrow &&& borrow) >>> now (\_ -> ioError (userError "the script failed")))) >>> arr (const ()),
        within (micros 80) (withSetupBuffers @(Solo Buffer) pool (arr (const ()))) >>> arr (const ()),
        request pool (together [borrow >>> release, borrow >>> release]),
        within (micros 120) (request pool ((borrow &&& borrow) >>> blockForever)) >>> arr (const ()),
        withSetupBuffers @(Buffer, Buffer) pool (arr (const ()))
      ]

-- | A script that borrows twice, so that the request holds its primary and an extra loan, and keeps them.
borrowTwice :: (Runtime arr) => arr Loans ()
borrowTwice = steps [borrow >>> arr (const ()), borrow >>> arr (const ())]

-- | A script that borrows twice and gives both back.
useBoth :: (Runtime arr) => arr Loans ()
useBoth = (borrow &&& borrow) >>> (release *** release) >>> arr (const ())

-- | Every buffer of the pools is free, once each, and nobody waits.
checkInventory :: BufferPool -> Int -> Int -> IO ()
checkInventory pool requests setups = do
  requestFree pool >>= \free -> do
    length free `shouldBe` requests
    length (nub (map bufferMemory free)) `shouldBe` requests
  setupFree pool >>= \free -> do
    length free `shouldBe` setups
    length (nub (map bufferMemory free)) `shouldBe` setups
  requestWaiting pool `shouldReturn` 0
  setupWaiting pool `shouldReturn` 0
