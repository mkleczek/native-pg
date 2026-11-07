{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module ScopeSpec (spec) where

import Control.Concurrent (forkIO, threadDelay, throwTo)
import Control.Concurrent.MVar
import Control.Exception (ErrorCall (..), Exception, MaskingState (..), SomeException, getMaskingState, throwIO, try)
import Control.Monad (void)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Network.Streaming.Scope
import Skeletest
import qualified Skeletest.Predicate as P
import System.Timeout (timeout)

newtype Boom = Boom String deriving (Show, Eq)

instance Exception Boom

-- | The exception that ended a scope's body, or its releases.
thrown :: IO () -> IO (Either Boom ())
thrown = try

thrownClosed :: IO () -> IO (Either ScopeClosed ())
thrownClosed = try

-- | What the releases of a scope saw, in the order they ran.
type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

note :: Log -> String -> IO ()
note logRef entry = modifyIORef' logRef (entry :)

entries :: Log -> IO [String]
entries = fmap reverse . readIORef

-- | A resource that notes its release and how the scope ended.
resource :: Scope s -> Log -> String -> IO (Owned s String)
resource scope logRef name =
  acquireIn scope (note logRef ("acquire " <> name) *> pure name) $ \ending _ ->
    note logRef ("release " <> name <> " " <> ended ending)

ended :: Ending -> String
ended = \case
  Completed -> "completed"
  Aborted failure -> "aborted " <> show failure

spec :: Spec
spec = describe "a scope" $ do
  describe "releases" $ do
    xfail "scope: nothing is registered, so no release runs yet" $ it "run last in, first out when the body returns" $ do
      logRef <- newLog
      result <- withScope $ \scope -> do
        _ <- resource scope logRef "a"
        _ <- resource scope logRef "b"
        _ <- resource scope logRef "c"
        note logRef "body"
        pure (42 :: Int)
      result `shouldBe` 42
      entries logRef
        `shouldReturn` [ "acquire a", "acquire b", "acquire c", "body",
                         "release c completed", "release b completed", "release a completed"
                       ]

    xfail "scope: nothing is registered, so no release runs yet" $ it "run once each, and not before the scope closes" $ do
      counts <- newIORef (0 :: Int, 0 :: Int)
      withScope $ \scope -> do
        _ <- acquireIn scope (pure ()) (\_ _ -> modifyIORef' counts (\(first, second) -> (first + 1, second)))
        _ <- acquireIn scope (pure ()) (\_ _ -> modifyIORef' counts (\(first, second) -> (first, second + 1)))
        readIORef counts `shouldReturn` (0, 0)
      readIORef counts `shouldReturn` (1, 1)

    xfail "scope: nothing is registered, so no release runs yet" $ it "give the resource they release back to the release" $ do
      seen <- newIORef ([] :: [String])
      withScope $ \scope ->
        void $ acquireIn scope (pure "the resource") (\_ held -> modifyIORef' seen (held :))
      readIORef seen `shouldReturn` ["the resource"]

    xfail "scope: nothing is registered, so no release runs yet" $ it "run when the body throws, learn of the exception, and let it propagate" $ do
      logRef <- newLog
      result <- thrown $ withScope $ \scope -> do
        _ <- resource scope logRef "a"
        _ <- resource scope logRef "b"
        throwIO (Boom "body")
      result `shouldBe` Left (Boom "body")
      entries logRef
        `shouldReturn` [ "acquire a", "acquire b",
                         "release b aborted Boom \"body\"", "release a aborted Boom \"body\""
                       ]

    xfail "scope: nothing is registered, so no release runs yet" $ it "run when the body is interrupted, and learn of it" $ do
      logRef <- newLog
      inside <- newEmptyMVar
      finished <- newEmptyMVar
      worker <- forkIO $
        try @SomeException
          ( withScope $ \scope -> do
              _ <- resource scope logRef "a"
              putMVar inside ()
              threadDelay 10000000
          )
          >>= putMVar finished
      takeMVar inside
      throwTo worker (ErrorCall "interrupted")
      timeout 2000000 (takeMVar finished) >>= \outcome ->
        fmap (either show (const "returned")) outcome `shouldBe` Just "interrupted"
      entries logRef `shouldReturn` ["acquire a", "release a aborted interrupted"]

    xfail "scope: nothing is registered, so no release runs yet" $ it "run masked" $ do
      states <- newIORef []
      withScope $ \scope ->
        void $ acquireIn scope getMaskingState (\_ _ -> getMaskingState >>= \state -> modifyIORef' states (state :))
      readIORef states `shouldReturn` [MaskedInterruptible]

    xfail "scope: nothing is registered, so no release runs yet" $ it "all run when one of them throws, and the last failure is the one that propagates" $ do
      logRef <- newLog
      result <- thrown $ withScope $ \scope -> do
        _ <- resource scope logRef "a"
        _ <- acquireIn scope (pure ()) (\ending _ -> note logRef ("release b " <> ended ending) *> throwIO (Boom "b"))
        _ <- resource scope logRef "c"
        _ <- acquireIn scope (pure ()) (\ending _ -> note logRef ("release d " <> ended ending) *> throwIO (Boom "d"))
        pure ()
      result `shouldBe` Left (Boom "b")
      entries logRef
        `shouldReturn` [ "acquire a", "acquire c",
                         "release d completed",
                         "release c aborted Boom \"d\"",
                         "release b aborted Boom \"d\"",
                         "release a aborted Boom \"b\""
                       ]

    xfail "scope: nothing is registered, so no release runs yet" $ it "replace the body's exception with the failure of a release, as a bracket's do" $ do
      result <- thrown $ withScope $ \scope -> do
        _ <- acquireIn scope (pure ()) (\_ _ -> throwIO (Boom "release"))
        throwIO (Boom "body")
      result `shouldBe` Left (Boom "release")

    xfail "scope: nothing is registered, so no release runs yet" $ it "tell the releases after a failed one that the scope aborted, though its body returned" $ do
      logRef <- newLog
      result <- thrown $ withScope $ \scope -> do
        _ <- resource scope logRef "a"
        _ <- acquireIn scope (pure ()) (\_ _ -> throwIO (Boom "b"))
        pure ()
      result `shouldBe` Left (Boom "b")
      entries logRef `shouldReturn` ["acquire a", "release a aborted Boom \"b\""]

    xfail "scope: nothing is registered, so no release runs yet" $ it "see an asynchronous exception that arrives while one runs, and still all run" $ do
      logRef <- newLog
      releasing <- newEmptyMVar
      finished <- newEmptyMVar
      worker <- forkIO $
        try @SomeException
          ( withScope $ \scope -> do
              _ <- resource scope logRef "a"
              -- An interruptible wait inside a release, the way a release may wait
              _ <- acquireIn scope (pure ()) (\_ _ -> putMVar releasing () *> threadDelay 10000000)
              pure ()
          )
          >>= putMVar finished
      takeMVar releasing
      throwTo worker (ErrorCall "during release")
      timeout 2000000 (takeMVar finished) >>= \outcome ->
        fmap (either show (const "returned")) outcome `shouldBe` Just "during release"
      entries logRef `shouldReturn` ["acquire a", "release a aborted during release"]

  describe "acquisitions" $ do
    xfail "scope: an acquisition is not masked yet" $ it "run masked" $ do
      states <- newIORef []
      withScope $ \scope ->
        void $ acquireIn scope (getMaskingState >>= \state -> modifyIORef' states (state :)) (\_ _ -> pure ())
      readIORef states `shouldReturn` [MaskedInterruptible]

    xfail "scope: nothing is registered, so no release runs yet" $ it "register nothing when the acquisition throws, and the releases before it still run" $ do
      logRef <- newLog
      result <- thrown $ withScope $ \scope -> do
        _ <- resource scope logRef "a"
        _ <- acquireIn scope (throwIO (Boom "acquire")) (\_ () -> note logRef "release failed acquisition")
        note logRef "not reached"
      result `shouldBe` Left (Boom "acquire")
      entries logRef `shouldReturn` ["acquire a", "release a aborted Boom \"acquire\""]

    xfail "scope: nothing is registered, so no release runs yet" $ it "register nothing when interrupted while blocked, and the releases before it still run" $ do
      logRef <- newLog
      blocked <- newEmptyMVar
      finished <- newEmptyMVar
      worker <- forkIO $
        try @SomeException
          ( withScope $ \scope -> do
              _ <- resource scope logRef "a"
              void $ acquireIn scope (putMVar blocked () *> threadDelay 10000000) (\_ _ -> note logRef "release interrupted")
          )
          >>= putMVar finished
      takeMVar blocked
      throwTo worker (ErrorCall "during acquisition")
      timeout 2000000 (takeMVar finished) >>= \outcome ->
        fmap (either show (const "returned")) outcome `shouldBe` Just "during acquisition"
      entries logRef `shouldReturn` ["acquire a", "release a aborted during acquisition"]

    xfail "scope: nothing is registered, so no release runs yet" $ it "fail in a scope that has closed, without acquiring" $ do
      logRef <- newLog
      result <- thrownClosed $ withScope $ \scope -> do
        _ <- acquireIn scope (pure ()) $ \_ _ -> do
          -- Runs while the scope closes
          void $ resource scope logRef "late"
        pure ()
      result `shouldSatisfy` P.left P.anything
      entries logRef `shouldReturn` []

  describe "withScope" $ do
    it "runs a body that acquires nothing" $
      withScope (\_ -> pure 'x') `shouldReturn` 'x'

    xfail "scope: nothing is registered, so no release runs yet" $ it "gives each scope its own releases" $ do
      logRef <- newLog
      withScope $ \outer -> do
        _ <- resource outer logRef "outer"
        withScope $ \inner -> void (resource inner logRef "inner")
        note logRef "between"
      entries logRef
        `shouldReturn` [ "acquire outer", "acquire inner", "release inner completed", "between",
                         "release outer completed"
                       ]
