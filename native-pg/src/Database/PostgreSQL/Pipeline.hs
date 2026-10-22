{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Pipelines, with the names and shape of hasql's: statements sent to the
-- server together, before one Sync, whose results are read in order. That's
-- one round trip for all of them.
--
-- A pipeline is applicative, not a monad: no statement can depend on the
-- result of another, which is what lets them be sent together. After a
-- statement fails on the server, the server skips the ones after it. A
-- result that fails to decode fails the pipeline too, but the results after
-- it are still read.
module Database.PostgreSQL.Pipeline
  ( Pipeline,
    statement,
    runPipeline,
    runPipelineWithStatus,
    runPipelineIn,
    runPipelineWithStatusIn,

    -- * Prepared statements
    Prepare,
    Prepared (..),
  )
where

import Control.Applicative ((<|>))
import Control.Monad (join)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.Word (Word32)
import Data.Functor ((<&>))
import Data.Maybe (fromMaybe)
import Database.PostgreSQL.Protocol (FormatCode (..), TransactionStatus)
import Database.PostgreSQL.Session
  ( Batch,
    Execution (..),
    Phase (Ready),
    Session,
    batched,
    runBatch,
  )
import qualified Database.PostgreSQL.Session as Session
import Database.PostgreSQL.Statement

-- | Statements to send together. Building it looks up the statements that
-- may be prepared, so it runs in 'IO'.
newtype Pipeline a = Pipeline (Maybe Prepare -> IO (Planned a))

-- | The statements, and the SQL of the last one, to which an error after
-- them is attributed.
data Planned a = Planned (Maybe ByteString) (Batch QueryError (Either QueryError a))

instance Functor Planned where
  fmap f (Planned final batch) = Planned final (fmap (fmap f) batch)

instance Functor Pipeline where
  fmap f (Pipeline plan) = Pipeline (fmap (fmap f) . plan)

instance Applicative Pipeline where
  pure value = Pipeline (\_ -> pure (Planned Nothing (pure (Right value))))
  Pipeline left <*> Pipeline right = Pipeline $ \prepare -> do
    Planned leftFinal leftBatch <- left prepare
    Planned rightFinal rightBatch <- right prepare
    pure (Planned (rightFinal <|> leftFinal) ((<*>) <$> leftBatch <*> rightBatch))

-- | Gives how a statement runs prepared, from its SQL and its parameters'
-- types, e.g. from a statement cache.
type Prepare = ByteString -> [Maybe Word32] -> IO Prepared

data Prepared
  = -- | It's prepared under the name already.
    PreparedAs ByteString
  | -- | Prepared under the name as it runs. The action runs once the server
    -- has parsed it, even if running it fails, e.g. to remember that it's
    -- prepared.
    PrepareAs ByteString (IO ())

-- | The statement with the parameters, and its result.
statement :: params -> Statement params result -> Pipeline result
statement params (Statement query encode (Result initial step finish) preparable) = Pipeline $ \prepare -> do
  let parameters = encode params
      types = paramType <$> parameters
  executed <- case prepare of
    Just prepared
      | preparable ->
          prepared query types <&> \case
            PreparedAs name -> Session.Prepared name
            PrepareAs name parsed -> Session.Preparing name query types parsed
    _ -> pure (Session.Unprepared query types)
  let execution =
        Execution
          { executionStatement = executed,
            executionParameterFormats = paramFormat <$> parameters,
            executionParameters = paramValue <$> parameters,
            -- The decoders read the binary formats.
            executionResultFormats = [BinaryFormat]
          }
      folding (Left failure) _ = pure (Left failure)
      folding (Right accumulated) current = step accumulated current
      outcome (accumulated, _) = first (QueryError query) (either (Left . RowFailed) finish accumulated)
  pure . Planned (Just query) $ outcome <$> batched (QueryError query . ServerFailed) execution folding (Right initial)

-- | Runs the pipeline on the session. Statements that may be prepared run as
-- the function gives, if there's one, and otherwise unnamed.
runPipeline :: Maybe Prepare -> Pipeline a -> Session 'Ready -> IO (Either QueryError a)
runPipeline prepare statements session = fst <$> runPipelineWithStatus prepare statements session

-- | Like 'runPipeline', with the transaction status after the statements.
runPipelineWithStatus :: Maybe Prepare -> Pipeline a -> Session 'Ready -> IO (Either QueryError a, TransactionStatus)
runPipelineWithStatus prepare (Pipeline plan) session = do
  Planned final batch <- plan prepare
  first join <$> runBatch session (QueryError (fromMaybe "" final) . ServerFailed) batch

-- | Runs a pipeline using an explicitly scoped operation workspace.
runPipelineIn :: Session.Context -> Maybe Prepare -> Pipeline a -> IO (Either QueryError a)
runPipelineIn context prepare pipeline = runPipeline prepare pipeline (Session.sessionFromContext context)

runPipelineWithStatusIn :: Session.Context -> Maybe Prepare -> Pipeline a -> IO (Either QueryError a, TransactionStatus)
runPipelineWithStatusIn context prepare pipeline = runPipelineWithStatus prepare pipeline (Session.sessionFromContext context)
