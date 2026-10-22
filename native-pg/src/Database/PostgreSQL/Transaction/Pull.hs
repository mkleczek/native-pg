{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
module Database.PostgreSQL.Transaction.Pull
  ( Transaction, Pipeline, Result, Field, Statement (..), RowDecoder,
    runTransaction, runTransactionWith, statement, pipeline, finalPipeline,
    nextRow, fieldLength, nextChunk, finishTransaction, condemn, sql,
    row, streamingRow, singleRow, rowMaybe, noResult,
    foldRows, rowList, singleRowValue, rowMaybeValue,
    FormatCode (..), IsolationLevel (..), Mode (..), Param (..), Prepare, Prepared (..),
  ) where
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.IO.Unlift (MonadUnliftIO (..))
import Data.ByteString (ByteString)
import Data.Functor.Const (Const)
import Data.Kind (Type)
import Database.PostgreSQL.Types (FormatCode (..))
import Database.PostgreSQL.Session (Session, Phase (Ready), DecodeRow (..))
import Database.PostgreSQL.Statement (Param (..))
import Database.PostgreSQL.Pipeline (Prepare, Prepared (..))
import Database.PostgreSQL.Transaction (IsolationLevel (..), Mode (..))

data Transaction (tx :: Type) a
instance Functor (Transaction tx) where fmap = error "pull cursor scaffold"
instance Applicative (Transaction tx) where
  pure = error "pull cursor scaffold"
  (<*>) = error "pull cursor scaffold"
instance Monad (Transaction tx) where (>>=) = error "pull cursor scaffold"
instance MonadIO (Transaction tx) where liftIO = error "pull cursor scaffold"
instance MonadUnliftIO (Transaction tx) where withRunInIO = error "pull cursor scaffold"
data Pipeline (tx :: Type) a
instance Functor (Pipeline tx) where fmap = error "pull cursor scaffold"
instance Applicative (Pipeline tx) where
  pure = error "pull cursor scaffold"
  (<*>) = error "pull cursor scaffold"
data Result (tx :: Type) (row :: Type -> Type)
data Field (tx :: Type)
data RowDecoder (row :: Type -> Type)
data Statement params row = Statement ByteString (params -> [Param]) (RowDecoder row) Bool
runTransaction :: IsolationLevel -> Mode -> Session 'Ready -> (forall tx. Transaction tx a) -> IO a
runTransaction _ _ _ _ = error "pull cursor scaffold"
runTransactionWith :: Prepare -> IsolationLevel -> Mode -> Session 'Ready -> (forall tx. Transaction tx a) -> IO a
runTransactionWith _ _ _ _ _ = error "pull cursor scaffold"
statement :: params -> Statement params row -> Pipeline tx (Result tx row)
statement = error "pull cursor scaffold"
pipeline, finalPipeline :: Pipeline tx a -> Transaction tx a
pipeline = error "pull cursor scaffold"
finalPipeline = error "pull cursor scaffold"
nextRow :: Result tx row -> Transaction tx (Maybe (row tx))
nextRow = error "pull cursor scaffold"
fieldLength :: Field tx -> Int
fieldLength = error "pull cursor scaffold"
nextChunk :: Field tx -> Transaction tx ByteString
nextChunk = error "pull cursor scaffold"
finishTransaction, condemn :: Transaction tx ()
finishTransaction = error "pull cursor scaffold"
condemn = error "pull cursor scaffold"
sql :: ByteString -> Transaction tx ()
sql = error "pull cursor scaffold"
row :: forall columns row. DecodeRow columns => (forall tx. RowFunction columns (row tx)) -> RowDecoder row
row _ = error "pull cursor scaffold"
streamingRow :: forall columns row. DecodeRow columns => (forall tx. RowFunction columns (Maybe (Field tx) -> row tx)) -> RowDecoder row
streamingRow _ = error "pull cursor scaffold"
singleRow, rowMaybe :: RowDecoder row -> RowDecoder row
singleRow = error "pull cursor scaffold"
rowMaybe = error "pull cursor scaffold"
noResult :: RowDecoder (Const ())
noResult = error "pull cursor scaffold"

foldRows :: (acc -> row tx -> Transaction tx acc) -> acc -> Result tx row -> Transaction tx acc
foldRows = error "pull cursor scaffold"
rowList :: Result tx (Const a) -> Transaction tx [a]
rowList = error "pull cursor scaffold"
singleRowValue :: Result tx (Const a) -> Transaction tx a
singleRowValue = error "pull cursor scaffold"
rowMaybeValue :: Result tx (Const a) -> Transaction tx (Maybe a)
rowMaybeValue = error "pull cursor scaffold"
