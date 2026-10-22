{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Statements, with the names and shape of hasql's: SQL, its parameters, and
-- how its result is read, by typed row decoders or by a fold over its rows.
--
-- A result can also stream. 'foldRows' gives each row to a step whose type
-- has a scope variable, as 'runST' does, so the row and the streams taken
-- from it can't leave the step.
module Database.PostgreSQL.Statement
  ( -- * Statements
    Statement (..),
    Param (..),
    QueryError (..),
    StatementError (..),

    -- * Results
    Result (..),
    RowDecoder,
    row,
    noResult,
    singleRow,
    rowMaybe,
    rowList,

    -- * Streaming
    ScopedRow,
    ScopedStream,
    foldRows,
    scopedRow,
    decodeScoped,
    decodeScopedStreaming,
    decodeScopedStreamingNullable,
    streamLength,
    streamChunk,
    streamChunks,
  )
where

import Control.Exception (Exception)
import Control.Monad (join)
import Data.ByteString (ByteString)
import Database.PostgreSQL.Decoding (DecodingError (..))
import Database.PostgreSQL.Protocol (FormatCode (..), OptionalOid, Parameter)
import Database.PostgreSQL.Protocol.Codec (ByteStream (..))
import Database.PostgreSQL.Session
  ( DecodeRow (RowFunction, mapRowFunction),
    Row,
    ServerError,
    decodeRow,
    columnStreamWithChunks,
    decodeRowStreamingUsing,
    rowColumnCount,
  )

-- | A statement: its SQL, its parameters, and how its result is read.
data Statement params result = Statement
  { statementSql :: ByteString,
    statementParams :: params -> [Param],
    statementResult :: Result result,
    -- | Whether it may run as a prepared statement, with
    -- 'transactionNoRetryWith'. Otherwise it runs unnamed.
    statementPreparable :: Bool
  }

-- | A parameter.
data Param = Param
  { -- | 'Nothing' lets the server infer the type.
    paramType :: OptionalOid,
    paramFormat :: FormatCode,
    -- | 'Nothing' is NULL.
    paramValue :: Maybe Parameter
  }

-- | A statement failed: its SQL, and why.
data QueryError = QueryError ByteString StatementError
  deriving (Eq, Show)

instance Exception QueryError

data StatementError
  = -- | The server reported an error.
    ServerFailed ServerError
  | -- | A row couldn't be decoded.
    RowFailed DecodingError
  | -- | The result had this many rows, which its shape doesn't allow.
    UnexpectedAmountOfRows Int
  deriving (Eq, Show)

-- | How the rows of a result become its value: a fold over them, and a last
-- step that can reject the result.
data Result a = forall accumulated. Result accumulated (accumulated -> Row -> IO (Either DecodingError accumulated)) (accumulated -> Either StatementError a)

instance Functor Result where
  fmap f (Result initial step finish) = Result initial step (fmap f . finish)

-- | Decodes a row into a value.
newtype RowDecoder a = RowDecoder (Row -> IO (Either DecodingError a))

instance Functor RowDecoder where
  fmap f (RowDecoder decode) = RowDecoder (fmap (fmap f) . decode)

-- | Decodes a row with the columns listed, applying a function to their
-- values, e.g. @row \@'[Int64, Text] Item@.
row :: forall columns a. DecodeRow columns => RowFunction columns a -> RowDecoder a
row function = RowDecoder (\current -> decodeRow @columns current (mapRowFunction @columns @a @(IO a) pure function))
{-# INLINE row #-}

-- | Skips the rows, if there are any.
noResult :: Result ()
noResult = Result () (\_ _ -> pure (Right ())) Right

-- | Exactly one row.
singleRow :: RowDecoder a -> Result a
singleRow decoder = Result (0, Nothing) (firstRow decoder) $ \case
  (1, Just value) -> Right value
  (count, _) -> Left (UnexpectedAmountOfRows count)

-- | No row, or one.
rowMaybe :: RowDecoder a -> Result (Maybe a)
rowMaybe decoder = Result (0, Nothing) (firstRow decoder) $ \case
  (count, value) | count <= 1 -> Right value
  (count, _) -> Left (UnexpectedAmountOfRows count)

-- | Counts the rows, and decodes only the first.
firstRow :: RowDecoder a -> (Int, Maybe a) -> Row -> IO (Either DecodingError (Int, Maybe a))
firstRow (RowDecoder decode) (count, Nothing) current = fmap (\value -> (count + 1, Just value)) <$> decode current
firstRow _ (count, value) _ = pure (Right (count + 1, value))

-- | All the rows.
rowList :: RowDecoder a -> Result [a]
rowList (RowDecoder decode) = Result [] step (Right . reverse)
  where
    step values current = fmap (: values) <$> decode current

-- | A row that can be read only within the step it was given to.
newtype ScopedRow s = ScopedRow Row

-- | A column stream that can be read only within that step.
data ScopedStream s = ScopedStream ByteStream ((ByteString -> IO ()) -> IO ())

-- | Folds the rows with the step. The rows, and the streams taken from them,
-- have the step's scope in their type, which the result can't mention, so
-- they can't outlive the step.
foldRows :: (forall s. accumulated -> ScopedRow s -> IO accumulated) -> accumulated -> Result accumulated
foldRows step initial = Result initial (\accumulated current -> Right <$> step accumulated (ScopedRow current)) Right

-- | A row decoder from a step within the row's scope, e.g. to stream a column
-- of a 'singleRow'. The result can't mention the scope.
scopedRow :: (forall s. ScopedRow s -> IO (Either DecodingError a)) -> RowDecoder a
scopedRow decode = RowDecoder (decode . ScopedRow)

-- | Like 'row', within a step.
decodeScoped :: forall columns s r. DecodeRow columns => ScopedRow s -> RowFunction columns (IO r) -> IO (Either DecodingError r)
decodeScoped (ScopedRow current) = decodeRow @columns current
{-# INLINE decodeScoped #-}

-- | Decodes the columns listed, and streams one more, the last, to the
-- function.
decodeScopedStreaming :: forall columns s r. DecodeRow columns => ScopedRow s -> RowFunction columns (ScopedStream s -> IO r) -> IO (Either DecodingError r)
decodeScopedStreaming (ScopedRow current) function =
  join <$> decodeScopedStreamingNullable @columns @s @(Either DecodingError r) (ScopedRow current)
    (mapRowFunction @columns @(ScopedStream s -> IO r) @(Maybe (ScopedStream s) -> IO (Either DecodingError r)) streamOrFail function)
  where
    streamOrFail continue = maybe
      (pure (Left (ColumnDecodingFailed (rowColumnCount current) "unexpected NULL in a streamed column")))
      (fmap Right . continue)
{-# INLINE decodeScopedStreaming #-}

-- | Like 'decodeScopedStreaming', but the streamed column may be NULL, which
-- the function gets as 'Nothing'.
decodeScopedStreamingNullable :: forall columns s r. DecodeRow columns => ScopedRow s -> RowFunction columns (Maybe (ScopedStream s) -> IO r) -> IO (Either DecodingError r)
decodeScopedStreamingNullable (ScopedRow current) function =
  decodeRowStreamingUsing @columns @(ScopedStream s) @r (fmap (uncurry ScopedStream) . columnStreamWithChunks) current function
{-# INLINE decodeScopedStreamingNullable #-}

-- | The stream's length in bytes.
streamLength :: ScopedStream s -> Int
streamLength (ScopedStream stream _) = byteStreamLength stream

-- | The next chunk of the stream, or an empty one at its end.
streamChunk :: ScopedStream s -> IO ByteString
streamChunk (ScopedStream stream _) = byteStreamNext stream

-- | Consumes the remaining stream through borrowed chunks. A chunk is valid
-- only during its callback; copy it if the consumer needs to retain it.
-- Field completion does not establish statement or transaction success.
streamChunks :: ScopedStream s -> (ByteString -> IO ()) -> IO ()
streamChunks (ScopedStream _ consume) = consume
