{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Private row cursors, borrowed column consumption and row decoding.
module Database.PostgreSQL.Internal.Row
  ( Row, Column (..), DecodeRow (..),
    readRows, nextRow, startRow, finishRow, invalidateRow,
    rowColumnCount, rowValueUnread, rowUnreadBytes, nextColumn, columnBytes,
    columnBorrowedStream, decodeRow, decodeRowStreamingUsing,
  ) where

import Control.Exception (SomeException, catch, fromException, onException, throwIO, toException)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int32)
import Data.Kind (Type)
import Database.PostgreSQL.Decoding (DecodeField (..), DecodingError (..))
import Database.PostgreSQL.Diagnostics (ServerError)
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Internal.Exchange
import Database.PostgreSQL.Internal.Protocol.Codec
  ( Alternatives (..), ByteStream (..), Input, InputError (..), Parsable (..),
    failInput, inputPosition, readInputSome, readInputBorrowedSome, skipInput,
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Internal.Protocol.Messages
import Network.Streaming.Watch (Watch)

data RowsMessage
  = RowStarted Int32 Int
  | Completed ByteString

readRows :: Session 'Rows -> (acc -> Row -> IO acc) -> acc -> IO (Either ServerError (acc, ByteString))
readRows session@(Session context) step = go
  where
    go acc = do
      message <-
        pullEither session $
          (\(DataRowHeader rowLength columnCount) -> Reply (RowStarted rowLength (fromIntegral columnCount)))
            :|: (\(CommandComplete tag) -> Reply (Completed (BS.copy tag)))
            :|: (\EmptyQueryResponse -> Reply (Completed ""))
            :|: (\PortalSuspended -> Reply (Completed ""))
            :|: sessionTail
      case message of
        Left failure -> pure (Left failure)
        Right (Completed tag) -> pure (Right (acc, tag))
        Right (RowStarted rowLength columnCount) -> do
          row <- startRow context rowLength columnCount
          acc' <- step acc row `onException` invalidateRow row
          finishRow row
          go acc'

nextRow :: Session 'Rows -> IO (Either ServerError (Maybe Row))
nextRow session@(Session context) = do
  message <- pullEither session $
    (\(DataRowHeader size columns) -> Reply (RowStarted size (fromIntegral columns)))
      :|: (\(CommandComplete (_ :: ByteString)) -> Reply (Completed BS.empty))
      :|: (\EmptyQueryResponse -> Reply (Completed BS.empty))
      :|: (\PortalSuspended -> Reply (Completed BS.empty))
      :|: sessionTail
  case message of
    Left failure -> pure (Left failure)
    Right (Completed _) -> pure (Right Nothing)
    Right (RowStarted size columns) -> Right . Just <$> startRow context size columns

-- | A row, whose columns are pulled in order. It's valid only during the step
-- it's given to.
data Row = Row
  { rowContext :: Context,
    rowColumnCount :: {-# UNPACK #-} !Int,
    -- | The position where the row ends.
    rowEnd :: {-# UNPACK #-} !Int,
    rowCursor :: IORef RowCursor
  }

data RowCursor = RowCursor
  { cursorActive :: !Bool,
    -- | How many columns were pulled.
    cursorColumn :: !Int,
    -- | Unread bytes of the current column's value.
    cursorValueLeft :: !Int,
    -- | Whether the current column's value is being read already.
    cursorValueTaken :: !Bool,
    -- | A synchronous borrowed consumer owns the column.
    cursorBorrowed :: !Bool
  }

data Column
  = NullColumn
  | -- | A value of this many bytes, to take with 'columnBytes' or
    -- 'columnBorrowedStream'.
    Column Int
  deriving (Eq, Show)

startRow :: Context -> Int32 -> Int -> IO Row
startRow context rowLength columnCount = do
  unless (rowLength >= 6 && columnCount >= 0) $
    throwIO (ProtocolError ("invalid DataRow length " <> show rowLength <> " or column count " <> show columnCount))
  position <- inputPosition <$> readIORef (contextInput context)
  cursor <- newIORef (RowCursor True 0 0 False False)
  pure (Row context columnCount (position + fromIntegral rowLength - 6) cursor)

-- | Skips what the step left unread, so the next message can be read.
finishRow :: Row -> IO ()
finishRow row = do
  void (activeCursor row)
  writeIORef (rowCursor row) (RowCursor False 0 0 True False)
  position <- inputPosition <$> readIORef (contextInput (rowContext row))
  inRow row (skipInput (rowEnd row - position))

-- | Whether the row's current value was taken, to be read by pulls, and has
-- bytes left. A row that was finished has none.
rowValueUnread :: Row -> IO Bool
rowValueUnread row = do
  cursor <- readIORef (rowCursor row)
  pure (cursorActive cursor && cursorValueTaken cursor && cursorValueLeft cursor > 0)

-- | How many bytes of the row nothing has consumed: what the columns have left
-- and the columns that weren't pulled. None for a row that was finished.
rowUnreadBytes :: Row -> IO Int
rowUnreadBytes row = do
  cursor <- readIORef (rowCursor row)
  if cursorActive cursor
    then (rowEnd row -) . inputPosition <$> readIORef (contextInput (rowContext row))
    else pure 0

-- | Pulls the next column, skipping what's left unread of the current one.
-- 'Nothing' after the last column.
nextColumn :: Row -> IO (Maybe Column)
nextColumn row = do
  cursor <- activeCursor row
  when (cursorValueLeft cursor > 0) $ inRow row (skipInput (cursorValueLeft cursor))
  if cursorColumn cursor == rowColumnCount row
    then do
      writeIORef (rowCursor row) cursor {cursorValueLeft = 0, cursorValueTaken = True}
      pure Nothing
    else do
      size <- inRow row (fieldParser @Int32 @('FixedBytesAtom 4))
      let column = cursorColumn cursor + 1
      case compare size (-1) of
        EQ -> do
          writeIORef (rowCursor row) (RowCursor True column 0 True False)
          pure (Just NullColumn)
        GT -> do
          writeIORef (rowCursor row) (RowCursor True column (fromIntegral size) False False)
          pure (Just (Column (fromIntegral size)))
        LT -> throwIO (ProtocolError ("invalid column length " <> show size))

-- | Materializes the current column into an owned value that remains valid
-- across later input reads. May allocate memory proportional to the field;
-- use 'columnBorrowedStream' for borrowed body transfer.
columnBytes :: Row -> IO ByteString
columnBytes row = do
  cursor <- untakenCursor row
  value <- inRow row (readOwnedBytes (cursorValueLeft cursor))
  writeIORef (rowCursor row) cursor {cursorValueLeft = 0, cursorValueTaken = True}
  pure value

-- | The current column's value as chunks borrowed until the next pull, with
-- the pull that waits in place with the wait it is given, and gives 'Nothing'
-- at the column's end. Both expire when the row or its column advances.
columnBorrowedStream :: Row -> IO (ByteStream, (IO Watch -> IO ()) -> IO (Maybe ByteString))
columnBorrowedStream row = do
  cursor <- untakenCursor row
  writeIORef (rowCursor row) cursor {cursorValueTaken = True}
  let current = do
        now <- activeCursor row
        unless (cursorColumn now == cursorColumn cursor) $
          throwIO (RowMisuse "a field was read after the next column was pulled")
        pure now
      next = do
        now <- current
        if cursorValueLeft now == 0
          then pure BS.empty
          else do
            chunk <- inRow row (readInputBorrowedSome (cursorValueLeft now))
            writeIORef (rowCursor row) now {cursorValueLeft = cursorValueLeft now - BS.length chunk}
            pure chunk
      waiting wait = do
        now <- current
        if cursorValueLeft now == 0
          then pure Nothing
          else do
            chunk <- runContextBorrowWaiting (rowContext row) wait (cursorValueLeft now) `catch` failedRead row
            writeIORef (rowCursor row) now {cursorValueLeft = cursorValueLeft now - BS.length chunk}
            pure (Just chunk)
  pure (ByteStream (cursorValueLeft cursor) next, waiting)

-- | The failure of a read of the column: the row is invalid, since input may
-- have been refilled, and an error of the input is a failure of the protocol.
failedRead :: Row -> SomeException -> IO a
failedRead row failure = do
  invalidateRow row
  throwIO $ case fromException failure of
    Just (InputError reason) -> toException (ProtocolError reason)
    Nothing -> failure

-- Explicit materializers own their results separately from the reusable
-- body workspace. Each copied chunk is stable before the next refill.
readOwnedBytes :: Int -> Input ByteString
readOwnedBytes count = gather count []
  where
    gather remaining chunks
      | remaining == 0 = pure (BS.concat (reverse chunks))
      | otherwise = do
          chunk <- readInputSome remaining
          gather (remaining - BS.length chunk) (chunk : chunks)

readOwnedNullable :: Input (Maybe ByteString)
readOwnedNullable = do
  size <- fieldParser @Int32 @('FixedBytesAtom 4)
  case compare size (-1) of
    EQ -> pure Nothing
    GT -> Just <$> readOwnedBytes (fromIntegral size)
    LT -> failInput ("negative length " <> show size <> " is invalid for MaybeBytes32Atom")

-- | A row's columns, described by the Haskell types they decode to.
class DecodeRow (columns :: [Type]) where
  -- | A function from the columns' values, in order, to @final@.
  type RowFunction columns (final :: Type) :: Type

  -- | How many columns there are.
  rowArity :: Int

  -- | Decodes the columns, numbered from the given one, and applies the
  -- function to them.
  applyColumns :: Int -> RowFunction columns final -> Input (Either DecodingError final)

  -- | Applies a function to what the row function finally gives, e.g. to
  -- make a pure constructor of the columns' values give an action.
  mapRowFunction :: (final -> final') -> RowFunction columns final -> RowFunction columns final'

instance DecodeRow '[] where
  type RowFunction '[] final = final
  rowArity = 0
  {-# INLINE rowArity #-}
  applyColumns _ final = pure (Right final)
  {-# INLINE applyColumns #-}
  mapRowFunction f = f
  {-# INLINE mapRowFunction #-}

instance (DecodeField a, DecodeRow columns) => DecodeRow (a ': columns) where
  type RowFunction (a ': columns) final = a -> RowFunction columns final
  rowArity = 1 + rowArity @columns
  {-# INLINE rowArity #-}
  mapRowFunction f function = mapRowFunction @columns f . function
  {-# INLINE mapRowFunction #-}
  applyColumns column function = do
    value <- readOwnedNullable
    case decodeField value of
      Left message -> pure (Left (ColumnDecodingFailed column message))
      Right decoded -> applyColumns @columns (column + 1) (function decoded)
  {-# INLINE applyColumns #-}

-- | Decodes the row's columns, as listed by their types, e.g.
-- @decodeRow \@'[Int32, Text] row (\\count name -> ...)@, and runs the
-- action the function returns. The row has to have exactly these columns,
-- and none of them may have been pulled. The columns are decoded in one pass
-- over the input. Materialized fields are owned and may allocate their full
-- size; use a streaming decoder for a borrowed body column.
decodeRow :: forall columns r. DecodeRow columns => Row -> RowFunction columns (IO r) -> IO (Either DecodingError r)
decodeRow row function = do
  started <- startDecoding row (rowArity @columns)
  case started of
    Left failure -> pure (Left failure)
    Right () -> do
      decoded <- inRow row (applyColumns @columns 1 function)
      writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True False)
      either (pure . Left) (fmap Right) decoded
-- Inlined so that each call site unrolls the decoding of its own columns.
{-# INLINE decodeRow #-}

-- | Chooses how the final column is consumed while its cursor is unclaimed.
decodeRowStreamingUsing :: forall columns stream r. DecodeRow columns => (Row -> IO stream) -> Row -> RowFunction columns (Maybe stream -> IO r) -> IO (Either DecodingError r)
decodeRowStreamingUsing makeStream row function = do
  let streamed = rowArity @columns + 1
  started <- startDecoding row streamed
  case started of
    Left failure -> pure (Left failure)
    Right () -> do
      decoded <- inRow row $ do
        applied <- applyColumns @columns 1 function
        case applied of
          Left failure -> pure (Left failure)
          Right continue -> do
            size <- fieldParser @Int32 @('FixedBytesAtom 4)
            pure (Right (fromIntegral size, continue))
      case decoded of
        Left failure -> do
          writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True False)
          pure (Left failure)
        Right (size, continue)
          | size < 0 -> do
              writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) 0 True False)
              Right <$> continue Nothing
          | otherwise -> do
              writeIORef (rowCursor row) (RowCursor True (rowColumnCount row) size False False)
              stream <- makeStream row
              Right <$> continue (Just stream)
{-# INLINE decodeRowStreamingUsing #-}

-- | Checks that no column of the row has been pulled yet, and that it has
-- the expected number of columns.
startDecoding :: Row -> Int -> IO (Either DecodingError ())
startDecoding row expected = do
  cursor <- activeCursor row
  unless (cursorColumn cursor == 0) $
    throwIO (RowMisuse "a row was decoded after some of its columns were pulled")
  pure $
    if expected == rowColumnCount row
      then Right ()
      else Left (ColumnCountMismatch expected (rowColumnCount row))

activeCursor :: Row -> IO RowCursor
activeCursor row = do
  ensureContextActive (rowContext row)
  cursor <- readIORef (rowCursor row)
  unless (cursorActive cursor) $ throwIO (RowMisuse "a row was used after its step returned or a read failed")
  when (cursorBorrowed cursor) $ throwIO (RowMisuse "a row was used during its column callback")
  pure cursor

untakenCursor :: Row -> IO RowCursor
untakenCursor row = do
  cursor <- activeCursor row
  when (cursorValueTaken cursor) $
    throwIO (RowMisuse "a column value was read twice, or before its column was pulled")
  pure cursor

inRow :: Row -> Input a -> IO a
inRow row parse = runOn (rowContext row) (rowEnd row) parse `onException` invalidateRow row

-- A parser may have compacted supplied input before it throws. Its previous
-- cursor cannot safely be resumed even if the row callback catches the error.
invalidateRow :: Row -> IO ()
invalidateRow row = atomicModifyIORef' (rowCursor row) (\cursor -> (cursor {cursorActive = False}, ()))
