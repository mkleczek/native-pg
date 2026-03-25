{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.PostgreSQL.Protocol.Codec.Input
  ( InputSource (..),
    InputT,
    MonadInput (..),
    runBodyParser,
    runBodyParserT,
    runInputT
  )
where

import Control.Monad (unless)
import Control.Monad.Except (ExceptT (..), MonadError (..), catchError, runExceptT)
import Control.Monad.Identity (Identity (runIdentity))
import Control.Monad.Reader (ReaderT (..), ask, runReaderT)
import Control.Monad.Reader.Class (MonadReader)
import Control.Monad.State.Class (MonadState)
import Control.Monad.State.Strict (StateT (..), get, gets, modify', put, runStateT)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Functor (($>))
import Data.Word (Word8)

newtype InputSource m = InputSource
  { readInput :: Int -> m ByteString
  }

data InputState = InputState
  { inputBuffer :: ByteString,
    inputLimits :: [Int],
    inputFinished :: Bool
  }

newtype InputT m a = InputT
  { unInputT :: ExceptT String (ReaderT (InputSource m) (StateT InputState m)) a
  }
  deriving newtype (Functor, Applicative, Monad, MonadError String, MonadReader (InputSource m), MonadState InputState)

class MonadError String m => MonadInput m where
  peekInputByte :: m Word8
  readInputExactly :: Int -> m ByteString
  readInputRemaining :: m ByteString
  readInputCStringBytes :: m ByteString
  withInputLimit :: Int -> m a -> m a

runInputT :: Monad m => InputSource m -> ByteString -> InputT m a -> m (Either String (a, ByteString))
runInputT source input inputAction =
  fmap withBuffer $
    flip runStateT (InputState input [] False) $
      runReaderT (runExceptT inputAction') source
  where
    inputAction' = unInputT inputAction
    withBuffer (result, state) =
      fmap (, inputBuffer state) result

runBodyParser :: ByteString -> InputT Identity a -> Either String (a, ByteString)
runBodyParser input =
  runIdentity . runBodyParserT (InputSource (const (pure BS.empty))) input

runBodyParserT :: Monad m => InputSource m -> ByteString -> InputT m a -> m (Either String (a, ByteString))
runBodyParserT = runInputT

instance Monad m => MonadInput (InputT m) where
  peekInputByte = do
    ensureAvailableInput 1
    maybe
      (throwError "unexpected end of input while peeking a byte")
      pure
      =<< gets (fmap fst . BS.uncons . visibleBuffer)

  readInputExactly count = do
    guardInput
      (count >= 0)
      ("negative byte count " <> show count <> " is invalid")
    ensureAvailableInput count
    consumeExactly count

  readInputRemaining = drainRemainingInput

  readInputCStringBytes = do
    ensureCStringTerminator
    visible <- gets visibleBuffer
    maybe
      (throwError "unterminated C-string")
      (\index -> do
          bytes <- readInputExactly index
          _ <- readInputExactly 1
          pure bytes
      )
      (BS.elemIndex 0 visible)

  withInputLimit count action = do
    pushLimit count
    result <- catchError (Right <$> action) (pure . Left)
    remaining <- popLimit
    unless (remaining == 0) $
      discardExactly remaining
    case result of
      Left err ->
        throwError err
      Right value -> do
        guardInput
          (remaining == 0)
          ("bounded parser left " <> show remaining <> " unconsumed bytes")
        pure value

ensureAvailableInput :: Monad m => Int -> InputT m ()
ensureAvailableInput count = do
  limit <- currentLimit
  guardInput
    (maybe True (count <=) limit)
    ( "unexpected end of input: needed "
        <> show count
        <> " bytes, but only "
        <> show limit
        <> " remain in bounded input"
    )
  fillUntilInput count
  available <- gets (BS.length . visibleBuffer)
  guardInput
    (available >= count)
    ( "unexpected end of input: needed "
        <> show count
        <> " bytes, but only "
        <> show available
        <> " remain"
    )

ensureCStringTerminator :: Monad m => InputT m ()
ensureCStringTerminator = do
  found <- gets (maybe False (const True) . BS.elemIndex 0 . visibleBuffer)
  unless found $ do
    canReadMore <- canReadMoreWithinLimit
    if canReadMore
      then fillSome 4096 >> ensureCStringTerminator
      else throwError "unterminated C-string"

fillUntilInput :: Monad m => Int -> InputT m ()
fillUntilInput count = do
  available <- gets (BS.length . visibleBuffer)
  if available < count
    then do
      canReadMore <- canReadMoreWithinLimit
      if canReadMore
        then fillSome (count - available) >> fillUntilInput count
        else pure ()
    else pure ()

drainRemainingInput :: Monad m => InputT m ByteString
drainRemainingInput = do
  limit <- currentLimit
  maybe drainUntilEOF readInputExactly limit
  where
    drainUntilEOF = do
      finished <- gets inputFinished
      if finished
        then do
          available <- gets (BS.length . inputBuffer)
          readInputExactly available
        else fillSome 4096 >> drainUntilEOF

pushLimit :: Monad m => Int -> InputT m ()
pushLimit count = do
  current <- currentLimit
  maybe
    (pure ())
    (\remaining ->
        unless (count <= remaining) $
          error "internal error: attempted to push a limit larger than the active bounded input"
    )
    current
  modify' $ \state ->
    state
      { inputLimits = count : inputLimits state
      }

popLimit :: Monad m => InputT m Int
popLimit = do
  state <- get
  case inputLimits state of
    remaining : limits -> do
      put state {inputLimits = limits}
      pure remaining
    [] ->
      error "internal error: attempted to pop an input limit when none was active"

discardExactly :: Monad m => Int -> InputT m ()
discardExactly count = consumeExactly count $> ()

consumeExactly :: Monad m => Int -> InputT m ByteString
consumeExactly count = do
  state <- get
  let (bytes, rest) = BS.splitAt count (inputBuffer state)
  put $
    state
      { inputBuffer = rest,
        inputLimits = fmap (subtract count) (inputLimits state)
      }
  pure bytes

currentLimit :: Monad m => InputT m (Maybe Int)
currentLimit = gets (safeHead . inputLimits)

visibleBuffer :: InputState -> ByteString
visibleBuffer state =
  maybe
    (inputBuffer state)
    (`BS.take` inputBuffer state)
    (safeHead (inputLimits state))

canReadMoreWithinLimit :: Monad m => InputT m Bool
canReadMoreWithinLimit =
  gets $
    \state ->
      not (inputFinished state)
        && maybe True (\remaining -> BS.length (inputBuffer state) < remaining) (safeHead (inputLimits state))

fillSome :: Monad m => Int -> InputT m ()
fillSome requested = do
  source <- ask
  state <- get
  let remainingBudget =
        maybe
          requested
          (\remaining -> max 0 (remaining - BS.length (inputBuffer state)))
          (safeHead (inputLimits state))
      requestedBytes = max 1 (min requested remainingBudget)
  if inputFinished state || requestedBytes == 0
    then pure ()
    else do
      chunk <- liftBase (readInput source requestedBytes)
      let chunk' = BS.take requestedBytes chunk
      if BS.null chunk'
        then put state {inputFinished = True}
        else
          put $
            state
              { inputBuffer = inputBuffer state <> chunk'
              }

liftBase :: Monad m => m a -> InputT m a
liftBase action =
  InputT . ExceptT . ReaderT $ \_ ->
    StateT $ \state ->
      fmap (\value -> (Right value, state)) action

guardInput :: MonadError String m => Bool -> String -> m ()
guardInput condition message = unless condition (throwError message)

safeHead :: [a] -> Maybe a
safeHead values =
  case values of
    [] -> Nothing
    x : _ -> Just x
