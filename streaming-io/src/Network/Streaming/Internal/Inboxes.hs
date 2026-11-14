{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

-- | What an inbox is made of, for the instances of 'Network.Streaming.Classes.Inboxes': the address end, and the shelf that
-- both ends share, with the changes that the arrows make to it. The package doesn't expose this module: a program that
-- could make an 'Address' of its own would choose its reach.
--
-- A change is a function of the whole shelf, applied with plain reads and writes by the parts of one core, and by
-- compare-and-swap by the parts of any core, so that one set of changes serves both reaches. A part that waits leaves a
-- closure in the shelf that puts it back among the parts that can go on.
module Network.Streaming.Internal.Inboxes
  ( Address (..),
    Shelf,
    Change,
    changeHere,
    newShelf,
    hasValue,
    postNow,
    postOrQueue,
    takeNow,
    takeWoken,
    Looked (..),
    lookOrWait,
    collectedWrongly,
    stopWaiting,
    withdraw,
    closeShelf,
  )
where

import Data.Foldable (traverse_)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Sequence (Seq, ViewL (..), viewl, (|>))
import qualified Data.Sequence as Seq
import GHC.TypeLits (Symbol)
import Network.Streaming.Internal.Tickets (Reach (..))

-- | The end of an inbox that others post to, which has the inbox's name. An address of reach @'Here s@ mentions the region
-- @s@, so that it can't leave the core that made the inbox; an address of reach @'Anywhere@ mentions no core, and can be
-- kept in structures that cores share.
data family Address (name :: Symbol) (r :: Reach) a

-- | An address of an inbox of this core: its shelf, which only the parts of this core read and change.
newtype instance Address name ('Here _s) a = HereAddress (IORef (Shelf a))

-- | An address of an inbox of any core: its shelf, which the parts of every core change atomically.
newtype instance Address name 'Anywhere a = AnywhereAddress (IORef (Shelf a))

-- | What an inbox holds: nothing; nothing, and the owner waits, which a signal wakes when a value is there; a value, and the
-- posters that wait to put theirs after it, the first to have come first; or that it is closed. A value is evaluated: a
-- thunk would keep what it was made from alive as long as the shelf.
data Shelf a
  = Vacant
  | Collecting !(IO ())
  | Holding !a !(Seq (Poster a))
  | Shut

-- | A part that waits to post: the token it is known by, its value, and what wakes it with whether the value went in.
data Poster a = Poster !(IORef ()) !a !(Bool -> IO ())

-- | How a shelf is changed: by a function that gives the new shelf and what the change says.
type Change = forall x r. IORef x -> (x -> (x, r)) -> IO r

-- | The change by plain reads and writes, for the parts of one core. It evaluates what it installs, as the compare-and-swap
-- of "Network.Streaming.Internal.Atomic" does.
changeHere :: Change
changeHere ref change = readIORef ref >>= \old -> case change old of (!new, !result) -> result <$ writeIORef ref new
{-# INLINE changeHere #-}

-- | A new inbox's shelf.
newShelf :: IO (IORef (Shelf a))
newShelf = newIORef Vacant

-- | Whether the inbox has a value, which a look doesn't take.
hasValue :: IORef (Shelf a) -> IO Bool
hasValue shelf = (\case Holding {} -> True; _ -> False) <$> readIORef shelf

-- | Posts the value if the inbox has room, and wakes the owner if it waits: it says whether the inbox took it, or is Nothing
-- if the inbox is full, and then nothing changes.
postNow :: Change -> IORef (Shelf a) -> a -> IO (Maybe Bool)
postNow change shelf value = posting change shelf value Nothing

-- | Posts the value, or if the inbox is full, queues the poster, which is woken with whether its value went in. It says
-- whether the inbox took the value, or is Nothing if the poster waits.
postOrQueue :: Change -> IORef (Shelf a) -> IORef () -> a -> (Bool -> IO ()) -> IO (Maybe Bool)
postOrQueue change shelf token value woken = posting change shelf value (Just (token, woken))

posting :: Change -> IORef (Shelf a) -> a -> Maybe (IORef (), Bool -> IO ()) -> IO (Maybe Bool)
posting change shelf !value waiting = do
  (posted, wake) <- change shelf $ \case
    Vacant -> (Holding value Seq.empty, (Just True, pure ()))
    Collecting signal -> (Holding value Seq.empty, (Just True, signal))
    full@(Holding held posters) -> case waiting of
      Nothing -> (full, (Nothing, pure ()))
      Just (token, woken) -> (Holding held (posters |> Poster token value woken), (Nothing, pure ()))
    Shut -> (Shut, (Just False, pure ()))
  posted <$ wake

-- | Takes the value, if there is one, and lets in the value of the poster that waited longest, which wakes that poster.
takeNow :: Change -> IORef (Shelf a) -> IO (Maybe a)
takeNow change shelf = do
  (taken, wake) <- change shelf $ \case
    Holding value posters -> case viewl posters of
      Poster _ next woken :< rest -> (Holding next rest, (Just value, woken True))
      EmptyL -> (Vacant, (Just value, pure ()))
    other -> (other, (Nothing, pure ()))
  taken <$ wake

-- | Takes the value for an owner that a post woke, which is there.
takeWoken :: Change -> IORef (Shelf a) -> IO a
takeWoken change shelf = takeNow change shelf >>= maybe (ioError (userError "an inbox woke its owner without a value")) pure

-- | What the owner found when it looked for a value: it waits, a value is there, or it can't collect.
data Looked = Registered | Found | Unusable

-- | The owner waits for a value, unless one is there, and the signal wakes it once one is posted. It can't collect if another part
-- waits to, or if the inbox is closed.
lookOrWait :: Change -> IORef (Shelf a) -> IO () -> IO Looked
lookOrWait change shelf signal = change shelf $ \case
  Vacant -> (Collecting signal, Registered)
  held@Holding {} -> (held, Found)
  other -> (other, Unusable)

-- | The failure of an owner that can't collect.
collectedWrongly :: IO a
collectedWrongly = ioError (userError "an inbox was collected from by two parts, or after it was closed")

-- | The owner stops waiting, and says whether it did: it didn't if a post has woken it already, and then the value is in the inbox.
stopWaiting :: Change -> IORef (Shelf a) -> IO Bool
stopWaiting change shelf = change shelf $ \case
  Collecting _ -> (Vacant, True)
  other -> (other, False)

-- | The poster stops waiting, and says whether it did: it didn't if its value went in already, or the inbox closed.
withdraw :: Change -> IORef (Shelf a) -> IORef () -> IO Bool
withdraw change shelf token = change shelf $ \case
  Holding held posters
    | Just index <- Seq.findIndexL (\(Poster other _ _) -> other == token) posters -> (Holding held (Seq.deleteAt index posters), True)
  other -> (other, False)

-- | Closes the inbox, and gives back the value left in it. It wakes every poster that waits with False, in the order they came.
closeShelf :: Change -> IORef (Shelf a) -> IO (Maybe a)
closeShelf change shelf = do
  (left, wake) <- change shelf $ \case
    Holding value posters -> (Shut, (Just value, traverse_ (\(Poster _ _ woken) -> woken False) posters))
    _ -> (Shut, (Nothing, pure ()))
  left <$ wake
