{-# LANGUAGE LambdaCase #-}

-- | Stages: a direction's work as first-order steps. A stage's state is data,
-- and its step is a function fixed when the stage is made, so a stage that
-- can't go on leaves a value behind, not a captured continuation. A step never
-- waits: its result is one of a fixed set of commands to the driver that runs
-- it.
module Network.Directions.Stage
  ( -- * Stages
    Stage (..),
    Step (..),
    Want (..),
    (~>),
    pipeWith,

    -- * The bytes between stages
    Gap,
    In,
    Out,
    newGap,
    closedGap,
    inputOf,
    outputOf,
    pendingBytes,
    consume,
    inputEnded,
    withRoom,
    produce,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI
import Data.Functor ((<&>))
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrArray, plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (moveBytes)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peekElemOff, pokeElemOff)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes, unsafeWithForeignPtr)
import Network.Streaming.Watch (Watch)

-- | A stage of a direction: how to make its first state, and its step.
data Stage = forall s. Stage (IO s) (s -> In -> Out -> IO (Step s))

-- | What a step tells its driver: one of a fixed set of commands. A step never
-- waits.
data Step s
  = -- | It moved bytes, or ended something: step it again.
    Again !s
  | -- | It can't go on until one of these happens.
    Await !s [Want]
  | -- | It's done, and its output ends.
    Done

-- | What a stage that can't go on waits for.
data Want
  = -- | Bytes in its input, which the stage before it gives.
    Input
  | -- | Room in its output, which the stage after it makes.
    Room
  | -- | An event outside the direction, such as a link that becomes readable:
    -- the registration of its watch.
    Event (IO Watch)

infixr 1 ~>

-- | The first stage's output as the second's input, through a gap of 16 KiB.
(~>) :: Stage -> Stage -> Stage
(~>) = pipeWith 16384

-- | The first stage's output as the second's input, through a gap of this many
-- bytes. A step of the pair steps both: the pair moved if either did, and it
-- waits for what either waits for from outside the pair. What one waits for
-- from the other, the other's step brings, so it isn't waited for.
pipeWith :: Int -> Stage -> Stage -> Stage
pipeWith size (Stage startFirst stepFirst) (Stage startSecond stepSecond) =
  Stage (Both <$> startFirst <*> newGap size <*> startSecond) step
  where
    step state input output = case state of
      Both first gap second ->
        stepFirst first input (Out gap) >>= \case
          Again first' -> stepThen True [] (Both first' gap) gap second output
          Await first' wants -> stepThen False (filter fromBefore wants) (Both first' gap) gap second output
          Done -> endGap gap *> stepThen True [] (After gap) gap second output
      After gap second -> stepThen False [] (After gap) gap second output
    stepThen moved waitedBefore rebuild gap second output =
      stepSecond second (In gap) output <&> \case
        Again second' -> Again (rebuild second')
        Await second' wants
          | moved -> Again (rebuild second')
          | otherwise -> Await (rebuild second') (waitedBefore <> filter fromAfter wants)
        Done -> Done
    -- The first stage's room and the second's input are the gap between them.
    fromBefore = \case
      Room -> False
      _ -> True
    fromAfter = \case
      Input -> False
      _ -> True

-- | Two stages and the gap between them; once the first is done, the gap and
-- the second.
data Pair a b = Both !a !Gap !b | After !Gap !b

-- | The bytes between two stages: [start, end) of a buffer, whose size bounds
-- them, and whether the stage before has ended. A gap is inside one direction,
-- which one thread runs, so its marks need no synchronization.
data Gap = Gap
  { gapMemory :: !(ForeignPtr Word8),
    gapSize :: !Int,
    -- | The start, the end, and 1 once the stage before has ended.
    gapMarks :: !(ForeignPtr Int)
  }

-- | A stage's view of the gap before it.
newtype In = In Gap

-- | A stage's view of the gap after it.
newtype Out = Out Gap

newGap :: Int -> IO Gap
newGap size = do
  memory <- mallocPlainForeignPtrBytes size
  marks <- mallocForeignPtrArray 3
  let gap = Gap memory size marks
  gap <$ (setMark gap 0 0 *> setMark gap 1 0 *> setMark gap 2 0)

-- | A gap with nothing in it and no room, whose stage before has ended: a
-- direction's first stage reads nothing, and its last writes nothing.
closedGap :: IO Gap
closedGap = newGap 0 >>= \gap -> gap <$ endGap gap

inputOf :: Gap -> In
inputOf = In

outputOf :: Gap -> Out
outputOf = Out

-- | The pending bytes, without a copy. They are valid until the stage
-- consumes them, and no longer than its step.
pendingBytes :: In -> IO ByteString
pendingBytes (In gap) = do
  start <- mark gap 0
  end <- mark gap 1
  pure (BSI.BS (plusForeignPtr (gapMemory gap) start) (end - start))

-- | Takes this many of the pending bytes. A gap with none left starts over at
-- the start of its buffer.
consume :: In -> Int -> IO ()
consume (In gap) count = do
  start <- (+ count) <$> mark gap 0
  end <- mark gap 1
  if start >= end then setMark gap 0 0 *> setMark gap 1 0 else setMark gap 0 start

-- | Whether the stage before has ended. Bytes it gave may still be pending.
inputEnded :: In -> IO Bool
inputEnded (In gap) = (/= 0) <$> mark gap 2

-- | Gives the room after the pending bytes to write to: where it starts, and
-- its size. If the room is at the end of the buffer and the pending bytes
-- aren't at its start, they move there first.
withRoom :: Out -> (Ptr Word8 -> Int -> IO a) -> IO a
withRoom (Out gap) use = do
  start <- mark gap 0
  end <- mark gap 1
  withForeignPtr (gapMemory gap) $ \base -> do
    end' <-
      if end == gapSize gap && start > 0
        then do
          moveBytes base (base `plusPtr` start) (end - start)
          setMark gap 0 0
          (end - start) <$ setMark gap 1 (end - start)
        else pure end
    use (base `plusPtr` end') (gapSize gap - end')

-- | Makes this many bytes, written to the room, pending.
produce :: Out -> Int -> IO ()
produce (Out gap) count = mark gap 1 >>= setMark gap 1 . (+ count)

endGap :: Gap -> IO ()
endGap gap = setMark gap 2 1

mark :: Gap -> Int -> IO Int
mark gap index = unsafeWithForeignPtr (gapMarks gap) (`peekElemOff` index)

setMark :: Gap -> Int -> Int -> IO ()
setMark gap index value = unsafeWithForeignPtr (gapMarks gap) (\marks -> pokeElemOff marks index value)
