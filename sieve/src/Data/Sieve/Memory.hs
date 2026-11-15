{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-- | The arrays that the table is made of, and the primitives that read and write them, with the memory ordering of each.
--
-- * @readArray#@ and @writeArray#@ of 'Pointers': the write is a store-release on both of the architectures that matter, and
--   so is made after the stores that initialised the object that it publishes; the read is an acquire load on ARM, a plain
--   load on x86. (GHC 9.10, aarch64: @stlr@ and @ldar@.)
-- * 'readInt' and 'writeInt': plain loads and stores of one machine word, which can't tear. They are ordered by nothing but
--   the dependencies of the loads that follow them.
-- * 'readIntAtomic' and 'writeIntAtomic': sequentially consistent load and store (@ldar@ and @stlr@ on ARM, which are
--   sequentially consistent between themselves; plain @mov@ and an @xchg@ on x86).
-- * 'casInt' and 'fetchAddInt': read-modify-writes that are full barriers.
-- * 'readByte' and 'writeByte': plain loads and stores of one byte, which don't touch the bytes beside it.
module Data.Sieve.Memory
  ( Pointers,
    newPointers,
    readPointer,
    writePointer,
    Ints,
    newInts,
    newAlignedInts,
    readInt,
    writeInt,
    readIntAtomic,
    writeIntAtomic,
    casInt,
    fetchAddInt,
    Bytes,
    newBytes,
    readByte,
    writeByte,
  )
where

import GHC.Exts
import GHC.IO (IO (..))
import GHC.Word (Word8 (..))

-- | An array of pointers to values, each of which is replaced as a whole.
data Pointers a = Pointers (MutableArray# RealWorld a)

-- | A new array of this size, every pointer the value.
newPointers :: Int -> a -> IO (Pointers a)
newPointers (I# size) value = IO (\s -> case newArray# size value s of (# s', array #) -> (# s', Pointers array #))
{-# INLINE newPointers #-}

readPointer :: Pointers a -> Int -> IO a
readPointer (Pointers array) (I# i) = IO (readArray# array i)
{-# INLINE readPointer #-}

-- | Replaces a pointer. What the value points to was written before, and a reader that reads this pointer sees it.
writePointer :: Pointers a -> Int -> a -> IO ()
writePointer (Pointers array) (I# i) value = IO (\s -> (# writeArray# array i value s, () #))
{-# INLINE writePointer #-}

-- | An array of machine words.
data Ints = Ints (MutableByteArray# RealWorld)

-- | A new array of this size, every word the value.
newInts :: Int -> Int -> IO Ints
newInts (I# size) (I# value) = IO $ \s -> case newByteArray# (size *# 8#) s of
  (# s', array #) -> (# fill array 0# s', Ints array #)
  where
    -- A tail loop, since an array can have many words.
    fill array i s
      | isTrue# (i >=# size) = s
      | otherwise = fill array (i +# 1#) (writeIntArray# array i value s)
{-# INLINE newInts #-}

-- | A new array of zeros that starts on the line of a processor's cache that a multiple of 128 bytes leaves it on, in memory
-- that doesn't move: for counters that each core has a line of its own of.
newAlignedInts :: Int -> IO Ints
newAlignedInts (I# size) = IO $ \s -> case newAlignedPinnedByteArray# (size *# 8#) 128# s of
  (# s', array #) -> (# setByteArray# array 0# (size *# 8#) 0# s', Ints array #)

readInt :: Ints -> Int -> IO Int
readInt (Ints array) (I# i) = IO (\s -> case readIntArray# array i s of (# s', x #) -> (# s', I# x #))
{-# INLINE readInt #-}

writeInt :: Ints -> Int -> Int -> IO ()
writeInt (Ints array) (I# i) (I# x) = IO (\s -> (# writeIntArray# array i x s, () #))
{-# INLINE writeInt #-}

readIntAtomic :: Ints -> Int -> IO Int
readIntAtomic (Ints array) (I# i) = IO (\s -> case atomicReadIntArray# array i s of (# s', x #) -> (# s', I# x #))
{-# INLINE readIntAtomic #-}

writeIntAtomic :: Ints -> Int -> Int -> IO ()
writeIntAtomic (Ints array) (I# i) (I# x) = IO (\s -> (# atomicWriteIntArray# array i x s, () #))
{-# INLINE writeIntAtomic #-}

-- | Sets the word to the new value if it is the old, and says what it was: the old, if it did.
casInt :: Ints -> Int -> Int -> Int -> IO Int
casInt (Ints array) (I# i) (I# old) (I# new) = IO (\s -> case casIntArray# array i old new s of (# s', x #) -> (# s', I# x #))
{-# INLINE casInt #-}

-- | Adds to the word, and says what it was.
fetchAddInt :: Ints -> Int -> Int -> IO Int
fetchAddInt (Ints array) (I# i) (I# n) = IO (\s -> case fetchAddIntArray# array i n s of (# s', x #) -> (# s', I# x #))
{-# INLINE fetchAddInt #-}

-- | An array of bytes.
data Bytes = Bytes (MutableByteArray# RealWorld)

-- | A new array of this size, every byte zero.
newBytes :: Int -> IO Bytes
newBytes (I# size) = IO $ \s -> case newByteArray# size s of
  (# s', array #) -> (# setByteArray# array 0# size 0# s', Bytes array #)
{-# INLINE newBytes #-}

readByte :: Bytes -> Int -> IO Word8
readByte (Bytes array) (I# i) = IO (\s -> case readWord8Array# array i s of (# s', x #) -> (# s', W8# x #))
{-# INLINE readByte #-}

writeByte :: Bytes -> Int -> Word8 -> IO ()
writeByte (Bytes array) (I# i) (W8# x) = IO (\s -> (# writeWord8Array# array i x s, () #))
{-# INLINE writeByte #-}
