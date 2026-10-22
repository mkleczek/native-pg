{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FunctionalDependencies #-}
-- | Fixed-size list prefixes. Instances retain elements without copying payloads.
module Network.Streaming.Internal.ListPrefix
  ( ListPrefix (..) ) where

import Data.Tuple (Solo (MkSolo))

class ListPrefix tuple a | tuple -> a where
  prefixSize :: Int
  takePrefix :: [a] -> Maybe (tuple, [a])
  prepend :: tuple -> [a] -> [a]

instance ListPrefix (Solo a) a where
  prefixSize = 1
  takePrefix (a : rest) = Just (MkSolo a, rest)
  takePrefix _ = Nothing
  prepend (MkSolo a) rest = a : rest

instance ListPrefix (a, a) a where
  prefixSize = 2
  takePrefix (a : b : rest) = Just ((a, b), rest)
  takePrefix _ = Nothing
  prepend (a, b) rest = a : b : rest

instance ListPrefix (a, a, a) a where
  prefixSize = 3
  takePrefix (a : b : c : rest) = Just ((a, b, c), rest)
  takePrefix _ = Nothing
  prepend (a, b, c) rest = a : b : c : rest

instance ListPrefix (a, a, a, a) a where
  prefixSize = 4
  takePrefix (a : b : c : d : rest) = Just ((a, b, c, d), rest)
  takePrefix _ = Nothing
  prepend (a, b, c, d) rest = a : b : c : d : rest

instance ListPrefix (a, a, a, a, a) a where
  prefixSize = 5
  takePrefix (a : b : c : d : e : rest) = Just ((a, b, c, d, e), rest)
  takePrefix _ = Nothing
  prepend (a, b, c, d, e) rest = a : b : c : d : e : rest
