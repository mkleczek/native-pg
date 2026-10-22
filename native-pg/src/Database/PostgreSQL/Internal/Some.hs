{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}

module Database.PostgreSQL.Internal.Some (
  Some,
  toSome,
  withSome
)
where

import Data.Kind (Type)
import GHC.Exts (Any)
import Unsafe.Coerce (unsafeCoerce)

-- A universal, zero-cost "Some" wrapper
newtype Some (f :: k -> Type) = Some Any

-- To "wrap" any indexed type into the universal representation
toSome :: f a -> Some f
toSome = Some . unsafeCoerce

-- To "unwrap" safely, you must provide a polymorphic callback (CPS style)
withSome :: Some f -> (forall (a :: k). f a -> r) -> r
withSome (Some x) k = k (unsafeCoerce x)
