{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module NamedPolymorphic where

import Control.Arrow ((>>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named

-- | A step for any record that has a size, which adds a field to it and reads the field that it added.
doubled :: (Steps arr, HasField "size" r Int) => arr r Int
doubled = adding "double" (now (\r -> pure (r.size * 2))) >>> now (\r -> pure r.double)
