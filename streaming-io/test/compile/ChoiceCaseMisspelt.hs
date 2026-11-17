{-# LANGUAGE DataKinds #-}

module ChoiceCaseMisspelt where

import Control.Arrow (arr)
import Network.Streaming.Classes
import Network.Streaming.Named

-- | A prism of a case that the sum doesn't have: the cases are "done" and "failed", and the prism is of "faild".
missing :: (Steps arr) => arr (Either ("done" := Int) ("failed" := String)) (Either ("done" := Int) ("failed" := Int))
missing = _Case "faild" (arr length)
