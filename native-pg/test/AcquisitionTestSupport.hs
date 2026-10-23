module AcquisitionTestSupport (getAcquisition) where

import Control.Concurrent.Resource.Acquisition qualified as Acquire
import GHC.Event qualified as Timer

getAcquisition :: IO Acquire.Context
getAcquisition = Acquire.newContext <$> Timer.getSystemTimerManager
