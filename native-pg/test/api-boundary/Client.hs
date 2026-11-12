{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ConstraintKinds #-}
module Client where

import Database.PostgreSQL.ConnectionString ()
import Database.PostgreSQL.ConnectionString.Types ()
import Database.PostgreSQL.Decoding ()
import Database.PostgreSQL.Diagnostics (ConnectionError (..))
import Database.PostgreSQL.Encoders (Parameter (..))
import Database.PostgreSQL.Session.Settings ()
import Database.PostgreSQL.Snippet ()
import Database.PostgreSQL.Transport (serverEndPoint)
import Database.PostgreSQL.Types (Param, paramType)
import qualified Control.Concurrent.Resource.Acquisition as Acquisition
import Control.Exception (bracket)
import Data.Word (Word32)
import Network.Streaming.BufferPool
  ( BufferPool, PoolSettings (..), newBufferPool
  , BufferAccess, withRequestBuffers, BufferLoan, borrowBuffer, loanBuffer, releaseBuffer
  , BufferGrant, withSetupBuffers
  )
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.PositiveInt (positiveInt)

misordered :: ConnectionError -> Maybe Int
misordered failure = case failure of
  ResultOutOfOrder unread -> Just unread
  _ -> Nothing

parameterType :: Param -> Maybe Word32
parameterType = paramType

-- A client can manage short loans while the request owns admission.
requestBuffer :: BufferPool -> (Buffer -> IO a) -> IO a
requestBuffer resources use = withRequestBuffers resources $ \access ->
  bracket (borrowBuffer access) releaseBuffer (use . loanBuffer)

manualLoan :: BufferAccess -> IO BufferLoan
manualLoan = borrowBuffer

setupBuffers :: BufferGrant grant => BufferPool -> (grant -> IO a) -> IO a
setupBuffers = withSetupBuffers

resourceSettings :: PoolSettings
resourceSettings = PoolSettings
  { bufferSize = positiveInt @4096
  , ordinaryBufferCount = positiveInt @2
  , reserveBufferCount = positiveInt @1
  , setupBufferCount = positiveInt @2
  }

createResources :: Acquisition.Context -> IO BufferPool
createResources acquisition = newBufferPool acquisition resourceSettings
