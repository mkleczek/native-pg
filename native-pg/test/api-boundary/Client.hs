{-# LANGUAGE DataKinds #-}
module Client where

import Database.PostgreSQL.ConnectionString ()
import Database.PostgreSQL.ConnectionString.Types ()
import Database.PostgreSQL.Decoding ()
import Database.PostgreSQL.Diagnostics ()
import Database.PostgreSQL.Encoders (Parameter (..))
import Database.PostgreSQL.Pipeline ()
import Database.PostgreSQL.Session ()
import Database.PostgreSQL.Session.Settings ()
import Database.PostgreSQL.Snippet ()
import Database.PostgreSQL.Statement (Result)
import Database.PostgreSQL.Streaming (ByteStream (..), withBuffers)
import Database.PostgreSQL.Streaming.Types ()
import Database.PostgreSQL.Transaction ()
import Database.PostgreSQL.Transport (Channel, Link (..), newChannel, serverEndPoint)
import Database.PostgreSQL.Transport.Link ()
import Database.PostgreSQL.Types ()
import qualified Database.PostgreSQL.Pipeline as Pipeline
import qualified Database.PostgreSQL.Statement as Statement
import Data.ByteString (ByteString)
import Data.Word (Word32)

prepare :: Pipeline.Prepare -> ByteString -> [Maybe Word32] -> IO Pipeline.Prepared
prepare = id

parameterType :: Statement.Param -> Maybe Word32
parameterType = Statement.paramType
