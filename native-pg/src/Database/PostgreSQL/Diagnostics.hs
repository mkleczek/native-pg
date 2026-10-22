{-# LANGUAGE OverloadedStrings #-}

-- | Errors and notices reported by the server or client.
module Database.PostgreSQL.Diagnostics
  ( Diagnostics (..),
    ServerError (..),
    Severity (..),
    ConnectionError (..),
    QueryError (..),
    StatementError (..),
    isConnectionFatal,
  ) where

import Control.Exception (Exception (..))
import Data.ByteString (ByteString)
import Data.List (intercalate)
import Data.Maybe (catMaybes)
import qualified Data.Text as Text
import Data.Text.Encoding (decodeUtf8With)
import Data.Text.Encoding.Error (lenientDecode)
import Database.PostgreSQL.Decoding (DecodingError)

-- | The fields of an ErrorResponse or a NoticeResponse, copied out of the
-- read buffer.
data Diagnostics = Diagnostics
  { diagnosticSeverity :: Severity,
    -- | The SQLSTATE, e.g. @23505@. Its first two characters are its class.
    diagnosticCode :: ByteString,
    diagnosticMessage :: ByteString,
    diagnosticDetail :: Maybe ByteString,
    diagnosticHint :: Maybe ByteString,
    -- | Where in the query the error is: a character index, from 1.
    diagnosticPosition :: Maybe Int,
    -- | Like 'diagnosticPosition', for an internally generated query.
    diagnosticInternalPosition :: Maybe Int,
    diagnosticInternalQuery :: Maybe ByteString,
    -- | The call stack in which the error happened, e.g. of PL/pgSQL
    -- functions.
    diagnosticContext :: Maybe ByteString,
    diagnosticSchema :: Maybe ByteString,
    diagnosticTable :: Maybe ByteString,
    diagnosticColumn :: Maybe ByteString,
    diagnosticDataType :: Maybe ByteString,
    diagnosticConstraint :: Maybe ByteString,
    -- | Where in PostgreSQL's source code the error was reported.
    diagnosticSourceFile :: Maybe ByteString,
    diagnosticSourceLine :: Maybe Int,
    diagnosticSourceFunction :: Maybe ByteString
  }
  deriving (Eq, Show)

data Severity
  = SeverityError
  | -- | The server ends the session after the message.
    SeverityFatal
  | -- | The server ends all sessions after the message.
    SeverityPanic
  | SeverityWarning
  | SeverityNotice
  | SeverityDebug
  | SeverityInfo
  | SeverityLog
  | -- | A severity this client doesn't know, as the server named it.
    SeverityOther ByteString
  deriving (Eq, Show)

-- | An error the server reported.
newtype ServerError = ServerError Diagnostics
  deriving (Eq, Show)

-- | Worded as libpq words it: the severity and the message, then the detail
-- and the hint on lines of their own.
instance Exception ServerError where
  displayException (ServerError fields) =
    intercalate "\n" $
      (severityName (diagnosticSeverity fields) <> ":  " <> text (diagnosticMessage fields))
        : catMaybes
          [ ("DETAIL:  " <>) . text <$> diagnosticDetail fields,
            ("HINT:  " <>) . text <$> diagnosticHint fields
          ]
    where
      text = Text.unpack . decodeUtf8With lenientDecode
      severityName severity = case severity of
        SeverityError -> "ERROR"
        SeverityFatal -> "FATAL"
        SeverityPanic -> "PANIC"
        SeverityWarning -> "WARNING"
        SeverityNotice -> "NOTICE"
        SeverityDebug -> "DEBUG"
        SeverityInfo -> "INFO"
        SeverityLog -> "LOG"
        SeverityOther other -> text other

data ConnectionError
  = -- | The server refused the connection.
    ConnectionRejected ServerError
  | -- | The server ended the session with this error, e.g. because an
    -- administrator terminated it.
    ConnectionTerminated ServerError
  | -- | The server's messages don't follow the protocol.
    ProtocolError String
  | -- | The server asked for an authentication method this client doesn't
    -- implement.
    UnsupportedAuthentication String
  | -- | Authentication failed on the client's side: a password is needed but
    -- missing, or the server's SCRAM signature doesn't match.
    AuthenticationError String
  | -- | The session is busy with another operation, broke in an earlier
    -- one, or has been closed.
    SessionUnusable String
  | -- | A row or a column stream was used out of turn.
    RowMisuse String
  | -- | TLS couldn't be started, e.g. because the server doesn't take it.
    TlsFailure String
  deriving (Eq, Show)

instance Exception ConnectionError where
  displayException failure = case failure of
    ConnectionRejected rejection -> displayException rejection
    ConnectionTerminated termination -> displayException termination
    ProtocolError reason -> "protocol error: " <> reason
    UnsupportedAuthentication method -> "the server asks for authentication with " <> method <> ", which isn't supported"
    AuthenticationError reason -> reason
    SessionUnusable reason -> reason
    RowMisuse reason -> reason
    TlsFailure reason -> reason

-- | A statement failed: its SQL, and why.
data QueryError = QueryError ByteString StatementError
  deriving (Eq, Show)

instance Exception QueryError

data StatementError
  = -- | The server reported an error.
    ServerFailed ServerError
  | -- | A row couldn't be decoded.
    RowFailed DecodingError
  | -- | The result had this many rows, which its shape doesn't allow.
    UnexpectedAmountOfRows Int
  deriving (Eq, Show)

-- | Whether the server ends the session after this error.
isConnectionFatal :: ServerError -> Bool
isConnectionFatal (ServerError fields) =
  case diagnosticSeverity fields of
    SeverityFatal -> True
    SeverityPanic -> True
    _ -> False
