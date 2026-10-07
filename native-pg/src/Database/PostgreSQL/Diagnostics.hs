{-# LANGUAGE OverloadedStrings #-}

-- | The fields of the server's errors and notices.
module Database.PostgreSQL.Diagnostics
  ( Diagnostics (..),
    ServerError (..),
    Severity (..),
    diagnostics,
    isConnectionFatal,
  )
where

import Control.Applicative ((<|>))
import Control.Exception (Exception (..))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.List (intercalate)
import Data.Maybe (catMaybes)
import qualified Data.Text as Text
import Data.Text.Encoding (decodeUtf8With)
import Data.Text.Encoding.Error (lenientDecode)
import Data.Word (Word8)
import Database.PostgreSQL.Protocol.Messages.Types (ErrorNoticeField, Scalar (..))

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

-- | Decodes the fields, which have to include the severity, the SQLSTATE
-- and the message. Of a repeated field the first occurrence counts, and
-- fields the protocol doesn't define are ignored.
diagnostics :: [ErrorNoticeField ByteString] -> Either String Diagnostics
diagnostics fields = do
  severity <- required "severity" (field 'V' <|> field 'S')
  code <- required "SQLSTATE" (field 'C')
  message <- required "message" (field 'M')
  pure
    Diagnostics
      { diagnosticSeverity = toSeverity severity,
        diagnosticCode = code,
        diagnosticMessage = message,
        diagnosticDetail = field 'D',
        diagnosticHint = field 'H',
        diagnosticPosition = number =<< field 'P',
        diagnosticInternalPosition = number =<< field 'p',
        diagnosticInternalQuery = field 'q',
        diagnosticContext = field 'W',
        diagnosticSchema = field 's',
        diagnosticTable = field 't',
        diagnosticColumn = field 'c',
        diagnosticDataType = field 'd',
        diagnosticConstraint = field 'n',
        diagnosticSourceFile = field 'F',
        diagnosticSourceLine = number =<< field 'L',
        diagnosticSourceFunction = field 'R'
      }
  where
    field name = BS.copy <$> lookup (byte name) [(code, value) | Scalar (code, value) <- fields]
    required name = maybe (Left ("an error or notice without its " <> name)) Right
    number value = case Char8.readInt value of
      Just (parsed, rest) | BS.null rest -> Just parsed
      _ -> Nothing

-- | Whether the server ends the session after this error.
isConnectionFatal :: ServerError -> Bool
isConnectionFatal (ServerError fields) =
  case diagnosticSeverity fields of
    SeverityFatal -> True
    SeverityPanic -> True
    _ -> False

-- | The non-localized severity, which servers since 9.6 send in field @V@,
-- or else the one in field @S@, which may be translated.
toSeverity :: ByteString -> Severity
toSeverity severity = case severity of
  "ERROR" -> SeverityError
  "FATAL" -> SeverityFatal
  "PANIC" -> SeverityPanic
  "WARNING" -> SeverityWarning
  "NOTICE" -> SeverityNotice
  "DEBUG" -> SeverityDebug
  "INFO" -> SeverityInfo
  "LOG" -> SeverityLog
  other -> SeverityOther other

byte :: Char -> Word8
byte = fromIntegral . fromEnum
