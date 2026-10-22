{-# LANGUAGE OverloadedStrings #-}

-- | Decode wire error fields into owned diagnostics.
module Database.PostgreSQL.Internal.Diagnostics (diagnostics) where

import Control.Applicative ((<|>))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Word (Word8)
import Database.PostgreSQL.Diagnostics (Diagnostics (..), Severity (..))
import Database.PostgreSQL.Internal.Protocol.Messages.Types (ErrorNoticeField, Scalar (..))

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
