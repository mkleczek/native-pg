{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Exception (displayException, evaluate)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (toList)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Int (Int32)
import Data.List (isInfixOf)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Word (Word8)
import Database.PostgreSQL.Authentication
import Database.PostgreSQL.ConnectionString
import Database.PostgreSQL.Diagnostics
import Database.PostgreSQL.Protocol
import Database.PostgreSQL.Protocol.Codec
import Database.PostgreSQL.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Transport (Address (..), ConnectError (..), TcpOptions (..), defaultTcpOptions)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "frontend messages" $ do
    it "encodes Sync" $
      encodes @Sync @'SyncType Sync "S\0\0\0\4"
    it "encodes Query" $
      encodes @(Query ByteString) @'QueryType (Query "SELECT 1") ("Q" <> int32 13 <> "SELECT 1\0")
    it "encodes Parse with an unspecified parameter type" $
      encodes
        @(Parse ByteString)
        @'ParseType
        (Parse "" "SELECT $1" [Nothing])
        ("P" <> int32 21 <> "\0SELECT $1\0" <> int16 1 <> int32 0)
    it "encodes Bind with a NULL parameter" $
      encodes
        @(Bind ByteString)
        @'BindType
        (Bind (BindBody "" "statement" [BinaryFormat] [Just "1", Nothing] [TextFormat]))
        ("B" <> int32 34 <> "\0statement\0" <> int16 1 <> int16 1 <> int16 2 <> int32 1 <> "1" <> int32 (-1) <> int16 1 <> int16 0)
    it "encodes Execute" $
      encodes @(Execute ByteString) @'ExecuteType (Execute "" 0) ("E" <> int32 9 <> "\0" <> int32 0)
    it "encodes Describe of a portal" $
      encodes @(Describe ByteString) @'DescribeType (Describe Portal "") ("D" <> int32 6 <> "P\0")
    it "encodes a StartupMessage, whose length counts itself" $
      encodes @(StartupMessage ByteString) @'StartupMessageType startupMessage startupMessageBytes
    it "encodes a CancelRequest" $
      encodes @(CancelRequest ByteString) @'CancelRequestType cancelRequest cancelRequestBytes
    it "rejects a zero byte inside a C string" $
      evaluate (outputByteString (serialize @(Query ByteString) @'QueryType (Query "a\0b")))
        `shouldThrow` anyErrorCall
    it "round-trips every frontend message" $ do
      roundTrip @(StartupMessage ByteString) @'StartupMessageType startupMessage
      roundTrip @SSLRequest @'SSLRequestType SSLRequest
      roundTrip @GSSENCRequest @'GSSENCRequestType GSSENCRequest
      roundTrip @(CancelRequest ByteString) @'CancelRequestType cancelRequest
      roundTrip @(PasswordMessage ByteString) @'PasswordMessageType (PasswordMessage "secret")
      roundTrip @(GSSResponse ByteString) @'GSSResponseType (GSSResponse "token")
      roundTrip @(SASLInitialResponse ByteString) @'SASLInitialResponseType (SASLInitialResponse "SCRAM-SHA-256" (Just "n,,n=,r=nonce"))
      roundTrip @(SASLInitialResponse ByteString) @'SASLInitialResponseType (SASLInitialResponse "SCRAM-SHA-256" Nothing)
      roundTrip @(SASLResponse ByteString) @'SASLResponseType (SASLResponse "c=biws,r=nonce,p=proof")
      roundTrip @(Bind ByteString) @'BindType (Bind (BindBody "portal" "statement" [TextFormat, BinaryFormat] [Just "", Nothing] []))
      roundTrip @(Close ByteString) @'CloseType (Close PreparedStatement "statement")
      roundTrip @(Describe ByteString) @'DescribeType (Describe PreparedStatement "statement")
      roundTrip @(Execute ByteString) @'ExecuteType (Execute "portal" 100)
      roundTrip @Flush @'FlushType Flush
      roundTrip @(FunctionCall ByteString) @'FunctionCallType (FunctionCall (Scalar 1) [BinaryFormat] [Just "x"] TextFormat)
      roundTrip @(Parse ByteString) @'ParseType (Parse "statement" "SELECT $1, $2" [Just (Scalar 23), Nothing])
      roundTrip @(Query ByteString) @'QueryType (Query "SELECT 1")
      roundTrip @Sync @'SyncType Sync
      roundTrip @Terminate @'TerminateType Terminate
      roundTrip @(CopyData ByteString) @'CopyDataType (CopyData "1\t2\n")
      roundTrip @CopyDone @'CopyDoneType CopyDone
      roundTrip @(CopyFail ByteString) @'CopyFailType (CopyFail "client gave up")

  describe "backend messages" $ do
    it "decodes AuthenticationOk" $
      decodes @AuthenticationOk @'AuthenticationOkType ("R" <> int32 8 <> int32 0) AuthenticationOk
    it "decodes AuthenticationSASL" $
      decodes
        @(AuthenticationSASL ByteString)
        @'AuthenticationSASLType
        ("R" <> int32 42 <> int32 10 <> "SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0")
        (AuthenticationSASL ["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"])
    it "decodes BackendKeyData with a protocol 3.2 secret key" $
      decodes
        @(BackendKeyData ByteString)
        @'BackendKeyDataType
        ("K" <> int32 40 <> int32 42 <> BS.replicate 32 7)
        (BackendKeyData (Scalar (Scalar 42, Scalar (BS.replicate 32 7))))
    it "decodes NegotiateProtocolVersion" $
      decodes
        @(NegotiateProtocolVersion ByteString)
        @'NegotiateProtocolVersionType
        ("v" <> int32 24 <> int32 196608 <> int32 1 <> "_pq_.option\0")
        (NegotiateProtocolVersion protocolVersion3_0 ["_pq_.option"])
    it "decodes ReadyForQuery" $
      decodes @ReadyForQuery @'ReadyForQueryType ("Z" <> int32 5 <> "T") (ReadyForQuery TxInTransaction)
    it "rejects an unknown transaction status" $
      parseEnvelope @ReadyForQuery @'ReadyForQueryType ("Z" <> int32 5 <> "X")
        `shouldSatisfy` isLeft
    it "rejects a message whose fields don't fill its length" $
      parseEnvelope @(CommandComplete ByteString) @'CommandCompleteType ("C" <> int32 12 <> "SELECT\0x")
        `shouldSatisfy` isLeft
    it "round-trips every backend message" $ do
      roundTrip @AuthenticationOk @'AuthenticationOkType AuthenticationOk
      roundTrip @AuthenticationKerberosV5 @'AuthenticationKerberosV5Type AuthenticationKerberosV5
      roundTrip @AuthenticationCleartextPassword @'AuthenticationCleartextPasswordType AuthenticationCleartextPassword
      roundTrip @(AuthenticationMD5Password ByteString) @'AuthenticationMD5PasswordType (AuthenticationMD5Password "salt")
      roundTrip @AuthenticationGSS @'AuthenticationGSSType AuthenticationGSS
      roundTrip @(AuthenticationGSSContinue ByteString) @'AuthenticationGSSContinueType (AuthenticationGSSContinue "token")
      roundTrip @AuthenticationSSPI @'AuthenticationSSPIType AuthenticationSSPI
      roundTrip @(AuthenticationSASL ByteString) @'AuthenticationSASLType (AuthenticationSASL ["SCRAM-SHA-256"])
      roundTrip @(AuthenticationSASLContinue ByteString) @'AuthenticationSASLContinueType (AuthenticationSASLContinue "r=nonce,s=salt,i=4096")
      roundTrip @(AuthenticationSASLFinal ByteString) @'AuthenticationSASLFinalType (AuthenticationSASLFinal "v=signature")
      roundTrip @(BackendKeyData ByteString) @'BackendKeyDataType (BackendKeyData (Scalar (Scalar 7, Scalar "key!")))
      roundTrip @BindComplete @'BindCompleteType BindComplete
      roundTrip @CloseComplete @'CloseCompleteType CloseComplete
      roundTrip @(CommandComplete ByteString) @'CommandCompleteType (CommandComplete "SELECT 1")
      roundTrip @CopyInResponse @'CopyInResponseType (CopyInResponse (Scalar (CopyText, [TextFormat])))
      roundTrip @CopyOutResponse @'CopyOutResponseType (CopyOutResponse (Scalar (CopyBinary, [BinaryFormat, BinaryFormat])))
      roundTrip @CopyBothResponse @'CopyBothResponseType (CopyBothResponse (Scalar (CopyBinary, [])))
      roundTrip @(DataRow ByteString) @'DataRowType (DataRow [Just "1", Nothing, Just ""])
      roundTrip @EmptyQueryResponse @'EmptyQueryResponseType EmptyQueryResponse
      roundTrip @(ErrorResponse ByteString) @'ErrorResponseType (ErrorResponse errorFields)
      roundTrip @(FunctionCallResponse ByteString) @'FunctionCallResponseType (FunctionCallResponse Nothing)
      roundTrip @(NegotiateProtocolVersion ByteString) @'NegotiateProtocolVersionType (NegotiateProtocolVersion protocolVersion3_0 [])
      roundTrip @NoData @'NoDataType NoData
      roundTrip @(NoticeResponse ByteString) @'NoticeResponseType (NoticeResponse errorFields)
      roundTrip @(NotificationResponse ByteString) @'NotificationResponseType (NotificationResponse (Notification (Scalar 9) "channel" "payload"))
      roundTrip @ParameterDescription @'ParameterDescriptionType (ParameterDescription [Scalar 23, Scalar 114])
      roundTrip @(ParameterStatus ByteString) @'ParameterStatusType (ParameterStatus (Scalar ("server_version", "18.0")))
      roundTrip @ParseComplete @'ParseCompleteType ParseComplete
      roundTrip @PortalSuspended @'PortalSuspendedType PortalSuspended
      roundTrip @ReadyForQuery @'ReadyForQueryType (ReadyForQuery TxFailedTransaction)
      roundTrip @(RowDescription ByteString) @'RowDescriptionType (RowDescription [rowField, rowField {rowFieldTableOid = Nothing, rowFieldColumnAttributeNumber = Nothing}])

  describe "choice" $ do
    it "dispatches on the message tag" $
      dispatch ("1" <> int32 4) `shouldBe` Right ["ParseComplete"]
    it "dispatches 'R' messages on the subcode after the length" $ do
      dispatch ("R" <> int32 8 <> int32 0) `shouldBe` Right ["AuthenticationOk"]
      dispatch ("R" <> int32 12 <> int32 5 <> "salt") `shouldBe` Right ["AuthenticationMD5Password \"salt\""]
    it "doesn't peek past the end of a short message" $
      dispatch ("2" <> int32 4) `shouldBe` Right ["BindComplete"]
    it "parses consecutive messages from one input" $
      dispatch ("1" <> int32 4 <> "2" <> int32 4 <> "Z" <> int32 5 <> "I")
        `shouldBe` Right ["ParseComplete", "BindComplete", "ReadyForQuery TxIdle"]
    it "rejects a message no alternative accepts" $
      dispatch ("D" <> int32 6 <> int16 0) `shouldSatisfy` isLeft

  describe "input" $ do
    it "parses messages arriving a byte at a time into small buffers" $ do
      source <- chunkedSource (map BS.singleton (BS.unpack conversation)) 8
      replies <- pullAll source emptyInputBuffer
      map fst replies `shouldBe` ["ParseComplete", "DataRow", "CommandComplete", "ReadyForQuery TxIdle"]
    it "keeps slices it returned valid after later reads" $ do
      source <- chunkedSource [BS.take 20 conversation, BS.drop 20 conversation] 16
      replies <- pullAll source emptyInputBuffer
      concatMap snd replies `shouldBe` [Just "42", Nothing, Just "a longer value", Just "SELECT 1"]

  describe "diagnostics" $ do
    it "decodes an error's fields, preferring the untranslated severity" $
      fmap
        (\d -> (diagnosticSeverity d, diagnosticCode d, diagnosticMessage d, diagnosticPosition d, diagnosticHint d))
        ( diagnostics
            [ Scalar (byte 'S', "FEHLER"),
              Scalar (byte 'V', "ERROR"),
              Scalar (byte 'C', "42P01"),
              Scalar (byte 'M', "relation does not exist"),
              Scalar (byte 'P', "15"),
              Scalar (byte 'Z', "a field the protocol doesn't define")
            ]
        )
        `shouldBe` Right (SeverityError, "42P01", "relation does not exist", Just 15, Nothing)
    it "rejects an error without a SQLSTATE" $
      diagnostics [Scalar (byte 'S', "ERROR"), Scalar (byte 'M', "no code")] `shouldSatisfy` isLeft

  describe "authentication" $ do
    it "answers an MD5 password request" $
      md5Password "user" "pencil" "salt" `shouldBe` "md5d3d9bcccbaa2fb5234570d4d0f22b055"
    it "computes SCRAM-SHA-256 as in RFC 7677" $ do
      let (clientFirst, client) = scramClientFirst "user" "rOprNGfwEbeRWgbNEkqO"
      clientFirst `shouldBe` "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
      case scramClientFinal "pencil" client rfc7677ServerFirst of
        Left err -> expectationFailure err
        Right (clientFinal, serverSignature) -> do
          clientFinal
            `shouldBe` "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
          scramVerifyServerFinal serverSignature "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=" `shouldBe` Right ()
          scramVerifyServerFinal serverSignature "v=AAAA" `shouldSatisfy` isLeft
          scramVerifyServerFinal serverSignature "e=invalid-proof" `shouldSatisfy` isLeft
    it "rejects a server nonce that doesn't extend the client's" $ do
      let (_, client) = scramClientFirst "user" "rOprNGfwEbeRWgbNEkqO"
      fmap fst (scramClientFinal "pencil" client "r=other,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096") `shouldSatisfy` isLeft

  describe "connection strings" $ do
    it "fills in libpq's defaults" $
      parse ""
        `shouldBe` Right
          ConnectionString
            { connectionHosts = Host "/tmp" Nothing 5432 :| [],
              connectionUser = "local",
              connectionPassword = Nothing,
              connectionDatabase = "local",
              connectionParameters = [],
              connectionTimeout = Nothing,
              connectionTargetSessionAttrs = AnySession,
              connectionProtocolVersion = protocolVersion3_0,
              connectionMinProtocolVersion = protocolVersion3_0,
              connectionTcpOptions = defaultTcpOptions
            }
    it "reads a URI" $ do
      let parsed = parse "postgresql://alice:p%40ss@db.example.com:6543/app?application_name=api&connect_timeout=10"
      hosts parsed `shouldBe` Right [Host "db.example.com" Nothing 6543]
      fmap connectionUser parsed `shouldBe` Right "alice"
      fmap connectionPassword parsed `shouldBe` Right (Just "p@ss")
      fmap connectionDatabase parsed `shouldBe` Right "app"
      fmap connectionParameters parsed `shouldBe` Right [("application_name", "api")]
      fmap connectionTimeout parsed `shouldBe` Right (Just 10)
    it "reads a key/value string, with quoted and escaped values" $ do
      let parsed = parse "host=db port = 5433 user='o\\'brien' password='a b' dbname=x\\ y"
      hosts parsed `shouldBe` Right [Host "db" Nothing 5433]
      fmap connectionUser parsed `shouldBe` Right "o'brien"
      fmap connectionPassword parsed `shouldBe` Right (Just "a b")
      fmap connectionDatabase parsed `shouldBe` Right "x y"
    it "reads several hosts, each with its port or the default" $
      hosts (parse "postgresql://h1:5432,h2:5433,[::1]/db")
        `shouldBe` Right [Host "h1" Nothing 5432, Host "h2" Nothing 5433, Host "::1" Nothing 5432]
    it "gives a single port to every host" $
      hosts (parse "host=h1,h2 port=5433") `shouldBe` Right [Host "h1" Nothing 5433, Host "h2" Nothing 5433]
    it "connects to a socket in a directory host" $ do
      fmap (fmap hostSocketAddress) (hosts (parse "host=/var/run/postgresql port=5433"))
        `shouldBe` Right [UnixAddress "/var/run/postgresql/.s.PGSQL.5433"]
      hosts (parse "postgresql://%2Fvar%2Frun%2Fpostgresql/db") `shouldBe` Right [Host "/var/run/postgresql" Nothing 5432]
    it "connects to hostaddr instead of looking up the host" $ do
      let parsed = hosts (parse "host=db hostaddr=10.0.0.1 port=5433")
      parsed `shouldBe` Right [Host "db" (Just "10.0.0.1") 5433]
      fmap (fmap hostSocketAddress) parsed `shouldBe` Right [TcpAddress "10.0.0.1" 5433]
    it "takes defaults from the environment" $ do
      let parsed =
            parseWith
              [ ("PGHOST", "envhost"),
                ("PGPORT", "5555"),
                ("PGUSER", "envuser"),
                ("PGDATABASE", "envdb"),
                ("PGPASSWORD", "secret"),
                ("PGAPPNAME", "envapp")
              ]
              "user=given"
      hosts parsed `shouldBe` Right [Host "envhost" Nothing 5555]
      fmap connectionUser parsed `shouldBe` Right "given"
      fmap connectionDatabase parsed `shouldBe` Right "envdb"
      fmap connectionPassword parsed `shouldBe` Right (Just "secret")
      fmap connectionParameters parsed `shouldBe` Right [("application_name", "envapp")]
    it "takes the last value given for an option" $ do
      fmap connectionTargetSessionAttrs (parse "postgresql://h/db?target_session_attrs=read-only&target_session_attrs=read-write")
        `shouldBe` Right ReadWriteSession
      hosts (parse "postgresql://h1/db?host=h2") `shouldBe` Right [Host "h2" Nothing 5432]
    it "names the application with fallback_application_name when application_name is missing" $ do
      fmap connectionParameters (parse "fallback_application_name=fallback")
        `shouldBe` Right [("application_name", "fallback")]
      fmap connectionParameters (parse "fallback_application_name=fallback application_name=app")
        `shouldBe` Right [("application_name", "app")]
    it "rejects strings that require TLS" $ do
      mapM_
        (\string -> parse string `shouldSatisfy` failsWith "requires TLS, which isn't supported")
        ["sslmode=require", "sslmode=verify-ca", "sslmode=verify-full", "requiressl=1", "postgresql://h/db?ssl=true"]
      parseWith [("PGSSLMODE", "verify-full")] "" `shouldSatisfy` failsWith "PGSSLMODE=verify-full requires TLS"
      parse "gssencmode=require" `shouldSatisfy` failsWith "requires GSSAPI encryption, which isn't supported"
      mapM_ (\string -> parse string `shouldSatisfy` isRight) ["sslmode=disable", "sslmode=allow", "sslmode=prefer"]
    it "rejects options it doesn't know or implement" $ do
      parse "foo=bar" `shouldSatisfy` failsWith "invalid connection option \"foo\""
      parse "postgresql://h/db?foo=bar" `shouldSatisfy` failsWith "invalid URI query parameter: \"foo\""
      parse "service=x" `shouldSatisfy` failsWith "service isn't supported"
      parse "require_auth=password" `shouldSatisfy` failsWith "require_auth isn't supported"
      parse "load_balance_hosts=random" `shouldSatisfy` failsWith "requires random load balancing"
      parse "passfile=/pgpass" `shouldSatisfy` failsWith "password files aren't supported"
      parse "passfile=/pgpass password=x" `shouldSatisfy` isRight
    it "reports malformed strings as libpq does" $ do
      parse "host" `shouldSatisfy` failsWith "missing \"=\" after \"host\" in connection info string"
      parse "password='abc" `shouldSatisfy` failsWith "unterminated quoted string"
      parse "postgresql://[::1/db" `shouldSatisfy` failsWith "end of string reached when looking for matching \"]\""
      parse "postgresql://%zz@h" `shouldSatisfy` failsWith "invalid percent-encoded token"
      parse "port=abc" `shouldSatisfy` failsWith "invalid port number: \"abc\""
      parse "host=h1,h2 port=1,2,3" `shouldSatisfy` failsWith "could not match 3 port numbers to 2 hosts"
      parse "target_session_attrs=master" `shouldSatisfy` failsWith "invalid target_session_attrs value: \"master\""
    it "waits at least two seconds for a host, as libpq does" $ do
      fmap connectionTimeout (parse "connect_timeout=1") `shouldBe` Right (Just 2)
      fmap connectionTimeout (parse "connect_timeout=0") `shouldBe` Right Nothing
    it "reads keepalive and protocol version options" $ do
      fmap (tcpKeepalives . connectionTcpOptions) (parse "keepalives=0") `shouldBe` Right False
      fmap (tcpKeepalivesIdle . connectionTcpOptions) (parse "keepalives_idle=30") `shouldBe` Right (Just 30)
      fmap connectionProtocolVersion (parse "max_protocol_version=3.2") `shouldBe` Right protocolVersion3_2
      parse "min_protocol_version=3.2" `shouldSatisfy` failsWith "min_protocol_version is greater than max_protocol_version"
    it "needs a user when the local one can't be looked up" $ do
      parseConnectionString (const Nothing) Nothing "" `shouldSatisfy` failsWith "a user has to be given"
      parseConnectionString (const Nothing) Nothing "user=u" `shouldSatisfy` isRight
    it "words failures as libpq does" $ do
      displayException (UnresolvedHost "no_host" "nodename nor servname provided, or not known")
        `shouldBe` "could not translate host name \"no_host\" to address: nodename nor servname provided, or not known"
      displayException (ConnectFailure [(Host "/tmp" Nothing 5432, TimedOut), (Host "db" Nothing 5433, Unsuitable "session is read-only")])
        `shouldBe` "connection to server on socket \"/tmp/.s.PGSQL.5432\" failed: timeout expired\nconnection to server at \"db\", port 5433 failed: session is read-only"

rfc7677ServerFirst :: ByteString
rfc7677ServerFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"

-- | A server's replies to an extended query.
conversation :: ByteString
conversation =
  BS.concat
    [ outputByteString (serialize @ParseComplete @'ParseCompleteType ParseComplete),
      outputByteString (serialize @(DataRow ByteString) @'DataRowType (DataRow [Just "42", Nothing, Just "a longer value"])),
      outputByteString (serialize @(CommandComplete ByteString) @'CommandCompleteType (CommandComplete "SELECT 1")),
      outputByteString (serialize @ReadyForQuery @'ReadyForQueryType (ReadyForQuery TxIdle))
    ]

type QueryReplies =
  '[ 'PayloadField 'ParseCompleteType,
     'PayloadField 'DataRowType,
     'PayloadField 'CommandCompleteType,
     'PayloadField 'ReadyForQueryType
   ]

-- | Pulls messages until the source ends, keeping the byte strings they
-- carry.
pullAll :: InputSource -> InputBuffer -> IO [(String, [Maybe ByteString])]
pullAll source buffer = do
  (reply, buffer') <- runInput source buffer (parseChoice @QueryReplies replies)
  case reply of
    ("ReadyForQuery TxIdle", _) -> pure [reply]
    _ -> (reply :) <$> pullAll source buffer'
  where
    replies =
      (\ParseComplete -> ("ParseComplete", []))
        :|: (\(DataRow values) -> ("DataRow", values))
        :|: (\(CommandComplete tag) -> ("CommandComplete", [Just tag]))
        :|: (\(ReadyForQuery status) -> ("ReadyForQuery " <> show status, []))
        :|: NoAlternatives

-- | A source that delivers the chunks one read at a time, in pieces no
-- larger than a read asks for.
chunkedSource :: [ByteString] -> Int -> IO InputSource
chunkedSource chunks readSize = do
  queue <- newIORef chunks
  pure
    InputSource
      { readInputInto = \pointer size -> do
          remaining <- readIORef queue
          case remaining of
            [] -> pure 0
            chunk : others -> do
              let (now, later) = BS.splitAt size chunk
              writeIORef queue (if BS.null later then others else later : others)
              BS.useAsCStringLen now $ \(origin, count) -> copyBytes pointer (castPtr origin) count
              pure (BS.length now),
        inputReadSize = readSize
      }

-- | Messages a client may receive after a Parse, Bind and Sync, plus two
-- authentication messages that share the 'R' tag.
type Replies =
  '[ 'PayloadField 'AuthenticationOkType,
     'PayloadField 'AuthenticationMD5PasswordType,
     'PayloadField 'ParseCompleteType,
     'PayloadField 'BindCompleteType,
     'PayloadField 'ReadyForQueryType
   ]

dispatch :: ByteString -> Either String [String]
dispatch input = do
  (reply, rest) <- runBodyParser input (parseChoice @Replies replies)
  if BS.null rest then pure [reply] else (reply :) <$> dispatch rest
  where
    replies =
      (\AuthenticationOk -> "AuthenticationOk")
        :|: (\(AuthenticationMD5Password salt) -> "AuthenticationMD5Password " <> show (salt :: ByteString))
        :|: (\ParseComplete -> "ParseComplete")
        :|: (\BindComplete -> "BindComplete")
        :|: (\(ReadyForQuery status) -> "ReadyForQuery " <> show status)
        :|: NoAlternatives

encodes :: forall a s. Serializable a s => a -> ByteString -> Expectation
encodes message expected = outputByteString (serialize @a @s message) `shouldBe` expected

decodes :: forall a s. (Parsable a s, Eq a, Show a) => ByteString -> a -> Expectation
decodes input expected = parseEnvelope @a @s input `shouldBe` Right (Envelope expected)

roundTrip :: forall a s. (Parsable a s, Serializable a s, Eq a, Show a) => a -> Expectation
roundTrip message = parseEnvelope @a @s (outputByteString (serialize @a @s message)) `shouldBe` Right (Envelope message)

startupMessage :: StartupMessage ByteString
startupMessage =
  StartupMessage
    protocolVersion3_2
    [ Scalar ("user", "postgres"),
      Scalar ("database", "postgres")
    ]

startupMessageBytes :: ByteString
startupMessageBytes = int32 41 <> int32 196610 <> "user\0postgres\0database\0postgres\0\0"

cancelRequest :: CancelRequest ByteString
cancelRequest = CancelRequest (Scalar (Scalar 42, Scalar (BS.pack [0xDE, 0xAD, 0xBE, 0xEF])))

cancelRequestBytes :: ByteString
cancelRequestBytes = int32 16 <> int32 80877102 <> int32 42 <> BS.pack [0xDE, 0xAD, 0xBE, 0xEF]

errorFields :: [ErrorNoticeField ByteString]
errorFields = [Scalar (byte 'S', "ERROR"), Scalar (byte 'C', "42P01"), Scalar (byte 'M', "relation does not exist")]

rowField :: RowField ByteString
rowField =
  RowField
    { rowFieldName = "id",
      rowFieldTableOid = Just (Scalar 16384),
      rowFieldColumnAttributeNumber = Just 1,
      rowFieldTypeOid = Scalar 23,
      rowFieldTypeSize = 4,
      rowFieldTypeModifier = -1,
      rowFieldFormat = BinaryFormat
    }

int32 :: Int32 -> ByteString
int32 value = BS.pack [fromIntegral (value `div` 16777216), fromIntegral (value `div` 65536), fromIntegral (value `div` 256), fromIntegral value]

int16 :: Int32 -> ByteString
int16 value = BS.drop 2 (int32 value)

byte :: Char -> Word8
byte = fromIntegral . fromEnum

parse :: ByteString -> Either String ConnectionString
parse = parseWith []

parseWith :: [(String, String)] -> ByteString -> Either String ConnectionString
parseWith environment = parseConnectionString (`lookup` environment) (Just "local")

hosts :: Either String ConnectionString -> Either String [Host]
hosts = fmap (toList . connectionHosts)

failsWith :: String -> Either String a -> Bool
failsWith message = either (message `isInfixOf`) (const False)

isRight :: Either a b -> Bool
isRight = not . isLeft

isLeft :: Either a b -> Bool
isLeft = either (const True) (const False)
