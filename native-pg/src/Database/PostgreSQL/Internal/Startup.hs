{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

-- | The startup of a session, on a socket that is connected: the startup message, the authentication that the server
-- asks for, and the parameters and the key that it reports, up to the point where it is ready for a statement.
--
-- Every message is read through the cursors, whole and copied, in whichever size of buffer there is: there are few of
-- them, and they are small. The startup is a loop over a phase, each message of the server taking it to the next, with
-- what the client answers.
module Database.PostgreSQL.Internal.Startup
  ( startup,
  )
where

import Control.Arrow (arr, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, SomeException, fromException, throwIO)
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Maybe (maybeToList)
import Database.PostgreSQL.Errors (ConnectError (..), ServerError (..))
import Database.PostgreSQL.Internal.Authentication
  ( ChannelBinding (..),
    ScramClient,
    md5Password,
    newScramNonce,
    scramClientFinal,
    scramClientFirst,
    scramMechanism,
    scramVerifyServerFinal,
  )
import Database.PostgreSQL.Internal.Diagnostics (bodyDiagnostics)
import Database.PostgreSQL.Internal.Frontend (sendOutput, sync)
import Database.PostgreSQL.Internal.Protocol.Codec.Cursor
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Internal.Protocol.Messages hiding (FramedMessage)
import Database.PostgreSQL.Internal.Carrier (Carrier)
import Database.PostgreSQL.Session.Settings (ChannelBindingMode (..), ConnectSettings (..), ServerInfo (..), TargetSessionAttrs (..))
import GHC.IO.Exception (IOException (..))
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Classes (Effect (..), Runtime, Scope (..))
import Network.Streaming.Combinators (pattern Again, pattern Finish, Step, consume, iterateWith, keeping)
import Network.Streaming.Reader (Reader, newReader)
import Network.Streaming.With (With (..), asks, lift, withOnly)
import System.IO.Error (isUserError)

-- | The messages of the startup, which are told apart by their tags, and each read whole.
type StartupMessages =
  'PayloadChoice
    '[ FramedMessage 'R' 'RemainingBytesAtom,
       FramedMessage 'E' 'RemainingBytesAtom,
       FramedMessage 'N' 'RemainingBytesAtom,
       FramedMessage 'S' 'RemainingBytesAtom,
       FramedMessage 'K' 'RemainingBytesAtom,
       FramedMessage 'Z' 'RemainingBytesAtom,
       FramedMessage 'v' 'RemainingBytesAtom,
       FramedMessage '1' 'RemainingBytesAtom,
       FramedMessage '2' 'RemainingBytesAtom,
       FramedMessage 'D' 'RemainingBytesAtom,
       FramedMessage 'C' 'RemainingBytesAtom
     ]

-- | What the startup works with: the carrier, the buffer that its messages are rendered into and received in, which,
-- one after the other, need no more than one, and the reader of the replies.
data Setup = Setup !Carrier !Buffer !(Reader Carrier)

-- | Where the startup is, and what it keeps: the version of the protocol, whichever the server settled on.
data Phase
  = -- | The answer to the startup message: the request for a method of authentication, or its end.
    Negotiating ProtocolVersion
  | -- | The server's challenge, after the first message of SCRAM.
    Challenged ProtocolVersion ScramClient
  | -- | The server's signature, which it has to give, after the client's proof.
    Verifying ProtocolVersion ByteString
  | -- | The end of the authentication, after the password or the exchange.
    Authenticated ProtocolVersion
  | -- | The parameters that the server reports, last first, and its key for cancelling, until it is ready.
    Initializing ProtocolVersion [(ByteString, ByteString)] (Maybe (BackendKey ByteString))
  | -- | The answer to the question that the server's report left open, until the server is ready again: the text of the
    -- first value of the first row if there is one, or the server's failure, which is reported once it is ready.
    Checking ServerInfo Question (Either ServerError (Maybe ByteString))

-- | What the server's report didn't say about whether it qualifies, which the client asks: the statement, the text that
-- its first value has when the answer is yes, whether the host has to say yes, and why it isn't suitable if not.
data Question = Question
  { askStatement :: ByteString,
    askYes :: ByteString,
    askWanted :: Bool,
    askWhy :: String
  }

-- | What the server's report says about whether it qualifies.
data Verdict = Qualifies | Disqualified String | Asks Question

-- | Starts the session: it sends the startup message, authenticates and reads the server's report, up to its being
-- ready for a statement, or says why it couldn't. A server that ends the stream, and one that says what the client
-- doesn't understand, are ways that it couldn't.
startup :: (Runtime arr) => ConnectSettings -> arr (Carrier, Buffer) (Either ConnectError ServerInfo)
startup settings =
  tryA (now (\(carrier, buffer) -> Setup carrier buffer <$> newReader carrier buffer) >>> withOnly (opening settings))
    >>> now (either failed (pure . Right))

-- | What a failure of the startup is: the server's rejection and the client's own are as they were thrown, and a
-- stream that ended or broke, and messages that don't follow the protocol, are the connection's.
failed :: SomeException -> IO (Either ConnectError a)
failed failure
  | Just connecting <- fromException failure = pure (Left connecting)
  | Just stream <- fromException failure = pure (Left (Broke (brokenBy stream)))
  | Just (CursorError reason) <- fromException failure = pure (Left (ProtocolError reason))
  | otherwise = throwIO failure
  where
    brokenBy :: IOException -> String
    brokenBy io
      | isUserError io = "server closed the connection unexpectedly"
      | otherwise = ioe_description io

opening :: (Runtime arr) => ConnectSettings -> With Setup arr () ServerInfo
opening settings =
  const (startupMessage settings)
    ^>> send
    >>> const (Negotiating (connectProtocolVersion settings))
    ^>> loop settings

-- | The loop: the message that comes, and what the phase makes of it.
loop :: (Runtime arr) => ConnectSettings -> With Setup arr Phase ServerInfo
loop settings =
  iterateWith
    (keeping received >>> now (uncurry (react settings)))
    (answered ^>> (returnA ||| consume send))
  where
    answered (answer, phase) = maybe (Left phase) (Right . (,phase)) answer

-- | Sends the output, rendered into the buffer.
send :: (Runtime arr) => With Setup arr Output ()
send = With ((\(Setup carrier buffer _, output) -> (carrier, buffer, output)) ^>> sendOutput)

-- | The next message of the server: its tag, and its body.
received :: forall arr. (Runtime arr) => With Setup arr () (Char, ByteString)
received = asks (arr (\(Setup _ _ reader) -> reader)) >>> lift readMessage
  where
    readMessage :: arr (Reader Carrier) (Char, ByteString)
    readMessage =
      (messages :: Reader Carrier -> At StartupMessages Carrier)
        ^>> withOnly (asks message >>> With (cases (tagged 'R' :> tagged 'E' :> tagged 'N' :> tagged 'S' :> tagged 'K' :> tagged 'Z' :> tagged 'v' :> tagged '1' :> tagged '2' :> tagged 'D' :> tagged 'C' :> NoCases)))
    tagged :: Char -> arr (At 'RemainingBytesAtom Carrier) (Char, ByteString)
    tagged tag = remaining >>^ (tag,)

-- | What the client makes of a message of the server in the phase: the server's report when it is ready, or what to
-- answer, if anything, and the phase that follows. A rejection and what the client can't accept are thrown.
react :: ConnectSettings -> (Char, ByteString) -> Phase -> IO (Step (Maybe Output, Phase) ServerInfo)
react settings (tag, body) phase = case (tag, phase) of
  ('E', Checking info question _) -> either (throwIO . ProtocolError) (continuing . Checking info question . Left . ServerError) (bodyDiagnostics body)
  ('E', _) -> either (throwIO . ProtocolError) (throwIO . ConnectionRejected . ServerError) (bodyDiagnostics body)
  ('N', _) -> continuing phase
  ('v', Negotiating _) -> continuing (Negotiating (versionOf (be32 body)))
  ('R', Negotiating version) -> requested version
  ('R', Challenged version client) | code == 11 -> do
    password <- requirePassword
    case scramClientFinal password client rest of
      Left reason -> throwIO (AuthenticationError reason)
      Right (final, signature) -> answering (serialize (SASLResponse final)) (Verifying version signature)
  ('R', Verifying version signature) | code == 12 -> case scramVerifyServerFinal signature rest of
    Left reason -> throwIO (AuthenticationError reason)
    Right () -> continuing (Authenticated version)
  ('R', Authenticated version) | code == 0 -> continuing (Initializing version [] Nothing)
  ('S', Initializing version parameters key) -> continuing (Initializing version (parameterOf body : parameters) key)
  ('K', Initializing version parameters _) -> continuing (Initializing version parameters (Just (keyOf body)))
  ('Z', Initializing version parameters key) -> qualifying (ServerInfo version (reverse parameters) key)
  ('1', Checking {}) -> continuing phase
  ('2', Checking {}) -> continuing phase
  ('D', Checking info question (Right _)) -> continuing (Checking info question (Right (Just (firstValue body))))
  ('C', Checking {}) -> continuing phase
  ('Z', Checking _ _ (Left failure)) -> throwIO (CheckFailed failure)
  ('Z', Checking info question (Right answer))
    | (answer == Just (askYes question)) == askWanted question -> pure (Finish info)
    | otherwise -> throwIO (Unsuitable (askWhy question))
  _ -> throwIO (ProtocolError ("unexpected message " <> show tag <> " in the startup"))
  where
    code = be32 body
    rest = BS.drop 4 body
    required = connectChannelBinding settings == ChannelBindingRequire
    continuing next = pure (Again (Nothing, next))
    answering output next = pure (Again (Just output, next))
    -- The end of the startup: the server's report is what the host qualifies by, or is asked what it left out.
    qualifying info = case verdict settings info of
      Qualifies -> pure (Finish info)
      Disqualified why -> throwIO (Unsuitable why)
      Asks question -> answering (questionOutput question) (Checking info question (Right Nothing))
    requirePassword = maybe (throwIO (AuthenticationError "fe_sendauth: no password supplied")) pure (connectPassword settings)
    -- The server's first message: what it asks for.
    requested version = case code of
      0
        | required -> throwIO (AuthenticationError "channel binding required, but server authenticated client without channel binding")
        | otherwise -> continuing (Initializing version [] Nothing)
      10
        | required -> throwIO (AuthenticationError "channel binding is required, but server did not offer an authentication method that supports channel binding")
        | scramMechanism `elem` takeWhile (not . BS.null) (BS.split 0 rest) -> do
            _ <- requirePassword
            (clientFirst, client) <- scramClientFirst WithoutChannelBinding "" <$> newScramNonce
            answering (serialize (SASLInitialResponse scramMechanism (Just clientFirst))) (Challenged version client)
        | otherwise -> throwIO (UnsupportedAuthentication "none of the server's SASL authentication mechanisms are supported")
      _ | required -> throwIO (AuthenticationError "channel binding required but not supported by server's authentication request")
      3 -> requirePassword >>= \password -> answering (serialize (PasswordMessage password)) (Authenticated version)
      5 -> requirePassword >>= \password -> answering (serialize (PasswordMessage (md5Password (connectUser settings) password (BS.take 4 rest)))) (Authenticated version)
      2 -> throwIO (UnsupportedAuthentication "Kerberos 5 authentication not supported")
      7 -> throwIO (UnsupportedAuthentication "GSSAPI authentication not supported")
      9 -> throwIO (UnsupportedAuthentication "SSPI authentication not supported")
      other -> throwIO (UnsupportedAuthentication ("authentication method " <> show other <> " not supported"))
-- Not inlined: it is plain IO, the same for every runtime, and inlined it was copied into each runtime's startup.
{-# NOINLINE react #-}

-- | Whether the server qualifies by its report: it has the protocol version that the settings ask for, and the session
-- attributes. A server that doesn't report what the attributes are made of, as those before PostgreSQL 14 don't, is asked.
verdict :: ConnectSettings -> ServerInfo -> Verdict
verdict settings info
  | serverProtocolVersion info < connectMinProtocolVersion settings =
      Disqualified
        ( "server only supports protocol version "
            <> version (serverProtocolVersion info)
            <> ", but min_protocol_version was set to "
            <> version (connectMinProtocolVersion settings)
        )
  | otherwise = case connectTargetSessionAttrs settings of
      ReadWriteSession -> readOnly False "session is read-only"
      ReadOnlySession -> readOnly True "session is not read-only"
      PrimarySession -> standby False "server is in hot standby mode"
      StandbySession -> standby True "server is not in hot standby mode"
      -- The two passes of prefer-standby are made by the caller.
      _ -> Qualifies
  where
    reported name = lookup name (serverParameters info)
    -- A session is read-only in a standby, and if it is by default.
    readOnly wanted why = case (reported "in_hot_standby", reported "default_transaction_read_only") of
      (Just inStandby, Just readOnlyDefault) -> decided wanted why (inStandby == "on" || readOnlyDefault == "on")
      _ -> Asks (Question "SHOW transaction_read_only" "on" wanted why)
    standby wanted why = case reported "in_hot_standby" of
      Just inStandby -> decided wanted why (inStandby == "on")
      Nothing -> Asks (Question "SELECT pg_catalog.pg_is_in_recovery()" "t" wanted why)
    decided wanted why actual = if actual == wanted then Qualifies else Disqualified why
    version (ProtocolVersion major minor) = show major <> "." <> show minor

-- | The question as an unnamed statement of the extended protocol, with no parameters, whose value comes as text.
questionOutput :: Question -> Output
questionOutput question =
  serialize (Parse "" (askStatement question) [])
    <> serialize (Bind (BindBody "" "" [] ([] :: [Maybe Parameter]) []))
    <> serialize (Execute "" 0 :: Execute ByteString)
    <> sync

-- | The text of the first value of a DataRow: after the number of its columns, a length and the bytes. A NULL, whose
-- length is -1, has none.
firstValue :: ByteString -> ByteString
firstValue body = BS.take (be32 (BS.drop 2 body)) (BS.drop 6 body)

-- | The startup message: the user, the database if there is one, and the other parameters.
startupMessage :: ConnectSettings -> Output
startupMessage settings =
  serialize
    ( StartupMessage (connectProtocolVersion settings) $
        map Scalar $
          ("user", connectUser settings)
            : maybeToList (("database",) <$> connectDatabase settings)
            <> connectParameters settings
    )

-- | The number of the first four bytes, as the carrier has it: the most significant first.
be32 :: ByteString -> Int
be32 = BS.foldl' (\total byte -> total `shiftL` 8 .|. fromIntegral byte) 0 . BS.take 4

-- | The version that a number says, the major one in its upper half.
versionOf :: Int -> ProtocolVersion
versionOf number = ProtocolVersion (fromIntegral (number `shiftR` 16)) (fromIntegral (number .&. 0xffff))

-- | What a ParameterStatus says: the name of the parameter, and its value.
parameterOf :: ByteString -> (ByteString, ByteString)
parameterOf body = (name, BS.takeWhile (/= 0) (BS.drop 1 rest))
  where
    (name, rest) = BS.break (== 0) body

-- | What a BackendKeyData says: the process, and the secret that is the rest of the message.
keyOf :: ByteString -> BackendKey ByteString
keyOf body = Scalar (Scalar (fromIntegral (be32 body)), Scalar (BS.drop 4 body))
