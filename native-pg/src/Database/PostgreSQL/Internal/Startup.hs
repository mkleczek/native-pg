{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

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

import Control.Arrow (arr, first, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
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
import Database.PostgreSQL.Internal.Frontend (sendOutput)
import Database.PostgreSQL.Internal.Protocol.Codec.Cursor
import Database.PostgreSQL.Internal.Protocol.Codec.Output (Output)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Internal.Protocol.Messages hiding (FramedMessage)
import Database.PostgreSQL.Session.Settings (ChannelBindingMode (..), ConnectSettings (..), ServerInfo (..))
import GHC.IO.Exception (IOException (..))
import Network.Socket (Socket)
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Classes (Effect (..), Runtime, Scope (..))
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
       FramedMessage 'v' 'RemainingBytesAtom
     ]

-- | What the startup works with: the socket, the buffer that its messages are rendered into and received in, which,
-- one after the other, need no more than one, and the reader of the replies.
data Setup = Setup !Socket !Buffer !(Reader Socket)

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

-- | Starts the session: it sends the startup message, authenticates and reads the server's report, up to its being
-- ready for a statement, or says why it couldn't. A server that ends the stream, and one that says what the client
-- doesn't understand, are ways that it couldn't.
startup :: (Runtime arr) => ConnectSettings -> arr (Socket, Buffer) (Either ConnectError ServerInfo)
startup settings =
  tryA (now (\(sock, buffer) -> Setup sock buffer <$> newReader sock buffer) >>> withOnly (opening settings))
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
opening settings = arr (const (startupMessage settings)) >>> send >>> arr (const (Negotiating (connectProtocolVersion settings))) >>> loop settings

-- | The loop: the message that comes, and what the phase makes of it.
loop :: (Runtime arr) => ConnectSettings -> With Setup arr Phase ServerInfo
loop settings = (returnA &&& (arr (const ()) >>> received)) >>> now (uncurry (react settings)) >>> (returnA ||| continuing)
  where
    continuing = (\(answer, phase) -> maybe (Left phase) (\output -> Right (output, phase)) answer) ^>> (loop settings ||| (first send >>> arr snd >>> loop settings))

-- | Sends the output, rendered into the buffer.
send :: (Runtime arr) => With Setup arr Output ()
send = With ((\(Setup sock buffer _, output) -> (sock, buffer, output)) ^>> sendOutput)

-- | The next message of the server: its tag, and its body.
received :: forall arr. (Runtime arr) => With Setup arr () (Char, ByteString)
received = asks (arr (\(Setup _ _ reader) -> reader)) >>> lift readMessage
  where
    readMessage :: arr (Reader Socket) (Char, ByteString)
    readMessage =
      (messages :: Reader Socket -> At StartupMessages Socket)
        ^>> withOnly (asks message >>> With (cases (tagged 'R' :> tagged 'E' :> tagged 'N' :> tagged 'S' :> tagged 'K' :> tagged 'Z' :> tagged 'v' :> NoCases)))
    tagged :: Char -> arr (At 'RemainingBytesAtom Socket) (Char, ByteString)
    tagged tag = remaining >>^ (tag,)

-- | What the client makes of a message of the server in the phase: the server's report when it is ready, or what to
-- answer, if anything, and the phase that follows. A rejection and what the client can't accept are thrown.
react :: ConnectSettings -> Phase -> (Char, ByteString) -> IO (Either ServerInfo (Maybe Output, Phase))
react settings phase (tag, body) = case (tag, phase) of
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
  ('Z', Initializing version parameters key) -> pure (Left (ServerInfo version (reverse parameters) key))
  _ -> throwIO (ProtocolError ("unexpected message " <> show tag <> " in the startup"))
  where
    code = be32 body
    rest = BS.drop 4 body
    required = connectChannelBinding settings == ChannelBindingRequire
    continuing next = pure (Right (Nothing, next))
    answering output next = pure (Right (Just output, next))
    requirePassword = maybe (throwIO (AuthenticationError "the server asks for a password, but none is set")) pure (connectPassword settings)
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
        | otherwise -> throwIO (UnsupportedAuthentication ("SASL with " <> show (takeWhile (not . BS.null) (BS.split 0 rest))))
      _ | required -> throwIO (AuthenticationError "channel binding required but not supported by server's authentication request")
      3 -> requirePassword >>= \password -> answering (serialize (PasswordMessage password)) (Authenticated version)
      5 -> requirePassword >>= \password -> answering (serialize (PasswordMessage (md5Password (connectUser settings) password (BS.take 4 rest)))) (Authenticated version)
      2 -> throwIO (UnsupportedAuthentication "Kerberos V5")
      7 -> throwIO (UnsupportedAuthentication "GSSAPI")
      9 -> throwIO (UnsupportedAuthentication "SSPI")
      other -> throwIO (ProtocolError ("unknown authentication request " <> show other))

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

-- | The number of the first four bytes, as the wire has it: the most significant first.
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
