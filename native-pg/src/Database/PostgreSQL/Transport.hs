-- At -O2, liberate-case copies the receive loop once for each place it calls
-- itself, and one loop is enough. At -O1 the flag does nothing.
{-# OPTIONS_GHC -fno-liberate-case #-}

-- | Public transport API.
module Database.PostgreSQL.Transport
  ( Address (..),
    TcpOptions (..),
    defaultTcpOptions,
    TlsClient (..),
    TlsLink (..),
    Handshake (..),
    shake,
    TlsSettings (..),
    ServerVerification (..),
    RootCertificates (..),
    TlsVersion (..),
    TlsError (..),
    serverEndPoint,
  )
where

import Control.Exception (Exception (..))
import Crypto.Hash (HashAlgorithm, SHA224 (..), SHA256 (..), SHA384 (..), SHA512 (..), hashWith)
import Data.Bits (shiftL, testBit, (.&.), (.|.))
import qualified Data.ByteArray as ByteArray
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word (Word8)
import Foreign.Ptr (Ptr)
import Network.Socket (HostName, PortNumber, Socket)
import Network.Streaming.Classes (Effect, Link (..))
import Network.Streaming.NotImplemented (notImplemented)

data Address
  = -- | A host name or IP address, and a port.
    TcpAddress HostName PortNumber
  | -- | The path of a Unix-domain socket, e.g.
    -- @/var/run/postgresql/.s.PGSQL.5432@.
    UnixAddress FilePath
  deriving (Eq, Show)

-- | Options for TCP connections. Unix-domain sockets ignore them.
data TcpOptions = TcpOptions
  { -- | Probe an idle connection, so that a peer that vanished without closing
    -- it is noticed.
    tcpKeepalives :: Bool,
    -- | Seconds of idleness before the first probe, seconds between probes,
    -- and how many unanswered probes end the connection. 'Nothing' keeps the
    -- system's default, as do platforms without the option.
    tcpKeepalivesIdle :: Maybe Int,
    tcpKeepalivesInterval :: Maybe Int,
    tcpKeepalivesCount :: Maybe Int,
    -- | Milliseconds that sent data may stay unacknowledged before the
    -- connection is closed, on platforms with the option.
    tcpUserTimeout :: Maybe Int
  }
  deriving (Eq, Show)

-- | Keepalives with the system's timings, as libpq has by default.
defaultTcpOptions :: TcpOptions
defaultTcpOptions = TcpOptions True Nothing Nothing Nothing Nothing

-- | The TLS implementation that a connection starts TLS with, which native-pg-tls
-- supplies: without one, a connection can't encrypt, as libpq built without TLS
-- can't. It starts a session over a connected socket, for the settings and the server's
-- name, which has not shaken hands yet.
newtype TlsClient = TlsClient {startTls :: TlsSettings -> HostName -> Socket -> IO TlsLink}

-- | A TLS session over a socket, as a TLS client makes it: its calls, none of which
-- waits. Its 'Link' instance waits for them, by blocking on the socket's events while
-- they find nothing ready, and 'shake' does the handshake the same way.
data TlsLink = TlsLink
  { -- | The socket that the session is over, whose events say when a call may find something.
    linkSocket :: !Socket,
    -- | Receives decrypted bytes into the memory, up to the size: how many, 0 at the end of
    -- the stream, or 'Nothing' if none are ready.
    linkTryReceive :: !(Ptr Word8 -> Int -> IO (Maybe Int)),
    -- | Encrypts what it can of the pieces and writes what the socket takes: how many bytes of
    -- the pieces it took. What it took and has not written waits for 'linkFlush'.
    linkTrySend :: !([ByteString] -> IO Int),
    -- | Writes what was taken and not written yet: 'True' once none is left.
    linkFlush :: !(IO Bool),
    -- | Whether input is in hand: decrypted bytes, or the end of the stream. It reads what the
    -- socket has, and takes none of the decrypted bytes.
    linkHasInput :: !(IO Bool),
    -- | Does what the handshake can do next, and says what it needs.
    linkHandshake :: !(IO Handshake),
    -- | Tells the peer that the output ends, if the socket takes it at once, and ends the
    -- socket's output.
    linkShutdownSend :: !(IO ()),
    -- | Tells the peer that the session ends, if the socket takes it at once, closes the socket,
    -- and frees the session.
    linkEnd :: !(IO ())
  }

-- | What the handshake needs to go on, or its end with the data that SCRAM's channel binding
-- takes, for tls-server-end-point: the bytes, or why there are none, or none.
data Handshake
  = -- | Bytes from the socket.
    NeedsInput
  | -- | Room in the socket.
    NeedsRoom
  | Complete (Maybe (Either String ByteString))

-- | A stub, until the code is there: the handshake of the link, which waits for the
-- socket as the handshake needs, and gives the data for channel binding.
shake :: (Effect arr) => arr TlsLink (Maybe (Either String ByteString))
shake = notImplemented "the TLS handshake"

-- | A stub, until the code is there.
instance (Effect arr) => Link arr TlsLink where
  receive = notImplemented "a receive over TLS"
  sendAll = notImplemented "a send over TLS"
  awaitInput = notImplemented "a wait for input over TLS"
  shutdownSend = notImplemented "the end of output over TLS"
  close = notImplemented "closing a TLS link"

-- | How a TLS link is configured.
data TlsSettings = TlsSettings
  { tlsVerification :: ServerVerification,
    -- | A certificate chain and its private key, in PEM files, for the
    -- server to authenticate the client with, if it asks.
    tlsClientCertificate :: Maybe (FilePath, FilePath),
    -- | Certificate revocation lists, in PEM files, which every certificate
    -- of the server's chain is checked against when it's verified.
    tlsRevocationLists :: [FilePath],
    -- | The versions of TLS the connection may use.
    tlsVersions :: [TlsVersion],
    -- | Whether to send the server's name, unless it's an IP address, as
    -- libpq's @sslsni@ asks.
    tlsServerNameIndication :: Bool,
    -- | Whether the server has to choose ALPN's @postgresql@, as it does
    -- when TLS starts directly.
    tlsAlpnRequired :: Bool
  }
  deriving (Eq, Show)

-- | The versions of TLS rustls has.
data TlsVersion = Tls12 | Tls13
  deriving (Eq, Ord, Show, Enum, Bounded)

-- | How the server's certificate is checked.
data ServerVerification
  = -- | Not at all: the connection is encrypted, but the server may not be
    -- the one asked for, as with libpq's @sslmode=require@.
    NoVerification
  | -- | Against root certificates, and the server's name, as with libpq's
    -- @sslmode=verify-full@.
    VerifyFull RootCertificates
  deriving (Eq, Show)

-- | The certificates of the authorities a server's certificate has to come
-- from.
data RootCertificates
  = -- | In a PEM file.
    RootCertificateFile FilePath
  | -- | The system's, as with libpq's @sslrootcert=system@.
    SystemRootCertificates
  deriving (Eq, Show)

-- | TLS failed: the handshake, the server's certificate, or a record.
-- 'displayException' words it as libpq does.
newtype TlsError = TlsError String
  deriving (Show)

instance Exception TlsError where
  displayException (TlsError reason) = "SSL error: " <> reason

-- | Channel binding's data for tls-server-end-point (RFC 5929): the hash of
-- the server's certificate, given in DER, with the hash its signature
-- algorithm uses, or SHA-256 for MD5 and SHA-1. Fails for a certificate
-- whose signature algorithm has no hash, e.g. Ed25519, as PostgreSQL does.
serverEndPoint :: ByteString -> Either String ByteString
serverEndPoint certificate = do
  (_, body, _) <- element 0x30 certificate
  -- The certificate's signed part, then its signature's algorithm.
  (_, _, afterSigned) <- element 0x30 body
  (_, algorithm, _) <- element 0x30 afterSigned
  (_, identifier, _) <- element 0x06 algorithm
  case lookup identifier hashes of
    Just hash -> Right (hash certificate)
    Nothing -> Left "could not determine the hash of the server certificate's signature algorithm"
  where
    hashes =
      [ (rsa 4, digest SHA256), -- MD5
        (rsa 5, digest SHA256), -- SHA-1
        (rsa 14, digest SHA224),
        (rsa 11, digest SHA256),
        (rsa 12, digest SHA384),
        (rsa 13, digest SHA512),
        (ecdsa [1], digest SHA256), -- SHA-1
        (ecdsa [3, 1], digest SHA224),
        (ecdsa [3, 2], digest SHA256),
        (ecdsa [3, 3], digest SHA384),
        (ecdsa [3, 4], digest SHA512)
      ]
    -- 1.2.840.113549.1.1.n and 1.2.840.10045.4.n
    rsa n = BS.pack [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, n]
    ecdsa ns = BS.pack ([0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04] <> ns)
    digest :: HashAlgorithm algorithm => algorithm -> ByteString -> ByteString
    digest algorithm = ByteArray.convert . hashWith algorithm

-- | A DER element with the tag: its header, its content, and what follows.
element :: Word8 -> ByteString -> Either String (ByteString, ByteString, ByteString)
element tag bytes = case BS.unpack (BS.take 2 bytes) of
  [actual, first]
    | actual /= tag -> Left "malformed certificate"
    | not (testBit first 7) -> split 2 (fromIntegral first)
    | otherwise -> do
        let count = fromIntegral (first .&. 0x7f)
            lengthBytes = BS.take count (BS.drop 2 bytes)
        if count == 0 || count > 4 || BS.length lengthBytes < count
          then Left "malformed certificate"
          else split (2 + count) (BS.foldl' (\total byte -> total `shiftL` 8 .|. fromIntegral byte) 0 lengthBytes)
  _ -> Left "malformed certificate"
  where
    split headerLength contentLength
      | BS.length bytes < headerLength + contentLength = Left "malformed certificate"
      | otherwise =
          let (header, rest) = BS.splitAt headerLength bytes
              (content, after) = BS.splitAt contentLength rest
           in Right (header, content, after)
