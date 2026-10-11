{-# LANGUAGE OverloadedStrings #-}

-- | The client side of PostgreSQL's password authentication methods.
--
-- SASLprep isn't applied to SCRAM passwords, so a password whose SASLprep
-- form differs from its bytes (only possible with non-ASCII characters)
-- fails to authenticate.
module Database.PostgreSQL.Authentication
  ( ScramClient,
    md5Password,
    newScramNonce,
    scramClientFinal,
    scramClientFirst,
    scramMechanism,
    scramVerifyServerFinal,
    serverEndPoint,
  )
where

import Control.Monad (unless)
import Crypto.Hash (HashAlgorithm, MD5 (..), SHA224 (..), SHA256 (..), SHA384 (..), SHA512 (..), hashWith)
import Crypto.KDF.PBKDF2 (Parameters (..), fastPBKDF2_SHA256)
import Crypto.MAC.HMAC (HMAC, hmac)
import Crypto.Random (getRandomBytes)
import Data.Bits (shiftL, testBit, xor, (.&.), (.|.))
import qualified Data.ByteArray as ByteArray
import Data.ByteArray.Encoding (Base (Base16), convertToBase)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Char8 as Char8
import Data.Word (Word8)

-- | The answer to an MD5 password request:
-- @"md5" <> md5hex (md5hex (password <> user) <> salt)@.
md5Password :: ByteString -> ByteString -> ByteString -> ByteString
md5Password user password salt = "md5" <> md5Hex (md5Hex (password <> user) <> salt)
  where
    md5Hex = convertToBase Base16 . hashWith MD5

scramMechanism :: ByteString
scramMechanism = "SCRAM-SHA-256"

-- | What the client remembers between its first and its final message.
data ScramClient = ScramClient
  { scramNonce :: ByteString,
    scramClientFirstBare :: ByteString
  }

-- | A nonce of 24 printable characters, none of them a comma.
newScramNonce :: IO ByteString
newScramNonce = Base64.encode <$> getRandomBytes 18

-- | The client-first-message for a user name and a nonce. PostgreSQL ignores
-- the user name in it and uses the one from the startup message.
scramClientFirst :: ByteString -> ByteString -> (ByteString, ScramClient)
scramClientFirst user nonce = (gs2Header <> bare, ScramClient nonce bare)
  where
    bare = "n=" <> user <> ",r=" <> nonce

-- | Answers the server-first-message with the client-final-message, and
-- returns the server signature the server-final-message has to carry.
scramClientFinal :: ByteString -> ScramClient -> ByteString -> Either String (ByteString, ByteString)
scramClientFinal password client serverFirst = do
  let attributes = BS.split comma serverFirst
  serverNonce <- attribute 'r' attributes
  salt <- Base64.decode =<< attribute 's' attributes
  iterations <- positiveInt =<< attribute 'i' attributes
  unless
    (scramNonce client `BS.isPrefixOf` serverNonce && BS.length serverNonce > BS.length (scramNonce client))
    (Left "the server's nonce doesn't extend the client's")
  let saltedPassword = fastPBKDF2_SHA256 (Parameters iterations 32) password salt :: ByteString
      clientKey = hmacSha256 saltedPassword "Client Key"
      storedKey = ByteArray.convert (hashWith SHA256 clientKey)
      withoutProof = "c=" <> Base64.encode gs2Header <> ",r=" <> serverNonce
      authMessage = BS.intercalate "," [scramClientFirstBare client, serverFirst, withoutProof]
      proof = BS.pack (BS.zipWith xor clientKey (hmacSha256 storedKey authMessage))
      serverSignature = hmacSha256 (hmacSha256 saltedPassword "Server Key") authMessage
  pure (withoutProof <> ",p=" <> Base64.encode proof, serverSignature)
  where
    positiveInt value = case Char8.readInt value of
      Just (count, rest) | BS.null rest, count > 0 -> Right count
      _ -> Left ("invalid iteration count " <> show value)

-- | Checks the server-final-message against the expected server signature.
scramVerifyServerFinal :: ByteString -> ByteString -> Either String ()
scramVerifyServerFinal expected serverFinal =
  case attribute 'v' attributes of
    Right encoded -> do
      signature <- Base64.decode encoded
      unless (ByteArray.constEq signature expected) (Left "the server's signature doesn't match")
    Left _ -> case attribute 'e' attributes of
      Right serverError -> Left ("the server rejected the proof: " <> Char8.unpack serverError)
      Left _ -> Left ("invalid server-final-message " <> show serverFinal)
  where
    attributes = BS.split comma serverFinal

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

-- | No channel binding: it needs TLS.
gs2Header :: ByteString
gs2Header = "n,,"

attribute :: Char -> [ByteString] -> Either String ByteString
attribute name attributes =
  case [value | pair <- attributes, Just value <- [BS.stripPrefix (Char8.pack [name, '=']) pair]] of
    value : _ -> Right value
    [] -> Left ("missing attribute " <> [name])

hmacSha256 :: ByteString -> ByteString -> ByteString
hmacSha256 key message = ByteArray.convert (hmac key message :: HMAC SHA256)

comma :: Word8
comma = 44
