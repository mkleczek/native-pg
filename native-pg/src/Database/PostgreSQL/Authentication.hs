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
  )
where

import Control.Monad (unless)
import Crypto.Hash (MD5 (..), SHA256 (..), hashWith)
import Crypto.KDF.PBKDF2 (Parameters (..), fastPBKDF2_SHA256)
import Crypto.MAC.HMAC (HMAC, hmac)
import Crypto.Random (getRandomBytes)
import Data.Bits (xor)
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
