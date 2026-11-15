{-# LANGUAGE OverloadedStrings #-}

-- | A scripted DNS server over UDP at 127.0.0.1, for the cases about host names: for each name that it is asked, the
-- script says whether it answers with addresses, says that the name doesn't exist, answers late, or says nothing. It
-- answers a question for any other type of record than A as for a name that has no records of that type, as a server does
-- for a name without IPv6 addresses.
module DnsServer
  ( Reply (..),
    Script,
    DnsServer (..),
    withDnsServer,
  )
where

import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Exception (SomeException, bracket, catch)
import Control.Monad (forever, void)
import Data.Bits (shiftR, (.&.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Char (toLower)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Word (Word16, Word8)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes

-- | What the server does with a question.
data Reply
  = -- | Answers with these IPv4 addresses, in this order.
    Addresses [(Word8, Word8, Word8, Word8)]
  | -- | Says that the name doesn't exist.
    NoSuchName
  | -- | Does what the reply says after this many microseconds, without holding up the questions after it.
    Later Int Reply
  | -- | Says nothing.
    Silent

-- | What to do with a question, from the name in it: lower case, without the final dot.
type Script = ByteString -> Reply

-- | A running server: its port, and the questions it has been asked so far, each with its name and the type of record,
-- oldest first.
data DnsServer = DnsServer
  { dnsPort :: Socket.PortNumber,
    dnsQuestions :: IO [(ByteString, Word16)]
  }

-- | A question of a query: its identifier, the name, the type of record, and the question's own bytes, which a response
-- repeats.
data Question = Question ByteString ByteString Word16 ByteString

-- | Runs the action while a server that follows the script is running.
withDnsServer :: Script -> (DnsServer -> IO a) -> IO a
withDnsServer script action =
  bracket (Socket.socket Socket.AF_INET Socket.Datagram Socket.defaultProtocol) Socket.close $ \sock -> do
    Socket.bind sock (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    port <- Socket.socketPort sock
    asked <- newIORef []
    let serve = forever $ do
          (packet, from) <- SocketBytes.recvFrom sock 512
          case parsed packet of
            Nothing -> pure ()
            Just asking@(Question _ name kind _) -> do
              atomicModifyIORef' asked (\before -> (before <> [(name, kind)], ()))
              answer asking (script name) from
        answer asking@(Question identifier _ kind echoed) reply from = case reply of
          Silent -> pure ()
          Later microseconds later -> void (forkIO (threadDelay microseconds *> answer asking later from `catch` \(_ :: SomeException) -> pure ()))
          NoSuchName -> void (SocketBytes.sendTo sock (response identifier 3 echoed []) from)
          Addresses found -> void (SocketBytes.sendTo sock (response identifier 0 echoed (if kind == 1 then found else [])) from)
    bracket (forkIO (serve `catch` \(_ :: SomeException) -> pure ())) killThread $ \_ ->
      action (DnsServer port (readIORef asked))

-- | The first question of a query. A query's header is twelve bytes, then the name as labels that end with an empty one,
-- the type of record and the class.
parsed :: ByteString -> Maybe Question
parsed packet = do
  (name, afterName) <- labels body []
  let fixed = BS.take 4 afterName
      echoed = BS.take (BS.length body - BS.length afterName + 4) body
  if BS.length fixed < 4
    then Nothing
    else Just (Question (BS.take 2 packet) name (fromIntegral (BS.index fixed 0) * 256 + fromIntegral (BS.index fixed 1)) echoed)
  where
    body = BS.drop 12 packet
    labels bytes found = case BS.uncons bytes of
      Nothing -> Nothing
      Just (0, rest) -> Just (Char8.map toLower (Char8.intercalate "." (reverse found)), rest)
      Just (size, rest) ->
        let (label, after) = BS.splitAt (fromIntegral size) rest
         in if BS.length label < fromIntegral size then Nothing else labels after (label : found)

-- | The response to a query, with the code of the result, the question repeated, and these answers, each with the name
-- of the question as a pointer to it, a time to live of a minute, and the address.
response :: ByteString -> Word8 -> ByteString -> [(Word8, Word8, Word8, Word8)] -> ByteString
response identifier code echoed found =
  identifier
    <> BS.pack [0x81, 0x80 + code]
    <> word 1
    <> word (length found)
    <> word 0
    <> word 0
    <> echoed
    <> BS.concat [BS.pack [0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, a, b, c, d] | (a, b, c, d) <- found]
  where
    word :: Int -> ByteString
    word number = BS.pack [fromIntegral ((number `shiftR` 8) .&. 0xff), fromIntegral (number .&. 0xff)]
