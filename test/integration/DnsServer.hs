{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | A scripted DNS server over UDP and TCP at 127.0.0.1, for the cases about host names: for each name that it is asked, the
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
import Control.Exception (IOException, SomeException, bracket, catch, finally, throwIO, try)
import Control.Monad (forM_, forever, void)
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

-- | Runs the action while a server that follows the script is running, over UDP and over TCP on the same port.
withDnsServer :: Script -> (DnsServer -> IO a) -> IO a
withDnsServer script action =
  bracket bound (\(sock, listener, _) -> Socket.close sock *> Socket.close listener) $ \(sock, listener, port) -> do
      Socket.listen listener 4
      asked <- newIORef []
      let -- The reply that the script gives to a question, which is noted.
          ask (Question _ name kind _) = atomicModifyIORef' asked (\before -> (before <> [(name, kind)], ())) *> pure (script name)
          -- The reply to a question, with what sends it.
          answer send asking@(Question identifier _ kind echoed) = \case
            Silent -> pure ()
            Later microseconds later -> void (forkIO (threadDelay microseconds *> answer send asking later `catch` \(_ :: SomeException) -> pure ()))
            NoSuchName -> send (response identifier 3 echoed [])
            Addresses found -> send (response identifier 0 echoed (if kind == 1 then found else []))
          serveUdp = forever $ do
            (packet, from) <- SocketBytes.recvFrom sock 512
            forM_ (parsed packet) $ \asking -> ask asking >>= answer (\bytes -> void (SocketBytes.sendTo sock bytes from)) asking
          -- Over TCP each message has its length in two bytes before it, in both directions.
          serveTcp = forever $ do
            (client, _) <- Socket.accept listener
            void . forkIO $ (`finally` Socket.close client) $ (`catch` \(_ :: SomeException) -> pure ()) $ forever $ do
              header <- received client 2
              packet <- received client (fromIntegral (BS.index header 0) * 256 + fromIntegral (BS.index header 1))
              forM_ (parsed packet) $ \asking -> ask asking >>= answer (\bytes -> SocketBytes.sendAll client (BS.pack [fromIntegral (BS.length bytes `shiftR` 8), fromIntegral (BS.length bytes .&. 0xff)] <> bytes)) asking
          received client size
            | size <= 0 = pure BS.empty
            | otherwise = do
                piece <- SocketBytes.recv client size
                if BS.null piece then ioError (userError "closed") else (piece <>) <$> received client (size - BS.length piece)
          quietly serving = forkIO (serving `catch` \(_ :: SomeException) -> pure ())
      bracket (quietly serveUdp) killThread $ \_ ->
        bracket (quietly serveTcp) killThread $ \_ ->
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

-- | A UDP socket and a TCP socket that listens on the same port, which the system chooses for the UDP socket, and tries
-- again with another one if that is taken for TCP.
bound :: IO (Socket.Socket, Socket.Socket, Socket.PortNumber)
bound = attempt (20 :: Int)
  where
    loopback port = Socket.SockAddrInet port (Socket.tupleToHostAddress (127, 0, 0, 1))
    attempt count = do
      sock <- Socket.socket Socket.AF_INET Socket.Datagram Socket.defaultProtocol
      listener <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
      tried <-
        try $ do
          Socket.bind sock (loopback 0)
          port <- Socket.socketPort sock
          Socket.setSocketOption listener Socket.ReuseAddr 1
          Socket.bind listener (loopback port)
          pure (sock, listener, port)
      case tried of
        Right found -> pure found
        Left (failure :: IOException)
          | count > 0 -> Socket.close sock *> Socket.close listener *> attempt (count - 1)
          | otherwise -> Socket.close sock *> Socket.close listener *> throwIO failure
