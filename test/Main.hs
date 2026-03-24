{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Database.PostgreSQL.Protocol
import Database.PostgreSQL.Protocol.Codec
  ( Atom (..),
    PasswordTagContext (..),
    Payload (..),
    WireFrame (..),
    parseAuthBackendMessage,
    parseAuthFrontendMessage,
    parseBackendSessionMessage,
    parseCopyMessage,
    parseFrontendSessionMessage,
    parseStartupPacket,
    renderWireFrame,
    serializeAuthBackendMessage,
    serializeCopyMessage,
    serializeFrontendSessionMessage,
    serializeStartupPacket,
  )
import qualified Database.PostgreSQL.Protocol.Codec.Copy as CopyCodec

assertEqual :: (Eq a, Show a) => String -> a -> a -> IO ()
assertEqual label expected actual
  | expected == actual = pure ()
  | otherwise =
      error $
        label
          <> ": expected "
          <> show expected
          <> ", got "
          <> show actual

assertRight :: Show e => String -> Either e a -> IO a
assertRight label result =
  case result of
    Left err ->
      error $
        label
          <> ": expected Right, got Left "
          <> show err
    Right value ->
      pure value

frontendMessage :: FrontendSessionMessage 'QueryType
frontendMessage = Query "select 1"

backendMessage :: BackendSessionMessage 'ReadyForQueryType
backendMessage = ReadyForQuery TxIdle

sharedMessage :: CopyMessage 'CopyDataType
sharedMessage = CopyData "row-1"

dedicatedCopyDoneMessage :: CopyMessage 'CopyDoneType
dedicatedCopyDoneMessage = CopyDone

copyPayloadSingleton ::
  CopyMessageType
    100
    ('PayloadField 'RemainingBytesAtom 'PayloadEnd)
copyPayloadSingleton = copyMessageType sharedMessage

startupMessage :: StartupPacket 'StartupMessageType
startupMessage =
  StartupMessage
    protocolVersion3_2
    [ StartupParameter "user" "postgres",
      StartupParameter "database" "postgres"
    ]

cancelMessage :: StartupPacket 'CancelRequestType
cancelMessage =
  CancelRequest $
    BackendKey
      { backendKeyProcessId = ProcessId 42,
        backendKeySecretKey = SecretKey "01234567"
      }

authMd5Message :: AuthBackendMessage 'AuthenticationMD5PasswordType
authMd5Message = AuthenticationMD5Password "salt"

someStartupPacketName :: SomeStartupPacket -> String
someStartupPacketName (SomeStartupPacket packet) = startupPacketName packet

someAuthBackendMessageName :: SomeAuthBackendMessage -> String
someAuthBackendMessageName (SomeAuthBackendMessage message) = authBackendMessageName message

someAuthFrontendMessageName :: SomeAuthFrontendMessage -> String
someAuthFrontendMessageName (SomeAuthFrontendMessage message) = authFrontendMessageName message

someCopyMessageName :: SomeCopyMessage -> String
someCopyMessageName (SomeCopyMessage message) = copyMessageName message

someFrontendSessionMessageName :: SomeFrontendSessionMessage -> String
someFrontendSessionMessageName (SomeFrontendSessionMessage message) = frontendSessionMessageName message

someBackendSessionMessageName :: SomeBackendSessionMessage -> String
someBackendSessionMessageName (SomeBackendSessionMessage message) = backendSessionMessageName message

expectNamedRight ::
  String ->
  String ->
  (a -> String) ->
  Either String a ->
  IO ()
expectNamedRight label expectedName getName result = do
  value <- assertRight label result
  assertEqual label expectedName (getName value)

md5AuthBody :: ByteString
md5AuthBody = BS.pack [0, 0, 0, 5, 115, 97, 108, 116]

saslInitialBody :: ByteString
saslInitialBody =
  BS.concat
    [ "SCRAM-SHA-256",
      BS.pack [0, 0, 0, 0, 4],
      "test"
    ]

startupBody :: ByteString
startupBody =
  BS.concat
    [ "user",
      BS.pack [0],
      "postgres",
      BS.pack [0],
      "database",
      BS.pack [0],
      "postgres",
      BS.pack [0, 0]
    ]

main :: IO ()
main = do
  assertEqual "frontend singleton" QueryType (frontendSessionMessageType frontendMessage)
  assertEqual "backend singleton" ReadyForQueryType (backendSessionMessageType backendMessage)
  assertEqual "shared singleton" CopyDataType (copyMessageType sharedMessage)
  assertEqual "copy payload singleton" CopyDataType copyPayloadSingleton
  assertEqual "frontend code" 'Q' (frontendSessionMessageCode frontendMessage)
  assertEqual "backend code" 'Z' (backendSessionMessageCode backendMessage)
  assertEqual "shared code" 'd' (copyMessageCode sharedMessage)
  assertEqual
    "startup packet code"
    (protocolVersionCode protocolVersion3_2)
    (startupPacketCode startupMessage)
  assertEqual
    "cancel packet code"
    cancelRequestCode
    (startupPacketCode cancelMessage)
  startupFrame <- assertRight "serialize startup" (serializeStartupPacket startupMessage)
  authFrame <- assertRight "serialize auth md5" (serializeAuthBackendMessage authMd5Message)
  copyFrame <- assertRight "serialize copy data" (serializeCopyMessage sharedMessage)
  queryFrame <- assertRight "serialize query" (serializeFrontendSessionMessage frontendMessage)
  assertEqual
    "startup frame"
    (StartupFrame (protocolVersionCode protocolVersion3_2) startupBody)
    startupFrame
  assertEqual
    "auth md5 frame"
    (TaggedFrame 82 md5AuthBody)
    authFrame
  assertEqual
    "copy frame"
    (TaggedFrame 100 "row-1")
    copyFrame
  assertEqual
    "copy done payload (dedicated)"
    ()
    (CopyCodec.toPayload dedicatedCopyDoneMessage)
  assertEqual
    "startup raw frame"
    (BS.pack [0, 0, 0, 41, 0, 3, 0, 2] <> startupBody)
    (renderWireFrame startupFrame)
  assertEqual
    "copy raw frame"
    (BS.pack [100, 0, 0, 0, 9] <> "row-1")
    (renderWireFrame copyFrame)
  assertEqual
    "query raw frame"
    (BS.pack [81, 0, 0, 0, 13] <> "select 1" <> BS.pack [0])
    (renderWireFrame queryFrame)
  expectNamedRight
    "parse startup packet"
    "StartupMessage"
    someStartupPacketName
    (parseStartupPacket startupFrame)
  expectNamedRight
    "parse backend auth family"
    "AuthenticationMD5Password"
    someAuthBackendMessageName
    (parseAuthBackendMessage (TaggedFrame 82 md5AuthBody))
  expectNamedRight
    "parse backend auth ok"
    "AuthenticationOk"
    someAuthBackendMessageName
    (parseAuthBackendMessage (TaggedFrame 82 (BS.pack [0, 0, 0, 0])))
  expectNamedRight
    "parse frontend p family"
    "SASLInitialResponse"
    someAuthFrontendMessageName
    (parseAuthFrontendMessage ExpectSaslInitialResponse (TaggedFrame 112 saslInitialBody))
  expectNamedRight
    "parse copy message"
    "CopyData"
    someCopyMessageName
    (parseCopyMessage copyFrame)
  expectNamedRight
    "parse frontend session message"
    "Query"
    someFrontendSessionMessageName
    (parseFrontendSessionMessage queryFrame)
  expectNamedRight
    "parse backend session message"
    "ReadyForQuery"
    someBackendSessionMessageName
    (parseBackendSessionMessage (TaggedFrame 90 (BS.pack [73])))
  putStrLn "Protocol message smoke test passed."
