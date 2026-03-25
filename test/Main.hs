{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import qualified Data.ByteString as BS
import Database.PostgreSQL.Protocol

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

startupMessage :: StartupMessage BS.ByteString
startupMessage =
  StartupMessage
    protocolVersion3_2
    [ Scalar ("user", "postgres"),
      Scalar ("database", "postgres")
    ]

startupMessageEnvelope :: StartupMessageEnvelope BS.ByteString
startupMessageEnvelope = Envelope startupMessage

cancelRequestBody :: BS.ByteString
cancelRequestBody =
  BS.pack
    [ 0x00, 0x00, 0x00, 0x0C,
      0x04, 0xD2, 0x16, 0x2E,
      0x00, 0x00, 0x00, 0x2A,
      0xDE, 0xAD, 0xBE, 0xEF
    ]

startupMessageBody :: BS.ByteString
startupMessageBody =
  BS.concat
    [ BS.pack [0x00, 0x00, 0x00, 0x25],
      BS.pack [0x00, 0x03, 0x00, 0x02],
      "user\0postgres\0database\0postgres\0\0"
    ]

cancelRequestMessage :: CancelRequest BS.ByteString
cancelRequestMessage =
  CancelRequest
    (Scalar (Scalar 42, Scalar (BS.pack [0xDE, 0xAD, 0xBE, 0xEF])))

main :: IO ()
main = do
  assertEqual
    "startup protocol version"
    protocolVersion3_2
    (startupMessageProtocolVersion startupMessage)
  assertEqual
    "startup parameters"
    [ Scalar ("user", "postgres"),
      Scalar ("database", "postgres")
    ]
    (startupMessageParameters startupMessage)
  assertEqual
    "startup envelope"
    (Envelope startupMessage)
    startupMessageEnvelope
  assertEqual
    "startup message parser"
    (Right startupMessageEnvelope)
    (parseStartupMessage startupMessageBody)
  assertEqual
    "cancel request parser"
    (Right (Envelope cancelRequestMessage))
    (parseCancelRequest cancelRequestBody)
  putStrLn "Message smoke test passed."
