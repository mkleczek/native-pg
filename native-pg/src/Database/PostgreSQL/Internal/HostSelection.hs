{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Host fallback, qualification, and per-host timeouts.
module Database.PostgreSQL.Internal.HostSelection
  ( connectFirstUsing,
    queryValue,
  ) where

import Control.Exception (Exception (..), Handler (..), IOException, bracketOnError, catches, throwIO)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (toList)
import Data.Functor (($>))
import Data.Maybe (fromMaybe)
import Database.PostgreSQL.ConnectionString.Types
import Database.PostgreSQL.Internal.Protocol.Codec (InputError (..))
import Database.PostgreSQL.Session
  ( Column (..), Execution (..), Phase (Ready), ProtocolVersion (..),
    ServerError, Session, Statement (..), columnBytes, execute, nextColumn,
  )
import Database.PostgreSQL.Session.Settings
import System.Timeout (timeout)

connectFirstUsing
  :: (ConnectSettings -> IO (connection, ServerInfo))
  -> (connection -> IO ())
  -> (connection -> ByteString -> IO (Either ServerError ByteString))
  -> (ConnectSettings -> ConnectSettings)
  -> ConnectionString
  -> IO (connection, ServerInfo, Host)
connectFirstUsing open dispose value adjust string =
  either (throwIO . ConnectFailure . reverse) pure =<< case connectionTargetSessionAttrs string of
    PreferStandbySession -> attempt StandbySession hosts [] >>= either (attempt AnySession hosts) (pure . Right)
    target -> attempt target hosts []
  where
    hosts = toList (connectionHosts string)
    attempt _ [] failures = pure (Left failures)
    attempt target (host : others) failures =
      tryHost target host >>= \case
        Right connected -> pure (Right connected)
        Left failure -> attempt target others ((host, failure) : failures)
    tryHost target host =
      fromMaybe (Left TimedOut) <$> withTimeout (establish target host `catches` handlers)
    withTimeout = maybe (fmap Just) (\seconds -> timeout (seconds * 1000000)) (connectionTimeout string)
    handlers =
      [ Handler (pure . Left . NotConnected),
        Handler (pure . Left . NotStarted),
        Handler (\(failure :: IOException) -> pure (Left (Broke (displayException failure)))),
        Handler (\(InputError why) -> pure (Left (Broke why)))
      ]
    establish target host =
      bracketOnError (open (adjust (hostConnectSettings string host))) (dispose . fst) $ \(session, info) ->
        qualify target session info >>= \case
          Nothing -> pure (Right (session, info, host))
          Just failure -> dispose session $> Left failure
    qualify target session info
      | serverProtocolVersion info < connectionMinProtocolVersion string =
          pure . Just . Unsuitable $
            "server only supports protocol version "
              <> version (serverProtocolVersion info)
              <> ", but min_protocol_version was set to "
              <> version (connectionMinProtocolVersion string)
      | otherwise = case target of
          ReadWriteSession -> expect False "session is read-only" <$> readOnly
          ReadOnlySession -> expect True "session is not read-only" <$> readOnly
          PrimarySession -> expect False "server is in hot standby mode" <$> standby
          StandbySession -> expect True "server is not in hot standby mode" <$> standby
          _ -> pure Nothing
      where
        reported name = lookup name (serverParameters info)
        -- Servers since 14 report both; for older ones, ask.
        readOnly = case (reported "in_hot_standby", reported "default_transaction_read_only") of
          (Just inStandby, Just readOnlyDefault) -> pure (Right (inStandby == "on" || readOnlyDefault == "on"))
          _ -> fmap (== "on") <$> value session "SHOW transaction_read_only"
        standby = case reported "in_hot_standby" of
          Just inStandby -> pure (Right (inStandby == "on"))
          Nothing -> fmap (== "t") <$> value session "SELECT pg_catalog.pg_is_in_recovery()"
        expect wanted why = \case
          Left failure -> Just (CheckFailed failure)
          Right actual
            | actual == wanted -> Nothing
            | otherwise -> Just (Unsuitable why)
    version (ProtocolVersion major minor) = show major <> "." <> show minor

-- | The first column of the first row a query returns, as text.
queryValue :: Session 'Ready -> ByteString -> IO (Either ServerError ByteString)
queryValue session query = do
  (result, _) <- execute session (Execution (Unprepared query []) [] [] []) firstValue Nothing
  pure (fromMaybe "" . fst <$> result)
  where
    firstValue (Just value) _ = pure (Just value)
    firstValue Nothing row =
      nextColumn row >>= \case
        Just (Column _) -> Just . BS.copy <$> columnBytes row
        _ -> pure (Just "")
