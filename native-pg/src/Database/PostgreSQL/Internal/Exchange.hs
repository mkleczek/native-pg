{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | Private protocol phases and message exchange over an operation context.
module Database.PostgreSQL.Internal.Exchange
  ( Phase (..), Accepts (..), Session (..), sessionConnection,
    Reply (..), startupTail, sessionTail, onError, onNotice, onParameterStatus,
    serverError, sessionError, pullReply, keepNotification, copyNotification,
    pullEither, pull, receive, runOn, operation, send, sendOutputs, continueIn,
    sync, awaitReady,
  ) where

import Control.Exception (throwIO)
import Control.Monad (when, (<=<))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (atomicModifyIORef')
import Database.PostgreSQL.Diagnostics (ServerError (..))
import Database.PostgreSQL.Internal.Context
import Database.PostgreSQL.Internal.Diagnostics (diagnostics)
import Database.PostgreSQL.Diagnostics (isConnectionFatal)
import Database.PostgreSQL.Internal.Protocol.Codec (Alternatives (..), Input, Output, ParseConstructor, Serializable, parseChoice)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import Database.PostgreSQL.Internal.Protocol.Messages

-- | The phases of a connection, each named after what the client waits for in
-- it.
data Phase
  = -- | The answer to the startup message: an authentication request, or
    -- AuthenticationOk when none is needed.
    Authentication
  | -- | The server's SCRAM challenge, after the client's first SASL message.
    SaslContinue
  | -- | The server's SCRAM signature, after the client's proof.
    SaslFinal
  | -- | AuthenticationOk, after the client's password.
    AuthenticationResult
  | -- | The server's parameters and backend key, until it's ready for
    -- queries.
    Initialization
  | -- | Ready for a query.
    Ready
  | -- | The result of a Parse.
    ParseResult
  | -- | The result of a Bind.
    BindResult
  | -- | The result of a Close.
    CloseResult
  | -- | The parameters' types, which a Describe of a statement answers
    -- first.
    DescribeResult
  | -- | The columns of the statement's rows, or NoData, which follow them.
    RowDescriptionResult
  | -- | The rows of an Execute, until the command completes.
    Rows
  | -- | The ReadyForQuery that answers a Sync.
    SyncResult

-- | The server messages a phase accepts. Handlers for a phase's messages are
-- given in the same order.
class Accepts (phase :: Phase) where
  type Accepted phase :: [Payload]

instance Accepts 'Authentication where
  type
    Accepted 'Authentication =
      '[ 'PayloadField 'AuthenticationOkType,
         'PayloadField 'AuthenticationCleartextPasswordType,
         'PayloadField 'AuthenticationMD5PasswordType,
         'PayloadField 'AuthenticationSASLType,
         'PayloadField 'AuthenticationKerberosV5Type,
         'PayloadField 'AuthenticationGSSType,
         'PayloadField 'AuthenticationSSPIType,
         'PayloadField 'NegotiateProtocolVersionType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'SaslContinue where
  type
    Accepted 'SaslContinue =
      '[ 'PayloadField 'AuthenticationSASLContinueType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'SaslFinal where
  type
    Accepted 'SaslFinal =
      '[ 'PayloadField 'AuthenticationSASLFinalType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'AuthenticationResult where
  type
    Accepted 'AuthenticationResult =
      '[ 'PayloadField 'AuthenticationOkType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

instance Accepts 'Initialization where
  type
    Accepted 'Initialization =
      '[ 'PayloadField 'ParameterStatusType,
         'PayloadField 'BackendKeyDataType,
         'PayloadField 'ReadyForQueryType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType
       ]

-- | Between queries the server only sends messages of its own accord.
instance Accepts 'Ready where
  type
    Accepted 'Ready =
      '[ 'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'ParseResult where
  type
    Accepted 'ParseResult =
      '[ 'PayloadField 'ParseCompleteType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'BindResult where
  type
    Accepted 'BindResult =
      '[ 'PayloadField 'BindCompleteType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'CloseResult where
  type
    Accepted 'CloseResult =
      '[ 'PayloadField 'CloseCompleteType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'DescribeResult where
  type
    Accepted 'DescribeResult =
      '[ 'PayloadField 'ParameterDescriptionType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

instance Accepts 'RowDescriptionResult where
  type
    Accepted 'RowDescriptionResult =
      '[ 'PayloadField 'RowDescriptionType,
         'PayloadField 'NoDataType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

-- | Rows are read only up to their headers; their columns are pulled after.
instance Accepts 'Rows where
  type
    Accepted 'Rows =
      '[ 'PayloadField 'DataRowHeaderType,
         'PayloadField 'CommandCompleteType,
         'PayloadField 'EmptyQueryResponseType,
         'PayloadField 'PortalSuspendedType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

-- | An error here comes from the Sync itself, e.g. a deferred constraint
-- failing as the implicit transaction commits.
instance Accepts 'SyncResult where
  type
    Accepted 'SyncResult =
      '[ 'PayloadField 'ReadyForQueryType,
         'PayloadField 'ErrorResponseType,
         'PayloadField 'NoticeResponseType,
         'PayloadField 'ParameterStatusType,
         'PayloadField 'NotificationResponseType
       ]

-- | A connection in a known phase.
newtype Session (phase :: Phase) = Session Context

sessionConnection :: Session phase -> Connection
sessionConnection (Session context) = contextConnection context

-- | What a handler makes of a message.
data Reply a
  = Reply a
  | -- | Keep pulling.
    Skip
  | -- | Keep the notification for 'awaitNotification', and keep pulling.
    Notified (Notification ByteString)
  | Rejected [ErrorNoticeField ByteString]

-- | Handlers for the messages any phase of startup accepts last.
{-# INLINE startupTail #-}
startupTail :: Alternatives '[ErrorResponse ByteString -> Reply a, NoticeResponse ByteString -> Reply a] (Reply a)
startupTail = onError :|: onNotice :|: NoAlternatives

-- | Handlers for the messages any phase after startup accepts last. Notices
-- and reports of parameters and notifications are skipped for now.
{-# INLINE sessionTail #-}
sessionTail ::
  Alternatives
    '[ ErrorResponse ByteString -> Reply a,
       NoticeResponse ByteString -> Reply a,
       ParameterStatus ByteString -> Reply a,
       NotificationResponse ByteString -> Reply a
     ]
    (Reply a)
sessionTail = onError :|: onNotice :|: onParameterStatus :|: (\(NotificationResponse notification) -> Notified notification) :|: NoAlternatives

onError :: ErrorResponse ByteString -> Reply a
onError (ErrorResponse fields) = Rejected fields

onNotice :: NoticeResponse ByteString -> Reply a
onNotice _ = Skip

onParameterStatus :: ParameterStatus ByteString -> Reply a
onParameterStatus _ = Skip

-- | Decodes an error's fields, which are copied out of the read buffer.
serverError :: [ErrorNoticeField ByteString] -> IO ServerError
serverError = either (throwIO . ProtocolError) (pure . ServerError) . diagnostics

-- | An error during a session. One after which the server ends the session
-- is thrown as 'ConnectionTerminated', leaving the session broken.
sessionError :: [ErrorNoticeField ByteString] -> IO ServerError
sessionError fields = do
  failure <- serverError fields
  when (isConnectionFatal failure) $ throwIO (ConnectionTerminated failure)
  pure failure

-- | Pulls messages until one gives a reply or an error. Skipped messages are
-- pulled past.
-- Keep parser dictionaries specializable across the module boundary.
{-# INLINABLE pullReply #-}
pullReply ::
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions (Reply a)) (Reply a) =>
  Session phase ->
  Alternatives functions (Reply a) ->
  IO (Either [ErrorNoticeField ByteString] a)
pullReply session handlers = do
  reply <- receive session handlers
  case reply of
    Reply a -> pure (Right a)
    Skip -> pullReply session handlers
    Notified notification -> do
      keepNotification session notification
      pullReply session handlers
    Rejected fields -> pure (Left fields)

-- | Keeps a notification, copied out of the read buffer, for
-- 'awaitNotification'.
keepNotification :: Session phase -> Notification ByteString -> IO ()
keepNotification session notification =
  atomicModifyIORef' (connectionNotifications (sessionConnection session)) (\kept -> (copyNotification notification : kept, ()))

copyNotification :: Notification ByteString -> Notification ByteString
copyNotification (Notification processId channel payload) = Notification processId (BS.copy channel) (BS.copy payload)

-- | Pulls a reply during a session; see 'sessionError'.
{-# INLINE pullEither #-}
pullEither ::
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions (Reply a)) (Reply a) =>
  Session phase ->
  Alternatives functions (Reply a) ->
  IO (Either ServerError a)
pullEither session handlers =
  pullReply session handlers >>= either (fmap Left . sessionError) (pure . Right)

-- | Pulls a reply during startup, when any error is thrown as
-- 'ConnectionRejected'.
{-# INLINE pull #-}
pull ::
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions (Reply a)) (Reply a) =>
  Session phase ->
  Alternatives functions (Reply a) ->
  IO a
pull session handlers =
  pullReply session handlers >>= either (throwIO . ConnectionRejected <=< serverError) pure

-- | Pulls the next message, which has to be one the phase accepts, and passes
-- it to its handler.
{-# INLINE receive #-}
receive ::
  forall phase functions r.
  ParseConstructor ('PayloadChoice (Accepted phase)) (Alternatives functions r) r =>
  Session phase ->
  Alternatives functions r ->
  IO r
receive (Session context) handlers = runOn context maxBound (parseChoice @(Accepted phase) handlers)

-- | Runs a parser on the context's unconsumed bytes, reading no further
-- than the given position.
runOn :: Context -> Int -> Input a -> IO a
runOn = runContextInput

-- | Runs an operation within its workspace and connection ownership.
operation :: Session 'Ready -> IO a -> IO a
operation (Session context) = runContextOperation context

{-# INLINE send #-}
send :: forall a s phase. Serializable (Envelope a s) s => Session phase -> Envelope a s -> IO ()
send session message = sendOutputs session (serialize message)

sendOutputs :: Session phase -> Output -> IO ()
sendOutputs (Session context) = sendContextOutput context

-- | The session in the phase the protocol continues in.
continueIn :: Session phase -> Session next
continueIn (Session context) = Session context

sync :: Output
sync = serialize Sync

-- | Pulls the ReadyForQuery that ends a Sync. An error before it, e.g. a
-- deferred constraint failing at the implicit commit, replaces a successful
-- result, given by the function.
awaitReady :: Session phase -> (ServerError -> e) -> Either e a -> IO (Either e a, TransactionStatus)
awaitReady session failed result = do
  let syncing = continueIn session :: Session 'SyncResult
  reply <-
    receive syncing $
      (\(ReadyForQuery status) -> Reply status) :|: sessionTail
  case reply of
    Reply status -> pure (result, status)
    Skip -> awaitReady session failed result
    Notified notification -> do
      keepNotification session notification
      awaitReady session failed result
    Rejected fields -> do
      failure <- sessionError fields
      awaitReady session failed (result >>= const (Left (failed failure)))
