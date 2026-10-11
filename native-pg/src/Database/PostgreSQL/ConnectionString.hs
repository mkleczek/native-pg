{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}

-- | libpq's connection strings, URIs and key/value strings, with the defaults
-- libpq takes from the environment, and connecting to the first suitable host
-- one names.
--
-- TLS is used as @sslmode@ asks, except for @verify-ca@, and @require@ with a
-- root certificate file, which libpq verifies as @verify-ca@. Options this
-- client doesn't implement are rejected, unless they only matter with TLS or
-- GSSAPI. Password and service files aren't read.
module Database.PostgreSQL.ConnectionString
  ( -- * Connection strings
    ConnectionString (..),
    Host (..),
    InvalidConnectionString (..),
    TargetSessionAttrs (..),
    hostConnectSettings,
    hostSocketAddress,
    parseConnectionString,
    readConnectionString,

    -- * Connecting
    ConnectFailure (..),
    HostFailure (..),
    connectFirst,
  )
where

import Control.Applicative ((<|>))
import Control.Exception (Exception (..), Handler (..), IOException, bracketOnError, catches, throwIO, try)
import Control.Monad (join, unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Foldable (for_, toList)
import Data.Functor (($>), (<&>))
import Data.List (intercalate)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (catMaybes, fromMaybe, isJust, mapMaybe)
import qualified Data.Text as Text
import Data.Text.Encoding (decodeUtf8With, encodeUtf8)
import Data.Text.Encoding.Error (lenientDecode)
import Data.Word (Word8)
import Database.PostgreSQL.Protocol (ProtocolVersion (..), protocolVersion3_0, protocolVersion3_2)
import Database.PostgreSQL.Protocol.Codec (InputError (..))
import Database.PostgreSQL.Session
import Database.PostgreSQL.Transport (Address (..), ConnectError, RootCertificates (..), TcpOptions (..), TlsVersion (..))
import Network.Socket (PortNumber)
import System.Directory (doesFileExist)
import System.Environment (lookupEnv)
import System.Timeout (timeout)
#if !defined(mingw32_HOST_OS)
import System.Posix.User (getEffectiveUserName)
#endif

-- | What a connection string asks for, with the environment's defaults.
data ConnectionString = ConnectionString
  { -- | Tried in order.
    connectionHosts :: NonEmpty Host,
    connectionUser :: ByteString,
    connectionPassword :: Maybe ByteString,
    connectionDatabase :: ByteString,
    -- | Startup parameters: @application_name@, or else
    -- @fallback_application_name@, @options@ and @client_encoding@, where
    -- given.
    connectionParameters :: [(ByteString, ByteString)],
    -- | Seconds each host has to connect, authenticate and qualify.
    connectionTimeout :: Maybe Int,
    connectionTargetSessionAttrs :: TargetSessionAttrs,
    -- | The version asked for, from @max_protocol_version@, and the lowest one
    -- accepted, from @min_protocol_version@.
    connectionProtocolVersion :: ProtocolVersion,
    connectionMinProtocolVersion :: ProtocolVersion,
    connectionTcpOptions :: TcpOptions,
    connectionSslMode :: SslMode,
    -- | The client certificate and its private key, from @sslcert@ and
    -- @sslkey@, or libpq's default files in the home directory, unless
    -- @sslcertmode@ is @disable@.
    connectionSslCertificate :: Maybe (FilePath, FilePath),
    -- | Certificate revocation lists: from @sslcrl@, or, if neither it nor
    -- @sslcrldir@ is given, libpq's default file in the home directory, and
    -- from @sslcrldir@.
    connectionSslRevocationList :: Maybe FilePath,
    connectionSslRevocationListDirectory :: Maybe FilePath,
    -- | The versions of TLS from @ssl_min_protocol_version@, @TLSv1.2@ by
    -- default, to @ssl_max_protocol_version@, that rustls has.
    connectionSslVersions :: [TlsVersion],
    connectionSslServerNameIndication :: Bool,
    -- | From @sslnegotiation@: whether TLS starts without asking the server
    -- first, which needs an sslmode that requires TLS.
    connectionSslDirect :: Bool
  }
  deriving (Eq, Show)

-- | A server to try.
data Host = Host
  { -- | As the connection string names it: a host name, an IP address, or the
    -- directory of a Unix-domain socket.
    hostName :: String,
    -- | From @hostaddr@: the IP address to connect to instead of looking up
    -- the name.
    hostAddress :: Maybe String,
    hostPort :: PortNumber
  }
  deriving (Eq, Show)

-- | Which servers qualify, from @target_session_attrs@.
data TargetSessionAttrs
  = AnySession
  | -- | Not in hot standby, and not read-only by default.
    ReadWriteSession
  | ReadOnlySession
  | -- | Not in hot standby.
    PrimarySession
  | StandbySession
  | -- | A standby if a host is one, or else any server.
    PreferStandbySession
  deriving (Eq, Show)

-- | A connection string that can't be used, for the reason given.
newtype InvalidConnectionString = InvalidConnectionString String
  deriving (Show)

instance Exception InvalidConnectionString where
  displayException (InvalidConnectionString reason) = reason

-- | Parses the connection string, and fills in what it leaves out from the
-- environment as libpq does.
readConnectionString :: ByteString -> IO (Either String ConnectionString)
readConnectionString string = do
  -- Only the variables it reads: what it leaves unread keeps them, and a
  -- connection that keeps its string to cancel with would keep a copy of the
  -- whole environment.
  environment <- catMaybes <$> traverse (\name -> fmap (name,) <$> lookupEnv name) ("HOME" : mapMaybe snd optionTable)
  user <- localUserName
  case parseConnectionString (`lookup` environment) user string of
    -- libpq verifies the server's certificate with sslmode=require too if
    -- the default root certificate file exists.
    Right parsed
      | connectionSslMode parsed == SslRequire,
        Just roots <- defaultRootCertificate <$> lookup "HOME" environment ->
          doesFileExist roots <&> \verifying ->
            if verifying then Left (requireWithRoots roots) else Right parsed
    parsed -> pure parsed

-- | Like 'readConnectionString', with the environment variables and the local
-- user's name, if it could be looked up, given.
parseConnectionString :: (String -> Maybe String) -> Maybe String -> ByteString -> Either String ConnectionString
parseConnectionString environment localUser string = do
  given <- options string
  resolve (setting environment given) (environment "HOME") localUser

-- | The operating system's name of the user, which libpq uses when none is
-- given. The lookup can fail, e.g. for a container user without a passwd
-- entry, which only matters if no user is given.
localUserName :: IO (Maybe String)
localUserName = either (\(_ :: IOException) -> Nothing) Just <$> try lookUpUserName
  where
#if defined(mingw32_HOST_OS)
    lookUpUserName = maybe (ioError (userError "USERNAME isn't set")) pure =<< lookupEnv "USERNAME"
#else
    lookUpUserName = getEffectiveUserName
#endif

-- | The root certificate file libpq verifies the server's certificate
-- against unless another is given, in the home directory.
defaultRootCertificate :: FilePath -> FilePath
defaultRootCertificate home = home <> "/.postgresql/root.crt"

-- | sslmode=require with root certificates verifies the server's certificate
-- as verify-ca does.
requireWithRoots :: FilePath -> String
requireWithRoots roots =
  "sslmode=require with the root certificate file \"" <> roots <> "\" verifies the server's certificate as verify-ca does, which isn't supported: use sslmode=verify-full"

-- | Where libpq connects when no host is given.
defaultHost :: String
#if defined(mingw32_HOST_OS)
defaultHost = "localhost"
#else
defaultHost = "/tmp"
#endif

-- | The options libpq has, with the environment variables that give their
-- defaults.
optionTable :: [(ByteString, Maybe String)]
optionTable =
  [ ("host", Just "PGHOST"),
    ("hostaddr", Just "PGHOSTADDR"),
    ("port", Just "PGPORT"),
    ("dbname", Just "PGDATABASE"),
    ("user", Just "PGUSER"),
    ("password", Just "PGPASSWORD"),
    ("passfile", Just "PGPASSFILE"),
    ("require_auth", Just "PGREQUIREAUTH"),
    ("channel_binding", Just "PGCHANNELBINDING"),
    ("connect_timeout", Just "PGCONNECT_TIMEOUT"),
    ("client_encoding", Just "PGCLIENTENCODING"),
    ("options", Just "PGOPTIONS"),
    ("application_name", Just "PGAPPNAME"),
    ("fallback_application_name", Nothing),
    ("keepalives", Nothing),
    ("keepalives_idle", Nothing),
    ("keepalives_interval", Nothing),
    ("keepalives_count", Nothing),
    ("tcp_user_timeout", Nothing),
    ("replication", Nothing),
    ("gssencmode", Just "PGGSSENCMODE"),
    ("sslmode", Just "PGSSLMODE"),
    ("sslnegotiation", Just "PGSSLNEGOTIATION"),
    ("requiressl", Just "PGREQUIRESSL"),
    ("sslcompression", Just "PGSSLCOMPRESSION"),
    ("sslcert", Just "PGSSLCERT"),
    ("sslkey", Just "PGSSLKEY"),
    ("sslkeylogfile", Nothing),
    ("sslpassword", Nothing),
    ("sslcertmode", Just "PGSSLCERTMODE"),
    ("sslrootcert", Just "PGSSLROOTCERT"),
    ("sslcrl", Just "PGSSLCRL"),
    ("sslcrldir", Just "PGSSLCRLDIR"),
    ("sslsni", Just "PGSSLSNI"),
    ("requirepeer", Just "PGREQUIREPEER"),
    ("ssl_min_protocol_version", Just "PGSSLMINPROTOCOLVERSION"),
    ("ssl_max_protocol_version", Just "PGSSLMAXPROTOCOLVERSION"),
    ("min_protocol_version", Just "PGMINPROTOCOLVERSION"),
    ("max_protocol_version", Just "PGMAXPROTOCOLVERSION"),
    ("krbsrvname", Just "PGKRBSRVNAME"),
    ("gsslib", Just "PGGSSLIB"),
    ("gssdelegation", Just "PGGSSDELEGATION"),
    ("scram_client_key", Nothing),
    ("scram_server_key", Nothing),
    ("service", Just "PGSERVICE"),
    ("target_session_attrs", Just "PGTARGETSESSIONATTRS"),
    ("load_balance_hosts", Just "PGLOADBALANCEHOSTS"),
    ("oauth_issuer", Nothing),
    ("oauth_client_id", Nothing),
    ("oauth_client_secret", Nothing),
    ("oauth_scope", Nothing)
  ]

-- | The keywords and values a connection string gives, in order. Like libpq,
-- it takes @requiressl@ as @sslmode@, @require@ for 1 and @prefer@ otherwise.
options :: ByteString -> Either String [(ByteString, ByteString)]
options string =
  fmap (map asSslMode) . maybe (keyValueOptions string) (uriOptions string) $
    BS.stripPrefix "postgresql://" string <|> BS.stripPrefix "postgres://" string
  where
    asSslMode ("requiressl", value) = ("sslmode", if "1" `BS.isPrefixOf` value then "require" else "prefer")
    asSslMode option = option

-- | @keyword = value@ pairs separated by whitespace. A value can be quoted
-- with @'@, and a backslash escapes the next character.
keyValueOptions :: ByteString -> Either String [(ByteString, ByteString)]
keyValueOptions = go []
  where
    go found input = case Char8.uncons (Char8.dropWhile isBlank input) of
      Nothing -> Right (reverse found)
      Just _ -> do
        let (keyword, afterKeyword) = Char8.break (\c -> c == '=' || isBlank c) (Char8.dropWhile isBlank input)
        afterEquals <- case Char8.uncons (Char8.dropWhile isBlank afterKeyword) of
          Just ('=', rest) -> Right (Char8.dropWhile isBlank rest)
          _ -> Left ("missing \"=\" after " <> quote keyword <> " in connection info string")
        (value, rest) <- case Char8.uncons afterEquals of
          Just ('\'', quoted) -> quotedValue [] quoted
          _ -> Right (unquotedValue [] afterEquals)
        unless (isJust (lookup keyword optionTable)) $
          Left ("invalid connection option " <> quote keyword)
        go ((keyword, value) : found) rest
    unquotedValue value input = case Char8.uncons input of
      Nothing -> (finish value, "")
      Just (c, rest)
        | isBlank c -> (finish value, rest)
        | c == '\\' -> maybe (finish value, "") (\(escaped, rest') -> unquotedValue (escaped : value) rest') (Char8.uncons rest)
        | otherwise -> unquotedValue (c : value) rest
    quotedValue value input = case Char8.uncons input of
      Just ('\\', rest) | Just (escaped, rest') <- Char8.uncons rest -> quotedValue (escaped : value) rest'
      Just ('\'', rest) -> Right (finish value, rest)
      Just (c, rest) | c /= '\\' -> quotedValue (c : value) rest
      _ -> Left "unterminated quoted string in connection info string"
    finish = Char8.pack . reverse

-- | libpq's URI:
-- @postgresql://[user[:password]\@][host][:port][,...][/dbname][?keyword=value&...]@,
-- percent-encoded. Like libpq, it stores what it finds as options, in this
-- order, so parameters can override the parts before them.
uriOptions :: ByteString -> ByteString -> Either String [(ByteString, ByteString)]
uriOptions uri body = do
  -- Credentials end at the first @ before any /.
  let (beforeSeparator, separator) = Char8.break (\c -> c == '@' || c == '/') body
  (credentials, afterCredentials) <- case Char8.uncons separator of
    Just ('@', rest) -> do
      let (user, password) = Char8.break (== ':') beforeSeparator
      user' <- percentDecoded user
      password' <- percentDecoded (BS.drop 1 password)
      Right (stored "user" user' <> stored "password" password', rest)
    _ -> Right ([], body)
  (hosts, ports, afterHosts) <- hostList "" "" afterCredentials
  hosts' <- percentDecoded hosts
  ports' <- percentDecoded ports
  (database, query) <- case Char8.uncons afterHosts of
    Just ('/', rest) -> do
      let (database, query) = Char8.break (== '?') rest
      database' <- percentDecoded database
      Right (stored "dbname" database', query)
    _ -> Right ([], afterHosts)
  parameters <- uriParameters (BS.drop 1 query)
  Right (credentials <> stored "host" hosts' <> stored "port" ports' <> database <> parameters)
  where
    -- Empty parts aren't stored, so that defaults apply.
    stored keyword value = [(keyword, value) | not (BS.null value)]
    -- Hosts and their ports, each list joined with commas as libpq joins them,
    -- and what follows: a / or ?, or nothing.
    hostList hosts ports input = do
      (host, afterHost) <- case Char8.uncons input of
        Just ('[', rest) -> do
          let (address, afterAddress) = Char8.break (== ']') rest
              afterBracket = BS.drop 1 afterAddress
          when (BS.null afterAddress) $
            Left ("end of string reached when looking for matching \"]\" in IPv6 host address in URI: " <> quote uri)
          when (BS.null address) $
            Left ("IPv6 host address may not be empty in URI: " <> quote uri)
          case Char8.uncons afterBracket of
            Just (c, _)
              | c `notElem` (":/?," :: String) ->
                  Left
                    ( "unexpected character "
                        <> quote (Char8.singleton c)
                        <> " at position "
                        <> show (BS.length uri - BS.length afterBracket + 1)
                        <> " in URI (expected \":\" or \"/\"): "
                        <> quote uri
                    )
            _ -> Right (address, afterBracket)
        _ -> Right (Char8.break (`elem` (":/?," :: String)) input)
      let (port, afterPort) = case Char8.uncons afterHost of
            Just (':', rest) -> Char8.break (`elem` ("/?," :: String)) rest
            _ -> ("", afterHost)
      case Char8.uncons afterPort of
        Just (',', rest) -> hostList (hosts <> host <> ",") (ports <> port <> ",") rest
        _ -> Right (hosts <> host, ports <> port, afterPort)
    uriParameters query
      | BS.null query = Right []
      | otherwise = do
          let (parameter, rest) = Char8.break (== '&') query
          (keyword, value) <- case Char8.split '=' parameter of
            [keyword, value] -> Right (keyword, value)
            [keyword] -> Left ("missing key/value separator \"=\" in URI query parameter: " <> quote keyword)
            keyword : _ -> Left ("extra key/value separator \"=\" in URI query parameter: " <> quote keyword)
            [] -> Left ("missing key/value separator \"=\" in URI query parameter: " <> quote parameter)
          keyword' <- percentDecoded keyword
          value' <- percentDecoded value
          -- As libpq does, for compatibility with JDBC.
          let (keyword'', value'')
                | keyword' == "ssl" && value' == "true" = ("sslmode", "require")
                | otherwise = (keyword', value')
          unless (isJust (lookup keyword'' optionTable)) $
            Left ("invalid URI query parameter: " <> quote keyword'')
          ((keyword'', value'') :) <$> uriParameters (BS.drop 1 rest)

percentDecoded :: ByteString -> Either String ByteString
percentDecoded encoded = BS.pack <$> go (BS.unpack encoded)
  where
    go [] = Right []
    go (37 : rest) = case rest of
      high : low : rest'
        | Just h <- hexDigit high,
          Just l <- hexDigit low ->
            if h == 0 && l == 0
              then Left ("forbidden value %00 in percent-encoded value: " <> quote encoded)
              else (h * 16 + l :) <$> go rest'
      _ -> Left ("invalid percent-encoded token: " <> quote encoded)
    go (byte : rest) = (byte :) <$> go rest
    hexDigit :: Word8 -> Maybe Word8
    hexDigit digit
      | digit >= 48 && digit <= 57 = Just (digit - 48)
      | digit >= 65 && digit <= 70 = Just (digit - 55)
      | digit >= 97 && digit <= 102 = Just (digit - 87)
      | otherwise = Nothing

-- | An option's value, and where it came from for messages: the keyword, or
-- the environment variable.
data Setting = Setting ByteString String

-- | The last value given for the keyword, or else the environment's.
setting :: (String -> Maybe String) -> [(ByteString, ByteString)] -> ByteString -> Maybe Setting
setting environment given keyword =
  case lookup keyword (reverse given) of
    Just value -> Just (Setting value (Char8.unpack keyword))
    Nothing -> do
      variable <- join (lookup keyword optionTable)
      value <- environment variable
      Just (Setting (encodeUtf8 (Text.pack value)) variable)

-- | The connection string the options give, with the home directory and the
-- local user's name for defaults.
resolve :: (ByteString -> Maybe Setting) -> Maybe String -> Maybe String -> Either String ConnectionString
resolve option home localUser = do
  let systemRoots = given "sslrootcert" == Just "system"
      rootCertificates = case given "sslrootcert" of
        Just "system" -> Right SystemRootCertificates
        Just roots -> Right (RootCertificateFile (text roots))
        Nothing ->
          maybe
            (Left "could not get home directory to locate root certificate file\nEither provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode to disable server certificate verification.")
            (Right . RootCertificateFile . defaultRootCertificate)
            home
  sslMode <- case option "sslmode" of
    Just (Setting value source)
      | systemRoots && value /= "verify-full" ->
          Left ("weak sslmode " <> quote value <> " may not be used with sslrootcert=system (use \"verify-full\")")
      | otherwise -> case value of
          "require" -> maybe (Right SslRequire) (Left . requireWithRoots . text) (given "sslrootcert")
          "verify-full" -> SslVerifyFull <$> rootCertificates
          "verify-ca" -> Left (source <> "=verify-ca verifies the server's certificate but not its name, which isn't supported: use verify-full")
          "disable" -> Right SslDisable
          "allow" -> Right SslAllow
          "prefer" -> Right SslPrefer
          _ -> Left ("invalid sslmode value: " <> quote value)
    -- As in libpq, the system's root certificates make verify-full the
    -- default, and PGREQUIRESSL counts only if no sslmode is given.
    Nothing
      | systemRoots -> Right (SslVerifyFull SystemRootCertificates)
      | otherwise -> Right $ case option "requiressl" of
          Just (Setting value _) | "1" `BS.isPrefixOf` value -> SslRequire
          _ -> SslPrefer
  sslDirect <- case given "sslnegotiation" of
    Nothing -> Right False
    Just "postgres" -> Right False
    Just "direct"
      | sslModeRequired sslMode -> Right True
      | otherwise -> Left ("weak sslmode \"" <> sslModeName sslMode <> "\" may not be used with sslnegotiation=direct (use \"require\", \"verify-ca\", or \"verify-full\")")
    Just other -> Left ("invalid sslnegotiation value: " <> quote other)
  choice "sslcertmode" ["disable", "allow"] ["require"] "knowing whether the server asked for the certificate"
  choice "channel_binding" ["disable", "prefer"] ["require"] "TLS"
  choice "gssencmode" ["disable", "prefer"] ["require"] "GSSAPI encryption"
  choice "load_balance_hosts" ["disable"] ["random"] "random load balancing"
  for_ (option "replication") $ \(Setting value source) ->
    unless (value `elem` ["0", "false", "off", "no"]) $
      Left (source <> "=" <> text value <> " asks for a replication connection, which isn't supported")
  -- rustls takes keys only in plaintext, and logs no keys through it.
  mapM_ unsupported ["service", "require_auth", "requirepeer", "scram_client_key", "scram_server_key", "sslpassword", "sslkeylogfile"]
  certificate <- case (given "sslcertmode", given "sslcert", given "sslkey", home) of
    (Just "disable", _, _, _) -> Right Nothing
    (_, Just chain, Just key, _) -> Right (Just (text chain, text key))
    (_, chain, key, Just directory) ->
      Right (Just (maybe (directory <> "/.postgresql/postgresql.crt") text chain, maybe (directory <> "/.postgresql/postgresql.key") text key))
    (_, Nothing, _, Nothing) -> Right Nothing
    (_, Just _, Nothing, Nothing) -> Left "could not get home directory to locate client certificate files"
  hosts <- hostList
  user <- case given "user" of
    Just user -> Right user
    Nothing -> maybe (Left "could not look up the local user's name, so a user has to be given") (Right . encodeUtf8 . Text.pack) localUser
  password <- case given "password" of
    Nothing | Just _ <- given "passfile" -> Left "password files aren't supported, so a password has to be given"
    password -> Right password
  connectTimeout <- traverse (integer "connect_timeout") (given "connect_timeout")
  target <- case maybe "any" (\(Setting value _) -> value) (option "target_session_attrs") of
    "any" -> Right AnySession
    "read-write" -> Right ReadWriteSession
    "read-only" -> Right ReadOnlySession
    "primary" -> Right PrimarySession
    "standby" -> Right StandbySession
    "prefer-standby" -> Right PreferStandbySession
    other -> Left ("invalid target_session_attrs value: " <> quote other)
  minVersion <- protocolVersion "min_protocol_version"
  maxVersion <- protocolVersion "max_protocol_version"
  when (minVersion > maxVersion) $
    Left "min_protocol_version is greater than max_protocol_version"
  -- rustls has no version below 1.2, so a lower minimum is 1.2's.
  minTls <- maybe (Right Tls12) (fmap (max Tls12 . fromMaybe Tls12) . tlsVersion "ssl_min_protocol_version") (given "ssl_min_protocol_version")
  maxTls <- maybe (Right (Just maxBound)) (tlsVersion "ssl_max_protocol_version") (given "ssl_max_protocol_version")
  sslVersions <- case maxTls of
    Nothing -> Left "ssl_max_protocol_version is lower than TLSv1.2, the lowest version supported"
    Just highest
      | minTls > highest -> Left "invalid SSL protocol version range"
      | otherwise -> Right [minTls .. highest]
  keepalives <- maybe (Right True) (fmap (/= 0) . integer "keepalives") (given "keepalives")
  tcp <-
    TcpOptions keepalives
      <$> positive "keepalives_idle"
      <*> positive "keepalives_interval"
      <*> positive "keepalives_count"
      <*> positive "tcp_user_timeout"
  Right
    ConnectionString
      { connectionHosts = hosts,
        connectionUser = user,
        connectionPassword = password,
        connectionDatabase = fromMaybe user (given "dbname"),
        connectionParameters =
          [("application_name", name) | Just name <- [given "application_name" <|> given "fallback_application_name"]]
            <> [("options", value) | Just value <- [given "options"]]
            <> [("client_encoding", value) | Just value <- [given "client_encoding"]],
        -- libpq waits at least two seconds, so that rounding doesn't make a
        -- connection fail almost at once.
        connectionTimeout = (\seconds -> if seconds <= 0 then Nothing else Just (max 2 seconds)) =<< connectTimeout,
        connectionTargetSessionAttrs = target,
        connectionProtocolVersion = maxVersion,
        connectionMinProtocolVersion = minVersion,
        connectionTcpOptions = tcp,
        connectionSslMode = sslMode,
        connectionSslCertificate = certificate,
        connectionSslRevocationList = case (given "sslcrl", given "sslcrldir") of
          (Just file, _) -> Just (text file)
          (Nothing, Nothing) -> (<> "/.postgresql/root.crl") <$> home
          (Nothing, Just _) -> Nothing,
        connectionSslRevocationListDirectory = text <$> given "sslcrldir",
        connectionSslVersions = sslVersions,
        connectionSslServerNameIndication = given "sslsni" /= Just "0",
        connectionSslDirect = sslDirect
      }
  where
    -- An empty value counts as none, as it does in libpq.
    given keyword = case option keyword of
      Just (Setting value _) | not (BS.null value) -> Just value
      _ -> Nothing
    choice keyword allowed needing feature =
      for_ (option keyword) $ \(Setting value source) ->
        if
          | value `elem` allowed -> Right ()
          | value `elem` needing -> Left (source <> "=" <> text value <> " requires " <> feature <> ", which isn't supported")
          | otherwise -> Left ("invalid " <> text keyword <> " value: " <> quote value)
    unsupported keyword =
      for_ (given keyword) $ \_ -> Left (text keyword <> " isn't supported")
    integer keyword value =
      case Char8.readInt (trimmed value) of
        Just (parsed, rest) | BS.null rest -> Right parsed
        _ -> Left ("invalid integer value " <> quote value <> " for connection option " <> quote keyword)
    -- Zero or less keeps the system's default.
    positive keyword = do
      parsed <- traverse (integer keyword) (given keyword)
      Right (parsed >>= \value -> if value <= 0 then Nothing else Just value)
    sslModeRequired = \case
      SslRequire -> True
      SslVerifyFull _ -> True
      _ -> False
    sslModeName = \case
      SslDisable -> "disable"
      SslAllow -> "allow"
      SslPrefer -> "prefer"
      SslRequire -> "require"
      SslVerifyFull _ -> "verify-full"
    -- A version of TLS rustls has, or Nothing for one lower.
    tlsVersion keyword value = case value of
      "TLSv1" -> Right Nothing
      "TLSv1.1" -> Right Nothing
      "TLSv1.2" -> Right (Just Tls12)
      "TLSv1.3" -> Right (Just Tls13)
      _ -> Left ("invalid " <> text keyword <> " value: " <> quote value)
    protocolVersion keyword = case given keyword of
      Nothing -> Right protocolVersion3_0
      Just "3.0" -> Right protocolVersion3_0
      Just "3.2" -> Right protocolVersion3_2
      Just "latest" -> Right protocolVersion3_2
      Just other -> Left ("invalid " <> text keyword <> " value: " <> quote other)
    -- Each host, with its address and port, by position; a single port is
    -- every host's.
    hostList = do
      let names = maybe [] commaSeparated (given "host")
          addresses = maybe [] commaSeparated (given "hostaddr")
          ports = maybe [] commaSeparated (given "port")
          count = if null addresses then max 1 (length names) else length addresses
      unless (null names || null addresses || length names == count) $
        Left ("could not match " <> show (length names) <> " host names to " <> show count <> " hostaddr values")
      unless (length ports <= 1 || length ports == count) $
        Left ("could not match " <> show (length ports) <> " port numbers to " <> show count <> " hosts")
      parsed <- traverse (host names addresses ports) [0 .. count - 1]
      case parsed of
        first : others -> Right (first :| others)
        [] -> Left "no host"
    host names addresses ports index = do
      let name = at index names
          address = at index addresses
          portText = if length ports == 1 then at 0 ports else at index ports
      port <-
        if BS.null portText
          then Right 5432
          else case Char8.readInt (trimmed portText) of
            Just (number, rest) | BS.null rest, number >= 1, number <= 65535 -> Right (fromIntegral number)
            _ -> Left ("invalid port number: " <> quote portText)
      Right
        Host
          { hostName =
              if
                | not (BS.null name) -> text name
                | not (BS.null address) -> text address
                | otherwise -> defaultHost,
            hostAddress = if BS.null address then Nothing else Just (text address),
            hostPort = port
          }
    at index values = case drop index values of
      value : _ -> value
      [] -> ""
    commaSeparated value = if BS.null value then [""] else Char8.split ',' value

-- | The settings for connecting to one of the hosts.
hostConnectSettings :: ConnectionString -> Host -> ConnectSettings
hostConnectSettings string host =
  (connectSettings (hostSocketAddress host) (connectionUser string))
    { connectPassword = connectionPassword string,
      connectDatabase = Just (connectionDatabase string),
      connectParameters = connectionParameters string,
      connectProtocolVersion = connectionProtocolVersion string,
      connectTcpOptions = connectionTcpOptions string,
      connectSslMode = connectionSslMode string,
      connectSslCertificate = connectionSslCertificate string,
      connectSslRevocationList = connectionSslRevocationList string,
      connectSslRevocationListDirectory = connectionSslRevocationListDirectory string,
      connectSslVersions = connectionSslVersions string,
      connectSslServerNameIndication = connectionSslServerNameIndication string,
      connectSslDirect = connectionSslDirect string,
      -- With hostaddr, the address is the IP address the name resolved to.
      connectServerName = Just (hostName host)
    }

-- | Where to connect. A socket directory names the Unix-domain socket for the
-- port in it, and one starting with @\@@ a socket in Linux's abstract
-- namespace.
hostSocketAddress :: Host -> Address
hostSocketAddress host = case (hostAddress host, hostName host) of
  (Just address, _) -> TcpAddress address (hostPort host)
  (Nothing, directory@('/' : _)) -> UnixAddress (socketFile directory)
  (Nothing, '@' : abstract) -> UnixAddress ('\0' : socketFile abstract)
  (Nothing, name) -> TcpAddress name (hostPort host)
  where
    socketFile directory = directory <> "/.s.PGSQL." <> show (hostPort host)

-- | No host qualified: each attempt's host and failure, in order.
-- 'displayException' words it as libpq does.
newtype ConnectFailure = ConnectFailure [(Host, HostFailure)]
  deriving (Show)

-- | Why a host didn't qualify.
data HostFailure
  = -- | The socket didn't connect.
    NotConnected ConnectError
  | -- | The server rejected the connection, or the startup failed.
    NotStarted ConnectionError
  | -- | The connection broke during the startup.
    Broke String
  | -- | The connect timeout passed first.
    TimedOut
  | -- | The server doesn't have the session attributes or the protocol
    -- version asked for.
    Unsuitable String
  | -- | Checking the session attributes failed.
    CheckFailed ServerError
  deriving (Show)

instance Exception ConnectFailure where
  displayException (ConnectFailure failures) = intercalate "\n" (map describe failures)
    where
      describe = \case
        (_, NotConnected failure) -> displayException failure
        (host, failure) -> "connection to server " <> location host <> " failed: " <> reason failure
      reason = \case
        NotConnected failure -> displayException failure
        NotStarted failure -> displayException failure
        Broke why -> why
        TimedOut -> "timeout expired"
        Unsuitable why -> why
        CheckFailed failure -> displayException failure
      location host = case hostSocketAddress host of
        UnixAddress path -> "on socket " <> show path
        TcpAddress address port
          | address == hostName host -> "at " <> show address <> ", port " <> show port
          | otherwise -> "at " <> show (hostName host) <> " (" <> address <> "), port " <> show port

-- | Connects to the first host that accepts the connection and qualifies
-- under @target_session_attrs@, giving each host the connect timeout. With
-- @prefer-standby@, the hosts are tried for a standby first, and then for any
-- server. The settings for each host can be adjusted, e.g. for a bigger read
-- buffer. Throws 'ConnectFailure' when no host qualifies.
connectFirst :: (ConnectSettings -> ConnectSettings) -> ConnectionString -> IO (Session 'Ready, ServerInfo, Host)
connectFirst adjust string =
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
      bracketOnError (connect (adjust (hostConnectSettings string host))) (close . fst) $ \(session, info) ->
        qualify target session info >>= \case
          Nothing -> pure (Right (session, info, host))
          Just failure -> close session $> Left failure
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
          _ -> fmap (== "on") <$> queryValue session "SHOW transaction_read_only"
        standby = case reported "in_hot_standby" of
          Just inStandby -> pure (Right (inStandby == "on"))
          Nothing -> fmap (== "t") <$> queryValue session "SELECT pg_catalog.pg_is_in_recovery()"
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

-- | C's isspace, which libpq uses, in the C locale.
isBlank :: Char -> Bool
isBlank c = c `elem` (" \t\n\r\f\v" :: String)

trimmed :: ByteString -> ByteString
trimmed = Char8.dropWhile isBlank . fst . Char8.spanEnd isBlank

quote :: ByteString -> String
quote value = "\"" <> text value <> "\""

text :: ByteString -> String
text = Text.unpack . decodeUtf8With lenientDecode
