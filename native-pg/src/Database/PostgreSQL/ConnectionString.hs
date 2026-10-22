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
-- GSSAPI. Password files follow libpq's lookup and permission rules; service
-- files aren't read.
module Database.PostgreSQL.ConnectionString
  ( ConnectionString (..),
    Defaults (..),
    Host (..),
    InvalidConnectionString (..),
    TargetSessionAttrs (..),
    hostConnectSettings,
    hostSocketAddress,
    parseConnectionString,
    readConnectionString,
    ConnectFailure (..),
    HostFailure (..),
    connectFirst
  ) where

import Control.Applicative ((<|>))
#if defined(mingw32_HOST_OS)
import Control.Exception (IOException, try)
#else
import Control.Exception (IOException, bracket, try)
#endif
import Control.Monad (join, unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Foldable (for_, toList)
import Data.Functor ((<&>))
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (catMaybes, fromMaybe, isJust, listToMaybe, mapMaybe)
import qualified Data.Text as Text
import Data.Text.Encoding (decodeUtf8With, encodeUtf8)
import Data.Text.Encoding.Error (lenientDecode)
import Data.Word (Word8)
import Database.PostgreSQL.ConnectionString.Types
import qualified Database.PostgreSQL.Internal.HostSelection as HostSelection
import Database.PostgreSQL.Session (Session, close, connection)
import Database.PostgreSQL.Types (protocolVersion3_0, protocolVersion3_2)
import Database.PostgreSQL.Internal.Startup (connectOwned)
import Database.PostgreSQL.Session.Settings
import Database.PostgreSQL.Transport (RootCertificates (..), TcpOptions (..), TlsVersion (..))
import System.Directory (doesFileExist)
import System.Environment (lookupEnv)
#if !defined(mingw32_HOST_OS)
import Data.Bits ((.&.))
import System.IO (hClose)
import System.Posix.User (getEffectiveUserID, getEffectiveUserName, getUserEntryForID, homeDirectory)
import System.Posix.Files (fileMode, getFdStatus, isRegularFile)
import System.Posix.IO (OpenMode (ReadOnly), OpenFileFlags (..), closeFd, defaultFileFlags, dup, fdToHandle, openFd)
#endif

-- | Parses the connection string, and fills in what it leaves out from the
-- environment and the defaults as libpq does.
readConnectionString :: Defaults -> ByteString -> IO (Either String ConnectionString)
readConnectionString defaults string = do
  -- Only the variables it reads: what it leaves unread keeps them, and a
  -- connection that keeps its string to cancel with would keep a copy of the
  -- whole environment.
  environment <- catMaybes <$> traverse (\name -> fmap (name,) <$> lookupEnv name) (mapMaybe snd optionTable)
  user <- localUserName
  let parsed = do
        given <- options string
        resolved <- resolve (setting (`lookup` environment) given) defaults user
        pure (given, resolved)
  case parsed of
    Left failure -> pure (Left failure)
    Right (given, resolved) -> do
      withPasswords <- passwordsFromFile defaults (setting (`lookup` environment) given) resolved
      -- libpq verifies the server's certificate with sslmode=require too if
      -- the default root certificate file exists.
      case withPasswords of
        parsedWithPasswords
          | connectionSslMode parsedWithPasswords == SslRequire,
            Just roots <- rootCertificateIn <$> defaultCertificateDirectory defaults ->
              doesFileExist roots <&> \verifying ->
                if verifying then Left (requireWithRoots roots) else Right parsedWithPasswords
          | otherwise -> pure (Right parsedWithPasswords)

-- | Like 'readConnectionString', with the environment variables and the local
-- user's name, if it could be looked up, given. This pure parser accepts
-- @passfile@ but leaves password-file lookup to 'readConnectionString'.
parseConnectionString :: Defaults -> (String -> Maybe String) -> Maybe String -> ByteString -> Either String ConnectionString
parseConnectionString defaults environment localUser string = do
  given <- options string
  resolve (setting environment given) defaults localUser

-- | Password files are only consulted when neither the connection string nor
-- PGPASSWORD supplied a password. Keep the matches by host: a later host may
-- require different credentials after an earlier connection attempt fails.
passwordsFromFile :: Defaults -> (ByteString -> Maybe Setting) -> ConnectionString -> IO ConnectionString
passwordsFromFile defaults option parsed
  | isJust (connectionPassword parsed) = pure parsed
  | otherwise = do
      file <- maybe defaultPasswordFile (pure . Just . text) (value "passfile")
      case file of
        Nothing -> pure parsed
        Just path -> do
          contents <- readPasswordFile path
          let entries = map (Char8.dropWhileEnd (== '\r')) (Char8.lines contents)
              names = maybe [] (Char8.split ',') (value "host")
              addresses = maybe [] (Char8.split ',') (value "hostaddr")
              matches (index, host) =
                (host,) <$> passwordFor entries (matchingHost names addresses index) (Char8.pack (show (hostPort host))) (connectionDatabase parsed) (connectionUser parsed)
          pure parsed{connectionHostPasswords = mapMaybe matches (zip [0 ..] (toList (connectionHosts parsed)))}
  where
    value keyword = case option keyword of
      Just (Setting bytes _) | not (BS.null bytes) -> Just bytes
      _ -> Nothing
    at index = fromMaybe BS.empty . listToMaybe . drop index
    matchingHost names addresses index
      | not (BS.null name) =
          if BS.null address && text name == defaultHost defaults && isSocket (text name) then "localhost" else name
      | not (BS.null address) = address
      | otherwise = "localhost"
      where
        name = at index names
        address = at index addresses
    isSocket ('/' : _) = True
    isSocket ('@' : _) = True
    isSocket _ = False

-- | The default path follows libpq, independently of the TLS directory.
defaultPasswordFile :: IO (Maybe FilePath)
defaultPasswordFile =
#if defined(mingw32_HOST_OS)
  fmap (<> "/postgresql/pgpass.conf") <$> lookupEnv "APPDATA"
#else
  either (\(_ :: IOException) -> Nothing) Just <$> try locate
  where
    locate = do
      home <- lookupEnv "HOME"
      directory <- case home of
        Just directory | not (null directory) -> pure directory
        _ -> homeDirectory <$> (getEffectiveUserID >>= getUserEntryForID)
      pure (directory <> "/.pgpass")
#endif

-- | Missing, unreadable, non-regular and insecure files are ignored. On Unix,
-- inspect the opened descriptor rather than checking a path and reopening it.
readPasswordFile :: FilePath -> IO ByteString
readPasswordFile path = either (\(_ :: IOException) -> BS.empty) id <$> try readContents
  where
#if defined(mingw32_HOST_OS)
    readContents = BS.readFile path
#else
    readContents = bracket open closeFd $ \fd -> do
      status <- getFdStatus fd
      if isRegularFile status && fileMode status .&. 0o077 == 0
        then bracket (dup fd >>= fdToHandle) hClose BS.hGetContents
        else pure BS.empty
    -- O_NONBLOCK prevents a named pipe from blocking before fstat rejects it.
#if MIN_VERSION_unix(2,8,0)
    open = openFd path ReadOnly defaultFileFlags{nonBlock = True}
#else
    open = openFd path ReadOnly Nothing defaultFileFlags{nonBlock = True}
#endif
#endif

-- | First matching line wins. Only an unescaped, whole-field '*' is a
-- wildcard. The password is the remainder of the line, with escapes removed.
passwordFor :: [ByteString] -> ByteString -> ByteString -> ByteString -> ByteString -> Maybe ByteString
passwordFor entries host port database user = listToMaybe (mapMaybe match entries)
  where
    match line
      | BS.null line || BS.head line == 35 = Nothing
      | otherwise = do
          (hostField, rest1) <- field line
          (portField, rest2) <- field rest1
          (databaseField, rest3) <- field rest2
          (userField, password) <- field rest3
          if and (zipWith matches [hostField, portField, databaseField, userField] [host, port, database, user])
            then Just (unescape password)
            else Nothing
    matches rawField value = rawField == "*" || unescape rawField == value
    field = go [] . BS.unpack
      where
        go _ [] = Nothing
        go acc (92 : next : rest) = go (next : 92 : acc) rest
        go acc (58 : rest) = Just (BS.pack (reverse acc), BS.pack rest)
        go acc (next : rest) = go (next : acc) rest
    unescape = BS.pack . go . BS.unpack
      where
        go (92 : next : rest) = next : go rest
        go (next : rest) = next : go rest
        go [] = []

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
-- against unless another is given, in the certificate directory.
rootCertificateIn :: FilePath -> FilePath
rootCertificateIn directory = directory <> "/root.crt"

-- | sslmode=require with root certificates verifies the server's certificate
-- as verify-ca does.
requireWithRoots :: FilePath -> String
requireWithRoots roots =
  "sslmode=require with the root certificate file \"" <> roots <> "\" verifies the server's certificate as verify-ca does, which isn't supported: use sslmode=verify-full"

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

-- | The connection string the options give, with the defaults and the local
-- user's name.
resolve :: (ByteString -> Maybe Setting) -> Defaults -> Maybe String -> Either String ConnectionString
resolve option defaults localUser = do
  let systemRoots = given "sslrootcert" == Just "system"
      rootCertificates = case given "sslrootcert" of
        Just "system" -> Right SystemRootCertificates
        Just roots -> Right (RootCertificateFile (text roots))
        Nothing ->
          maybe
            (Left "could not get home directory to locate root certificate file\nEither provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode to disable server certificate verification.")
            (Right . RootCertificateFile . rootCertificateIn)
            (defaultCertificateDirectory defaults)
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
  channelBinding <- case given "channel_binding" of
    Nothing -> Right ChannelBindingPrefer
    Just "disable" -> Right ChannelBindingDisable
    Just "prefer" -> Right ChannelBindingPrefer
    Just "require" -> Right ChannelBindingRequire
    Just other -> Left ("invalid channel_binding value: " <> quote other)
  choice "gssencmode" ["disable", "prefer"] ["require"] "GSSAPI encryption"
  choice "load_balance_hosts" ["disable"] ["random"] "random load balancing"
  for_ (option "replication") $ \(Setting value source) ->
    unless (value `elem` ["0", "false", "off", "no"]) $
      Left (source <> "=" <> text value <> " asks for a replication connection, which isn't supported")
  -- rustls takes keys only in plaintext, and logs no keys through it.
  mapM_ unsupported ["service", "require_auth", "requirepeer", "scram_client_key", "scram_server_key", "sslpassword", "sslkeylogfile"]
  certificate <- case (given "sslcertmode", given "sslcert", given "sslkey", defaultCertificateDirectory defaults) of
    (Just "disable", _, _, _) -> Right Nothing
    (_, Just chain, Just key, _) -> Right (Just (text chain, text key))
    (_, chain, key, Just directory) ->
      Right (Just (maybe (directory <> "/postgresql.crt") text chain, maybe (directory <> "/postgresql.key") text key))
    (_, Nothing, _, Nothing) -> Right Nothing
    (_, Just _, Nothing, Nothing) -> Left "could not get home directory to locate client certificate files"
  hosts <- hostList
  user <- case given "user" of
    Just user -> Right user
    Nothing -> maybe (Left "could not look up the local user's name, so a user has to be given") (Right . encodeUtf8 . Text.pack) localUser
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
        connectionPassword = given "password",
        connectionHostPasswords = [],
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
          (Nothing, Nothing) -> (<> "/root.crl") <$> defaultCertificateDirectory defaults
          (Nothing, Just _) -> Nothing,
        connectionSslRevocationListDirectory = text <$> given "sslcrldir",
        connectionSslVersions = sslVersions,
        connectionSslServerNameIndication = given "sslsni" /= Just "0",
        connectionSslDirect = sslDirect,
        connectionChannelBinding = channelBinding
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
                | otherwise -> defaultHost defaults,
            hostAddress = if BS.null address then Nothing else Just (text address),
            hostPort = port
          }
    at index values = case drop index values of
      value : _ -> value
      [] -> ""
    commaSeparated value = if BS.null value then [""] else Char8.split ',' value

-- | Connects to the first host that accepts the connection and qualifies
-- under @target_session_attrs@, giving each host the connect timeout. With
-- @prefer-standby@, the hosts are tried for a standby first, and then for any
-- server. The settings for each host can be adjusted, e.g. for a bigger read
-- buffer. Throws 'ConnectFailure' when no host qualifies.
connectFirst :: (ConnectSettings -> ConnectSettings) -> ConnectionString -> IO (Session, ServerInfo, Host)
connectFirst = HostSelection.connectFirstUsing connectOwned (close . connection) HostSelection.queryValue

-- | C's isspace, which libpq uses, in the C locale.
isBlank :: Char -> Bool
isBlank c = c `elem` (" \t\n\r\f\v" :: String)

trimmed :: ByteString -> ByteString
trimmed = Char8.dropWhile isBlank . fst . Char8.spanEnd isBlank

quote :: ByteString -> String
quote value = "\"" <> text value <> "\""

text :: ByteString -> String
text = Text.unpack . decodeUtf8With lenientDecode
