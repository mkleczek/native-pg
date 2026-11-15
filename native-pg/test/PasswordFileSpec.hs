{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module PasswordFileSpec (spec) where

import Control.Exception (bracket)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Foldable (toList)
import Data.List (isPrefixOf)
import Database.PostgreSQL.ConnectionString
import Database.PostgreSQL.Session.Settings (connectPassword)
import Skeletest
import qualified Skeletest.Predicate as P
import System.Directory (createDirectory, getTemporaryDirectory, removeDirectoryRecursive, removeFile)
import System.Environment (getEnvironment, lookupEnv, setEnv, unsetEnv)
import System.IO (hClose, openBinaryTempFile)
#if !defined(mingw32_HOST_OS)
import System.Posix.Files (setFileMode, createNamedPipe)
import System.Posix.User (getEffectiveUserName)
#endif

spec :: Spec
spec = do
  passwordFiles
  readOnce

-- | The password files, which the defaults read, and what a connection string takes from them.
passwordFiles :: Spec
passwordFiles = describe "password files" $ do
  it "resolves a different password for each host and port" $ isolated $
    withPassfile "h1:5432:d:u:first\nh2:5433:d:u:second\n" $ \file ->
      passwords file "host=h1,h2 port=5432,5433" `shouldReturn` [Just "first", Just "second"]
  it "uses the first matching entry, including wildcards in all four fields" $ isolated $
    withPassfile "# comment\n\nbroken\nother:5432:d:u:no\nh:5433:d:u:no\nh:5432:other:u:no\nh:5432:d:other:no\n*:5432:d:u:first\nh:5432:d:u:second\n" $ \file -> do
      passwords file "host=h" `shouldReturn` [Just "first"]
      withPassfile "h:*:*:*:wild\n" $ \wild ->
        passwords wild "host=h port=6543 dbname=other user=other" `shouldReturn` [Just "wild"]
  it "unescapes colons and backslashes in matching fields and passwords" $ isolated $
    withPassfile "db\\:host\\\\slash:5432:db\\:name:u\\:ser:p\\:ass\\\\word\r\n" $ \file ->
      passwords file ("host=" <> quoted "db:host\\slash" <> " dbname=" <> quoted "db:name" <> " user=" <> quoted "u:ser") `shouldReturn` [Just "p:ass\\word"]
  it "treats an escaped asterisk as a literal rather than a wildcard" $ isolated $
    withPassfile "\\*:5432:d:u:literal\n*:5432:d:u:wild\n" $ \file -> do
      passwords file "host=other" `shouldReturn` [Just "wild"]
      passwords file "host=*" `shouldReturn` [Just "literal"]
  it "matches the named host before hostaddr and uses localhost for default sockets" $ isolated $
    withPassfile "localhost:5432:d:u:local\n/tmp:5432:d:u:socket-name\n/custom:5432:d:u:socket\n10.0.0.1:5432:d:u:address\nnamed:5432:d:u:name\n" $ \file -> do
      passwords file "" `shouldReturn` [Just "local"]
      passwords file "host=/tmp" `shouldReturn` [Just "local"]
      passwords file "host=/custom" `shouldReturn` [Just "socket"]
      passwords file "hostaddr=10.0.0.1" `shouldReturn` [Just "address"]
      passwords file "host=named hostaddr=10.0.0.1" `shouldReturn` [Just "name"]
      passwords file "host=/tmp hostaddr=10.0.0.1" `shouldReturn` [Just "socket-name"]
      passwords file "host=,named hostaddr=,10.0.0.1" `shouldReturn` [Just "local", Just "name"]
  it "uses the user as the database name when no database is supplied" $ isolated $
    withPassfile "*:5432:u:u:default-db\n" $ \file ->
      readPasswordsFrom [file] ("user=u sslmode=disable passfile=" <> quoted (Char8.pack file)) `shouldReturn` [Just "default-db"]
  it "uses PGPASSFILE and lets an explicit passfile override it" $ isolated $
    withPassfile "*:5432:d:u:environment\n" $ \environmentFile ->
      withPassfile "*:5432:d:u:explicit\n" $ \explicitFile ->
        withVariable "PGPASSFILE" (Just environmentFile) $ do
          readPasswords base `shouldReturn` [Just "environment"]
          passwords explicitFile "" `shouldReturn` [Just "explicit"]
  it "gives supplied passwords priority over password files" $ isolated $
    withPassfile "*:5432:d:u:file\n" $ \file ->
      withVariable "PGPASSWORD" (Just "environment") $ do
        passwords file "" `shouldReturn` [Just "environment"]
        passwords file "password=explicit" `shouldReturn` [Just "explicit"]
        -- An explicitly empty password suppresses PGPASSWORD and requests file lookup.
        passwords file "password=''" `shouldReturn` [Just "file"]
  it "reads the platform's default password file" $ isolated $
    withTemporaryDirectory $ \directory -> do
#if defined(mingw32_HOST_OS)
      let subdirectory = directory <> "/postgresql"
      createDirectory subdirectory
      BS.writeFile (subdirectory <> "/pgpass.conf") "*:5432:d:u:default\n"
      withVariable "APPDATA" (Just directory) $
#else
      let file = directory <> "/.pgpass"
      BS.writeFile file "*:5432:d:u:default\n"
      setFileMode file 0o600
      withVariable "HOME" (Just directory) $
#endif
        readPasswords base `shouldReturn` [Just "default"]
  it "ignores missing files, non-regular files and entries that do not match" $ isolated $
    withTemporaryDirectory $ \directory -> do
      passwords (directory <> "/missing") "" `shouldReturn` [Nothing]
      passwords directory "" `shouldReturn` [Nothing]
      withPassfile "other:5432:d:u:password\n" $ \file ->
        passwords file "host=h" `shouldReturn` [Nothing]
#if !defined(mingw32_HOST_OS)
  it "rejects any group or world access and accepts a private Unix file" $ isolated $
    withPassfile "*:5432:d:u:private\n" $ \file -> do
      setFileMode file 0o640
      passwords file "" `shouldReturn` [Nothing]
      setFileMode file 0o604
      passwords file "" `shouldReturn` [Nothing]
      setFileMode file 0o600
      passwords file "" `shouldReturn` [Just "private"]
  it "ignores a named pipe without blocking" $ isolated $
    withTemporaryDirectory $ \directory -> do
      let pipe = directory <> "/pipe"
      createNamedPipe pipe 0o600
      passwords pipe "" `shouldReturn` [Nothing]
#endif

base :: ByteString
base = "user=u dbname=d sslmode=disable"

passwords :: FilePath -> ByteString -> IO [Maybe ByteString]
passwords file extra = readPasswordsFrom [file] (base <> " passfile=" <> quoted (Char8.pack file) <> " " <> extra)

readPasswords :: ByteString -> IO [Maybe ByteString]
readPasswords = readPasswordsFrom []

-- | The passwords that the connection string gets for its hosts, from the defaults that read these password files, and the
-- default one.
readPasswordsFrom :: [FilePath] -> ByteString -> IO [Maybe ByteString]
readPasswordsFrom files string = do
  defaults <- readDefaults "/tmp" Nothing files
  either (ioError . userError) pure (passwordsOf defaults string)

-- | The passwords that the connection string gets for its hosts, from the defaults.
passwordsOf :: Defaults -> ByteString -> Either String [Maybe ByteString]
passwordsOf defaults string = do
  parsed <- parseConnectionString defaults string
  pure [connectPassword (hostConnectSettings parsed host) | host <- toList (connectionHosts parsed)]

quoted :: ByteString -> ByteString
quoted value = "'" <> BS.concatMap escape value <> "'"
  where
    escape byte | byte == 39 || byte == 92 = BS.pack [92, byte]
                | otherwise = BS.singleton byte

withPassfile :: ByteString -> (FilePath -> IO a) -> IO a
withPassfile contents = bracket create removeFile
  where
    create = do
      directory <- getTemporaryDirectory
      (file, handle) <- openBinaryTempFile directory "native-pg-passfile"
      BS.hPut handle contents
      hClose handle
      pure file

withTemporaryDirectory :: (FilePath -> IO a) -> IO a
withTemporaryDirectory = bracket create removeDirectoryRecursive
  where
    create = do
      directory <- getTemporaryDirectory
      (path, handle) <- openBinaryTempFile directory "native-pg-home"
      hClose handle
      removeFile path
      createDirectory path
      pure path

withVariable :: String -> Maybe String -> IO a -> IO a
withVariable name value action = bracket (lookupEnv name) restore $ \_ -> do
  restore value
  action
  where
    restore = maybe (unsetEnv name) (setEnv name)

isolated :: IO a -> IO a
isolated action = do
  environment <- getEnvironment
  clear [name | (name, _) <- environment, "PG" `isPrefixOf` name]
  where
    clear names = foldr (flip withVariable Nothing) action names

-- | What the defaults take from the process they take once, when a program starts: the environment, the name of the
-- local user, the password files and the default root certificate file. Parsing a connection string then reads nothing.
readOnce :: Spec
readOnce = describe "defaults read once" $ do
  let hostsOf string defaults = map hostName . toList . connectionHosts <$> parseConnectionString defaults string
  it "reads the environment when it reads the defaults, and parsing reads it no more" $ isolated $
    withVariable "PGHOST" (Just "before.example") $ do
      defaults <- readDefaults "/tmp" Nothing []
      withVariable "PGHOST" (Just "after.example") $
        hostsOf "user=u" defaults `shouldBe` Right ["before.example"]
#if !defined(mingw32_HOST_OS)
  it "reads the name of the local user" $ isolated $ do
    defaults <- readDefaults "/tmp" Nothing []
    getEffectiveUserName >>= \name -> defaultUser defaults `shouldBe` Just name
  it "reads the default password file when it reads the defaults, and parsing reads it no more" $ isolated $
    withTemporaryDirectory $ \directory -> do
      let file = directory <> "/.pgpass"
      BS.writeFile file "*:5432:d:u:first\n"
      setFileMode file 0o600
      withVariable "HOME" (Just directory) $ do
        defaults <- readDefaults "/tmp" Nothing []
        BS.writeFile file "*:5432:d:u:second\n"
        passwordsOf defaults base `shouldBe` Right [Just "first"]
        removeFile file
        passwordsOf defaults base `shouldBe` Right [Just "first"]
#endif
  it "reads a password file that a connection string names when it reads the defaults, and not again" $ isolated $
    withPassfile "*:5432:d:u:first\n" $ \file -> do
      defaults <- readDefaults "/tmp" Nothing [file]
      BS.writeFile file "*:5432:d:u:second\n"
      passwordsOf defaults (base <> " passfile=" <> quoted (Char8.pack file)) `shouldBe` Right [Just "first"]
  -- A file that the defaults didn't read can't be read now: it is taken for one that can't be read, as a missing file is.
  it "takes a password file that the defaults didn't read for one that can't be read" $ isolated $
    withPassfile "*:5432:d:u:first\n" $ \file -> do
      defaults <- readDefaults "/tmp" Nothing []
      passwordsOf defaults (base <> " passfile=" <> quoted (Char8.pack file)) `shouldBe` Right [Nothing]
  it "looks for the default root certificate file when it reads the defaults, and parsing doesn't look again" $ isolated $
    withTemporaryDirectory $ \directory -> do
      BS.writeFile (directory <> "/root.crt") ""
      defaults <- readDefaults "/tmp" (Just directory) []
      removeFile (directory <> "/root.crt")
      parseConnectionString defaults "host=h user=u sslmode=require"
        `shouldSatisfy` P.left (P.hasInfix "verifies the server's certificate as verify-ca does")
  it "doesn't take a root certificate file that appeared later for one that was there" $ isolated $
    withTemporaryDirectory $ \directory -> do
      defaults <- readDefaults "/tmp" (Just directory) []
      BS.writeFile (directory <> "/root.crt") ""
      parseConnectionString defaults "host=h user=u sslmode=require" `shouldSatisfy` P.right P.anything
