{-# LANGUAGE DataKinds #-}

-- | Draws the server's program, built at the 'Diagram' arrow, which runs nothing: it writes @fulmen-server.html@ and one
-- @ANCHOR.mmd@ for each page into the directory that is its argument, which has to exist. It then says, for each page,
-- how many actions have no name, which are drawn as "IO", and how many decisions have no question, which are drawn as
-- "choose". It ends with how many pairs of steps were drawn as one, and a line for each exchange, which says what the
-- ends of it are in. The pages are written without the plumbing as @ANCHOR.mmd@ and with it as @ANCHOR.plumbing.mmd@.
--
-- > fulmen-diagram DIRECTORY
module Main (main) where

import Control.Monad (forM_)
import Data.List (intercalate)
import qualified Network.Fulmen as Fulmen
import qualified Network.Socket as Socket
import Network.Streaming.BufferPool (BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes (Annotate (..), Effect (..), Label (..))
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.Diagram.Mermaid (Page (..), Pair (..), html, pairs, pages)
import Network.Streaming.PositiveInt (positiveInt)
import System.Environment (getArgs)
import System.Exit (die)
import System.IO (IOMode (..), hPutStr, hSetEncoding, utf8, withFile)

main :: IO ()
main = do
  args <- getArgs
  case args of
    [directory] -> draw directory
    _ -> die "usage: fulmen-diagram DIRECTORY"

-- | Writes the pages of the server. The listener and the pool are the settings' own, and are never used: nothing runs.
-- The server has four workers on each core, so that the pages show a count of them.
draw :: FilePath -> IO ()
draw directory = do
  listener <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
  pool <- newBufferPool (BufferPoolSettings (positiveInt @16384) (positiveInt @3) (positiveInt @1) (positiveInt @1))
  let program = server ((Fulmen.defaultSettings listener pool) {Fulmen.connectionsPerCore = 4})
  found <- pages program
  exchanges <- pairs program
  Socket.close listener
  write (directory ++ "/fulmen-server.html") (html "Fulmen server" found)
  forM_ found $ \page -> do
    write (directory ++ "/" ++ pageAnchor page ++ ".mmd") (pageMermaid page)
    write (directory ++ "/" ++ pageAnchor page ++ ".plumbing.mmd") (pagePlumbing page)
  putStrLn ("wrote " ++ show (length found) ++ " pages into " ++ directory)
  report found
  putStrLn ("folds: " ++ show (sum (pageFolds <$> found)) ++ " pairs of steps drawn as one")
  forM_ exchanges $ \pair ->
    putStrLn . concat $
      ["exchange ", pairName pair, ": answered in ", boxes (pairAnswering pair), "; asked in ", boxes (pairAsking pair)]

-- | The boxes that the ends are in, each as the path of box titles from the top.
boxes :: [[String]] -> String
boxes = intercalate ", " . fmap (intercalate " > ")

-- | A line for each page with its counts of unnamed actions and unquestioned decisions, and then the totals.
report :: [Page] -> IO ()
report found = do
  forM_ found $ \page -> putStrLn (pageAnchor page ++ ": " ++ counts (pageUnnamed page) (pageUnquestioned page))
  putStrLn ("total: " ++ counts (sum (pageUnnamed <$> found)) (sum (pageUnquestioned <$> found)))
  where
    counts unnamed unquestioned = show unnamed ++ " unnamed actions, " ++ show unquestioned ++ " unquestioned decisions"

-- | The server with a handler that does nothing.
server :: Fulmen.Settings -> Diagram Fulmen.Cores ()
server settings = Fulmen.serve settings handler
  where
    handler = named (Label "handler" "the application's handler") (now (const (pure ())))

-- | Writes the text as UTF-8, whatever the locale is.
write :: FilePath -> String -> IO ()
write path text = withFile path WriteMode $ \file -> hSetEncoding file utf8 *> hPutStr file text
