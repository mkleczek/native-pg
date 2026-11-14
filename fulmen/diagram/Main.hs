-- | Draws the server's program, built at the 'Diagram' arrow, which runs nothing: it writes @fulmen-server.html@ and one
-- @ANCHOR.mmd@ for each page into the directory that is its argument, which has to exist. It then says, for each page,
-- how many actions have no name, which are drawn as "IO", and how many decisions have no question, which are drawn as
-- "choose". It ends with how many pairs of steps were drawn as one, and a line for each exchange, which says what the
-- ends of it are in. The pages are written without the plumbing as @ANCHOR.mmd@ and with it as @ANCHOR.plumbing.mmd@.
--
-- > fulmen-diagram DIRECTORY
--
-- With @--lint@ it prints the lint of the server's structure instead, and ends with a failure if a finding is not
-- allowed by a part of the program.
--
-- > fulmen-diagram --lint
module Main (main) where

import Control.Monad (forM_, unless)
import Data.List (intercalate)
import Network.Streaming.Diagram.Mermaid (Page (..), Pair (..), html, pairs, pages)
import Network.Streaming.Lint (Report (..), lint, renderReport)
import ServerProgram (withServer)
import System.Environment (getArgs)
import System.Exit (die, exitFailure)
import System.IO (IOMode (..), hPutStr, hSetEncoding, utf8, withFile)

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["--lint"] -> lintServer
    [directory] -> draw directory
    _ -> die "usage: fulmen-diagram DIRECTORY | fulmen-diagram --lint"

-- | Prints the lint of the server, and fails if a finding is not allowed.
lintServer :: IO ()
lintServer = do
  linted <- withServer lint
  putStr (renderReport linted)
  unless (null (reportFindings linted)) exitFailure

-- | Writes the pages of the server.
draw :: FilePath -> IO ()
draw directory = do
  found <- withServer pages
  exchanges <- withServer pairs
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

-- | Writes the text as UTF-8, whatever the locale is.
write :: FilePath -> String -> IO ()
write path text = withFile path WriteMode $ \file -> hSetEncoding file utf8 *> hPutStr file text
