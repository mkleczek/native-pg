{-# LANGUAGE OverloadedStrings #-}

-- | What a conformance case checks, on bytes: the responses that the server sent, parsed as a sequence of HTTP/1.x
-- responses (RFC 9112 section 6.3), and compared with what a case expects. Nothing here depends on fulmen.
module ConformanceWire
  ( Response (..),
    Parsed (..),
    parseResponses,
    matchOutcome,
    describeObserved,
  )
where

import ConformanceCases
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.Char (isDigit, isHexDigit, isSpace, toLower)
import Data.List (intercalate)
import Data.Maybe (isJust)
import Numeric (readHex)

-- | A response, as it was framed: the version, the status, the field lines in order, and the body, decoded.
data Response = Response
  { responseVersion :: ByteString,
    responseStatus :: Int,
    responseFields :: [(ByteString, ByteString)],
    responseBody :: ByteString
  }
  deriving (Show)

-- | The responses in the bytes, and what follows the last: bytes that don't make a response, or a response that is not
-- complete, and why the bytes couldn't be parsed, if they couldn't.
data Parsed = Parsed
  { parsedResponses :: [Response],
    parsedRest :: ByteString,
    parsedProblem :: Maybe String
  }

-- | The responses that the bytes have. The flags say which of the requests were HEAD, in order: such a response has no
-- body, however its head frames one. A response that runs to the end of the connection is complete once it ended.
parseResponses :: [Bool] -> Bool -> ByteString -> Parsed
parseResponses toHead ended = go toHead []
  where
    go flags found bytes
      | B.null bytes = Parsed (reverse found) bytes Nothing
      | otherwise = case splitHead bytes of
          Nothing -> Parsed (reverse found) bytes Nothing
          Just (headLines, afterHead) -> case parseHead headLines of
            Left problem -> Parsed (reverse found) bytes (Just problem)
            Right (version, status, fields) ->
              let noBody = or (take 1 flags) || status < 200 || status == 204 || status == 304
               in case frame noBody fields afterHead of
                    Incomplete -> Parsed (reverse found) bytes Nothing
                    Malformed problem -> Parsed (reverse found) bytes (Just problem)
                    Complete body rest -> go (drop 1 flags) (Response version status fields body : found) rest
    frame noBody fields afterHead
      | noBody = Complete "" afterHead
      | any chunkedCoding (valuesOf "transfer-encoding" fields) = chunkedBody afterHead
      | (value : _) <- valuesOf "content-length" fields = case readNumber value of
          Just count
            | B.length afterHead >= count -> Complete (B.take count afterHead) (B.drop count afterHead)
            | otherwise -> Incomplete
          Nothing -> Malformed "a Content-Length that is not a number"
      | ended = Complete afterHead ""
      | otherwise = Incomplete
    chunkedCoding value = "chunked" `B.isSuffixOf` lower (strip value)

data Framed = Incomplete | Malformed String | Complete ByteString ByteString

-- | The head, as its lines, and what follows it.
splitHead :: ByteString -> Maybe ([ByteString], ByteString)
splitHead bytes = case B.breakSubstring "\r\n\r\n" bytes of
  (headBytes, rest)
    | B.null rest -> Nothing
    | otherwise -> Just (splitLines headBytes, B.drop 4 rest)

splitLines :: ByteString -> [ByteString]
splitLines bytes = case B.breakSubstring "\r\n" bytes of
  (line, rest)
    | B.null rest -> [line]
    | otherwise -> line : splitLines (B.drop 2 rest)

parseHead :: [ByteString] -> Either String (ByteString, Int, [(ByteString, ByteString)])
parseHead [] = Left "no status line"
parseHead (statusLine : fieldLines) = do
  (version, status) <- case C.words statusLine of
    (version : code : _)
      | "HTTP/1." `B.isPrefixOf` version,
        C.length code == 3,
        C.all isDigit code,
        Just number <- readNumber code ->
          Right (version, number)
    _ -> Left ("a status line that is not one: " <> show statusLine)
  fields <- mapM field fieldLines
  pure (version, status, fields)
  where
    field line = case C.break (== ':') line of
      (name, rest)
        | B.null rest || B.null name -> Left ("a field line that is not one: " <> show line)
        | otherwise -> Right (name, strip (B.drop 1 rest))

-- | The body of a chunked response, decoded, and what follows it.
chunkedBody :: ByteString -> Framed
chunkedBody = go []
  where
    go chunks bytes = case B.breakSubstring "\r\n" bytes of
      (_, rest) | B.null rest -> Incomplete
      (sizeLine, rest) -> case parseSize sizeLine of
        Nothing -> Malformed ("a chunk size that is not one: " <> show sizeLine)
        Just 0 -> trailers (B.drop 2 rest) (B.concat (reverse chunks))
        Just size
          | B.length rest < 2 + size + 2 -> Incomplete
          | B.take 2 (B.drop (2 + size) rest) /= "\r\n" -> Malformed "a chunk that is not followed by a CRLF"
          | otherwise -> go (B.take size (B.drop 2 rest) : chunks) (B.drop (2 + size + 2) rest)
    -- The trailer section, which is dropped, ends with an empty line.
    trailers bytes body = case B.breakSubstring "\r\n" bytes of
      (_, rest) | B.null rest -> Incomplete
      (line, rest)
        | B.null line -> Complete body (B.drop 2 rest)
        | otherwise -> trailers (B.drop 2 rest) body
    parseSize line = case C.unpack (C.takeWhile (/= ';') line) of
      digits
        | not (null digits), all isHexDigit digits, [(size, "")] <- readHex digits -> Just size
        | otherwise -> Nothing

valuesOf :: ByteString -> [(ByteString, ByteString)] -> [ByteString]
valuesOf name fields = [value | (field, value) <- fields, lower field == name]

lower :: ByteString -> ByteString
lower = C.map toLower

strip :: ByteString -> ByteString
strip = C.dropWhile isSpace . C.dropWhileEnd isSpace

readNumber :: ByteString -> Maybe Int
readNumber bytes = case C.readInt bytes of
  Just (number, rest) | B.null rest, number >= 0 -> Just number
  _ -> Nothing

-- | Whether the observed exchange is this outcome, or why it is not: the same number of responses, each as expected, and
-- the connection as the outcome says.
matchOutcome :: Outcome -> Parsed -> Bool -> Either String ()
matchOutcome outcome parsed ended = do
  maybe (Right ()) (\problem -> Left ("the responses can't be parsed: " <> problem)) (parsedProblem parsed)
  let expected = outResponses outcome
      observed = parsedResponses parsed
  if length observed == length expected
    then Right ()
    else Left ("expected " <> show (length expected) <> " responses, got " <> show (length observed))
  if B.null (parsedRest parsed) then Right () else Left ("bytes of an unfinished response, or after the last: " <> show (B.take 80 (parsedRest parsed)))
  mapM_ (\(index, (want, got)) -> either (\why -> Left ("response " <> show (index :: Int) <> ": " <> why)) Right (matchResponse want got)) (zip [1 ..] (zip expected observed))
  case outConnection outcome of
    ConnClosed | not ended -> Left "the connection was not closed"
    ConnOpen | ended -> Left "the connection was closed"
    _ -> Right ()

matchResponse :: ResponseExpect -> Response -> Either String ()
matchResponse want got = do
  case rsStatus want of
    Status expected | responseStatus got /= expected -> Left ("status " <> show (responseStatus got) <> ", expected " <> show expected)
    StatusOneOf expected | responseStatus got `notElem` expected -> Left ("status " <> show (responseStatus got) <> ", expected one of " <> show expected)
    _ -> Right ()
  case rsVersion want of
    Just expected | expected /= responseVersion got -> Left ("version " <> show (responseVersion got) <> ", expected " <> show expected)
    _ -> Right ()
  mapM_ (checkHeader (responseFields got)) (rsHeaders want)
  case rsBody want of
    BodyIs expected | expected /= responseBody got -> Left ("body " <> show (B.take 80 (responseBody got)) <> " (" <> show (B.length (responseBody got)) <> " bytes), expected " <> show (B.take 80 expected) <> " (" <> show (B.length expected) <> " bytes)")
    BodyContains needle | not (needle `B.isInfixOf` responseBody got) -> Left ("body " <> show (B.take 80 (responseBody got)) <> " doesn't contain " <> show needle)
    _ -> Right ()

checkHeader :: [(ByteString, ByteString)] -> HeaderCheck -> Either String ()
checkHeader fields check = case check of
  HeaderValues name expected
    | valuesOf (lower name) fields /= expected -> Left ("field " <> show name <> " has " <> show (valuesOf (lower name) fields) <> ", expected " <> show expected)
  HeaderPresent name
    | null (valuesOf (lower name) fields) -> Left ("field " <> show name <> " is missing")
  HeaderAbsent name
    | not (null (valuesOf (lower name) fields)) -> Left ("field " <> show name <> " is there: " <> show (valuesOf (lower name) fields))
  HeaderNoValue name value
    | any (\line -> lower value `elem` map (lower . strip) (C.split ',' line)) (valuesOf (lower name) fields) -> Left ("field " <> show name <> " has the value " <> show value)
  HeaderSatisfies name description predicate -> case valuesOf (lower name) fields of
    [value] | predicate value -> Right ()
    values -> Left ("field " <> show name <> " is not " <> description <> ": " <> show values)
  _ -> Right ()

-- | What was observed, in a line: the statuses, and whether the connection was closed.
describeObserved :: Parsed -> Bool -> String
describeObserved parsed ended =
  intercalate ", " (map (show . responseStatus) (parsedResponses parsed))
    <> (if isJust (parsedProblem parsed) || not (B.null (parsedRest parsed)) then " and an unfinished or unparsable rest of " <> show (B.length (parsedRest parsed)) <> " bytes" else "")
    <> (if ended then "; closed" else "; open")
