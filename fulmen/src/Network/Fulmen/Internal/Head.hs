{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The head of a request: where it ends in the bytes that have arrived, which is found as they arrive and looks at
-- each byte once, and then what it says, parsed whole into owned metadata. A head that can't be served is refused
-- with the status that says why: 414 for a request line that is too long, 431 for a header line or a head that is, 400
-- for a head that is malformed, 413 for a body over its limit and 501 for a transfer coding that isn't understood.
module Network.Fulmen.Internal.Head
  ( Scan (..),
    Spill (..),
    spilled,
    joined,
    notYet,
    startScan,
    scanHead,
    Framing (..),
    Parsed (..),
    parseHead,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU
import Network.Fulmen.Internal.Bytes (commaList, equalsLower, isToken, lowerCase, trim)
import Network.Fulmen.Request (Method (..), Request (..), Version (..))

-- | The bytes of a head that have been taken out of the buffer, so that the buffer can be received into again: how many,
-- and the pieces, the latest first. A head that is more than the buffer is read in several of its fills.
data Spill = NoSpill | Spill !Int [ByteString]

-- | How many bytes a spill has.
spilled :: Spill -> Int
spilled = \case
  NoSpill -> 0
  Spill size _ -> size

-- | The head as owned bytes: the pieces that were taken out of the buffer, and then the bytes in it.
joined :: Spill -> ByteString -> ByteString
joined spill rest = case spill of
  NoSpill -> rest
  Spill _ pieces -> B.concat (reverse (rest : pieces))

-- | Where the request line starts, while it is not known: its line is not complete, as leading empty lines come before it.
notYet :: Int
notYet = minBound

-- | How far the bytes have been looked at: they were, up to a point, with the start of the line that is not yet
-- complete and the start of the request line, and what has been taken out of the buffer already, where a start that
-- is before the buffer is negative, and the line in progress is as long as its start says; or the head is complete,
-- from the start of its request line to the end of its empty line, which is in the buffer, with the start negative if
-- the head began before it; or the buffer is full and the head is not complete; or it is refused.
data Scan
  = Scanning !Int !Int !Int !Spill
  | Complete !Int !Int !Spill
  | Full !Int !Int !Spill
  | Refused !Int

startScan :: Scan
startScan = Scanning 0 0 notYet NoSpill

-- | Looks at the bytes that arrived since the scan. A line may take @lineLimit@ bytes, without its end, and the head
-- @headLimit@ in all, with what was taken out of the buffer, which has the capacity that it is given: a head that
-- fills it before its end arrives is 'Full', for the caller to take the bytes out.
scanHead :: Int -> Int -> Int -> ByteString -> Scan -> Scan
scanHead lineLimit headLimit capacity bytes = \case
  Scanning from lineStart requestStart spill -> go from lineStart requestStart spill (headLimit - spilled spill)
  done -> done
  where
    end = B.length bytes
    go from lineStart requestStart spill room = case B.elemIndex 10 (BU.unsafeDrop from bytes) of
      Nothing -> unfinished lineStart requestStart spill room
      Just offset ->
        let feed = from + offset
            lineEnd = if feed > max 0 lineStart && BU.unsafeIndex bytes (feed - 1) == 13 then feed - 1 else feed
            size = lineEnd - lineStart
            first = requestStart == notYet
         in if
              | size > lineLimit -> Refused (if first then 414 else 431)
              | feed + 1 > room -> Refused 431
              | size == 0 -> if first then go (feed + 1) (feed + 1) requestStart spill room else Complete requestStart (feed + 1) spill
              | otherwise -> go (feed + 1) (feed + 1) (if first then lineStart else requestStart) spill room
    -- No more ends of lines: the line in progress may already be too long, or the head, or may not have room.
    unfinished lineStart requestStart spill room
      | end - lineStart > lineLimit + 1 = Refused status
      | end > room = Refused 431
      | end >= capacity = Full lineStart requestStart spill
      | otherwise = Scanning end lineStart requestStart spill
      where
        status = if requestStart == notYet then 414 else 431

-- | How the body of the request is framed.
data Framing
  = -- | The request has no body.
    NoContent
  | -- | A body of this many bytes, which isn't zero.
    Length !Int
  | -- | A body in chunks.
    Chunked
  deriving (Eq, Show)

-- | The request, how its body is framed, and whether the client wants the connection kept open.
data Parsed = Parsed !Request !Framing !Bool

-- | What a head says: the bytes are the head, from the request line to the empty line, which are all the head has,
-- and the request owns them. @bodyLimit@ is the most that a body may have.
parseHead :: Maybe Int -> ByteString -> Either Int Parsed
parseHead bodyLimit owned = do
  (requestLine, fieldLines) <- case splitLines owned of
    line : others -> Right (line, others)
    [] -> Left 400
  (method, target, version) <- parseRequestLine requestLine
  fields <- traverse (maybe (Left 400) Right . parseField) fieldLines
  let named name = [value | (field, value) <- fields, equalsLower name field]
  case (version, named "host") of
    (Http11, [value]) | B.any (\byte -> byte == 32 || byte == 9) value -> Left 400
    (Http11, [_]) -> Right ()
    (Http11, _) -> Left 400
    _ -> Right ()
  framing <- framingOf bodyLimit (named "content-length") (named "transfer-encoding")
  let options = concatMap commaList (named "connection")
      closes = "close" `elem` options
      keepsAlive = case version of
        Http11 -> not closes
        Http10 -> "keep-alive" `elem` options && not closes
  Right (Parsed (Request method target version fields) framing keepsAlive)

-- | The lines of a head, without their ends, up to the empty line.
splitLines :: ByteString -> [ByteString]
splitLines bytes = case B.elemIndex 10 bytes of
  Nothing -> []
  Just offset ->
    let line = B.take offset bytes
        stripped = if not (B.null line) && B.last line == 13 then B.init line else line
     in if B.null stripped then [] else stripped : splitLines (B.drop (offset + 1) bytes)

-- | Method, target and version, each separated by one space.
parseRequestLine :: ByteString -> Either Int (Method, ByteString, Version)
parseRequestLine line = case B.split 32 line of
  [method, target, version]
    | not (B.null method) && B.all isToken method,
      not (B.null target) && B.all (\byte -> byte > 32 && byte /= 127) target ->
        case version of
          "HTTP/1.1" -> Right (Method method, target, Http11)
          "HTTP/1.0" -> Right (Method method, target, Http10)
          _ | "HTTP/" `B.isPrefixOf` version -> Left 505
          _ -> Left 400
  _ -> Left 400

-- | A header field: a name that is a token, a colon, and a value without the whitespace around it. A line that
-- begins with whitespace is the continuation of the line before, which is refused.
parseField :: ByteString -> Maybe (ByteString, ByteString)
parseField line = case B.elemIndex 58 line of
  Just colon
    | colon > 0,
      B.all isToken (B.take colon line),
      not (B.any isControl value) ->
        Just (B.take colon line, value)
    where
      value = trim (B.drop (colon + 1) line)
  _ -> Nothing
  where
    isControl byte = (byte < 32 && byte /= 9) || byte == 127

-- | The framing of the body, from the lengths and the transfer codings that the head states.
framingOf :: Maybe Int -> [ByteString] -> [ByteString] -> Either Int Framing
framingOf bodyLimit lengths codings = case (lengths, codings) of
  ([], []) -> Right NoContent
  (_ : _, _ : _) -> Left 400
  (_, []) -> do
    sizes <- traverse number (concatMap commaListKeepingEmpty lengths)
    case sizes of
      size : others
        | all (== size) others ->
            if size > toInteger (maxBound :: Int)
              then Left 400
              else
                if maybe False (fromInteger size >) bodyLimit
                  then Left 413
                  else Right (if size == 0 then NoContent else Length (fromInteger size))
      _ -> Left 400
  ([], _) -> case filter (not . B.null) (map (lowerCase . trim) (concatMap (B.split 44) codings)) of
    [final] | final == "chunked" -> Right Chunked
    others
      | not (null others) && last others == "chunked" -> Left 501
      | otherwise -> Left 400
  where
    number token
      | not (B.null token) && B.all (\byte -> byte >= 48 && byte <= 57) token = Right (B.foldl' (\total byte -> total * 10 + toInteger (byte - 48)) 0 token)
      | otherwise = Left 400
    -- A list may be empty only if it is not the whole value: an empty element is not a length.
    commaListKeepingEmpty = map trim . B.split 44
