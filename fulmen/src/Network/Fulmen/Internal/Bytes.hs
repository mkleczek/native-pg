-- | What the head of a request and the head of a response both need of bytes: the characters of a token, and names
-- that are compared without regard to case.
module Network.Fulmen.Internal.Bytes
  ( isToken,
    trim,
    lowerCase,
    equalsLower,
    commaList,
    parseField,
    isField,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU
import Data.Word (Word8)

-- | A character of a token: the name of a method or of a field.
isToken :: Word8 -> Bool
isToken byte =
  (byte >= 48 && byte <= 57)
    || (byte >= 65 && byte <= 90)
    || (byte >= 97 && byte <= 122)
    || byte `elem` punctuation
  where
    -- The characters of "!#$%&'*+-.^_`|~".
    punctuation = [33, 35, 36, 37, 38, 39, 42, 43, 45, 46, 94, 95, 96, 124, 126] :: [Word8]

-- | Without the spaces and tabs around it.
trim :: ByteString -> ByteString
trim = B.dropWhileEnd blank . B.dropWhile blank
  where
    blank byte = byte == 32 || byte == 9

lowerCase :: ByteString -> ByteString
lowerCase = B.map lowered

-- | Whether the name is the lower-case name, in any case, comparing the bytes where they are: nothing is allocated.
equalsLower :: ByteString -> ByteString -> Bool
equalsLower lower name = size == B.length name && go 0
  where
    size = B.length lower
    go !index
      | index >= size = True
      | BU.unsafeIndex lower index == lowered (BU.unsafeIndex name index) = go (index + 1)
      | otherwise = False

lowered :: Word8 -> Word8
lowered byte = if byte >= 65 && byte <= 90 then byte + 32 else byte

-- | The elements of a list, in lower case, without the empty ones.
commaList :: ByteString -> [ByteString]
commaList = filter (not . B.null) . map (lowerCase . trim) . B.split 44

-- | A header field: a name that is a token, a colon, and a value without the whitespace around it, which is not a
-- control character but a tab.
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

-- | Whether the line is a header field.
isField :: ByteString -> Bool
isField = maybe False (const True) . parseField
