{-# LANGUAGE OverloadedStrings #-}

-- | HTTP/1.1 server conformance cases for fulmen, as data.
--
-- Every case is raw request bytes sent over one TCP connection, plus what the
-- server's answer must satisfy. Nothing here depends on fulmen's Haskell API,
-- and the module imports nothing but @base@ and @bytestring@.
--
-- = How a runner executes a case
--
-- * __Server.__ Start a fulmen server with the case's 'Handler' and
--   'Settings' (a field left 'Nothing' is the server's default), connect one
--   TCP client, and run 'caseSteps' in order. A background reader collects
--   every byte the server sends from the moment of connecting.
-- * __Steps.__ See 'Step'. A step that waits ('AwaitReceived') fails the case
--   if what it waits for has not arrived after the /patience/ (3 s, plus the
--   case's timeout setting for the timeout cases).
-- * __Responses.__ After the last step, keep reading until the connection
--   closes or the patience runs out. Parse what arrived as a sequence of
--   HTTP/1.x responses (RFC 9112 section 6.3): a response to a HEAD request
--   ('rsToHead'), and every 1xx, 204 and 304 response, has no body; otherwise
--   the body is framed by @Transfer-Encoding: chunked@, else by
--   @Content-Length@, else (HTTP/1.0 style) it runs to the end of the
--   connection. Interim (1xx) responses are responses in the sequence. The
--   body of a chunked response is compared decoded.
-- * __Matching.__ A case passes if the exchange matches at least one of the
--   'Outcome's in 'caseExpect' (almost every case lists exactly one). An
--   outcome matches if the observed responses are exactly the listed ones in
--   number and order, each satisfies its 'ResponseExpect', and the
--   connection ends as the outcome's 'ConnectionExpect' says. Bytes after the
--   last listed response (a second response, say) are a mismatch.
-- * __Connection.__ 'ConnClosed': the server closes (FIN or RST) within the
--   patience, once it has sent the listed responses. 'ConnOpen': the server
--   has not closed 300 ms after the last listed response. 'ConnAny': not
--   checked. When the server closes while the client still has unread request
--   bytes in flight, a reset may eat the last response: a runner treats
--   ECONNRESET after the expected responses as a close, and the cases where
--   this can lose a response say so in their note.
--
-- = The handlers
--
-- The fixed set the runner maps to fulmen handlers; see 'Handler'. Where this
-- module needs an exact byte format (the 'Headers' body, the 'Ignore' body) it
-- is stated there.
--
-- = Origins
--
-- 'caseOrigin' says where a case comes from: a Warp test (file and test name,
-- all at wai commit @b89c12ac0037@), an RFC section ('RFC': derived by hand,
-- for points the Warp tests do not cover), or fulmen's own stated scope
-- ('Spec'). 'caseNote' records where fulmen is expected to differ from Warp,
-- and anything a runner author should know.
module ConformanceCases
  ( -- * The cases
    Case (..)
  , cases
  , casesByArea
  , duplicateNames
    -- * Parts of a case
  , Area (..)
  , areaTitle
  , Origin (..)
  , Handler (..)
  , ignoreBody
  , headersBody
  , Settings (..)
  , noSettings
  , Step (..)
  , Outcome (..)
  , ConnectionExpect (..)
  , ResponseExpect (..)
  , StatusCheck (..)
  , HeaderCheck (..)
  , BodyCheck (..)
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.Char (toLower)
import Data.List (intersperse, nub, (\\))
import Numeric (showHex)

----------------------------------------------------------------------
-- Types

-- | One conformance case.
data Case = Case
  { caseName :: String
    -- ^ Unique within 'cases'.
  , caseArea :: Area
  , caseOrigin :: Origin
  , caseHandler :: Handler
  , caseSettings :: Settings
  , caseSteps :: [Step]
  , caseExpect :: [Outcome]
    -- ^ Acceptable ways for the exchange to end; at least one must match.
  , caseNote :: String
    -- ^ Empty, or where fulmen differs from Warp, or advice to the runner.
  }

-- | What a case exercises. 'cases' is grouped by area.
data Area
  = ARequestLine
  | AHeaderFields
  | AHeadSplits
  | AHost
  | AHeadLimits
  | ABodyLimit
  | AContentLength
  | AChunked
  | AFraming
  | AExpect
  | AUnreadBody
  | AResponseFraming
  | AResponseHeaders
  | AConnection
  | APipelining
  | AHandlerErrors
  | ATimeouts
  | AStreaming
  deriving (Eq, Ord, Show, Enum, Bounded)

areaTitle :: Area -> String
areaTitle a = case a of
  ARequestLine -> "request line"
  AHeaderFields -> "header fields"
  AHeadSplits -> "head arriving in pieces"
  AHost -> "Host"
  AHeadLimits -> "head limits (414, 431)"
  ABodyLimit -> "body limit (413)"
  AContentLength -> "Content-Length bodies"
  AChunked -> "chunked request bodies"
  AFraming -> "ambiguous body framing"
  AExpect -> "Expect: 100-continue"
  AUnreadBody -> "unread request bodies"
  AResponseFraming -> "response framing"
  AResponseHeaders -> "response headers"
  AConnection -> "connection persistence"
  APipelining -> "pipelining"
  AHandlerErrors -> "handler failure"
  ATimeouts -> "timeouts"
  AStreaming -> "streaming"

-- | Where a case comes from.
data Origin
  = Warp String String
    -- ^ A Warp test: the file (in the wai repo, at @b89c12ac0037@) and the
    -- test, as @describe@ path and @it@ text. A case adapted from a test
    -- that sits in the fork's @streams-test@ says so in the file name.
  | RFC String String
    -- ^ Derived by hand from the RFC: the section (for example
    -- @"RFC 9112 7.1.1"@) and the point it makes.
  | Spec String
    -- ^ The behaviour fulmen's scope statement asks for, with no Warp test
    -- or RFC rule behind it beyond the status code's meaning.
  deriving (Eq, Show)

-- | The handlers a case may assume. Each answers every request on the
-- connection the same way.
data Handler
  = Echo
    -- ^ 200 with the request body (decoded, trailers dropped) as the response
    -- body, and a @Content-Length@ of its length. It reads the whole request
    -- body before it answers, so nothing of the answer is sent before then.
  | Fixed Int [(ByteString, ByteString)] ByteString
    -- ^ Answers with this status, these headers (in this order, verbatim: no
    -- header is added or removed by the handler) and this body. The server
    -- adds what a server adds (@Date@, @Server@, @Connection@, framing), and
    -- drops the body where the status or method forbids it. A body whose
    -- length does not agree with a @Content-Length@ among the headers is not
    -- used by any case.
  | Streamed [ByteString]
    -- ^ 200 with no @Content-Length@ and these chunks written in order, each
    -- flushed: a body of unknown length. Empty chunks are written as nothing
    -- (they must not end a chunked body).
  | Headers
    -- ^ 200 with a body listing the request as the handler saw it, in the
    -- format 'headersBody' gives: a @METHOD@ line, a @TARGET@ line (the
    -- request-target exactly as received), then one @HEADER@ line per header
    -- field line in the order received, @HEADER name=value@ with the name
    -- lower-cased and the value stripped of surrounding whitespace. A field
    -- repeated in the request is listed once per field line. The handler does
    -- not read the request body. The response has a @Content-Length@.
  | Ignore
    -- ^ 200 with the fixed body 'ignoreBody' and a @Content-Length@, without
    -- reading the request body.
  | Slow Int [ByteString]
    -- ^ Like 'Streamed', but waits this many milliseconds before every chunk.
  | EchoStreaming
    -- ^ ADDED, for Warp's @streaming echo #249@: 200 with no
    -- @Content-Length@; reads the request body piece by piece and writes each
    -- piece to the response as soon as it arrives, flushed. Echo cannot do
    -- this, because it needs the whole body before it can state a length.
  | Fail
    -- ^ ADDED, for Warp's @ExceptionSpec@: fails (throws) before it has
    -- started an answer. The server must answer 500 on its behalf.
  deriving (Eq, Show)

-- | The body of the 'Ignore' handler's answer.
ignoreBody :: ByteString
ignoreBody = "ignored"

-- | The body of the 'Headers' handler's answer for a request with this
-- method, target and header fields (names as written in the request; the
-- handler lower-cases them).
headersBody :: ByteString -> ByteString -> [(ByteString, ByteString)] -> ByteString
headersBody method target fields =
  B.concat
    ( ["METHOD ", method, "\n", "TARGET ", target, "\n"]
        ++ concat [["HEADER ", C.map toLower n, "=", v, "\n"] | (n, v) <- fields]
    )

-- | Server settings a case names. 'Nothing' is the server's default.
data Settings = Settings
  { lineLimit :: Maybe Int
    -- ^ Bytes a request line or header line (and a chunk-size or trailer
    -- line) may take, the size of the server's line buffer. Default 16384.
    -- Cases do not probe the exact boundary, only well inside and well
    -- outside it.
  , headLimit :: Maybe Int
    -- ^ Bytes the whole head (request line and header fields) may take.
    -- Default: the server's own, larger than 'lineLimit'.
  , bodyLimit :: Maybe Int
    -- ^ Bytes a request body may have. Default: none that a case reaches.
  , timeoutMs :: Maybe Int
    -- ^ How long a client may stay silent in the middle of a head or body
    -- before the server closes the connection.
  }
  deriving (Eq, Show)

noSettings :: Settings
noSettings = Settings Nothing Nothing Nothing Nothing

-- | What the client does.
data Step
  = Send ByteString
    -- ^ Write these bytes in one write.
  | PauseMs Int
    -- ^ Do nothing (but keep reading) for this long.
  | AwaitReceived ByteString
    -- ^ Wait until the bytes received so far from the server contain these
    -- (as a substring of the raw stream, framing included).
  | HalfClose
    -- ^ Shut down the sending side of the connection (the server reads
    -- end-of-file), and go on reading.
  | PauseReadingMs Int
    -- ^ Stop reading from the socket for this long, so that what the server
    -- sends fills the socket buffers. Reading resumes afterwards.
  deriving (Eq, Show)

-- | One acceptable way for an exchange to end.
data Outcome = Outcome
  { outResponses :: [ResponseExpect]
    -- ^ Exactly these responses, in this order, interim ones included. The
    -- empty list means the server sends nothing at all.
  , outConnection :: ConnectionExpect
  }

data ConnectionExpect = ConnClosed | ConnOpen | ConnAny
  deriving (Eq, Show)

-- | What one response must satisfy.
data ResponseExpect = ResponseExpect
  { rsStatus :: StatusCheck
  , rsVersion :: Maybe ByteString
    -- ^ The version in the status line, @"HTTP/1.1"@ or @"HTTP/1.0"@.
  , rsHeaders :: [HeaderCheck]
  , rsBody :: BodyCheck
  , rsToHead :: Bool
    -- ^ The request this answers was HEAD: no body follows the head however
    -- the head frames it (the runner needs this to parse the stream).
  }

data StatusCheck = Status Int | StatusOneOf [Int]
  deriving (Eq, Show)

data HeaderCheck
  = HeaderValues ByteString [ByteString]
    -- ^ The field lines named so (case-insensitively), in order, have
    -- exactly these values: no more, no fewer.
  | HeaderPresent ByteString
    -- ^ At least one field line has this name.
  | HeaderAbsent ByteString
  | HeaderNoValue ByteString ByteString
    -- ^ No field line with this name has this value (compared
    -- case-insensitively, the field value taken as a list of options): the
    -- field may be absent or say something else.
  | HeaderSatisfies ByteString String (ByteString -> Bool)
    -- ^ Exactly one field line has this name and its value satisfies the
    -- predicate (described by the string).

data BodyCheck
  = BodyIs ByteString
    -- ^ The (decoded) body is exactly this; @BodyIs ""@ also means no body.
  | BodyContains ByteString
  | BodyAny

----------------------------------------------------------------------
-- Building cases

mk :: Area -> Origin -> String -> Handler -> [Step] -> [Outcome] -> Case
mk area origin name handler steps outcomes =
  Case
    { caseName = name
    , caseArea = area
    , caseOrigin = origin
    , caseHandler = handler
    , caseSettings = noSettings
    , caseSteps = steps
    , caseExpect = outcomes
    , caseNote = ""
    }

withSettings :: Settings -> Case -> Case
withSettings s c = c {caseSettings = s}

note :: String -> Case -> Case
note n c = c {caseNote = n}

closes, stays, anyConn :: [ResponseExpect] -> Outcome
closes rs = Outcome rs ConnClosed
stays rs = Outcome rs ConnOpen
anyConn rs = Outcome rs ConnAny

resp :: Int -> ResponseExpect
resp n = ResponseExpect (Status n) Nothing [] BodyAny False

-- | A 200 with this body.
ok :: ByteString -> ResponseExpect
ok b = (resp 200) {rsBody = BodyIs b}

-- | A 200 from the 'Headers' handler for this request.
okHeaders :: ByteString -> ByteString -> [(ByteString, ByteString)] -> ResponseExpect
okHeaders m t fs = ok (headersBody m t fs)

withHeaders :: ResponseExpect -> [HeaderCheck] -> ResponseExpect
withHeaders r hs = r {rsHeaders = rsHeaders r ++ hs}

withVersion :: ByteString -> ResponseExpect -> ResponseExpect
withVersion v r = r {rsVersion = Just v}

toHead :: ResponseExpect -> ResponseExpect
toHead r = r {rsToHead = True}

headerIs :: ByteString -> ByteString -> HeaderCheck
headerIs n v = HeaderValues n [v]

-- | The request was refused: this status, nothing else, connection closed.
refused :: Int -> [Outcome]
refused n = [closes [resp n]]

-- | Refused with one of these statuses.
refusedOneOf :: [Int] -> [Outcome]
refusedOneOf ns = [closes [(resp 0) {rsStatus = StatusOneOf ns}]]

-- | The server gives up without a good answer: nothing, or an error status.
-- Either way the connection ends.
givesUp :: [Outcome]
givesUp = [closes [], closes [(resp 0) {rsStatus = StatusOneOf [400, 408]}]]

sends :: [ByteString] -> [Step]
sends = intersperse (PauseMs 10) . map Send

bytewise :: ByteString -> [Step]
bytewise = map (Send . B.singleton) . B.unpack

num :: Int -> ByteString
num = C.pack . show

-- | The IMF-fixdate of RFC 9110 section 5.6.7, for example
-- @Mon, 13 Aug 2012 04:22:55 GMT@.
isImfFixdate :: ByteString -> Bool
isImfFixdate v =
  B.length v == 29
    && C.index v 3 == ','
    && C.index v 4 == ' '
    && " GMT" `B.isSuffixOf` v

----------------------------------------------------------------------
-- Requests

singleGet, singlePostHello, chunkedHead :: ByteString
singleGet = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
singlePostHello = "POST /hello HTTP/1.1\r\nHost: localhost\r\nContent-length: 5\r\n\r\nHello"
chunkedHead = "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"

-- | A chunked POST to /hello as Warp's tests send it: head, then body.
singleChunkedPostHello :: [ByteString]
singleChunkedPostHello =
  [ "POST /hello HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
  , "5\r\nHello\r\n0\r\n\r\n"
  ]

-- | The head of a POST with a body of this length.
postHead :: Int -> ByteString
postHead n = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: " <> num n <> "\r\n\r\n"

postBody :: ByteString -> ByteString
postBody b = postHead (B.length b) <> b

get :: ByteString -> ByteString
get target = "GET " <> target <> " HTTP/1.1\r\nHost: localhost\r\n\r\n"

-- | A GET with these header lines after Host (each without its CRLF).
getWith :: [ByteString] -> ByteString
getWith hs = "GET / HTTP/1.1\r\nHost: localhost\r\n" <> B.concat [h <> "\r\n" | h <- hs] <> "\r\n"

-- | The chunked coding of these chunks, with the last-chunk and an empty
-- trailer section.
chunked :: [ByteString] -> ByteString
chunked cs = B.concat ([C.pack (showHex (B.length c) "") <> "\r\n" <> c <> "\r\n" | c <- cs] ++ ["0\r\n\r\n"])

chunkedPost :: [ByteString] -> ByteString
chunkedPost cs = chunkedHead <> chunked cs

-- | What a request smuggler appends: a second request, which a server that
-- frames the first one differently than the front end would answer.
smuggled :: ByteString
smuggled = "GET /smuggled HTTP/1.1\r\nHost: foo\r\n\r\n"

-- | A POST with these header lines (each ending in CRLF) after Host, then the
-- end of the head, then a smuggled request.
smuggle :: ByteString -> ByteString
smuggle hdrs = "POST / HTTP/1.1\r\nHost: foo\r\n" <> hdrs <> "\r\n" <> smuggled

-- | A body of this many bytes, easy to tell apart when it goes wrong.
filler :: Int -> ByteString
filler n = B.pack (take n (cycle [65 .. 90]))

hostLocal :: (ByteString, ByteString)
hostLocal = ("Host", "localhost")

-- | The origin of a case taken from Warp's RunSpec.
runSpec :: String -> Origin
runSpec = Warp "warp/test/RunSpec.hs"

----------------------------------------------------------------------
-- All cases

cases :: [Case]
cases =
  concat
    [ requestLineCases
    , headerFieldCases
    , headSplitCases
    , hostCases
    , headLimitCases
    , bodyLimitCases
    , contentLengthCases
    , chunkedCases
    , framingCases
    , expectCases
    , unreadBodyCases
    , responseFramingCases
    , responseHeaderCases
    , connectionCases
    , pipeliningCases
    , handlerErrorCases
    , timeoutCases
    , streamingCases
    ]

-- | The number of cases in each area.
casesByArea :: [(Area, Int)]
casesByArea = [(a, length [() | c <- cases, caseArea c == a]) | a <- [minBound .. maxBound]]

-- | Names used by more than one case (there should be none).
duplicateNames :: [String]
duplicateNames = let ns = map caseName cases in nub (ns \\ nub ns)

----------------------------------------------------------------------
-- Request line

requestLineCases :: [Case]
requestLineCases =
  [ mk ARequestLine (Warp "warp/test/doctests.hs" "RequestHeader.parseRequestLine: a path with a query")
      "a path and a query string are passed as received"
      Headers
      [Send (get "/a/b?x=1&y=%20z")]
      [stays [okHeaders "GET" "/a/b?x=1&y=%20z" [hostLocal]]]
  , mk ARequestLine (RFC "RFC 9112 3.2.4" "asterisk-form is the target of a server-wide OPTIONS")
      "OPTIONS * is served"
      Headers
      [Send "OPTIONS * HTTP/1.1\r\nHost: localhost\r\n\r\n"]
      [stays [okHeaders "OPTIONS" "*" [hostLocal]]]
  , mk ARequestLine (RFC "RFC 9110 9.1" "a method other than GET reaches the handler as sent")
      "a DELETE is passed as received"
      Headers
      [Send "DELETE /thing/1 HTTP/1.1\r\nHost: localhost\r\n\r\n"]
      [stays [okHeaders "DELETE" "/thing/1" [hostLocal]]]
  , mk ARequestLine (Warp "warp/test/doctests.hs" "RequestHeader.parseRequestLine: \"GET \" is an invalid first line")
      "a request line with no target and no version is refused"
      Headers
      [Send "GET \r\nHost: localhost\r\n\r\n"]
      (refused 400)
      & note "Warp closes without answering on a bad request line (BadFirstLine); fulmen answers 400 as its scope says."
  , mk ARequestLine (Warp "warp/test/doctests.hs" "RequestHeader.parseRequestLine: \"GET /\" has no version")
      "a request line with no version (HTTP/0.9 style) is refused"
      Headers
      [Send "GET /\r\nHost: localhost\r\n\r\n"]
      (refused 400)
      & note "Warp closes without answering (BadFirstLine); fulmen answers 400."
  , mk ARequestLine (RFC "RFC 9112 3" "request-line = method SP request-target SP HTTP-version")
      "a request line with a missing target is refused"
      Headers
      [Send "GET  HTTP/1.1\r\nHost: localhost\r\n\r\n"]
      (refused 400)
      & note "Warp closes without answering (BadFirstLine); fulmen answers 400."
  , mk ARequestLine (Warp "warp/test/doctests.hs" "RequestHeader.parseRequestLine: \"GET /NotHTTP UNKNOWN/1.1\" is a non-HTTP request")
      "a request line that names another protocol is refused"
      Headers
      [Send "GET /NotHTTP UNKNOWN/1.1\r\nHost: localhost\r\n\r\n"]
      (refused 400)
  , mk ARequestLine (RFC "RFC 9112 3" "a line that is not a request line at all")
      "text that is not HTTP is refused"
      Headers
      [Send "THIS IS NOT HTTP AT ALL\r\n\r\n"]
      (refused 400)
  , mk ARequestLine (RFC "RFC 9112 2.2" "a server SHOULD ignore at least one empty line before the request-line")
      "one empty line before the request line is ignored or the request refused"
      Headers
      [Send ("\r\n" <> get "/x")]
      [stays [okHeaders "GET" "/x" [hostLocal]], closes [resp 400]]
      & note "RFC: SHOULD ignore. Warp reads the empty line as an empty head and answers 400, so both are accepted."
  ]

-- | Left-to-right application, to read a case and then its note.
(&) :: a -> (a -> b) -> b
x & f = f x

infixl 1 &

----------------------------------------------------------------------
-- Header fields

headerFieldCases :: [Case]
headerFieldCases =
  [ mk AHeaderFields (runSpec "special input / refuses multiline headers")
      "a header line continued on the next line (obs-fold) is refused"
      Headers
      [Send "GET / HTTP/1.1\r\nHost: localhost\r\nfoo:    bar\r\n baz\r\n\tbin\r\n\r\n"]
      (refused 400)
      & note "Warp's test has no Host header; one is added here so that the continuation is the only fault."
  , mk AHeaderFields (runSpec "special input / refuses multiline headers split across writes")
      "a continuation line that arrives in a later write is refused"
      Headers
      [ Send "GET / HTTP/1.1\r\nHost: localhost\r\nfoo: and\r\n"
      , PauseMs 50
      , Send " baz as well\r\n\r\n"
      ]
      (refused 400)
  , mk AHeaderFields (RFC "RFC 9112 5.2" "obs-fold with a tab; the server MUST reject or replace it")
      "a header line continued with a tab is refused"
      Headers
      [Send "GET / HTTP/1.1\r\nHost: localhost\r\nX-Foo: bar\r\n\tbaz\r\n\r\n"]
      (refused 400)
  , mk AHeaderFields (RFC "RFC 9112 2.2" "whitespace between the start-line and the first field")
      "a header line that starts with a space before the first field is refused or ignored"
      Headers
      [Send "GET / HTTP/1.1\r\n X: y\r\nHost: localhost\r\n\r\n"]
      [closes [resp 400], stays [okHeaders "GET" "/" [hostLocal]]]
      & note "RFC: reject, or consume the line without processing it."
  , mk AHeaderFields (runSpec "header field names / refuses a continuation line")
      "a continuation line cannot hide a Content-Length"
      Headers
      [Send (smuggle "X-Foo: bar\r\n Content-Length: 36\r\n")]
      (refused 400)
  , mk AHeaderFields (runSpec "special input / no space between colon and value")
      "no space is needed between the colon and the value"
      Headers
      [Send "GET / HTTP/1.1\r\nHost: foo\r\nfoo:bar\r\n\r\n"]
      [stays [okHeaders "GET" "/" [("Host", "foo"), ("foo", "bar")]]]
  , mk AHeaderFields (runSpec "header field names / refuses whitespace between a field name and its colon")
      "whitespace between a field name and its colon is refused"
      Headers
      [Send (smuggle "Content-Length : 36\r\n")]
      (refused 400)
  , mk AHeaderFields (runSpec "header field names / refuses a control character in a field name")
      "a control character in a field name is refused"
      Headers
      [Send (smuggle "Content-Length\v: 36\r\n")]
      (refused 400)
  , mk AHeaderFields (runSpec "header field names / refuses an empty field name")
      "an empty field name is refused"
      Headers
      [Send (smuggle ": 36\r\n")]
      (refused 400)
  , mk AHeaderFields (runSpec "header field values / refuses a bare CR")
      "a bare CR in a field value is refused, so it cannot end a line"
      Headers
      [Send (smuggle "X-Foo: a\rContent-Length: 36\r\n")]
      (refused 400)
  , mk AHeaderFields (runSpec "header field values / refuses a NUL")
      "a NUL in a field value is refused"
      Headers
      [Send (smuggle "X-Foo: a\NULb\r\n")]
      (refused 400)
  , mk AHeaderFields (RFC "RFC 9112 5" "field-line = field-name \":\" OWS field-value OWS")
      "a header line with no colon is refused"
      Headers
      [Send (getWith ["NoSemiColon"])]
      (refused 400)
      & note "Warp reads such a line as a field with an empty value; fulmen answers 400 as its scope says."
  , mk AHeaderFields (RFC "RFC 9112 5.1" "the field value does not include leading or trailing whitespace")
      "whitespace around a field value is not part of the value"
      Headers
      [Send (getWith ["X-Spaced:   padded value \t "])]
      [stays [okHeaders "GET" "/" [hostLocal, ("X-Spaced", "padded value")]]]
      & note "Warp strips leading whitespace only; trailing whitespace stays in the value."
  , mk AHeaderFields (RFC "RFC 9112 5" "a field value may be empty")
      "a header with an empty value is passed on empty"
      Headers
      [Send (getWith ["X-Empty:"])]
      [stays [okHeaders "GET" "/" [hostLocal, ("X-Empty", "")]]]
  , mk AHeaderFields (RFC "RFC 9110 5.3" "repeated field lines are kept as separate lines")
      "a repeated header is listed once per field line, in order"
      Headers
      [Send (getWith ["X-A: 1", "X-B: x", "X-A: 2"])]
      [stays [okHeaders "GET" "/" [hostLocal, ("X-A", "1"), ("X-B", "x"), ("X-A", "2")]]]
  , mk AHeaderFields (RFC "RFC 9110 5.1" "field names are case-insensitive")
      "header names are matched without regard to case"
      Headers
      [Send "GET / HTTP/1.1\r\nHOST: localhost\r\nX-MiXeD: Value\r\n\r\n"]
      [stays [okHeaders "GET" "/" [("HOST", "localhost"), ("X-MiXeD", "Value")]]]
  ]

----------------------------------------------------------------------
-- The head arriving in pieces

headSplitCases :: [Case]
headSplitCases =
  [ mk AHeadSplits (Warp "warp/test/RequestSpec.hs" ("headerLines / " ++ source)) ("the head " ++ name) Headers steps [stays [expected]]
  | (source, name, pieces) <- splits
  , let steps = intersperse (PauseMs 20) (map Send pieces)
  ]
    ++ [ mk AHeadSplits (Warp "warp/test/RequestSpec.hs" "headerLines / can handle a stupid case (3)")
          "the head arrives one byte at a time"
          Headers
          (bytewise headText)
          [stays [expected]]
       ]
    ++ [ mk AHeadSplits (runSpec "connection termination / IncompleteHeaders")
          ("a head cut off after " ++ what ++ ", then end of input, gets no 200")
          Headers
          [Send partial, PauseMs 50, HalfClose]
          givesUp
          & note "Warp raises IncompleteHeaders and, the client having gone, nothing is read; a 400 is acceptable to a client that only half-closed."
       | (what, partial) <-
            [ ("a header line and the CR of the blank line", "GET / HTTP/1.1\r\ncontent-length: 10\r\n\r")
            , ("a header line and its CRLF", "GET / HTTP/1.1\r\ncontent-length: 10\r\n")
            , ("a header line and its CR", "GET / HTTP/1.1\r\ncontent-length: 10\r")
            , ("part of a header name", "GET / HTTP/1.1\r\ncontent-lengt")
            ]
       ]
    ++ [ mk AHeadSplits (Warp "warp/test/RequestSpec.hs" "headerLines / random chunking")
          "the head arrives in irregular pieces of one to nine bytes"
          Headers
          (intersperse (PauseMs 5) (map Send (irregular [3, 1, 4, 1, 5, 9, 2, 6, 5] headText)))
          [stays [expected]]
       , mk AHeadSplits (Warp "warp/test/RequestSpec.hs" "headerLines / known bad chunking behavior #239")
          "a head cut between the CR and LF of its last field line, with Connection: Close"
          Headers
          [Send "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Close\r", PauseMs 20, Send "\n\r\n"]
          [ closes
              [ okHeaders "GET" "/" [hostLocal, ("Connection", "Close")]
                  `withHeaders` [headerIs "Connection" "close"]
              ]
          ]
          & note "Warp's test has no Host header; one is added."
       ]
  where
    headText = "GET / HTTP/1.1\r\nHost: localhost\r\nStatus: 200\r\nContent-Type: text/plain\r\n\r\n"
    expected = okHeaders "GET" "/" [hostLocal, ("Status", "200"), ("Content-Type", "text/plain")]
    pre = "GET / HTTP/1.1\r\nHost: localhost\r\nStatus: 200"
    splits =
      [ ("a normal case", "arrives in one write", [headText])
      , ("nasty case (1)", "is cut inside a value and after a CRLF", [pre, "\r\nContent-Type: text/plain", "\r\n\r\n"])
      , ("nasty case (1)", "is cut right after a CRLF", [pre <> "\r\n", "Content-Type: text/plain", "\r\n\r\n"])
      , ("nasty case (2)", "has every CR and LF in a write of its own at the line ends", [pre, "\r", "\nContent-Type: text/plain", "\r", "\n\r\n"])
      , ("nasty case (3)", "has every CR and LF in a write of its own, the blank line too", [pre, "\r", "\n", "Content-Type: text/plain", "\r", "\n", "\r", "\n"])
      ]
    irregular sizes = go (cycle sizes)
      where
        go (n : ns) x
          | B.null x = []
          | otherwise = B.take n x : go ns (B.drop n x)
        go [] _ = []

----------------------------------------------------------------------
-- Host

hostCases :: [Case]
hostCases =
  [ mk AHost (runSpec "Host / refuses a HTTP/1.1 request without one")
      "an HTTP/1.1 request without Host is refused"
      Headers
      [Send "GET /nohost HTTP/1.1\r\n\r\n"]
      (refused 400)
  , mk AHost (runSpec "Host / refuses more than one")
      "more than one Host with different values is refused"
      Headers
      [Send "GET /dup HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n"]
      (refused 400)
  , mk AHost (RFC "RFC 9112 3.2" "more than one Host field line")
      "more than one Host with the same value is refused"
      Headers
      [Send "GET /dup HTTP/1.1\r\nHost: a\r\nHost: a\r\n\r\n"]
      (refused 400)
  , mk AHost (runSpec "Host / serves a HTTP/1.0 request without one")
      "an HTTP/1.0 request without Host is served"
      Headers
      [Send "GET /nohost HTTP/1.0\r\n\r\n"]
      [closes [withVersion "HTTP/1.0" (okHeaders "GET" "/nohost" [])]]
  , mk AHost (RFC "RFC 9112 3.2" "a Host field with an invalid field value")
      "a Host with spaces inside is refused"
      Headers
      [Send "GET / HTTP/1.1\r\nHost: not a host\r\n\r\n"]
      (refused 400)
      & note "Warp does not validate the value."
  , mk AHost (RFC "RFC 9110 5.1" "the field name is case-insensitive")
      "host is recognised in any case"
      Headers
      [Send "GET / HTTP/1.1\r\nhOsT: localhost\r\n\r\n"]
      [stays [okHeaders "GET" "/" [("hOsT", "localhost")]]]
  ]

----------------------------------------------------------------------
-- Head limits: 414 and 431

-- | A line limit small enough to cross in a test.
smallLine :: Settings
smallLine = noSettings {lineLimit = Just 1024}

headLimitCases :: [Case]
headLimitCases =
  [ mk AHeadLimits (RFC "RFC 9110 15.5.15" "414 URI Too Long")
      "a request line longer than the buffer is answered 414"
      Headers
      [Send ("GET /" <> B.replicate 2000 97 <> " HTTP/1.1\r\nHost: localhost\r\n\r\n")]
      (refused 414)
      & withSettings smallLine
  , mk AHeadLimits (Warp "warp/test/RequestSpec.hs" "headerLines / throws OverLargeHeader when too large")
      "a request line longer than the buffer is answered 414 before its end arrives"
      Headers
      [Send ("GET /" <> B.replicate 2000 97)]
      (refused 414)
      & withSettings smallLine
      & note "Warp's test feeds an endless line with no newline; Warp answers 400. fulmen answers 414 as soon as the buffer is full, without waiting for a line end."
  , mk AHeadLimits (Spec "a line that fits the buffer is accepted")
      "a request line that fits the buffer is served"
      Headers
      [Send (get ("/" <> B.replicate 900 97))]
      [stays [okHeaders "GET" ("/" <> B.replicate 900 97) [hostLocal]]]
      & withSettings smallLine
  , mk AHeadLimits (RFC "RFC 6585 5" "431 Request Header Fields Too Large")
      "a header line longer than the buffer is answered 431"
      Headers
      [Send (getWith ["X-Long: " <> B.replicate 2000 97])]
      (refused 431)
      & withSettings smallLine
  , mk AHeadLimits (Warp "warp/test/RequestSpec.hs" "headerLines / throws OverLargeHeader when too large")
      "a header line longer than the buffer is answered 431 before its end arrives"
      Headers
      [Send ("GET / HTTP/1.1\r\nHost: localhost\r\nX-Long: " <> B.replicate 2000 97)]
      (refused 431)
      & withSettings smallLine
      & note "Warp answers 400."
  , mk AHeadLimits (Spec "a line that fits the buffer is accepted")
      "a header line that fits the buffer is served"
      Headers
      [Send (getWith ["X-Long: " <> B.replicate 900 97])]
      [stays [okHeaders "GET" "/" [hostLocal, ("X-Long", B.replicate 900 97)]]]
      & withSettings smallLine
  , mk AHeadLimits (Spec "the default buffer is 16 KiB")
      "with the default buffer a header line of 20000 bytes is answered 431"
      Headers
      [Send (getWith ["X-Long: " <> B.replicate 20000 97])]
      (refused 431)
  , mk AHeadLimits (Spec "the default buffer is 16 KiB")
      "with the default buffer a request line of 20000 bytes is answered 414"
      Headers
      [Send (get ("/" <> B.replicate 20000 97))]
      (refused 414)
  , mk AHeadLimits (Warp "warp/streams-test/AnswerSpec.hs" "a client that has gone / serves a request that arrives during the answer after the answer (X-Long of 5000 bytes)")
      "with the default buffer a header line of 5000 bytes is served"
      Headers
      [Send (getWith ["X-Long: " <> B.replicate 5000 97])]
      [stays [okHeaders "GET" "/" [hostLocal, ("X-Long", B.replicate 5000 97)]]]
  , mk AHeadLimits (Warp "warp/test/RequestSpec.hs" "headerLines / throws OverLargeHeader when too many")
      "a head of many short lines over the head limit is answered 431"
      Headers
      [Send ("GET / HTTP/1.1\r\nHost: localhost\r\n" <> B.concat (replicate 600 "X-N: v\r\n") <> "\r\n")]
      (refused 431)
      & withSettings noSettings {headLimit = Just 2048}
      & note "Warp's test feeds endless short lines; Warp answers 400. The status for the head as a whole is 431 as for one line."
  , mk AHeadLimits (Spec "each line fits the buffer, only the whole head is over its limit")
      "a head of lines that each fit, over the head limit, is answered 431"
      Headers
      [Send (getWith (replicate 8 ("X-Big: " <> B.replicate 400 98)))]
      (refused 431)
      & withSettings noSettings {lineLimit = Just 1024, headLimit = Just 2048}
  , mk AHeadLimits (Spec "a head under its limit is accepted")
      "a head of several lines within the head limit is served"
      Headers
      [Send (getWith (replicate 3 ("X-Big: " <> B.replicate 400 98)))]
      [stays [okHeaders "GET" "/" (hostLocal : replicate 3 ("X-Big", B.replicate 400 98))]]
      & withSettings noSettings {lineLimit = Just 1024, headLimit = Just 2048}
  , mk AHeadLimits (Spec "the limits are settings, whatever the size of the buffer")
      "with the default settings a header line of 40000 bytes is served"
      Headers
      [Send (getWith ["X-Long: " <> B.replicate 40000 97])]
      [stays [okHeaders "GET" "/" [hostLocal, ("X-Long", B.replicate 40000 97)]]]
      & note "PostgREST under Warp took a Cookie of this size (its io suite sends one); Warp's limit is 50 KiB for the whole head."
  , mk AHeadLimits (Spec "the limits are settings, whatever the size of the buffer")
      "with the default settings a head of 100 lines of 400 bytes, which is more than the buffer, is served"
      Headers
      [Send (getWith (replicate 100 ("X-Big: " <> B.replicate 400 98)))]
      [stays [okHeaders "GET" "/" (hostLocal : replicate 100 ("X-Big", B.replicate 400 98))]]
  , mk AHeadLimits (Spec "the limits are settings, whatever the size of the buffer")
      "a head that is more than the buffer, whose lines are cut at the end of the buffer where a CR and a LF are, is served"
      Headers
      [Send (getWith ["X-Pad: " <> B.replicate 16343 97, "X-Next: 1"])]
      [stays [okHeaders "GET" "/" [hostLocal, ("X-Pad", B.replicate 16343 97), ("X-Next", "1")]]]
      & note "The first 16384 bytes of the head are the request line, Host and X-Pad's name and value, and the CR of its line end; the LF is the first byte after."
  , mk AHeadLimits (Spec "the limits are settings, whatever the size of the buffer")
      "a request with a header line of 40000 bytes and a body is answered with the body"
      Echo
      [Send ("POST / HTTP/1.1\r\nHost: localhost\r\nX-Long: " <> B.replicate 40000 97 <> "\r\nContent-Length: 5\r\n\r\nhello")]
      [stays [ok "hello"]]
  , mk AHeadLimits (Spec "the head limit counts the whole head, however many buffers it is")
      "with the default settings a head of 200 lines of 400 bytes is answered 431"
      Headers
      [Send (getWith (replicate 200 ("X-Big: " <> B.replicate 400 98)))]
      (refused 431)
  , mk AHeadLimits (Spec "the line limit is not the buffer's")
      "with the default settings a header line of 60000 bytes is answered 431"
      Headers
      [Send (getWith ["X-Long: " <> B.replicate 60000 97])]
      (refused 431)
  , mk AHeadLimits (Spec "the line limit is not the buffer's")
      "with the default settings a request line of 60000 bytes is answered 414"
      Headers
      [Send (get ("/" <> B.replicate 60000 97))]
      (refused 414)
  , mk AHeadLimits (Spec "the bytes after a head that is more than the buffer belong to the next request")
      "a head that is more than the buffer is followed by the request that came with it"
      Ignore
      [Send (getWith ["X-Long: " <> B.replicate 40000 97] <> singleGet)]
      [stays [ok ignoreBody, ok ignoreBody]]
  , mk AHeadLimits (Spec "after a framing error the connection is closed")
      "a request that follows an oversized head on the connection is not answered"
      Headers
      [Send (getWith ["X-Long: " <> B.replicate 2000 97] <> get "/next")]
      (refused 431)
      & withSettings smallLine
  ]

----------------------------------------------------------------------
-- Body limit: 413

limit10 :: Settings
limit10 = noSettings {bodyLimit = Just 10}

bodyLimitCases :: [Case]
bodyLimitCases =
  [ mk ABodyLimit (RFC "RFC 9110 15.5.14" "413 Content Too Large")
      "a Content-Length over the limit is answered 413 before any body is sent"
      Echo
      [Send (postHead 1000)]
      (refused 413)
      & withSettings limit10
  , mk ABodyLimit (RFC "RFC 9110 15.5.14" "413 Content Too Large")
      "a Content-Length one byte over the limit is answered 413 with the body sent"
      Echo
      [Send (postBody "01234567890")]
      (refused 413)
      & withSettings limit10
  , mk ABodyLimit (Spec "a body at the limit is accepted")
      "a Content-Length body exactly at the limit is echoed"
      Echo
      [Send (postBody "0123456789")]
      [stays [ok "0123456789"]]
      & withSettings limit10
  , mk ABodyLimit (RFC "RFC 9110 15.5.14" "413 Content Too Large")
      "a chunked body over the limit is answered 413"
      Echo
      [Send (chunkedPost ["012345", "678901"])]
      (refused 413)
      & withSettings limit10
      & note "The limit is only known once the second chunk has been read; the 413 must be the first byte of the answer."
  , mk ABodyLimit (Spec "a body at the limit is accepted")
      "a chunked body exactly at the limit is echoed"
      Echo
      [Send (chunkedPost ["01234", "56789"])]
      [stays [ok "0123456789"]]
      & withSettings limit10
  , mk ABodyLimit (RFC "RFC 9110 15.5.14" "413 Content Too Large")
      "a declared length over the limit is refused even if the handler would not read the body"
      Ignore
      [Send (postHead 1000)]
      (refused 413)
      & withSettings limit10
  ]

----------------------------------------------------------------------
-- Content-Length bodies

contentLengthCases :: [Case]
contentLengthCases =
  [ mk AContentLength (runSpec "no hanging / has body, read")
      "a body that arrives one byte at a time is read"
      Echo
      (bytewise singlePostHello)
      [stays [ok "Hello"]]
  , mk AContentLength (Spec "a body arriving after the head")
      "a body in a write of its own, after the head, is read"
      Echo
      [Send (postHead 5), PauseMs 50, Send "Hello"]
      [stays [ok "Hello"]]
  , mk AContentLength (Spec "a body arriving with the head")
      "a body in the same write as the head is read"
      Echo
      [Send (postBody "Hello")]
      [stays [ok "Hello"]]
  , mk AContentLength (Spec "a body arriving in pieces")
      "a body in three writes with pauses between is read"
      Echo
      [Send (postHead 30), PauseMs 30, Send (filler 10), PauseMs 30, Send (B.drop 10 (filler 20)), PauseMs 30, Send (B.drop 20 (filler 30))]
      [stays [ok (filler 30)]]
  , mk AContentLength (Spec "a body larger than the socket buffers")
      "a body of 1 MiB is echoed in full"
      Echo
      [Send (postBody (filler 1048576))]
      [stays [ok (filler 1048576) `withHeaders` [headerIs "Content-Length" "1048576"]]]
  , mk AContentLength (Spec "an empty body")
      "a Content-Length of 0 gives an empty body"
      Echo
      [Send (postHead 0)]
      [stays [ok "" `withHeaders` [headerIs "Content-Length" "0"]]]
  , mk AContentLength (RFC "RFC 9110 8.6" "Content-Length = 1*DIGIT, leading zeros included")
      "a Content-Length with a leading zero is read as the number"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 05\r\n\r\nHello"]
      [stays [ok "Hello"]]
  , mk AContentLength (RFC "RFC 9110 5.1" "field names are case-insensitive")
      "content-length is recognised in lower case and in capitals"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: localhost\r\ncontent-length: 5\r\n\r\nHello", Send "POST / HTTP/1.1\r\nHost: localhost\r\nCONTENT-LENGTH: 3\r\n\r\nabc"]
      [stays [ok "Hello", ok "abc"]]
  , mk AContentLength (RFC "RFC 9110 6.4.1" "a request may carry content whatever its method")
      "a GET with a body has the body read"
      Echo
      [Send "GET / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nHello"]
      [stays [ok "Hello"]]
  , mk AContentLength (runSpec "Content-Length framing / accepts Content-Lengths that agree")
      "two Content-Length lines that agree (5 and 05) are accepted"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: 5\r\nContent-Length: 05\r\n\r\nHello"]
      [stays [ok "Hello"]]
  , mk AContentLength (RFC "RFC 9110 8.6" "identical Content-Length values may be repeated")
      "two identical Content-Length lines are accepted"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nHello"]
      [stays [ok "Hello"]]
  , mk AContentLength (RFC "RFC 9110 8.6" "a list of identical Content-Length values")
      "a Content-Length list of identical values (5, 5) is accepted"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: 5, 5\r\n\r\nHello"]
      [stays [ok "Hello"]]
  , mk AContentLength (runSpec "Content-Length framing / refuses Content-Lengths that disagree")
      "two Content-Length lines that disagree are refused"
      Echo
      [Send (smuggle "Content-Length: 0\r\nContent-Length: 36\r\n")]
      (refused 400)
  , mk AContentLength (RFC "RFC 9110 8.6" "a list of differing Content-Length values")
      "a Content-Length list of different values (5, 6) is refused"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: 5, 6\r\n\r\nHello!"]
      (refused 400)
  , mk AContentLength (runSpec "Content-Length framing / refuses a Content-Length that is not a number")
      "a Content-Length that is not a number is refused"
      Echo
      [Send (smuggle "Content-Length: abc\r\n")]
      (refused 400)
  , mk AContentLength (runSpec "Content-Length framing / refuses a Content-Length that overflows")
      "a Content-Length that does not fit in 63 bits is refused"
      Echo
      [Send (smuggle "Content-Length: 9223372036854775808\r\n")]
      (refused 400)
  , mk AContentLength (RFC "RFC 9110 8.6" "an invalid Content-Length makes the framing invalid")
      "a negative Content-Length is refused"
      Echo
      [Send (smuggle "Content-Length: -5\r\n")]
      (refused 400)
  , mk AContentLength (RFC "RFC 9110 8.6" "an invalid Content-Length makes the framing invalid")
      "a Content-Length with a plus sign is refused"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: +5\r\n\r\nHello"]
      (refused 400)
  , mk AContentLength (RFC "RFC 9110 8.6" "an invalid Content-Length makes the framing invalid")
      "a hexadecimal Content-Length is refused"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: 0x5\r\n\r\nHello"]
      (refused 400)
  , mk AContentLength (RFC "RFC 9110 8.6" "an invalid Content-Length makes the framing invalid")
      "an empty Content-Length is refused"
      Echo
      [Send (smuggle "Content-Length:\r\n")]
      (refused 400)
  , mk AContentLength (Warp "warp/test/ConduitSpec.hs" "a body decoded from pieces / refuses a known length that is cut short, however the bytes arrive")
      "a body shorter than its Content-Length, then end of input, gets no 200"
      Echo
      [Send (postHead 10 <> "hello"), PauseMs 50, HalfClose]
      givesUp
      & note "Warp raises ConnectionClosedByPeer and sends nothing; an error response is also acceptable. A 200 with the short body is the failure (yesodweb/wai#1129 is the chunked form)."
  ]

----------------------------------------------------------------------
-- Chunked request bodies

-- | A chunked POST whose body is this raw text, in one write; the case
-- expects a refusal.
chunkRefused :: String -> Origin -> ByteString -> Case
chunkRefused name origin body =
  mk AChunked origin name Echo [Send (chunkedHead <> body)] (refused 400)

-- | A chunked POST whose body is this raw text, in one write; the case
-- expects the echo of the decoded body.
chunkAccepted :: String -> Origin -> ByteString -> ByteString -> Case
chunkAccepted name origin body echoed =
  mk AChunked origin name Echo [Send (chunkedHead <> body)] [stays [ok echoed]]

chunkedCases :: [Case]
chunkedCases =
  [ mk AChunked (runSpec "chunked bodies / works")
      "two chunked requests back to back are decoded (Transfer-Encoding: Chunked)"
      Echo
      [ Send
          ( "POST / HTTP/1.1\r\nHost: foo\r\nTransfer-Encoding: Chunked\r\n\r\n"
              <> "c\r\nHello World\n\r\n3\r\nBye\r\n0\r\n\r\n"
              <> "POST / HTTP/1.1\r\nHost: foo\r\nTransfer-Encoding: Chunked\r\n\r\n"
              <> "b\r\nHello World\r\n0\r\n\r\n"
          )
      ]
      [stays [ok "Hello World\nBye", ok "Hello World"]]
  , mk AChunked (runSpec "chunked bodies / lots of chunks")
      "fifty chunks, each in a write of its own, in two requests"
      Echo
      ( concat
          ( replicate
              2
              ( [Send "POST / HTTP/1.1\r\nHost: foo\r\nTransfer-Encoding: Chunked\r\n\r\n"]
                  ++ replicate 50 (Send "5\r\n12345\r\n")
                  ++ [Send "0\r\n\r\n"]
              )
          )
      )
      [stays (replicate 2 (ok (B.concat (replicate 50 "12345"))))]
  , mk AChunked (runSpec "chunked bodies / in chunks")
      "two chunked requests sent one byte at a time are decoded"
      Echo
      ( bytewise
          ( "POST / HTTP/1.1\r\nHost: foo\r\nTransfer-Encoding: Chunked\r\n\r\n"
              <> "c\r\nHello World\n\r\n3\r\nBye\r\n0\r\n\r\n"
              <> "POST / HTTP/1.1\r\nHost: foo\r\nTransfer-Encoding: Chunked\r\n\r\n"
              <> "b\r\nHello World\r\n0\r\n\r\n"
          )
      )
      [stays [ok "Hello World\nBye", ok "Hello World"]]
  , mk AChunked (runSpec "malformed chunked bodies / reads a chunk size split over several reads")
      "a chunk size of two digits split over several writes is read"
      Echo
      ( Send "POST / HTTP/1.1\r\nHost: foo\r\nTransfer-Encoding: chunked\r\n\r\n"
          : concat [[PauseMs 50, Send p] | p <- ["1", "2", "\r\n", B.replicate 0x12 65, "\r\n0\r\n\r\n"]]
      )
      [stays [ok (B.replicate 0x12 65)]]
  , mk AChunked (Warp "warp/test/ConduitSpec.hs" "chunk boundaries")
      "chunk framing cut at the CR and LF of the chunk lines is decoded"
      Echo
      [Send chunkedHead, PauseMs 20, Send "5\r\n", PauseMs 20, Send "12345\r\n3\r", PauseMs 20, Send "\n678\r\n0\r\n", PauseMs 20, Send "\r\n"]
      [stays [ok "12345678"]]
  , mk AChunked (Warp "warp/test/ConduitSpec.hs" "chunkedSource")
      "a chunked body ends at its last chunk, and the bytes after it are the next request"
      Echo
      [Send (chunkedPost ["12345", "678"] <> singleGet)]
      [stays [ok "12345678", ok ""]]
  , mk AChunked (Warp "warp/test/ConduitSpec.hs" "a body decoded from pieces (sizes in upper or lower case, with leading zeros)")
      "chunk sizes in capitals and with leading zeros are read"
      Echo
      [Send (chunkedHead <> "A\r\n0123456789\r\n000a\r\nabcdefghij\r\n0000\r\n\r\n")]
      [stays [ok "0123456789abcdefghij"]]
  , mk AChunked (Spec "the empty chunked body")
      "a chunked body with no chunk is an empty body"
      Echo
      [Send (chunkedHead <> "0\r\n\r\n")]
      [stays [ok "" `withHeaders` [headerIs "Content-Length" "0"]]]
  , mk AChunked (RFC "RFC 9112 6.1" "transfer coding names are case-insensitive")
      "Transfer-Encoding: CHUNKED is read as chunked"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nTRANSFER-ENCODING: CHUNKED\r\n\r\n5\r\nHello\r\n0\r\n\r\n"]
      [stays [ok "Hello"]]
  , mk AChunked (Spec "the response to a chunked request is framed by the handler")
      "the echo of a chunked body states its length"
      Echo
      [Send (chunkedPost ["Hello", " World"])]
      [stays [ok "Hello World" `withHeaders` [headerIs "Content-Length" "11"]]]
  , -- chunk extensions
    chunkAccepted
      "a chunk extension is accepted"
      (RFC "RFC 9112 7.1.1" "chunk-ext = *( BWS \";\" BWS chunk-ext-name [ BWS \"=\" BWS chunk-ext-val ] )")
      "5;name=value\r\nHello\r\n0\r\n\r\n"
      "Hello"
  , chunkAccepted
      "a chunk extension without a value is accepted"
      (RFC "RFC 9112 7.1.1" "chunk-ext-val is optional")
      "5;flag\r\nHello\r\n0\r\n\r\n"
      "Hello"
  , chunkAccepted
      "a chunk extension with a quoted value holding a semicolon is accepted"
      (RFC "RFC 9112 7.1.1" "chunk-ext-val = token / quoted-string")
      "5;note=\"a;b c\"\r\nHello\r\n0\r\n\r\n"
      "Hello"
  , chunkAccepted
      "several chunk extensions on one chunk are accepted"
      (RFC "RFC 9112 7.1.1" "chunk-ext may repeat")
      "5;a=1;b=2;c\r\nHello\r\n0\r\n\r\n"
      "Hello"
  , chunkAccepted
      "a chunk extension on the last chunk is accepted"
      (RFC "RFC 9112 7.1.1" "last-chunk = 1*(\"0\") [ chunk-ext ] CRLF")
      "5\r\nHello\r\n0;done=1\r\n\r\n"
      "Hello"
  , mk AChunked (RFC "RFC 9112 7.1.1" "BWS is allowed before the semicolon of a chunk extension")
      "a chunk extension after whitespace is accepted or refused"
      Echo
      [Send (chunkedHead <> "5 ;a=b\r\nHello\r\n0\r\n\r\n")]
      [stays [ok "Hello"], closes [resp 400]]
      & note "RFC grammar allows it; Warp refuses the space (400). Both are accepted here."
  , -- trailers
    mk AChunked (RFC "RFC 9112 7.1.2" "chunked trailer section")
      "trailer fields are accepted, not echoed, and the next request is answered"
      Echo
      [Send (chunkedHead <> "5\r\nHello\r\n0\r\nX-Checksum: abc\r\nX-Other: z\r\n\r\n" <> singleGet)]
      [stays [ok "Hello", ok ""]]
  , mk AChunked (RFC "RFC 9112 7.1.2" "trailer fields after a last-chunk with an extension")
      "trailer fields after a last chunk with an extension are accepted"
      Echo
      [Send (chunkedHead <> "5\r\nHello\r\n0;x=1\r\nX-Checksum: abc\r\n\r\n" <> singleGet)]
      [stays [ok "Hello", ok ""]]
  , mk AChunked (RFC "RFC 9112 7.1.2" "trailer fields may arrive in later writes")
      "a trailer section that arrives in pieces is read to its end"
      Echo
      [Send (chunkedHead <> "5\r\nHello\r\n0\r\nX-Check"), PauseMs 50, Send "sum: abc\r", PauseMs 50, Send "\n\r", PauseMs 50, Send "\n", Send singleGet]
      [stays [ok "Hello", ok ""]]
  , mk AChunked (runSpec "malformed chunked bodies / reads the trailer section to its end")
      "the trailer section is read to its empty line, so a request line there is not a request"
      Echo
      [Send (chunkedHead <> "0\r\nX-Trailer: y\r\n" <> smuggled)]
      [stays [ok ""], closes [resp 400]]
      & note "Warp takes 'GET /smuggled HTTP/1.1' for a trailer line and answers 200 once. A server that refuses a trailer line with no colon answers 400. Either way /smuggled must not be answered."
  , -- malformed
    chunkRefused
      "a chunk size that is not a number is refused"
      (runSpec "malformed chunked bodies / refuses a chunk size that is not a number")
      smuggled
  , chunkRefused
      "a chunk not closed off by CRLF is refused"
      (runSpec "malformed chunked bodies / refuses a chunk that is not closed off by CRLF")
      ("5\r\nHello" <> smuggled)
  , chunkRefused
      "a chunk size that overflows is refused"
      (runSpec "malformed chunked bodies / refuses a chunk size that overflows")
      "ffffffffffffffffff\r\n"
  , chunkRefused
      "a chunk size line of text is refused"
      (Warp "warp/test/ConduitSpec.hs" "limitCases / a size line that is not a size")
      "xyz\r\nabc"
  , chunkRefused
      "a chunk extension with no size is refused"
      (Warp "warp/test/ConduitSpec.hs" "limitCases / an extension without a size")
      ";x\r\nabc"
  , chunkRefused
      "chunk data longer than its size is refused"
      (Warp "warp/test/ConduitSpec.hs" "limitCases / chunk data that is not closed off")
      "3\r\nabcXY\r\n0\r\n\r\n"
  , chunkRefused
      "a CR that is not part of a line end in a chunk size line is refused"
      (Warp "warp/test/ConduitSpec.hs" "limitCases / a CR that is not part of a line end")
      "3\r\r\nabc\r\n0\r\n\r\n"
  , chunkRefused
      "a negative chunk size is refused"
      (RFC "RFC 9112 7.1" "chunk-size = 1*HEXDIG")
      "-5\r\nHello\r\n0\r\n\r\n"
  , chunkRefused
      "a chunk size with a 0x prefix is refused"
      (RFC "RFC 9112 7.1" "chunk-size = 1*HEXDIG")
      "0x5\r\nHello\r\n0\r\n\r\n"
  , mk AChunked (runSpec "malformed chunked bodies / refuses a chunk cut short by the end of input")
      "a chunk cut short by the end of input gets no 200"
      Echo
      [Send (chunkedHead <> "64\r\n" <> B.replicate 10 65), PauseMs 50, HalfClose]
      givesUp
      & note "yesodweb/wai#1129: the handler must not be given the short body as if it were whole."
  , mk AChunked (Warp "warp/test/ConduitSpec.hs" "limitCases / a body that is cut in the middle of a size line")
      "a body cut in the middle of a chunk size line gets no 200"
      Echo
      [Send (chunkedHead <> "3\r\nabc\r\n1"), PauseMs 50, HalfClose]
      givesUp
  , mk AChunked (RFC "RFC 9112 7.1" "the last-chunk and trailer section end the body")
      "a body with no last chunk, then end of input, gets no 200"
      Echo
      [Send (chunkedHead <> "5\r\nHello\r\n"), PauseMs 50, HalfClose]
      givesUp
  , mk AChunked (Warp "warp/test/ConduitSpec.hs" "limitCases / a chunk extension that is too long")
      "a chunk extension longer than the line buffer is refused"
      Echo
      [Send (chunkedHead <> "5;" <> B.replicate 2000 120 <> "\r\nHello\r\n0\r\n\r\n")]
      refusedLine
      & withSettings smallLine
      & note "Warp's own limit for a chunk-size line is 1024 bytes and its error is 400; fulmen's limit is the line buffer, and either 400 or 431 is accepted."
  , mk AChunked (Warp "warp/test/ConduitSpec.hs" "limitCases / a trailer line that is too long")
      "a trailer line longer than the line buffer is refused"
      Echo
      [Send (chunkedHead <> "5\r\nHello\r\n0\r\nX-T: " <> B.replicate 2000 120 <> "\r\n\r\n")]
      refusedLine
      & withSettings smallLine
      & note "Warp's trailer limit is 8192 bytes in all and a line over 1024 bytes is refused (400); fulmen's is the line buffer."
  , mk AChunked (Warp "warp/test/ConduitSpec.hs" "limitCases / a size line that is a byte too long")
      "a chunk size line of many leading zeros longer than the line buffer is refused"
      Echo
      [Send (chunkedHead <> B.replicate 2000 48 <> "5\r\nHello\r\n0\r\n\r\n")]
      refusedLine
      & withSettings smallLine
  ]
  where
    refusedLine = refusedOneOf [400, 431]

----------------------------------------------------------------------
-- Ambiguous body framing

framingCases :: [Case]
framingCases =
  concat
    [ [ mk AFraming (runSpec ("ambiguous body framing / " ++ source))
          ("the coding " ++ name ++ " is read as chunked (smuggled request not answered)")
          Ignore
          [Send (smuggle ("Transfer-Encoding: " <> value <> "\r\n"))]
          [closes [ok ignoreBody]]
          & note "The handler does not read the body. Warp answers 200, then fails to drain the body, which is a request line and not a chunk size, and closes. The /smuggled request must not be answered."
      , mk AFraming (runSpec "ambiguous body framing / still reads an ordinary chunked body")
          ("the coding " ++ name ++ " frames a real chunked body")
          Echo
          [Send ("POST / HTTP/1.1\r\nHost: foo\r\nTransfer-Encoding: " <> value <> "\r\n\r\n5\r\nHello\r\n0\r\n\r\n")]
          [stays [ok "Hello"]]
      ]
    | (source, name, value) <-
        [ ("reads a trailing space as part of the chunked coding", "\"chunked \" (trailing space)", "chunked ")
        , ("reads a trailing tab as part of the chunked coding", "\"chunked\" followed by a tab", "chunked\t")
        , ("ignores an empty element after the chunked coding", "\"chunked,\" (empty element after)", "chunked,")
        , ("ignores an empty element before the chunked coding", "\", chunked\" (empty element before)", ", chunked")
        ]
    ]
    ++ [ mk AFraming (runSpec "ambiguous body framing / refuses a Transfer-Encoding with no coding at all")
          "a Transfer-Encoding with no coding at all is refused"
          Ignore
          [Send (smuggle "Transfer-Encoding: ,\r\n")]
          (refused 400)
       , mk AFraming (runSpec "ambiguous body framing / refuses a coding it cannot decode under chunked")
          "a coding the server cannot decode under chunked (gzip, chunked) is answered 501"
          Ignore
          [Send (smuggle "Transfer-Encoding: gzip, chunked\r\n")]
          (refused 501)
       , mk AFraming (RFC "RFC 9112 6.1" "a server that receives a transfer coding it does not understand SHOULD respond 501")
          "an unknown coding as the only coding is refused"
          Ignore
          [Send (smuggle "Transfer-Encoding: foo\r\n")]
          (refusedOneOf [400, 501])
          & note "RFC 9112 6.3 asks for 400 when chunked is not the final coding, and 6.1 for 501 for an unknown coding: both are accepted."
       , mk AFraming (runSpec "ambiguous body framing / refuses chunked that is not the final coding")
          "chunked that is not the final coding (chunked, gzip) is refused"
          Ignore
          [Send (smuggle "Transfer-Encoding: chunked, gzip\r\n")]
          (refused 400)
       , mk AFraming (runSpec "ambiguous body framing / refuses a transfer coding that is not chunked")
          "a transfer coding that is not chunked (identity) is refused"
          Ignore
          [Send (smuggle "Transfer-Encoding: identity\r\n")]
          (refused 400)
       , mk AFraming (runSpec "ambiguous body framing / joins Transfer-Encoding field lines before reading them")
          "two Transfer-Encoding lines are read as one list (chunked, identity) and refused"
          Ignore
          [Send (smuggle "Transfer-Encoding: chunked\r\nTransfer-Encoding: identity\r\n")]
          (refused 400)
       , mk AFraming (runSpec "ambiguous body framing / refuses Content-Length together with Transfer-Encoding")
          "Content-Length together with Transfer-Encoding is refused"
          Ignore
          [Send (smuggle "Content-Length: 0\r\nTransfer-Encoding: chunked\r\n")]
          (refused 400)
       , mk AFraming (RFC "RFC 9112 6.1" "a server MAY reject Content-Length with Transfer-Encoding, or use the latter, and MUST close the connection afterwards")
          "Content-Length together with a real chunked body is refused, or read as chunked and the connection closed"
          Echo
          [Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n0\r\n\r\n"]
          [closes [resp 400], closes [ok "Hello"]]
          & note "Warp refuses (400). RFC allows processing it as chunked, then closing."
       , mk AFraming (RFC "RFC 9112 6.1" "Transfer-Encoding and Content-Length order does not matter")
          "Transfer-Encoding before Content-Length is refused as well"
          Ignore
          [Send (smuggle "Transfer-Encoding: chunked\r\nContent-Length: 0\r\n")]
          (refused 400)
       ]

----------------------------------------------------------------------
-- Expect: 100-continue

continue100 :: ByteString
continue100 = "HTTP/1.1 100 Continue\r\n\r\n"

interim100 :: ResponseExpect
interim100 = resp 100

expectCases :: [Case]
expectCases =
  [ mk AExpect (Warp "warp/streams-test/AnswerSpec.hs" "the request body / gets its 100 Continue when the application pulls it")
      "a client that waits for 100 Continue gets it when the handler reads the body"
      Echo
      [ Send "POST / HTTP/1.1\r\nHost: foo\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\n"
      , AwaitReceived continue100
      , Send "abc"
      ]
      [stays [interim100, ok "abc"]]
      & note "Warp's own handleExpect (Request.hs) sends the 100 when the body is first read; no test in warp/test covers it."
  , mk AExpect (RFC "RFC 9110 10.1.1" "100-continue with a chunked body")
      "a client that waits for 100 Continue sends a chunked body after it"
      Echo
      [ Send "POST / HTTP/1.1\r\nHost: foo\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\n\r\n"
      , AwaitReceived continue100
      , Send "5\r\nHello\r\n0\r\n\r\n"
      ]
      [stays [interim100, ok "Hello"]]
  , mk AExpect (RFC "RFC 9110 10.1.1" "the expectation is case-insensitive")
      "Expect: 100-Continue in other case gets 100 Continue as well"
      Echo
      [ Send "POST / HTTP/1.1\r\nHost: foo\r\nExpect: 100-Continue\r\nContent-Length: 3\r\n\r\n"
      , AwaitReceived continue100
      , Send "abc"
      ]
      [stays [interim100, ok "abc"]]
      & note "Warp compares the value case-sensitively, so it never sends the 100 here and the client would wait out its patience."
  , mk AExpect (RFC "RFC 9110 10.1.1" "a server MAY omit 100 if it has already received some or all of the content")
      "a client that sends the body without waiting gets the answer, with or without 100 Continue"
      Echo
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\nabc"]
      [stays [interim100, ok "abc"], stays [ok "abc"]]
  , mk AExpect (RFC "RFC 9110 10.1.1" "a server that answers without reading the content ought to say whether it will close")
      "a handler that ignores the body of an expecting request does not leave the connection in limbo"
      Ignore
      [Send "POST / HTTP/1.1\r\nHost: foo\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n", PauseMs 300, Send "hello", PauseMs 50, Send singleGet]
      [ stays [interim100, ok ignoreBody, ok ignoreBody]
      , stays [ok ignoreBody, ok ignoreBody]
      , closes [ok ignoreBody `withHeaders` [headerIs "Connection" "close"]]
      ]
      & note "Three ways are fine: ask for the body with 100 and drain it, answer and drain what arrives later, or answer with Connection: close and close. The bytes 'hello' must never be read as a request."
  , mk AExpect (RFC "RFC 9110 10.1.1" "a server MUST ignore a 100-continue expectation in an HTTP/1.0 request")
      "an HTTP/1.0 request with Expect: 100-continue and its body is answered"
      Echo
      [Send "POST / HTTP/1.0\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\nabc"]
      [ closes [withVersion "HTTP/1.0" (ok "abc")]
      , closes [withVersion "HTTP/1.0" (resp 100), withVersion "HTTP/1.0" (ok "abc")]
      ]
      & note "RFC says ignore the expectation, so no 100. Warp answers an HTTP/1.0 request's expectation with 'HTTP/1.0 100 Continue'; both are accepted since the task says HTTP/1.0 is answered as Warp does."
  ]

----------------------------------------------------------------------
-- Unread request bodies

unreadBodyCases :: [Case]
unreadBodyCases =
  [ mk AUnreadBody (runSpec "pipelining / has body, ignore")
      "a small Content-Length body the handler ignores is drained and the next request answered"
      Ignore
      [Send (postBody (filler 1000) <> singleGet)]
      [stays [ok ignoreBody, ok ignoreBody]]
  , mk AUnreadBody (runSpec "pipelining / chunked body, ignore")
      "a small chunked body the handler ignores is drained and the next request answered"
      Ignore
      [Send (chunkedPost [filler 500, filler 500] <> singleGet)]
      [stays [ok ignoreBody, ok ignoreBody]]
  , mk AUnreadBody (Warp "warp/streams-test/AnswerSpec.hs" "the request body / is drained when the application leaves it unread, so the next request is served")
      "an ignored body of three bytes is not read as the start of the next request"
      Ignore
      [Send "POST /one HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\n\r\nabcGET /two HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"]
      [closes [ok ignoreBody, ok ignoreBody]]
  , mk AUnreadBody (Warp "warp/streams-test/AnswerSpec.hs" "the request body / keeps the bytes of an unread body that arrive while the answer waits, for the drain")
      "the body of an ignored request arriving after the answer is drained"
      Ignore
      [Send (postHead 10), PauseMs 100, Send "0123456789", PauseMs 50, Send singleGet]
      [stays [ok ignoreBody, ok ignoreBody]]
  , mk AUnreadBody (Spec "the handler may ignore a large body; the server drains or closes as HTTP requires")
      "a large ignored body is drained, or the connection closed after the answer"
      Ignore
      [Send (postBody (filler 100000) <> singleGet)]
      [stays [ok ignoreBody, ok ignoreBody], closes [ok ignoreBody]]
      & note "Warp drains at most 8192 bytes of an ignored body and otherwise closes after the answer. A close with unread data can reset the connection and cost the client the answer; the runner treats a reset after the first response as a close, and the response is required only if it arrived."
  , mk AUnreadBody (Spec "the handler answers without waiting for the body")
      "a handler that ignores the body answers before the body arrives"
      Ignore
      [Send (postHead 100000)]
      [anyConn [ok ignoreBody]]
      & note "Nothing of the body is sent. Whether the server then waits to drain or closes is not checked."
  ]

----------------------------------------------------------------------
-- Response framing

responseFramingCases :: [Case]
responseFramingCases =
  [ mk AResponseFraming (Spec "a declared Content-Length")
      "a handler's Content-Length is the response's length"
      (Fixed 200 [("Content-Type", "text/plain"), ("Content-Length", "5")] "hello")
      [Send singleGet]
      [ stays
          [ ok "hello"
              `withHeaders` [headerIs "Content-Length" "5", headerIs "Content-Type" "text/plain", HeaderAbsent "Transfer-Encoding", HeaderAbsent "Connection"]
          ]
      ]
  , mk AResponseFraming (runSpec "streaming response with length")
      "a response with a declared length of 20 bytes is not chunked"
      (Fixed 200 [("Content-Length", "20")] "HelloHelloHelloHello")
      [Send singleGet]
      [stays [ok "HelloHelloHelloHello" `withHeaders` [headerIs "Content-Length" "20", HeaderAbsent "Transfer-Encoding"]]]
      & note "Warp's test writes the body in four streamed writes; on the wire it is the same response."
  , mk AResponseFraming (Warp "warp/streams-test/AnswerSpec.hs" "the header and the body / frames an answer without a length as chunks, and serves the next request after it")
      "a body of unknown length is chunked, and the next response follows it"
      (Streamed ["abc", B.replicate 4096 120])
      [Send (singleGet <> singleGet)]
      [ stays
          (replicate 2 (ok ("abc" <> B.replicate 4096 120) `withHeaders` [headerIs "Transfer-Encoding" "chunked", HeaderAbsent "Content-Length"]))
      ]
  , mk AResponseFraming (Spec "a streamed body with no chunks")
      "a streamed body with no chunk is an empty chunked body"
      (Streamed [])
      [Send (singleGet <> singleGet)]
      [stays (replicate 2 (ok "" `withHeaders` [headerIs "Transfer-Encoding" "chunked", HeaderAbsent "Content-Length"]))]
  , mk AResponseFraming (Warp "warp/test/RunnerConnectionSpec.hs" "contract / connSendMany / sends the pieces in order, whatever their sizes (empty pieces)")
      "an empty chunk in a streamed body does not end the chunked body"
      (Streamed ["head", "", filler 20000, "x", "", B.replicate 9000 51])
      [Send (singleGet <> singleGet)]
      [stays (replicate 2 (ok (B.concat ["head", filler 20000, "x", B.replicate 9000 51])))]
      & note "A premature 0-length chunk would end the first response early and break the parse of the second; hence two pipelined requests."
  , mk AResponseFraming (Warp "warp/streams-test/AnswerSpec.hs" "the header and the body / delivers every byte in order through partial sends, for chunks (sizes 1 to 70000)")
      "chunks of very different sizes arrive whole and in order"
      (Streamed [B.replicate n (fromIntegral (65 + n `mod` 26)) | n <- [1, 17, 4096, 70000, 255, 16384, 3]])
      [Send singleGet]
      [stays [ok (B.concat [B.replicate n (fromIntegral (65 + n `mod` 26)) | n <- [1, 17, 4096, 70000, 255, 16384, 3]])]]
  , mk AResponseFraming (Warp "warp/streams-test/AnswerSpec.hs" "a fixed answer / delivers a large body to a client that reads it slowly")
      "a response of 4 MiB is delivered whole to a client that does not read for a while"
      (Fixed 200 [("Content-Length", "4194304")] (filler 4194304))
      [Send singleGet, PauseReadingMs 500]
      [stays [ok (filler 4194304)]]
      & note "The runner stops reading so that the server's writes block; the server must neither drop the rest nor time out."
  , mk AResponseFraming (Warp "warp/streams-test/AnswerSpec.hs" "the header and the body / delivers every byte in order through partial sends, for chunks")
      "a chunked response of 4 MiB is delivered whole to a client that does not read for a while"
      (Streamed (replicate 64 (filler 65536)))
      [Send singleGet, PauseReadingMs 500]
      [stays [ok (B.concat (replicate 64 (filler 65536)))]]
  , mk AResponseFraming (runSpec "head requests / builder")
      "the response to a HEAD has no body, and the next response follows its head"
      (Fixed 200 [("Content-Length", "5")] "hello")
      [Send ("HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n" <> singleGet)]
      [stays [toHead (ok "" `withHeaders` [headerIs "Content-Length" "5"]), ok "hello"]]
  , mk AResponseFraming (runSpec "head requests / streaming")
      "the response to a HEAD of a streamed body has no body"
      (Streamed ["never", "sent"])
      [Send ("HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n" <> singleGet)]
      [stays [toHead (ok ""), ok "neversent"]]
      & note "The head may or may not carry Transfer-Encoding: chunked (RFC 9110 9.3.2); it must have no body bytes."
  , mk AResponseFraming (RFC "RFC 9110 9.3.2" "a server must not send a Content-Length in a response to HEAD unless it is the length of the body of the GET")
      "the response to a HEAD gets no Content-Length that its handler did not give"
      (Fixed 200 [] "hello")
      [Send ("HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n" <> singleGet)]
      [stays [toHead (ok "" `withHeaders` [HeaderAbsent "Content-Length", HeaderAbsent "Transfer-Encoding"]), ok "hello" `withHeaders` [headerIs "Content-Length" "5"]]]
      & note "PostgREST answers a HEAD with a declared length of 0, which Warp does not turn into a Content-Length: 0."
  , mk AResponseFraming (RFC "RFC 9110 9.3.2" "a server must not send a Content-Length in a response to HEAD unless it is the length of the body of the GET")
      "the response to a HEAD of a streamed body gets no Transfer-Encoding and no Content-Length"
      (Streamed ["never", "sent"])
      [Send ("HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n" <> singleGet)]
      [stays [toHead (ok "" `withHeaders` [HeaderAbsent "Content-Length", HeaderAbsent "Transfer-Encoding"]), ok "neversent"]]
  , mk AResponseFraming (RFC "RFC 9110 9.3.2" "a server must not send a Content-Length in a response to HEAD unless it is the length of the body of the GET")
      "the response to an HTTP/1.0 HEAD gets no Content-Length that its handler did not give"
      (Fixed 200 [] "foo")
      [Send "HEAD / HTTP/1.0\r\nHost: localhost\r\n\r\n"]
      [closes [withVersion "HTTP/1.0" (toHead (ok "" `withHeaders` [HeaderAbsent "Content-Length", HeaderAbsent "Transfer-Encoding"]))]]
  , mk AResponseFraming (Warp "warp/streams-test/AnswerSpec.hs" "no body / sends only the header and pulls nothing, for a 204")
      "a 204 has no body, no Content-Length and no Transfer-Encoding"
      (Fixed 204 [] "")
      [Send (singleGet <> singleGet)]
      [stays (replicate 2 (resp 204 `withHeaders` [HeaderAbsent "Content-Length", HeaderAbsent "Transfer-Encoding", HeaderAbsent "Connection"]))]
  , mk AResponseFraming (Spec "1xx, 204 and 304 have no body")
      "a body a handler gives a 204 is not sent"
      (Fixed 204 [] "must not be sent")
      [Send (singleGet <> singleGet)]
      [stays (replicate 2 (resp 204 `withHeaders` [HeaderAbsent "Content-Length", HeaderAbsent "Transfer-Encoding"]))]
  , mk AResponseFraming (Warp "warp/streams-test/AnswerSpec.hs" "no body / sends only the header and pulls nothing, for a 304")
      "a 304 has no body and no Transfer-Encoding"
      (Fixed 304 [("ETag", "\"abc\"")] "")
      [Send (singleGet <> singleGet)]
      [stays (replicate 2 (resp 304 `withHeaders` [headerIs "ETag" "\"abc\"", HeaderAbsent "Transfer-Encoding", HeaderAbsent "Connection"]))]
  , mk AResponseFraming (RFC "RFC 9110 8.6" "a 304 may carry the Content-Length of the 200 it stands for")
      "a 304 with a Content-Length has no body all the same"
      (Fixed 304 [("Content-Length", "5")] "hello")
      [Send (singleGet <> singleGet)]
      [stays (replicate 2 (resp 304 `withHeaders` [headerIs "Content-Length" "5"]))]
  ]

----------------------------------------------------------------------
-- Response headers

responseHeaderCases :: [Case]
responseHeaderCases =
  [ mk AResponseHeaders (Warp "warp/streams-test/AnswerSpec.hs" "a fixed answer / sends its own length and the headers Warp adds, and keeps the connection open")
      "a Date and a Server header are added, once each"
      (Fixed 200 [("Content-Length", "5")] "hello")
      [Send singleGet]
      [ stays
          [ ok "hello"
              `withHeaders` [ HeaderSatisfies "Date" "an IMF-fixdate such as Mon, 13 Aug 2012 04:22:55 GMT" isImfFixdate
                            , HeaderSatisfies "Server" "a non-empty product token" (not . B.null)
                            ]
          ]
      ]
  , mk AResponseHeaders (runSpec "only one date and server header")
      "a Date and a Server the handler sets are kept and not duplicated"
      (Fixed 200 [("server", "server"), ("date", "date"), ("Content-Length", "0")] "")
      [Send singleGet]
      [stays [ok "" `withHeaders` [headerIs "Server" "server", headerIs "Date" "date"]]]
  , mk AResponseHeaders (Warp "warp/streams-test/AnswerSpec.hs" "a fixed answer / sends its own length and the headers Warp adds (X-Path)")
      "headers the handler sets reach the client in order"
      (Fixed 200 [("X-One", "1"), ("X-Two", "2"), ("Content-Length", "0")] "")
      [Send singleGet]
      [stays [ok "" `withHeaders` [headerIs "X-One" "1", headerIs "X-Two" "2"]]]
  , mk AResponseHeaders (RFC "RFC 9110 5.3" "repeated field lines stay separate lines")
      "a header the handler repeats is sent as separate lines"
      (Fixed 200 [("Set-Cookie", "a=1"), ("Set-Cookie", "b=2"), ("Content-Length", "0")] "")
      [Send singleGet]
      [stays [ok "" `withHeaders` [HeaderValues "Set-Cookie" ["a=1", "b=2"]]]]
  , mk AResponseHeaders (Warp "warp/test/ResponseSpec.hs" "sanitizeHeaderValue / replaces multiline header value's [CR LF NUL] with SP (CR LF SP)")
      "CR LF SP in a header value the handler sets become three spaces"
      (Fixed 200 [("X-Test", "foo\r\n bar"), ("Content-Length", "0")] "")
      [Send singleGet]
      [stays [ok "" `withHeaders` [headerIs "X-Test" "foo   bar"]]]
  , mk AResponseHeaders (Warp "warp/test/ResponseSpec.hs" "sanitizeHeaderValue / replaces multiline header value's [CR LF NUL] with SP (a lone LF, a lone CR, a NUL)")
      "a lone LF, a lone CR and a NUL in header values become spaces"
      (Fixed 200 [("X-Lf", "foo\nbar"), ("X-Cr", "foo\rbar"), ("X-Nul", "foo\NULbar"), ("Content-Length", "0")] "")
      [Send singleGet]
      [stays [ok "" `withHeaders` [headerIs "X-Lf" "foo bar", headerIs "X-Cr" "foo bar", headerIs "X-Nul" "foo bar"]]]
  , mk AResponseHeaders (Warp "warp/test/ResponseSpec.hs" "sanitizeHeaderValue / replaces multiline header value's [CR LF NUL] with SP (header injection)")
      "a header value cannot inject another header line"
      (Fixed 200 [("X-Test", "a\r\nX-Injected: yes"), ("Content-Length", "0")] "")
      [Send singleGet]
      [stays [ok "" `withHeaders` [HeaderAbsent "X-Injected"]]]
  ]

----------------------------------------------------------------------
-- Connection persistence

connectionCases :: [Case]
connectionCases =
  [ mk AConnection (Warp "warp/streams-test/AnswerSpec.hs" "a fixed answer / sends its own length and the headers Warp adds, and keeps the connection open")
      "HTTP/1.1 keeps the connection open and says nothing about it"
      (Fixed 200 [("Content-Length", "3")] "foo")
      [Send singleGet, PauseMs 20, Send singleGet]
      [stays (replicate 2 (ok "foo" `withHeaders` [HeaderAbsent "Connection"]))]
  , mk AConnection (RFC "RFC 9112 9.3" "HTTP/1.1 connections persist unless a side says close")
      "Connection: keep-alive on HTTP/1.1 keeps the connection open"
      (Fixed 200 [("Content-Length", "3")] "foo")
      [Send "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"]
      [stays [ok "foo"]]
  ]
    ++ [ mk AConnection (Warp "warp/test/ConnectionSpec.hs" ("Connection header / HTTP/1.1 Connection: close behavior / sends \"Connection: close\" when " ++ what))
          ("a " ++ what ++ " is answered with Connection: close, then the connection closes")
          (Fixed status [("Content-Length", num' (B.length body)) | status /= 204] body)
          [Send req]
          [closes [isHead (resp status `withHeaders` [headerIs "Connection" "close"]) isHeadReq]]
       | (what, status, body, req, isHeadReq) <-
            [ ("GET request has \"Connection: close\" (200 OK)", 200, "foo", "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", False)
            , ("HEAD request has \"Connection: close\" (200 OK)", 200, "foo", "HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", True)
            , ("request has \"Connection: close\" (204 No Content)", 204, "", "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", False)
            , ("request has \"Connection: close\" (500 Internal Server Error)", 500, "error", "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", False)
            ]
       ]
    ++ [ mk AConnection (Warp "warp/test/ConnectionSpec.hs" "Connection header / HTTP/1.1 Connection: close behavior / does NOT send \"Connection: close\" for HTTP/1.1 HEAD request")
          "an HTTP/1.1 HEAD does not get Connection: close and keeps the connection open"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send "HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n"]
          [stays [toHead (ok "" `withHeaders` [HeaderAbsent "Connection"])]]
       , mk AConnection (RFC "RFC 9112 9.6" "a server MUST close after the final response to a request with close, and not read further requests")
          "a request after one with Connection: close is not answered"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send ("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" <> singleGet)]
          [closes [ok "foo" `withHeaders` [headerIs "Connection" "close"]]]
       , mk AConnection (RFC "RFC 9110 7.6.1" "the Connection option name is case-insensitive")
          "Connection: CLOSE closes the connection"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: CLOSE\r\n\r\n"]
          [closes [ok "foo" `withHeaders` [headerIs "Connection" "close"]]]
       , mk AConnection (RFC "RFC 9110 7.6.1" "Connection is a list of options; close among them closes")
          "Connection: keep-alive, close closes the connection"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive, close\r\n\r\n"]
          [closes [ok "foo" `withHeaders` [headerIs "Connection" "close"]]]
          & note "Warp compares the whole value to close, so it keeps this connection open."
       , mk AConnection (RFC "RFC 9110 7.6.1" "Connection is a list of options; close among them closes")
          "a close in a second Connection line closes the connection"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: te\r\nConnection: close\r\n\r\n"]
          [closes [ok "foo" `withHeaders` [headerIs "Connection" "close"]]]
       , -- HTTP/1.0
         mk AConnection (Warp "warp/test/ConnectionSpec.hs" "Connection header / HTTP/1.0 Connection: close behavior / sends \"Connection: close\" on regular HTTP/1.0 GET request")
          "an HTTP/1.0 request is answered as HTTP/1.0 with Connection: close, and the connection closes"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send "GET / HTTP/1.0\r\nHost: localhost\r\n\r\n"]
          [closes [withVersion "HTTP/1.0" (ok "foo" `withHeaders` [headerIs "Connection" "close"])]]
       , mk AConnection (Spec "HTTP/1.0 keep-alive follows Warp")
          "an HTTP/1.0 request with Connection: keep-alive and a response with a length keeps the connection open"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send "GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n", PauseMs 20, Send "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n"]
          [stays (replicate 2 (withVersion "HTTP/1.0" (ok "foo" `withHeaders` [HeaderNoValue "Connection" "close"])))]
          & note "Warp does not add 'Connection: keep-alive' to the response; it only must not say close. A server that adds keep-alive is accepted."
       , mk AConnection (Warp "warp/test/ConnectionSpec.hs" "Connection header / HTTP/1.0 Connection: close behavior / sends \"Connection: close\" when response implies close (HTTP/1.0 Keep-Alive, but no Content-Length)")
          "an HTTP/1.0 keep-alive request whose response has no length is answered with Connection: close and the body runs to the close"
          (Streamed ["foo"])
          [Send "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n"]
          [closes [withVersion "HTTP/1.0" (ok "foo" `withHeaders` [headerIs "Connection" "close", HeaderAbsent "Transfer-Encoding", HeaderAbsent "Content-Length"])]]
          & note "No chunked coding to an HTTP/1.0 client: the end of the body is the end of the connection."
       , mk AConnection (Spec "HTTP/1.0 is answered as Warp does")
          "an HTTP/1.0 HEAD without keep-alive is answered without a body and the connection closes"
          (Fixed 200 [("Content-Length", "3")] "foo")
          [Send "HEAD / HTTP/1.0\r\nHost: localhost\r\n\r\n"]
          [closes [withVersion "HTTP/1.0" (toHead (ok "" `withHeaders` [headerIs "Content-Length" "3"]))]]
       ]
  where
    num' = C.pack . show
    isHead r b = if b then toHead r else r

----------------------------------------------------------------------
-- Pipelining

-- | The scenarios of Warp's RunSpec: each request as the pieces Warp's test
-- sends it in, and what Echo answers.
runScenarios :: [(String, [([ByteString], ByteString)])]
runScenarios =
  [ ("no body", replicate 5 ([singleGet], ""))
  , ("has body", [([singlePostHello], "Hello"), ([singleGet], "")])
  , ("chunked body", [(singleChunkedPostHello, "Hello"), ([singleGet], "")])
  ]

pipeliningCases :: [Case]
pipeliningCases =
  [ mk APipelining (runSpec (group ++ " / " ++ scenario ++ ", " ++ hname))
      (group ++ ", " ++ scenario ++ ", " ++ hname)
      handler
      (arrange (concatMap fst requests))
      [stays [answer echoed | (_, echoed) <- requests]]
  | (group, arrange) <- [("non-pipelining", sends), ("pipelining", \pieces -> [Send (B.concat pieces)])]
  , (scenario, requests) <- runScenarios
  , (hname, handler, answer) <- [("read (Echo)", Echo, ok), ("ignore (Ignore)", Ignore, const (ok ignoreBody))]
  ]
    ++ [ mk APipelining (Spec "requests sent back to back are answered in order")
          "three requests in one write are answered in order"
          Headers
          [Send (get "/one" <> get "/two" <> get "/three")]
          [stays [okHeaders "GET" t [hostLocal] | t <- ["/one", "/two", "/three"]]]
       , mk APipelining (Spec "requests sent back to back are answered in order")
          "fifty requests in one write get fifty answers in order"
          Headers
          [Send (B.concat [get ("/" <> num i) | i <- [1 .. 50]])]
          [stays [okHeaders "GET" ("/" <> num i) [hostLocal] | i <- [1 .. 50]]]
       , mk APipelining (Spec "requests sent back to back are answered in order")
          "pipelined requests of different kinds are answered in order"
          Echo
          [Send (postBody "one" <> get "/two" <> chunkedPost ["th", "ree"] <> get "/four" <> postBody "five")]
          [stays [ok "one", ok "", ok "three", ok "", ok "five"]]
       , mk APipelining (Spec "after a framing error the connection is closed")
          "a request with a framing error among pipelined requests ends the answers"
          Headers
          [Send (get "/one" <> "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: abc\r\n\r\n" <> get "/three")]
          [closes [okHeaders "GET" "/one" [hostLocal], resp 400]]
          & note "The first request is answered; the bad second one gets 400; the third is not answered."
       ]

----------------------------------------------------------------------
-- Handler failure

handlerErrorCases :: [Case]
handlerErrorCases =
  [ mk AHandlerErrors (Warp "warp/test/ExceptionSpec.hs" "responds even if there is an exception / ioException")
      "a handler that fails before answering is answered 500"
      Fail
      [Send (get "/ioException")]
      [anyConn [resp 500]]
      & note "The connection after a handler failure is not specified: Warp keeps it open in the plain path and closes it in the streaming one. The 500 is required either way."
  ]

----------------------------------------------------------------------
-- Timeouts

timeout1s :: Settings
timeout1s = noSettings {timeoutMs = Just 1000}

timeoutCases :: [Case]
timeoutCases =
  [ mk ATimeouts (Spec "a client that stops sending in the middle of the head")
      "a client that stops in the middle of the request line is closed after the timeout"
      Headers
      [Send "GET /slow HTTP/1.1\r\nHo"]
      givesUp
      & withSettings timeout1s
      & note "Warp closes without a response; a 408 is accepted too. The runner waits the timeout plus the patience."
  , mk ATimeouts (Warp "warp/test/RunSpec.hs" "connection termination / IncompleteHeaders (a head that never ends)")
      "a client that stops in the middle of the header fields is closed after the timeout"
      Headers
      [Send "GET / HTTP/1.1\r\nHost: localhost\r\nX-Slow: a"]
      givesUp
      & withSettings timeout1s
  , mk ATimeouts (Spec "a client that stops sending in the middle of the body")
      "a client that stops in the middle of a Content-Length body is closed after the timeout"
      Echo
      [Send (postHead 100 <> "0123456789")]
      givesUp
      & withSettings timeout1s
  , mk ATimeouts (Spec "a client that stops sending in the middle of the body")
      "a client that stops in the middle of a chunk is closed after the timeout"
      Echo
      [Send (chunkedHead <> "10\r\n0123456789")]
      givesUp
      & withSettings timeout1s
  , mk ATimeouts (Spec "a client that stops sending in the middle of the body")
      "a client that stops after a chunk, before the last chunk, is closed after the timeout"
      Echo
      [Send (chunkedHead <> "5\r\nHello\r\n")]
      givesUp
      & withSettings timeout1s
  , mk ATimeouts (runSpec "chunked bodies / timeout in request body")
      "a body that arrives slowly but within the timeout is read in full"
      Echo
      [ Send "POST / HTTP/1.1\r\nHost: foo\r\n"
      , Send ("content-length: " <> num (2048 + 13) <> "\r\n\r\n")
      , PauseMs 400
      , Send (B.replicate 2048 88)
      , PauseMs 400
      , Send "This is short"
      ]
      [stays [ok (B.replicate 2048 88 <> "This is short")]]
      & withSettings timeout1s
      & note "Warp's test also has the handler sleep half a second after reading, to check the timeout does not fire while the handler runs. The first piece is 2048 bytes because Warp's anti-slowloris rule counts only receives of at least that size as activity; fulmen's rule is that any bytes are."
  , mk ATimeouts (Warp "warp/streams-test/AnswerSpec.hs" "timeouts / does not time out while the loop waits only for a slow source")
      "a handler that is slower than the timeout is not cut off"
      (Slow 1500 ["one", "two", "three"])
      [Send singleGet]
      [stays [ok "onetwothree" `withHeaders` [headerIs "Transfer-Encoding" "chunked"]]]
      & withSettings timeout1s
      & note "The timeout is for a client that stops sending in the middle of a head or body, not for the handler."
  ]

----------------------------------------------------------------------
-- Streaming

streamingCases :: [Case]
streamingCases =
  [ mk AStreaming (runSpec "streaming echo #249")
      "a chunked request body is echoed as it arrives, before the last chunk"
      EchoStreaming
      [ Send "POST / HTTP/1.1\r\nHost: foo\r\ntransfer-encoding: chunked\r\n\r\n"
      , PauseMs 10
      , Send "5\r\nhello\r\n"
      , AwaitReceived "hello"
      , Send "0\r\n\r\n"
      ]
      [stays [ok "hello" `withHeaders` [headerIs "Transfer-Encoding" "chunked", HeaderAbsent "Content-Length"]]]
      & note "Warp's test only checks that the status line arrives after the first chunk is echoed; here the client also waits for the echoed bytes before it sends the last chunk, which fails if the server buffers the response."
  , mk AStreaming (runSpec "streaming echo #249")
      "a Content-Length request body is echoed as it arrives, before the rest"
      EchoStreaming
      [ Send "POST / HTTP/1.1\r\nHost: foo\r\nContent-Length: 10\r\n\r\n"
      , PauseMs 10
      , Send "hello"
      , AwaitReceived "hello"
      , Send "world"
      ]
      [stays [ok "helloworld" `withHeaders` [headerIs "Transfer-Encoding" "chunked", HeaderAbsent "Content-Length"]]]
  , mk AStreaming (Warp "warp/streams-test/AnswerSpec.hs" "timeouts / does not time out while the loop waits only for a slow source (chunks, without a timeout)")
      "a body streamed with pauses between the chunks arrives whole"
      (Slow 300 ["one", "two", "three"])
      [Send singleGet]
      [stays [ok "onetwothree" `withHeaders` [headerIs "Transfer-Encoding" "chunked"]]]
  , mk AStreaming (Warp "warp/streams-test/AnswerSpec.hs" "a client that has gone / keeps sending after the client half-closes")
      "a client that half-closes after its request still gets the whole answer"
      (Slow 200 ["one", "two", "three"])
      [Send singleGet, HalfClose]
      [closes [ok "onetwothree" `withHeaders` [headerIs "Transfer-Encoding" "chunked"]]]
      & note "After the client's end-of-file the server answers the request and then closes."
  ]
