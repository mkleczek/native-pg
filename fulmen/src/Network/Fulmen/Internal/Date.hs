-- | The @Date@ header's value, which is made once a second: a response reads the clock, and renders the date only when
-- the second is not the one that was rendered last, so that most responses allocate nothing for it. The cores share
-- it, and the one that sees a new second renders it: two that do at once render the same.
module Network.Fulmen.Internal.Date
  ( Date,
    newDate,
    currentDate,
    formatDate,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Foreign.C.Types (CTime (..))
import Foreign.Ptr (Ptr, nullPtr)

-- | A date, as the second of the epoch it was rendered for and its text.
data Date = Date !Int !ByteString

-- | A date that has not been rendered yet.
newDate :: IO (IORef Date)
newDate = newIORef (Date 0 B.empty)

-- | The text of the date now, in the form of RFC 9110 section 5.6.7.
currentDate :: IORef Date -> IO ByteString
currentDate ref = do
  CTime seconds <- c_time nullPtr
  Date rendered text <- readIORef ref
  if rendered == fromIntegral seconds
    then pure text
    else do
      let text' = formatDate (fromIntegral seconds)
      writeIORef ref (Date (fromIntegral seconds) text')
      pure text'

foreign import ccall unsafe "time"
  c_time :: Ptr CTime -> IO CTime

-- | The IMF-fixdate of a second of the epoch, for example @Mon, 13 Aug 2012 04:22:55 GMT@.
formatDate :: Int -> ByteString
formatDate seconds =
  C.pack (weekday ++ ", " ++ two day ++ " " ++ monthName ++ " " ++ show year ++ " " ++ two hours ++ ":" ++ two minutes ++ ":" ++ two secondsOfMinute ++ " GMT")
  where
    (days, secondsOfDay) = seconds `divMod` 86400
    (hours, rest) = secondsOfDay `divMod` 3600
    (minutes, secondsOfMinute) = rest `divMod` 60
    -- The epoch began on a Thursday.
    weekday = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"] !! (days `mod` 7)
    (year, month, day) = civil days
    monthName = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"] !! (month - 1)
    two n = if n < 10 then '0' : show n else show n

-- | The year, month and day of a day of the epoch, in the proleptic Gregorian calendar.
civil :: Int -> (Int, Int, Int)
civil days = (if month <= 2 then year + 1 else year, month, day)
  where
    z = days + 719468
    era = z `div` 146097
    dayOfEra = z - era * 146097
    yearOfEra = (dayOfEra - dayOfEra `div` 1460 + dayOfEra `div` 36524 - dayOfEra `div` 146096) `div` 365
    year = yearOfEra + era * 400
    dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra `div` 4 - yearOfEra `div` 100)
    shifted = (5 * dayOfYear + 2) `div` 153
    day = dayOfYear - (153 * shifted + 2) `div` 5 + 1
    month = if shifted < 10 then shifted + 3 else shifted - 9
