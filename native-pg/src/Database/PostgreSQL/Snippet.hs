{-# LANGUAGE LambdaCase #-}

-- | SQL put together from text and parameters, with the names of hasql's
-- dynamic statements. Placeholders are numbered as the parameters come, so
-- the SQL and the parameters always match.
module Database.PostgreSQL.Snippet
  ( Snippet,
    sql,
    param,
    encoderAndParam,
    render,
    dynamicallyParameterized,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as LBS
import Data.String (IsString (..))
import Database.PostgreSQL.Encoders (NullableOrNot)
import qualified Database.PostgreSQL.Encoders as Encoders
import Database.PostgreSQL.Transaction (Param, RowDecoder, Statement (..))

-- | Text and parameters, in order. Appending is cheap, whatever the sizes.
newtype Snippet = Snippet ([Chunk] -> [Chunk])

data Chunk
  = Text ByteString
  | Parameter Param

instance Semigroup Snippet where
  Snippet left <> Snippet right = Snippet (left . right)

instance Monoid Snippet where
  mempty = Snippet id

instance IsString Snippet where
  fromString = sql . Char8.pack

sql :: ByteString -> Snippet
sql bytes = Snippet (Text bytes :)

param :: Param -> Snippet
param parameter = Snippet (Parameter parameter :)

-- | The parameter for the value, as hasql's 'encoderAndParam' does.
encoderAndParam :: NullableOrNot a -> a -> Snippet
encoderAndParam encoder = param . Encoders.param encoder

-- | The SQL, with a placeholder in place of each parameter, and the
-- parameters.
render :: Snippet -> (ByteString, [Param])
render (Snippet chunks) = finish (foldl' step (1 :: Int, mempty, id) (chunks []))
  where
    step (next, query, parameters) = \case
      Text bytes -> (next, query <> Builder.byteString bytes, parameters)
      Parameter parameter -> (next + 1, query <> Builder.char7 '$' <> Builder.intDec next, parameters . (parameter :))
    finish (_, query, parameters) = (LBS.toStrict (Builder.toLazyByteString query), parameters [])

-- | A statement of the snippet, whose parameters are all in it.
dynamicallyParameterized :: Snippet -> RowDecoder row -> Bool -> Statement () row
dynamicallyParameterized snippet result preparable =
  let (query, parameters) = render snippet
   in Statement query (const parameters) result preparable
