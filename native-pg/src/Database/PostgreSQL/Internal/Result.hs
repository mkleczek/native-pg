-- | Representation shared with the pipeline planner, hidden from clients.
module Database.PostgreSQL.Internal.Result (Result (..)) where

import Database.PostgreSQL.Decoding (DecodingError)
import Database.PostgreSQL.Diagnostics (StatementError)
import Database.PostgreSQL.Session (Row)

-- | How the rows of a result become its value: a fold over them, and a last
-- step that can reject the result.
data Result a = forall accumulated. Result accumulated (accumulated -> Row -> IO (Either DecodingError accumulated)) (accumulated -> Either StatementError a)

instance Functor Result where
  fmap f (Result initial step finish) = Result initial step (fmap f . finish)
