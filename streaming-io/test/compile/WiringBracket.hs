{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}

module WiringBracket where

import Control.Arrow ((>>>), (^>>))
import GHC.Records (HasField)
import Network.Streaming.Classes
import Network.Streaming.Named
import Network.Streaming.Wiring

-- | A step that reads its port "i", and gives it.
same :: (Steps arr, HasField "i" r Int) => arr r Int
same = now (\r -> pure r.i)

-- | A step that reads its port "i", and gives a record of two outputs.
twoOf :: (Steps arr, HasField "i" r Int) => arr r ("x" := Int, "y" := Int)
twoOf = now (\r -> pure (newRecord ["x", "y"] r.i (r.i + 1)))

-- | A release that reads the ports "res" and "exit".
closing :: (Steps arr, HasField "res" r Int, HasField "exit" r Exit) => arr r ()
closing = now (\r -> r.res `seq` r.exit `seq` pure ())

-- | A release that reads the ports "x", "y" and "j".
closingAll :: (Steps arr, HasField "x" r Int, HasField "y" r Int, HasField "j" r Int) => arr r ()
closingAll = now (\r -> r.x `seq` r.y `seq` r.j `seq` pure ())

-- | A release that reads the port "j" and the exit.
closingJ :: (Steps arr, HasField "j" r Int, HasField "exit" r Exit) => arr r ()
closingJ = now (\r -> r.j `seq` r.exit `seq` pure ())

-- | A release that takes the exit whole.
closingExit :: (Steps arr) => arr Exit ()
closingExit = now (\exit -> exit `seq` pure ())

-- | A release that takes a pair, the name of the flow and the exit.
closingPair :: (Steps arr) => arr (Int, Exit) ()
closingPair = now (\(j, exit) -> j `seq` exit `seq` pure ())

-- | A wrapper that has a resource: it is given a number, and gives the body its double, beside the record that the body reads.
doubling :: (Steps arr) => arr (Int, a) z -> arr (Int, a) z
doubling body = (\(n, a) -> (n * 2, a)) ^>> body

-- | The same wrapper, given a record that has the port "n".
doublingRecord :: (Steps arr, HasField "n" r Int) => arr (Int, a) z -> arr (r, a) z
doublingRecord body = (\(r, a) -> (r.n * 2, a)) ^>> body

-- | The same wrapper, given a pair of numbers, which it adds.
summing :: (Steps arr) => arr (Int, a) z -> arr ((Int, Int), a) z
summing body = (\((m, n), a) -> (m + n, a)) ^>> body

-- | A wrapper that has none.
plainly :: (Steps arr) => arr a z -> arr a z
plainly body = body

-- | A line that acquires and is released after the rest of the flow, by a release that reads the name that the line gave and
-- the exit, by a release that reads the names of a pattern and a name of the flow, by a release that takes the exit whole,
-- and by a release that takes a pair; a release only, with a name and with a list; a line that is released and keeps nothing;
-- a line that is a wrapper, with a name, a list and a tuple of names for its input, and one that has no resource.
wired :: (Steps arr) => arr ("i" := Int, "j" := Int) Int
wired =
  result
    "total"
    ( "res" <: same -< ["i"] `releasedBy` closing -< ["res", "exit"]
        >>> ["x", "y"] <: twoOf -< ["i"] `releasedBy` closingAll -< ["x", "y", "j"]
        >>> "kept" <: same -< ["i"] `releasedBy` closingExit -< "exit"
        >>> same -< ["i"] `releasedBy` closingPair -< ("j", "exit")
        >>> onExit (closingJ -< ["j", "exit"])
        >>> "doubled" <: inside doubling "j"
        >>> "other" <: inside doublingRecord ["n" =: "j"]
        >>> "pair" <: inside summing ("j", "doubled")
        >>> inside_ plainly
        >>> "total" <: same -< ["i"]
    )
