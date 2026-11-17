-- | How the connections that the acceptor took in one round are shared out between the cores that wait for them.
module Network.Fulmen.Internal.Split
  ( shares,
  )
where

import qualified Data.IntMap.Strict as IntMap
import qualified Data.Set as Set

-- | How many of the sockets each core is given, in the order of the cores, from how many connection workers each core has and how
-- many more it has room for. Each socket goes to the core that would have the fewest connection workers with it, the later of the
-- cores if they tie, which is the one that has waited longest, until the room of every core is used. The cores have as many connection
-- workers as one another afterwards, as far as their room allows.
shares :: Int -> [(Int, Int)] -> [Int]
shares sockets [(_, room)] = [max 0 (min sockets room)]
shares sockets cores = [IntMap.findWithDefault 0 at given | at <- [0 .. length cores - 1]]
  where
    given = fill sockets open IntMap.empty
    -- The cores that have room left, by how many connection workers each would have, and by the position that the later core has first.
    open = Set.fromList [(have, negate at, room) | (at, (have, room)) <- zip [0 ..] cores, room > 0]
    fill left candidates taken
      | left <= 0 = taken
      | otherwise = case Set.minView candidates of
          Nothing -> taken
          Just ((have, back, room), others) ->
            fill (left - 1) (if room > 1 then Set.insert (have + 1, back, room - 1) others else others) (IntMap.insertWith (+) (negate back) 1 taken)
