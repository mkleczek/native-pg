-- | How the connections that the acceptor took in one round are shared out between the cores that wait for them.
module Network.Fulmen.Internal.Split
  ( shares,
  )
where

-- | How many of the sockets each core is given, in the order of the cores, from how many connection workers each core has and how
-- many more it has room for: the first core takes as many as it has room for, and the next takes what is left.
shares :: Int -> [(Int, Int)] -> [Int]
shares sockets cores = case cores of
  [] -> []
  (_, room) : others -> let given = min sockets room in given : shares (sockets - given) others
