-- | How the connections that the acceptor took in one round are shared out between the cores that wait for them: all that
-- the room allows is given, no core is given more than it has room for, and the cores are as even as that allows.
module SplitSpec (spec) where

import Control.Monad (forM_)
import Network.Fulmen.Internal.Split (shares)
import Skeletest

spec :: Spec
spec = describe "sharing a batch of connections between the cores" $ do
  it "gives a core that is alone as many as it has room for, and no more" $ do
    shares 3 [(0, 4)] `shouldBe` [3]
    shares 9 [(2, 4)] `shouldBe` [4]
    shares 0 [(0, 4)] `shouldBe` [0]

  it "gives each core all of its room when the batch fills the room of the cores, and no core more than that" $ do
    shares 5 [(3, 1), (0, 4)] `shouldBe` [1, 4]
    shares 7 [(3, 1), (0, 4)] `shouldBe` [1, 4]
    shares 5 [(0, 1), (0, 4)] `shouldBe` [1, 4]

  it "gives a connection to the core that has the fewest" $ do
    shares 1 [(3, 4), (0, 4)] `shouldBe` [0, 1]
    shares 1 [(0, 4), (3, 4)] `shouldBe` [1, 0]

  it "gives the connections of a batch to the cores so that they have as many as one another afterwards" $ do
    shares 4 [(0, 4), (0, 4)] `shouldBe` [2, 2]
    shares 6 [(0, 4), (2, 4)] `shouldBe` [4, 2]
    shares 3 [(2, 4), (0, 4)] `shouldBe` [0, 3]

  it "gives the connection to the core that has waited longest, the last of the cores, when they have as many" $ do
    shares 1 [(1, 3), (1, 3)] `shouldBe` [0, 1]
    shares 2 [(1, 3), (1, 3), (1, 3)] `shouldBe` [0, 1, 1]

  it "gives all that the room allows, within the room of each core, and as evenly as the room allows, for every small case" $
    forM_ cases $ \(sockets, cores) -> do
      let given = shares sockets cores
          rooms = map snd cores
          after = zipWith (\(have, _) more -> have + more) cores given
      length given `shouldBe` length cores
      sum given `shouldBe` min sockets (sum rooms)
      forM_ (zip given rooms) $ \(more, room) -> (more >= 0 && more <= room) `shouldBe` True
      -- No socket could be moved from a core that was given one to a core that has room, and leave them more even.
      forM_ [(giving, receiving) | giving <- zip3 given after rooms, receiving <- zip3 given after rooms] $ \((more, mine, _), (taken, theirs, room)) ->
        (more == 0 || taken == room || mine <= theirs + 1) `shouldBe` True
  where
    -- One to three cores, with up to three connection workers and room for up to three more, and up to eight sockets.
    cases = [(sockets, cores) | cores <- coresOf 3, sockets <- [0 .. 8]]
    coresOf :: Int -> [[(Int, Int)]]
    coresOf most = concat [sequence (replicate count [(have, room) | have <- [0 .. 3], room <- [1 .. 3]]) | count <- [1 .. most]]
