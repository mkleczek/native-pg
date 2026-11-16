{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PatternSynonyms #-}

-- | The lint of "Network.Streaming.Lint": small programs, built at the diagram arrow, that each check finds, and near
-- misses that it doesn't.
module LintSpec (spec) where

import Control.Arrow (ArrowChoice, arr, (&&&), (>>>), (>>^), (^>>), (|||))
import Data.List (isSuffixOf)
import GHC.Stack (SrcLoc (..))
import GHC.TypeLits (KnownSymbol)
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, Step, answering, asking, box, done, foreverA, iterateWith, tap, withInbox)
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (..))
import Network.Streaming.Diagram (Diagram)
import Network.Streaming.Lint
import Skeletest

-- | What the programs need of an arrow.
type Program arr =
  (ArrowChoice arr, Effect arr, GiveWay arr, Scope arr, Race arr, Within arr, Concurrent arr, Annotate arr)

-- | A step that never waits.
quick :: (Effect arr) => arr a ()
quick = now (const (pure ()))

-- | A step that goes round again from a number above 0, and finishes at 0, and never waits.
countDown :: (Effect arr) => arr Int (Step Int ())
countDown = now (\n -> pure (if n > 0 then Again (n - 1) else Finish ()))

-- | A scope whose acquire waits.
waitingAcquire :: (Program arr) => arr () ()
waitingAcquire = scope (giveWay >>> quick) quick quick

-- | The checks that were found, and the checks that were allowed, with their reasons.
summary :: Report -> ([Check], [(Check, String)])
summary report =
  ( findingCheck <$> reportFindings report,
    [(findingCheck (allowedFinding allowed), allowedReason allowed) | allowed <- reportAllowed report]
  )

spec :: Spec
spec = do
  acquires
  loops
  spawns
  deadlines
  shields
  allowances
  inboxes
  exchanges
  findings

-- | A scope whose acquire waits, or does not.
acquires :: Spec
acquires = describe "a scope" $ do
  it "has a wait in its acquire found" $ do
    report <- lint (waitingAcquire :: Diagram () ())
    summary report `shouldBe` ([WaitInAcquire], [])

  it "has a wait in its acquire found, however deep it is in boxes" $ do
    report <- lint (scope (box "acquire" (box "inner" (giveWay >>> quick))) quick quick :: Diagram () ())
    summary report `shouldBe` ([WaitInAcquire], [])

  it "is not found when only its body waits" $ do
    report <- lint (scope quick quick (giveWay >>> quick) :: Diagram () ())
    summary report `shouldBe` ([], [])

  it "is not found when only its release waits, which is shielded and may" $ do
    report <- lint (scope quick (giveWay >>> quick) quick :: Diagram () ())
    summary report `shouldBe` ([], [])

-- | Loops, which are trees that contain themselves.
loops :: Spec
loops = describe "a loop" $ do
  it "whose again path never waits is found" $ do
    report <- lint (iterateWith countDown (arr id))
    summary report `shouldBe` ([LoopWithoutWait], [])

  it "whose again path waits is not found" $ do
    report <- lint (iterateWith countDown giveWay)
    summary report `shouldBe` ([], [])

  it "whose body waits before it decides is not found, whatever the paths after it" $ do
    report <- lint (iterateWith (giveWay >>> countDown) (arr id))
    summary report `shouldBe` ([], [])

  it "that waits only when it finishes is found" $ do
    report <- lint waitsOnlyWhenDone
    summary report `shouldBe` ([LoopWithoutWait], [])

  it "with a wait on one of the branches that go round is found, and on both is not" $ do
    one <- lint (branching giveWay (arr id))
    both <- lint (branching giveWay giveWay)
    (summary one, summary both) `shouldBe` (([LoopWithoutWait], []), ([], []))

  it "that goes round with nothing in it is found, and with a wait is not" $ do
    spinning <- lint (foreverA (arr id) :: Diagram Int ())
    waiting <- lint (foreverA giveWay :: Diagram Int ())
    (summary spinning, summary waiting) `shouldBe` (([LoopWithoutWait], []), ([], []))

  it "counts a wait in a part of a fan-out that comes before the way round" $ do
    report <- lint (foreverA ((giveWay &&& quick) >>^ fst) :: Diagram Int ())
    summary report `shouldBe` ([], [])

  it "does not count a wait in a part that comes after the way round, which is never reached" $ do
    report <- lint afterTheWayRound
    summary report `shouldBe` ([LoopWithoutWait], [])

  it "that is a part of a group that runs together is judged by itself, whatever the other parts wait for" $ do
    spinning <- lint (together [foreverA (arr id), foreverA giveWay] :: Diagram () ())
    waiting <- lint (together [foreverA giveWay, foreverA giveWay] :: Diagram () ())
    (summary spinning, summary waiting) `shouldBe` (([LoopWithoutWait], []), ([], []))

  it "inside a loop whose passes all wait does not make the outer loop found" $ do
    report <- lint (iterateWith countDown (iterateWith (giveWay >>> countDown) (arr id) >>^ const 3))
    summary report `shouldBe` ([], [])

  it "inside a loop is found, with the outer loop, when the inner can finish without a wait" $ do
    report <- lint (iterateWith countDown (box "inner" (iterateWith countDown (arr id)) >>^ const 3))
    (summary report, findingPath <$> reportFindings report)
      `shouldBe` (([LoopWithoutWait, LoopWithoutWait], []), [["inner"], []])

-- | Loops that spawn parts, which is no wait: the part that is spawned runs on its own.
spawns :: Spec
spawns = describe "a loop that spawns" $ do
  it "only is found, and one that waits for something and spawns is not" $ do
    only <- lint (group AwaitAll (foreverA (tap (spawn quick))) :: Diagram () ((), ()))
    waiting <- lint (group AwaitAll (foreverA (tap (snd ^>> block) >>> tap (spawn quick))) :: Diagram Event ((), ()))
    (summary only, summary waiting) `shouldBe` (([LoopWithoutWait], []), ([], []))

  it "is found whatever the part that is spawned waits for, which is not a wait of the loop" $ do
    report <- lint (group AwaitAll (foreverA (tap (spawn giveWay))) :: Diagram () ((), ()))
    summary report `shouldBe` ([LoopWithoutWait], [])

  it "has a loop in the part that is spawned judged by itself, as the parts that run together are" $ do
    spinning <- lint (group AwaitAll (tap (snd ^>> block) >>> tap (spawn (foreverA (arr id))) >>> done) >>> done :: Diagram Event ())
    waiting <- lint (group AwaitAll (tap (snd ^>> block) >>> tap (spawn (foreverA giveWay)) >>> done) >>> done :: Diagram Event ())
    (summary spinning, summary waiting) `shouldBe` (([LoopWithoutWait], []), ([], []))

-- | A loop that gives way only on the way out.
waitsOnlyWhenDone :: (Program arr) => arr Int ()
waitsOnlyWhenDone = loop
  where
    loop = countDown >>> (loop ||| giveWay)

-- | A loop that has two ways to go round, which are the steps that are given.
branching :: (Program arr) => arr Int Int -> arr Int Int -> arr Int ()
branching even' odd' = loop
  where
    loop = now (\n -> pure (if even n then Left n else Right n)) >>> ((even' >>> loop) ||| (odd' >>> loop))

-- | A loop whose way round is in a part, and whose wait is in the part after it.
afterTheWayRound :: (Program arr) => arr Int ()
afterTheWayRound = loop
  where
    loop = (loop &&& giveWay) >>> done

-- | Deadlines and races, which need a wait to decide anything.
deadlines :: Spec
deadlines = describe "a deadline or a race" $ do
  it "over a body that can't wait is found" $ do
    report <- lint (within (millis 5) quick :: Diagram () (Maybe ()))
    summary report `shouldBe` ([DeadlineWithoutWait], [])

  it "over a body that waits is not" $ do
    report <- lint (within (millis 5) (giveWay >>> quick) :: Diagram () (Maybe ()))
    summary report `shouldBe` ([], [])

  -- A deadline whose duration is an input is a deadline too: the finding says so.
  it "whose duration is an input, over a body that can't wait, is found" $ do
    report <- lint (timeoutA quick :: Diagram (Duration, ()) (Maybe ()))
    (summary report, findingWhat <$> reportFindings report) `shouldBe` (([DeadlineWithoutWait], []), ["within the duration of its input"])

  it "whose duration is an input, over a body that waits, is not" $ do
    report <- lint (timeoutA (giveWay >>> quick) :: Diagram (Duration, ()) (Maybe ()))
    summary report `shouldBe` ([], [])

  it "that has a side that can't wait is found, and one that has both sides wait is not" $ do
    one <- lint (race quick (giveWay >>> quick) :: Diagram () (Either () ()))
    both <- lint (race (giveWay >>> quick) (giveWay >>> quick) :: Diagram () (Either () ()))
    (summary one, summary both) `shouldBe` (([DeadlineWithoutWait], []), ([], []))

  it "that has no side that can wait is found once" $ do
    report <- lint (race quick quick :: Diagram () (Either () ()))
    summary report `shouldBe` ([DeadlineWithoutWait], [])

-- | Shields.
shields :: Spec
shields = describe "a shield" $ do
  it "around a wait is found, even if a deadline inside it ends the wait" $ do
    plain <- lint (shield (giveWay >>> quick) :: Diagram () ())
    bounded <- lint (shield (within (millis 5) (giveWay >>> quick)) :: Diagram () (Maybe ()))
    (summary plain, summary bounded) `shouldBe` (([ShieldAroundWait], []), ([ShieldAroundWait], []))

  it "around steps that never wait is not" $ do
    report <- lint (shield (quick >>> quick) :: Diagram () ())
    summary report `shouldBe` ([], [])

-- | A scope that opens the inbox of the name, for a body that is given the inbox and its address.
owning :: forall name. (KnownSymbol name) => Diagram ((Inbox Diagram name 'Anywhere (), Address name 'Anywhere ()), ()) () -> Diagram () ()
owning = withInbox @name @'Anywhere (quick :: Diagram ((), ()) ())

-- | A post to the inbox, and a collect from it.
poster :: forall name. (KnownSymbol name) => Diagram (Address name 'Anywhere ()) ()
poster = (,()) ^>> post >>> quick

collector :: forall name. (KnownSymbol name) => Diagram (Inbox Diagram name 'Anywhere ()) ()
collector = collect >>> quick

-- | A body that posts to the inbox and collects from it.
postAndCollect :: forall name. (KnownSymbol name) => Diagram ((Inbox Diagram name 'Anywhere (), Address name 'Anywhere ()), ()) ()
postAndCollect = fst ^>> ((snd ^>> poster) &&& (fst ^>> collector) >>> done)

-- | The checks on inboxes, which look at the whole program by the names of its inboxes.
inboxes :: Spec
inboxes = describe "an inbox" $ do
  it "that is posted to and collected from, in a scope, is not found" $ do
    report <- lint (owning @"jobs" postAndCollect)
    summary report `shouldBe` ([], [])

  it "that has posts and no collects is found as dead" $ do
    report <- lint (owning @"jobs" (snd . fst ^>> poster))
    summary report `shouldBe` ([DeadInbox], [])
    (findingWhat <$> reportFindings report) `shouldBe` ["posts to jobs"]

  it "that has collects and no posts is found as dead" $ do
    report <- lint (owning @"jobs" (fst . fst ^>> collector))
    summary report `shouldBe` ([DeadInbox], [])
    (findingWhat <$> reportFindings report) `shouldBe` ["collects from jobs"]

  it "is paired by name only: a post and a collect with different names are both dead" $ do
    report <- lint (owning @"jobs" (snd . fst ^>> poster) >>> owning @"tasks" (fst . fst ^>> collector))
    (findingWhat <$> reportFindings report) `shouldBe` ["posts to jobs", "collects from tasks"]

  it "is paired across the boxes of the program, as the program is walked into them" $ do
    report <- lint (owning @"jobs" (fst ^>> ((snd ^>> box "producer" poster) &&& (fst ^>> box "consumer" collector) >>> done)))
    summary report `shouldBe` ([], [])

  it "that is dead is allowed by a part that says why, with the reason" $ do
    report <- lint (allowing DeadInbox "what is posted is collected by another program" (owning @"jobs" (snd . fst ^>> poster)))
    summary report `shouldBe` ([], [(DeadInbox, "what is posted is collected by another program")])

  it "that is opened at two sites of the program is found, at the second" $ do
    report <- lint (owning @"jobs" postAndCollect >>> owning @"jobs" (postAndCollect >>> quick))
    summary report `shouldBe` ([InboxOpenedTwice], [])
    (findingWhat <$> reportFindings report) `shouldBe` ["opens jobs again"]

  it "that is opened at one site that parts share is not found, whatever the number of parts" $ do
    let site = owning @"jobs" postAndCollect
    report <- lint (together [site, site, site])
    summary report `shouldBe` ([], [])

  it "is opened at two sites only if the names are the same: another name is a near miss" $ do
    report <- lint (owning @"jobs" postAndCollect >>> owning @"tasks" (postAndCollect >>> quick))
    summary report `shouldBe` ([], [])

  it "that is opened at two sites is allowed by a part that says why, with the reason" $ do
    report <- lint (allowing InboxOpenedTwice "the sites are in different servers" (owning @"jobs" postAndCollect >>> owning @"jobs" (postAndCollect >>> quick)))
    summary report `shouldBe` ([], [(InboxOpenedTwice, "the sites are in different servers")])

-- | Marks a case as one that fails until tickets are rendezvous.
pending :: Spec -> Spec
pending = xfail "tickets are not rendezvous yet"

-- | A step that awaits a ticket of any core, which the lint sees as a wait, and one that hands over to it, and one that fulfils it.
awaitingStep :: Diagram () ()
awaitingStep = const undefined ^>> await @_ @'Anywhere @() >>> quick

handingOver :: Diagram () ()
handingOver = const undefined ^>> handWaiting @_ @'Anywhere @() >>> quick

-- | The checks on exchanges, which look at the whole program by the names that 'asking' and 'answering' give the ends of a ticket.
exchanges :: Spec
exchanges = pending $ describe "an exchange" $ do
  it "that has an asking end and an answering end is not found" $ do
    report <- lint (asking "jobs" awaitingStep >>> answering "jobs" handingOver)
    summary report `shouldBe` ([], [])

  it "that has an asking end and no answering end is found as dead" $ do
    report <- lint (asking "jobs" awaitingStep)
    summary report `shouldBe` ([DeadExchange], [])
    (findingWhat <$> reportFindings report) `shouldBe` ["asks jobs"]

  it "that has an answering end and no asking end is found as dead, whatever it does: hands over, or fulfils" $ do
    report <- lint (answering "jobs" handingOver >>> answering "jobs" (const undefined ^>> fulfil @_ @'Anywhere @() >>> quick))
    summary report `shouldBe` ([DeadExchange], [])
    (findingWhat <$> reportFindings report) `shouldBe` ["answers jobs"]

  it "is paired by name only: an asking end and an answering end with different names are both dead" $ do
    report <- lint (asking "jobs" awaitingStep >>> answering "tasks" handingOver)
    (findingWhat <$> reportFindings report) `shouldBe` ["asks jobs", "answers tasks"]

  it "is paired across the boxes of the program, as the program is walked into them" $ do
    report <- lint (box "consumer" (asking "jobs" awaitingStep) >>> box "producer" (answering "jobs" handingOver))
    summary report `shouldBe` ([], [])

  it "that is dead is allowed by a part that says why, with the reason" $ do
    report <- lint (allowing DeadExchange "what is handed over is awaited by another program" (answering "jobs" handingOver))
    summary report `shouldBe` ([], [(DeadExchange, "what is handed over is awaited by another program")])

  it "that a loop hands over to, and a loop awaits, is a wait of each loop" $ do
    report <- lint (together [foreverA (tap (asking "jobs" awaitingStep)), foreverA (tap (answering "jobs" handingOver))])
    summary report `shouldBe` ([], [])

  it "has the steps that hand over and that watch counted as waits: a loop of them is not found" $ do
    report <- lint (foreverA (tap (const undefined ^>> untilParked @_ @'Anywhere @() >>> quick)) :: Diagram () ())
    summary report `shouldBe` ([], [])

-- | What a part allows, and what it doesn't.
allowances :: Spec
allowances = describe "an allowance" $ do
  it "for the check of a finding inside it makes the finding allowed, with its reason" $ do
    report <- lint (allowing WaitInAcquire "the acquire cleans up after itself" waitingAcquire :: Diagram () ())
    summary report `shouldBe` ([], [(WaitInAcquire, "the acquire cleans up after itself")])

  it "for another check does not" $ do
    report <- lint (allowing ShieldAroundWait "it is meant" waitingAcquire :: Diagram () ())
    summary report `shouldBe` ([WaitInAcquire], [])

  it "does not reach a finding beside it" $ do
    report <- lint (allowing WaitInAcquire "it is meant" quick >>> waitingAcquire :: Diagram () ())
    summary report `shouldBe` ([WaitInAcquire], [])

  it "holds for a loop when the loop is inside it" $ do
    report <- lint (allowing LoopWithoutWait "it spins on purpose" (foreverA (arr id)) :: Diagram Int ())
    summary report `shouldBe` ([], [(LoopWithoutWait, "it spins on purpose")])

  it "holds for a part that is shared, only where it is in it" $ do
    let shared = waitingAcquire :: Diagram () ()
    report <- lint (allowing WaitInAcquire "it is meant" shared >>> shared)
    summary report `shouldBe` ([WaitInAcquire], [(WaitInAcquire, "it is meant")])

-- | What a finding says.
findings :: Spec
findings = describe "a finding" $ do
  it "is one for a part that is shared, whatever the number of places it is used in" $ do
    let shared = waitingAcquire :: Diagram () ()
    report <- lint (shared >>> shared >>> shared)
    summary report `shouldBe` ([WaitInAcquire], [])

  it "has the boxes on the way from the top, the place of the nearest box, and the wait" $ do
    report <- lint (named (Label "outer" "") (box "inner" (scope (giveWay >>> quick) quick quick)) :: Diagram () ())
    let found = reportFindings report
    (findingPath <$> found, findingWhat <$> found) `shouldBe` ([["outer", "inner"]], ["waits at give way"])
    [maybe False (isSuffixOf "LintSpec.hs" . srcLocFile) (findingSource finding) | finding <- found] `shouldBe` [True]

  it "is described, in the text of the report, with where it is, what it is about, and the counts" $ do
    report <- lint (shield (giveWay >>> quick) :: Diagram () ())
    case lines (renderReport report) of
      [place, what, _, counts] ->
        (place, what, counts)
          `shouldBe` ("ShieldAroundWait: the program", "  waits at give way", "1 findings, 0 allowed")
      other -> other `shouldBe` []
