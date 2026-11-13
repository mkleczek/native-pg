{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- | A pool of connections, or of anything else that is loaned, as library code on the primitives of the classes
-- alone: one atomically updated reference to the items that are free and to the 'Giver's of the parts that wait
-- for some, and an arrow's tickets of reach 'Anywhere for the waiting: the pool is shared by the parts of every
-- core, so the givers it keeps can't belong to one. It is written once, for any arrow that has them: parts on the
-- cores of "Network.Streaming.Delimited" share it, and so do the threads of "Network.Streaming.Threads".
--
-- A waiter wants one item or several at once, and says how many must be free before it is served, which can be
-- more than it takes: the buffers of "Network.Streaming.BufferPool" keep a reserve that way. Waiters that want the
-- same number free wait in a lane, in the order they came, and a free item goes to the oldest of the lanes' first
-- waiters that can have it. A connection pool has the one lane of waiters that want one connection, so that a
-- released connection goes to the first of them.
module Network.Streaming.Pool
  ( Pool,
    PoolError (..),
    withLoan,
    withLoanWithin,
    urgently,
    newPool,
    closePool,
    checkout,
    checkoutWithin,
    reserve,
    release,
    free,
    checkoutAt,
    checkoutMany,
    checkoutManyOf,
    takeNow,
    releaseNow,
    queueNow,
    waiters,
  )
where

import Control.Arrow (ArrowChoice, arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (Exception)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.Sequence (Seq, ViewL (..), viewl, (|>))
import qualified Data.Sequence as Seq
import Network.Streaming.Classes
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.Tickets (giveNow)

-- | The items that are free, and how many, the number that the next waiter to queue gets, which says which of two
-- waiters came first, and the waiters in their lanes, by how many items must be free to serve the first of the lane.
-- After every change no lane's first waiter can be served: it needs more items than are free.
data State conn = State !Int ![conn] !Int !(IntMap (Seq (Waiter conn)))

-- | The items and the waiters, and whether the pool has been closed.
data Pool conn = Pool !(IORef (State conn)) !(IORef Bool)

-- | A waiter, by what gives it its items, a ticket's giver for one item or for a number of them at once, and by when it
-- came: the number that the pool counts arrivals with.
data Waiter conn
  = One !(Giver 'Anywhere conn) !Int
  | Many !Int !(Giver 'Anywhere [conn]) !Int

-- | The items that were taken from the free ones for a waiter, to give it.
data Handoff conn
  = OneTo !(Giver 'Anywhere conn) conn
  | ManyTo !(Giver 'Anywhere [conn]) [conn]

-- | Why a pool gave nothing to use.
data PoolError
  = -- | None was free before the pool's checkout deadline.
    CheckoutTimedOut
  | -- | The pool has been closed.
    PoolClosed
  deriving (Eq, Show)

instance Exception PoolError

-- | The body, with a connection or whatever else the pool lends, checked out
-- for as long as it runs and given back when it ends, however it ends: one pool
-- for connections and anything else that is loaned. The checkout waits for as
-- long as it takes: the deadline of a checkout is the caller's, by 'within'. A
-- pool that has been closed lends nothing: 'PoolClosed'.
withLoan :: (Runtime arr) => Pool c -> arr (c, a) b -> arr a (Either PoolError b)
withLoan pool = loaned pool (checkout pool >>^ Just)

-- | The same, but the checkout gives up after the time: 'CheckoutTimedOut',
-- once its place in the queue is given up.
withLoanWithin :: (Runtime arr) => Duration -> Pool c -> arr (c, a) b -> arr a (Either PoolError b)
withLoanWithin limit pool = loaned pool (checkoutWithin limit pool)

-- | The scope around a checkout, which is the arrow that gives the item or, if
-- its deadline passed, nothing, and the release of what it gave.
loaned :: (Runtime arr) => Pool c -> arr () (Maybe c) -> arr (c, a) b -> arr a (Either PoolError b)
loaned pool checkingOut body = scope acquire giveBack run
  where
    acquire = now (\_ -> (\closed -> if closed then Left () else Right ()) <$> isClosed pool) >>> (arr (const (Left PoolClosed)) ||| (checkingOut >>^ maybe (Left CheckoutTimedOut) Right))
    giveBack = fst ^>> (arr (const ()) ||| release pool)
    run = (\(acquired, input) -> fmap (,input) acquired) ^>> (arr Left ||| (body >>^ Right))

-- | The body, as a holder of what the pool lent goes: the pool serves those
-- that hold something first, so that they can give it back. For now it is the
-- body as it is, since the pool serves its waiters in the order they came;
-- serving holders first is a later step.
urgently :: (Runtime arr) => arr a b -> arr a b
urgently = id

newPool :: [conn] -> IO (Pool conn)
-- The state is evaluated before it is kept: 'update' needs the reference to always hold an evaluated value.
newPool conns = Pool <$> (newIORef $! State (length conns) conns 0 IntMap.empty) <*> newIORef False

-- | Closes the pool, and gives the items that are free now, for the caller to dispose of: a pool that is closed lends
-- nothing, so 'withLoan' fails with 'PoolClosed'. The caller closes it once the parts that use it have ended: an item
-- that is released after the pool was closed is free, and nobody collects it, and a part that waits for an item
-- keeps waiting.
closePool :: Pool conn -> IO [conn]
closePool (Pool ref closed) = writeIORef closed True *> update ref (\(State _ conns arrival lanes) -> (State 0 [] arrival lanes, conns))

-- | Whether the pool has been closed.
isClosed :: Pool conn -> IO Bool
isClosed (Pool _ closed) = readIORef closed

-- | The connections that are free now.
free :: Pool conn -> IO [conn]
free (Pool ref _) = (\(State _ conns _ _) -> conns) <$> readIORef ref

-- | How many waiters are queued: one that has gone is counted until the pool reaches it, to give it an item.
waiters :: Pool conn -> IO Int
waiters (Pool ref _) = (\(State _ _ _ lanes) -> sum (Seq.length <$> lanes)) <$> readIORef ref

-- | The waiter queued, in the lane of those that need as many items free, with the number of its arrival.
queued :: Int -> (Int -> Waiter conn) -> State conn -> State conn
queued need make (State count conns arrival lanes) =
  State count conns (arrival + 1) (IntMap.alter (Just . maybe (Seq.singleton waiter) (|> waiter)) need lanes)
  where
    waiter = make arrival

-- | Serves the waiters that the free items can serve, and says what is taken for each: the oldest of the first waiters
-- of the lanes that need no more than are free, as long as there is one.
settle :: State conn -> (State conn, [Handoff conn])
settle state@(State _ _ _ lanes)
  | IntMap.null lanes = (state, [])
  | otherwise = go state []
  where
    go st@(State count conns arrival lanes') handed = case oldest count lanes' of
      Just (need, waiter, others)
        | Just (count', conns', handoff) <- serve count conns waiter ->
            go (State count' conns' arrival (if Seq.null others then IntMap.delete need lanes' else IntMap.insert need others lanes')) (handoff : handed)
      _ -> (st, reverse handed)
    serve count conns = \case
      One giver _ | conn : others <- conns -> Just (count - 1, others, OneTo giver conn)
      Many many giver _ | many <= count -> let (taken, others) = splitAt many conns in Just (count - many, others, ManyTo giver taken)
      _ -> Nothing

-- | The first waiter of the lane whose first waiter is the oldest, among the lanes that need no more items than are free.
oldest :: Int -> IntMap (Seq (Waiter conn)) -> Maybe (Int, Waiter conn, Seq (Waiter conn))
oldest count lanes = foldl' older Nothing (IntMap.toAscList (fst (IntMap.split (count + 1) lanes)))
  where
    older best (need, lane) = case viewl lane of
      waiter :< others -> case best of
        Just (_, earlier, _) | arrivalOf earlier < arrivalOf waiter -> best
        _ -> Just (need, waiter, others)
      EmptyL -> best
    arrivalOf = \case One _ arrival -> arrival; Many _ _ arrival -> arrival

-- | The items, free again, and what is taken for the waiters that can have them now.
putBack :: [conn] -> State conn -> (State conn, [Handoff conn])
putBack [conn] (State count conns arrival lanes)
  -- No lane needs as few items as were free, so one item that is back can only serve the lane that needs one more: its
  -- first waiter if it takes one item, which then has the very item, and the free items are as they were.
  | Just lane <- IntMap.lookup (count + 1) lanes,
    One giver _ :< others <- viewl lane =
      (State count conns arrival (if Seq.null others then IntMap.delete (count + 1) lanes else IntMap.insert (count + 1) others lanes), [OneTo giver conn])
putBack returned (State count conns arrival lanes) = settle (State (count + length returned) (returned ++ conns) arrival lanes)

-- | One item, if at least this many are free. It passes no waiter that could be served: none can be, or it would have been, so
-- a part that comes now with what is free is the oldest of those that can have it.
claimOne :: Int -> State conn -> (State conn, Either () conn)
claimOne need state@(State count conns arrival lanes) = case conns of
  conn : others | count >= need -> (State (count - 1) others arrival lanes, Right conn)
  _ -> (state, Left ())

-- | The same for this many items at once, when at least the number that is needed are free.
claimMany :: Int -> Int -> State conn -> (State conn, Either () [conn])
claimMany many need state@(State count conns arrival lanes)
  | count >= max many need = let (taken, others) = splitAt many conns in (State (count - many) others arrival lanes, Right taken)
  | otherwise = (state, Left ())

-- | What the item or the items are, if the claim finds them, and else the end to wait at, once the part's giver is queued. A ticket is
-- made only then, and the claim is made again with it: what is released between the look and the queueing is there for this
-- part to take. Asking is a point to give way at, as a link's receive is: a part that does nothing else than check
-- connections out and release them doesn't keep the others of its core from running.
reserved ::
  (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) =>
  IORef (State conn) ->
  Int ->
  (State conn -> (State conn, Either () taken)) ->
  (Giver 'Anywhere taken -> Int -> Waiter conn) ->
  arr () (Either (Waiting arr 'Anywhere taken) taken)
reserved ref need claim handout = giveWay >>> now (\() -> update ref claim) >>> (queue ||| arr Right)
  where
    queue = ticket >>> now (\(awaiting, giver) -> update ref (queueOrClaim awaiting giver))
    queueOrClaim awaiting giver state = case claim state of
      (claimed, Right taken) -> (claimed, Right taken)
      (_, Left ()) -> (queued need (handout giver) state, Left awaiting)

-- | A connection if one is free, or else the end to wait at for the one that
-- is released to this part: it queues the part's giver. The waiter can
-- 'abandon' it instead, which gives back a connection that came meanwhile.
reserve :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Pool conn -> arr () (Either (Waiting arr 'Anywhere conn) conn)
reserve = reserveAt 1

-- | One item if at least this many are free: a waiter for an item that leaves a number free for others, as a reserve is.
reserveAt :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Int -> Pool conn -> arr () (Either (Waiting arr 'Anywhere conn) conn)
reserveAt need (Pool ref _) = reserved ref need (claimOne need) One

-- | This many items at once, if at least the number needed are free: the waiter queues in the lane of what serves it, which
-- is never fewer than it takes.
reserveMany :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Int -> Int -> Pool conn -> arr () (Either (Waiting arr 'Anywhere [conn]) [conn])
reserveMany need many (Pool ref _) = reserved ref (max many need) (claimMany many need) (Many many)

-- | The item or the items once the part's ticket is given them, which the part waits at, and gives up its place at when it is
-- cancelled, so that what was given to it meanwhile goes back through the arrow.
waited :: (ArrowChoice arr, Effect arr, Exchange arr 'Anywhere, Scope arr) => arr taken () -> arr (Waiting arr 'Anywhere taken) taken
waited giveBack = scope returnA giveUp (arr fst >>> await)
  where
    giveUp = (\(awaiting, exit) -> case exit of Done -> Left (); _ -> Right awaiting) ^>> (returnA ||| (abandon >>> (maybe (Left ()) Right ^>> (returnA ||| giveBack))))

-- | A connection, once one is free: the first part to ask has the first that
-- is released. A part that is cancelled while it waits gives up its place, and
-- a connection that was given to it meanwhile goes back to the pool, so that
-- none is lost.
checkout :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool conn -> arr () conn
checkout = checkoutAt 1

-- | The same for an item that is only given when at least this many are free.
checkoutAt :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Int -> Pool conn -> arr () conn
checkoutAt need pool = reserveAt need pool >>> (waited (release pool) ||| returnA)

-- | The same for this many items at once, when at least the number needed are free.
checkoutMany :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Int -> Int -> Pool conn -> arr () [conn]
checkoutMany need many pool = reserveMany need many pool >>> (waited (now (releaseNow pool)) ||| returnA)

-- | The same as 'checkoutMany' for a pool that is only known when the program runs, which is the input: it gives the
-- pool again with the items, for the release. A part that is cancelled while it waits gives up its place, and the items
-- that were given to it meanwhile go back to the pool, as in 'checkoutMany'.
checkoutManyOf :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Int -> Int -> arr (Pool conn) (Pool conn, [conn])
checkoutManyOf need many = giveWay >>> now claim >>> (queue ||| returnA)
  where
    claim pool@(Pool ref _) = either (const (Left pool)) (Right . (pool,)) <$> update ref (claimMany many need)
    queue = ((),) ^>> first ticket >>> now (\((awaiting, giver), pool@(Pool ref _)) -> either (\waiting -> Left (pool, waiting)) (Right . (pool,)) <$> update ref (queueOrClaim awaiting giver)) >>> (waitedIn ||| returnA)
    queueOrClaim awaiting giver state = case claimMany many need state of
      (claimed, Right taken) -> (claimed, Right taken)
      (_, Left ()) -> (queued (max many need) (Many many giver) state, Left awaiting)
    waitedIn = scope returnA giveUp ((\((pool, waiting), _) -> (waiting, pool)) ^>> first await >>^ \(items, pool) -> (pool, items))
    giveUp = (\((pool, waiting), exit) -> case exit of Done -> Left (); _ -> Right (pool, waiting)) ^>> (returnA ||| ((\(pool, waiting) -> (waiting, pool)) ^>> first abandon >>> now (\(given, pool) -> maybe (pure ()) (releaseNow pool) given)))

-- | A connection, if one is free within the time, as 'checkout' waits for it: 'Nothing' if it isn't, once the
-- place in the queue is given up.
checkoutWithin :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr, Within arr) => Duration -> Pool conn -> arr () (Maybe conn)
checkoutWithin limit pool = within limit (checkout pool)

-- | An item if at least this many are free, without waiting: the part that takes it passes no waiter, for none that can be
-- served is queued.
takeNow :: Pool conn -> Int -> IO (Maybe conn)
takeNow (Pool ref _) need = either (const Nothing) Just <$> update ref (claimOne need)

-- | Gives the connection to the first part that waits for one. A waiter that
-- has gone doesn't take it, and the next is tried; with none left, the
-- connection is free.
release :: (Effect arr) => Pool conn -> arr conn ()
release pool = now (releaseNow pool . pure)

-- | Gives the items back to the pool: the oldest waiters that can have them are given them, and the rest are free. It
-- is one action that never waits, so a part that is cancelled can't lose an item between a waiter that has gone and the
-- next: it hands them over as the arrow's 'give' does it, but without its point to give way at.
releaseNow :: Pool conn -> [conn] -> IO ()
releaseNow (Pool ref _) conns = update ref (putBack conns) >>= handOver ref

-- | Queues the giver, in the lane of the waiters that need this many items free, for one item, which it is given at
-- once if it is the oldest of the waiters that can have an item. The giver says whether it took the item, so a giver can be
-- made of another, for a part that waits for the item or for something else.
queueNow :: Pool conn -> Int -> Giver 'Anywhere conn -> IO ()
queueNow (Pool ref _) need giver = update ref (settle . queued need (One giver)) >>= handOver ref

-- | Gives the items to the waiters they were taken for. A waiter that has gone doesn't take them, and they are put back, to
-- go to the next that can have them.
handOver :: IORef (State conn) -> [Handoff conn] -> IO ()
handOver _ [] = pure ()
handOver ref (handoff : others) =
  offer handoff >>= \case
    [] -> handOver ref others
    untaken -> update ref (putBack untaken) >>= \more -> handOver ref (more ++ others)
  where
    -- Gives them, and says what the waiter didn't take.
    offer = \case
      OneTo giver conn -> (\taken -> [conn | not taken]) <$> giveNow giver conn
      ManyTo giver conns -> (\taken -> if taken then [] else conns) <$> giveNow giver conns
