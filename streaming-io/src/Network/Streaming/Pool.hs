{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- | A pool of connections, or of anything else that is loaned, as library code on the primitives of the classes
-- alone: one atomically updated reference to the items that are free and to the 'Promise's of the parts that wait
-- for some, and an arrow's tickets of reach 'Anywhere for the waiting: the pool is shared by the parts of every
-- core, so the promises it keeps can't belong to one. It is written once, for any arrow that has them: parts on the
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
    withLoanIn,
    withLoanWithinIn,
    urgently,
    newPool,
    closePool,
    closePoolWith,
    checkout,
    checkoutWithin,
    checkoutIn,
    reserve,
    release,
    offer,
    free,
    checkoutAt,
    checkoutMany,
    checkoutManyOf,
    takeNow,
    takeNowWith,
    releaseNow,
    queueNow,
    waiters,
  )
where

import Control.Arrow (ArrowChoice, arr, first, returnA, second, (>>>), (>>^), (^>>), (|||))
import Control.Exception (Exception, IOException, catch)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.Sequence (Seq, ViewL (..), ViewR (..), viewl, viewr, (<|), (|>))
import qualified Data.Sequence as Seq
import Data.Tuple (swap)
import Network.Streaming.Classes
import Network.Streaming.Combinators (alongside, boolA, decide, done, keeping, step)
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.Tickets (fulfilNow)

-- | The items that are free, and how many, the number that the next waiter to queue gets, which says which of two
-- waiters came first, and the waiters in their lanes, by how many items must be free to serve the first of the lane.
-- After every change no lane's first waiter can be served: it needs more items than are free.
data State conn = State !Int ![conn] !Int !(IntMap (Seq (Waiter conn)))

-- | The items and the waiters, and whether the pool has been closed.
data Pool conn = Pool !(IORef (State conn)) !(IORef Bool)

-- | A waiter, by the ticket's promise that is fulfilled with its items, for one item or for a number of them at once, and by when it
-- came: the number that the pool counts arrivals with. A closed pool has a last waiter that takes every item that nobody who waited
-- before it takes, and disposes of it: it stays at the end of its lane, and those that queue after it are put before it.
data Waiter conn
  = One !(Promise 'Anywhere conn) !Int
  | Many !Int !(Promise 'Anywhere [conn]) !Int
  | Disposal (conn -> IO ()) !Int

-- | The items that were taken from the free ones for a waiter, for its promise to be fulfilled with, or for a closed pool to dispose of.
data Handoff conn
  = OneTo !(Promise 'Anywhere conn) conn
  | ManyTo !(Promise 'Anywhere [conn]) [conn]
  | Dispose (conn -> IO ()) conn

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
-- once its place in the queue is given up. The deadline is armed only if the
-- checkout has to wait: an item that is free is lent as 'withLoan' lends it.
withLoanWithin :: (Runtime arr) => Duration -> Pool c -> arr (c, a) b -> arr a (Either PoolError b)
withLoanWithin limit pool = loaned pool (checkoutWithin limit pool)

-- | The same for a pool that is the arrow's input, with the input of the body after it: a program that is built once serves any
-- pool, as the pools of tenants are.
withLoanIn :: (Runtime arr) => arr (c, a) b -> arr (Pool c, a) (Either PoolError b)
withLoanIn = loanedIn (checkoutIn >>^ Right)

-- | The same, but the checkout gives up after the time, as 'withLoanWithin' does.
withLoanWithinIn :: (Runtime arr) => Duration -> arr (c, a) b -> arr (Pool c, a) (Either PoolError b)
withLoanWithinIn limit = loanedIn ((1,) ^>> checkoutAtWithin limit >>^ maybe (Left CheckoutTimedOut) Right)

-- | The scope around a checkout, which is the arrow that gives the item or, if
-- its deadline passed, nothing, and the release of what it gave.
loaned :: (Runtime arr) => Pool c -> arr () (Maybe c) -> arr (c, a) b -> arr a (Either PoolError b)
loaned pool checkingOut body = scope acquire giveBack run
  where
    acquire =
      now (const (isClosed pool))
        >>> boolA (checkingOut >>^ maybe (Left CheckoutTimedOut) Right) (arr (const (Left PoolClosed)))
    giveBack = fst ^>> (done ||| release pool)
    run = withInput ^>> (arr Left ||| (body >>^ Right))
    withInput (acquired, input) = fmap (,input) acquired

-- | 'loaned' for a pool that is the arrow's input: the arrow that gives the item takes the pool, and the release takes it
-- again, with the item.
loanedIn :: (Runtime arr) => arr (Pool c) (Either PoolError c) -> arr (c, a) b -> arr (Pool c, a) (Either PoolError b)
loanedIn checkingOut body = scope acquire giveBack run
  where
    acquire =
      now (\(pool, _) -> (\closed -> if closed then Left () else Right pool) <$> isClosed pool)
        >>> (taking "closed" (arr (const (Left PoolClosed))) ||| taking "open" (alongside checkingOut >>^ \(got, pool) -> fmap (pool,) got))
    giveBack = fst ^>> (done ||| offer)
    run = withInput ^>> (arr Left ||| (body >>^ Right))
    withInput (acquired, (_, input)) = fmap (\(_, item) -> (item, input)) acquired

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
-- keeps waiting. 'closePoolWith' disposes of the items that come back.
closePool :: Pool conn -> IO [conn]
closePool (Pool ref closed) = writeIORef closed True *> update ref (\(State _ conns arrival lanes) -> (State 0 [] arrival lanes, conns))

-- | Closes the pool, and has the function dispose of the items that are free now, and of each item that is given back
-- after, as the last loans end: a pool that is closed lends nothing, so 'withLoan' fails with 'PoolClosed', and keeps no
-- item free. A part that waits for an item when the pool closes is served first, by the items that come back, and its
-- item is disposed of when it gives it back. The disposal runs in the part that gives the item back, and must be
-- quick; a failure of it to close something, an 'IOException', is ignored. Closing a pool again changes nothing.
closePoolWith :: (conn -> IO ()) -> Pool conn -> IO ()
closePoolWith dispose (Pool ref closed) = writeIORef closed True *> update ref disposing >>= handOver ref
  where
    disposing state@(State _ _ _ lanes)
      | disposesIn lanes = (state, [])
      | otherwise = settle (queued 1 (Disposal dispose) state)

-- | Whether the pool has been closed.
isClosed :: Pool conn -> IO Bool
isClosed (Pool _ closed) = readIORef closed

-- | The connections that are free now.
free :: Pool conn -> IO [conn]
free (Pool ref _) = (\(State _ conns _ _) -> conns) <$> readIORef ref

-- | How many waiters are queued: one that has gone is counted until the pool reaches it, to give it an item. The disposal of a closed
-- pool is not one.
waiters :: Pool conn -> IO Int
waiters (Pool ref _) = (\(State _ _ _ lanes) -> sum (waitersOf <$> lanes)) <$> readIORef ref
  where
    waitersOf lane = if endsWithDisposal lane then Seq.length lane - 1 else Seq.length lane

-- | The waiter queued, in the lane of those that need as many items free, with the number of its arrival.
queued :: Int -> (Int -> Waiter conn) -> State conn -> State conn
queued need make (State count conns arrival lanes) =
  State count conns (arrival + 1) (IntMap.alter (Just . maybe (Seq.singleton waiter) enqueue) need lanes)
  where
    waiter = make arrival
    -- The disposal of a closed pool stays last: a waiter that comes after the pool closed is served before it.
    enqueue lane = case viewr lane of
      before :> disposal@(Disposal _ _) -> (before |> waiter) |> disposal
      _ -> lane |> waiter

-- | Whether the lane ends with the disposal of a closed pool, and whether the pool disposes of its items.
endsWithDisposal :: Seq (Waiter conn) -> Bool
endsWithDisposal lane = case viewr lane of
  _ :> Disposal _ _ -> True
  _ -> False

disposesIn :: IntMap (Seq (Waiter conn)) -> Bool
disposesIn lanes = maybe False endsWithDisposal (IntMap.lookup 1 lanes)

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
            go (State count' conns' arrival (remaining need waiter others lanes')) (handoff : handed)
      _ -> (st, reverse handed)
    serve count conns = \case
      One promise _ | conn : others <- conns -> Just (count - 1, others, OneTo promise conn)
      Many many promise _ | many <= count -> let (taken, others) = splitAt many conns in Just (count - many, others, ManyTo promise taken)
      Disposal dispose _ | conn : others <- conns -> Just (count - 1, others, Dispose dispose conn)
      _ -> Nothing
    -- The lanes once the waiter that was served has left its lane: a disposal doesn't, as it serves every item.
    remaining need waiter others lanes' = case waiter of
      Disposal _ _ -> IntMap.insert need (waiter <| others) lanes'
      _ -> if Seq.null others then IntMap.delete need lanes' else IntMap.insert need others lanes'

-- | The first waiter of the lane whose first waiter is the oldest, among the lanes that need no more items than are free.
oldest :: Int -> IntMap (Seq (Waiter conn)) -> Maybe (Int, Waiter conn, Seq (Waiter conn))
oldest count lanes = foldl' older Nothing (IntMap.toAscList (fst (IntMap.split (count + 1) lanes)))
  where
    older best (need, lane) = case viewl lane of
      waiter :< others -> case best of
        Just (_, earlier, _) | arrivalOf earlier < arrivalOf waiter -> best
        _ -> Just (need, waiter, others)
      EmptyL -> best
    arrivalOf = \case One _ arrival -> arrival; Many _ _ arrival -> arrival; Disposal _ arrival -> arrival

-- | The items, free again, and what is taken for the waiters that can have them now.
putBack :: [conn] -> State conn -> (State conn, [Handoff conn])
putBack [conn] (State count conns arrival lanes)
  -- No lane needs as few items as were free, so one item that is back can only serve the lane that needs one more: its
  -- first waiter if it takes one item, which then has the very item, and the free items are as they were.
  | Just lane <- IntMap.lookup (count + 1) lanes,
    One promise _ :< others <- viewl lane =
      (State count conns arrival (if Seq.null others then IntMap.delete (count + 1) lanes else IntMap.insert (count + 1) others lanes), [OneTo promise conn])
putBack returned (State count conns arrival lanes) = settle (State (count + length returned) (returned ++ conns) arrival lanes)

-- | One item, if at least this many are free. It passes no waiter that could be served: none can be, or it would have been, so
-- a part that comes now with what is free is the oldest of those that can have it.
claimOne :: Int -> State conn -> (State conn, Either () conn)
claimOne need state@(State count conns arrival lanes) = case conns of
  conn : others | count >= need -> (State (count - 1) others arrival lanes, Right conn)
  _ -> (state, Left ())

-- | The item at the position that the chooser gives in the free items, which are in the order that they are taken in.
-- Nothing is taken if it gives none, or a position that is not one of the items. It passes no waiter that could be served,
-- as 'claimOne' doesn't.
claimAt :: ([conn] -> Maybe Int) -> State conn -> (State conn, Either () conn)
claimAt choose state@(State count conns arrival lanes) = case choose conns of
  Just position | position >= 0, (before, conn : after) <- splitAt position conns -> (State (count - 1) (before ++ after) arrival lanes, Right conn)
  _ -> (state, Left ())

-- | The same for this many items at once, when at least the number that is needed are free.
claimMany :: Int -> Int -> State conn -> (State conn, Either () [conn])
claimMany many need state@(State count conns arrival lanes)
  | count >= max many need = let (taken, others) = splitAt many conns in (State (count - many) others arrival lanes, Right taken)
  | otherwise = (state, Left ())

-- | What the item or the items are, if the claim finds them, and else the future to await, once the part's promise is queued. A ticket is
-- made only then, and the claim is made again with it: what is released between the look and the queueing is there for this
-- part to take. Asking is a point to give way at, as a link's receive is: a part that does nothing else than check
-- connections out and release them doesn't keep the others of its core from running.
reserved ::
  (ArrowChoice arr, Effect arr, GiveWay arr, Annotate arr, Exchange arr 'Anywhere) =>
  IORef (State conn) ->
  Int ->
  (State conn -> (State conn, Either () taken)) ->
  (Promise 'Anywhere taken -> Int -> Waiter conn) ->
  arr () (Either (Future arr 'Anywhere taken) taken)
reserved ref need claim handout =
  named (Label "reserved" "Takes what is free if enough is, and else queues the promise and returns the future to await.") $
    giveWay
      >>> giving "item free?" (step "claim from free items" (const (update ref claim)))
      >>> (taking "no" queue ||| taking "yes" (arr Right))
  where
    queue = ticket >>> step "queue promise" (\(awaiting, promise) -> update ref (queueOrClaim awaiting promise))
    queueOrClaim awaiting promise state = case claim state of
      (claimed, Right taken) -> (claimed, Right taken)
      (_, Left ()) -> (queued need (handout promise) state, Left awaiting)

-- | A connection if one is free, or else the future to await for the one that
-- is released to this part: it queues the part's promise. The waiter can
-- 'abandon' it instead, which gives back a connection that came meanwhile.
reserve :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Pool conn -> arr () (Either (Future arr 'Anywhere conn) conn)
reserve = reserveAt 1

-- | One item if at least this many are free: a waiter for an item that leaves a number free for others, as a reserve is.
reserveAt :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Int -> Pool conn -> arr () (Either (Future arr 'Anywhere conn) conn)
reserveAt need (Pool ref _) = reserved ref need (claimOne need) One

-- | This many items at once, if at least the number needed are free: the waiter queues in the lane of what serves it, which
-- is never fewer than it takes.
reserveMany :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Int -> Int -> Pool conn -> arr () (Either (Future arr 'Anywhere [conn]) [conn])
reserveMany need many (Pool ref _) = reserved ref (max many need) (claimMany many need) (Many many)

-- | The item or the items once the part's promise is fulfilled with them, which the part awaits, and gives up its place when it is
-- cancelled, so that what was fulfilled meanwhile goes back through the arrow. The pool that they are of is in the
-- input, with the future, and the arrow that gives back takes it with them.
waited :: (ArrowChoice arr, Annotate arr, Effect arr, Exchange arr 'Anywhere, Scope arr) => arr (Pool conn, taken) () -> arr (Pool conn, Future arr 'Anywhere taken) taken
waited giveBack =
  named (Label "waited" "Waits for the promise of the item to be fulfilled, and gives up the place in the queue if cancelled meanwhile.") $
    scope returnA giveUp (fst ^>> second await >>^ snd)
  where
    giveUp =
      decide "got its item?" unlessDone
        >>> ( taking "yes" returnA
                ||| taking
                  "no"
                  ( swap
                      ^>> first abandon
                      >>> decide "fulfilled meanwhile?" (\(fulfilled, pool) -> maybe (Left ()) (Right . (pool,)) fulfilled)
                      >>> (taking "no" returnA ||| taking "yes" giveBack)
                  )
            )

-- | The input of a release, which it has to give up, unless the body of the scope completed: then there is nothing to give up,
-- and it is 'Left'.
unlessDone :: (a, Exit) -> Either () a
unlessDone (input, exit) = case exit of Done -> Left (); _ -> Right input

-- | A connection, once one is free: the first part to ask has the first that
-- is released. A part that is cancelled while it waits gives up its place, and
-- a connection that was given to it meanwhile goes back to the pool, so that
-- none is lost.
checkout :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool conn -> arr () conn
checkout pool = const (1, pool) ^>> checkoutAt

-- | The same for an item that is only given when at least this many are free, which is in the input with the pool: a
-- part that is cancelled while it waits gives up its place, and an item that was given to it meanwhile goes back to the
-- pool.
checkoutAt :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => arr (Int, Pool conn) conn
checkoutAt =
  named (Label "checkoutAt" "Takes an item of the pool if enough are free, and else queues for one and waits.") $
    giveWay
      >>> giving "item free?" (step "claim from free items" claimAtNow)
      >>> (taking "no" queueAt ||| taking "yes" returnA)

-- | The same, but the checkout gives up after the time, once its place in the queue is given up: 'Nothing'. The deadline
-- is armed when the checkout has to wait, and an item that is free is taken without it.
checkoutAtWithin :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr, Within arr) => Duration -> arr (Int, Pool conn) (Maybe conn)
checkoutAtWithin limit =
  named (Label "checkoutAtWithin" "Takes an item of the pool if enough are free, and else queues for one and waits for it until the deadline.") $
    giveWay
      >>> giving "item free?" (step "claim from free items" claimAtNow)
      >>> (taking "no" (within limit queueAt) ||| taking "yes" (arr Just))

-- | The item, if at least this many are free, and else the input again.
claimAtNow :: (Int, Pool conn) -> IO (Either (Int, Pool conn) conn)
claimAtNow input@(need, Pool ref _) = either (const (Left input)) Right <$> update ref (claimOne need)

-- | Queues for an item that is only given when at least this many are free, and waits for it: the part that is cancelled
-- while it waits gives up its place, and an item that was given to it meanwhile goes back to the pool. The item that
-- was released between the first look and the queueing is taken at once.
queueAt :: (ArrowChoice arr, Annotate arr, Effect arr, Exchange arr 'Anywhere, Scope arr) => arr (Int, Pool conn) conn
queueAt =
  keeping ticket
    >>> giving "item free?" (step "queue promise" queueing)
    >>> ( taking "no" (waited (step "return item" (\(pool, item) -> releaseNow pool [item])))
            ||| taking "yes" returnA
        )
  where
    queueing ((awaiting, promise), (need, pool@(Pool ref _))) =
      either (Left . (pool,)) Right <$> update ref (queueOrClaim need awaiting promise)
    queueOrClaim need awaiting promise state = case claimOne need state of
      (claimed, Right taken) -> (claimed, Right taken)
      (_, Left ()) -> (queued need (One promise) state, Left awaiting)

-- | The same for this many items at once, when at least the number needed are free.
checkoutMany :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Int -> Int -> Pool conn -> arr () [conn]
checkoutMany need many pool = reserveMany need many pool >>> (((pool,) ^>> waited (now (uncurry releaseNow))) ||| returnA)

-- | The same as 'checkoutMany' for a pool that is only known when the program runs, which is the input: it gives the
-- pool again with the items, for the release. A part that is cancelled while it waits gives up its place, and the items
-- that were given to it meanwhile go back to the pool, as in 'checkoutMany'.
checkoutManyOf :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Int -> Int -> arr (Pool conn) (Pool conn, [conn])
checkoutManyOf need many = giveWay >>> now claim >>> (queue ||| returnA)
  where
    claim pool@(Pool ref _) = either (const (Left pool)) (Right . (pool,)) <$> update ref (claimMany many need)
    queue =
      keeping ticket
        >>> now (\((awaiting, promise), pool@(Pool ref _)) -> either (\future -> Left (pool, future)) (Right . (pool,)) <$> update ref (queueOrClaim awaiting promise))
        >>> (waitedIn ||| returnA)
    queueOrClaim awaiting promise state = case claimMany many need state of
      (claimed, Right taken) -> (claimed, Right taken)
      (_, Left ()) -> (queued (max many need) (Many many promise) state, Left awaiting)
    waitedIn = scope returnA giveUp (swap . fst ^>> first await >>^ swap)
    giveUp =
      unlessDone
        ^>> (returnA ||| (swap ^>> first abandon >>> now (\(fulfilled, pool) -> maybe (pure ()) (releaseNow pool) fulfilled)))

-- | An item of the pool that is the arrow's input, once one is free, as 'checkout' waits for it.
checkoutIn :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => arr (Pool conn) conn
checkoutIn = (1,) ^>> checkoutAt

-- | A connection, if one is free within the time, as 'checkout' waits for it: 'Nothing' if it isn't, once the
-- place in the queue is given up. The deadline is armed only if the checkout has to wait.
checkoutWithin :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr, Within arr) => Duration -> Pool conn -> arr () (Maybe conn)
checkoutWithin limit pool = const (1, pool) ^>> checkoutAtWithin limit

-- | An item if at least this many are free, without waiting: the part that takes it passes no waiter, for none that can be
-- served is queued.
takeNow :: Pool conn -> Int -> IO (Maybe conn)
takeNow (Pool ref _) need = either (const Nothing) Just <$> update ref (claimOne need)

-- | An item that the caller chooses, if one is free, without waiting: the chooser is given the free items in the order that
-- the pool takes them in, and says the position of the one to take. With 'Nothing', or a position that is not one of the
-- items, none is taken. The chooser runs again if another part changes the pool meanwhile, so it must be quick. As 'takeNow'
-- does, it passes no waiter that could be served: none can be, or it would have been. A pool that has been closed lends
-- nothing.
takeNowWith :: ([conn] -> Maybe Int) -> Pool conn -> IO (Maybe conn)
takeNowWith choose pool@(Pool ref _) = isClosed pool >>= \closed -> if closed then pure Nothing else either (const Nothing) Just <$> update ref (claimAt choose)

-- | Gives the connection to the first part that waits for one. A waiter that
-- has gone doesn't take it, and the next is tried; with none left, the
-- connection is free.
release :: (Effect arr, Annotate arr) => Pool conn -> arr conn ()
release pool = step "return item" (releaseNow pool . pure)

-- | Gives the item back to the pool that is the arrow's input, as 'release' does.
offer :: (Effect arr, Annotate arr) => arr (Pool conn, conn) ()
offer = step "return item" (\(pool, conn) -> releaseNow pool [conn])

-- | Gives the items back to the pool: the oldest waiters that can have them are given them, and the rest are free. It
-- is one action that never waits, so a part that is cancelled can't lose an item between a waiter that has gone and the
-- next: it hands them over as the arrow's 'fulfil' does it, but without its point to give way at.
releaseNow :: Pool conn -> [conn] -> IO ()
releaseNow (Pool ref _) conns = update ref (putBack conns) >>= handOver ref

-- | Queues the promise, in the lane of the waiters that need this many items free, for one item. It is fulfilled at
-- once if it is the oldest of the waiters that can have an item. The promise says whether it took the item, so a promise can be
-- made of another, for a part that waits for the item or for something else.
queueNow :: Pool conn -> Int -> Promise 'Anywhere conn -> IO ()
queueNow (Pool ref _) need promise = update ref (settle . queued need (One promise)) >>= handOver ref

-- | Gives the items to the waiters they were taken for. A waiter that has gone doesn't take them, and they are put back, to
-- go to the next that can have them.
handOver :: IORef (State conn) -> [Handoff conn] -> IO ()
handOver _ [] = pure ()
handOver ref (handoff : others) =
  giveTo handoff >>= \case
    [] -> handOver ref others
    untaken -> update ref (putBack untaken) >>= \more -> handOver ref (more ++ others)
  where
    -- Gives them, and says what the waiter didn't take.
    giveTo = \case
      OneTo promise conn -> (\taken -> [conn | not taken]) <$> fulfilNow promise conn
      ManyTo promise conns -> (\taken -> if taken then [] else conns) <$> fulfilNow promise conns
      Dispose dispose conn -> [] <$ dispose conn `catch` \(_ :: IOException) -> pure ()
