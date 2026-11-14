{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}

-- | The loans of a request, as programs use them: the buffers that the request in flight works in, which a program
-- gets as an input and borrows more from, and the setup buffers that a connection is set up with. They are steps of
-- the arrow, and every wait of theirs is a ticket of the pool, so they run under any arrow that has them.
--
-- A request holds one primary buffer for its whole scope, which it was admitted for: it is admitted only when the
-- buffers that remain are as many as the reserve, so that the reserve is there for extra loans. It lends at most two
-- loans at a time: the primary, and one extra, which may come from the reserve and which the request gives back right
-- after use. A borrow that waits for an extra loan holds the extra place, so the reserve is asked for by no more than
-- one borrow of each request.
module Network.Streaming.Loans
  ( Loans,
    Loan,
    loanBuffer,
    withRequestBuffers,
    borrow,
    release,
    withSetupBuffers,
    withSetupBuffersOf,
  )
where

import Control.Arrow (first, returnA, (>>>), (^>>), (|||))
import Control.Exception (throwIO)
import Control.Monad (unless, when, (>=>))
import Data.Functor (($>))
import Data.IORef (IORef, newIORef)
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.BufferPool (BufferGrant)
import Network.Streaming.Classes
import Network.Streaming.Combinators (decide, done, keeping, step)
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.BufferPool (BufferPool (..))
import Network.Streaming.Internal.ListPrefix (ListPrefix (..))
import Network.Streaming.Internal.Tickets (Giver (..), giveNow)
import Network.Streaming.Pool (checkoutAt, checkoutMany, checkoutManyOf, queueNow, releaseNow, takeNow)

-- | What a request borrows buffers with: the pool, its primary buffer, and what is lent of them.
data Loans = Loans !BufferPool !Buffer !(IORef State)

-- | A buffer lent to the request, until it is released.
data Loan = Loan !Loans !Kind !Int !Buffer

data Kind = Primary | Extra

-- | What is lent: whether the request has ended, and if not, whether the primary is lent, what is in the extra place,
-- and the number of the issued loan or waiting borrow, which no other has had. The parts of a request may run on threads
-- of their own, so it changes by compare-and-swap.
data State = Ended | Open !Lent !Extra !Int

-- | The number of the loan that the primary is lent as.
data Lent = NotLent | Lent !Int

-- | The extra place is free, holds the extra loan of this number, or holds a borrow that waits for one, with its giver:
-- it is given the extra loan, or the primary if that is given back first, or, when the request ends, nothing.
data Extra = NoExtra | Held !Int !Buffer | Asked !Int !(Giver 'Anywhere (Maybe Loan))

closed, twoLoans :: IOError
closed = userError "request buffer access is closed"
twoLoans = userError "request already has two buffer loans"

loanBuffer :: Loan -> Buffer
loanBuffer (Loan _ _ _ buffer) = buffer

-- | The program as one request in flight: it is given its loans besides its input, and has them until it ends,
-- however it ends. Every loan is returned then, and every alias of a buffer is invalid. A request that has to wait to
-- be admitted gives up its place when it is cancelled, and a buffer that it was given meanwhile goes back.
withRequestBuffers :: (Runtime arr) => BufferPool -> arr (Loans, a) b -> arr a b
withRequestBuffers pool body =
  named (Label "withRequestBuffers" "Admits a request with its buffers, runs it, and gives them back however it ends.") $
    allowing
      WaitInAcquire
      ( "the acquire waits in the pool's checkout, which if it is cancelled gives up its place in the queue "
          ++ "and returns a buffer that came meanwhile, so nothing is held for the release to return"
      )
      (scope admit (fst ^>> ended) body)
  where
    admit =
      done
        >>> giving "primary buffer" (checkoutAt (1 + reserveCount pool) (requestBuffers pool))
        >>> giving
          "request loans"
          (step "open loans" (\buffer -> Loans pool buffer <$> newIORef (Open NotLent NoExtra 0)))

-- | The request has ended. What it holds goes back to the pool, once, and a borrow of it that still waits is told.
ended :: (Effect arr, Annotate arr) => arr Loans ()
ended =
  step "return buffers" $ \(Loans pool primary ref) -> do
    (buffers, asking) <- update ref (\case Ended -> (Ended, ([], Nothing)); Open _ extra _ -> (Ended, (primary : heldBy extra, askedBy extra)))
    releaseNow (requestBuffers pool) buffers
    mapM_ (`giveNow` Nothing) asking
  where
    heldBy = \case Held _ buffer -> [buffer]; _ -> []
    askedBy = \case Asked _ giver -> Just giver; _ -> Nothing

-- | An extra loan: the primary if it isn't lent, and else a buffer of the pool, which may be of its reserve, waiting
-- while none is free. It fails at once if the request has two loans, or has ended.
borrow :: (Runtime arr) => arr Loans Loan
borrow =
  named (Label "borrow" "Lends the primary buffer if it is free, and else waits for an extra loan.") $
    giving "loan free now?" (step "try borrow" immediately) >>> (taking "no" wait ||| taking "yes" returnA)

-- | The primary if it isn't lent. Or else, if the extra place is free, a buffer of the pool if one is free now, and
-- if none is, the loans, to wait for an extra loan with. No waiter that could have the buffer is queued, so it doesn't pass one.
immediately :: Loans -> IO (Either Loans Loan)
immediately loans@(Loans pool primary ref) = update ref transition >>= either throwIO (either takeFree (pure . Right))
  where
    transition Ended = (Ended, Left closed)
    transition state@(Open lent extra issued) = case lent of
      NotLent -> (Open (Lent issued) extra (issued + 1), Right (Right (Loan loans Primary issued primary)))
      Lent _ -> case extra of
        NoExtra -> (state, Right (Left loans))
        _ -> (state, Left twoLoans)
    -- The state may have changed meanwhile: the buffer goes back, and the borrow that waits decides again.
    takeFree _ =
      takeNow (requestBuffers pool) 1 >>= \case
        Nothing -> pure (Left loans)
        Just buffer ->
          update ref (hold buffer) >>= \case
            Just loan -> pure (Right loan)
            Nothing -> Left loans <$ releaseNow (requestBuffers pool) [buffer]
    hold buffer = \case
      Open lent@(Lent _) NoExtra issued -> (Open lent (Held issued buffer) (issued + 1), Just (Loan loans Extra issued buffer))
      state -> (state, Nothing)

-- | What a borrow has when it holds the extra place: the primary, which was given back as it came, or the end it waits at.
data Registration arr = Got !Loan | Queued !Loans !Int !(Waiting arr 'Anywhere (Maybe Loan))

-- | A borrow that waits for an extra loan: its loans and its number, the giver that the pool queues for it, and the end
-- it waits at.
data Asking arr = Asking
  { askLoans :: !Loans,
    askNumber :: !Int,
    askGiver :: !(Giver 'Anywhere (Maybe Loan)),
    askWaiting :: !(Waiting arr 'Anywhere (Maybe Loan))
  }

-- | A borrow that waits for an extra loan. It holds the extra place, and waits in the pool's queue for a buffer, and
-- at its request for the primary to be given back, whichever comes first, and gives up both when it is cancelled.
wait :: (Runtime arr) => arr Loans Loan
wait =
  named (Label "wait" "Holds the extra place and waits in the pool's queue for a buffer, and gives both up if cancelled.") $
    keeping ticket >>> scope (step "take extra place" register) giveUp body
  where
    body = decide "primary given back?" registered >>> (taking "no" queueAndAwait ||| taking "yes" returnA)
    registered (registration, ((_, giver), _)) = case registration of
      Got loan -> Right loan
      Queued loans number waiting -> Left (Asking loans number giver waiting)
    queueAndAwait = step "queue for a buffer" queueing >>> await >>> step "take lent loan" (maybe (throwIO closed) pure)
    -- The pool queues a giver of a buffer, which lends the buffer to this borrow if it is still there to take it.
    queueing Asking {askLoans = loans@(Loans pool _ _), askNumber, askGiver, askWaiting} =
      askWaiting <$ queueNow (requestBuffers pool) 1 (forwarding loans askNumber askGiver)
    giveUp = decide "give up place?" stillQueued >>> (taking "no" returnA ||| taking "yes" abandoned)
    stillQueued (registration, exit) = case registration of
      Queued loans number waiting | not (isDone exit) -> Right (waiting, (loans, number))
      _ -> Left ()
    isDone = \case Done -> True; _ -> False
    -- A loan that was given before the borrow gave up its place is the borrow's, and goes back.
    abandoned =
      first abandon
        >>> decide "loan already given?" givenLoan
        >>> (taking "no" (step "free extra place" (uncurry withdraw)) ||| taking "yes" release)
    givenLoan (given, (loans, number)) = case given of
      Just (Just loan) -> Right loan
      _ -> Left (loans, number)

-- | Takes the extra place, or the primary if it was given back meanwhile.
register :: ((Waiting arr 'Anywhere (Maybe Loan), Giver 'Anywhere (Maybe Loan)), Loans) -> IO (Registration arr)
register ((waiting, giver), loans@(Loans _ primary ref)) = update ref transition >>= either throwIO pure
  where
    transition Ended = (Ended, Left closed)
    transition state@(Open lent extra issued) = case lent of
      NotLent -> (Open (Lent issued) extra (issued + 1), Right (Got (Loan loans Primary issued primary)))
      Lent _ -> case extra of
        NoExtra -> (Open lent (Asked issued giver) (issued + 1), Right (Queued loans issued waiting))
        _ -> (state, Left twoLoans)

-- | Frees the extra place of a borrow that gives up, if it still holds it.
withdraw :: Loans -> Int -> IO ()
withdraw (Loans _ _ ref) number = update ref $ \case
  Open lent (Asked asked _) issued | asked == number -> (Open lent NoExtra issued, ())
  state -> (state, ())

-- | The giver that the pool keeps for a borrow that waits. Given a buffer, it lends it to the borrow, as the extra loan, if the
-- borrow still holds the extra place; and it says that it didn't take it if the borrow was given the primary, or has gone,
-- so that the buffer is the pool's again. A request that has ended meanwhile has returned the buffer itself, which the
-- pool must not do again, so then it says that it took it.
forwarding :: Loans -> Int -> Giver 'Anywhere (Maybe Loan) -> Giver 'Anywhere Buffer
forwarding loans@(Loans _ _ ref) number (AnywhereGiver giver) = AnywhereGiver $ \buffer ->
  update ref (claim buffer) >>= \case
    Nothing -> pure False
    Just loan@(Loan _ _ lent _) -> do
      taken <- giver (Just loan)
      if taken then pure True else not <$> update ref (unclaim lent)
  where
    claim buffer = \case
      Open primary (Asked asked _) issued | asked == number -> (Open primary (Held issued buffer) (issued + 1), Just (Loan loans Extra issued buffer))
      state -> (state, Nothing)
    unclaim lent = \case
      Open primary (Held held _) issued | held == lent -> (Open primary NoExtra issued, True)
      state -> (state, False)

-- | What giving a loan back leaves to do: nothing, which is so for a loan that has ended or is stale; the primary to
-- hand to the borrow that waits, which is the request's own loan again; or a buffer to return to the pool.
data Returned = Settled | Deliver !(Giver 'Anywhere (Maybe Loan)) !Loan | Back !Loans !Buffer

-- | Gives the loan back. A loan that has ended, or is already back, is ignored. The primary goes to the borrow that waits
-- for an extra loan, if there is one, and an extra loan goes back to the pool, and on to the oldest waiter that can have it.
-- It is one action that never waits, so it is not cancelled half way.
release :: (Effect arr, Annotate arr) => arr Loan ()
release =
  step "release loan" $
    returned >=> \case
      Settled -> pure ()
      -- The primary is lent as a new loan, which is undone if the borrow that waited for it has gone.
      Deliver giver loan -> giveNow giver (Just loan) >>= \taken -> unless taken (undeliver loan)
      Back (Loans pool _ _) buffer -> releaseNow (requestBuffers pool) [buffer]

returned :: Loan -> IO Returned
returned (Loan loans@(Loans _ primary ref) kind number buffer) = update ref transition
  where
    transition = \case
      Open (Lent lent) extra issued
        | Primary <- kind,
          lent == number ->
            case extra of
              Asked _ giver -> (Open (Lent issued) NoExtra (issued + 1), Deliver giver (Loan loans Primary issued primary))
              _ -> (Open NotLent extra issued, Settled)
      Open lent (Held held _) issued | Extra <- kind, held == number -> (Open lent NoExtra issued, Back loans buffer)
      state -> (state, Settled)

-- | The primary is not lent as this loan, for a borrow that has gone.
undeliver :: Loan -> IO ()
undeliver (Loan (Loans _ _ ref) _ number _) = update ref $ \case
  Open (Lent lent) extra issued | lent == number -> (Open NotLent extra issued, ())
  state -> (state, ())

-- | The program with setup buffers, granted all at once from the pool's own inventory for them, which requests
-- don't draw on: for setting up a connection. They are given back when the program ends, however it ends. A grant
-- that no setup could ever have fails at once, and the others wait in the pool's queue, as the grants of the same size
-- in order, and a smaller one that fits goes before a larger one that doesn't.
withSetupBuffers :: forall grant arr a b. (Runtime arr, BufferGrant grant) => BufferPool -> arr (grant, a) b -> arr a b
withSetupBuffers pool = scope acquire (step "return setup buffers" (\(grant, _) -> releaseNow (setupBuffers pool) (prepend grant [])))
  where
    size = prefixSize @grant
    acquire =
      step "check grant fits" (const (when (size > setupCount pool) (ioError (userError "buffer count must fit capacity"))))
        >>> checkoutMany size size (setupBuffers pool)
        >>> step
          "split grant"
          (\buffers -> maybe (ioError (userError "a grant of the wrong number of buffers")) (pure . fst) (takePrefix buffers :: Maybe (grant, [Buffer])))

-- | The same, for the setup buffers of the pool that the loans in the input belong to: a program that is built without
-- a pool, as a connection's setup is, and is run in whichever request it is called in. The program takes the loans
-- besides its input, after the grant.
withSetupBuffersOf :: forall grant arr a b. (Runtime arr, BufferGrant grant) => arr (grant, (Loans, a)) b -> arr (Loans, a) b
withSetupBuffersOf body =
  scope acquire (step "return setup buffers" (\((setups, grant), _) -> releaseNow setups (prepend grant []))) (first snd ^>> body)
  where
    size = prefixSize @grant
    acquire =
      step
        "check grant fits"
        (\(Loans pool _ _, _) -> when (size > setupCount pool) (ioError (userError "buffer count must fit capacity")) $> setupBuffers pool)
        >>> checkoutManyOf size size
        >>> step
          "split grant"
          (\(setups, buffers) -> maybe (ioError (userError "a grant of the wrong number of buffers")) (pure . (setups,) . fst) (takePrefix buffers :: Maybe (grant, [Buffer])))
