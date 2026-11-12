-- | The loans of a request, as programs use them: the buffers that the request
-- in flight works in, which a program gets as an input and borrows more from.
-- The loans are those of "Network.Streaming.BufferPool"; the steps that borrow
-- and give back are stubs, which fail with 'NotImplemented' until the pool's
-- waits are tickets.
module Network.Streaming.Loans
  ( Loans,
    Loan,
    loanBuffer,
    withRequestBuffers,
    borrow,
    release,
  )
where

import Network.Streaming.BufferPool (BufferAccess, BufferLoan, BufferPool, loanBuffer)
import Network.Streaming.Classes (Effect, Runtime)
import Network.Streaming.NotImplemented (notImplemented)

-- | What a request borrows buffers with: its primary buffer, and the right to
-- more.
type Loans = BufferAccess

-- | A buffer lent to the request, until it is released.
type Loan = BufferLoan

-- | The program as one request in flight: it is given its loans besides its
-- input, and has them until it ends.
withRequestBuffers :: (Runtime arr) => BufferPool -> arr (Loans, a) b -> arr a b
withRequestBuffers _ _ = notImplemented "withRequestBuffers"

-- | An extra loan, waiting while none is free.
borrow :: (Effect arr) => arr Loans Loan
borrow = notImplemented "borrow"

release :: (Effect arr) => arr Loan ()
release = notImplemented "release"
