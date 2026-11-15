{-# LANGUAGE PatternSynonyms #-}

-- | Helpers that route values through any arrow and need nothing of the capability classes but the annotations of
-- diagrams, so that the classes can use them too. "Network.Streaming.Combinators" re-exports them. All are INLINE, for
-- the reason given there.
module Network.Streaming.Plumbing
  ( done,
    dup,
    whether,
    decide,
    alongside,
    keeping,
    tap,
    consume,
    through,
    _1,
    _2,
    on,
    boolA,
    Step,
    pattern Again,
    pattern Finish,
    iterateA,
    iterateWith,
    foreverA,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, second, (>>>), (>>^), (^>>), (|||))
import Data.Bool (bool)
import Network.Streaming.Annotate (Annotate, giving, taking)

-- | What a step of a loop gives: 'Again' with the state to go round with, or 'Finish' with the result. It is 'Either',
-- so 'Control.Arrow.left', '+++' and '|||' work on it.
type Step s r = Either s r

-- | The loop goes round again, with this state.
pattern Again :: s -> Step s r
pattern Again s = Left s

-- | The loop ends, with this result.
pattern Finish :: r -> Step s r
pattern Finish r = Right r

{-# COMPLETE Again, Finish #-}

-- | Runs the step until it finishes: each 'Again' goes round again with its state.
iterateA :: (ArrowChoice arr, Annotate arr) => arr s (Step s r) -> arr s r
iterateA step = iterateWith step returnA
{-# INLINE iterateA #-}

-- | Runs the body until it finishes. On 'Again', runs the second arrow on what the body gave, and goes round again
-- with its result. In diagrams the two branches are named "again" and "finish".
iterateWith :: (ArrowChoice arr, Annotate arr) => arr s (Step t r) -> arr t s -> arr s r
iterateWith body next = loop
  where
    loop = body >>> (taking "again" (next >>> loop) ||| taking "finish" returnA)
{-# INLINE iterateWith #-}

-- | Runs the step for ever, each time on what it gave the time before.
foreverA :: (Arrow arr) => arr a a -> arr a b
foreverA step = loop
  where
    loop = step >>> loop
{-# INLINE foreverA #-}

-- | Ignores the input.
done :: (Arrow arr) => arr a ()
done = arr (const ())
{-# INLINE done #-}

-- | The value twice.
dup :: a -> (a, a)
dup a = (a, a)
{-# INLINE dup #-}

-- | The value, on the Left if the test holds for it and on the Right if not, so that @whether test ^>> (yes ||| no)@
-- reads in the order of @if@.
whether :: (a -> Bool) -> a -> Either a a
whether test a = if test a then Left a else Right a
{-# INLINE whether #-}

-- | A pure step that chooses a branch, named by the question it answers.
decide :: (Arrow arr, Annotate arr) => String -> (a -> Either b c) -> arr a (Either b c)
decide question = giving question . arr
{-# INLINE decide #-}

-- | Runs the arrow on the input, and pairs its result with the input, unchanged.
alongside :: (Arrow arr) => arr a b -> arr a (b, a)
alongside step = dup ^>> first step
{-# INLINE alongside #-}

-- | Runs an arrow that takes no input, and pairs its result with the input, unchanged: 'alongside' for an arrow that
-- doesn't read the input.
keeping :: (Arrow arr) => arr () b -> arr c (b, c)
keeping act = ((),) ^>> first act
{-# INLINE keeping #-}

-- | Runs the arrow on the input for its effect, and passes the input on unchanged.
tap :: (Arrow arr) => arr a () -> arr a a
tap step = alongside step >>^ snd
{-# INLINE tap #-}

-- | Runs the arrow on the first of the pair for its effect, and passes the second on.
consume :: (Arrow arr) => arr a () -> arr (a, c) c
consume step = first step >>^ snd
{-# INLINE consume #-}

-- | Runs the arrow on the part that the lens picks. A lens is a function from an arrow on a part to an arrow on the
-- whole, such as '_1', '_2' or a composition of them like @_2 . _1@.
through :: (arr a b -> arr s t) -> arr a b -> arr s t
through l = l
{-# INLINE through #-}

-- | The first of a pair, as a lens.
_1 :: (Arrow arr) => arr a b -> arr (a, c) (b, c)
_1 = first
{-# INLINE _1 #-}

-- | The second of a pair, as a lens.
_2 :: (Arrow arr) => arr a b -> arr (c, a) (c, b)
_2 = second
{-# INLINE _2 #-}

-- | Runs the arrow on the part that the lens picks, and ignores the rest.
on :: (Arrow arr) => (arr a () -> arr s t) -> arr a () -> arr s ()
on l p = l p >>> done
{-# INLINE on #-}

-- | Runs the first arrow if the input is 'False', and the second if it is 'True', like 'bool'. In diagrams the two
-- branches are named "false" and "true".
boolA :: (ArrowChoice arr, Annotate arr) => arr () b -> arr () b -> arr Bool b
boolA no yes = bool (Left ()) (Right ()) ^>> (taking "false" no ||| taking "true" yes)
{-# INLINE boolA #-}
