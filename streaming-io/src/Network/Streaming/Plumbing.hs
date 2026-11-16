{-# LANGUAGE PatternSynonyms #-}

-- | Helpers that route values through any arrow and need nothing of the capability classes but the annotations of
-- diagrams, so that the classes can use them too. "Network.Streaming.Combinators" re-exports them, but for 'each',
-- 'folding', 'prism' and 'traversal', which are imported from here. All are INLINE, for the reason given there.
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
    _Left,
    _Right,
    _Just,
    prism,
    each,
    traversal,
    folding,
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

import Control.Arrow (Arrow, ArrowChoice, arr, first, left, returnA, right, second, (>>>), (>>^), (^>>), (|||))
import Data.Bool (bool)
import Data.Foldable (toList)
import Data.Functor.Const (Const (..))
import Network.Streaming.Annotate (Annotate, (-|), (|-))

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
    loop = body >>> ("again" |- (next >>> loop) ||| "finish" |- returnA)
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
decide question f = arr f -| question
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

-- | The left of an 'Either', as a prism: runs the arrow on a 'Left' and passes a 'Right' through.
_Left :: (ArrowChoice arr) => arr a b -> arr (Either a c) (Either b c)
_Left = left
{-# INLINE _Left #-}

-- | The right of an 'Either', as a prism: runs the arrow on a 'Right' and passes a 'Left' through.
_Right :: (ArrowChoice arr) => arr a b -> arr (Either c a) (Either c b)
_Right = right
{-# INLINE _Right #-}

-- | The 'Just' of a 'Maybe', as a prism: runs the arrow on the value and passes 'Nothing' through. In diagrams the
-- branches are named "Just" and "Nothing".
_Just :: (ArrowChoice arr, Annotate arr) => arr a b -> arr (Maybe a) (Maybe b)
_Just p = maybe (Left Nothing) Right ^>> ("Nothing" |- returnA ||| "Just" |- (p >>^ Just))
{-# INLINE _Just #-}

-- | A prism from how a case is built and how it is matched: the arrow runs on the focus of a 'Right', and a 'Left' is
-- the whole result already and passes through without running it.
prism :: (ArrowChoice arr) => (b -> t) -> (s -> Either t a) -> arr a b -> arr s t
prism build match p = match ^>> (returnA ||| (p >>^ build))
{-# INLINE prism #-}

-- | Runs the arrow on each element of a list in order, and gives the results in the same order. In diagrams the loop
-- is named "each", and it asks "another focus?".
each :: (ArrowChoice arr, Annotate arr) => arr a b -> arr [a] [b]
each p = "each" |- ((\as -> (as, [])) ^>> iterateWith (arr pick -| "another focus?") (first p >>^ push))
  where
    pick ([], bs) = Finish (reverse bs)
    pick (a : rest, bs) = Again (a, (rest, bs))
    push (b, (rest, bs)) = (rest, b : bs)
{-# INLINE each #-}

-- | Runs the arrow on each focus of a traversal, in the order that the traversal visits them, and puts the results in
-- the places of the foci. The traversal is in the form of the van Laarhoven lenses, as 'traverse' is.
traversal ::
  (ArrowChoice arr, Annotate arr) =>
  (forall f. (Applicative f) => (a -> f b) -> s -> f t) ->
  arr a b ->
  arr s t
traversal trav p = alongside (focuses ^>> each p) >>^ \(bs, s) -> refill (trav (const pop) s) bs
  where
    focuses = getConst . trav (\a -> Const [a])
{-# INLINE traversal #-}

-- | Runs the arrow on each part that the function gives, in order, and combines the results.
folding :: (ArrowChoice arr, Annotate arr, Foldable f, Monoid m) => (s -> f a) -> arr a m -> arr s m
folding parts p = (toList . parts) ^>> each p >>^ mconcat
{-# INLINE folding #-}

-- | Hands out the results of a traversal, one for each focus, from the front of a list, in continuation-passing style,
-- so that a long traversal needs a constant stack.
newtype Refill b a = Refill (forall r. [b] -> (a -> [b] -> r) -> r)

instance Functor (Refill b) where
  fmap f (Refill put) = Refill (\bs k -> put bs (\a bs' -> k (f a) bs'))

instance Applicative (Refill b) where
  pure a = Refill (\bs k -> k a bs)
  Refill f <*> Refill x = Refill (\bs k -> f bs (\g bs' -> x bs' (\a bs'' -> k (g a) bs'')))
  liftA2 f (Refill x) (Refill y) = Refill (\bs k -> x bs (\a bs' -> y bs' (\b bs'' -> k (f a b) bs'')))

-- | The next result.
pop :: Refill b b
pop = Refill $ \bs k -> case bs of
  b : rest -> k b rest
  [] -> error "Network.Streaming.Plumbing.traversal: the traversal has more foci than results"

-- | The traversal's result, with its foci replaced by the results from the front of the list.
refill :: Refill b t -> [b] -> t
refill (Refill put) bs = put bs const

-- | Runs the arrow on the part that the lens picks, and ignores the rest.
on :: (Arrow arr) => (arr a () -> arr s t) -> arr a () -> arr s ()
on l p = l p >>> done
{-# INLINE on #-}

-- | Runs the first arrow if the input is 'False', and the second if it is 'True', like 'bool'. In diagrams the two
-- branches are named "false" and "true".
boolA :: (ArrowChoice arr, Annotate arr) => arr () b -> arr () b -> arr Bool b
boolA no yes = bool (Left ()) (Right ()) ^>> ("false" |- no ||| "true" |- yes)
{-# INLINE boolA #-}
