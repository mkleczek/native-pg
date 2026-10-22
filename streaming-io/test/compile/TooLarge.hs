{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}

module TooLarge where

#include "MachDeps.h"

import Network.Streaming.PositiveInt

value :: PositiveInt
#if WORD_SIZE_IN_BITS == 64
value = positiveInt @9223372036854775808
#elif WORD_SIZE_IN_BITS == 32
value = positiveInt @2147483648
#else
#error Unsupported Int width
#endif
