{-# LANGUAGE DataKinds #-}

module CleanupCollects where

import Control.Arrow ((>>>), (^>>))
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, undoing)

-- | A cleanup that collects from an inbox, which waits.
collecting :: (Steps arr, Inboxes arr 'Anywhere) => arr () ()
collecting = const undefined ^>> collect @_ @'Anywhere @"mail" @Int >>> done

-- | A cleanup that can wait, which undoing refuses.
rejected :: (Steps arr) => arr () ()
rejected = undoing done collecting
