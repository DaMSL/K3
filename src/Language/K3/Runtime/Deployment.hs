{-# LANGUAGE ViewPatterns #-}

module Language.K3.Runtime.Deployment where

import Data.Tree
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Literal
import Language.K3.Core.Type

-- | A bootstrap environment for a peer.
type PeerBootstrap = [(Identifier, K3 Literal)]

-- | A system environment, to bootstrap a set of deployed peers.
type SystemEnvironment = [(Address, PeerBootstrap)]


bool :: K3 Literal -> Maybe Bool
bool (tag -> LBool b) = Just b
bool _ = Nothing

byte :: K3 Literal -> Maybe Word8
byte (tag -> LByte b) = Just b
byte _ = Nothing

int :: K3 Literal -> Maybe Int
int (tag -> LInt i) = Just i
int _ = Nothing

real :: K3 Literal -> Maybe Double
real (tag -> LReal r) = Just r
real _ = Nothing

string :: K3 Literal -> Maybe String
string (tag -> LString s) = Just s
string _ = Nothing

none :: K3 Literal -> Maybe (Maybe a)
none (tag -> LNone _) = Just Nothing
none _ = Nothing

some :: (K3 Literal -> Maybe a) -> K3 Literal -> Maybe (Maybe a)
some f (details -> (LSome, [x], _)) = Just $ f x
some _ _ = Nothing

tuple :: ([K3 Literal] -> a) -> K3 Literal -> Maybe a
tuple f (details -> (LTuple, ch, _)) = Just $ f ch
tuple _ _ = Nothing

record :: ([(Identifier, K3 Literal)] -> a) -> K3 Literal -> Maybe a
record f (details -> (LRecord ids, ch, _)) = Just . f $ zip ids ch
record _ _ = Nothing

empty :: (K3 Type -> a) -> K3 Literal -> Maybe a
empty f (details -> (LEmpty t, [], _)) = Just $ f t
empty _ _ = Nothing

collection :: (K3 Type -> [K3 Literal] -> a) -> K3 Literal -> Maybe a
collection f (details -> (LCollection t, elems, _)) = Just $ f t elems
collection _ _ = Nothing

address :: (K3 Literal -> K3 Literal -> a) -> K3 Literal -> Maybe a
address f (details -> (LAddress, [h,p], _)) = Just $ f h p
address _ _ = Nothing

details :: K3 Literal -> (Literal, [K3 Literal], [Annotation Literal])
details (Node (tg :@: anns) ch) = (tg, ch, anns)
