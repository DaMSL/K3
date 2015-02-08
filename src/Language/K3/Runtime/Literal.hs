{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TemplateHaskell #-}

-- | Native Haskell value construction from K3 literals.
module Language.K3.Runtime.Literal where

import Control.Applicative ( (<$>), (<*>) )
import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Literal

import Language.K3.Runtime.Engine

bool :: K3 Literal -> EngineM a Bool
bool (tag -> LBool b) = return b
bool _ = throwEngineError $ EngineError "Invalid boolean literal"

byte :: K3 Literal -> EngineM a Word8
byte (tag -> LByte b) = return b
byte _ = throwEngineError $ EngineError "Invalid byte literal"

int :: K3 Literal -> EngineM a Int
int (tag -> LInt i) = return i
int _ = throwEngineError $ EngineError "Invalid int literal"

real :: K3 Literal -> EngineM a Double
real (tag -> LReal r) = return r
real _ = throwEngineError $ EngineError "Invalid real literal"

string :: K3 Literal -> EngineM a String
string (tag -> LString s) = return s
string _ = throwEngineError $ EngineError "Invalid string literal"

none :: K3 Literal -> EngineM a (Maybe b)
none (tag -> LNone _) = return Nothing
none _ = throwEngineError $ EngineError "Invalid option literal"

some :: (K3 Literal -> EngineM a b) -> K3 Literal -> EngineM a (Maybe b)
some f (details -> (LSome, [x], _)) = f x >>= return . Just
some _ _ = throwEngineError $ EngineError "Invalid option literal"

option :: (K3 Literal -> EngineM a b) -> K3 Literal -> EngineM a (Maybe b)
option _ l@(tag -> LNone _) = none l
option someF l@(tag -> LSome) = some someF l
option _ _ = throwEngineError $ EngineError "Invalid option literal"

indirection :: (K3 Literal -> EngineM a b) -> K3 Literal -> EngineM a (MVar b)
indirection f (details -> (LIndirect, [x], _)) = f x >>= liftIO . newMVar
indirection _ _ = throwEngineError $ EngineError "Invalid indirection literal"

tuple :: ([K3 Literal] -> EngineM a b) -> K3 Literal -> EngineM a b
tuple f (details -> (LTuple, ch, _)) = f ch
tuple _ _ = throwEngineError $ EngineError "Invalid tuple literal"

record :: ([(Identifier, K3 Literal)] -> EngineM a b) -> K3 Literal -> EngineM a b
record f (details -> (LRecord ids, ch, _)) = f $ zip ids ch
record _ _ = throwEngineError $ EngineError "Invalid record literal"

-- TODO: native collection construction
empty :: (() -> EngineM a b) -> K3 Literal -> EngineM a b
empty f (details -> (LEmpty _, [], _)) = f ()
empty _ _ = throwEngineError $ EngineError "Invalid empty collection literal"

-- TODO: native collection construction
collection :: ([K3 Literal] -> EngineM a b) -> K3 Literal -> EngineM a b
collection f (details -> (LCollection _, elems, _)) = f elems
collection _ _ = throwEngineError $ EngineError "Invalid collection literal"

anyCollection :: ([K3 Literal] -> EngineM a b) -> K3 Literal -> EngineM a b
anyCollection f (tag -> LEmpty _) = f []
anyCollection f (details -> (LCollection _, elems, _)) = f elems
anyCollection _ _ = throwEngineError $ EngineError "Invalid collection literal"

address :: K3 Literal -> EngineM a Address
address (details -> (LAddress, [h,p], _)) = Address <$> ((,) <$> string h <*> int p)
address _ = throwEngineError $ EngineError "Invalid address literal"

