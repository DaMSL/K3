{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

module Language.K3.Interpreter.Builtins.Math where

import Control.Monad.State

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors

genMathBuiltin :: Identifier -> K3 Type -> Maybe (Interpretation Value)

-- random :: int -> int
genMathBuiltin "random" _ =
  Just $ vfun $ \x -> case x of
    VInt upper -> liftIO (Random.randomRIO (0::Int, upper)) >>= return . VInt
    _          -> throwE $ RunTimeInterpretationError $ "Expected int but got " ++ show x

-- randomFraction :: () -> real
genMathBuiltin "randomFraction" _ = Just $ vfun $ \_ -> liftIO Random.randomIO >>= return . VReal

-- hash :: forall a . a -> int
genMathBuiltin "hash" _ = Just $ vfun $ \v -> valueHash v

-- range :: int -> collection {i : int} @ { Collection }
genMathBuiltin "range" _ =
  Just $ vfun $ \(VInt upper) ->
    initialAnnotatedCollection "Collection"
      $ map (\i -> VRecord (insertMember "i" (VInt i, MemImmut) $ emptyMembers)) [0..(upper-1)]

-- truncate :: int -> real
genMathBuiltin "truncate" _ = Just $ vfun $ \x -> case x of
  VReal r   -> return $ VInt $ truncate r
  _         -> throwE $ RunTimeInterpretationError $ "Expected real but got " ++ show x

-- real_of_int :: real -> int
genMathBuiltin "real_of_int" _ = Just $ vfun $ \x -> case x of
  VInt i    -> return $ VReal $ fromIntegral i
  _         -> throwE $ RunTimeInterpretationError $ "Expected int but got " ++ show x

-- get_max_int :: () -> int
genMathBuiltin "get_max_int" _ = Just $ vfun $ \_  -> return $ VInt maxBound

genMathBuiltin _ _ = Nothing
