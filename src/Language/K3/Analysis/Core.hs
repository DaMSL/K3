{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Analysis.Core
( freeVariables
, bindingVariables
, modifiedVariables
, lambdaClosures
, lambdaClosuresDecl
) where

import Control.Arrow
import Data.List

import Data.IntMap ( IntMap )
import qualified Data.IntMap as IntMap

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Utils.Pretty

-- | Retrieves all free variables in an expression.
freeVariables :: K3 Expression -> [Identifier]
freeVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EVariable n) = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> EAssign i)   = return $ concat chAcc ++ [i]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extractVariable chAcc (tag -> ELetIn i)    = return $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc

-- | Retrieves all variables introduced by a binder
bindingVariables :: Binder -> [Identifier]
bindingVariables (BIndirection i) = [i]
bindingVariables (BTuple is)      = is
bindingVariables (BRecord ivs)    = snd (unzip ivs)

-- | Retrieves all variables modified in an expression.
modifiedVariables :: K3 Expression -> [Identifier]
modifiedVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EAssign n)   = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extractVariable chAcc (tag -> ELetIn i)    = return $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc

-- | Computes the closure variables captured at lambda expressions.
--   This is a one-pass bottom-up implementation.
type ClosureEnv = IntMap [Identifier]

lambdaClosures :: K3 Declaration -> Either String ClosureEnv
lambdaClosures p = foldExpression lambdaClosuresExpr IntMap.empty p >>= return . fst

lambdaClosuresDecl :: Identifier -> ClosureEnv -> K3 Declaration -> Either String (ClosureEnv, K3 Declaration)
lambdaClosuresDecl n lc p = foldNamedDeclExpression n lambdaClosuresExpr lc p

lambdaClosuresExpr :: ClosureEnv -> K3 Expression -> Either String (ClosureEnv, K3 Expression)
lambdaClosuresExpr lc expr = do
  (lcenv,_) <- biFoldMapTree bind extract [] (IntMap.empty, []) expr
  return $ (IntMap.union lcenv lc, expr)

  where
    bind :: [Identifier] -> K3 Expression -> Either String ([Identifier], [[Identifier]])
    bind l (tag -> ELambda i) = return (l, [i:l])
    bind l (tag -> ELetIn  i) = return (l, [l, i:l])
    bind l (tag -> EBindAs b) = return (l, [l, bindingVariables b ++ l])
    bind l (tag -> ECaseOf i) = return (l, [l, i:l, l])
    bind l (children -> ch)   = return (l, replicate (length ch) l)

    extract :: [Identifier] -> [(ClosureEnv, [Identifier])] -> K3 Expression -> Either String (ClosureEnv, [Identifier])
    extract _ chAcc (tag -> EVariable i) = rt chAcc (++[i])
    extract _ chAcc (tag -> EAssign i)   = rt chAcc (++[i])
    extract l (concatLc -> (lcAcc,chAcc)) e@(tag -> ELambda n) = extendLc lcAcc e $ filter (onlyLocals n l) $ concat chAcc
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> EBindAs b) = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> ELetIn i)  = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> ECaseOf i) = return . (lcAcc,) $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extract _ chAcc _ = rt chAcc id

    onlyLocals n l i = i /= n && i `elem` l

    concatLc :: [(ClosureEnv, [Identifier])] -> (ClosureEnv, [[Identifier]])
    concatLc subAcc = let (x,y) = unzip subAcc in (IntMap.unions x, y)

    extendLc :: ClosureEnv -> K3 Expression -> [Identifier] -> Either String (ClosureEnv, [Identifier])
    extendLc lcenv e ids = case e @~ isEUID of
      Just (EUID (UID i)) -> return $ (IntMap.insert i (nub ids) lcenv, ids)
      _ -> Left $ boxToString $ ["No UID found on lambda"] %$ prettyLines e

    rt subAcc f = return $ second (f . concat) $ concatLc subAcc
