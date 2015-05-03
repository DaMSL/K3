{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Analysis.Core
( freeVariables
, bindingVariables
, modifiedVariables
, lambdaClosures
, lambdaClosuresDecl
, variablePositions
, variablePositionsDecl
) where

import Control.Arrow
import Data.Bits
import Data.List
import Data.Word ( Word8 )

import Data.IntMap ( IntMap )
import qualified Data.IntMap as IntMap

import Data.Vector.Unboxed ( Vector )
import qualified Data.Vector.Unboxed as Vector

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Utils.Pretty

-- | Lambda closures, as a map of lambda expression UIDs to closure variable identifiers.
type ClosureEnv = IntMap [Identifier]

-- | Binding point scopes, as a map of binding point expression UIDS to current scope
--   including the new binding.
type ScopeEnv = IntMap [Identifier]

-- | Vector for binding usage bits
type BVector = Vector Word8

-- | Scope usage bitmasks, as a map of expression UID to a pair of scope uid and bitvector.
type ScopeUsageEnv = IntMap (UID, BVector)

-- | Expression-variable metadata container.
data VarPosEnv = VarPosEnv { lcenv :: ClosureEnv, scenv :: ScopeEnv, vuenv :: ScopeUsageEnv }

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


-- | Compute lambda closures and binding point scopes in a single pass.
vp0 :: VarPosEnv
vp0 = VarPosEnv IntMap.empty IntMap.empty IntMap.empty

variablePositions :: K3 Declaration -> Either String VarPosEnv
variablePositions p = foldExpression variablePositionsExpr vp0 p >>= return . fst

variablePositionsDecl :: Identifier -> VarPosEnv -> K3 Declaration -> Either String (VarPosEnv, K3 Declaration)
variablePositionsDecl n vp p = foldNamedDeclExpression n variablePositionsExpr vp p

variablePositionsExpr :: VarPosEnv -> K3 Expression -> Either String (VarPosEnv, K3 Expression)
variablePositionsExpr vp expr = do
  (nvp,_) <- biFoldMapTree bind extract ([], -1) (vp0, []) expr
  let rvp = vp { lcenv = IntMap.union (lcenv nvp) (lcenv vp)
               , scenv = IntMap.union (scenv nvp) (scenv vp)
               , vuenv = IntMap.union (vuenv nvp) (vuenv vp) }
  return $ (rvp, expr)

  where
    uidOf :: K3 Expression -> Either String Int
    uidOf ((@~ isEUID) -> Just (EUID (UID i))) = return i
    uidOf e = Left $ boxToString $ ["No UID found for uidOf"]

    bind :: ([Identifier], Int) -> K3 Expression -> Either String (([Identifier], Int), [([Identifier], Int)])
    bind l e@(tag -> ELambda i) = uidOf e >>= \u -> return (l, [((i:fst l), u)])
    bind l e@(tag -> ELetIn  i) = uidOf e >>= \u -> return (l, [l, ((i:fst l), u)])
    bind l e@(tag -> EBindAs b) = uidOf e >>= \u -> return (l, [l, (bindingVariables b ++ fst l, u)])
    bind l e@(tag -> ECaseOf i) = uidOf e >>= \u -> return (l, [l, ((i:fst l), u), l])
    bind l (children -> ch)   = return (l, replicate (length ch) l)

    extract :: ([Identifier], Int) -> [(VarPosEnv, [Identifier])] -> K3 Expression -> Either String (VarPosEnv, [Identifier])
    extract td        chAcc e@(tag -> EVariable i) = rt td chAcc e [] (const concatVP) ((++[i]) . concat)
    extract td        chAcc e@(tag -> EAssign i)   = rt td chAcc e [] (const concatVP) ((++[i]) . concat)
    extract td@(sc,_) chAcc e@(tag -> ELambda n)   = rt td chAcc e [n] (\u as is -> scope [n] sc u [closure n sc u as is] is) (prune 0 [n])
    extract td@(sc,_) chAcc e@(tag -> EBindAs b)   = let bvs = bindingVariables b in
                                                     rt td chAcc e bvs (scope bvs sc) (prune 1 bvs)
    extract td@(sc,_) chAcc e@(tag -> ELetIn i)    = rt td chAcc e [i] (scope [i] sc) (prune 1 [i])
    extract td@(sc,_) chAcc e@(tag -> ECaseOf i)   = rt td chAcc e [i] (scope [i] sc) (prune 1 [i])
    extract td@(sc,_) chAcc e = rt td chAcc e [] (const concatVP) concat

    concatVP :: [VarPosEnv] -> [[Identifier]] -> VarPosEnv
    concatVP vps _ = VarPosEnv { lcenv = IntMap.unions (map lcenv vps)
                               , scenv = IntMap.unions (map scenv vps)
                               , vuenv = IntMap.unions (map vuenv vps) }

    closure :: Identifier -> [Identifier] -> Int -> [VarPosEnv] -> [[Identifier]] -> VarPosEnv
    closure n sc i vps subvars = vp { lcenv = IntMap.insert i clvars (lcenv vp) }
      where vp = concatVP vps subvars
            clvars = subvarsInScope [n] sc subvars

    scope :: [Identifier] -> [Identifier] -> Int -> [VarPosEnv] -> [[Identifier]] -> VarPosEnv
    scope n sc i vps subvars = vp { scenv = IntMap.insert i (sc++n) (scenv vp) }
      where vp = concatVP vps subvars

    varusage :: [Identifier] -> [Identifier] -> Int -> Int -> [VarPosEnv] -> [[Identifier]] -> VarPosEnv
    varusage n sc scu u vps subvars = vp { vuenv = IntMap.insert u (UID scu, usedmask) (vuenv vp) }
      where vp = concatVP vps subvars
            usedvars = subvarsInScope n sc subvars
            usedmask = Vector.fromList $ snd $ foldl mkmask (0,[]) sc

            mkmask (c, m)   i | ix c == 0 = (c+1, fromBool (used i) : m)
            mkmask (c, h:t) i             = (c+1, (if used i then h `setBit` (ix c) else h):t)
            mkmask _ _ = error "Mask initialization failed"

            sz = finiteBitSize (0 :: Word8)
            ix c = c `mod` sz
            used i = i `elem` usedvars
            fromBool False = 0
            fromBool True  = bit 0

    prune :: Int -> [Identifier] -> [[Identifier]] -> [Identifier]
    prune i vars subvars = concatMap (\(j,l) -> if i == j then (l \\ vars) else l) $ zip [0..(length subvars)] subvars

    subvarsInScope :: [Identifier] -> [Identifier] -> [[Identifier]] -> [Identifier]
    subvarsInScope n sc subvars = nub $ filter (onlyLocals n sc) $ concat subvars

    onlyLocals :: [Identifier] -> [Identifier] -> Identifier -> Bool
    onlyLocals n l i = i `notElem` n && i `elem` l

    rt (sc,scu) (unzip -> (acc, iAcc)) e bnds accF iAccF = do
      u <- uidOf e
      return (varusage bnds sc scu u [accF u acc iAcc] iAcc, iAccF iAcc)
