{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Hindley-Milner type inference.
--
--   For K3, this does not support subtyping, nor does it check annotation
--   composition. Instead it treats annotated collections as ad-hoc external types,
--   and requires structural type equality to hold on function application.
--   Both of these functionalities are not required for K3-Mosaic code.
--
--   Most of the scaffolding is taken from Oleg Kiselyov's tutorial:
--   http://okmij.org/ftp/Computation/FLOLAC/TInfLetP.hs
--
module Language.K3.Analysis.HMTypes.Inference where

import Control.Monad.Identity
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.List
import Data.Map ( Map )
import qualified Data.Map as Map
import Data.Maybe
import Data.Tree

import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Constructor.Type as TC
import Language.K3.Analysis.Common
import Language.K3.Analysis.HMTypes.DataTypes

-- | Misc. helpers
(.+) :: K3 Expression -> K3 QType -> K3 Expression
(.+) e qt = e @+ (EQType $ mutabilityE e qt)

infixr 5 .+

immutQT :: K3 QType -> K3 QType
immutQT = (@+ QTImmutable)

mutQT :: K3 QType -> K3 QType
mutQT = (@+ QTMutable)

mutabilityT :: K3 Type -> K3 QType -> K3 QType
mutabilityT t qt = case t @~ isTQualified of
                     Nothing -> qt
                     Just TImmutable -> immutQT qt
                     Just TMutable   -> mutQT qt
                     _ -> error "Invalid type qualifier annotation"

mutabilityE :: K3 Expression -> K3 QType -> K3 QType
mutabilityE e qt = case e @~ isEQualified of
                     Nothing -> qt
                     Just EImmutable -> immutQT qt
                     Just EMutable   -> mutQT qt
                     _ -> error "Invalid expression qualifier annotation"

qTypeOf :: K3 Expression -> Maybe (K3 QType)
qTypeOf e = case e @~ isEQType of
              Just (EQType qt) -> Just qt
              _ -> Nothing

qTypeOfM :: K3 Expression -> TVEnvM (K3 QType)
qTypeOfM e = case e @~ isEQType of
              Just (EQType qt) -> return qt
              _ -> left $ "Untyped expression: " ++ show e

projectNamedPairs :: [Identifier] -> [(Identifier, a)] -> [a]
projectNamedPairs ids idv = snd $ unzip $ filter (\(k,_) -> k `elem` ids) idv

rebuildNamedPairs :: [(Identifier, a)] -> [Identifier] -> [a] -> [a]
rebuildNamedPairs oldIdv newIds newVs = map (replaceNewPair $ zip newIds newVs) oldIdv
  where replaceNewPair pairs (k,v) = maybe v id $ lookup k pairs


-- | A type environment.
type TEnv = [(Identifier, QPType)]

-- | Annotation type environment.
type TAEnv = Map Identifier TMEnv

-- | Declared type variable environment.
type TDVEnv = [(Identifier, QTVarId)]

-- | Annotation member environment.
--   The boolean indicates whether the member is a lifted attribute.
type TMEnv = [(Identifier, (QPType, Bool))]

-- | Type inference environment
type TIEnv = (TEnv, TAEnv, TDVEnv)

-- | A type variable environment.
data TVEnv = TVEnv QTVarId (Map QTVarId (K3 QType)) deriving Show

-- | A state monad for the type variable environment.
type TVEnvM = EitherT String (State TVEnv)

{- TEnv helpers -}
tenv0 :: TEnv
tenv0 = []

tlkup :: TEnv -> Identifier -> Either String QPType
tlkup env x = maybe err Right $ lookup x env 
 where err = Left $ "Unbound variable in type environment: " ++ x

text :: TEnv -> Identifier -> QPType -> TEnv
text env x t = (x,t) : env


{- TAEnv helpers -}
taenv0 :: TAEnv 
taenv0 = Map.empty

talkup :: TAEnv -> Identifier -> Either String TMEnv
talkup env x = maybe err Right $ Map.lookup x env
  where err = Left $ "Unbound variable in annotation environment: " ++ x

taext :: TAEnv -> Identifier -> TMEnv -> TAEnv
taext env x te = Map.insert x te env

{- TDVEnv helpers -}
tdvenv0 :: TDVEnv
tdvenv0 = []

tdvlkup :: TDVEnv -> Identifier -> Either String (K3 QType)
tdvlkup env x = maybe err (Right . tvar) $ lookup x env
  where err = Left $ "Unbound declared variable in environment: " ++ x

tdvext :: TDVEnv -> Identifier -> QTVarId -> TDVEnv
tdvext env x v = (x,v) : env

{- TIEnv helpers -}
tienv0 :: TIEnv
tienv0 = (tenv0, taenv0, tdvenv0)

tilkupe :: TIEnv -> Identifier -> Either String QPType
tilkupe (te,_,_) x = tlkup te x

tilkupa :: TIEnv -> Identifier -> Either String TMEnv
tilkupa (_,ta,_) x = talkup ta x

tiexte :: TIEnv -> Identifier -> QPType -> TIEnv
tiexte (te,ta,tdv) x t = (text te x t, ta, tdv)

tiexta :: TIEnv -> Identifier -> TMEnv -> TIEnv
tiexta (te,ta,tdv) x ate = (te, taext ta x ate, tdv) 

tilkupdv :: TIEnv -> Identifier -> Either String (K3 QType)
tilkupdv (_,_,tdv) x = tdvlkup tdv x

tiextdv :: TIEnv -> Identifier -> QTVarId -> TIEnv
tiextdv (te, ta, tdv) x v = (te, ta, tdvext tdv x v)

{- TVEnvM helpers -}

runTVEnvM :: TVEnv -> TVEnvM a -> (Either String a, TVEnv)
runTVEnvM env m = flip runState env $ runEitherT m

liftEitherM :: Either String a -> TVEnvM a
liftEitherM = either left return 

-- Allocate a fresh type variable
newtv :: TVEnvM (K3 QType)
newtv = do
 TVEnv n s <- get
 put (TVEnv (succ n) s)
 return $ tvar n

tvenv0 :: TVEnv
tvenv0 = TVEnv 0 Map.empty

tvlkup :: TVEnv -> QTVarId -> Maybe (K3 QType)
tvlkup (TVEnv _ s) v = Map.lookup v s

tvext :: TVEnv -> QTVarId -> K3 QType -> TVEnv
tvext (TVEnv c s) v t = TVEnv c $ Map.insert v t s

-- TVE domain predicate: check to see if a TVarName is in the domain of TVE
tvdomainp :: TVEnv -> QTVarId -> Bool
tvdomainp (TVEnv _ s) v = Map.member v s

-- Give the list of all type variables that are allocated in TVE but
-- not bound there
tvfree :: TVEnv -> [QTVarId]
tvfree (TVEnv c s) = filter (\v -> not (Map.member v s)) [0..c-1]

-- Deep substitute, throughout type structure
tvsub :: TVEnv -> K3 QType -> K3 QType
tvsub tve qt = runIdentity $ mapTree sub qt
  where
    sub ch t@(tag -> QTCon d) = return $ foldl (@+) (tdata d ch) $ annotations t

    sub ch t@(tag -> QTOperator QTLower) =
      return $ if null ch then error "Invalid qtype lower operator"
               else if null $ concatMap freevars ch
                      then tvopeval tve QTLower ch
                      else foldl (@+) (tlower ch) $ annotations t

    sub _ (tag -> QTVar v) | Just t <- tvlkup tve v = return $ tvsub tve t
    sub _ t = return t

-- `Shallow' substitution
tvchase :: TVEnv -> K3 QType -> K3 QType
tvchase tve (tag -> QTVar v) | Just t <- tvlkup tve v = tvchase tve t
tvchase _ t = t

-- 'Shallow' substitution, additionally returning the last variable in
--  the chased chain.
tvchasev :: TVEnv -> Maybe QTVarId -> K3 QType -> (Maybe QTVarId, K3 QType)
tvchasev tve _ (tag -> QTVar v) | Just ctv <- tvlkup tve v = tvchasev tve (Just v) ctv
tvchasev _ lastV tv = (lastV, tv)

-- Compute (quite unoptimally) the characteristic function of the set 
--  forall tvb \in fv(tve_before). Union fv(tvsub(tve_after,tvb))
tvdependentset :: TVEnv -> TVEnv -> (QTVarId -> Bool)
tvdependentset tve_before tve_after = 
    \tv -> any (\tvb -> occurs tv (tvar tvb) tve_after) tvbs
 where
   tvbs = tvfree tve_before

-- Return the list of type variables in t (possibly with duplicates)
freevars :: K3 QType -> [QTVarId]
freevars t = runIdentity $ foldMapTree extractVars [] t
  where
    extractVars _ (tag -> QTVar v) = return [v]
    extractVars chAcc _ = return $ concat chAcc

-- | Lower bound computation for numeric and record types.
--   This function does not preserve annotations.
tvlower :: TVEnv -> K3 QType -> K3 QType -> K3 QType
tvlower tve a b = tvlower' (tvchase tve a) (tvchase tve b)
  where
    tvlower' a' b' = case (tag a', tag b') of
      (QTPrimitive p1, QTPrimitive p2)
        | [p1, p2] `intersect` [QTReal, QTInt, QTNumber] == [p1,p2] -> 
            tprim $ toEnum $ minimum $ map fromEnum [p1,p2]
      
      (QTCon (QTRecord i1), QTCon (QTRecord i2)) 
        | i1 `intersect` i2 == i1 -> mergedRecord True  i1 a' i2 b'
        | i1 `intersect` i2 == i2 -> mergedRecord False i2 b' i1 a'
        | otherwise -> trec $ nub $ zip (i1 ++ i2) $ (children a') ++ (children b')

      (QTCon (QTCollection _), QTCon (QTRecord _)) -> coveringCollection a' b'
      (QTCon (QTRecord _), QTCon (QTCollection _)) -> coveringCollection b' a'

      (_, _) -> lowerError a' b'

    mergedRecord subAsLeft subid subqt supid supqt =
      trec $ zip supid $
        mergeCovering subAsLeft (zip subid $ children subqt) (zip supid $ children supqt)

    mergeCovering subAsLeft sub sup =
      let lowerF = if subAsLeft then \supV subV -> tvlower tve subV supV
                                else \supV subV -> tvlower tve supV subV
      in map (\(k,v) -> maybe v (lowerF v) $ lookup k sub) sup

    coveringCollection ct rt@(tag -> QTCon (QTRecord ids))
      | selfRecord <- last (children ct)
      , QTCon (QTRecord liftedAttrIds) <- tag selfRecord
      , liftedAttrIds `intersect` ids == ids
      = ct
      | otherwise = lowerError ct rt

    coveringCollection x y = lowerError x y

    lowerError x y = error $ unwords $ ["Invalid lower bound operands: ", show x, "and", show y]

-- | Type operator evaluation.
tvopeval :: TVEnv -> QTOp -> [K3 QType] -> K3 QType
tvopeval tve QTLower ch = foldl1 (tvlower tve) ch

consistentTLower :: TVEnv -> [K3 QType] -> Either String (K3 QType, TVEnv)
consistentTLower tve ch =
    let (varCh, nonvarCh) = partition isQTVar $ nub ch in
    if null varCh && null nonvarCh then error "Invalid lower qtype"
    else if null varCh then return (tlower $ nonvarCh, tve)
    else
      let (vch, lb) = if null nonvarCh
                        then (tail varCh, tvchase tve $ head varCh)
                        else (varCh, tvopeval tve QTLower nonvarCh)
      in 
        foldM (extractAndUnifyV lb) tve vch
          >>= \ntve -> return (tlower $ [lb]++vch, ntve)
  where
    extractAndUnifyV t tve' (tag -> QTVar v) = unifyv v t tve'
    extractAndUnifyV _ _ _ = error "Invalid type var during lower qtype merge"


-- Unification helpers.
collectionSubRecord :: K3 QType -> K3 QType -> Bool
collectionSubRecord ct@(tag -> QTCon (QTCollection _)) (tag -> QTCon (QTRecord ids)) =
  let selfRecord = last $ children ct in
  case tag selfRecord of
    QTCon (QTRecord liftedAttrIds) -> liftedAttrIds `intersect` ids == ids
    _ -> False

collectionSubRecord _ _ = False


-- The unification. If unification failed, return the reason
-- TODO: refactor jointly with unifyWithOverride
-- TODO: unification for QTSelf
unify :: K3 QType -> K3 QType -> TVEnv -> Either String TVEnv
unify t1 t2 tve = unify' (tvchase tve t1) (tvchase tve t2) tve

-- If either t1 or t2 are type variables, they are definitely unbound
unify' :: K3 QType -> K3 QType -> TVEnv -> Either String TVEnv
unify' (tag -> QTPrimitive p1) (tag -> QTPrimitive p2) tve
  | p1 == p2 = Right tve
  | ([p1, p2] `intersect` [QTNumber, QTInt, QTReal])  == [p1,p2] = Right tve
  | otherwise = Left $ unwords ["Unification mismatch on primitives: ", show p1, "and", show p2]

-- | Record subtyping for projection
unify' t1@(tag -> QTCon d1@(QTRecord f1)) t2@(tag -> QTCon d2@(QTRecord f2)) tve
  | f1 `intersect` f2 == f1
    = unifyChildren tve d1 d1 "record subtype" 
                    (children t1)
                    (projectNamedPairs f1 $ zip f2 $ children t2)
  
  | f1 `intersect` f2 == f2
    = unifyChildren tve d2 d2 "record subtype"
                    (projectNamedPairs f2 $ zip f1 $ children t1)
                    (children t2)

-- | Collection-as-record subtyping for projection
unify' t1@(tag -> QTCon (QTCollection _)) t2@(tag -> QTCon (QTRecord f2)) tve
  | selfRecord <- (last $ children t1)
  , QTCon (QTRecord liftedAttrIds) <- tag selfRecord
  , liftedAttrIds `intersect` f2 == f2
  = unifyChildren tve (QTRecord liftedAttrIds) (QTRecord liftedAttrIds) "collection subtype" 
                  (projectNamedPairs f2 $ zip liftedAttrIds $ children selfRecord) (children t2)

unify' t1@(tag -> QTCon (QTRecord f1)) t2@(tag -> QTCon (QTCollection _)) tve
  | selfRecord <- (last $ children t2)
  , QTCon (QTRecord liftedAttrIds) <- tag selfRecord
  , liftedAttrIds `intersect` f1 == f1
  = unifyChildren tve (QTRecord liftedAttrIds) (QTRecord liftedAttrIds) "collection subtype" 
                  (children t1) (projectNamedPairs f1 $ zip liftedAttrIds $ children selfRecord)

unify' t1@(tag -> QTCon d1) t2@(tag -> QTCon d2) tve =
  unifyChildren tve d1 d2 "datatypes" (children t1) (children t2)

-- | Unification of a delayed LB operator applies to the lower bounds of each set.
unify' t1@(tag -> QTOperator QTLower) t2@(tag -> QTOperator QTLower) tve =
  either (Left . lowerErr) return $ unify (lb t1) (lb t2) tve
  where lb t = tvopeval tve QTLower $ children t
        lowerErr s = unwords ["Unification mismatch on lower bound: ", show t1, "and", show t2, s]

unify' (tag -> QTVar v) t tve = unifyv v t tve
unify' t (tag -> QTVar v) tve = unifyv v t tve

-- | Unification of a lower bound with a non-bound applies to the non-bound
--   and the lower bound of the deferred set.
--   This pattern match applies after type variable unification to allow typevars
--   to match the lower-bound set.
unify' t1@(tag -> QTOperator QTLower) t2 tve =
  either (Left . lowerErr) return $ unify (tvopeval tve QTLower $ children t1) t2 tve
  where lowerErr s = unwords ["Unification mismatch on lower bound: ", show t1, "and", show t2, s]

unify' t1 t2@(tag -> QTOperator QTLower) tve =
  either (Left . lowerErr) return $ unify t1 (tvopeval tve QTLower $ children t2) tve
  where lowerErr s = unwords ["Unification mismatch on lower bound: ", show t1, "and", show t2, s]

-- | Top unifies with any value. Bottom unifies with only itself.
unify' t1 t2 tve | tag t1 == QTTop    || tag t2 == QTTop    = Right tve
                 | tag t1 == QTBottom && tag t2 == QTBottom = Right tve
                 | otherwise = Left $ unwords ["Unification mismatch:", show t1, "and", show t2]

unifyChildren :: (Eq a, Show a)
              => TVEnv -> a -> a -> String 
              -> [K3 QType] -> [K3 QType] -> Either String TVEnv 
unifyChildren tve tga tgb kind a b
  | tga == tgb = unifyList a b tve $ \s -> err s
  | otherwise  = Left $ err ""
  where err s  = unwords ["Unification mismatch on ", kind, ": ", show tga, "and", show tgb, s]

unifyList :: [K3 QType] -> [K3 QType] -> TVEnv -> (String -> String) -> Either String TVEnv
unifyList a b tve errf =
  if length a == length b
    then foldM (\x (y,z) -> unify y z x) tve $ zip a b
    else Left $ errf "Unification mismatch on lists."

-- Unify a free variable v1 with t2
unifyv :: QTVarId -> K3 QType -> TVEnv -> Either String TVEnv
unifyv v1 t@(tag -> QTVar v2) tve =
    if v1 == v2
      then Right tve
      else Right $ tvext tve v1 t

unifyv v t tve =
    if occurs v t tve
      then Left $ unwords ["occurs check:", show v, "in", show $ tvsub tve t]
      else Right $ tvext tve v t

-- The occurs check: if v appears free in t
occurs :: QTVarId -> K3 QType -> TVEnv -> Bool
occurs v t@(tag -> QTCon _) tve = or $ map (flip (occurs v) tve) $ children t
occurs v (tag -> QTVar v2)  tve = maybe (v == v2) (flip (occurs v) tve) $ tvlkup tve v2
occurs _ _ _ = False


-- TODO: unification for QTSelf
unifyWithOverride :: K3 QType -> K3 QType -> TVEnv -> Either String (K3 QType, TVEnv)
unifyWithOverride qt1 qt2 tve = do
    let (v1,qt1') = tvchasev tve Nothing qt1
    let (v2,qt2') = tvchasev tve Nothing qt2
    let vs        = catMaybes [v1, v2]
    (qt, ntve) <- unifyWO qt1' qt2' tve
    ntve2      <- foldM (\acc v -> unifyv v qt acc) ntve vs 
    return (if null vs then qt else tvar (head vs), ntve2)
  
  where
    unifyWO t1@(isQTNumeric -> True) t2@(isQTNumeric -> True) tve' = Right (tvlower tve t1 t2, tve')

    unifyWO t1@(tag -> QTPrimitive p1) (tag -> QTPrimitive p2) tve'
      | p1 == p2 = Right (t1, tve')
      | otherwise = Left $ unwords ["Unification mismatch on primitives: ", show p1, "and", show p2]

    -- | Record subtyping for projection
    unifyWO t1@(tag -> QTCon d1@(QTRecord f1)) t2@(tag -> QTCon d2@(QTRecord f2)) tve'
      | f1 `intersect` f2 == f1 = onRecord (t1,d1,f1) (t2,d2,f2) tve'
      | f1 `intersect` f2 == f2 = onRecord (t2,d2,f2) (t1,d1,f1) tve'

    -- | Collection-as-record subtyping for projection
    unifyWO t1@(tag -> QTCon (QTCollection _)) t2@(tag -> QTCon (QTRecord _)) tve'
      | collectionSubRecord t1 t2 = onCollection t1 t2 tve'

    unifyWO t1@(tag -> QTCon (QTRecord _)) t2@(tag -> QTCon (QTCollection _)) tve'
      | collectionSubRecord t2 t1 = onCollection t2 t1 tve'

    unifyWO t1@(tag -> QTCon d1) t2@(tag -> QTCon d2) tve' =
      onChildren d1 d2 "datatypes" (children t1) (children t2) tve' (tdata d1)

    -- | Unification of a delayed LB operator applies to the lower bounds of each set.
    --   This returns a merged delayed LB operator containing all variables, and the lower
    --   bound of the non-variable elements.
    unifyWO t1@(tag -> QTOperator QTLower) t2@(tag -> QTOperator QTLower) tve' = do
      (_, ntve) <- unifyWithOverride (lowerBound tve' t1) (lowerBound tve t2) tve'
      consistentTLower ntve $ children t1 ++ children t2

    unifyWO tv@(tag -> QTVar v) t tve' = unifyv v t tve' >>= return . (tv,)
    unifyWO t tv@(tag -> QTVar v) tve' = unifyv v t tve' >>= return . (tv,)

    -- | Unification of a lower bound with a non-bound applies to the non-bound
    --   and the lower bound of the deferred set.
    --   This pattern match applies after type variable unification to allow typevars
    --   to match the lower-bound set.
    unifyWO t1@(tag -> QTOperator QTLower) t2 tve' = do
      (_, ntve) <- unifyWithOverride (lowerBound tve' t1) t2 tve'
      consistentTLower ntve $ children t1 ++ [t2]

    unifyWO t1 t2@(tag -> QTOperator QTLower) tve' = do
      (_, ntve) <- unifyWithOverride t1 (lowerBound tve t2) tve'
      consistentTLower ntve $ [t1] ++ children t2

    -- | Top unifies with any value. Bottom unifies with only itself.
    unifyWO t1@(tag -> tg1) t2@(tag -> tg2) tve' 
      | tg1 == QTTop = Right (t2, tve')
      | tg2 == QTTop = Right (t1, tve')
      | tg1 == tg2   = Right (t1, tve')
      | otherwise    = Left $ unwords ["Unification mismatch:", show t1, "and", show t2]

    rcr (chAcc, envAcc) (b,c) = do
      (nch, nenv) <- unifyWithOverride b c envAcc
      return (chAcc++[nch], nenv)

    onCollection ct@(tag -> QTCon ccon@(QTCollection _)) rt@(tag -> QTCon (QTRecord ids)) tve' = do
      let selfRecord = last $ children ct
      liftedAttrIds  <- case tag selfRecord of
                            QTCon (QTRecord x) -> return x
                            _ -> Left "Invalid collection self record"
      let selfPairs = zip liftedAttrIds $ children $ selfRecord
      onChildren (QTRecord liftedAttrIds) (QTRecord liftedAttrIds) "collection subtype" 
                  (projectNamedPairs ids selfPairs) (children rt) tve'
                  (\nch -> let nSelf = tdata (QTRecord liftedAttrIds) $ rebuildNamedPairs selfPairs ids nch
                           in tdata ccon $ (init $ children ct) ++ [nSelf])

    onCollection ct rt _ = Left $ unwords ["Invalid collection arguments", show ct, "and", show rt]

    onRecord (subT, subCon, subIds) (supT, supCon, supIds) tve' =
        let supPairs = zip supIds $ children supT
        in onChildren subCon subCon "record subtype" 
                      (children subT) (projectNamedPairs subIds $ supPairs) tve'
                      (\nch -> tdata supCon $ rebuildNamedPairs supPairs subIds nch)

    onChildren tga tgb kind a b tve' ctor
      | tga == tgb = onList a b tve' ctor $ \s -> childrenErr tga tgb kind s
      | otherwise  = Left $ childrenErr tga tgb kind ""

    onList a b tve' ctor errf =
      if length a == length b
        then foldM rcr ([], tve') (zip a b) >>= \(nch, ntve) -> return (ctor nch, ntve)
        else Left $ errf "Unification mismatch on lists."

    lowerBound tve' t = tvopeval tve' QTLower $ children t

    childrenErr tga tgb kind s = unwords ["Unification mismatch on ", kind, ": ", show tga, "and", show tgb, s]

-- Monadic version of unify
unifyM :: K3 QType -> K3 QType -> (String -> String) -> TVEnvM ()
unifyM t1 t2 errf = do
  tve <- get
  case unify t1 t2 tve of 
    Right tve2 -> put tve2
    Left  err  -> left $ errf err

unifyWithOverrideM :: K3 QType -> K3 QType -> (String -> String) -> TVEnvM (K3 QType)
unifyWithOverrideM t1 t2 errf = do
  tve <- get
  case unifyWithOverride t1 t2 tve of 
    Right (t, tve2) -> put tve2 >> return t
    Left  err       -> left $ errf err

-- | Given a polytype, for every polymorphic type var, replace all of
--   its occurrences in t with a fresh type variable. We do this by
--   creating a substitution tve and applying it to t.
--   We also strip any mutability qualifiers here since we only instantiate
--   on variable access.
instantiate :: QPType -> TVEnvM (K3 QType)
instantiate (QPType tvs t) = do
  tve <- associate_with_freshvars tvs
  let (Node (tg :@: anns) ch) = tvsub tve t
  return $ Node (tg :@: filter (not . isQTQualified) anns) ch
 where
 associate_with_freshvars [] = return tvenv0
 associate_with_freshvars (tv:rtvs) = do
   tve     <- associate_with_freshvars rtvs
   tvfresh <- newtv
   return $ tvext tve tv tvfresh

-- | Generalization for let-polymorphism.
generalize :: TVEnvM (K3 QType) -> TVEnvM QPType
generalize ta = do
 tve_before <- get
 t          <- ta
 tve_after  <- get
 let t'    = tvsub tve_after t
 let tvdep = tvdependentset tve_before tve_after
 let fv    = filter (not . tvdep) $ nub $ freevars t'
 return $ QPType fv t 
 -- ^ We return an unsubstituted type to preserve type variables
 --   for late binding based on overriding unification performed
 --   in function application.
 --   Old implementation: return $ QPType fv t'

monomorphize :: (Monad m) => K3 QType -> m QPType
monomorphize t = return $ QPType [] t

inferProgramTypes :: K3 Declaration -> Either String (K3 Declaration)
inferProgramTypes prog = do
    ((initEnv, _), ntve) <- let (a,b) = runTVEnvM tvenv0 $ initializeTypeEnv
                            in a >>= return . (, b)
    (_, nProg) <- fst $ runTVEnvM ntve $ foldProgram declF annMemF exprF initEnv prog
    return nProg
  where
    initializeTypeEnv :: TVEnvM (TIEnv, K3 Declaration)
    initializeTypeEnv = foldProgram initDeclF initAMemF initExprF tienv0 prog

    initDeclF :: TIEnv -> K3 Declaration -> TVEnvM (TIEnv, K3 Declaration)
    initDeclF env d@(tag -> DGlobal n t _) =
      if isTFunction t then qpType env t >>= \qpt -> return (tiexte env n qpt, d)
                       else return (env, d)
    
    initDeclF env d@(tag -> DTrigger n t _) =
      trigType t >>= \qpt -> return (tiexte env n qpt, d)
      where trigType x = qType env x >>= \qt -> return (ttrg qt) >>= monomorphize

    initDeclF env d@(tag -> DAnnotation n tdeclvars mems) = mkAnnMemEnv >>= \at -> return (at, d)
      where mkAnnMemEnv = mapM memType mems >>= return . tiexta env n . catMaybes
            memType (Lifted      _ mn mt _ _) = unifyMemInit True  mn mt
            memType (Attribute   _ mn mt _ _) = unifyMemInit False mn mt
            memType (MAnnotation _ _ _) = return Nothing
            unifyMemInit lifted mn mt = do
              qpt <- qpType env (TC.forAll tdeclvars mt)
              return (Just (mn, (qpt, lifted)))

    initDeclF env d = return (env, d)

    initAMemF :: TIEnv -> AnnMemDecl -> TVEnvM (TIEnv, AnnMemDecl)
    initAMemF env mem  = return (env, mem)

    initExprF :: TIEnv -> K3 Expression -> TVEnvM (TIEnv, K3 Expression)
    initExprF env expr = return (env, expr)

    unifyInitializer :: TIEnv -> Identifier -> Either (Maybe QPType) QPType -> Maybe (K3 Expression)
                     -> TVEnvM TIEnv
    unifyInitializer env n qptE eOpt = do
      (qpt, r) <- case qptE of
                    Left (Nothing)   -> liftEitherM (tilkupe env n) >>= \qpt' -> return (qpt', env)
                    Left (Just qpt') -> return (qpt', tiexte env n qpt')
                    Right qpt'       -> return (qpt', env)
      case (qpt, eOpt) of
        (QPType [] qt1, Just e) -> do
          qt2 <- qTypeOfM e
          void $ unifyM qt1 qt2 unifyInitErrF
          return r

        (_, Nothing) -> return r
        (_, _) -> polyTypeErr

    declF :: TIEnv -> K3 Declaration -> TVEnvM (TIEnv, K3 Declaration)
    declF env d@(tag -> DGlobal n t eOpt) = do
      qptE <- if isTFunction t then return (Left Nothing)
                               else (qpType env t >>= return . Left . Just)
      if isTEndpoint t then return (env, d)
                       else unifyInitializer env n qptE eOpt >>= return . (,d)
    
    declF env d@(tag -> DTrigger n _ e) =
      liftEitherM (tilkupe env n) >>= \(QPType qtvars qt) -> 
        case tag qt of
          QTCon QTTrigger -> let nqptE = Right $ QPType qtvars $ tfun (head $ children qt) tunit
                             in unifyInitializer env n nqptE (Just e) >>= return . (,d)
          _ -> trigTypeErr n
    
    declF env d@(tag -> DAnnotation n _ mems) =
        liftEitherM (tilkupa env n) >>= mkAnnMemEnv >>= \at -> return (at, d)
      where mkAnnMemEnv amEnv = mapM_ (memType amEnv) mems >> return env
            memType amEnv (Lifted      _ mn _ meOpt _) = unifyMemInit amEnv mn meOpt
            memType amEnv (Attribute   _ mn _ meOpt _) = unifyMemInit amEnv mn meOpt
            memType _ (MAnnotation _ _ _) = return ()
            unifyMemInit amEnv mn meOpt = 
              maybe (memLookupErr mn) (return . fst) (lookup mn amEnv) >>=
                \qpt -> (void $ unifyInitializer env mn (Right qpt) meOpt)

    declF env d = return (env, d)

    annMemF :: TIEnv -> AnnMemDecl -> TVEnvM (TIEnv, AnnMemDecl)
    annMemF env mem = return (env, mem)

    exprF :: TIEnv -> K3 Expression -> TVEnvM (TIEnv, K3 Expression)
    exprF env e = inferExprTypes env e >>= return . (env,)

    memLookupErr n = left $ "No annotation member in initial environment: " ++ n
    polyTypeErr   = left $ "Invalid polymorphic declaration type"
    trigTypeErr n = left $ "Invlaid trigger declaration type for: " ++ n
    unifyInitErrF s = "Failed to unify initializer: " ++ s

inferExprTypes :: TIEnv -> K3 Expression -> TVEnvM (K3 Expression)
inferExprTypes tienv expr = do
    nexpr <- mapIn1RebuildTree lambdaBinding sidewaysBinding inferQType tienv expr
    tve   <- get
    return $ exprQtSub tve nexpr

  where
    exprQtSub :: TVEnv -> K3 Expression -> K3 Expression
    exprQtSub tve e = runIdentity $ mapTree subNode e
      where subNode ch (Node (tg :@: anns) _) = return $ Node (tg :@: map subAnns anns) ch
            subAnns (EQType qt) = EQType $ tvsub tve qt
            subAnns x = x

    extMonoQT :: TIEnv -> Identifier -> K3 QType -> TVEnvM TIEnv
    extMonoQT env i t = monomorphize t >>= return . tiexte env i

    lambdaBinding :: TIEnv -> K3 Expression -> K3 Expression -> TVEnvM TIEnv
    lambdaBinding env _ (tag -> ELambda i) = newtv >>= extMonoQT env i
    lambdaBinding env _ _ = return env

    sidewaysBinding :: TIEnv -> K3 Expression -> K3 Expression -> TVEnvM (TIEnv, [TIEnv])
    sidewaysBinding env ch1 (tag -> ELetIn i) = do
      ipt <- generalize $ qTypeOfM ch1
      let nEnv = tiexte env i ipt
      return (env, [nEnv])

    sidewaysBinding env ch1 (tag -> EBindAs b) = do
        ch1T <- qTypeOfM ch1
        nEnv <- case b of
                  BIndirection i -> do
                    itv <- newtv
                    void $ unifyM ch1T (tind itv) $ bindErr "indirection"
                    extMonoQT env i itv
                  
                  BTuple ids -> do
                    idtvs <- mapM (const newtv) ids
                    void  $  unifyM ch1T (ttup idtvs) $ bindErr "tuple"
                    foldM (\x (y,z) -> extMonoQT x y z) env $ zip ids idtvs

                  -- TODO: partial bindings?
                  BRecord ijs -> do
                    jtvs <- mapM (const newtv) ijs
                    void $  unifyM ch1T (trec $ flip zip jtvs $ map fst ijs) $ bindErr "record"
                    foldM (\x (y,z) -> extMonoQT x y z) env $ flip zip jtvs $ map snd ijs

        return (env, [nEnv])

      where
        bindErr kind reason = unwords ["Invalid", kind, "bind-as:", reason]
    
    sidewaysBinding env ch1 (tag -> ECaseOf i) = do
      ch1T <- qTypeOfM ch1
      itv  <- newtv
      void $  unifyM ch1T (topt itv) $ (("Invalid case-of source expression: ")++)
      nEnv <- extMonoQT env i itv
      return (env, [nEnv, env])
    
    sidewaysBinding env _ (children -> ch) = return (env, replicate (length ch - 1) env)

    inferQType :: TIEnv -> [K3 Expression] -> K3 Expression -> TVEnvM (K3 Expression)
    inferQType _ _ n@(tag -> EConstant (CBool   _)) = return $ n .+ tbool
    inferQType _ _ n@(tag -> EConstant (CByte   _)) = return $ n .+ tbyte
    inferQType _ _ n@(tag -> EConstant (CInt    _)) = return $ n .+ tint
    inferQType _ _ n@(tag -> EConstant (CReal   _)) = return $ n .+ treal
    inferQType _ _ n@(tag -> EConstant (CString _)) = return $ n .+ tstr

    inferQType _ _ n@(tag -> EConstant (CNone nm)) = do
      tv <- newtv
      let ntv = case nm of { NoneMut -> mutQT tv; NoneImmut -> immutQT tv }
      return $ n .+ (topt ntv)

    inferQType env _ n@(tag -> EConstant (CEmpty  t)) = do
        cqt <- qType env t
        let annIds =  namedEAnnotations $ annotations n
        memEnvs <- either left return $ mapM (tilkupa tienv) annIds
        colqt   <- mkCollectionQType annIds memEnvs cqt
        return $ n .+ colqt

    -- | Variable specialization. Note that instantiate strips qualifiers.
    inferQType env _ n@(tag -> EVariable i) =
        either (lookupError i) instantiate (tilkupe env i) >>= return . (n .+)
      where lookupError j reason = left $ unwords ["No type environment binding for ", j, ":", reason]

    -- | Data structures. Qualifiers are taken from child expressions by rebuildE.
    inferQType _ ch n@(tag -> ESome)       = qTypeOfM (head ch) >>= return . ((rebuildE n ch) .+) . topt
    inferQType _ ch n@(tag -> EIndirect)   = qTypeOfM (head ch) >>= return . ((rebuildE n ch) .+) . tind
    inferQType _ ch n@(tag -> ETuple)      = mapM qTypeOfM ch   >>= return . ((rebuildE n ch) .+) . ttup 
    inferQType _ ch n@(tag -> ERecord ids) = mapM qTypeOfM ch   >>= return . ((rebuildE n ch) .+) . trec . zip ids

    -- | Lambda expressions are passed the post-processed environment, 
    --   so the type variable for the identifier is bound in the type environment.
    inferQType env ch n@(tag -> ELambda i) = do
        ipt  <- either lambdaBindingErr return $ tilkupe env i
        chqt <- qTypeOfM $ head ch
        case ipt of 
          QPType [] iqt -> return $ rebuildE n ch .+ tfun iqt chqt
          _             -> polyBindingErr
      where lambdaBindingErr reason = left $ unwords ["Could not find typevar for lambda binding: ", i, reason]
            polyBindingErr = left "Invalid forall type in lambda binding"

    -- | Assignment expressions unify their source and target types, as well as 
    --   ensuring that the source is mutable.
    inferQType env ch n@(tag -> EAssign i) = do
      ipt <- either assignBindingErr return $ tilkupe env i
      eqt <- qTypeOfM $ head ch
      case ipt of 
        QPType [] iqt
          | (iqt @~ isQTQualified) == Just QTMutable ->
              do { void $ unifyM iqt eqt (("Invalid assignment to " ++ i ++ ": ") ++);
                   return $ rebuildE n ch .+ tunit }
          | otherwise -> mutabilityErr i
        
        _ -> polyBindingErr
      where assignBindingErr reason = left $ unwords ["Could not find binding type for assignment: ", i, reason]
            polyBindingErr          = left "Invalid forall type in assignment"
            mutabilityErr j         = left $ "Invalid assigment to non-mutable binding: " ++ j

    inferQType _ ch n@(tag -> EProject i) = do
      srcqt   <- qTypeOfM $ head ch
      fieldqt <- newtv
      void    $  unifyM srcqt (tlower $ [trec [(i, fieldqt)]]) (("Invalid record projection: ")++)
      return  $  rebuildE n ch .+ fieldqt

    -- TODO: reorder inferred record fields based on argument at application.
    inferQType _ ch n@(tag -> EOperate OApp) = do
      fnqt   <- qTypeOfM $ head ch
      argqt  <- qTypeOfM $ last ch
      retqt  <- newtv
      void   $  unifyWithOverrideM fnqt (tfun argqt retqt) (("Invalid function application: ") ++)
      return $  rebuildE n ch .+ retqt

    inferQType _ ch n@(tag -> EOperate OSeq) = do
        lqt <- qTypeOfM $ head ch
        rqt <- qTypeOfM $ last ch
        void $ unifyM tunit lqt (("Invalid left sequence operand: ") ++)
        return $ rebuildE n ch .+ rqt

    -- | Check trigger-address pair and unify trigger type and message argument.
    inferQType _ ch n@(tag -> EOperate OSnd) = do
        trgtv <- newtv
        void $ unifyBinaryM (ttup [ttrg trgtv, taddr]) trgtv ch sndError
        return $ rebuildE n ch .+ tunit
      where sndError side reason = "Invalid " ++ side ++ " send operand: " ++ reason

    -- | Unify operand types based on the kind of operator.
    inferQType _ ch n@(tag -> EOperate op) 
      | numeric op = do
            (lqt, rqt) <- unifyBinaryM tnum tnum ch numericError
            resultqt   <- delayNumericQt lqt rqt
            return $ rebuildE n ch .+ resultqt

      | comparison op || logic op = do
            void $ unifyBinaryM tbool tbool ch boolError
            return $ rebuildE n ch .+ tbool

      | textual op = do
            void $ unifyBinaryM tstr tstr ch stringError
            return $ rebuildE n ch .+ tstr

      | op == ONeg = do
            chqt <- unifyUnaryM tnum ch (("Invalid unary minus operand: ") ++)
            let resultqt = case tag chqt of
                             QTPrimitive _  -> chqt
                             QTVar _ -> chqt
                             _ -> tnum
            return $ rebuildE n ch .+ resultqt

      | op == ONot = do
            void $ unifyUnaryM tbool ch (("Invalid negation operand: ") ++)
            return $ rebuildE n ch .+ tbool

      | otherwise = left $ "Invalid operation: " ++ show op

      where 
        delayNumericQt l r
          | or (map isQTVar   [l, r]) = return $ tlower [l,r]
          | or (map isQTLower [l, r]) = return $ tlower $ concatMap childrenOrSelf [l,r]
          | otherwise = get >>= \tve -> return $ tvlower tve l r

        childrenOrSelf t@(tag -> QTOperator QTLower) = children t
        childrenOrSelf t = [t]

        numericError side reason = "Invalid " ++ side ++ " numeric operand: " ++ reason
        stringError side reason = "Invalid " ++ side ++ " string operand: " ++ reason
        boolError side reason =
          let kind = if comparison op then "comparsion" else "logic" 
          in unwords ["Invalid", side, kind, "operand:", reason]

    -- First child generation has already been performed in sidewaysBinding
    inferQType _ ch n@(tag -> ELetIn _) = do
      bqt <- qTypeOfM $ last ch
      return $ rebuildE n ch .+ bqt
    
    -- First child unification has already been performed in sidewaysBinding
    inferQType _ ch n@(tag -> EBindAs _) = do
      bqt <- qTypeOfM $ last ch
      return $ rebuildE n ch .+ bqt
    
    -- First child unification has already been performed in sidewaysBinding
    inferQType _ ch n@(tag -> ECaseOf _) = do
      sqt   <- qTypeOfM $ ch !! 1
      nqt   <- qTypeOfM $ last ch
      retqt <- unifyWithOverrideM sqt nqt (("Mismatched case-of branch types: ") ++)
      return $ rebuildE n ch .+ retqt

    inferQType _ ch n@(tag -> EIfThenElse) = do
      pqt   <- qTypeOfM $ head ch
      tqt   <- qTypeOfM $ ch !! 1
      eqt   <- qTypeOfM $ last ch
      void  $  unifyM pqt tbool $ (("Invalid if-then-else predicate: ") ++)
      retqt <- unifyWithOverrideM tqt eqt $ (("Mismatched condition branches: ") ++)
      return $ rebuildE n ch .+ retqt

    inferQType _ _ n@(tag -> EAddress) = return $ n .+ taddr

    inferQType _ ch n  = return $ rebuildE n ch
      -- ^ TODO unhandled: ESelf, EImperative

    rebuildE (Node t _) ch = Node t ch

    unifyBinaryM lexpected rexpected ch errf = do
      lqt <- qTypeOfM $ head ch
      rqt <- qTypeOfM $ last ch
      void $ unifyM lexpected lqt (errf "left")
      void $ unifyM rexpected rqt (errf "right")
      return (lqt, rqt)

    unifyUnaryM expected ch errf = do
        chqt <- qTypeOfM $ head ch
        void $ unifyM chqt expected errf
        return chqt

    numeric    op = op `elem` [OAdd, OSub, OMul, ODiv, OMod]
    comparison op = op `elem` [OEqu, ONeq, OLth, OLeq, OGth, OGeq]
    logic      op = op `elem` [OAnd, OOr]
    textual    op = op `elem` [OConcat]


{- Collection type construction -}
mkCollectionQType :: [Identifier] -> [TMEnv] -> K3 QType -> TVEnvM (K3 QType)
mkCollectionQType annIds memEnvs contentQt = do
    flatEnvs <- assertNoDuplicateIds
    let (lifted, regular) = partition (snd . snd) flatEnvs
    finalQt <- case tag contentQt of
                 QTCon (QTRecord ids) ->
                    return $ trec $
                      (zip ids $ children contentQt) ++ (membersAsRecordFields regular)
                 _ -> nonRecordContentErr contentQt
    let selfQt = trec $ membersAsRecordFields lifted
    let nselfQt = subCTVars contentQt finalQt selfQt
    return $ tcol contentQt finalQt nselfQt annIds
  where 
    subCTVars ct ft st = runIdentity $ mapTree (subCF ct ft) st
    subCF ct _ _ (tag -> QTContent) = return ct
    subCF _ ft _ (tag -> QTFinal)   = return ft
    subCF _ _ ch (Node t _) = return $ Node t ch

    assertNoDuplicateIds = 
      let flatEnvs = concat memEnvs
          ids      = map fst flatEnvs
      in if nub ids /= ids then nameConflictErr else return flatEnvs
    
    membersAsRecordFields attrs = map (\(j,(QPType _ qt,_)) -> (j,qt)) attrs 
      -- ^ TODO: handle free vars?

    nameConflictErr        = left $ "Conflicting annotation member names: " ++ show annIds
    nonRecordContentErr qt = left $ "Invalid content record type: " ++ show qt


{- Type conversion -}

qpType :: TIEnv -> K3 Type -> TVEnvM QPType

-- | At top level foralls, we extend the declared var env in the type inference
--   environment with fresh qtype variables. This leads to substitutions for any
--   corresponding declared variables in the type tree.
qpType tienv t@(tag -> TForall tvars) = do
  tvmap <- mapM (\(TypeVarDecl i _ _) -> newtv >>= varId >>= return . (i,)) tvars
  let ntienv = (foldl (\a (b,c) -> tiextdv a b c) tienv tvmap)
  chQt <- qType ntienv (head $ children t)
  return $ QPType (map snd tvmap) chQt

  where varId (tag -> QTVar i) = return i
        varId _ = left $ "Invalid type variable for type var bindings"

qpType tienv t = --qType tienv t >>= monomorphize -- TODO: generalize?
  generalize (qType tienv t)

-- | We currently do not support forall type quantifiers present at an
--   arbitrary location in the K3 Type tree since forall types are not
--   part of the QType datatype and grammar.
--   The above qpType method constructs polymorphic QTypes, which handles
--   top-level polymorphic types, creating mappings for declared variables
--   in a K3 Type to QType typevars.
--   
qType :: TIEnv -> K3 Type -> TVEnvM (K3 QType)
qType tienv t = foldMapTree mkQType (ttup []) t >>= return . mutabilityT t
  where 
    mkQType _ (tag -> TTop)    = return ttop
    mkQType _ (tag -> TBottom) = return tbot

    mkQType _ (tag -> TBool)    = return tbool
    mkQType _ (tag -> TByte)    = return tbyte
    mkQType _ (tag -> TInt)     = return tint
    mkQType _ (tag -> TReal)    = return treal
    mkQType _ (tag -> TString)  = return tstr
    mkQType _ (tag -> TNumber)  = return tnum
    mkQType _ (tag -> TAddress) = return taddr

    mkQType ch n@(tag -> TOption)       = return $ topt $ mutability0 ch n
    mkQType ch n@(tag -> TIndirection)  = return $ tind $ mutability0 ch n
    mkQType ch n@(tag -> TTuple)        = return $ ttup $ mutabilityN ch n
    mkQType ch n@(tag -> TRecord ids)   = return $ trec $ zip ids $ mutabilityN ch n

    mkQType ch n@(tag -> TCollection) = do
        let cqt = head ch
        let annIds = namedTAnnotations $ annotations n
        memEnvs  <- mapM (liftEitherM . tilkupa tienv) annIds
        mkCollectionQType annIds memEnvs cqt
    
    mkQType ch (tag -> TFunction) = return $ tfun (head ch) $ last ch
    mkQType ch (tag -> TTrigger)  = return $ ttrg $ head ch
    mkQType ch (tag -> TSource)   = return $ tsrc $ head ch
    mkQType ch (tag -> TSink)     = return $ tsnk $ head ch
    
    mkQType _ (tag -> TBuiltIn TContent)   = return tcontent
    mkQType _ (tag -> TBuiltIn TStructure) = return tfinal
    mkQType _ (tag -> TBuiltIn TSelf)      = return tself

    mkQType _ (tag -> TDeclaredVar x) = liftEitherM (tilkupdv tienv x)

    mkQType _ (tag -> TForall _) = left $ "Invalid forall type for QType"
      -- ^ TODO: we can only handle top-level foralls, and not arbitrary
      --   foralls nested in type trees.

    mkQType _ t_ = left $ "No QType construction for " ++ show t_

    mutability0 nch n = mutabilityT (head $ children n) $ head nch
    mutabilityN nch n = map (uncurry mutabilityT) $ zip (children n) nch