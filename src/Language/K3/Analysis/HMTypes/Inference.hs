{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TemplateHaskell #-}
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

import Data.Function
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

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

logVoid :: (Functor m, Monad m) => String -> m ()
--logVoid s = void $ _debug s
logVoid s = trace s $ return ()

-- | Misc. helpers
logAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
logAction msgF action = do
  doLog (msgF Nothing)
  result <- action
  doLog (msgF $ Just result)
  return result
  where doLog Nothing  = return ()
        doLog (Just s) = logVoid s

(.+) :: K3 Expression -> K3 QType -> K3 Expression
(.+) e qt = e @+ (EQType $ mutabilityE e qt)

infixr 5 .+

immutQT :: K3 QType -> K3 QType
immutQT = (@+ QTImmutable)

mutQT :: K3 QType -> K3 QType
mutQT = (@+ QTMutable)

mutabilityT :: K3 Type -> K3 QType -> K3 QType
mutabilityT t qt = maybe propagate (const qt) $ find isQTQualified $ annotations qt
  where propagate = case t @~ isTQualified of
          Nothing -> qt
          Just TImmutable -> immutQT qt
          Just TMutable   -> mutQT qt
          _ -> error "Invalid type qualifier annotation"

mutabilityE :: K3 Expression -> K3 QType -> K3 QType
mutabilityE e qt = maybe propagate (const qt) $ find isQTQualified $ annotations qt
  where propagate = case e @~ isEQualified of
          Nothing -> qt
          Just EImmutable -> immutQT qt
          Just EMutable   -> mutQT qt
          _ -> error "Invalid expression qualifier annotation"

qTypeOf :: K3 Expression -> Maybe (K3 QType)
qTypeOf e = case e @~ isEQType of
              Just (EQType qt) -> Just qt
              _ -> Nothing

qTypeOfM :: K3 Expression -> TInfM (K3 QType)
qTypeOfM e = case e @~ isEQType of
              Just (EQType qt) -> return qt
              _ -> left $ "Untyped expression: " ++ show e

projectNamedPairs :: [Identifier] -> [(Identifier, a)] -> [a]
projectNamedPairs ids idv = [v | i <- ids, let (Just v) = lookup i idv]

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

-- | A type variable environment.
data TVEnv = TVEnv QTVarId (Map QTVarId (K3 QType)) deriving Show

-- | A type inference environment.
type TIEnv = (TEnv, TAEnv, TDVEnv, TVEnv)

-- | The type inference monad
type TInfM = EitherT String (State TIEnv)

{- TEnv helpers -}
tenv0 :: TEnv
tenv0 = []

tlkup :: TEnv -> Identifier -> Either String QPType
tlkup env x = maybe err Right $ lookup x env
 where err = Left $ "Unbound variable in type environment: " ++ x

text :: TEnv -> Identifier -> QPType -> TEnv
text env x t = (x,t) : env

tdel :: TEnv -> Identifier -> TEnv
tdel env x = deleteBy ((==) `on` fst) (x, QPType [] tint) env


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

tdvdel :: TDVEnv -> Identifier -> TDVEnv
tdvdel env x = deleteBy ((==) `on` fst) (x,-1) env

{- TIEnv helpers -}
tienv0 :: TIEnv
tienv0 = (tenv0, taenv0, tdvenv0, tvenv0)

-- | Getters.
tiee :: TIEnv -> TEnv
tiee (te,_,_,_) = te

tiae :: TIEnv -> TAEnv
tiae (_,ta,_,_) = ta

tidve :: TIEnv -> TDVEnv
tidve (_,_,tdv,_) = tdv

tive :: TIEnv -> TVEnv
tive (_,_,_,tv) = tv

-- | Modifiers.
mtiee :: (TEnv -> TEnv) -> TIEnv -> TIEnv
mtiee f (te,x,y,z) = (f te, x, y, z)

mtiae :: (TAEnv -> TAEnv) -> TIEnv -> TIEnv
mtiae f (x,ta,y,z) = (x, f ta, y, z)

mtidve :: (TDVEnv -> TDVEnv) -> TIEnv -> TIEnv
mtidve f (x,y,tdv,z) = (x, y, f tdv, z)

mtive :: (TVEnv -> TVEnv) -> TIEnv -> TIEnv
mtive f (x,y,z,tv) = (x, y, z, f tv)

tilkupe :: TIEnv -> Identifier -> Either String QPType
tilkupe (te,_,_,_) x = tlkup te x

tilkupa :: TIEnv -> Identifier -> Either String TMEnv
tilkupa (_,ta,_,_) x = talkup ta x

tilkupdv :: TIEnv -> Identifier -> Either String (K3 QType)
tilkupdv (_,_,tdv,_) x = tdvlkup tdv x

tiexte :: TIEnv -> Identifier -> QPType -> TIEnv
tiexte (te,ta,tdv,tv) x t = (text te x t, ta, tdv, tv)

tiexta :: TIEnv -> Identifier -> TMEnv -> TIEnv
tiexta (te,ta,tdv,tv) x ate = (te, taext ta x ate, tdv, tv)

tiextdv :: TIEnv -> Identifier -> QTVarId -> TIEnv
tiextdv (te, ta, tdv, tv) x v = (te, ta, tdvext tdv x v, tv)

tidele :: TIEnv -> Identifier -> TIEnv
tidele (te,x,y,z) i = (tdel te i,x,y,z)

tideldv :: TIEnv -> Identifier -> TIEnv
tideldv (x,y,tdv,z) i = (x,y,tdvdel tdv i,z)

tiincrv :: TIEnv -> (QTVarId, TIEnv)
tiincrv (te, ta, tdv, TVEnv n s) = (n, (te, ta, tdv, TVEnv (succ n) s))


{- TVEnv helpers -}
tvenv0 :: TVEnv
tvenv0 = TVEnv 0 Map.empty

tvlkup :: TVEnv -> QTVarId -> Maybe (K3 QType)
tvlkup (TVEnv _ s) v = Map.lookup v s

tvext :: TVEnv -> QTVarId -> K3 QType -> TVEnv
tvext (TVEnv c s) v t = TVEnv c $ Map.insert v t s

tvmap :: TVEnv -> (K3 QType -> K3 QType) -> TVEnv
tvmap (TVEnv c s) f = TVEnv c $ Map.map f s

-- TVE domain predicate: check to see if a TVarName is in the domain of TVE
tvdomainp :: TVEnv -> QTVarId -> Bool
tvdomainp (TVEnv _ s) v = Map.member v s

-- Give the list of all type variables that are allocated in TVE but
-- not bound there
tvfree :: TVEnv -> [QTVarId]
tvfree (TVEnv c s) = filter (\v -> not (Map.member v s)) [0..c-1]

-- `Shallow' substitution
tvchase :: TVEnv -> K3 QType -> K3 QType
tvchase tve t = acyclicChase [] t
  where acyclicChase path qt@(tag -> QTVar v)
          | v `elem` path = qt
          | Just nt <- tvlkup tve v = acyclicChase (v:path) nt
        acyclicChase _ qt = qt

-- 'Shallow' substitution, additionally returning the last variable in
--  the chased chain.
tvchasev :: TVEnv -> Maybe (QTVarId, QTVarId) -> K3 QType -> (Maybe (QTVarId, QTVarId), K3 QType)
tvchasev tve firstAndLastV t = acyclicChasev [] firstAndLastV t
  where acyclicChasev path flv qt@(tag -> QTVar v)
          | v `elem` path            = (flv, qt)
          | Just ctv <- tvlkup tve v = acyclicChasev (v:path) (extendFirstLast flv v) ctv
          | otherwise                = (extendFirstLast flv v, qt)

        acyclicChasev _ flv qt = (flv, qt)

        extendFirstLast flv v = maybe (Just (v,v)) (\(u,_) -> Just (u,v)) flv

tvpath :: TVEnv -> QTVarId -> [QTVarId]
tvpath tve v = acyclicPath [] v
  where acyclicPath acc v' | v' `elem` acc = acc
                           | Just qt <- tvlkup tve v'
                           , QTVar nv <- tag qt = acyclicPath (v':acc) nv
        acyclicPath acc v' = v':acc

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

-- The occurs check: if v appears free in t
occurs :: QTVarId -> K3 QType -> TVEnv -> Bool
occurs v t tve = acyclicOccurs [] t
  where acyclicOccurs path qt@(tag -> QTCon _)      = or $ map (acyclicOccurs path) $ children qt
        acyclicOccurs path qt@(tag -> QTOperator _) = or $ map (acyclicOccurs path) $ children qt
        acyclicOccurs path (tag -> QTVar v2)
          | v == v2        = True
          | v2 `elem` path = False
          | otherwise      = maybe False (acyclicOccurs $ v2:path) $ tvlkup tve v2
        acyclicOccurs _ _ = False


{- TInfM helpers -}

runTInfM :: TIEnv -> TInfM a -> (Either String a, TIEnv)
runTInfM env m = flip runState env $ runEitherT m

reasonM :: (String -> String) -> TInfM a -> TInfM a
reasonM errf = mapEitherT $ \m -> m >>= \case
  Left  err -> get >>= \env -> (return . Left $ errf $ err ++ "\nType environment:\n" ++ pretty env)
  Right r   -> return $ Right r

liftEitherM :: Either String a -> TInfM a
liftEitherM = either left return

getTVE :: TInfM TVEnv
getTVE = get >>= return . tive

-- Allocate a fresh type variable
newtv :: TInfM (K3 QType)
newtv = do
  (nv, nenv) <- get >>= return . tiincrv
  put nenv
  return $ tvar nv


-- Deep substitute, throughout type structure
tvsub :: K3 QType -> TInfM (K3 QType)
tvsub qt = acyclicSub [] qt
  where acyclicSub path t@(tag -> QTVar v)
          | v `elem` path = return t
          | otherwise = getTVE >>= \tve ->
              case tvlkup tve v of
                Just t' -> acyclicSub (v:path) t' >>= flip extendAnns t
                _       -> return t

        acyclicSub path t@(tag -> QTCon d) = do
          ch <- mapM (acyclicSub path) $ children t
          return $ foldl (@+) (tdata d ch) $ annotations t

        acyclicSub path t@(tag -> QTOperator QTLower) = do
          ch <- mapM (acyclicSub path) $ children t
          if null ch then left "Invalid qtype lower operator"
          else if null $ concatMap freevars ch then tvopeval QTLower ch >>= flip extendAnns t
          else return $ foldl (@+) (tlower ch) $ annotations t

        acyclicSub _ t = return t
        extendAnns t1 t2 = return $ foldl (@+) t1 $ annotations t2 \\ annotations t1


-- | Lower bound computation for numeric and record types.
--   This function does not preserve annotations.
tvlower :: K3 QType -> K3 QType -> TInfM (K3 QType)
tvlower a b = getTVE >>= \tve -> tvshallowLowerRcr tvlower (tvchase tve a) (tvchase tve b)

tvshallowLower :: K3 QType -> K3 QType -> TInfM (K3 QType)
tvshallowLower a b = tvshallowLowerRcr tvshallowLower a b

tvshallowLowerRcr :: (K3 QType -> K3 QType -> TInfM (K3 QType))
                  -> K3 QType -> K3 QType -> TInfM (K3 QType)
tvshallowLowerRcr rcr a b =
    case (tag a, tag b) of
      (QTPrimitive p1, QTPrimitive p2)
        | [p1, p2] `intersect` [QTReal, QTInt, QTNumber] == [p1,p2] ->
            annLower a b >>= return . foldl (@+) (tprim $ toEnum $ minimum $ map fromEnum [p1,p2])
        | p1 == p2 -> return a

      (QTCon (QTRecord i1), QTCon (QTRecord i2))
        | i1 `contains` i2 -> mergedRecord False i1 a i2 b
        | i2 `contains` i1 -> mergedRecord True  i2 b i1 a
        | otherwise ->
            let idChPairs = zip (i1 ++ i2) $ children a ++ children b
            in annLower a b >>= return . foldl (@+) (trec $ nub idChPairs)
       where
        contains xs ys = xs `union` ys == xs

      (QTCon (QTCollection _), QTCon (QTRecord _)) -> coveringCollection a b
      (QTCon (QTRecord _), QTCon (QTCollection _)) -> coveringCollection b a

      (QTCon (QTCollection idsA), QTCon (QTCollection idsB))
        | idsA `intersect` idsB == idsA -> mergedCollection idsB a b
        | idsA `intersect` idsB == idsB -> mergedCollection idsA a b

      (QTVar _, QTVar _) -> return a
      (QTVar _, _) -> return a
      (_, QTVar _) -> return b

      -- | Function type lower bounds
      (QTCon QTFunction, QTCon QTFunction) -> do
        arga  <- tvsub $ head $ children a
        argb  <- tvsub $ head $ children b
        retlb <- rcr (last $ children a) $ last $ children b
        if arga /= argb
          then lowerError a b
          else annLower a b >>= return . foldl (@+) (tfun (head $ children a) retlb)

      -- | Self type lower bounds
      (QTSelf, QTSelf)                 -> return a
      (QTCon (QTCollection _), QTSelf) -> return a
      (QTSelf, QTCon (QTCollection _)) -> return b

      (_, _)
        | (isQTLower a && isQTLower b) || isQTLower a || isQTLower b -> do
          lb1 <- lowerBound a
          lb2 <- lowerBound b
          nlb <- rcr lb1 lb2
          if isQTLower a && isQTLower b
            then return $ tlower [nlb]
            else return nlb

        | a == b  -> return a
        | otherwise -> lowerError a b

  where
    mergedRecord supAsLeft subid subqt supid supqt = do
      fieldQt <- mergeCovering supAsLeft
                  (zip subid $ children subqt) (zip supid $ children supqt)
      annLower subqt supqt >>= return . foldl (@+) (trec $ zip subid fieldQt)

    mergedCollection annIds ct1 ct2 = do
       ctntLower <- rcr (head $ children ct1) (head $ children ct2)
       annLower ct1 ct2 >>= return . foldl (@+) (tcol ctntLower annIds)

    mergeCovering supAsLeft sub sup =
      let lowerF = if supAsLeft then \subV supV -> rcr supV subV
                                else \subV supV -> rcr subV supV
      in mapM (\(k,v) -> maybe (return v) (lowerF v) $ lookup k sup) sub

    coveringCollection ct rt@(tag -> QTCon (QTRecord _)) =
      collectionSubRecord ct rt >>= either (const $ lowerError ct rt) (const $ return ct)

    coveringCollection x y = lowerError x y

    annLower x@(annotations -> annA) y@(annotations -> annB) =
      let annAB   = nub $ annA ++ annB
          invalid = [QTMutable, QTImmutable]
      in if invalid `intersect` annAB == invalid then lowerError x y else return annAB

    lowerBound t@(tag -> QTOperator QTLower) = tvopevalWithLowerF rcr QTLower $ children t
    lowerBound t = return t

    lowerError x y = left $ boxToString $
      ["Invalid lower bound on: "] %+ prettyLines x %+ [" and "] %+ prettyLines y

-- | Type operator evaluation.
tvopeval :: QTOp -> [K3 QType] -> TInfM (K3 QType)
tvopeval op ch = tvopevalWithLowerF tvlower op ch

tvopevalShallow :: QTOp -> [K3 QType] -> TInfM (K3 QType)
tvopevalShallow op ch = tvopevalWithLowerF tvshallowLower op ch

tvopevalWithLowerF :: (K3 QType -> K3 QType -> TInfM (K3 QType)) -> QTOp -> [K3 QType] -> TInfM (K3 QType)
tvopevalWithLowerF _ _ [] = left $ "Invalid qt operator arguments"
tvopevalWithLowerF lowerF QTLower ch = foldM lowerF (head ch) $ tail ch

consistentTLower :: [K3 QType] -> TInfM (K3 QType)
consistentTLower ch =
    let (varCh, nonvarCh) = partition isQTVar $ nub ch in
    case (varCh, nonvarCh) of
      ([], []) -> left "Invalid lower qtype"
      ([], _)  -> tvopevalShallow QTLower nonvarCh
      (_, _)   -> lowerBoundWithVars varCh nonvarCh

  where
    lowerBoundWithVars vars extraLBTypes = do
      (boundTypes, freeVars) <- foldM partitionBoundV ([],[]) vars
      lb <- tvopevalShallow QTLower (boundTypes ++ extraLBTypes)
      nch <- foldM unifyFreeVar [lb] $ nub freeVars
      return $ tlower nch

    partitionBoundV (tacc,vacc) t@(tag -> QTVar _) = do
      tve <- getTVE
      let bt = tvchase tve t
      case tag bt of
        QTVar _ -> return (tacc, vacc++[bt])
        _ -> return (tacc++[bt], vacc)

    partitionBoundV _ _ = left "Invalid type var during lower qtype merge"

    unifyFreeVar tacc t2@(tag -> QTVar v) = unifyv v (head tacc) >> return (t2:tacc)
    unifyFreeVar _ _ = left "Invalid type var during lower qtype merge"


-- Unification helpers.
-- | Returns a self record and lifted attribute identifiers when given
--   a collection and a record that is a subtype of the collection.
collectionSubRecord :: K3 QType -> K3 QType -> TInfM (Either (Maybe (K3 QType)) (K3 QType, [Identifier]))
collectionSubRecord ct@(tag -> QTCon (QTCollection annIds)) (tag -> QTCon (QTRecord ids))
  = get >>= mkColQT >>= return . testF
  where
    mkColQT tienv = do
      memEnvs <- mapM (liftEitherM . tilkupa tienv) annIds
      mkCollectionFSQType annIds memEnvs (last $ children ct)

    testF (_, self)
      | QTCon (QTRecord liftedAttrIds) <- tag self
      , ids `intersect` liftedAttrIds == ids
      = Right (self, liftedAttrIds)

    testF (_, self) = Left $ Just self

collectionSubRecord _ _ = return $ Left Nothing

-- Unify a free variable v1 with t2
unifyv :: QTVarId -> K3 QType -> TInfM ()
unifyv v1 t@(tag -> QTVar v2)
  | v1 == v2  = return ()
  | otherwise = getTVE >>= \tve ->
    if occurs v1 t tve
      then logVoid $ prettyTaggedSPair "unifyv cycle" v1 t
      else do { logVoid $ prettyTaggedSPair "unifyv var" v1 t;
                modify $ mtive $ \tve' -> tvext tve' v1 t }

unifyv v t = getTVE >>= \tve -> do
  if not $ occurs v t tve
    then do { logVoid $ prettyTaggedSPair "unifyv noc" v t;
              modify $ mtive $ \tve' -> tvext tve' v t }

    else do { subQt <- tvsub t;
              boundQt <- return $ substituteSelfQt t;
              logVoid $ prettyTaggedTriple (unwords ["unifyv yoc", show v]) t subQt boundQt;
              modify $ mtive $ \tve' -> tvext (tvmap tve' substituteSelfQt) v boundQt; }

  where
    substituteSelfQt t'@(tag -> QTVar _) = t'
    substituteSelfQt t' = aux t'
      where aux n@(Node (QTVar v2 :@: anns) _)
              | v == v2   = foldl (@+) tself anns
              | otherwise = n

            aux (Node tg ch) = Node tg (map aux ch)


-- | Unification driver type synonyms.
type RecordParts = (K3 QType, QTData, [Identifier])
type QTypeCtor = [K3 QType] -> K3 QType
type UnifyPreF  a = K3 QType -> TInfM (a, K3 QType)
type UnifyPostF a = (a, a) -> K3 QType -> TInfM (K3 QType)

-- | A unification driver, i.e., common unification code for both
--   our standard unification method, and unification with variable overrides
unifyDrv :: (Show a) => UnifyPreF a -> UnifyPostF a -> K3 QType -> K3 QType -> TInfM (K3 QType)
unifyDrv preF postF qt1 qt2 = unifyDrvWithPaths preF postF [] [] qt1 qt2

unifyDrvWithPaths :: (Show a) => UnifyPreF a -> UnifyPostF a
                  -> [QTVarId] -> [QTVarId] -> K3 QType -> K3 QType -> TInfM (K3 QType)
unifyDrvWithPaths preF postF path1 path2 qt1 qt2 =
    case (cyclicPath path1 qt1, cyclicPath path2 qt2) of
      (True, _) -> preF qt2 >>= return . snd
      (_, True) -> preF qt1 >>= return . snd
      (_, _) -> doUnify

  where
    doUnify = do
        (p1, qt1') <- preF qt1
        (p2, qt2') <- preF qt2
        qt         <- unifyDrv' qt1' qt2'
        logVoid $ prettyTaggedPair (unwords ["unifyDrvPreL", show p1]) qt1 qt1'
        logVoid $ prettyTaggedPair (unwords ["unifyDrvPreR", show p2]) qt2 qt2'
        logVoid $ prettyTaggedTriple "unifyDrv" qt1' qt2' qt
        postF (p1, p2) qt

    unifyDrv' :: K3 QType -> K3 QType -> TInfM (K3 QType)
    unifyDrv' t1@(isQTNumeric -> True) t2@(isQTNumeric -> True) = tvlower t1 t2

    unifyDrv' t1@(tag -> QTPrimitive p1) (tag -> QTPrimitive p2)
      | p1 == p2  = return t1
      | otherwise = primitiveErr p1 p2

    -- | Self type unification
    unifyDrv' t1@(tag -> QTCon (QTCollection _)) (tag -> QTSelf) = return t1
    unifyDrv' (tag -> QTSelf) t2@(tag -> QTCon (QTCollection _)) = return t2

    -- | Record subtyping for projection
    unifyDrv' t1@(tag -> QTCon d1@(QTRecord f1)) t2@(tag -> QTCon d2@(QTRecord f2))
      | f2 `contains` f1 = onRecord (t1,d1,f1) (t2,d2,f2)
      | f1 `contains` f2 = onRecord (t2,d2,f2) (t1,d1,f1)
     where
       contains xs ys = xs `union` ys == xs

    -- | Collection-as-record subtyping for projection
    unifyDrv' t1@(tag -> QTCon (QTCollection _)) t2@(tag -> QTCon (QTRecord _))
      = collectionSubRecord t1 t2 >>= \case
          Right (selfRecordQt, liftedAttrIds) -> onCollection selfRecordQt liftedAttrIds t1 t2
          Left (Just selfRecordQt)            -> unifyErr t1 t2 "collection-record" $ prettyTaggedPair "field-mismatch" selfRecordQt t2
          Left Nothing                        -> unifyErr t1 t2 "collection-record" ""

    unifyDrv' t1@(tag -> QTCon (QTRecord _)) t2@(tag -> QTCon (QTCollection _))
      = collectionSubRecord t2 t1 >>= \case
          Right (selfRecordQt, liftedAttrIds) -> onCollection selfRecordQt liftedAttrIds t2 t1
          Left (Just selfRecordQt)            -> unifyErr t1 t2 "collection-record" $ prettyTaggedPair "field-mismatch" selfRecordQt t1
          Left Nothing                        -> unifyErr t1 t2 "collection-record" ""

    unifyDrv' t1@(tag -> QTCon (QTCollection idsA)) t2@(tag -> QTCon (QTCollection idsB))
        | idsA `intersect` idsB == idsA = onCollectionPair idsB t1 t2
        | idsA `intersect` idsB == idsB = onCollectionPair idsA t1 t2

    unifyDrv' t1@(tag -> QTCon d1) t2@(tag -> QTCon d2) =
      onChildren d1 d2 "datatypes" (children t1) (children t2) (tdata d1)

    -- | Unification of a delayed LB operator applies to the lower bounds of each set.
    --   This returns a merged delayed LB operator containing all variables, and the lower
    --   bound of the non-variable elements.
    unifyDrv' t1@(tag -> QTOperator QTLower) t2@(tag -> QTOperator QTLower) = do
      lb1 <- lowerBound t1
      lb2 <- lowerBound t2
      lbs <- case (lb1, lb2) of
               (Node (QTCon (QTRecord _) :@: _) _, Node (QTCon (QTRecord _) :@: _) _) ->
               {- do
                    lb <- tvlower lb1 lb2
                    return $ if lb `elem` [lb1, lb2] then [lb1,lb2] else [lb,lb1,lb2]
               -}
                do
                  tienv <- get
                  let (lbE, _) = runTInfM tienv $ tvlower lb1 lb2
                  let validLB lb = if lb `elem` [lb1, lb2] then [lb1,lb2] else [lb,lb1,lb2]
                  return $ either (const $ [lb1,lb2]) validLB lbE

               (_,_) -> return [lb1, lb2]

      void $ foldM rcr (head $ lbs) $ tail lbs
      let allChQT = children t1 ++ children t2
      r <- logAction (binaryLowerMsgF allChQT) $ consistentTLower allChQT
      case tag r of
        QTOperator QTLower -> return r
        _ -> return $ tlower [r]

    unifyDrv' tv@(tag -> QTVar v) t = unifyv v t >> return tv
    unifyDrv' t tv@(tag -> QTVar v) = unifyv v t >> return tv

    -- | Unification of a lower bound with a non-bound applies to the non-bound
    --   and the lower bound of the deferred set.
    --   This pattern match applies after type variable unification to allow typevars
    --   to match the lower-bound set.
    unifyDrv' t1@(tag -> QTOperator QTLower) t2 = do
      lb1 <- lowerBound t1
      nlb <- rcr lb1 t2
      logAction (unaryLowerMsgF "L") $ consistentTLower $ children t1 ++ [nlb]

    unifyDrv' t1 t2@(tag -> QTOperator QTLower) = do
      lb2 <- lowerBound t2
      nlb <- rcr t1 lb2
      logAction (unaryLowerMsgF "R") $ consistentTLower $ [nlb] ++ children t2

    -- | Top unifies with any value. Bottom unifies with only itself. Self unifies with itself.
    unifyDrv' t1@(tag -> tg1) t2@(tag -> tg2)
      | tg1 == QTTop = return t2
      | tg2 == QTTop = return t1
      | tg1 == tg2   = return t1
      | otherwise    = unifyErr t1 t2 "qtypes" ""

    cyclicPath :: [QTVarId] -> K3 QType -> Bool
    cyclicPath path qt = case tag qt of
      QTVar v -> v `elem` path
      _ -> False

    extendPath :: [QTVarId] -> K3 QType -> [QTVarId]
    extendPath path qt = case tag qt of
      QTVar v -> v:path
      _ -> path

    rcr :: K3 QType -> K3 QType -> TInfM (K3 QType)
    rcr a b = do
      logVoid $ prettyTaggedPair "unifyDrv recurring from " qt1 qt2
      logVoid $ prettyTaggedPair "unifyDrv recurring on "   a   b
      let npath1 = extendPath path1 qt1
      let npath2 = extendPath path2 qt2
      unifyDrvWithPaths preF postF npath1 npath2 a b

    onCollectionPair :: [Identifier] -> K3 QType -> K3 QType -> TInfM (K3 QType)
    onCollectionPair annIds t1 t2 =
      rcr (head $ children t1) (head $ children t2) >>= return . flip tcol annIds

    onCollection :: K3 QType -> [Identifier] -> K3 QType -> K3 QType -> TInfM (K3 QType)
    onCollection sQt liftedAttrIds
                 ct@(tag -> QTCon (QTCollection _)) rt@(tag -> QTCon (QTRecord ids))
      = do
          subChQt <- mapM (substituteSelfQt ct) $ children sQt
          let selfPairs   = zip liftedAttrIds subChQt
          let projSelfT   = projectNamedPairs ids selfPairs
          let tdcon       = QTRecord liftedAttrIds
          let errk        = "collection subtype"
          let colCtor   _ = ct
          onChildren tdcon tdcon errk projSelfT (children rt) colCtor

    onCollection _ _ ct rt =
      left $ unlines ["Invalid collection arguments:", pretty ct, "and", pretty rt]

    onRecord :: RecordParts -> RecordParts -> TInfM (K3 QType)
    onRecord (supT, supCon, supIds) (subT, subCon, subIds) =
      let subPairs    = zip subIds $ children subT
          subProjT    = projectNamedPairs supIds $ subPairs
          errk        = "record subtype"
          recCtor nch = tdata subCon $ rebuildNamedPairs subPairs supIds nch
      in onChildren supCon supCon errk (children supT) subProjT recCtor

    onChildren :: QTData -> QTData -> String -> [K3 QType] -> [K3 QType] -> QTypeCtor
               -> TInfM (K3 QType)
    onChildren tga tgb kind a b ctor
      | tga == tgb = onList a b ctor $ \s -> unifyErr tga tgb kind s
      | otherwise  = unifyErr tga tgb kind ""

    onList :: [K3 QType] -> [K3 QType] -> QTypeCtor -> (String -> TInfM (K3 QType))
           -> TInfM (K3 QType)
    onList a b ctor errf =
      if length a == length b
        then mapM (uncurry rcr) (zip a b) >>= return . ctor
        else errf "Unification mismatch on lists."

    substituteSelfQt :: K3 QType -> K3 QType -> TInfM (K3 QType)
    substituteSelfQt ct@(tag -> QTCon (QTCollection _)) qt = mapTree sub qt
      where sub _ (tag -> QTSelf) = return $ ct
            sub ch (Node n _)     = return $ Node n ch

    substituteSelfQt ct _ = subSelfErr ct

    lowerBound t = tvopeval QTLower $ children t

    primitiveErr a b = unifyErr a b "primitives" ""

    unifyErr a b kind s = left $ boxToString $
      [unwords ["Unification mismatch on ", kind, ":("]] %$ indent 2 [s] %$ [")"] %$ (prettyLines a %+ [" "] %+ prettyLines b)

    subSelfErr ct = left $ boxToString $
      ["Invalid self substitution, qtype is not a collection: "] ++ prettyLines ct

    unaryLowerMsgF _ Nothing = Nothing
    unaryLowerMsgF suffix (Just r) =
      Just $ boxToString $ ["consistentTLower" ++ suffix ++ " "] %+ prettyLines r

    binaryLowerMsgF _ Nothing = Nothing
    binaryLowerMsgF args (Just ret) =
      Just $ boxToString $ ["consistentTLowerB "]
               %+ (intersperseBoxes [" "] $ map prettyLines $ args ++ [ret])


-- | Type unification.
unifyM :: K3 QType -> K3 QType -> (String -> String) -> TInfM ()
unifyM t1 t2 errf = void $ logAction msgF $ reasonM errf $ unifyDrv preChase postId t1 t2
  where
    preChase qt = getTVE >>= \tve -> return ((), tvchase tve qt)
    postId _ qt = return qt
    msgF Nothing  = Just $ prettyTaggedPair "unifyM call" t1 t2
    msgF (Just r) = Just $ prettyTaggedTriple "unifyM result" t1 t2 r

-- | Type unification with variable overrides to the unification result.
unifyWithOverrideM :: K3 QType -> K3 QType -> (String -> String) -> TInfM (K3 QType)
unifyWithOverrideM qt1 qt2 errf =
  logAction msgF $ reasonM errf $ unifyDrv preChase (\x y -> postUnifyCased x y >>= logPostUnify) qt1 qt2
  where
    preChase qt = getTVE >>= \tve -> return $ tvchasev tve Nothing qt

    logPostUnify r = logVoid (boxToString $ ["postUnify "] %+ prettyLines r) >> return r

    postUnifyCased (vo1, vo2) qt = case (vo1, vo2) of
        (Just v1, Just v2) -> unifyTwoChain v1 v2 qt
        (Just v, _) -> unifyOne v qt
        (_, Just v) -> unifyOne v qt
        _ -> return qt

    unifyOne v qt = do
      checkedUnify (snd v) qt
      return $ foldl (@+) (tvar $ fst v) $ annotations qt

    unifyTwoChain v1 v2 qt = do
      tve <- getTVE
      let vtCtor v = foldl (@+) (tvar v) $ annotations qt
      let (c1, c2) = (fst v1 == snd v1, fst v2 == snd v2)
      (src, trg, chain) <-
        case (v1 == v2, snd v1 == snd v2, c1 || c2, fst v1 == fst v2) of
          (True, _, _, _)        -> return (fst v1, snd v2, Nothing)
          (False, True, True, _) -> return (fst (if c1 then v2 else v1), snd v1, Nothing)
          (False, True, _, _)    -> let (_, p1, p2) = commonSuffix (tvpath tve $ fst v1) (tvpath tve $ fst v2) in
                                    if null p1 then return (head p2, snd v1, Nothing)
                                    else if null p2 then return (head p1, snd v2, Nothing)
                                    else return (fst v1, snd v2, Just (last p1, head p2))
          (_, _, _, True) -> left "Unhandled case in unifyTwoChain"
          (_, _, _, _) ->
            if snd v2 == fst v1 then return (fst v2, snd v1, Nothing)
            else return (fst v1, snd v2, if snd v1 /= fst v2 then Just (snd v1, fst v2) else Nothing)

      checkedUnify trg qt
      void $ maybe (logVoid "unifyTwoChain no chain") (\(s,t) -> checkedUnify s $ vtCtor t) chain
      return $ vtCtor $ src

    commonSuffix l1 l2 =
      let (rl1, rl2) = (reverse l1, reverse l2)
          suffix = map fst $ takeWhile (uncurry (==)) $ zip rl1 rl2
          f l = reverse $ drop (length suffix) l
      in (reverse suffix, f rl1, f rl2)

    checkedUnify v qt = do
      tve <- getTVE
      if not (occurs v qt tve) then unifyv v qt
      else do
        let uqt = case tag qt of
                    QTVar _ -> tvchase tve qt
                    _ -> qt
        logVoid $ prettyTaggedSPair "checkedUnify adding cycle" v uqt
        unifyv v uqt

    msgF Nothing  = Just $ prettyTaggedPair "unifyOvM call" qt1 qt2
    msgF (Just r) = Just $ prettyTaggedTriple "unifyOvM result" qt1 qt2 r

-- | Given a polytype, for every polymorphic type var, replace all of
--   its occurrences in t with a fresh type variable. We do this by
--   creating a substitution tve and applying it to t.
--   We also strip any mutability qualifiers here since we only instantiate
--   on variable access.
instantiate :: QPType -> TInfM (K3 QType)
instantiate (QPType tvs t) = withFreshTVE $ do
  (Node (tg :@: anns) ch) <- tvsub t
  return $ Node (tg :@: filter (not . isQTQualified) anns) ch
 where
   wrapWithTVE tve_before tve_after m =
    modify (mtive $ const tve_before) >> m >>= \r -> modify (mtive $ const tve_after) >> return r

   withFreshTVE m = do
    ntve <- associate_with_freshvars tvs
    otve <- getTVE
    wrapWithTVE ntve otve m

   associate_with_freshvars [] = return tvenv0
   associate_with_freshvars (tv:rtvs) = do
     tve     <- associate_with_freshvars rtvs
     tvfresh <- newtv
     return $ tvext tve tv tvfresh

-- | Return a monomorphic polytype.
monomorphize :: (Monad m) => K3 QType -> m QPType
monomorphize t = return $ QPType [] t

-- | Generalization for let-polymorphism.
generalize :: TInfM (K3 QType) -> TInfM QPType
generalize ta = do
 tve_before <- getTVE
 t          <- ta
 tve_after  <- getTVE
 t'         <- tvsub t
 let tvdep = tvdependentset tve_before tve_after
 let fv    = filter (not . tvdep) $ nub $ freevars t'
 return $ QPType fv t
 -- ^ We return an unsubstituted type to preserve type variables
 --   for late binding based on overriding unification performed
 --   in function application.
 --   Old implementation: return $ QPType fv t'

-- | QType substitution helpers

-- | Replaces any type variables in a QType annotation on any subexpression.
substituteDeepQt :: K3 Expression -> TInfM (K3 Expression)
substituteDeepQt e = mapTree subNode e
  where subNode ch (Node (tg :@: anns) _) = do
          nanns <- mapM subAnns anns
          return (Node (tg :@: nanns) ch)

        subAnns (EQType qt) = tvsub qt >>= return . EQType
        subAnns x = return x

-- | Top-level type inference methods
inferProgramTypes :: K3 Declaration -> Either String (K3 Declaration)
inferProgramTypes prog = do
    (_, initEnv) <- let (a,b) = runTInfM tienv0 $ initializeTypeEnv
                    in a >>= return . (, b)
    (nProg, finalEnv) <- let (a,b) = runTInfM initEnv $ mapProgram declF annMemF exprF prog
                         in a >>= return . (, b)
    logVoid $ "Final type environment"
    logVoid $ pretty finalEnv
    return nProg
  where
    initializeTypeEnv :: TInfM (K3 Declaration)
    initializeTypeEnv = mapProgram initDeclF initAMemF initExprF prog

    withUnique :: Identifier -> TInfM (K3 Declaration) -> TInfM (K3 Declaration)
    withUnique n m = failOnValid (return ()) (uniqueErr "declaration" n) (flip tilkupe n) >>= const m

    withUniqueA :: Identifier -> TInfM (K3 Declaration) -> TInfM (K3 Declaration)
    withUniqueA n m = failOnValid (return ()) (uniqueErr "annotation" n) (flip tilkupa n) >>= const m

    failOnValid :: TInfM () -> TInfM () -> (TIEnv -> Either a b) -> TInfM ()
    failOnValid success failure f = get >>= \env -> either (const $ success) (const $ failure) $ f env

    uniqueErr :: String -> Identifier -> TInfM a
    uniqueErr s n = left $ unwords ["Invalid unique", s, "identifier:", n]

    initDeclF :: K3 Declaration -> TInfM (K3 Declaration)
    initDeclF d@(tag -> DGlobal n t _)
      | isTFunction t = withUnique n $ qpType t >>= \qpt -> modify (\env -> tiexte env n qpt) >> return d
      | otherwise     = return d

    initDeclF d@(tag -> DTrigger n t _) =
      withUnique n $ trigType t >>= \qpt -> modify (\env -> tiexte env n qpt) >> return d
      where trigType x = qType x >>= \qt -> return (ttrg qt) >>= monomorphize

    initDeclF d@(tag -> DDataAnnotation n [] tdeclvars mems) = withUniqueA n $ mkAnnMemEnv >> return d
      where mkAnnMemEnv = mapM memType mems >>= \l -> modify (\env -> tiexta env n $ catMaybes l)
            memType (Lifted      _ mn mt _ _) = unifyMemInit True  mn mt
            memType (Attribute   _ mn mt _ _) = unifyMemInit False mn mt
            memType (MAnnotation _ _ _) = return Nothing
            unifyMemInit lifted mn mt = do
              qpt <- qpType (TC.forAll tdeclvars mt)
              return (Just (mn, (qpt, lifted)))

    initDeclF (tag -> DDataAnnotation n _ _ _) = left $ unwords
      ["Invalid annotation", n, "with splice parameters in inferProgramTypes.initDeclF"]

    initDeclF d = return d

    initAMemF :: AnnMemDecl -> TInfM AnnMemDecl
    initAMemF mem  = return mem

    initExprF :: K3 Expression -> TInfM (K3 Expression)
    initExprF expr = return expr

    unifyInitializer :: Identifier -> Either (Maybe QPType) QPType -> Maybe (K3 Expression)
                     -> TInfM (Maybe (K3 Expression))
    unifyInitializer n qptE eOpt = do
      qpt <- case qptE of
              Left (Nothing)   -> get >>= \env -> liftEitherM (tilkupe env n)
              Left (Just qpt') -> modify (\env -> tiexte env n qpt') >> return qpt'
              Right qpt'       -> return qpt'

      case eOpt of
        Just e -> do
          qt1 <- instantiate qpt
          qt2 <- qTypeOfM e
          logVoid $ prettyTaggedPair ("unify init ") qt1 qt2
          void $ unifyWithOverrideM qt1 qt2 $ mkErrorF e unifyInitErrF
          --return $ Just e
          substituteDeepQt e >>= return . Just

        Nothing -> return Nothing

    declF :: K3 Declaration -> TInfM (K3 Declaration)
    declF d@(tag -> DGlobal n t eOpt) = do
      qptE <- if isTFunction t then return (Left Nothing)
                               else (qpType t >>= return . Left . Just)
      if isTEndpoint t
        then return d
        else unifyInitializer n qptE eOpt >>= \neOpt ->
               return $ (Node (DGlobal n t neOpt :@: annotations d) $ children d)

    declF d@(tag -> DTrigger n t e) =
      get >>= \env -> liftEitherM (tilkupe env n) >>= \(QPType qtvars qt) ->
        case tag qt of
          QTCon QTTrigger ->
            let nqptE = Right $ QPType qtvars $ tfun (head $ children qt) tunit
            in unifyInitializer n nqptE (Just e) >>= \neOpt ->
                 return $ maybe d (\ne -> Node (DTrigger n t ne :@: annotations d) $ children d) neOpt
          _ -> trigTypeErr n

    declF d@(tag -> DDataAnnotation n svars tvars mems) =
        get >>= \env -> liftEitherM (tilkupa env n) >>= chkAnnMemEnv >>= \nmems ->
          return (Node (DDataAnnotation n svars tvars nmems :@: annotations d) $ children d)

      where chkAnnMemEnv amEnv = mapM (memType amEnv) mems

            memType amEnv (Lifted mp mn mt meOpt mAnns) =
              unifyMemInit amEnv mn meOpt >>= \nmeOpt -> return (Lifted mp mn mt nmeOpt mAnns)

            memType amEnv (Attribute   mp mn mt meOpt mAnns) =
              unifyMemInit amEnv mn meOpt >>= \nmeOpt -> return (Attribute mp mn mt nmeOpt mAnns)

            memType _ mem@(MAnnotation _ _ _) = return mem

            unifyMemInit amEnv mn meOpt = do
              qpt <- maybe (memLookupErr mn) (return . fst) (lookup mn amEnv)
              unifyInitializer mn (Right qpt) meOpt

    declF d = return d

    annMemF :: AnnMemDecl -> TInfM AnnMemDecl
    annMemF mem = return mem

    exprF :: K3 Expression -> TInfM (K3 Expression)
    exprF e = inferExprTypes e

    mkErrorF :: K3 Expression -> (String -> String) -> (String -> String)
    mkErrorF e f s = spanAsString ++ f s
      where spanAsString = let spans = mapMaybe getSpan $ annotations e
                           in if null spans
                                then (boxToString $ ["["] %+ prettyLines e %+ ["]"])
                                else unwords ["[", show $ head spans, "] "]

    memLookupErr  n = left $ "No annotation member in initial environment: " ++ n
    trigTypeErr   n = left $ "Invlaid trigger declaration type for: " ++ n
    unifyInitErrF s = "Failed to unify initializer: " ++ s

-- | Expression type inference. Note this not perform a final type substitution, leaving
--   it to the caller as desired. This may leave type variables present in the
--   quicktype annotations attached to expressions.
inferExprTypes :: K3 Expression -> TInfM (K3 Expression)
inferExprTypes expr = mapIn1RebuildTree lambdaBinding sidewaysBinding inferQType expr

  where
    iu :: TInfM ()
    iu = return ()

    monoBinding :: Identifier -> K3 QType -> TInfM ()
    monoBinding i t = monomorphize t >>= \mt -> modify (\env -> tiexte env i mt)

    lambdaBinding :: K3 Expression -> K3 Expression -> TInfM ()
    lambdaBinding _ (tag -> ELambda i) = newtv >>= monoBinding i
    lambdaBinding _ _ = return ()

    sidewaysBinding :: K3 Expression -> K3 Expression -> TInfM [TInfM ()]
    sidewaysBinding ch1 (tag -> ELetIn i) = do
      ipt <- generalize $ qTypeOfM ch1
      modify $ \env -> tiexte env i ipt
      return [iu]

    sidewaysBinding ch1 (tag -> EBindAs b) = do
        ch1T <- qTypeOfM ch1
        case b of
          BIndirection i -> do
            itv <- newtv
            void $ unifyWithOverrideM ch1T (tind itv) $ bindErr "indirection"
            monoBinding i itv

          BTuple ids -> do
            idtvs <- mapM (const newtv) ids
            void   $ unifyWithOverrideM ch1T (ttup idtvs) $ bindErr "tuple"
            mapM_ (uncurry monoBinding) $ zip ids idtvs

          -- TODO: partial bindings?
          BRecord ijs -> do
            jtvs <- mapM (const newtv) ijs
            void $  unifyWithOverrideM ch1T (trec $ flip zip jtvs $ map fst ijs) $ bindErr "record"
            mapM_ (uncurry monoBinding) $ flip zip jtvs $ map snd ijs

        return [iu]

      where
        bindErr kind reason = unwords ["Invalid", kind, "bind-as:", reason]

    sidewaysBinding ch1 (tag -> ECaseOf i) = do
      ch1T <- qTypeOfM ch1
      itv  <- newtv
      void $  unifyWithOverrideM ch1T (topt itv) $ (("Invalid case-of source expression: ")++)
      return [monoBinding i itv, modify $ \env -> tidele env i]

    sidewaysBinding _ (children -> ch) = return (replicate (length ch - 1) iu)

    inferQType :: [K3 Expression] -> K3 Expression -> TInfM (K3 Expression)
    inferQType ch n = do
      (ruleTag, r) <- inferTagQType ch n
      logVoid $ showTInfRule ruleTag ch r
      return r

    inferTagQType :: [K3 Expression] -> K3 Expression -> TInfM (String, K3 Expression)
    inferTagQType _ n@(tag -> EConstant (CBool   _)) = return $ ("const",) $ n .+ tbool
    inferTagQType _ n@(tag -> EConstant (CByte   _)) = return $ ("const",) $ n .+ tbyte
    inferTagQType _ n@(tag -> EConstant (CInt    _)) = return $ ("const",) $ n .+ tint
    inferTagQType _ n@(tag -> EConstant (CReal   _)) = return $ ("const",) $ n .+ treal
    inferTagQType _ n@(tag -> EConstant (CString _)) = return $ ("const",) $ n .+ tstr

    inferTagQType _ n@(tag -> EConstant (CNone nm)) = do
      tv <- newtv
      let ntv = case nm of { NoneMut -> mutQT tv; NoneImmut -> immutQT tv }
      return $ ("const",) $ n .+ (topt ntv)

    inferTagQType _ n@(tag -> EConstant (CEmpty t)) = do
        cqt <- qType t
        let annIds =  namedEAnnotations $ annotations n
        colqt <- mkCollectionQType annIds cqt
        return $ ("const",) $ n .+ colqt

    -- | Variable specialization. Note that instantiate strips qualifiers.
    inferTagQType _ n@(tag -> EVariable i) = do
        env <- get
        qt  <- either (lookupError i) instantiate (tilkupe env i)
        return $ ("var",) $ n .+ qt

    -- | Data structures. Qualifiers are taken from child expressions by rebuildE.
    inferTagQType ch n@(tag -> ESome)       = qTypeOfM (head ch) >>= return . ("opt",) . ((rebuildE n ch) .+) . topt
    inferTagQType ch n@(tag -> EIndirect)   = qTypeOfM (head ch) >>= return . ("ind",) . ((rebuildE n ch) .+) . tind
    inferTagQType ch n@(tag -> ETuple)      = mapM qTypeOfM ch   >>= return . ("tup",) . ((rebuildE n ch) .+) . ttup
    inferTagQType ch n@(tag -> ERecord ids) = mapM qTypeOfM ch   >>= return . ("rec",) . ((rebuildE n ch) .+) . trec . zip ids

    -- | Lambda expressions are passed the post-processed environment,
    --   so the type variable for the identifier is bound in the type environment.
    inferTagQType ch n@(tag -> ELambda i) = do
        env  <- get
        ipt  <- either (lambdaBindingErr i) return $ tilkupe env i
        chqt <- qTypeOfM $ head ch
        void $ modify $ \env' -> tidele env' i
        case ipt of
          QPType [] iqt -> return $ ("lambda",) $ rebuildE n ch .+ tfun iqt chqt
          _             -> polyLambdaBindingErr

    -- | Assignment expressions unify their source and target types, as well as
    --   ensuring that the source is mutable.
    inferTagQType ch n@(tag -> EAssign i) = do
      env <- get
      ipt <- either (assignBindingErr i) return $ tilkupe env i
      eqt <- qTypeOfM $ head ch
      case ipt of
        QPType [] iqt@(tag -> QTVar _) ->
          let nqt = tvchase (tive env) iqt in
          if nqt == iqt
            then do { void $ unifyWithOverrideM iqt (eqt @+ QTMutable) $ mkErrorF n $ assignErrF i;
                      return $ ("assign",) $ rebuildE n ch .+ tunit }
            else unifyWithMutableQt eqt nqt

        QPType [] iqt -> unifyWithMutableQt eqt iqt
        _ -> polyAssignBindingErr

      where unifyWithMutableQt eqt iqt
              | (iqt @~ isQTQualified) == Just QTMutable =
                  do { void $ unifyM (iqt @- QTMutable) eqt $ mkErrorF n $ assignErrF i;
                       return $ ("assign",) $ rebuildE n ch .+ tunit }
              | otherwise = mutabilityErr i

    inferTagQType ch n@(tag -> EProject i) = do
      srcqt   <- qTypeOfM $ head ch
      fieldqt <- newtv
      let prjqt = tlower $ [trec [(i, fieldqt)]]
      void   $ unifyWithOverrideM srcqt prjqt $ mkErrorF n $ projectErrF srcqt prjqt
      return $ ("project",) $ rebuildE n ch .+ fieldqt

    -- TODO: reorder inferred record fields based on argument at application.
    inferTagQType ch n@(tag -> EOperate OApp) = do
      fnqt   <- qTypeOfM $ head ch
      argqt  <- qTypeOfM $ last ch
      retqt  <- newtv
      void   $ unifyWithOverrideM fnqt (tfun argqt retqt) $ mkErrorF n $ applyErrF fnqt argqt retqt
      return $ ("apply",) $ rebuildE n ch .+ retqt

    inferTagQType ch n@(tag -> EOperate OSeq) = do
        lqt <- qTypeOfM $ head ch
        rqt <- qTypeOfM $ last ch
        void $ unifyM tunit lqt $ mkErrorF n seqErrF
        return $ ("seq",) $ rebuildE n ch .+ rqt

    -- | Check trigger-address pair and unify trigger type and message argument.
    inferTagQType ch n@(tag -> EOperate OSnd) = do
        trgtv <- newtv
        void $ unifyBinaryM (ttup [ttrg trgtv, taddr]) trgtv ch n sndError
        return $ ("send",) $ rebuildE n ch .+ tunit

    -- | Unify operand types based on the kind of operator.
    inferTagQType ch n@(tag -> EOperate op)
      | numeric op = do
            (lqt, rqt) <- unifyBinaryM tnum tnum ch n numericError
            resultqt   <- delayNumericQt lqt rqt
            return $ ("numeric",) $ rebuildE n ch .+ resultqt

      | comparison op = do
          lqt <- qTypeOfM $ head ch
          rqt <- qTypeOfM $ last ch
          void $ unifyM lqt rqt $ mkErrorF n comparisonError
          return $ ("comp",) $ rebuildE n ch .+ tbool

      | logic op = do
            void $ unifyBinaryM tbool tbool ch n logicError
            return $ ("logic",) $ rebuildE n ch .+ tbool

      | textual op = do
            void $ unifyBinaryM tstr tstr ch n stringError
            return $ ("text",) $ rebuildE n ch .+ tstr

      | op == ONeg = do
            chqt <- unifyUnaryM tnum ch $ mkErrorF n uminusError
            let resultqt = case tag chqt of
                             QTPrimitive _  -> chqt
                             QTVar _ -> chqt
                             _ -> tnum
            return $ ("neg",) $ rebuildE n ch .+ resultqt

      | op == ONot = do
            void $ unifyUnaryM tbool ch $ mkErrorF n negateError
            return $ ("not",) $ rebuildE n ch .+ tbool

      | otherwise = left $ "Invalid operation: " ++ show op

      where
        delayNumericQt l r
          | or (map isQTVar   [l, r]) = return $ tlower [l,r]
          | or (map isQTLower [l, r]) = return $ tlower $ concatMap childrenOrSelf [l,r]
          | otherwise = tvlower l r

        childrenOrSelf t@(tag -> QTOperator QTLower) = children t
        childrenOrSelf t = [t]

    -- First child generation has already been performed in sidewaysBinding
    inferTagQType ch n@(tag -> ELetIn j) = do
      bqt <- qTypeOfM $ last ch
      void $ modify $ \env -> tidele env j
      return $ ("let-in",) $ rebuildE n ch .+ bqt

    -- First child unification has already been performed in sidewaysBinding
    inferTagQType ch n@(tag -> EBindAs b) = do
      bqt <- qTypeOfM $ last ch
      case b of
        BIndirection i -> modify $ \env -> tidele env i
        BTuple ids     -> modify $ \env -> foldl tidele env ids
        BRecord ijs    -> modify $ \env -> foldl tidele env $ map snd ijs
      return $ ("bind-as",) $ rebuildE n ch .+ bqt

    -- First child unification has already been performed in sidewaysBinding
    inferTagQType ch n@(tag -> ECaseOf _) = do
      sqt   <- qTypeOfM $ ch !! 1
      nqt   <- qTypeOfM $ last ch
      retqt <- unifyWithOverrideM sqt nqt $ mkErrorF n caseErrF
      return $ ("case-of",) $ rebuildE n ch .+ retqt

    inferTagQType ch n@(tag -> EIfThenElse) = do
      pqt   <- qTypeOfM $ head ch
      tqt   <- qTypeOfM $ ch !! 1
      eqt   <- qTypeOfM $ last ch
      void  $  unifyM pqt tbool $ mkErrorF n $ (("Invalid if-then-else predicate: ") ++)
      retqt <- unifyWithOverrideM tqt eqt $ mkErrorF n $ (("Mismatched condition branches: ") ++)
      return $ ("if-then",) $ rebuildE n ch .+ retqt

    inferTagQType ch n@(tag -> EAddress) = do
      hostqt <- qTypeOfM $ head ch
      portqt <- qTypeOfM $ last ch
      void $ unifyM hostqt tstr $ mkErrorF n $ (("Invalid address host string: ") ++)
      void $ unifyM portqt tint $ mkErrorF n $ (("Invalid address port int: ") ++)
      return $ ("address",) $ rebuildE n ch .+ taddr

    inferTagQType ch n  = return $ ("<unhandled>",) $ rebuildE n ch
      -- ^ TODO unhandled: ESelf, EImperative

    rebuildE (Node t _) ch = Node t ch

    unifyBinaryM lexpected rexpected ch n errf = do
      lqt <- qTypeOfM $ head ch
      rqt <- qTypeOfM $ last ch
      void $ unifyM lexpected lqt (mkErrorF n $ errf "left")
      void $ unifyM rexpected rqt (mkErrorF n $ errf "right")
      return (lqt, rqt)

    unifyUnaryM expected ch errf = do
        chqt <- qTypeOfM $ head ch
        void $ unifyM chqt expected errf
        return chqt

    numeric    op = op `elem` [OAdd, OSub, OMul, ODiv, OMod]
    comparison op = op `elem` [OEqu, ONeq, OLth, OLeq, OGth, OGeq]
    logic      op = op `elem` [OAnd, OOr]
    textual    op = op `elem` [OConcat]

    mkErrorF :: K3 Expression -> (String -> String) -> (String -> String)
    mkErrorF e f s = spanAsString ++ f s
      where spanAsString = let spans = mapMaybe getSpan $ annotations e
                           in if null spans then (boxToString $ ["["] %+ prettyLines e %+ ["]"])
                                            else unwords ["[", show $ head spans, "] "]

    -- | Error printing functions for unification cases
    assignErrF i = (("Invalid assignment to " ++ i ++ ": ") ++)
    seqErrF      = (("Invalid left sequence operand: ") ++)
    caseErrF     = (("Mismatched case-of branch types: ") ++)

    projectErrF srcqt prjqt =
      (unlines ["Invalid record projection:", pretty srcqt, "and", pretty prjqt] ++)

    applyErrF fnqt argqt retqt =
      (unlines ["Invalid function application:", pretty fnqt, "and", pretty (tfun argqt retqt), ":"] ++)

    msgWithTypeEnv msg        = get >>= \env -> left $ msg ++ "\nType envrionment:\n" ++ pretty env
    lookupError j reason      = msgWithTypeEnv $ unwords ["No type environment binding for ", j, ":", reason]
    lambdaBindingErr i reason = msgWithTypeEnv $ unwords ["Could not find typevar for lambda binding: ", i, reason]
    polyLambdaBindingErr      = msgWithTypeEnv "Invalid forall type in lambda binding"

    assignBindingErr i reason = msgWithTypeEnv $ unwords ["Could not find binding type for assignment: ", i, reason]
    polyAssignBindingErr      = msgWithTypeEnv "Invalid forall type in assignment"
    mutabilityErr j           = msgWithTypeEnv $ "Invalid assignment to non-mutable binding: " ++ j

    sndError     side reason = "Invalid " ++ side ++ " send operand: " ++ reason
    numericError side reason = "Invalid " ++ side ++ " numeric operand: " ++ reason
    stringError  side reason = "Invalid " ++ side ++ " string operand: " ++ reason
    logicError   side reason = unwords ["Invalid", side, "logic", "operand:", reason]
    comparisonError   reason = "Invalid comparison operands:" ++ reason

    uminusError reason = "Invalid unary minus operand: " ++ reason
    negateError reason = "Invalid negation operand: " ++ reason

    -- | Type judgement stringification, given a tag, premise expressions and a conclusion expression.
    showTInfRule :: String -> [K3 Expression] -> K3 Expression -> String
    showTInfRule rtag ch n =
      boxToString $ (rpsep %+ premise) %$ separator %$ (rpsep %+ conclusion)
      where nuid           = uidOf $ n @~ isEUID
            rprefix        = unwords [rtag, nuid]
            (rplen, rpsep) = (length rprefix, [replicate rplen ' '])
            premise        = if null ch then ["<empty>"]
                                        else (intersperseBoxes [" , "] $ map prettyLines $ mapMaybe qTypeOf ch)
            premLens       = map length premise
            headWidth      = if null premLens then 4 else maximum premLens
            separator      = [rprefix ++ replicate headWidth '-']
            conclusion     = maybe ["<invalid>"] prettyLines $ qTypeOf n
            uidOf (Just (EUID u)) = show u
            uidOf _               = "<No UID>"


{- Collection type construction -}

mkCollectionQType :: [Identifier] -> K3 QType -> TInfM (K3 QType)
mkCollectionQType annIds contentQt@(tag -> QTCon (QTRecord _)) = return $ tcol contentQt annIds
mkCollectionQType _ qt = left $ "Invalid content record type: " ++ show qt

mkCollectionFSQType :: [Identifier] -> [TMEnv] -> K3 QType -> TInfM (K3 QType, K3 QType)
mkCollectionFSQType annIds memEnvs contentQt = do
    flatEnvs <- assertNoDuplicateIds
    let (lifted, regular) = partition (snd . snd) flatEnvs
    finalQt <- mkFinalQType contentQt regular
    selfQt  <- membersAsRecordFields lifted >>= subCTVars contentQt finalQt . trec
    return (finalQt, selfQt)
  where
    mkFinalQType cqt regular =
      case tag cqt of
        QTCon (QTRecord ids) ->
           membersAsRecordFields regular >>= return . trec . ((zip ids $ children cqt) ++)
        _ -> nonRecordContentErr cqt

    subCTVars ct ft st = mapTree (subCF ct ft) st
    subCF ct _ _ (tag -> QTContent) = return ct
    subCF _ ft _ (tag -> QTFinal)   = return ft
    subCF _ _ ch (Node t _) = return $ Node t ch

    assertNoDuplicateIds =
      let flatEnvs = concat memEnvs
          ids      = map fst flatEnvs
      in if nub ids /= ids then nameConflictErr else return flatEnvs

    membersAsRecordFields attrs = mapM (\(j,(qpt,_)) -> instantiate qpt >>= return . (j,)) attrs

    nameConflictErr        = left $ "Conflicting annotation member names: " ++ show annIds
    nonRecordContentErr qt = left $ "Invalid content record type: " ++ show qt


{- Type conversion -}

qpType :: K3 Type -> TInfM QPType

-- | At top level foralls, we extend the declared var env in the type inference
--   environment with fresh qtype variables. This leads to substitutions for any
--   corresponding declared variables in the type tree.
qpType t@(tag -> TForall tvars) = do
  tvmap_ <- mapM (\(TypeVarDecl i _ _) -> newtv >>= varId >>= return . (i,)) tvars
  void $ modify $ extend tvmap_
  chQt <- qType (head $ children t)
  void $ modify $ prune tvmap_
  return $ QPType (map snd tvmap_) chQt

  where
    extend tvmap_ env = foldl (\a (b,c) -> tiextdv a b c) env tvmap_
    prune  tvmap_ env = foldl (\a (b,_) -> tideldv a b) env tvmap_
    varId (tag -> QTVar i) = return i
    varId _ = left $ "Invalid type variable for type var bindings"

qpType t = generalize (qType t)
  -- Old code: qType t >>= monomorphize

-- | We currently do not support forall type quantifiers present at an
--   arbitrary location in the K3 Type tree since forall types are not
--   part of the QType datatype and grammar.
--   The above qpType method constructs polymorphic QTypes, which handles
--   top-level polymorphic types, creating mappings for declared variables
--   in a K3 Type to QType typevars.
--
qType :: K3 Type -> TInfM (K3 QType)
qType t = foldMapTree mkQType (ttup []) t >>= return . mutabilityT t
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
        mkCollectionQType annIds cqt

    mkQType ch (tag -> TFunction) = return $ tfun (head ch) $ last ch
    mkQType ch (tag -> TTrigger)  = return $ ttrg $ head ch
    mkQType ch (tag -> TSource)   = return $ tsrc $ head ch
    mkQType ch (tag -> TSink)     = return $ tsnk $ head ch

    mkQType _ (tag -> TBuiltIn TContent)   = return tcontent
    mkQType _ (tag -> TBuiltIn TStructure) = return tfinal
    mkQType _ (tag -> TBuiltIn TSelf)      = return tself

    mkQType _ (tag -> TDeclaredVar x) = get >>= \tienv -> liftEitherM (tilkupdv tienv x)

    mkQType _ (tag -> TForall _) = left $ "Invalid forall type for QType"
      -- ^ TODO: we can only handle top-level foralls, and not arbitrary
      --   foralls nested in type trees.

    mkQType _ t_ = left $ "No QType construction for " ++ show t_

    mutability0 nch n = mutabilityT (head $ children n) $ head nch
    mutabilityN nch n = map (uncurry mutabilityT) $ zip (children n) nch


-- | Converts all QType annotations on program expressions to K3 types.
translateProgramTypes :: K3 Declaration -> Either String (K3 Declaration)
translateProgramTypes prog = mapProgram declF annMemF exprF prog
  where declF   d = return d
        annMemF m = return m
        exprF   e = translateExprTypes e

translateExprTypes :: K3 Expression -> Either String (K3 Expression)
translateExprTypes expr = mapTree translate expr >>= \e -> return $ flip addTQualifier e $ exprTQualifier expr
  where
    translate nch e@(Node (tg :@: anns) _) = do
      let nch' = case tg of
                   ELetIn _ -> [flip addTQualifier (head nch) $ letTQualifier e] ++ tail nch
                   _        -> nch
      nanns <- mapM (translateEQType $ e @~ isESpan) $ filter (not . isEType) anns
      return (Node (tg :@: nanns) nch')

    addTQualifier tqOpt e@(Node (tg :@: anns) ch) = maybe e (\tq -> Node (tg :@: map (inject tq) anns) ch) tqOpt
      where inject tq (EType t) = maybe (EType $ t @+ tq) (const $ EType t) $ find isTQualified $ annotations t
            inject _ a = a

    letTQualifier  e = exprTQualifier $ head $ children e
    exprTQualifier e = maybe Nothing (Just . translateAnnotation) $ extractEQTypeQualifier e

    extractEQTypeQualifier e =
      case find isEQType $ annotations e of
        Just (EQType qt) -> find isQTQualified $ annotations qt
        _ -> Nothing

    translateEQType spanOpt (EQType qt) = translateQType spanOpt qt >>= return . EType
    translateEQType _ x = return x

    translateAnnotation a = case a of
      QTMutable   -> TMutable
      QTImmutable -> TImmutable
      QTWitness   -> TWitness


translateQType :: Maybe (Annotation Expression) -> K3 QType -> Either String (K3 Type)
translateQType spanOpt qt = mapTree translateWithMutability qt
  where translateWithMutability ch qt'@(tag -> QTCon tg)
          | tg `elem` [QTOption, QTIndirection, QTTuple] = translate (attachToChildren ch qt') qt'

        translateWithMutability ch qt'@(tag -> QTCon (QTRecord _)) = translate (attachToChildren ch qt') qt'

        translateWithMutability ch qt' = translate ch qt'

        attachToChildren ch qt' =
          map (uncurry $ foldl (@+)) $ zip ch $ map (map translateAnnotation . annotations) $ children qt'

        translateAnnotation a = case a of
          QTMutable   -> TMutable
          QTImmutable -> TImmutable
          QTWitness   -> TWitness

        translate _ qt'
          | QTBottom     <- tag qt' = return TC.bottom
          | QTTop        <- tag qt' = return TC.top
          | QTContent    <- tag qt' = return $ TC.builtIn TContent
          | QTFinal      <- tag qt' = return $ TC.builtIn TStructure
          | QTSelf       <- tag qt' = return $ TC.builtIn TSelf
          | QTVar v      <- tag qt' = return $ TC.declaredVar ("v" ++ show v)
          | QTOperator _ <- tag qt' =
              let msg = "Invalid qtype translation for qtype operator"
              in Left $ boxToString
                      $ [unwords $ [msg, "(", maybe "" show spanOpt, ")"]]
                      %$ prettyLines qt'

        translate _ (tag -> QTPrimitive p) = case p of
          QTBool     -> return TC.bool
          QTByte     -> return TC.byte
          QTReal     -> return TC.real
          QTInt      -> return TC.int
          QTString   -> return TC.string
          QTAddress  -> return TC.address
          QTNumber   -> return TC.number

        translate ch (tag -> QTCon d) = case d of
          QTFunction          -> return $ TC.function (head ch) $ last ch
          QTOption            -> return $ TC.option $ head ch
          QTIndirection       -> return $ TC.indirection $ head ch
          QTTuple             -> return $ TC.tuple ch
          QTRecord        ids -> return $ TC.record $ zip ids ch
          QTCollection annIds -> return $ foldl (@+) (TC.collection $ head ch) $ map TAnnotation annIds
          QTTrigger           -> return $ TC.trigger $ head ch
          QTSource            -> return $ TC.source $ head ch
          QTSink              -> return $ TC.sink $ head ch

        translate _ qt' = Left . unwords $ ["No translation for ", "(", maybe "" show spanOpt, ")", show qt']


{- Instances -}
instance Pretty TIEnv where
  prettyLines (te, ta, tdv, tve) =
    ["TEnv: "]   %$ (indent 2 $ prettyLines te)  ++
    ["TAEnv: "]  %$ (indent 2 $ prettyLines ta)  ++
    ["TDVEnv: "] %$ (indent 2 $ prettyLines tdv) ++
    ["TVEnv: "]  %$ (indent 2 $ prettyLines tve)

instance Pretty TEnv where
  prettyLines te = prettyPairList te

instance Pretty TAEnv where
  prettyLines ta = Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] ta

instance Pretty TMEnv where
  prettyLines tme = prettyPairList tme

instance Pretty TDVEnv where
  prettyLines tdve = prettyPairList tdve

instance Pretty TVEnv where
  prettyLines (TVEnv n m) = ["# vars: " ++ show n] ++
                            (Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] m)

instance Pretty (QPType, Bool) where
  prettyLines (a,b) = (if b then ["(Lifted) "] else ["(Attr) "]) %+ prettyLines a

prettyPairList :: (Show a, Pretty b) => [(a,b)] -> [String]
prettyPairList l = foldl (\acc kvPair -> acc ++ prettyPair kvPair) [] l

prettyPair :: (Show a, Pretty b) => (a,b) -> [String]
prettyPair (a,b) = [show a ++ " => "] %+ prettyLines b

prettyTaggedSPair :: (Show a, Pretty b) => String -> a -> b -> String
prettyTaggedSPair s a b = boxToString $ [s ++ " " ++ show a] %+ [" and "] %+ prettyLines b

prettyTaggedPair :: (Pretty a, Pretty b) => String -> a -> b -> String
prettyTaggedPair s a b = boxToString $ [s ++ " "] %+ prettyLines a %+ [" and "] %+ prettyLines b

prettyTaggedTriple :: (Pretty a, Pretty b, Pretty c) => String -> a -> b -> c -> String
prettyTaggedTriple s a b c =
  boxToString $ [s ++ " "] %+ (intersperseBoxes [" , "] [prettyLines a, prettyLines b, prettyLines c])

prettyTaggedSTriple :: (Show a, Pretty b, Pretty c) => String -> a -> b -> c -> String
prettyTaggedSTriple s a b c =
  boxToString $ [s ++ " "] %+ (intersperseBoxes [" , "] [[show a], prettyLines b, prettyLines c])
