{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeFamilies #-}

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

import Control.Arrow ( (&&&) )
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State
import Control.Monad.Trans.Except

import Data.Binary ( Binary )
import Data.Serialize ( Serialize )

import Data.List
import Data.Maybe
import Data.Tree
import Debug.Trace

import Data.IntMap ( IntMap )
import qualified Data.IntMap as IntMap

import GHC.Generics ( Generic )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils
import Language.K3.Core.Constructor.Type as TC

import Language.K3.Analysis.Data.BindingEnv ( BindingEnv, BindingStackEnv )
import qualified Language.K3.Analysis.Data.BindingEnv as BEnv

import Language.K3.Analysis.HMTypes.DataTypes

import Language.K3.Utils.Logger

import Data.Text ( Text )
import Language.K3.Utils.PrettyText ( Pretty, (%$), (%+) )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

$(loggingFunctions)

hmtcTraceLogging :: Bool
hmtcTraceLogging = False

localLog :: (Functor m, Monad m) => Text -> m ()
localLog t = logVoid hmtcTraceLogging $ T.unpack t

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe Text) -> m a -> m a
localLogAction f a = logAction hmtcTraceLogging (maybe Nothing (Just . T.unpack) . f) a


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
              _ -> serrorM $ "Untyped expression: " ++ show e

projectNamedPairs :: [Identifier] -> [(Identifier, a)] -> [a]
projectNamedPairs ids idv = [v | i <- ids, let (Just v) = lookup i idv]

rebuildNamedPairs :: [(Identifier, a)] -> [Identifier] -> [a] -> [a]
rebuildNamedPairs oldIdv newIds newVs = map (replaceNewPair $ zip newIds newVs) oldIdv
  where replaceNewPair pairs (k,v) = maybe v id $ lookup k pairs

-- | A type environment.
type TEnv = BindingStackEnv QPType

-- | Annotation type environment.
type TAEnv = BindingEnv TMEnv

-- | Annotation member environment.
--   The boolean indicates whether the member is a lifted attribute.
type TMEnv = BindingEnv (QPType, Bool)

-- | Declared type variable environment.
type TDVEnv = BindingStackEnv QTVarId

-- | A type variable environment.
data TVEnv = TVEnv QTVarId (IntMap (K3 QType))
           deriving (Eq, Read, Show, Generic)

-- | A cyclic variable environment (tracks whether an identifer uses cyclic scope).
type TCEnv = BindingEnv Bool

-- | A type inference environment.
data TIEnv = TIEnv {
               tenv    :: TEnv,
               taenv   :: TAEnv,
               tdvenv  :: TDVEnv,
               tvenv   :: TVEnv,
               tcyclic :: TEnv,
               tcenv   :: TCEnv,
               tprop   :: [(Identifier, QPType)]
            }
            deriving (Eq, Read, Show, Generic)

-- | The type inference monad
type TInfM = ExceptT Text (State TIEnv)

-- | Data type for initializer type handling.
data IDeclaredAction = IDAExtend     QPType
                     | IDAPassThru   QPType
                     | IDAFunction
                     | IDATrigger    QPType
                     deriving (Eq, Ord, Read, Show, Generic)


{- Type inference instances -}
instance Binary IDeclaredAction
instance Binary TVEnv
instance Binary TIEnv

instance Serialize IDeclaredAction
instance Serialize TVEnv
instance Serialize TIEnv

{- TEnv helpers -}
tenv0 :: TEnv
tenv0 = BEnv.empty

tlkup :: TEnv -> Identifier -> Except Text QPType
tlkup env x = BEnv.slookup env x

text :: TEnv -> Identifier -> QPType -> TEnv
text env x t = BEnv.push env x t

tdel :: TEnv -> Identifier -> TEnv
tdel env x = BEnv.pop env x

-- Return a pair of TEnvs, the first made up of env2 entries different than
-- the named entries in env1, and the second containing env2 entries not present in env1.
tdiff :: TEnv -> TEnv -> (TEnv, TEnv)
tdiff env1 env2 = BEnv.foldl partdiff (BEnv.empty, BEnv.empty) env2
  where partdiff acc _ [] = acc
        partdiff (diffs, news) n (last -> qt) =
          case BEnv.mlookup env1 n of
            Nothing -> (diffs, BEnv.push news n qt)
            Just l -> (if qt `notElem` l then BEnv.push diffs n qt else diffs, news)

tmap :: TEnv -> (QPType -> QPType) -> TEnv
tmap env f = BEnv.map (map f) env


{- TAEnv helpers -}
taenv0 :: TAEnv
taenv0 = BEnv.empty

talkup :: TAEnv -> Identifier -> Except Text TMEnv
talkup env x = BEnv.lookup env x

taext :: TAEnv -> Identifier -> TMEnv -> TAEnv
taext env x te = BEnv.set env x te


{- TDVEnv helpers -}
tdvenv0 :: TDVEnv
tdvenv0 = BEnv.empty

tdvlkup :: TDVEnv -> Identifier -> Except Text (K3 QType)
tdvlkup env x = BEnv.slookup env x >>= return . tvar

tdvext :: TDVEnv -> Identifier -> QTVarId -> TDVEnv
tdvext env x v = BEnv.push env x v

tdvdel :: TDVEnv -> Identifier -> TDVEnv
tdvdel env x = BEnv.pop env x

{- Cyclic metadata helpers -}
tcenv0 :: TCEnv
tcenv0 = BEnv.empty

tclkup :: TCEnv -> Identifier -> Except Text Bool
tclkup env x = BEnv.lookup env x

tcext :: TCEnv -> Identifier -> Bool -> TCEnv
tcext env x c = BEnv.set env x c

{- TIEnv helpers -}
tienv0 :: TIEnv
tienv0 = TIEnv {
           tenv    = tenv0,
           taenv   = taenv0,
           tdvenv  = tdvenv0,
           tvenv   = tvenv0,
           tcyclic = tenv0,
           tcenv   = tcenv0,
           tprop   = []
         }

-- | Modifiers.
mtiee :: (TEnv -> TEnv) -> TIEnv -> TIEnv
mtiee f env = env {tenv = f $ tenv env}

mtiae :: (TAEnv -> TAEnv) -> TIEnv -> TIEnv
mtiae f env = env {taenv = f $ taenv env}

mtidve :: (TDVEnv -> TDVEnv) -> TIEnv -> TIEnv
mtidve f env = env {tdvenv = f $ tdvenv env}

mtive :: (TVEnv -> TVEnv) -> TIEnv -> TIEnv
mtive f env = env {tvenv = f $ tvenv env}

mtice :: (TCEnv -> TCEnv) -> TIEnv -> TIEnv
mtice f env = env {tcenv = f $ tcenv env}

mticyce :: (TEnv -> TEnv) -> TIEnv -> TIEnv
mticyce f env = env {tcyclic = f $ tcyclic env}

tilkupe :: TIEnv -> Identifier -> Except Text QPType
tilkupe env x = tlkup (tenv env) x

tilkupa :: TIEnv -> Identifier -> Except Text TMEnv
tilkupa env x = talkup (taenv env) x

tilkupdv :: TIEnv -> Identifier -> Except Text (K3 QType)
tilkupdv env x = tdvlkup (tdvenv env) x

tilkupc :: TIEnv -> Identifier -> Except Text Bool
tilkupc env x = tclkup (tcenv env) x

tiexte :: TIEnv -> Identifier -> QPType -> TIEnv
tiexte env x t = env {tenv=text (tenv env) x t}

tiexta :: TIEnv -> Identifier -> TMEnv -> TIEnv
tiexta env x ate = env {taenv=taext (taenv env) x ate}

tiextdv :: TIEnv -> Identifier -> QTVarId -> TIEnv
tiextdv env x v = env {tdvenv=tdvext (tdvenv env) x v}

tiextc :: TIEnv -> Identifier -> Bool -> TIEnv
tiextc env x cyc = env {tcenv=tcext (tcenv env) x cyc}

tidele :: TIEnv -> Identifier -> TIEnv
tidele env i = env {tenv=tdel (tenv env) i}

tideldv :: TIEnv -> Identifier -> TIEnv
tideldv env i = env {tdvenv=tdvdel (tdvenv env) i}

tiincrv :: TIEnv -> (QTVarId, TIEnv)
tiincrv env = let TVEnv n s = tvenv env
  in (n, env {tvenv=TVEnv (succ n) s})


{- TVEnv helpers -}
tvenv0 :: TVEnv
tvenv0 = TVEnv 0 IntMap.empty

tvlkup :: TVEnv -> QTVarId -> Maybe (K3 QType)
tvlkup (TVEnv _ s) v = IntMap.lookup v s

tvext :: TVEnv -> QTVarId -> K3 QType -> TVEnv
tvext (TVEnv c s) v t = TVEnv c $ IntMap.insert v t s

tvmap :: TVEnv -> (K3 QType -> K3 QType) -> TVEnv
tvmap (TVEnv c s) f = TVEnv c $ IntMap.map f s

-- TVE domain predicate: check to see if a TVarName is in the domain of TVE
tvdomainp :: TVEnv -> QTVarId -> Bool
tvdomainp (TVEnv _ s) v = IntMap.member v s

-- Give the list of all type variables that are allocated in TVE but
-- not bound there
tvfree :: TVEnv -> [QTVarId]
tvfree (TVEnv c s) = filter (\v -> IntMap.notMember v s) [0..c-1]

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
typevars :: K3 QType -> [QTVarId]
typevars t = runIdentity $ foldMapTree extractVars [] t
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


{- Cyclic environment helpers -}
resetCyclicEnv :: TIEnv -> TIEnv
resetCyclicEnv env = env {tenv = tenv0, tcyclic = tenv env}

popCyclicEnv :: TIEnv -> (TEnv, TEnv, TIEnv)
popCyclicEnv env = (tenv env, tcyclic env, env {tenv = tcyclic env})

-- Given the original linear type and cyclic type envs, restore them
-- while pushing any changes in the new cyclic type env to the linear type env.
pushCyclicEnv :: TEnv -> TEnv -> TIEnv -> TIEnv
pushCyclicEnv te tc env =
  let (tcdiff, tcnew) = tdiff tc $ tenv env
      nte             = BEnv.unions [tcdiff, te, tcnew]
  in env {tenv = nte, tcyclic = tenv env}

withCyclicEnv :: Identifier -> TInfM a -> TInfM a
withCyclicEnv n m = do
  (te, tce, env) <- get >>= return . popCyclicEnv
  put env
  r <- m
  env' <- get
  nte  <- liftExceptM (tilkupe env' n >>= return . text te n)
  put $ pushCyclicEnv nte tce env'
  return r


{- TInfM helpers -}

runTInfM :: TIEnv -> TInfM a -> (Either Text a, TIEnv)
runTInfM env m = flip runState env $ runExceptT m

runTInfE :: TIEnv -> TInfM a -> Either Text (a, TIEnv)
runTInfE e m = let (a,b) = runTInfM e m in a >>= return . (, b)

runTInfES :: TIEnv -> TInfM a -> Either String (a, TIEnv)
runTInfES e m = either (Left . T.unpack) Right $ runTInfE e m

reasonM :: (Text -> Text) -> TInfM a -> TInfM a
reasonM errf = mapExceptT $ \m -> m >>= \case
  Left  err -> get >>= \env -> (return . Left $ errf $ T.unlines [err, T.pack "Type environment:", PT.pretty env])
  Right r   -> return $ Right r

errorM :: Text -> TInfM a
errorM msg = reasonM id $ throwE msg

serrorM :: String -> TInfM a
serrorM msg = reasonM id $ throwE $ T.pack msg

liftExceptM :: Except Text a -> TInfM a
liftExceptM = mapExceptT (return . runIdentity)

tryExceptM :: (Text -> TInfM b) -> (a -> TInfM b) -> Except Text a -> TInfM b
tryExceptM onFail onSuccess m = catchE (liftExceptM m >>= onSuccess) onFail

getTVE :: TInfM TVEnv
getTVE = get >>= return . tvenv


-- Allocate a fresh type variable
newtv :: TInfM (K3 QType)
newtv = do
  (nv, nenv) <- get >>= return . tiincrv
  put nenv
  return $ tvar nv


-- Deep substitute, throughout type structure
tvsub :: Bool -> K3 QType -> TInfM (K3 QType)
tvsub evalLower qt = acyclicSub [] qt
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
          if null ch
            then serrorM "Invalid qtype lower operator"
            else rebuildLowerCh t ch

        acyclicSub _ t = return t

        rebuildLowerCh t ch = do
          let tvars = map tvar $ concatMap typevars ch
          if null tvars || evalLower then tvopeval QTLower ch >>= flip extendAnns t
          else return $ foldl (@+) (tlower ch) $ annotations t

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
        | i1 `contains` i2 -> subRecord False i1 a i2 b
        | i2 `contains` i1 -> subRecord True  i2 b i1 a
        | otherwise -> coveringRecord i1 i2 a b
       where
        contains xs ys = xs `union` ys == xs

      (QTCon (QTCollection _), QTCon (QTRecord _)) -> coveringCollection a b
      (QTCon (QTRecord _), QTCon (QTCollection _)) -> coveringCollection b a

      (QTCon (QTCollection idsA), QTCon (QTCollection idsB))
        | idsA `intersect` idsB == idsA -> subCollection idsB a b
        | idsA `intersect` idsB == idsB -> subCollection idsA a b

      (QTVar _, QTVar _) -> return a
      (QTVar _, _) -> return a
      (_, QTVar _) -> return b

      -- | Function type lower bounds
      (QTCon QTFunction, QTCon QTFunction) -> do
        arga  <- tvsub False $ head $ children a
        argb  <- tvsub False $ head $ children b
        retlb <- rcr (last $ children a) $ last $ children b
        if arga /= argb
          then lowerError a b
          else annLower a b >>= return . foldl (@+) (tfun (head $ children a) retlb)

      -- | Self type lower bounds
      (QTSelf, QTSelf)                 -> return a

      (QTCon (QTCollection _), QTSelf) -> return a
      (QTSelf, QTCon (QTCollection _)) -> return b

      (QTCon (QTRecord _), QTSelf)     -> return b
      (QTSelf, QTCon (QTRecord _))     -> return a

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
    subRecord supAsLeft subid subqt supid supqt = do
      fieldQt <- rcrRecordCommon supAsLeft (zip subid $ children subqt) (zip supid $ children supqt)
      annLower subqt supqt >>= return . foldl (@+) (trec $ zip subid fieldQt)

    {-
    coveringRecord i1 i2 a' b' =
      let idChPairs = zip (i1 ++ i2) $ children a' ++ children b'
      in annLower a' b' >>= return . foldl (@+) (trec $ nub idChPairs)
    -}

    coveringRecord i1 i2 a' b' =
      let (idt1, idt2) = (zip i1 $ children a', zip i2 $ children b') in
      let groupF acc (i,qt) = maybe (addAssoc acc i [qt]) (replaceAssoc acc i . (++[qt])) $ lookup i acc in
      let qtsByName = foldl groupF [] $ idt1 ++ idt2 in
      let rcrUnlessSingleton (i, qts) = case qts of
                                          [x] -> return (i,x)
                                          [x,y] -> rcr x y >>= return . (i,)
                                          _ -> lowerError a' b'
      in do
        nidqt <- mapM rcrUnlessSingleton qtsByName
        lanns <- annLower a' b'
        return $ foldl (@+) (trec nidqt) lanns

    rcrRecordCommon supAsLeft sub sup =
      let lowerF = if supAsLeft then \subV supV -> rcr supV subV
                                else \subV supV -> rcr subV supV
      in mapM (\(k,v) -> maybe (return v) (lowerF v) $ lookup k sup) sub

    subCollection annIds ct1 ct2 = do
       ctntLower <- rcr (head $ children ct1) (head $ children ct2)
       annLower ct1 ct2 >>= return . foldl (@+) (tcol ctntLower annIds)

    coveringCollection ct rt@(tag -> QTCon (QTRecord _)) =
      collectionSubRecord ct rt >>= either (const $ lowerError ct rt) (const $ return ct)

    coveringCollection x y = lowerError x y

    annLower x@(annotations -> annA) y@(annotations -> annB) =
      let annAB   = nub $ annA ++ annB
          invalid = [QTMutable, QTImmutable]
      in if invalid `intersect` annAB == invalid then lowerError x y else return annAB

    lowerBound t@(tag -> QTOperator QTLower) = tvopevalWithLowerF rcr QTLower $ children t
    lowerBound t = return t

    lowerError x y = throwE $ PT.boxToString $
      [T.pack "Invalid lower bound on: "] %+ PT.prettyLines x %+
      [T.pack " and "] %+ PT.prettyLines y

-- | Type operator evaluation.
tvopeval :: QTOp -> [K3 QType] -> TInfM (K3 QType)
tvopeval op ch = tvopevalWithLowerF tvlower op ch

tvopevalShallow :: QTOp -> [K3 QType] -> TInfM (K3 QType)
tvopevalShallow op ch = tvopevalWithLowerF tvshallowLower op ch

tvopevalWithLowerF :: (K3 QType -> K3 QType -> TInfM (K3 QType)) -> QTOp -> [K3 QType] -> TInfM (K3 QType)
tvopevalWithLowerF _ _ [] = serrorM "Invalid qt operator arguments"
tvopevalWithLowerF lowerF QTLower ch = foldM lowerF (head ch) $ tail ch

consistentTLower :: [K3 QType] -> TInfM (K3 QType)
consistentTLower ch =
    let (varCh, nonvarCh) = partition isQTVar $ nub ch in
    case (varCh, nonvarCh) of
      ([], []) -> serrorM "Invalid lower qtype"
      ([], _)  -> tvopevalShallow QTLower nonvarCh
      (_, _)   -> lowerBoundWithVars varCh nonvarCh

  where
    lowerBoundWithVars vars extraLBTypes = do
      (boundTypes, boundVars, freeVars) <- foldM partitionBoundV ([],[],[]) vars
      lb <- tvopevalShallow QTLower (boundTypes ++ extraLBTypes)
      nch <- foldM unifyFreeVar [lb] $ nub freeVars
      return $ tlower $ nch ++ boundVars

    partitionBoundV (tacc,bacc,facc) t@(tag -> QTVar _) = do
      tve <- getTVE
      let bt = tvchase tve t
      case tag bt of
        QTVar _ -> return (tacc, bacc, facc++[bt])
        _ -> return (tacc++[bt], bacc++[t], facc)

    partitionBoundV _ _ = serrorM "Invalid type var during lower qtype merge"

    unifyFreeVar tacc t2@(tag -> QTVar v) = unifyv v (head tacc) >> return (t2:tacc)
    unifyFreeVar _ _ = serrorM "Invalid type var during lower qtype merge"


-- Unification helpers.
-- | Returns a self record and lifted attribute identifiers when given
--   a collection and a record that is a subtype of the collection.
collectionSubRecord :: K3 QType -> K3 QType -> TInfM (Either (Maybe (K3 QType)) (K3 QType, [Identifier]))
collectionSubRecord ct@(tag -> QTCon (QTCollection annIds)) (tag -> QTCon (QTRecord ids))
  = get >>= mkColQT >>= return . testF
  where
    mkColQT tienv = do
      memEnvs <- mapM (liftExceptM . tilkupa tienv) annIds
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
      then localLog $ prettyTaggedSPair "unifyv cycle" v1 t
      else do { localLog $ prettyTaggedSPair "unifyv var" v1 t;
                modify $ mtive $ \tve' -> tvext tve' v1 t }

unifyv v t = getTVE >>= \tve -> do
  if not $ occurs v t tve
    then do { localLog $ prettyTaggedSPair "unifyv noc" v t;
              modify $ mtive $ \tve' -> tvext tve' v t }

    else do { selfvars <- return $ findSelfVars tve v t;
              subQt <- tvsub False t;
              boundQt <- return $ substituteSelfQt selfvars t;
              localLog $ prettyTaggedTriple (unwords ["unifyv yoc", show v, show selfvars]) t subQt boundQt;
              modify $ mtive $ \tve' -> tvext (tvmap tve' $ substituteSelfQt selfvars) v boundQt; }

  where
    findSelfVars tve _ (tag &&& tvchase tve -> (QTVar v', t''))
      | occurs v t'' tve = findSelfVars tve v' t''
      | otherwise = []

    findSelfVars _ curv (tag &&& (all isQTRecord . children) -> (QTOperator QTLower, True)) = [curv]
    findSelfVars tve curv (Node _ ch) = concatMap (findSelfVars tve curv) ch

    substituteSelfQt _ t'@(tag -> QTVar _) = t'
    substituteSelfQt selfvars t' = aux selfvars t'
      where aux sv n@(Node (QTVar v2 :@: anns) _)
              | v2 `elem` sv = foldl (@+) tself anns
              | otherwise = n

            aux sv (Node tg ch) = Node tg (map (aux sv) ch)


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
        localLog $ prettyTaggedPair (unwords ["unifyDrvPreL", show p1]) qt1 qt1'
        localLog $ prettyTaggedPair (unwords ["unifyDrvPreR", show p2]) qt2 qt2'
        localLog $ prettyTaggedTriple "unifyDrv" qt1' qt2' qt
        postF (p1, p2) qt

    unifyDrv' :: K3 QType -> K3 QType -> TInfM (K3 QType)
    unifyDrv' t1@(isQTNumeric -> True) t2@(isQTNumeric -> True) = tvlower t1 t2

    unifyDrv' t1@(tag -> QTPrimitive p1) (tag -> QTPrimitive p2)
      | p1 == p2  = return t1
      | otherwise = primitiveErr p1 p2

    -- | Self type unification
    unifyDrv' t1@(tag -> QTCon (QTCollection _)) (tag -> QTSelf) = return t1
    unifyDrv' (tag -> QTSelf) t2@(tag -> QTCon (QTCollection _)) = return t2

    unifyDrv' (tag -> QTCon (QTRecord _)) t2@(tag -> QTSelf) = return t2
    unifyDrv' t1@(tag -> QTSelf) (tag -> QTCon (QTRecord _)) = return t1

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
          Left (Just selfRecordQt)            -> unifyErrT t1 t2 "collection-record" $ prettyTaggedPair "field-mismatch" selfRecordQt t2
          Left Nothing                        -> unifyErr  t1 t2 "collection-record" ""

    unifyDrv' t1@(tag -> QTCon (QTRecord _)) t2@(tag -> QTCon (QTCollection _))
      = collectionSubRecord t2 t1 >>= \case
          Right (selfRecordQt, liftedAttrIds) -> onCollection selfRecordQt liftedAttrIds t2 t1
          Left (Just selfRecordQt)            -> unifyErrT t1 t2 "collection-record" $ prettyTaggedPair "field-mismatch" selfRecordQt t1
          Left Nothing                        -> unifyErr  t1 t2 "collection-record" ""

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
                do
                  tienv <- get
                  let (lbE, _) = runTInfM tienv $ localLogAction (binaryLowerRecMsgF lb1 lb2) $ tvlower lb1 lb2
                  let validLB lb = if lb `elem` [lb1, lb2] then [lb1,lb2] else [lb,lb1,lb2]
                  return $ either (const $ [lb1,lb2]) validLB lbE

               (_,_) -> return [lb1, lb2]

      void $ foldM rcr (head $ lbs) $ tail lbs
      let allChQT = children t1 ++ children t2
      r <- localLogAction (binaryLowerMsgF allChQT) $ consistentTLower allChQT
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
      localLogAction (unaryLowerMsgF "L") $ consistentTLower $ children t1 ++ [nlb]

    unifyDrv' t1 t2@(tag -> QTOperator QTLower) = do
      lb2 <- lowerBound t2
      nlb <- rcr t1 lb2
      localLogAction (unaryLowerMsgF "R") $ consistentTLower $ [nlb] ++ children t2

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
      localLog $ prettyTaggedPair "unifyDrv recurring from " qt1 qt2
      localLog $ prettyTaggedPair "unifyDrv recurring on "   a   b
      let npath1 = extendPath path1 qt1
      let npath2 = extendPath path2 qt2
      unifyDrvWithPaths preF postF npath1 npath2 a b

    onCollectionPair :: [Identifier] -> K3 QType -> K3 QType -> TInfM (K3 QType)
    onCollectionPair annIds t1@(annotations -> a1) t2@(annotations -> a2) =
      rcr (head $ children t1) (head $ children t2) >>= \cqt -> return (tcol cqt annIds @<- (nub $ a1 ++ a2))

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
      throwE $ T.unlines [T.pack "Invalid collection arguments:", PT.pretty ct, T.pack "and", PT.pretty rt]

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
      | tga `elem` tTrgOrSink && tgb `elem` tTrgOrSink = onList a b (tdata QTTrigger) $ \s -> unifyErr tga tgb kind s
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

    tTrgOrSink = [QTTrigger, QTSink]

    primitiveErr a b = unifyErr a b "primitives" ""

    unifyErr a b kind s = unifyErrT a b kind $ T.pack s

    unifyErrT a b kind s = errorM $ PT.boxToString $
      [T.unwords (map T.pack ["Unification mismatch on ", kind, ":("])]
        %$ PT.indent 2 [s] %$ [T.pack ")"] %$ (PT.prettyLines a %+ [T.pack " vs. "] %+ PT.prettyLines b)

    subSelfErr ct = errorM $ PT.boxToString $
      [T.pack "Invalid self substitution, qtype is not a collection: "] ++ PT.prettyLines ct

    unaryLowerMsgF _ Nothing = Nothing
    unaryLowerMsgF suffix (Just r) =
      Just $ PT.boxToString $ [T.pack $ "consistentTLower" ++ suffix ++ " "] %+ PT.prettyLines r

    binaryLowerMsgF _ Nothing = Nothing
    binaryLowerMsgF args (Just ret) =
      Just $ PT.boxToString $ [T.pack "consistentTLowerB "]
               %+ (PT.intersperseBoxes [T.pack " "] $ map PT.prettyLines $ args ++ [ret])

    binaryLowerRecMsgF _ _ Nothing = Nothing
    binaryLowerRecMsgF lrec1 lrec2 (Just lret) =
      Just $ PT.boxToString $ [T.pack "consistentTLowerB record "]
               %+ (PT.intersperseBoxes [T.pack " "] $ map PT.prettyLines $ [lrec1, lrec2, lret])


-- | Type unification.
unifyM :: K3 QType -> K3 QType -> (Text -> Text) -> TInfM ()
unifyM t1 t2 errf = void $ localLogAction msgF $ reasonM errf $ unifyDrv preChase postId t1 t2
  where
    preChase qt = getTVE >>= \tve -> return ((), tvchase tve qt)
    postId _ qt = return qt
    msgF Nothing  = Just $ prettyTaggedPair "unifyM call" t1 t2
    msgF (Just r) = Just $ prettyTaggedTriple "unifyM result" t1 t2 r

-- | Type unification with variable overrides to the unification result.
unifyWithOverrideM :: K3 QType -> K3 QType -> (Text -> Text) -> TInfM (K3 QType)
unifyWithOverrideM qt1 qt2 errf =
  localLogAction msgF $ reasonM errf $ unifyDrv preChase (\x y -> postUnifyCased x y >>= logPostUnify) qt1 qt2
  where
    preChase qt = getTVE >>= \tve -> return $ tvchasev tve Nothing qt

    logPostUnify r = localLog (PT.boxToString $ [T.pack "postUnify "] %+ PT.prettyLines r) >> return r

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
          (_, _, _, True) -> serrorM "Unhandled case in unifyTwoChain"
          (_, _, _, _) ->
            if snd v2 == fst v1 then return (fst v2, snd v1, Nothing)
            else return (fst v1, snd v2, if snd v1 /= fst v2 then Just (snd v1, fst v2) else Nothing)

      checkedUnify trg qt
      void $ maybe (localLog $ T.pack "unifyTwoChain no chain") (\(s,t) -> checkedUnify s $ vtCtor t) chain
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
        uqt <- inlineCyclicUnifyTarget tve [] qt
        localLog $ prettyTaggedSTriple "checkedUnify adding cycle" v qt uqt
        unifyv v uqt

    inlineCyclicUnifyTarget tve path qt@(tag -> QTVar v)
      | v `elem` path = return qt
      | otherwise = inlineCyclicUnifyTarget tve (v:path) $ tvchase tve qt

    inlineCyclicUnifyTarget tve path qt@(tag -> QTOperator QTLower) = do
      lqt <- tvopeval QTLower (children qt)
      nch <- inlineCyclicUnifyTarget tve path lqt
      return $ tlower [nch]

    inlineCyclicUnifyTarget _ _ qt = return qt

    msgF Nothing  = Just $ prettyTaggedPair "unifyOvM call" qt1 qt2
    msgF (Just r) = Just $ prettyTaggedTriple "unifyOvM result" qt1 qt2 r

-- | Given a polytype, for every polymorphic type var, replace all of
--   its occurrences in t with a fresh type variable. We do this by
--   creating a substitution tve and applying it to t.
--   We also strip any mutability qualifiers here since we only instantiate
--   on variable access.
instantiate :: QPType -> TInfM (K3 QType)
instantiate (QPType tvs t) = withFreshTVE $ do
  (Node (tg :@: anns) ch) <- tvsub False t
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
 t'         <- tvsub False t
 let tvdep = tvdependentset tve_before tve_after
 let fv    = filter (not . tvdep) $ nub $ typevars t'
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

        subAnns (EQType qt) = tvsub False qt >>= return . EQType
        subAnns x = return x

{- Type initialization -}

-- | Initialize a declaration's type from its type signature.
initializeDeclType :: K3 Declaration -> TInfM (K3 Declaration)
initializeDeclType d@(tag -> DGlobal n t _) = withUnique n $ do
  qpt <- qpType t
  modify (\env -> tiextc (tiexte env n qpt) n $ isTFunction t)
  return d

initializeDeclType d@(tag -> DTrigger n t _) = withUnique n $ do
    qpt <- trigType t
    modify (\env -> tiextc (tiexte env n qpt) n True)
    return d
  where trigType x = qType x >>= \qt -> return (ttrg qt) >>= monomorphize

initializeDeclType d@(tag -> DDataAnnotation n tdeclvars mems) = withUniqueA n $ mkAnnMemEnv >> return d
  where mkAnnMemEnv = mapM memType mems >>= \l -> modify (\env -> tiexta env n $ BEnv.fromList $ catMaybes l)
        memType (Lifted      _ mn mt _ _) = unifyMemInit True  mn mt
        memType (Attribute   _ mn mt _ _) = unifyMemInit False mn mt
        memType (MAnnotation _ _ _) = return Nothing
        unifyMemInit lifted mn mt = do
          qpt <- qpType (TC.forAll tdeclvars mt)
          return (Just (mn, (qpt, lifted)))

initializeDeclType d = return d

-- | Initialization helpers.
withUnique :: Identifier -> TInfM (K3 Declaration) -> TInfM (K3 Declaration)
withUnique n m = failOnValid (return ()) (uniqueErr "declaration" n) (flip tilkupe n) >>= const m

withUniqueA :: Identifier -> TInfM (K3 Declaration) -> TInfM (K3 Declaration)
withUniqueA n m = failOnValid (return ()) (uniqueErr "annotation" n) (flip tilkupa n) >>= const m

failOnValid :: TInfM () -> TInfM () -> (TIEnv -> Except a b) -> TInfM ()
failOnValid success failure f = do
  env <- get
  either (const $ success) (const $ failure) $ runExcept $ f env

uniqueErr :: String -> Identifier -> TInfM a
uniqueErr s n = serrorM $ unwords ["Invalid unique", s, "identifier:", n]


{- Top-level type inference methods -}

-- | Whole program inference
inferProgramTypes :: K3 Declaration -> Either String (K3 Declaration, TIEnv)
inferProgramTypes prog = do
    (_, initEnv)      <- runTInfES tienv0 $ initializeTypeEnv
    (nProg, finalEnv) <- runTInfES (resetCyclicEnv initEnv) $ inferAllDecls
    localLog $ T.pack $ "Final type environment"
    localLog $ PT.pretty finalEnv
    return (nProg, finalEnv)

  where
    initializeTypeEnv :: TInfM (K3 Declaration)
    initializeTypeEnv = mapProgram initializeDeclType return return Nothing prog

    inferAllDecls :: TInfM (K3 Declaration)
    inferAllDecls = mapProgramWithDecl inferDeclTypes (const return) (const return) Nothing prog

-- | Repeat inference for a specific declaration.
reinferProgDeclTypes :: TIEnv -> Identifier -> K3 Declaration -> Either String (K3 Declaration, TIEnv)
reinferProgDeclTypes env dn prog = runTInfES env inferNamedDecl
  where
    inferNamedDecl = mapProgramWithDecl onNamedDecl (const return) (const return) Nothing prog

    onNamedDecl d@(tag -> DGlobal  n _ _) | dn == n = inferDeclTypes d >>= translateDecl
    onNamedDecl d@(tag -> DTrigger n _ _) | dn == n = inferDeclTypes d >>= translateDecl
    onNamedDecl d = return d

    translateDecl d@(tag -> DGlobal  n t eOpt) = do
      neOpt <- liftExceptM $ maybe (return Nothing) (\e -> translateExprTypes e >>= return . Just) eOpt
      return $ replaceTag d $ DGlobal n t neOpt

    translateDecl d@(tag -> DTrigger n t e) = do
      ne <- liftExceptM $ translateExprTypes e
      return $ replaceTag d $ DTrigger n t ne

    translateDecl d = return d

-- | Declaration type inference
inferDeclTypes :: K3 Declaration -> TInfM (K3 Declaration)
inferDeclTypes d@(tag -> DGlobal n t eOpt) = inferAsCyclicType n $ do
  qptAct  <- if isTFunction t then return IDAFunction
                              else (qpType t >>= return . IDAExtend)
  if isTEndpoint t
    then return d
    else do
      unifyDeclInitializer n True qptAct eOpt >>= \neOpt' ->
             return $ (Node (DGlobal n t neOpt' :@: annotations d) $ children d)

inferDeclTypes d@(tag -> DTrigger n t e) = inferAsCyclicType n $ do
  env <- get
  QPType qtvars qt <- liftExceptM (tilkupe env n)
  case tag qt of
    QTCon QTTrigger ->
      let nqptAct = IDATrigger $ QPType qtvars $ tfun (head $ children qt) tunit
      in unifyDeclInitializer n True nqptAct (Just e) >>= \neOpt ->
           return $ maybe d (\ne' -> Node (DTrigger n t ne' :@: annotations d) $ children d) neOpt

    _ -> serrorM $ "Invalid trigger declaration type for: " ++ n

inferDeclTypes d@(tag -> DDataAnnotation n tvars mems) = do
    env   <- get
    amEnv <- liftExceptM (tilkupa env n)
    nmems <- chkAnnMemEnv amEnv
    return (Node (DDataAnnotation n tvars nmems :@: annotations d) $ children d)

  where
    chkAnnMemEnv amEnv = mapM (memType amEnv) mems

    memType amEnv (Lifted mp mn mt meOpt mAnns) =
      unifyMemInit amEnv mn meOpt >>= \nmeOpt -> return (Lifted mp mn mt nmeOpt mAnns)

    memType amEnv (Attribute   mp mn mt meOpt mAnns) =
      unifyMemInit amEnv mn meOpt >>= \nmeOpt -> return (Attribute mp mn mt nmeOpt mAnns)

    memType _ mem@(MAnnotation _ _ _) = return mem

    unifyMemInit amEnv mn meOpt = do
      qpt <- maybe (memLookupErr mn) (return . fst) (BEnv.mlookup amEnv mn)
      unifyDeclInitializer mn False (IDAPassThru qpt) meOpt

    memLookupErr mn = serrorM $ "No annotation member in initial environment: " ++ mn

inferDeclTypes d = return d

inferAsCyclicType :: Identifier -> TInfM a -> TInfM a
inferAsCyclicType n m = do
  cyclic <- get >>= liftExceptM . flip tilkupc n
  if cyclic then withCyclicEnv n m else m

unifyDeclInitializer :: Identifier -> Bool -> IDeclaredAction -> Maybe (K3 Expression)
                     -> TInfM (Maybe (K3 Expression))
unifyDeclInitializer n asCyclic qptAct eOpt = do
  qpt <- case qptAct of
           IDAExtend     qpt' -> modify (\env -> tiexte (tidele env n) n qpt') >> return qpt'
           IDAPassThru   qpt' -> return qpt'
           IDAFunction        -> get >>= \env -> liftExceptM (tilkupe env n) >>= \qpt' -> initializerPropagatedQPs qpt' eOpt >> return qpt'
           IDATrigger    qpt' -> initializerPropagatedQPs qpt' eOpt >> return qpt'

  case eOpt of
    Just e -> do
      qt1 <- instantiate qpt
      ne  <- (if asCyclic then inferAsCyclicType n else id) $ inferExprTypes e
      qt2 <- qTypeOfM ne
      localLog $ prettyTaggedPair ("unify init ") qt1 qt2
      void $ unifyWithOverrideM qt1 qt2 $ mkErrorF ne unifyInitErrF
      substituteDeepQt ne >>= return . Just

    Nothing -> return Nothing

  where
    mkErrorF :: K3 Expression -> (Text -> Text) -> (Text -> Text)
    mkErrorF e f s = spanAsString `T.append` f s
      where spanAsString = let uidSpans = filter (\a -> isESpan a || isEUID a) $ annotations e
                           in if null uidSpans
                                then (PT.boxToString $ [T.pack "["] %+ PT.prettyLines e %+ [T.pack "]"])
                                else T.pack $ show uidSpans

    unifyInitErrF s = (T.pack "Failed to unify initializer: ") `T.append` s

initializerPropagatedQPs :: QPType -> Maybe (K3 Expression) -> TInfM ()
initializerPropagatedQPs _ Nothing = return ()
initializerPropagatedQPs qpt (Just e) = modify (\env -> env {tprop = bindings})
  where bindings = mkStack [] qpt e
        mkStack acc (QPType _ (tnc -> (QTCon QTFunction, [a,r]))) (tnc -> (ELambda i, [b])) =
          mkStack (acc ++ [(i, QPType [] a)]) (QPType [] r) b

        mkStack acc _ _ = acc


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
    lambdaBinding _ (tag -> ELambda i) = do
      env <- get
      case tprop env of
        (j,qpt):t | i == j -> modify (\env' -> tiexte (env' {tprop = t}) i qpt)
        _ -> modify (\env' -> env' {tprop = []}) >> newtv >>= monoBinding i

    lambdaBinding _ _ = return ()

    sidewaysBinding :: K3 Expression -> K3 Expression -> TInfM [TInfM ()]
    sidewaysBinding ch1 (tag -> ELetIn i) = do
      ipt <- generalize $ qTypeOfM ch1
      modify $ \env -> tiexte env i ipt
      return [iu]

    sidewaysBinding ch1 e@(tag -> EBindAs b) = do
        ch1T <- qTypeOfM ch1
        case b of
          BIndirection i -> do
            itv <- newtv
            void $ unifyWithOverrideM ch1T (tind itv) $ bindErr e "indirection"
            monoBinding i itv

          BTuple ids -> do
            idtvs <- mapM (const newtv) ids
            void   $ unifyWithOverrideM ch1T (ttup idtvs) $ bindErr e "tuple"
            mapM_ (uncurry monoBinding) $ zip ids idtvs

          -- We unify with a lower-bounded record since records may be partially bound.
          BRecord ijs -> do
            jtvs <- mapM (const newtv) ijs
            void $  unifyWithOverrideM ch1T (tlower $ [trec $ flip zip jtvs $ map fst ijs]) $ bindErr e "record"
            mapM_ (uncurry monoBinding) $ flip zip jtvs $ map snd ijs

        return [iu]

      where
        bindErr errE kind reason = T.unlines [ T.unwords $ (map T.pack ["Invalid", kind, "bind-as:"]) ++ [reason]
                                             , T.pack "On:", PT.pretty errE
                                             , T.pack "Toplevel:", PT.pretty expr ]

    sidewaysBinding ch1 (tag -> ECaseOf i) = do
      ch1T <- qTypeOfM ch1
      itv  <- newtv
      void $  unifyWithOverrideM ch1T (topt itv) $ conerrF $ "Invalid case-of source expression: "
      return [monoBinding i itv, modify $ \env -> tidele env i]

    sidewaysBinding _ (children -> ch) = return (replicate (length ch - 1) iu)

    inferQType :: [K3 Expression] -> K3 Expression -> TInfM (K3 Expression)
    inferQType ch n = do
      (ruleTag, r) <- inferTagQType ch n
      localLog $ showTInfRule ruleTag ch r
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
        qt  <- tryExceptM (lookupError i) instantiate $ tilkupe env i
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
        ipt  <- tryExceptM (lambdaBindingErr i) return $ tilkupe env i
        chqt <- qTypeOfM $ head ch
        void $ modify $ \env' -> tidele env' i
        case ipt of
          QPType [] iqt -> return $ ("lambda",) $ rebuildE n ch .+ tfun iqt chqt
          _             -> polyLambdaBindingErr

    -- | Assignment expressions unify their source and target types, as well as
    --   ensuring that the source is mutable.
    inferTagQType ch n@(tag -> EAssign i) = do
      env <- get
      ipt <- tryExceptM (assignBindingErr i) return $ tilkupe env i
      eqt <- qTypeOfM $ head ch
      case ipt of
        QPType [] iqt@(tag -> QTVar _) ->
          let nqt = tvchase (tvenv env) iqt in
          if nqt == iqt
            then do { void $ unifyWithOverrideM iqt (eqt @+ QTMutable) $ mkErrorF expr n $ assignErrF i;
                      return $ ("assign",) $ rebuildE n ch .+ tunit }
            else unifyWithMutableQt eqt nqt

        QPType [] iqt -> unifyWithMutableQt eqt iqt
        _ -> polyAssignBindingErr

      where unifyWithMutableQt eqt iqt
              | (iqt @~ isQTQualified) == Just QTMutable =
                  do { void $ unifyM (iqt @- QTMutable) eqt $ mkErrorF expr n $ assignErrF i;
                       return $ ("assign",) $ rebuildE n ch .+ tunit }
              | otherwise = mutabilityErr i

    inferTagQType ch n@(tag -> EProject i) = do
      srcqt   <- qTypeOfM $ head ch
      fieldqt <- newtv
      let prjqt = tlower $ [trec [(i, fieldqt)]]
      void   $ unifyWithOverrideM srcqt prjqt $ mkErrorF expr n $ projectErrF srcqt prjqt
      return $ ("project",) $ rebuildE n ch .+ fieldqt

    -- TODO: reorder inferred record fields based on argument at application.
    inferTagQType ch n@(tag -> EOperate OApp) = do
      fnqt   <- qTypeOfM $ head ch
      argqt  <- qTypeOfM $ last ch
      retqt  <- newtv
      void   $ unifyWithOverrideM fnqt (tfun argqt retqt) $ mkErrorF expr n $ applyErrF fnqt argqt retqt
      return $ ("apply",) $ rebuildE n ch .+ retqt

    inferTagQType ch n@(tag -> EOperate OSeq) = do
        lqt <- qTypeOfM $ head ch
        rqt <- qTypeOfM $ last ch
        void $ unifyM tunit lqt $ mkErrorF expr n seqErrF
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
          void $ unifyM lqt rqt $ mkErrorF expr n comparisonError
          return $ ("comp",) $ rebuildE n ch .+ tbool

      | logic op = do
            void $ unifyBinaryM tbool tbool ch n logicError
            return $ ("logic",) $ rebuildE n ch .+ tbool

      | textual op = do
            void $ unifyBinaryM tstr tstr ch n stringError
            return $ ("text",) $ rebuildE n ch .+ tstr

      | op == ONeg = do
            chqt <- unifyUnaryM tnum ch $ mkErrorF expr n uminusError
            let resultqt = case tag chqt of
                             QTPrimitive _  -> chqt
                             QTVar _ -> chqt
                             _ -> tnum
            return $ ("neg",) $ rebuildE n ch .+ resultqt

      | op == ONot = do
            void $ unifyUnaryM tbool ch $ mkErrorF expr n negateError
            return $ ("not",) $ rebuildE n ch .+ tbool

      | otherwise = serrorM $ "Invalid operation: " ++ show op

      where
        delayNumericQt l r
          | any (\qt -> isQTVar qt || isQTLower qt) [l, r] = return $ tlower $ concatMap childrenOrSelf [l,r]
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
      retqt <- unifyWithOverrideM sqt nqt $ mkErrorF expr n caseErrF
      return $ ("case-of",) $ rebuildE n ch .+ retqt

    inferTagQType ch n@(tag -> EIfThenElse) = do
      pqt   <- qTypeOfM $ head ch
      tqt   <- qTypeOfM $ ch !! 1
      eqt   <- qTypeOfM $ last ch
      void  $  unifyM pqt tbool $ mkErrorF expr n $ (conerrF $ "Invalid if-then-else predicate: ")
      retqt <- unifyWithOverrideM tqt eqt $ mkErrorF expr n $ (conerrF $ "Mismatched condition branches: ")
      return $ ("if-then",) $ rebuildE n ch .+ retqt

    inferTagQType ch n@(tag -> EAddress) = do
      hostqt <- qTypeOfM $ head ch
      portqt <- qTypeOfM $ last ch
      void $ unifyM hostqt tstr $ mkErrorF expr n $ (conerrF $ "Invalid address host string: ")
      void $ unifyM portqt tint $ mkErrorF expr n $ (conerrF $ "Invalid address port int: ")
      return $ ("address",) $ rebuildE n ch .+ taddr

    inferTagQType ch n  = return $ ("<unhandled>",) $ rebuildE n ch
      -- ^ TODO unhandled: ESelf, EImperative

    rebuildE (Node t _) ch = Node t ch

    unifyBinaryM lexpected rexpected ch n errf = do
      lqt <- qTypeOfM $ head ch
      rqt <- qTypeOfM $ last ch
      void $ unifyM lexpected lqt (mkErrorF expr n $ errf $ T.pack "left")
      void $ unifyM rexpected rqt (mkErrorF expr n $ errf $ T.pack "right")
      return (lqt, rqt)

    unifyUnaryM expected ch errf = do
        chqt <- qTypeOfM $ head ch
        void $ unifyM chqt expected errf
        return chqt

    numeric    op = op `elem` [OAdd, OSub, OMul, ODiv, OMod]
    comparison op = op `elem` [OEqu, ONeq, OLth, OLeq, OGth, OGeq]
    logic      op = op `elem` [OAnd, OOr]
    textual    op = op `elem` [OConcat]

    mkErrorF :: K3 Expression -> K3 Expression -> (Text -> Text) -> (Text -> Text)
    mkErrorF tle e f s = (T.pack uidSpanAsString) `T.append` f s `T.append` exprsAsString
      where
        uidSpanAsString =
          let uidSpans = filter isEUIDSpan $ annotations e
          in if null uidSpans then "" else show uidSpans

        exprsAsString =
          (PT.boxToString $ [T.pack "On ["]       %$ PT.prettyLines e   %$ [T.pack "]"]
                         %$ [T.pack "Toplevel ["] %$ PT.prettyLines tle %$ [T.pack "]"])

    -- | Error printing functions for unification cases
    conerrF    s = T.append $ T.pack s
    assignErrF i = conerrF $ "Invalid assignment to " ++ i ++ ": "
    seqErrF      = conerrF $ "Invalid left sequence operand: "
    caseErrF     = conerrF $ "Mismatched case-of branch types: "

    projectErrF srcqt prjqt =
      (T.append $ T.unlines [T.pack "Invalid record projection:", PT.pretty srcqt, T.pack "and", PT.pretty prjqt])

    applyErrF fnqt argqt retqt =
      (T.append $ T.unlines [ T.pack "Invalid function application:", PT.pretty fnqt
                            , T.pack "and", PT.pretty (tfun argqt retqt), T.pack ":"])

    lookupError j reason      = errorM $ T.unwords [T.pack "No type environment binding for ", T.pack j, T.pack ":", reason]
    lambdaBindingErr i reason = errorM $ T.unwords [T.pack "Could not find typevar for lambda binding: ", T.pack i, reason]
    polyLambdaBindingErr      = errorM $ T.pack "Invalid forall type in lambda binding"

    assignBindingErr i reason = errorM $ T.unwords [T.pack "Could not find binding type for assignment: ", T.pack i, reason]
    polyAssignBindingErr      = errorM $ T.pack "Invalid forall type in assignment"
    mutabilityErr j           = errorM $ T.pack $ "Invalid assignment to non-mutable binding: " ++ j

    sndError     side reason = T.unwords [T.pack "Invalid", side, T.pack "send operand: ",    reason]
    numericError side reason = T.unwords [T.pack "Invalid", side, T.pack "numeric operand: ", reason]
    stringError  side reason = T.unwords [T.pack "Invalid", side, T.pack "string operand: ",  reason]
    logicError   side reason = T.unwords [T.pack "Invalid", side, T.pack "logic operand:",    reason]
    comparisonError   reason = conerrF "Invalid comparison operands:" reason

    uminusError reason = conerrF "Invalid unary minus operand: " reason
    negateError reason = conerrF "Invalid negation operand: " reason

    -- | Type judgement stringification, given a tag, premise expressions and a conclusion expression.
    showTInfRule :: String -> [K3 Expression] -> K3 Expression -> Text
    showTInfRule rtag ch n =
      PT.boxToString $ (rpsep %+ premise) %$ separator %$ (rpsep %+ conclusion)
      where nuid           = uidOf $ n @~ isEUID
            rprefix        = unwords [rtag, nuid]
            (rplen, rpsep) = (length rprefix, [T.pack $ replicate rplen ' '])
            premise        = if null ch then [T.pack "<empty>"]
                                        else (PT.intersperseBoxes [T.pack " , "] $ map PT.prettyLines $ mapMaybe qTypeOf ch)
            premLens       = map T.length premise
            headWidth      = if null premLens then 4 else maximum premLens
            separator      = [T.pack $ rprefix ++ replicate headWidth '-']
            conclusion     = maybe [T.pack "<invalid>"] PT.prettyLines $ qTypeOf n
            uidOf (Just (EUID u)) = show u
            uidOf _               = "<No UID>"


{- Collection type construction -}

mkCollectionQType :: [Identifier] -> K3 QType -> TInfM (K3 QType)
mkCollectionQType annIds contentQt@(tag -> QTCon (QTRecord _)) = return $ tcol contentQt annIds
mkCollectionQType _ qt = serrorM $ "Invalid content record type: " ++ show qt

mkCollectionFSQType :: [Identifier] -> [TMEnv] -> K3 QType -> TInfM (K3 QType, K3 QType)
mkCollectionFSQType annIds memEnvs contentQt = do
    flatEnvs <- assertNoDuplicateIds
    let (lifted, regular) = BEnv.partition (const snd) flatEnvs
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
      let ids = concat $ map BEnv.keys memEnvs
      in if nub ids /= ids then nameConflictErr else return $ BEnv.unions memEnvs

    membersAsRecordFields attrs =
      sequence $ BEnv.foldl (\acc j (qpt,_) -> acc ++ [instantiate qpt >>= return . (j,)]) [] attrs

    nameConflictErr        = serrorM $ "Conflicting annotation member names: " ++ show annIds
    nonRecordContentErr qt = serrorM $ "Invalid content record type: " ++ show qt


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
    varId _ = serrorM $ "Invalid type variable for type var bindings"

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
        case annIds of
          [] -> errorM $ PT.boxToString $ [T.pack "No collection annotations found on "] %+ PT.prettyLines n
          _ -> mkCollectionQType annIds cqt

    mkQType ch (tag -> TFunction) = return $ tfun (head ch) $ last ch
    mkQType ch (tag -> TTrigger)  = return $ ttrg $ head ch
    mkQType ch (tag -> TSource)   = return $ tsrc $ head ch
    mkQType ch (tag -> TSink)     = return $ tsnk $ head ch

    mkQType _ (tag -> TBuiltIn TContent)   = return tcontent
    mkQType _ (tag -> TBuiltIn TStructure) = return tfinal
    mkQType _ (tag -> TBuiltIn TSelf)      = return tself

    mkQType _ (tag -> TDeclaredVar x) = get >>= \tienv -> liftExceptM (tilkupdv tienv x)

    mkQType _ (tag -> TForall _) = serrorM "Invalid forall type for QType"
      -- ^ TODO: we can only handle top-level foralls, and not arbitrary
      --   foralls nested in type trees.

    mkQType _ t_ = serrorM $ "No QType construction for " ++ show t_

    mutability0 nch n = mutabilityT (head $ children n) $ head nch
    mutabilityN nch n = map (uncurry mutabilityT) $ zip (children n) nch


-- | Converts all QType annotations on program expressions to K3 types.
translateProgramTypes :: K3 Declaration -> Either String (K3 Declaration)
translateProgramTypes prog = liftExceptTM $ mapProgram declF annMemF exprF Nothing prog
  where liftExceptTM m = either (Left . T.unpack) Right $ runExcept m
        declF   d = return d
        annMemF m = return m
        exprF   e = translateExprTypes e

translateExprTypes :: K3 Expression -> Except Text (K3 Expression)
translateExprTypes expr = mapTree translate expr >>= \e -> return $ flip addTQualifier e $ exprTQualifier expr
  where
    translate nch e@(Node (tg :@: anns) _) = do
      let nch' = case tg of
                   ELetIn _ -> [flip addTQualifier (head nch) $ letTQualifier e] ++ tail nch
                   _        -> nch
      nanns <- mapM (translateEQType expr e $ e @~ isESpan) $ filter (not . isEType) anns
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

    translateEQType tle e spanOpt (EQType qt) = translateQType tle e spanOpt qt >>= return . EType
    translateEQType _ _ _ x = return x

    translateAnnotation a = case a of
      QTMutable   -> TMutable
      QTImmutable -> TImmutable
      QTWitness   -> TWitness


translateQType :: K3 Expression -> K3 Expression -> Maybe (Annotation Expression) -> K3 QType -> Except Text (K3 Type)
translateQType toplevel_expr expr spanOpt qt = mapTree translateWithMutability qt
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
              in throwE $ PT.boxToString
                        $ [T.pack $ unwords $ [msg, "(", maybe "" show spanOpt, ")"]]
                        %$ PT.prettyLines qt'
                        %$ [T.pack "On expression"]
                        %$ PT.prettyLines expr
                        %$ [T.pack "Toplevel expr"]
                        %$ PT.prettyLines toplevel_expr

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

        translate _ qt' = throwE . T.pack . unwords $
                            ["No translation for ", "(", maybe "" show spanOpt, ")", show qt']


{- Instances -}
instance Pretty TIEnv where
  prettyLines e =
    [T.pack "TEnv: "]    %$ (PT.indent 2 $ PT.prettyLines $ tenv    e) ++
    [T.pack "TAEnv: "]   %$ (PT.indent 2 $ PT.prettyLines $ taenv   e) ++
    [T.pack "TDVEnv: "]  %$ (PT.indent 2 $ PT.prettyLines $ tdvenv  e) ++
    [T.pack "TVEnv: "]   %$ (PT.indent 2 $ PT.prettyLines $ tvenv   e) ++
    [T.pack "TCyclic: "] %$ (PT.indent 2 $ PT.prettyLines $ tcyclic e) ++
    [T.pack "TCEnv: "]   %$ (PT.indent 2 $ PT.prettyLines $ tcenv   e)

instance Pretty TEnv where
  prettyLines = prettyPairBSEnv

instance Pretty TAEnv where
  prettyLines = prettyPairBEnv

instance Pretty TMEnv where
  prettyLines = prettyPairBEnv

instance Pretty TDVEnv where
  prettyLines = prettyPairBSEnv

instance Pretty TVEnv where
  prettyLines (TVEnv n m) = [T.pack $ "# vars: " ++ show n] ++
                            (IntMap.foldlWithKey' (\acc k v -> acc ++ prettyPair (k,v)) [] m)

instance Pretty TCEnv where
  prettyLines tce = BEnv.foldl (\acc k v -> acc ++ [T.pack $ k ++ " => " ++ show v]) [] tce

instance Pretty (QPType, Bool) where
  prettyLines (a,b) = (map T.pack $ if b then ["(Lifted) "] else ["(Attr) "]) %+ PT.prettyLines a

prettyPairBEnv :: (Pretty a) => BindingEnv a -> [Text]
prettyPairBEnv e = BEnv.foldl (\acc n v -> acc ++ prettyPair (n,v)) [] e

prettyPairBSEnv :: (Pretty a) => BindingStackEnv a -> [Text]
prettyPairBSEnv e = BEnv.foldl (\acc n v -> acc ++ concatMap (prettyPair . (n,)) v) [] e

prettyPairList :: (Show a, Pretty b) => [(a,b)] -> [Text]
prettyPairList l = foldl (\acc kvPair -> acc ++ prettyPair kvPair) [] l

prettyPair :: (Show a, Pretty b) => (a,b) -> [Text]
prettyPair (a,b) = [T.pack $ show a ++ " => "] %+ PT.prettyLines b

prettyTaggedSPair :: (Show a, Pretty b) => String -> a -> b -> Text
prettyTaggedSPair s a b = PT.boxToString $ [T.pack $ s ++ " " ++ show a] %+ [T.pack " and "] %+ PT.prettyLines b

prettyTaggedPair :: (Pretty a, Pretty b) => String -> a -> b -> Text
prettyTaggedPair s a b = PT.boxToString $ [T.pack $ s ++ " "] %+ PT.prettyLines a %+ [T.pack " and "] %+ PT.prettyLines b

prettyTaggedTriple :: (Pretty a, Pretty b, Pretty c) => String -> a -> b -> c -> Text
prettyTaggedTriple s a b c =
  PT.boxToString $ [T.pack $ s ++ " "] %+ (PT.intersperseBoxes [T.pack " , "]
                   [PT.prettyLines a, PT.prettyLines b, PT.prettyLines c])

prettyTaggedSTriple :: (Show a, Pretty b, Pretty c) => String -> a -> b -> c -> Text
prettyTaggedSTriple s a b c =
  PT.boxToString $ [T.pack $ s ++ " "] %+ (PT.intersperseBoxes [T.pack " , "]
                   [[T.pack $ show a], PT.prettyLines b, PT.prettyLines c])
