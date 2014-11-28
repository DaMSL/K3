{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Provenance (i.e., dataflow) analysis for K3 programs.
module Language.K3.Analysis.Provenance.Inference where

import Control.Arrow hiding ( left )
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Maybe
import Data.Map    ( Map    )
import Data.IntMap ( IntMap )

import qualified Data.Map    as Map
import qualified Data.IntMap as IntMap

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Constructors

import Language.K3.Utils.Pretty

-- | A provenance bindings environment.
type PEnv = Map Identifier (K3 Provenance)

-- | A provenance bindings environment for annotations,
--   indexed by annotation and attribute name.
type PAEnv = Map Identifier PMEnv
type PMEnv = Map Identifier (K3 Provenance, Bool)

-- | A provenance "pointer" environment
type PPEnv = IntMap (K3 Provenance)

-- | A lambda closure environment: ELambda UID => identifiers of closure variables.
type PLCEnv = IntMap [Identifier]

-- | A provenance inference environment.
data PIEnv = PIEnv {
               pcnt    :: Int,
               ppenv   :: PPEnv,
               penv    :: PEnv,
               paenv   :: PAEnv,
               plcenv  :: PLCEnv
            }

-- | The type inference monad
type PInfM = EitherT String (State PIEnv)

{- PPEnv helpers -}
ppenv0 :: PPEnv
ppenv0 = IntMap.empty

pplkup :: PPEnv -> Int -> Either String (K3 Provenance)
pplkup env x = maybe err Right $ IntMap.lookup x env
  where err = Left $ "Unbound variable in lineage environment: " ++ show x

ppext :: PPEnv -> Int -> K3 Provenance -> PPEnv
ppext env x p = IntMap.insert x p env

ppdel :: PPEnv -> Int -> PPEnv
ppdel env x = IntMap.delete x env


{- PEnv helpers -}
penv0 :: PEnv
penv0 = Map.empty

plkup :: PEnv -> Identifier -> Either String (K3 Provenance)
plkup env x = maybe err Right $ Map.lookup x env
  where err = Left $ "Unbound variable in lineage environment: " ++ x

pext :: PEnv -> Identifier -> K3 Provenance -> PEnv
pext env x p = Map.insert x p env

pdel :: PEnv -> Identifier -> PEnv
pdel env x = Map.delete x env

{- PAEnv helpers -}
paenv0 :: PAEnv
paenv0 = Map.empty

pmenv0 :: PMEnv
pmenv0 = Map.empty

palkup :: PAEnv -> Identifier -> Identifier -> Either String (K3 Provenance)
palkup env x y = maybe err (maybe err (Right . fst) . Map.lookup y) $ Map.lookup x env
  where err = Left $ "Unbound annotation member in lineage environment: " ++ x

paext :: PAEnv -> Identifier -> Identifier -> K3 Provenance -> Bool -> PAEnv
paext env x y p l = Map.insertWith Map.union x (Map.fromList [(y,(p,l))]) env

palkups :: PAEnv -> Identifier -> Either String PMEnv
palkups env x = maybe err Right $ Map.lookup x env
  where err = Left $ "Unbound annotation in lineage environment: " ++ x

paexts :: PAEnv -> Identifier -> PMEnv -> PAEnv
paexts env x ap' = Map.insertWith Map.union x ap' env

padel :: PAEnv -> Identifier -> Identifier -> PAEnv
padel env x y = (\f -> Map.alter f x env) $ \case
  Nothing -> Nothing
  Just aenv -> let naenv = Map.delete y aenv
               in if Map.null naenv then Nothing else Just naenv

{- PLCEnv helpers -}
plcenv0 :: PLCEnv
plcenv0 = IntMap.empty

plclkup :: PLCEnv -> Int -> Either String [Identifier]
plclkup env x = maybe err Right $ IntMap.lookup x env
  where err = Left $ "Unbound variable in lineage environment: " ++ show x

plcext :: PLCEnv -> Int -> [Identifier] -> PLCEnv
plcext env x p = IntMap.insert x p env

plcdel :: PLCEnv -> Int -> PLCEnv
plcdel env x = IntMap.delete x env

plcunions :: [PLCEnv] -> PLCEnv
plcunions = IntMap.unions

{- PIEnv helpers -}
pienv0 :: PLCEnv -> PIEnv
pienv0 lcenv = PIEnv 0 ppenv0 penv0 paenv0 lcenv

-- | Modifiers.
mpiee :: (PEnv -> PEnv) -> PIEnv -> PIEnv
mpiee f env = env {penv = f $ penv env}

mpiep :: (PPEnv -> PPEnv) -> PIEnv -> PIEnv
mpiep f env = env {ppenv = f $ ppenv env}

pilkupe :: PIEnv -> Identifier -> Either String (K3 Provenance)
pilkupe env x = plkup (penv env) x

piexte :: PIEnv -> Identifier -> K3 Provenance -> PIEnv
piexte env x p = env {penv=pext (penv env) x p}

pidele :: PIEnv -> Identifier -> PIEnv
pidele env i = env {penv=pdel (penv env) i}

pilkupp :: PIEnv -> Int -> Either String (K3 Provenance)
pilkupp env x = pplkup (ppenv env) x

piextp :: PIEnv -> Int -> K3 Provenance -> PIEnv
piextp env x p = env {ppenv=ppext (ppenv env) x p}

pidelp :: PIEnv -> Int -> PIEnv
pidelp env i = env {ppenv=ppdel (ppenv env) i}

pilkupas :: PIEnv -> Identifier -> Either String PMEnv
pilkupas env x = palkups (paenv env) x

piextas :: PIEnv -> Identifier -> PMEnv -> PIEnv
piextas env x p = env {paenv=paexts (paenv env) x p}


{- Fresh pointer and binding construction. -}
pifreshfp :: PIEnv -> (K3 Provenance, PIEnv)
pifreshfp pienv = 
  let i = pcnt pienv
      p = pbvar i
  in (p, piextp (pienv {pcnt=i+1}) i p)

pifreshbp :: PIEnv -> K3 Provenance -> (K3 Provenance, PIEnv)
pifreshbp pienv p = 
  let i  = pcnt pienv
      p' = pbvar i
  in (p', piextp (pienv {pcnt=i+1}) i p)

-- | Self-referential provenance pointer construction
--   This adds a new named pointer to both the named and pointer environments.
pifreshs :: PIEnv -> Identifier -> (K3 Provenance, PIEnv)
pifreshs pienv n = 
  let (p, nenv) = pifreshfp pienv in (p, piexte nenv n p)

-- | Provenance linked pointer construction.
pifresh :: PIEnv -> Identifier -> K3 Provenance -> (K3 Provenance, PIEnv)
pifresh pienv n p = 
  let (p', nenv) = pifreshbp pienv p in (p', piexte nenv n p')

pifreshAs :: PIEnv -> Identifier -> [(Identifier, Bool)] -> (PMEnv, PIEnv)
pifreshAs pienv n memN = 
  let (memP, npienv) = foldl (\(lacc,eacc) (_,l) -> first ((:lacc) . (,l)) $ pifreshfp eacc) ([], pienv) memN
      memNP = Map.fromList $ zip (map fst memN) memP
  in (memNP, piextas npienv n memNP)

{- Provenance pointer helpers -}

-- | Retrieves the provenance value referenced by a named pointer
piload :: PIEnv -> Identifier -> Either String (K3 Provenance)
piload pienv n = do
  p <- pilkupe pienv n
  case tag p of
    PBVar i -> pilkupp pienv i
    _ -> Left $ boxToString $ ["Invalid load on pointer"] %$ prettyLines p

-- | Sets the provenance value referenced by a named pointer
pistore :: PIEnv -> Identifier -> K3 Provenance -> Either String PIEnv
pistore pienv n p = do
  p' <- pilkupe pienv n
  case tag p' of
    PBVar i -> return $ piextp pienv i p
    _ -> Left $ boxToString $ ["Invalid store on pointer"] %$ prettyLines p'

-- | Traverses all pointers until reaching a non-pointer. 
--   This function stops on any cycles detected.
pichase :: PIEnv -> Identifier -> Either String (K3 Provenance)
pichase pienv n = pilkupe pienv n >>= aux [] 
  where aux path p@(tag -> PBVar i) | i `elem` path = return p
                                    | otherwise = pilkupp pienv i >>= aux (i:path)
        aux _ p = return p

pistorea :: PIEnv -> Identifier -> [(Identifier, K3 Provenance, Bool)] -> Either String PIEnv
pistorea pienv n memP = do
  pmenv <- pilkupas pienv n
  foldM (storemem pmenv) pienv memP

  where storemem pmenv eacc (i,p,_) = maybe (invalidMem i) (\(p',_) -> store eacc p' p) $ Map.lookup i pmenv
        store eacc (tag -> PBVar i) p = return $ piextp eacc i p
        store _ p _ = invalidStore p
        
        invalidMem   i = Left $ "Invalid store on annotation member" ++ i
        invalidStore p = Left $ boxToString $ ["Invalid store on pointer"] %$ prettyLines p


{- PInfM helpers -}

runPInfM :: PIEnv -> PInfM a -> (Either String a, PIEnv)
runPInfM env m = flip runState env $ runEitherT m

liftEitherM :: Either String a -> PInfM a
liftEitherM = either left return

pifreshsM :: Identifier -> PInfM (K3 Provenance)
pifreshsM n = get >>= return . flip pifreshs n >>= \(p,env) -> put env >> return p

pifreshM :: Identifier -> K3 Provenance -> PInfM (K3 Provenance)
pifreshM n p = get >>= return . (\env -> pifresh env n p) >>= \(p',env) -> put env >> return p'

pifreshAsM :: Identifier -> [(Identifier, Bool)] -> PInfM PMEnv
pifreshAsM n mems = get >>= return . (\env -> pifreshAs env n mems) >>= \(p,env) -> put env >> return p

piLookupEM :: Identifier -> PInfM (K3 Provenance)
piLookupEM n = get >>= liftEitherM . flip pilkupe n

piLoadEM :: Identifier -> PInfM (K3 Provenance)
piLoadEM n = get >>= liftEitherM . flip piload n

piStoreEM :: Identifier -> K3 Provenance -> PInfM ()
piStoreEM n p = get >>= liftEitherM . (\env -> pistore env n p) >>= put

piStoreAM :: Identifier -> [(Identifier, K3 Provenance, Bool)] -> PInfM ()
piStoreAM n memP = get >>= liftEitherM . (\env -> pistorea env n memP) >>= put

-- | Compute the lambda closure environment by tracking free variables at lambdas.
--   This is a one-pass bottom-up implementation.
extractLambdaClosures :: K3 Declaration -> Either String PLCEnv
extractLambdaClosures p = foldExpression lambdaClosure plcenv0 p >>= return . fst
  where
    lambdaClosure :: PLCEnv -> K3 Expression -> Either String (PLCEnv, K3 Expression) 
    lambdaClosure lcAcc expr = do
      (lcenv,_) <- foldMapTree extract (plcenv0, []) expr
      return $ (plcunions [lcAcc, lcenv], expr)

    extract :: [(PLCEnv, [Identifier])] -> K3 Expression -> Either String (PLCEnv, [Identifier])
    extract chAcc (tag -> EVariable i) = rt chAcc (++[i])
    extract chAcc (tag -> EAssign i)   = rt chAcc (++[i])
    extract (concatLc -> (lcAcc,chAcc)) e@(tag -> ELambda n) = extendLc lcAcc e $ filter (/= n) $ concat chAcc
    extract (concatLc -> (lcAcc,chAcc))   (tag -> EBindAs b) = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extract (concatLc -> (lcAcc,chAcc))   (tag -> ELetIn i)  = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extract (concatLc -> (lcAcc,chAcc))   (tag -> ECaseOf i) = return . (lcAcc,) $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extract chAcc _ = rt chAcc id

    concatLc :: [(PLCEnv, [Identifier])] -> (PLCEnv, [[Identifier]])
    concatLc subAcc = let (x,y) = unzip subAcc in (plcunions x, y)

    extendLc :: PLCEnv -> K3 Expression -> [Identifier] -> Either String (PLCEnv, [Identifier])
    extendLc lcenv e ids = case e @~ isEUID of
      Just (EUID (UID i)) -> return $ (plcext lcenv i ids, ids)
      _ -> Left $ boxToString $ ["No UID found on lambda"] %$ prettyLines e

    rt subAcc f = return $ second (f . concat) $ concatLc subAcc

inferProgramProvenance :: K3 Declaration -> Either String (K3 Declaration)
inferProgramProvenance p = do
  lcenv <- extractLambdaClosures p
  (_, initEnv) <- let (a,b) = runPInfM (pienv0 lcenv) (globalsProv p)
                  in a >>= return . (, b)
  _ <- fst $ runPInfM initEnv $ mapExpression (\e -> inferProvenance e >> return e) p
  return p

  where
    -- TODO: handle provenance annotations from the parser.
    globalsProv :: K3 Declaration -> PInfM (K3 Declaration)
    globalsProv d = do
      void $ mapProgram rcrDeclProv return return Nothing d
      mapProgram declProv return return Nothing d

    -- Add "blind" provenance pointers to every global for recursive calls.
    rcrDeclProv d@(tag -> DGlobal  n _ _) = pifreshsM n >> return d
    rcrDeclProv d@(tag -> DTrigger n _ _) = pifreshsM n >> return d
    rcrDeclProv d@(tag -> DDataAnnotation n _ mems) = mapM freshMems mems >>= pifreshAsM n . catMaybes >> return d
    rcrDeclProv d = return d

    freshMems (Lifted      Provides mn _ _ _) = return $ Just (mn, True)
    freshMems (Attribute   Provides mn _ _ _) = return $ Just (mn, False)
    freshMems _ = return Nothing

    -- Infer, replace pid->initializer
    declProv :: K3 Declaration -> PInfM (K3 Declaration)
    declProv d@(tag -> DGlobal n t eOpt) = do
      pOpt <- case eOpt of
                Just e  -> inferProvenance e
                Nothing -> provOfType [] t
      void $ piStoreEM n $ pglobal n pOpt
      return d

    declProv d@(tag -> DTrigger n _ e) = inferProvenance e >>= piStoreEM n . pglobal n >> return d
    declProv d@(tag -> DDataAnnotation n _ mems) = mapM inferMems mems >>= piStoreAM n . catMaybes >> return d
    declProv d = return d

    inferMems (Lifted      Provides mn mt meOpt _) = inferMember True  mn mt meOpt
    inferMems (Attribute   Provides mn mt meOpt _) = inferMember False mn mt meOpt
    inferMems _ = return Nothing

    inferMember lifted mn mt meOpt = do
      pOpt <- maybe (provOfType [] mt) inferProvenance meOpt
      return $ maybe Nothing (Just . (mn,,lifted)) pOpt

inferProvenance :: K3 Expression -> PInfM (Maybe (K3 Provenance))
inferProvenance expr = undefined

provOfType :: [Identifier] -> K3 Type -> PInfM (Maybe (K3 Provenance))
provOfType args t | isTFunction t =
   case tnc t of
    (TForall _, [ch])      -> provOfType args ch
    (TFunction, [_, retT]) -> let a = mkArg (length args + 1)
                              in provOfType (args++[a]) retT >>= return . Just . plambda a
    _ -> left $ boxToString $ ["Invalid function type"] %+ prettyLines t
  where mkArg i = "__arg" ++ show i

provOfType [] _   = return Nothing
provOfType args _ = return $ Just $ pderived $ map pfvar args
