{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Effect analysis for K3 programs.
module Language.K3.Analysis.SEffects.Inference where

import Control.Applicative
import Control.Arrow hiding ( left )
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.List
import Data.Maybe
import Data.Tree

import Data.Map    ( Map    )
import Data.IntMap ( IntMap )
import qualified Data.Map    as Map
import qualified Data.IntMap as IntMap

import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Constructors
import Language.K3.Analysis.Provenance.Inference()

import Language.K3.Analysis.SEffects.Core
import Language.K3.Analysis.SEffects.Constructors

import Data.Text ( Text )
import Language.K3.Utils.PrettyText ( Pretty, (%$), (%+) )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

effTraceLogging :: Bool
effTraceLogging = False

localLog :: (Functor m, Monad m) => String -> m ()
localLog = logVoid effTraceLogging

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
localLogAction = logAction effTraceLogging

-- | An effect bindings environment, using an option type to represent 
--   bindings that do not have an effect.
type FEnv = Map Identifier [K3 Effect]

-- | An effect "pointer" environment for bound effect variables.
type FPEnv = IntMap (K3 Effect)

-- | A provenance bindings environment, built from existing provenance annotations.
type FPBEnv = Map Identifier [K3 Provenance]

-- | A provenance "pointer" environment
type FPPEnv = IntMap (K3 Provenance)

-- | A effect bindings environment for annotations,
--   indexed by annotation and attribute name.
type FAEnv = Map Identifier FMEnv
type FMEnv = Map Identifier (K3 Effect, Bool)

-- | A lambda closure environment: ELambda UID => identifiers of closure variables.
type FLCEnv = IntMap [Identifier]

-- | A mapping from expression ids to a pair of effect and effect structure.
type EFMap = IntMap (K3 Effect, K3 Effect)

-- | An effect inference environment.
data FIEnv = FIEnv {
               fcnt    :: Int,
               fpenv   :: FPEnv,
               fenv    :: FEnv,
               faenv   :: FAEnv,
               fpbenv  :: FPBEnv,
               fppenv  :: FPPEnv,
               flcenv  :: FLCEnv,
               efmap   :: EFMap,
               fcase   :: [(FMatVar, PMatVar)]  -- Temporary storage stack for case variables.
            }

-- | The effects inference monad
type FInfM = EitherT Text (State FIEnv)

{- Data.Text helpers -}
mkErr :: String -> Either Text a
mkErr msg = Left $ T.pack msg

mkErrP :: PT.Pretty a => String -> a -> Either Text b
mkErrP msg a = Left $ T.unlines [T.pack msg, PT.pretty a]

{- FEnv helpers -}
fenv0 :: FEnv
fenv0 = Map.empty

flkup :: FEnv -> Identifier -> Either Text (K3 Effect)
flkup env x = maybe err safeHead $ Map.lookup x env
  where 
    safeHead l = if null l then err else Right $ head l
    err = mkErrP msg env
    msg = "Unbound variable in effect binding environment: " ++ x

fext :: FEnv -> Identifier -> K3 Effect -> FEnv
fext env x p = Map.insertWith (++) x [p] env

fdel :: FEnv -> Identifier -> FEnv
fdel env x = Map.update safeTail x env
  where safeTail []  = Nothing
        safeTail [_] = Nothing
        safeTail l   = Just $ tail l


{- FPEnv helpers -}
fpenv0 :: FPEnv
fpenv0 = IntMap.empty

fplkup :: FPEnv -> Int -> Either Text (K3 Effect)
fplkup env x = maybe err Right $ IntMap.lookup x env
  where err = mkErrP msg env
        msg = "Unbound variable in effect pointer environment: " ++ show x

fpext :: FPEnv -> Int -> K3 Effect -> FPEnv
fpext env x p = IntMap.insert x p env

fpdel :: FPEnv -> Int -> FPEnv
fpdel env x = IntMap.delete x env


{- FAEnv helpers -}
faenv0 :: FAEnv
faenv0 = Map.empty

fmenv0 :: FMEnv
fmenv0 = Map.empty

falkup :: FAEnv -> Identifier -> Identifier -> Either Text (K3 Effect)
falkup env x y = maybe err (maybe err (Right . fst) . Map.lookup y) $ Map.lookup x env
  where err = mkErr $ "Unbound annotation member in effect environment: " ++ x

faext :: FAEnv -> Identifier -> Identifier -> K3 Effect -> Bool -> FAEnv
faext env x y fOpt l = Map.insertWith Map.union x (Map.fromList [(y,(fOpt,l))]) env

falkups :: FAEnv -> Identifier -> Either Text FMEnv
falkups env x = maybe err Right $ Map.lookup x env
  where err = mkErr $ "Unbound annotation in effect environment: " ++ x

faexts :: FAEnv -> Identifier -> FMEnv -> FAEnv
faexts env x ap' = Map.insertWith Map.union x ap' env


{- EFMap helpers -}
efmap0 :: EFMap
efmap0 = IntMap.empty

eflkup :: EFMap -> K3 Expression -> Either Text (K3 Effect, K3 Effect)
eflkup efm e@((@~ isEUID) -> Just (EUID (UID uid))) = maybe lookupErr Right $ IntMap.lookup uid efm
  where lookupErr = Left $ PT.boxToString $ msg %$ PT.prettyLines e
        msg = [T.unwords $ map T.pack ["No effects found for", show uid]]

eflkup _ e = Left $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e

efext :: EFMap -> K3 Expression -> K3 Effect -> K3 Effect -> Either Text EFMap
efext efm e f s = case e @~ isEUID of
  Just (EUID (UID i)) -> Right $ IntMap.insert i (f,s) efm
  _ -> Left $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e


{- FPBEnv helpers -}
fpbenv0 :: FPBEnv
fpbenv0 = Map.empty

fpblkup :: FPBEnv -> Identifier -> Either Text (K3 Provenance)
fpblkup env x = maybe err safeHead $ Map.lookup x env
  where 
    safeHead l = if null l then err else Right $ head l
    err = mkErrP msg env
    msg = "Unbound variable in effect provenance binding environment: " ++ x

fpbext :: FPBEnv -> Identifier -> K3 Provenance -> FPBEnv
fpbext env x p = Map.insertWith (++) x [p] env

fpbdel :: FPBEnv -> Identifier -> FPBEnv
fpbdel env x = Map.update safeTail x env
  where safeTail []  = Nothing
        safeTail [_] = Nothing
        safeTail l   = Just $ tail l


{- FPPEnv helpers -}
fppenv0 :: FPPEnv
fppenv0 = IntMap.empty

fpplkup :: FPPEnv -> Int -> Either Text (K3 Provenance)
fpplkup env x = maybe err Right $ IntMap.lookup x env
  where err = mkErrP msg env
        msg = "Unbound pointer in lineage environment: " ++ show x


{- FLCEnv helpers -}
flcenv0 :: FLCEnv
flcenv0 = IntMap.empty

flclkup :: FLCEnv -> Int -> Either Text [Identifier]
flclkup env x = maybe err Right $ IntMap.lookup x env
  where err = mkErr $ "Unbound UID in closure environment: " ++ show x


{- FIEnv helpers -}
fienv0 :: FPPEnv -> FLCEnv -> FIEnv
fienv0 ppenv lcenv = FIEnv 0 fpenv0 fenv0 faenv0 fpbenv0 ppenv lcenv efmap0 []

-- | Modifiers.
mfiee :: (FEnv -> FEnv) -> FIEnv -> FIEnv
mfiee f env = env {fenv = f $ fenv env}

mfiep :: (FPEnv -> FPEnv) -> FIEnv -> FIEnv
mfiep f env = env {fpenv = f $ fpenv env}

filkupe :: FIEnv -> Identifier -> Either Text (K3 Effect)
filkupe env x = flkup (fenv env) x

fiexte :: FIEnv -> Identifier -> K3 Effect -> FIEnv
fiexte env x f = env {fenv=fext (fenv env) x f}

fidele :: FIEnv -> Identifier -> FIEnv
fidele env i = env {fenv=fdel (fenv env) i}

filkupep :: FIEnv -> Identifier -> Either Text (K3 Provenance)
filkupep env x = fpblkup (fpbenv env) x

fiextep :: FIEnv -> Identifier -> K3 Provenance -> FIEnv
fiextep env x f = env {fpbenv=fpbext (fpbenv env) x f}

fidelep :: FIEnv -> Identifier -> FIEnv
fidelep env i = env {fpbenv=fpbdel (fpbenv env) i}

filkupp :: FIEnv -> Int -> Either Text (K3 Effect)
filkupp env x = fplkup (fpenv env) x

fiextp :: FIEnv -> Int -> K3 Effect -> FIEnv
fiextp env x f = env {fpenv=fpext (fpenv env) x f}

fidelp :: FIEnv -> Int -> FIEnv
fidelp env i = env {fpenv=fpdel (fpenv env) i}

filkupa :: FIEnv -> Identifier -> Identifier -> Either Text (K3 Effect)
filkupa env x y = falkup (faenv env) x y

filkupas :: FIEnv -> Identifier -> Either Text FMEnv
filkupas env x = falkups (faenv env) x

fiextas :: FIEnv -> Identifier -> FMEnv -> FIEnv
fiextas env x f = env {faenv=faexts (faenv env) x f}

filkupm :: FIEnv -> K3 Expression -> Either Text (K3 Effect, K3 Effect)
filkupm env e = eflkup (efmap env) e

fiextm :: FIEnv -> K3 Expression -> K3 Effect -> K3 Effect -> Either Text FIEnv
fiextm env e f s = let efmapE = efext (efmap env) e f s
                   in either Left (\nep -> Right $ env {efmap=nep}) efmapE

filkuppp :: FIEnv -> Int -> Either Text (K3 Provenance)
filkuppp env x = fpplkup (fppenv env) x

filkupc :: FIEnv -> Int -> Either Text [Identifier]
filkupc env i = flclkup (flcenv env) i

fipopcase :: FIEnv -> Either Text ((FMatVar, PMatVar), FIEnv)
fipopcase env = case fcase env of
  [] -> Left $ T.pack "Uninitialized case matvar"
  h:t -> return (h, env {fcase=t})

fipushcase :: FIEnv -> (FMatVar, PMatVar) -> FIEnv
fipushcase env mvs = env {fcase=mvs:(fcase env)}


{- Fresh pointer and binding construction. -}

-- | Self-referential provenance pointer construction
fifreshfp :: FIEnv -> Identifier -> UID -> (K3 Effect, FIEnv)
fifreshfp fienv i u = 
  let j = fcnt fienv
      f = fbvar $ FMatVar i u j
  in (f, fiextp (fienv {fcnt=j+1}) j f)

fifreshbp :: FIEnv -> Identifier -> UID -> K3 Effect -> (K3 Effect, FIEnv)
fifreshbp fienv i u f = 
  let j  = fcnt fienv
      f' = fbvar $ FMatVar i u j
  in (f', fiextp (fienv {fcnt=j+1}) j f)

-- | Self-referential provenance pointer construction
--   This adds a new named pointer to both the named and pointer environments.
fifreshs :: FIEnv -> Identifier -> UID -> (K3 Effect, FIEnv)
fifreshs fienv n u = 
  let (f, nenv) = fifreshfp fienv n u in (f, fiexte nenv n f)

-- | Provenance linked pointer construction.
fifresh :: FIEnv -> Identifier -> UID -> K3 Effect -> (K3 Effect, FIEnv)
fifresh fienv n u f = 
  let (f', nenv) = fifreshbp fienv n u f in (f', fiexte nenv n f')

fifreshAs :: FIEnv -> Identifier -> [(Identifier, UID, Bool, Bool)] -> (FMEnv, FIEnv)
fifreshAs fienv n memN = 
  let mkMemF lacc l f                = lacc++[(f,l)]
      extMemF (lacc,eacc) (i,u,l,fn) = if fn then first (mkMemF lacc l) $ fifreshfp eacc i u
                                             else (lacc++[(fnone,l)], eacc)
      (memF, nfienv)                 = foldl extMemF ([], fienv) memN
      memNF                          = Map.fromList $ zip (map (\(a,_,_,_) -> a) memN) memF
  in (memNF, fiextas nfienv n memNF)


{- Effect pointer helpers -}

-- | Retrieves the provenance value referenced by a named pointer
fiload :: FIEnv -> Identifier -> Either Text (K3 Effect)
fiload fienv n = do
  f <- filkupe fienv n
  case tag f of
    FBVar mv -> filkupp fienv $ fmvptr mv
    _ -> Left $ PT.boxToString $ [T.pack "Invalid load on pointer"] %$ PT.prettyLines f

-- | Sets the provenance value referenced by a named pointer
fistore :: FIEnv -> Identifier -> UID -> K3 Effect -> Either Text FIEnv
fistore fienv n u f = do
  f' <- filkupe fienv n
  case tag f' of
    FBVar mv | (fmvn mv, fmvloc mv) == (n,u) -> return $ fiextp fienv (fmvptr mv) f
    _ -> Left $ PT.boxToString $ [T.pack "Invalid store on pointer"] %$ PT.prettyLines f'

fistorea :: FIEnv -> Identifier -> [(Identifier, UID, K3 Effect, Bool)] -> Either Text FIEnv
fistorea fienv n memF = do
  fmenv <- filkupas fienv n
  foldM (storemem fmenv) fienv memF

  where storemem fmenv eacc (i,u,f,_) = maybe (invalidMem i) (\(f',_) -> store eacc i u f' f) $ Map.lookup i fmenv
        store eacc i u (tag -> FBVar mv) f
          | (fmvn mv, fmvloc mv) == (i,u) = return $ fiextp eacc (fmvptr mv) f
        store eacc _ _ _ _ = return eacc
        
        invalidMem i = mkErr $ "Invalid store on annotation member" ++ i

-- | Traverses all pointers until reaching a non-pointer. 
--   This function stops on any cycles detected.
fichase :: FIEnv -> K3 Effect -> Either Text (K3 Effect)
fichase fienv cf = aux [] cf
  where aux path f@(tag -> FBVar (fmvptr -> i)) | i `elem` path = return f
                                                | otherwise = filkupp fienv i >>= aux (i:path)
        aux _ f = return f

-- Capture-avoiding substitution of any free variable with the given identifier.
-- Provenance variable substitution is left to another routine via fmapProv.
fisub :: FIEnv -> Identifier -> K3 Effect -> K3 Effect -> Either Text (K3 Effect, FIEnv)
fisub fienv i df sf = acyclicSub fienv emptyPtrSubs [] sf >>= \(renv, _, rf) -> return (rf, renv)
  where
    acyclicSub env subs _ (tag -> FFVar j) | i == j = return (env, subs, df)

    acyclicSub env subs path f@(tag -> FBVar mv@(fmvptr -> j))
      | j `elem` path = return (env, subs, f)
      | isPtrSub subs j = getPtrSub subs j >>= return . (env, subs,)
      | otherwise     = filkupp fienv j >>= subPtrIfDifferent env subs path mv

    -- TODO: we can short-circuit descending into the body if we
    -- are willing to stash the expression uid in a PLambda, using
    -- this uid to lookup i's presence in our precomputed closures.
    acyclicSub env subs _ f@(tag -> FLambda j) | i == j = return (env, subs, f)

    -- Avoid descent into materialization points of shadowed variables.
    acyclicSub env subs _ f@(tag -> FScope mvs) | i `elem` map fmvn mvs = return (env, subs, f)

    -- Handle higher-order function application (two-place FApply nodes) where a FFVar
    -- in the lambda position is replaced by a FLambda
    acyclicSub env subs path f@(tnc -> (FApply Nothing, [lrf@(tag -> FFVar _), argrf])) = do
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) [lrf, argrf]
      case nch of
        [lrf',argrf'] -> simplifyApply nenv Nothing [] lrf' argrf'
                           >>= \(f',nenv') -> return (nenv', nsubs, f')
        _ -> appSubErr f

    acyclicSub env subs path n@(Node _ ch) = do
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) ch
      return (nenv, nsubs, replaceCh n nch)

    rcr path (eacc,sacc,chacc) c = acyclicSub eacc sacc path c >>= \(e,s,nc) -> return (e,s,chacc++[nc])

    subPtrIfDifferent env subs path mv f = do
      (nenv, nsubs, nf) <- acyclicSub env subs path f
      if f == nf then return (nenv, nsubs, fbvar mv) else addPtrSub nenv nsubs mv nf

    -- Pointer substitutions, as a map of old PPtr => new PMatVar
    emptyPtrSubs   = IntMap.empty
    isPtrSub  fs j = IntMap.member j fs
    getPtrSub fs j = maybe (lookupErr j) (return . fbvar) $ IntMap.lookup j fs
    addPtrSub env fs mv f =
      let (nf, nenv) = fifresh env (fmvn mv) (fmvloc mv) f in
      case tag nf of
        FBVar nmv -> return (nenv, IntMap.insert (fmvptr mv) nmv fs, nf)
        _ -> freshErr nf

    lookupErr j = Left $ T.pack $ "Could not find pointer substitution for " ++ show j
    freshErr  f = Left $ PT.boxToString $ [T.pack "Invalid fresh FBVar result "] %$ PT.prettyLines f
    appSubErr f = Left $ PT.boxToString $ [T.pack "Invalid apply substitution at: "] %$ PT.prettyLines f


-- Apply a function to every provenance node in an effect tree.
fmapProv :: (K3 Provenance -> Either Text (K3 Provenance)) -> K3 Effect -> Either Text (K3 Effect)
fmapProv provF f = modifyTree onProv f
  where onProv (tag -> FRead p)  = provF p >>= return . fread
        onProv (tag -> FWrite p) = provF p >>= return . fwrite
        onProv f' = return f'


{- Apply simplification -}
chaseAppArg :: K3 Effect -> Either Text (K3 Effect)
chaseAppArg (tnc -> (FApply _, [_,_,_,_,sf])) = chaseAppArg sf
chaseAppArg sf = return sf

chaseLambda :: FIEnv -> Maybe (K3 Expression) -> [FPtr] -> K3 Effect -> Either Text [K3 Effect]
chaseLambda _ _ _ f@(tag -> FLambda _) = return [f]
chaseLambda _ _ _ f@(tag -> FFVar _)   = return [f]

chaseLambda env e path f@(tag -> FBVar (fmvptr -> i))
  | i `elem` path = return [f]
  | otherwise     = fichase env f >>= chaseLambda env e (i:path)

chaseLambda env e path (tnc -> (FApply _, [_,_,_,_,sf])) = chaseLambda env e path sf
chaseLambda env e path (tnc -> (FSet, rfl)) = mapM (chaseLambda env e path) rfl >>= return . concat
chaseLambda _ e _ f = Left $ PT.boxToString $ fErr f %$ exprErr e
  where fErr f' = [T.pack "Invalid application or lambda: "] %$ PT.prettyLines f'
        exprErr eOpt = maybe [] (\e' -> [T.pack "on"] %$ PT.prettyLines e') eOpt

simplifyApply :: FIEnv -> Maybe (K3 Expression) -> [Maybe (K3 Effect)] -> K3 Effect -> K3 Effect
              -> Either Text (K3 Effect, FIEnv)
simplifyApply fienv eOpt ef lrf arf = do
  upOpt    <- uidP eOpt
  arf'     <- chaseAppArg arf
  manyLrf  <- chaseLambda fienv eOpt [] lrf
  (manyLrf', nenv) <- foldM (doSimplify upOpt arf') ([], fienv) manyLrf

  case manyLrf' of
    []   -> applyLambdaErr lrf
    [rf] -> return (rf, nenv)
    _    -> return (fset manyLrf', nenv)

  where 
    doSimplify upOpt arf' (facc, eacc) lrf' =
      case tnc lrf' of
        (FLambda i, [_,bef,brf]) -> case upOpt of
          Just (uid, p) -> do
            let (ifbv, neacc) = fifreshbp eacc i uid arf'
            imv  <- fmv ifbv
            (nbef,n2eacc) <- fisub neacc  i ifbv bef >>= subProv i p
            (nbrf,n3eacc) <- fisub n2eacc i ifbv brf >>= subProv i p
            let apprf = fapply (Just imv) lrf arf (fromJust $ finit ef) (fromJust $ finit [Just nbef]) nbrf
            return (facc++[apprf], n3eacc)

          Nothing -> do
            -- This case handles application of external lambdas as direct inlining
            -- of the argument effect structure without lifting.
            -- Also, we replace any provenance symbols for the argument with temporaries
            -- since we have no further information from effect signatures.
            (nbef,neacc)  <- fisub eacc  i arf' bef >>= subProv i ptemp
            (nbrf,n2eacc) <- fisub neacc i arf' brf >>= subProv i ptemp
            let apprf = fapply Nothing lrf arf (fromJust $ finit ef) (fromJust $ finit [Just nbef]) nbrf
            return (facc++[apprf], n2eacc)

        -- Handle recursive functions and forward declarations by using an opaque return value.
        (FBVar _, _) -> return (facc ++ [fapply Nothing lrf arf (fromJust $ finit ef) fnone fnone], eacc)

        -- Handle higher-order and external functions as an unsimplified apply.
        (FFVar _, _) -> return (facc ++ [fapplyExt lrf arf], eacc)

        _ -> applyLambdaErr lrf

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = Left $ PT.boxToString $ [T.pack "No uid found on "] %+ PT.prettyLines e

    provOf  e = maybe (provErr e) (\case {(EProvenance p) ->  return p; _ -> provErr e}) $ e @~ isEProvenance
    provErr e = Left $ PT.boxToString $ [T.pack "No provenance found on "] %+ PT.prettyLines e

    uidP eOpt' = case eOpt' of
      Nothing -> return Nothing
      Just e  -> (\a b -> Just (a,b)) <$> uidOf e <*> argP e

    argP e = provOf e >>= \case
                            (tag -> PApply (Just mv))  -> return $ pbvar mv
                            (tag -> PMaterialize [mv]) -> return $ pbvar mv
                            p -> return p

    finit ef' = Just $ fseq $ catMaybes ef'

    subProv i p (f, env) = fmapProv (subpfvar i p) f >>= \f' -> return (f', env)

    subpfvar i p (tag -> PFVar j) | i == j = return p
    subpfvar _ _ p = return p

    fmv (tag -> FBVar mv) = return mv
    fmv f = Left $ PT.boxToString $ [T.pack "Invalid effect bound var: "] %$ PT.prettyLines f

    exprErr = maybe [] (\e -> PT.prettyLines e) eOpt
    applyLambdaErr f = Left $ PT.boxToString $ [T.pack "Invalid apply lambda effect: "]
                            %$ exprErr %$ [T.pack "Effect:"] %$ PT.prettyLines f

simplifyApplyM :: Maybe (K3 Expression) -> [Maybe (K3 Effect)] -> K3 Effect -> K3 Effect -> FInfM (K3 Effect)
simplifyApplyM eOpt ef lrf argrf = do
  env <- get
  (f, nenv) <- liftEitherM $ simplifyApply env eOpt ef lrf argrf
  void $ put nenv
  return f


{- FInfM helpers -}

runFInfM :: FIEnv -> FInfM a -> (Either Text a, FIEnv)
runFInfM env m = flip runState env $ runEitherT m

reasonM :: (Text -> Text) -> FInfM a -> FInfM a
reasonM errf = mapEitherT $ \m -> m >>= \case
  Left  err -> get >>= \env -> (return . Left $ errf $ T.unlines [err, T.pack "Effect environment:", PT.pretty env])
  Right r   -> return $ Right r

errorM :: Text -> FInfM a
errorM msg = reasonM id $ left msg

liftEitherM :: Either Text a -> FInfM a
liftEitherM = either left return

fifreshbpM :: Identifier -> UID -> K3 Effect -> FInfM (K3 Effect)
fifreshbpM n u f = get >>= return . (\env -> fifreshbp env n u f) >>= \(f',nenv) -> put nenv >> return f'

fifreshsM :: Identifier -> UID -> FInfM (K3 Effect)
fifreshsM n u = get >>= return . (\env -> fifreshs env n u) >>= \(f,env) -> put env >> return f

fifreshM :: Identifier -> UID -> K3 Effect -> FInfM (K3 Effect)
fifreshM n u f = get >>= return . (\env -> fifresh env n u f) >>= \(f',env) -> put env >> return f'

fifreshAsM :: Identifier -> [(Identifier, UID, Bool, Bool)] -> FInfM FMEnv
fifreshAsM n mems = get >>= return . (\env -> fifreshAs env n mems) >>= \(f,env) -> put env >> return f

filkupeM :: Identifier -> FInfM (K3 Effect)
filkupeM n = get >>= liftEitherM . flip filkupe n

fiexteM :: Identifier -> K3 Effect -> FInfM ()
fiexteM n f = get >>= \env -> return (fiexte env n f) >>= put

fideleM :: Identifier -> FInfM ()
fideleM n = get >>= \env -> return (fidele env n) >>= put

filkupepM :: Identifier -> FInfM (K3 Provenance)
filkupepM n = get >>= liftEitherM . flip filkupep n

fiextepM :: Identifier -> K3 Provenance -> FInfM ()
fiextepM n f = get >>= \env -> return (fiextep env n f) >>= put

fidelepM :: Identifier -> FInfM ()
fidelepM n = get >>= \env -> return (fidelep env n) >>= put

filkupaM :: Identifier -> Identifier -> FInfM (K3 Effect)
filkupaM n m = get >>= liftEitherM . (\env -> filkupa env n m)

filkupasM :: Identifier -> FInfM FMEnv
filkupasM n = get >>= liftEitherM . flip filkupas n

filkupmM :: K3 Expression -> FInfM (K3 Effect, K3 Effect)
filkupmM e = get >>= liftEitherM . flip filkupm e

fiextmM :: K3 Expression -> K3 Effect -> K3 Effect -> FInfM ()
fiextmM e f s = get >>= liftEitherM . (\env -> fiextm env e f s) >>= put

filoadM :: Identifier -> FInfM (K3 Effect)
filoadM n = get >>= liftEitherM . flip fiload n

fistoreM :: Identifier -> UID -> K3 Effect -> FInfM ()
fistoreM n u f = get >>= liftEitherM . (\env -> fistore env n u f) >>= put

fistoreaM :: Identifier -> [(Identifier, UID, K3 Effect, Bool)] -> FInfM ()
fistoreaM n memF = get >>= liftEitherM . (\env -> fistorea env n memF) >>= put

fichaseM :: K3 Effect -> FInfM (K3 Effect)
fichaseM f = get >>= liftEitherM . flip fichase f

fisubM :: Identifier -> K3 Effect -> K3 Effect -> FInfM (K3 Effect)
fisubM i ref f = get >>= liftEitherM . (\env -> fisub env i ref f) >>= \(f', nenv) -> put nenv >> return f'

fmapProvM :: (K3 Provenance -> Either Text (K3 Provenance)) -> K3 Effect -> FInfM (K3 Effect)
fmapProvM provF f = liftEitherM $ fmapProv provF f

filkupppM :: Int -> FInfM (K3 Provenance)
filkupppM n = get >>= liftEitherM . flip filkuppp n

filkupcM :: Int -> FInfM [Identifier]
filkupcM n = get >>= liftEitherM . flip filkupc n

fipopcaseM :: () -> FInfM (FMatVar, PMatVar)
fipopcaseM _ = get >>= liftEitherM . fipopcase >>= \(cmvs,env) -> put env >> return cmvs

fipushcaseM :: (FMatVar, PMatVar) -> FInfM ()
fipushcaseM mv = get >>= return . flip fipushcase mv >>= put


{- Analysis entrypoint -}
inferProgramEffects :: FPPEnv -> K3 Declaration -> Either String (K3 Declaration)
inferProgramEffects ppenv p =  do
  lcenv <- lambdaClosures p
  liftEitherTM $ fst $ runFInfM (fienv0 ppenv lcenv) $ doInference p

  where
    liftEitherTM = either (Left . T.unpack) Right

    doInference p' = do
      np   <- globalsEff p'
      np'  <- mapExpression inferExprEffects np
      np'' <- simplifyExprEffects np'
      markGlobals np''

    -- Globals cannot be captured in closures, so we elide them from the
    -- effect provenance bindings environment.
    globalsEff :: K3 Declaration -> FInfM (K3 Declaration)
    globalsEff d = do
      nd <- mapProgram rcrDeclEff return return Nothing d
      mapProgram declEff return return Nothing nd

    -- Add self-referential effect pointers to every global for cyclic scope.
    rcrDeclEff d@(tag -> DGlobal  n _ _) = uidOf d >>= \u -> fifreshsM n u >> declProv d n >> return d
    rcrDeclEff d@(tag -> DTrigger n _ _) = uidOf d >>= \u -> fifreshsM n u >> declProv d n >> return d
    rcrDeclEff d@(tag -> DDataAnnotation n _ mems) = mapM freshMems mems >>= fifreshAsM n . catMaybes >> return d
    rcrDeclEff d = return d

    freshMems m@(Lifted      Provides mn mt _ mas) = memUID m mas >>= \u -> return (Just (mn, u, True,  isTFunction mt))
    freshMems m@(Attribute   Provides mn mt _ mas) = memUID m mas >>= \u -> return (Just (mn, u, False, isTFunction mt))
    freshMems _ = return Nothing

    declProv d n = void $ provOf d >>= fiextepM n

    -- Infer based on initializer, and update effect pointer.
    declEff :: K3 Declaration -> FInfM (K3 Declaration)
    declEff d@(tag -> DGlobal n t eOpt) = do
      u <- uidOf d
      f <- maybe (effectsOfType [] t) inferEffects eOpt
      void $ fistoreM n u f
      return d

    declEff d@(tag -> DTrigger n _ e) = do
      u <- uidOf d
      f <- inferEffects e
      void $ fistoreM n u f
      return d

    declEff d@(tag -> DDataAnnotation n _ mems) = mapM inferMems mems >>= fistoreaM n . catMaybes >> return d
    declEff d = return d

    inferMems m@(Lifted      Provides mn mt meOpt mas) = memUID m mas >>= \u -> inferMember True  mn mt meOpt u
    inferMems m@(Attribute   Provides mn mt meOpt mas) = memUID m mas >>= \u -> inferMember False mn mt meOpt u
    inferMems _ = return Nothing

    inferMember lifted mn mt meOpt u = do
      mfOpt <- maybe (effectsOfType [] mt) inferEffects meOpt
      return $ Just (mn, u, mfOpt, lifted)

    markGlobals = mapProgram markGlobal return return Nothing

    markGlobal d@(tag -> DGlobal  n _ eOpt) = unlessHasEffect d $ \_ -> do
      f' <- maybe (filkupeM n) effectOf eOpt
      return (d @+ (DEffect $ Right f'))

    markGlobal d@(tag -> DTrigger _ _ e) = unlessHasEffect d $ \_ -> do
      f' <- effectOf e
      return (d @+ (DEffect $ Right f'))
    
    markGlobal d@(tag -> DDataAnnotation n tvars mems) = do
      nmems <- mapM (markMems n) mems
      return (replaceTag d $ DDataAnnotation n tvars nmems)

    markGlobal d = return d

    markMems n m@(Lifted Provides mn mt meOpt mas) = unlessMemHasEffect m mas $ \_ -> do
      f' <- maybe (filkupaM n mn) effectOf meOpt
      return (Lifted Provides mn mt meOpt $ mas ++ [DEffect $ Right f'])

    markMems n m@(Attribute Provides mn mt meOpt mas) = unlessMemHasEffect m mas $ \_ -> do
      f' <- maybe (filkupaM n mn) effectOf meOpt
      return (Attribute Provides mn mt meOpt $ mas ++ [DEffect $ Right f'])

    markMems _ m = return m

    unlessHasEffect d f = case d @~ isDEffect of
      Nothing -> f ()
      _ -> return d

    unlessMemHasEffect mem as f = case find isDEffect as of
      Nothing -> f ()
      _ -> return mem

    effectOf    e = maybe (effectErr e) (effectOfA e) $ e @~ isESEffect
    effectOfA e a = case a of {(ESEffect f') -> return f'; _ -> effectErr e}
    effectErr   e = errorM $ PT.boxToString $ [T.pack "No effect found on "] %+ PT.prettyLines e

    uidOf  n = maybe (uidErr n) (\case {(DUID u) -> return u ; _ ->  uidErr n}) $ n @~ isDUID
    uidErr n = errorM $ PT.boxToString $ [T.pack "No uid found on "] %+ PT.prettyLines n

    provOf    d = maybe (provErr d) (provOfA d) $ d @~ isDProvenance
    provOfA d a = case a of {(DProvenance p') -> either return return p'; _ -> provErr d}
    provErr   d = errorM $ PT.boxToString $ [T.pack "No provenance found on "] %+ PT.prettyLines d

    memUID memDecl as = case filter isDUID as of
                          [DUID u] -> return u
                          _ -> memUIDErr memDecl
    memUIDErr memDecl = errorM $ T.append (T.pack "Invalid member UID") $ PT.pretty memDecl


inferExprEffects :: K3 Expression -> FInfM (K3 Expression)
inferExprEffects expr = inferEffects expr >> substituteEffects expr

inferEffects :: K3 Expression -> FInfM (K3 Effect)
inferEffects expr = foldMapIn1RebuildTree topdown sideways infer iu Nothing expr
                      >>= return . snd
  where
    iu = return ()
    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = errorM $ PT.boxToString $ [T.pack "No uid found on "] %+ PT.prettyLines e

    provOf  e = maybe (provErr e) (\case {(EProvenance p) ->  return p; _ -> provErr e}) $ e @~ isEProvenance
    provErr e = errorM $ PT.boxToString $ [T.pack "No provenance found on "] %+ PT.prettyLines e

    topdown m _ (tag -> ELambda i) = m >> fiexteM i (ffvar i) >> fiextepM i (pfvar i) >> return iu
    topdown m _ _ = m >> return iu

    srt = return . (iu,)

    -- Effect bindings reference lambda effects where necessary to ensure
    -- defferred effect inlining for function values.
    -- Bindings are introduced based on types.
    sideways :: FInfM () -> Maybe (K3 Effect) -> K3 Effect -> K3 Expression -> FInfM (FInfM (), [FInfM ()])
    sideways m _ rf e@(tag -> ELetIn  i) = m >> do
      case (head $ children e) @~ isEType of
        Just (EType t) -> uidOf e >>= \u -> srt [freshM False e i u t rf]
        _ -> tAnnErr e

    sideways m _ rf e@(tag -> ECaseOf i) = m >> do
      u <- uidOf e
      case (head $ children e) @~ isEType of
        Just (EType t) -> srt [freshOptM e i u t rf, setcaseM i >> popVars i]
        _ -> tAnnErr e

      where setcaseM j = ((,) <$> (filkupeM j >>= fmv) <*> (filkupepM j >>= pmv)) >>= fipushcaseM

    sideways m _ rf e@(tag -> EBindAs b) = m >> do
      u <- uidOf e
      case (head $ children e) @~ isEType of
        Just (EType t) ->
          case b of
            BIndirection i -> srt [freshIndM e i u t rf]
            BTuple is      -> srt . (:[]) $ freshTupM e u t rf $ zip [0..length is -1] is
            BRecord ivs    -> srt . (:[]) $ freshRecM e u t rf ivs

        _ -> tAnnErr e

    sideways m _ _ (children -> ch) = m >> (srt $ replicate (length ch - 1) iu)

    infer :: FInfM () -> [Maybe (K3 Effect)] -> [K3 Effect] -> K3 Expression -> FInfM (Maybe (K3 Effect), K3 Effect)
    infer m _ _ e@(tag -> EConstant _) = m >> rt e (Nothing, fnone)
    infer m _ _ e@(tag -> EVariable i) = m >> ((\f p -> (Just (fread p), f)) <$> filkupeM i <*> filkupepM i) >>= rt e
    
    infer m ef rf e@(tag -> ESome)       = m >> rt e (finit ef, fdata Nothing    rf)
    infer m ef rf e@(tag -> EIndirect)   = m >> rt e (finit ef, fdata Nothing    rf)
    infer m ef rf e@(tag -> ETuple)      = m >> rt e (finit ef, fdata Nothing    rf)
    infer m ef rf e@(tag -> ERecord ids) = m >> rt e (finit ef, fdata (Just ids) rf)
    
    infer m ef [rf] e@(tag -> ELambda i) = m >> do
      UID u <- uidOf e
      clv   <- filkupcM u
      clf   <- mapM filkupepM clv >>= mapM (return . fread)
      rt e (Just fnone, flambda i (fseq clf) (fseq $ catMaybes ef) rf)

    infer m ef [lrf,arf] e@(tag -> EOperate OApp) = m >> do
      appf <- simplifyApplyM (Just e) ef lrf arf
      rt e (Just fnone, appf)

    infer m ef [rf] e@(tnc -> (EProject i, [esrc])) = m >> do
      psrc <- provOf esrc
      case esrc @~ isEType of
        Just (EType t) ->
          case tag t of
            TCollection -> collectionMemberEffect i ef esrc t psrc >>= rt e
            TRecord ids ->
              case tnc rf of
                (FData _, fdch) -> do
                  idx <- maybe (memErr i esrc) return $ elemIndex i ids
                  rt e (finit ef, fdch !! idx)
                (_,_) -> rt e (finit ef, fnone)
            _ -> prjErr esrc
        _ -> prjErr esrc

    infer m ef _      e@(tag -> EAssign i)     = m >> filkupepM i >>= \p -> rt e (finit $ ef ++ [Just $ fwrite p], fnone)
    infer m ef _      e@(tag -> EOperate OSnd) = m >> rt e (finit $ ef ++ [Just fio], fnone)
    infer m ef [_,rf] e@(tag -> EOperate OSeq) = m >> rt e (finit ef, rf)
    infer m ef _      e@(tag -> EOperate _)    = m >> rt e (finit ef, fnone)

    infer m [pe,te,ee] [_,tr,er] e@(tag -> EIfThenElse) =
      m >> rt e (finit [pe, Just $ fset $ catMaybes [te, ee]], fset [tr,er])

    infer m [initef,bef] [_,rf] e@(tag -> ELetIn  i) = m >> do
      mv <- filkupeM i >>= fmv
      popVars i
      rt e (Just fnone, fscope [mv] (fromJust $ finit [initef]) (fromJust $ finit [bef]) fnone rf)

    infer m [initef,bef] [_,rf] e@(tag -> EBindAs b) = m >> do
      fmvs <- mapM filkupeM (bindingVariables b) >>= mapM fmv
      ps   <- mapM filkupepM (bindingVariables b) >>= mapM pmv >>= mapM (filkupppM . pmvptr)
      mapM_ popVars (bindingVariables b)
      rt e (Just fnone, fscope fmvs (fromJust $ finit [initef])
                                    (fromJust $ finit [bef])
                                    (fseq $ map fwrite ps)
                                    rf)

    infer m [oef,sef,nef] [_,snf,rnf] e@(tag -> ECaseOf _) = m >> do
      (cfmv, cpmv) <- fipopcaseM ()
      rt e (Just fnone, fscope [cfmv] (fromJust $ finit [oef])
                                      (fset $ catMaybes [sef, nef])
                                      (fset [fwrite $ pbvar cpmv, fnone])
                                      (fset [snf, rnf]))
    
    infer m _ _ e@(tag -> EAddress) = m >> rt e (Nothing, fnone)

    -- TODO: unhandled cases: ESelf, EImperative
    infer m _ _ e = m >> inferErr e

    rt e r@(Nothing, rf) = fiextmM e fnone rf >> return r
    rt e r@(Just ef, rf) = fiextmM e ef rf    >> return r

    finit ef = Just $ fseq $ catMaybes ef
    popVars i = fideleM i >> fidelepM i

    matchLambdaEff f@(tag -> FLambda _) = return $ Just f
    matchLambdaEff f@(tag -> FFVar _)   = return $ Just f
    matchLambdaEff f@(tag -> FBVar _)   = return $ Just f
    matchLambdaEff _ = return Nothing

    forceLambdaEff t@(isTFunction -> True) f = matchLambdaEff f >>= maybe (effectsOfType [] t) return
    forceLambdaEff _ f = return f

    subIEff i (tnc -> (FData _, ch)) = return $ ch !! i
    subIEff _ _ = return fnone

    subEff _ (tnc -> (FData _, ch)) = return ch
    subEff tl _ = return $ replicate (length tl) fnone

    freshM asCase e i u t f =
      case e @~ isEProvenance of
        Just (EProvenance (tag -> PMaterialize mvs)) -> do
          mapM_ (\mv -> fiextepM (pmvn mv) (pbvar mv)) mvs
          forceLambdaEff t f >>= void . fifreshM i u

        Just (EProvenance (tnc -> (PSet, (safeHead -> Just (tag -> PMaterialize mvs))))) | asCase -> do
          trace (unlines [unwords ["Fresh as case ", i, show asCase, show mvs], T.unpack $ PT.pretty f])
            $ mapM_ (\mv -> fiextepM (pmvn mv) (pbvar mv)) mvs
          forceLambdaEff t f >>= void . fifreshM i u

        _ -> matErr e

      where safeHead []    = Nothing
            safeHead (h:_) = Just h

    freshOptM e i u (PTOption et) f = subIEff (0 :: Int) f >>= freshM True e i u et
    freshOptM _ _ _ t _ = optTErrM t

    freshIndM e i u (PTIndirection et) f = subIEff (0 :: Int) f >>= freshM False e i u et
    freshIndM _ _ _ t _ = indTErrM t

    mkTupFresh e u ((_,n),t,f) = freshM False e n u t f
    mkRecFresh e u ((_,d),t,f) = freshM False e d u t f

    freshTupM e u (PTTuple tl) f bi = do
      subf <- subEff tl f
      mapM_ (mkTupFresh e u) $ zip3 bi tl subf

    freshTupM _ _ _ t _ = tupTErrM t

    freshRecM e u (PTRecord tl) f bi = do
      subf <- subEff tl f
      mapM_ (mkRecFresh e u) $ zip3 bi tl subf

    freshRecM _ _ _ t _ = recTErrM t

    fmv (tag -> FBVar mv) = return mv
    fmv f = errorM $ PT.boxToString $ [T.pack "Invalid effect bound var: "] %$ PT.prettyLines f

    pmv (tag -> PBVar mv) = return mv
    pmv p = errorM $ PT.boxToString $ [T.pack "Invalid provenance bound var: "] %$ PT.prettyLines p

    inferErr e = errorM $ PT.boxToString $ [T.pack "Could not infer effects for "] %$ PT.prettyLines e

    matErr   e = errorM $ PT.boxToString $ [T.pack "No materialization on: "]    %+ PT.prettyLines e
    tAnnErr  e = errorM $ PT.boxToString $ [T.pack "No type annotation on: "]    %+ PT.prettyLines e
    optTErrM t = errorM $ PT.boxToString $ [T.pack "Invalid option type: "]      %+ PT.prettyLines t
    indTErrM t = errorM $ PT.boxToString $ [T.pack "Invalid indirection type: "] %+ PT.prettyLines t
    tupTErrM t = errorM $ PT.boxToString $ [T.pack "Invalid tuple type: "]       %+ PT.prettyLines t
    recTErrM t = errorM $ PT.boxToString $ [T.pack "Invalid record type: "]      %+ PT.prettyLines t

    prjErr   e = errorM $ PT.boxToString $ [T.pack "Invalid type on "] %+ PT.prettyLines e
    memErr i e = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Failed to project", i, "from"]]
                                         %$ PT.prettyLines e


substituteEffects :: K3 Expression -> FInfM (K3 Expression)
substituteEffects expr = modifyTree injectEffects expr
  where injectEffects e = filkupmM e >>= \(f,s) -> return ((e @+ ESEffect f) @+ EFStructure s)

effectsOfType :: [Identifier] -> K3 Type -> FInfM (K3 Effect)
effectsOfType args t | isTFunction t =
   case tnc t of
    (TForall _, [ch])      -> effectsOfType args ch
    (TFunction, [_, retT]) -> let a = mkArg (length args + 1)
                              in effectsOfType (args++[a]) retT-- >>= \ef -> return (flambda a fnone ef fnone)
    _ -> errorM $ PT.boxToString $ [T.pack "Invalid function type"] %+ PT.prettyLines t
  where mkArg i = "__arg" ++ show i

effectsOfType [] _   = return fnone
effectsOfType args _ = return $ foldl lam (flambda (last args) fnone ef fnone) $ init args
  where 
    lam rfacc a = flambda a fnone fnone rfacc
    ef = floop $ fseq $ concatMap (\i -> [fread $ pfvar i, fwrite $ pfvar i]) args


-- | Computes execution effects and effect structure for a collection field member.
--   TODO: much like its counterpart with provenance, this method recomputes the effect rather
--   using a cached result. 
collectionMemberEffect :: Identifier -> [Maybe (K3 Effect)] -> K3 Expression -> K3 Type -> K3 Provenance
                       -> FInfM (Maybe (K3 Effect), K3 Effect)
collectionMemberEffect i ef esrc t psrc =
  let annIds = namedTAnnotations $ annotations t in do
    memsEnv <- mapM filkupasM annIds >>= return . Map.unions
    (mrf, lifted) <- maybe memErr return $ Map.lookup i memsEnv
    mrf' <- fmapProvM (subpself psrc) mrf
    if not lifted then attrErr else return $ (Just $ fseq $ catMaybes ef, mrf')

  where
    subpself p (tag -> PFVar "self") = return p
    subpself _ p = return p

    memErr  = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Unknown projection of ", i, "on"]]           %+ PT.prettyLines esrc
    attrErr = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Invalid attribute projection of ", i, "on"]] %+ PT.prettyLines esrc


-- | Replaces 5-place FApply effect nodes with only their initial execution and
--   deferred body effects, as well as their effect structure.
simplifyEffects :: K3 Effect -> FInfM (K3 Effect)
simplifyEffects f = modifyTree simplify f
  where simplify f'@(tnc -> (FApply mvOpt, [_,_,ef,bf,r])) =
          return $ Node (FApply mvOpt :@: annotations f') [ef,bf,r]

        -- Rebuilding FSet, and FSeq will filter all fnones. 
        simplify (tnc -> (FSet, ch))  = return $ fset ch 
        simplify (tnc -> (FSeq, ch))  = return $ fseq ch 
        simplify f' = return f'

-- | Simplifies applies on all effect trees attached to an expression.
simplifyExprEffects :: K3 Declaration -> FInfM (K3 Declaration)
simplifyExprEffects d = mapExpression simplifyExpr d
  where simplifyExpr e = modifyTree simplifyEffAnns e

        simplifyEffAnns e = do
          (Just (ESEffect f1), Just (EFStructure f2)) <- annsOf e
          nf1 <- simplifyEffects f1
          nf2 <- simplifyEffects f2
          return $ ((e @<- (filter (not . isEffectAnn) $ annotations e)) @+ (ESEffect nf1)) @+ (EFStructure nf2)

        annsOf e = return (e @~ isESEffect, e @~ isEFStructure)

        isEffectAnn (ESEffect _) = True
        isEffectAnn (EFStructure _) = True
        isEffectAnn _ = False

{- Pattern synonyms for inference -}
pattern PTOption et <- Node (TOption :@: _) [et]
pattern PTIndirection et <- Node (TIndirection :@: _) [et]
pattern PTTuple tt <- Node (TTuple :@: _) tt
pattern PTRecord rt <- Node (TRecord _ :@: _) rt

{- Effect environment pretty printing -}
instance Pretty FIEnv where
  prettyLines (FIEnv c p e a pb _ cl ef _) =
    [T.pack $ "FCnt: " ++ show c] ++
    [T.pack "FEnv: "  ]  %$ (PT.indent 2 $ PT.prettyLines e)  ++
    [T.pack "FPEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines p)  ++
    [T.pack "FAEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines a)  ++
    [T.pack "FPBEnv: "]  %$ (PT.indent 2 $ PT.prettyLines pb) ++
    [T.pack "FLCEnv: "]  %$ (PT.indent 2 $ PT.prettyLines cl) ++
    [T.pack "EFMap: " ]  %$ (PT.indent 2 $ PT.prettyLines ef)

instance Pretty (IntMap (K3 Effect)) where
  prettyLines fp = IntMap.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] fp

instance Pretty (IntMap (K3 Effect, K3 Effect)) where
  prettyLines fp = IntMap.foldlWithKey (\acc k (u,v) -> acc ++ prettyTriple k u v) [] fp

instance Pretty FEnv where
  prettyLines fe = Map.foldlWithKey (\acc k v -> acc ++ prettyFrame k v) [] fe
    where prettyFrame k v = concatMap prettyPair $ flip zip v $ replicate (length v) k

instance Pretty FAEnv where
  prettyLines fa = Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] fa

instance Pretty FMEnv where
  prettyLines fm = Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k, fst v)) [] fm

prettyPair :: (Show a, Pretty b) => (a,b) -> [Text]
prettyPair (a,b) = [T.pack $ show a ++ " => "] %+ PT.prettyLines b

prettyTriple :: (Show a, Pretty b, Pretty c) => a -> b -> c -> [Text]
prettyTriple a b c =
  [T.pack $ show a ++ " "]
    %+ (PT.intersperseBoxes [T.pack " , "] [PT.prettyLines b, PT.prettyLines c])
