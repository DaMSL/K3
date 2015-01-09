{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Provenance (i.e., dataflow) analysis for K3 programs.
module Language.K3.Analysis.Provenance.Inference (
  PIEnv(..),
  PEnv,
  PAEnv,
  PMEnv,
  PPEnv,
  PLCEnv,
  EPMap,
  PInfM,

  pienv0,
  inferProgramProvenance,
  reinferProgDeclProvenance
) where

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

import Data.Text ( Text )
import Language.K3.Utils.PrettyText ( Pretty, (%$), (%+) )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

prvTraceLogging :: Bool
prvTraceLogging = False

localLog :: (Functor m, Monad m) => String -> m ()
localLog = logVoid prvTraceLogging

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
localLogAction = logAction prvTraceLogging


-- | A provenance bindings environment, layered to support shadowing.
type PEnv = Map Identifier [K3 Provenance]

-- | A provenance bindings environment for annotations,
--   indexed by annotation and attribute name.
type PAEnv = Map Identifier PMEnv
type PMEnv = Map Identifier (K3 Provenance, Bool)

-- | A provenance "pointer" environment
type PPEnv = IntMap (K3 Provenance)

-- | A lambda closure environment: ELambda UID => identifiers of closure variables.
type PLCEnv = IntMap [Identifier]

-- | A mapping of expression ids and provenance ids.
type EPMap = IntMap (K3 Provenance)

-- | A provenance inference environment.
data PIEnv = PIEnv {
               pcnt    :: Int,
               ppenv   :: PPEnv,
               penv    :: PEnv,
               paenv   :: PAEnv,
               plcenv  :: PLCEnv,
               epmap   :: EPMap,
               pcase   :: [PMatVar]   -- Temporary storage stack for case variables.
            }

-- | The type inference monad
type PInfM = EitherT Text (State PIEnv)

{- Data.Text helpers -}
mkErr :: String -> Either Text a
mkErr msg = Left $ T.pack msg

mkErrP :: PT.Pretty a => String -> a -> Either Text b
mkErrP msg a = Left $ T.unlines [T.pack msg, PT.pretty a]

{- PEnv helpers -}
penv0 :: PEnv
penv0 = Map.empty

plkup :: PEnv -> Identifier -> Either Text (K3 Provenance)
plkup env x = maybe err safeHead $ Map.lookup x env
  where
    safeHead l = if null l then err else Right $ head l
    err = mkErrP msg env
    msg = "Unbound variable in lineage binding environment: " ++ x

pext :: PEnv -> Identifier -> K3 Provenance -> PEnv
pext env x p = Map.insertWith (++) x [p] env

pdel :: PEnv -> Identifier -> PEnv
pdel env x = Map.update safeTail x env
  where safeTail []  = Nothing
        safeTail [_] = Nothing
        safeTail l   = Just $ tail l

pmem :: PEnv -> Identifier -> Either Text Bool
pmem env x = return $ Map.member x env

{- PPEnv helpers -}
ppenv0 :: PPEnv
ppenv0 = IntMap.empty

pplkup :: PPEnv -> Int -> Either Text (K3 Provenance)
pplkup env x = maybe err Right $ IntMap.lookup x env
  where err = mkErrP msg env
        msg = "Unbound pointer in lineage environment: " ++ show x

ppext :: PPEnv -> Int -> K3 Provenance -> PPEnv
ppext env x p = IntMap.insert x p env

ppdel :: PPEnv -> Int -> PPEnv
ppdel env x = IntMap.delete x env


{- PAEnv helpers -}
paenv0 :: PAEnv
paenv0 = Map.empty

pmenv0 :: PMEnv
pmenv0 = Map.empty

palkup :: PAEnv -> Identifier -> Identifier -> Either Text (K3 Provenance)
palkup env x y = maybe err (maybe err (Right . fst) . Map.lookup y) $ Map.lookup x env
  where err = mkErr $ "Unbound annotation member in lineage environment: " ++ x

paext :: PAEnv -> Identifier -> Identifier -> K3 Provenance -> Bool -> PAEnv
paext env x y p l = Map.insertWith Map.union x (Map.fromList [(y,(p,l))]) env

palkups :: PAEnv -> Identifier -> Either Text PMEnv
palkups env x = maybe err Right $ Map.lookup x env
  where err = mkErr $ "Unbound annotation in lineage environment: " ++ x

paexts :: PAEnv -> Identifier -> PMEnv -> PAEnv
paexts env x ap' = Map.insertWith Map.union x ap' env


{- PLCEnv helpers -}
plcenv0 :: PLCEnv
plcenv0 = IntMap.empty

plclkup :: PLCEnv -> Int -> Either Text [Identifier]
plclkup env x = maybe err Right $ IntMap.lookup x env
  where err = mkErr $ "Unbound UID in closure environment: " ++ show x


{- EPMap helpers -}
epmap0 :: EPMap
epmap0 = IntMap.empty

eplkup :: EPMap -> K3 Expression -> Either Text (K3 Provenance)
eplkup epm e@((@~ isEUID) -> Just (EUID (UID uid))) = maybe lookupErr Right $ IntMap.lookup uid epm
  where lookupErr = Left $ PT.boxToString $ msg %$ PT.prettyLines e
        msg = [T.unwords $ map T.pack ["No provenance found for", show uid]]

eplkup _ e = Left $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e

epext :: EPMap -> K3 Expression -> K3 Provenance -> Either Text EPMap
epext epm e p = case e @~ isEUID of
  Just (EUID (UID i)) -> Right $ IntMap.insert i p epm
  _ -> Left $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e


{- PIEnv helpers -}
pienv0 :: PLCEnv -> PIEnv
pienv0 lcenv = PIEnv 0 ppenv0 penv0 paenv0 lcenv epmap0 []

-- | Modifiers.
{-
mpiee :: (PEnv -> PEnv) -> PIEnv -> PIEnv
mpiee f env = env {penv = f $ penv env}

mpiep :: (PPEnv -> PPEnv) -> PIEnv -> PIEnv
mpiep f env = env {ppenv = f $ ppenv env}
-}

pilkupe :: PIEnv -> Identifier -> Either Text (K3 Provenance)
pilkupe env x = plkup (penv env) x

piexte :: PIEnv -> Identifier -> K3 Provenance -> PIEnv
piexte env x p = env {penv=pext (penv env) x p}

pidele :: PIEnv -> Identifier -> PIEnv
pidele env i = env {penv=pdel (penv env) i}

pimeme :: PIEnv -> Identifier -> Either Text Bool
pimeme env i = pmem (penv env) i

pilkupp :: PIEnv -> Int -> Either Text (K3 Provenance)
pilkupp env x = pplkup (ppenv env) x

piextp :: PIEnv -> Int -> K3 Provenance -> PIEnv
piextp env x p = env {ppenv=ppext (ppenv env) x p}

{-
pidelp :: PIEnv -> Int -> PIEnv
pidelp env i = env {ppenv=ppdel (ppenv env) i}
-}

pilkupa :: PIEnv -> Identifier -> Identifier -> Either Text (K3 Provenance)
pilkupa env x y = palkup (paenv env) x y

pilkupas :: PIEnv -> Identifier -> Either Text PMEnv
pilkupas env x = palkups (paenv env) x

piextas :: PIEnv -> Identifier -> PMEnv -> PIEnv
piextas env x p = env {paenv=paexts (paenv env) x p}

pilkupm :: PIEnv -> K3 Expression -> Either Text (K3 Provenance)
pilkupm env e = eplkup (epmap env) e

piextm :: PIEnv -> K3 Expression -> K3 Provenance -> Either Text PIEnv
piextm env e p = let epmapE = epext (epmap env) e p
                 in either Left (\nep -> Right $ env {epmap=nep}) epmapE

pilkupc :: PIEnv -> Int -> Either Text [Identifier]
pilkupc env i = plclkup (plcenv env) i

pipopcase :: PIEnv -> Either Text (PMatVar, PIEnv)
pipopcase env = case pcase env of
  [] -> Left $ T.pack "Uninitialized case matvar stack"
  h:t -> return (h, env {pcase=t})

pipushcase :: PIEnv -> PMatVar -> PIEnv
pipushcase env mv = env {pcase=mv:(pcase env)}


{- Fresh pointer and binding construction. -}

-- | Self-referential provenance pointer construction
pifreshfp :: PIEnv -> Identifier -> UID -> (K3 Provenance, PIEnv)
pifreshfp pienv i u =
  let j = pcnt pienv
      p = pbvar $ PMatVar i u j
  in (p, piextp (pienv {pcnt=j+1}) j p)

pifreshbp :: PIEnv -> Identifier -> UID -> K3 Provenance -> (K3 Provenance, PIEnv)
pifreshbp pienv i u p =
  let j  = pcnt pienv
      p' = pbvar $ PMatVar i u j
  in (p', piextp (pienv {pcnt=j+1}) j p)

-- | Self-referential provenance pointer construction
--   This adds a new named pointer to both the named and pointer environments.
pifreshs :: PIEnv -> Identifier -> UID -> (K3 Provenance, PIEnv)
pifreshs pienv n u =
  let (p, nenv) = pifreshfp pienv n u in (p, piexte nenv n p)

-- | Provenance linked pointer construction.
pifresh :: PIEnv -> Identifier -> UID -> K3 Provenance -> (K3 Provenance, PIEnv)
pifresh pienv n u p =
  let (p', nenv) = pifreshbp pienv n u p in (p', piexte nenv n p')

pifreshAs :: PIEnv -> Identifier -> [(Identifier, UID, Bool)] -> (PMEnv, PIEnv)
pifreshAs pienv n memN =
  let mkMemP lacc l p             = lacc++[(p,l)]
      extMemP (lacc,eacc) (i,u,l) = first (mkMemP lacc l) $ pifreshfp eacc i u
      (memP, npienv)              = foldl extMemP ([], pienv) memN
      memNP                       = Map.fromList $ zip (map (\(a,_,_) -> a) memN) memP
  in (memNP, piextas npienv n memNP)


{- Provenance pointer helpers -}

{-
-- | Retrieves the provenance value referenced by a named pointer
piload :: PIEnv -> Identifier -> Either Text (K3 Provenance)
piload pienv n = do
  p <- pilkupe pienv n
  case tag p of
    PBVar mv -> pilkupp pienv $ pmvptr mv
    _ -> Left $ PT.boxToString $ [T.pack "Invalid load on pointer"] %$ PT.prettyLines p
-}

-- | Sets the provenance value referenced by a named pointer
pistore :: PIEnv -> Identifier -> UID -> K3 Provenance -> Either Text PIEnv
pistore pienv n u p = do
  p' <- pilkupe pienv n
  case tag p' of
    PBVar mv | (pmvn mv, pmvloc mv) == (n,u) -> return $ piextp pienv (pmvptr mv) p
    _ -> Left $ PT.boxToString $ [T.pack "Invalid store on pointer"] %$ PT.prettyLines p'

pistorea :: PIEnv -> Identifier -> [(Identifier, UID, K3 Provenance, Bool)] -> Either Text PIEnv
pistorea pienv n memP = do
  pmenv <- pilkupas pienv n
  foldM (storemem pmenv) pienv memP

  where storemem pmenv eacc (i,u,p,_) = maybe (invalidMem i) (\(p',_) -> store eacc i u p' p) $ Map.lookup i pmenv
        store eacc i u (tag -> PBVar mv) p
          | (pmvn mv, pmvloc mv) == (i,u) = return $ piextp eacc (pmvptr mv) p
        store _ i u p _ = invalidStore i u p

        invalidMem   i     = mkErr $ "Invalid store on annotation member" ++ i
        invalidStore i u p = Left $ PT.boxToString $ storeMsg i u %$ PT.prettyLines p
        storeMsg     i u   = [T.unwords $ map T.pack ["Invalid store on pointer", "@loc", show (i,u)]]

-- | Traverses all pointers until reaching a non-pointer.
--   This function stops on any cycles detected.
pichase :: PIEnv -> K3 Provenance -> Either Text (K3 Provenance)
pichase pienv cp = aux [] cp
  where aux path p@(tag -> PBVar (pmvptr -> i)) | i `elem` path = return p
                                                | otherwise = pilkupp pienv i >>= aux (i:path)
        aux _ p = return p

-- Capture-avoiding substitution of any free variable with the given identifier.
pisub :: PIEnv -> Identifier -> K3 Provenance -> K3 Provenance -> Either Text (K3 Provenance, PIEnv)
pisub pienv i dp sp = acyclicSub pienv emptyPtrSubs [] sp >>= \(renv, _, rp) -> return (rp, renv)
  where
    acyclicSub env subs _ (tag -> PFVar j) | i == j = return (env, subs, dp)

    acyclicSub env subs path p@(tag -> PBVar mv@(pmvptr -> j))
      | j `elem` path   = return (env, subs, p)
      | isPtrSub subs j = getPtrSub subs j >>= return . (env, subs,)
      | otherwise       = pilkupp env j >>= subPtrIfDifferent env subs (j:path) mv

    -- TODO: we can short-circuit descending into the body if we
    -- are willing to stash the expression uid in a PLambda, using
    -- this uid to lookup i's presence in our precomputed closures.
    acyclicSub env subs _ p@(tag -> PLambda j) | i == j = return (env, subs, p)

    -- Avoid descent into materialization points of shadowed variables.
    acyclicSub env subs _ p@(tag -> PMaterialize mvs) | i `elem` map pmvn mvs = return (env, subs, p)

    -- Handle higher-order function application (two-place PApply nodes) where a PFVar
    -- in the lambda position is replaced by a PLambda
    acyclicSub env subs path p@(tnc -> (PApply Nothing, [lp@(tag -> PFVar _), argp])) = do
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) [lp, argp]
      case nch of
        [lp',argp'] -> simplifyApply nenv Nothing lp' argp' >>= \(p',nenv') -> return (nenv', nsubs, p')
        _ -> appSubErr p

    acyclicSub env subs path n@(Node _ ch) = do
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) ch
      return (nenv, nsubs, replaceCh n nch)

    rcr path (eacc,sacc,chacc) c = acyclicSub eacc sacc path c >>= \(e,s,nc) -> return (e,s,chacc++[nc])

    -- Preserve original pointer p unless any substitution actually occurred.
    -- We preserve sharing for the original pointer when substituting in deep structures containing PFVar i.
    -- This is implemented by creating a new pointer environment entry for the substitution, and
    -- then to track and apply pointer substitutions for PBVars referring to the old entry.
    -- This method is not tail-recursive.
    subPtrIfDifferent env subs path mv p = do
      (nenv, nsubs, np) <- acyclicSub env subs path p
      if p == np then return (nenv, nsubs, pbvar mv) else addPtrSub nenv nsubs mv np

    -- Pointer substitutions, as a map of old PPtr => new PMatVar
    emptyPtrSubs   = IntMap.empty
    isPtrSub  ps j = IntMap.member j ps
    getPtrSub ps j = maybe (lookupErr j) (return . pbvar) $ IntMap.lookup j ps
    addPtrSub env ps mv p =
      let (np, nenv) = pifresh env (pmvn mv) (pmvloc mv) p in
      case tag np of
        PBVar nmv -> return (nenv, IntMap.insert (pmvptr mv) nmv ps, np)
        _ -> freshErr np

    lookupErr j = Left $ T.pack $ "Could not find pointer substitution for " ++ show j
    freshErr  p = Left $ PT.boxToString $ [T.pack "Invalid fresh PBVar result "] %$ PT.prettyLines p
    appSubErr p = Left $ PT.boxToString $ [T.pack "Invalid apply substitution at: "] %$ PT.prettyLines p


{- Apply simplification -}

chaseLambda :: PIEnv -> [PPtr] -> K3 Provenance -> Either Text [K3 Provenance]
chaseLambda _ _ p@(tag -> PLambda _) = return [p]
chaseLambda _ _ p@(tag -> PFVar _)   = return [p]

chaseLambda env path p@(tag -> PBVar (pmvptr -> i))
  | i `elem` path = return [p]
  | otherwise     = pichase env p >>= chaseLambda env (i:path)

chaseLambda env path (tnc -> (PApply _, [_,_,r]))   = chaseLambda env path r
chaseLambda env path (tnc -> (PMaterialize _, [r])) = chaseLambda env path r
chaseLambda env path (tnc -> (PProject _, [_,r]))   = chaseLambda env path r
chaseLambda env path (tnc -> (PGlobal _, [r]))      = chaseLambda env path r
chaseLambda env path (tnc -> (PChoice, r:_))        = chaseLambda env path r -- TODO: for now we use the first choice.
chaseLambda env path (tnc -> (PSet, rl))            = mapM (chaseLambda env path) rl >>= return . concat
chaseLambda env _ p = Left $ PT.boxToString $ [T.pack "Invalid application or lambda: "]
                          %$ PT.prettyLines p %$ [T.pack "Env:"] %$ PT.prettyLines env

chaseAppArg :: K3 Provenance -> Either Text (K3 Provenance)
chaseAppArg (tnc -> (PApply _, [_,_,r])) = chaseAppArg r
chaseAppArg (tnc -> (PMaterialize _, [r])) = chaseAppArg r
chaseAppArg p = return p

simplifyApply :: PIEnv -> Maybe (K3 Expression) -> K3 Provenance -> K3 Provenance -> Either Text (K3 Provenance, PIEnv)
simplifyApply pienv eOpt lp argp = do
  let debugChase lp' argp' = flip trace lp' (T.unpack $ PT.boxToString
                                 $ [T.pack "chaseLambda"]
                                %$ [T.pack "Expr:"]  %$ (maybe [] PT.prettyLines eOpt)
                                %$ [T.pack "LProv:"] %$ PT.prettyLines lp'
                                %$ [T.pack "AProv:"] %$ PT.prettyLines argp')
  uOpt   <- maybe (return Nothing) (\e -> uidOf e >>= return . Just) eOpt
  argp'  <- chaseAppArg argp
  manyLp <- chaseLambda pienv [] {-(debugChase lp argp)-} lp
  (manyLp', nenv) <- foldM (doSimplify uOpt argp') ([], pienv) manyLp
  case manyLp' of
    []  -> appLambdaErr lp
    [p] -> return (p, nenv)
    _   -> return (pset manyLp', nenv)

  where
    doSimplify uOpt argp' (chacc, eacc) lp' =
      case tnc lp' of
        (PLambda i, [bp]) ->
          case uOpt of
            Just uid -> do
              let (ip, neacc) = pifreshbp eacc i uid argp'
              imv <- pmv ip
              (rp, reacc) <- pisub neacc i ip bp
              return (chacc ++ [papply (Just imv) lp argp rp], reacc)

            Nothing -> do
              (rp, reacc) <- pisub eacc i argp' bp
              return (chacc ++ [papply Nothing lp argp rp], reacc)

        -- Handle recursive functions, and forward declarations
        -- by using an opaque return value.
        (PBVar _, _) -> return (chacc ++ [papply Nothing lp argp ptemp], eacc)

        -- Handle higher-order and external functions as an unsimplified apply.
        (PFVar _, _) -> return (chacc ++ [papplyExt lp argp], eacc)

        _ -> appLambdaErr lp'

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = Left $ PT.boxToString $ [T.pack "No uid found for psimplifyapp on "] %$ PT.prettyLines e

    pmv (tag -> PBVar mv) = return mv
    pmv p = Left $ PT.boxToString $ [T.pack "Invalid provenance bound var: "] %$ PT.prettyLines p

    exprErr = flip (maybe []) eOpt $ \e -> PT.prettyLines e
    appLambdaErr p = Left $ PT.boxToString $ [T.pack "Invalid function provenance on:"]
                          %$ exprErr %$ [T.pack "Provenance:"] %$ PT.prettyLines p


simplifyApplyM :: Maybe (K3 Expression) -> K3 Provenance -> K3 Provenance -> PInfM (K3 Provenance)
simplifyApplyM eOpt lp argp = do
  env <- get
  (p', nenv) <- liftEitherM $ simplifyApply env eOpt lp argp
  void $ put nenv
  return p'


{- PInfM helpers -}

runPInfM :: PIEnv -> PInfM a -> (Either Text a, PIEnv)
runPInfM env m = flip runState env $ runEitherT m

runPInfE :: PIEnv -> PInfM a -> Either Text (a, PIEnv)
runPInfE env m = let (a,b) = runPInfM env m in a >>= return . (,b)

runPInfES :: PIEnv -> PInfM a -> Either String (a, PIEnv)
runPInfES env m = either (Left . T.unpack) Right $ runPInfE env m

reasonM :: (Text -> Text) -> PInfM a -> PInfM a
reasonM errf = mapEitherT $ \m -> m >>= \case
  Left  err -> get >>= \env -> (return . Left $ errf $ T.unlines [err, T.pack "Provenance environment:", PT.pretty env])
  Right r   -> return $ Right r

errorM :: Text -> PInfM a
errorM msg = reasonM id $ left msg

liftEitherM :: Either Text a -> PInfM a
liftEitherM = either left return

{-
pifreshbpM :: Identifier -> UID -> K3 Provenance -> PInfM (K3 Provenance)
pifreshbpM n u p = get >>= return . (\env -> pifreshbp env n u p) >>= \(p',nenv) -> put nenv >> return p'
-}

pifreshsM :: Identifier -> UID -> PInfM (K3 Provenance)
pifreshsM n u = get >>= return . (\env -> pifreshs env n u) >>= \(p,env) -> put env >> return p

pifreshM :: Identifier -> UID -> K3 Provenance -> PInfM (K3 Provenance)
pifreshM n u p = get >>= return . (\env -> pifresh env n u p) >>= \(p',env) -> put env >> return p'

pifreshAsM :: Identifier -> [(Identifier, UID, Bool)] -> PInfM PMEnv
pifreshAsM n mems = get >>= return . (\env -> pifreshAs env n mems) >>= \(p,env) -> put env >> return p

pilkupeM :: Identifier -> PInfM (K3 Provenance)
pilkupeM n = get >>= liftEitherM . flip pilkupe n

piexteM :: Identifier -> K3 Provenance -> PInfM ()
piexteM n p = get >>= \env -> return (piexte env n p) >>= put

pideleM :: Identifier -> PInfM ()
pideleM n = get >>= \env -> return (pidele env n) >>= put

pimemeM :: Identifier -> PInfM Bool
pimemeM n = get >>= liftEitherM . flip pimeme n

pilkuppM :: Int -> PInfM (K3 Provenance)
pilkuppM n = get >>= liftEitherM . flip pilkupp n

{-
piextpM :: Int -> K3 Provenance -> PInfM ()
piextpM n p = get >>= \env -> return (piextp env n p) >>= put

pidelpM :: Int -> PInfM ()
pidelpM n = get >>= \env -> return (pidelp env n) >>= put
-}

pilkupaM :: Identifier -> Identifier -> PInfM (K3 Provenance)
pilkupaM n m = get >>= liftEitherM . (\env -> pilkupa env n m)

pilkupasM :: Identifier -> PInfM PMEnv
pilkupasM n = get >>= liftEitherM . flip pilkupas n

pilkupmM :: K3 Expression -> PInfM (K3 Provenance)
pilkupmM e = get >>= liftEitherM . flip pilkupm e

piextmM :: K3 Expression -> K3 Provenance -> PInfM ()
piextmM e p = get >>= liftEitherM . (\env -> piextm env e p) >>= put

{-
piloadM :: Identifier -> PInfM (K3 Provenance)
piloadM n = get >>= liftEitherM . flip piload n
-}

pistoreM :: Identifier -> UID -> K3 Provenance -> PInfM ()
pistoreM n u p = get >>= liftEitherM . (\env -> pistore env n u p) >>= put

pistoreaM :: Identifier -> [(Identifier, UID, K3 Provenance, Bool)] -> PInfM ()
pistoreaM n memP = get >>= liftEitherM . (\env -> pistorea env n memP) >>= put

{-
pichaseM :: K3 Provenance -> PInfM (K3 Provenance)
pichaseM p = get >>= liftEitherM . flip pichase p

pisubM :: Identifier -> K3 Provenance -> K3 Provenance -> PInfM (K3 Provenance)
pisubM i rep p = get >>= liftEitherM . (\env -> pisub env i rep p) >>= \(p',nenv) -> put nenv >> return p'
-}

pilkupcM :: Int -> PInfM [Identifier]
pilkupcM n = get >>= liftEitherM . flip pilkupc n

pipopcaseM :: () -> PInfM PMatVar
pipopcaseM _ = get >>= liftEitherM . pipopcase >>= \(mv,env) -> put env >> return mv

pipushcaseM :: PMatVar -> PInfM ()
pipushcaseM mv = get >>= return . flip pipushcase mv >>= put


-- | Misc. inference helpers.
uidOfD :: K3 Declaration -> PInfM UID
uidOfD d = maybe uidErrD (\case {(DUID u) -> return u ; _ ->  uidErrD}) $ d @~ isDUID
  where uidErrD = errorM $ PT.boxToString $ [T.pack "No uid found for pdeclinf on "] %+ PT.prettyLines d

memUID :: AnnMemDecl -> [Annotation Declaration] -> PInfM UID
memUID memDecl as = case filter isDUID as of
                      [DUID u] -> return u
                      _ -> memUIDErr
  where memUIDErr = errorM $ T.append (T.pack "Invalid member UID") $ PT.pretty memDecl

unlessHasProvenance :: K3 Declaration -> (() -> PInfM (K3 Declaration)) -> PInfM (K3 Declaration)
unlessHasProvenance d f = case d @~ isDProvenance of
  Nothing -> f ()
  _ -> return d

unlessMemHasProvenance :: AnnMemDecl -> [Annotation Declaration] -> (() -> PInfM AnnMemDecl)
                       -> PInfM AnnMemDecl
unlessMemHasProvenance mem as f = case find isDProvenance as of
  Nothing -> f ()
  _ -> return mem

-- | Returns a global's provenance based on its initializer expression.
--   This method also stores the provenance in the global's provenance pointer entry.
globalProvOf :: Identifier -> UID -> K3 Expression -> PInfM (K3 Provenance)
globalProvOf n u e = do
  p <- provOf e >>= return . pglobal n
  void $ pistoreM n u p
  return p

provOf :: K3 Expression -> PInfM (K3 Provenance)
provOf e = maybe provErr (\case {(EProvenance p') -> return p'; _ ->  provErr}) $ e @~ isEProvenance
  where provErr = errorM $ PT.boxToString $ [T.pack "No provenance found on "] %+ PT.prettyLines e

-- | Attaches any inferred provenance to a declaration
markGlobalProv :: K3 Declaration -> PInfM (K3 Declaration)
markGlobalProv d@(tag -> DGlobal  n _ eOpt) = unlessHasProvenance d $ \_ -> do
  u  <- uidOfD d
  p' <- maybe (pilkupeM n) (globalProvOf n u) eOpt
  return (d @+ (DProvenance $ Right p'))

markGlobalProv d@(tag -> DTrigger n _ e) = unlessHasProvenance d $ \_ -> do
  u  <- uidOfD d
  p' <- globalProvOf n u e
  return (d @+ (DProvenance $ Right p'))

markGlobalProv d@(tag -> DDataAnnotation n tvars mems) = do
  nmems <- mapM (markMemsProv n) mems
  return (replaceTag d $ DDataAnnotation n tvars nmems)

  where
    markMemsProv an m@(Lifted Provides mn mt meOpt mas) = unlessMemHasProvenance m mas $ \_ -> do
      p' <- maybe (pilkupaM an mn) provOf meOpt
      return (Lifted Provides mn mt meOpt $ mas ++ [DProvenance $ Right p'])

    markMemsProv an m@(Attribute Provides mn mt meOpt mas) = unlessMemHasProvenance m mas $ \_ -> do
      p' <- maybe (pilkupaM an mn) provOf meOpt
      return (Attribute Provides mn mt meOpt $ mas ++ [DProvenance $ Right p'])

    markMemsProv _ m = return m

markGlobalProv d = return d


{- Analysis entry point -}
inferProgramProvenance :: K3 Declaration -> Either String (K3 Declaration, PIEnv)
inferProgramProvenance prog = do
  lcenv <- lambdaClosures prog
  liftEitherTM $ runPInfE (pienv0 lcenv) $ doInference prog

  where
    liftEitherTM = either (Left . T.unpack) Right

    doInference p = do
      np   <- globalsProv p
      np'  <- mapExpression inferExprProvenance np
      np'' <- simplifyProgramProvenance np'
      markAllGlobals np''

    globalsProv :: K3 Declaration -> PInfM (K3 Declaration)
    globalsProv p = inferAllRcrDecls p >>= inferAllDecls

    inferAllRcrDecls p = mapProgram initializeRcrDeclProv return return Nothing p
    inferAllDecls    p = mapProgram inferDeclProv return return Nothing p
    markAllGlobals   p = mapProgram markGlobalProv return return Nothing p


-- | Repeat provenance inference on a global with an initializer.
reinferProgDeclProvenance :: PIEnv -> Identifier -> K3 Declaration -> Either String (K3 Declaration, PIEnv)
reinferProgDeclProvenance env dn prog = runPInfES env inferNamedDecl
  where
    inferNamedDecl = mapProgramWithDecl onNamedDecl (const return) (const return) Nothing prog

    onNamedDecl d@(tag -> DGlobal  n _ (Just e)) | dn == n = inferDecl n d e
    onNamedDecl d@(tag -> DTrigger n _ e)        | dn == n = inferDecl n d e
    onNamedDecl d = return d

    inferDecl n d e = do
      present <- pimemeM n
      nd   <- if present then return d else initializeRcrDeclProv d
      nd'  <- inferDeclProv nd
      ne   <- inferExprProvenance e >>= simplifyExprProvenance
      nd'' <- rebuildDecl ne nd'
      markGlobalProv nd''

    rebuildDecl e d@(tnc -> (DGlobal  n t (Just _), ch)) = return $ Node (DGlobal  n t (Just e) :@: annotations d) ch
    rebuildDecl e d@(tnc -> (DTrigger n t _, ch))        = return $ Node (DTrigger n t e        :@: annotations d) ch
    rebuildDecl _ d = return d


-- | Declaration "blind" initialization.
--   Adds a self-referential provenance pointer to a global for recursive calls.
initializeRcrDeclProv :: K3 Declaration -> PInfM (K3 Declaration)
initializeRcrDeclProv d@(tag -> DGlobal  n _ _) = uidOfD d >>= \u -> pifreshsM n u >> return d
initializeRcrDeclProv d@(tag -> DTrigger n _ _) = uidOfD d >>= \u -> pifreshsM n u >> return d
initializeRcrDeclProv d@(tag -> DDataAnnotation n _ mems) = mapM freshMems mems >>= pifreshAsM n . catMaybes >> return d
  where
    freshMems m@(Lifted      Provides mn _ _ mas) = memUID m mas >>= \u -> return (Just (mn, u, True))
    freshMems m@(Attribute   Provides mn _ _ mas) = memUID m mas >>= \u -> return (Just (mn, u, False))
    freshMems _ = return Nothing

initializeRcrDeclProv d = return d

-- | Structure-based declaration initialization.
--   Infer based on initializer, and update provenance pointer.
inferDeclProv :: K3 Declaration -> PInfM (K3 Declaration)
inferDeclProv d@(tag -> DGlobal n t eOpt) = do
  u <- uidOfD d
  p' <- case d @~ isDProvenance of
          Just (DProvenance prv) -> either return return prv
          _ -> maybe (provOfType [] t) inferProvenance eOpt
  void $ pistoreM n u $ pglobal n p'
  return d

inferDeclProv d@(tag -> DTrigger n _ e) = do
  u <- uidOfD d
  p' <- case d @~ isDProvenance of
          Just (DProvenance prv) -> either return return prv
          _ -> inferProvenance e
  void $ pistoreM n u $ pglobal n p'
  return d

inferDeclProv d@(tag -> DDataAnnotation n _ mems) = do
  mProvs <- mapM inferMemsProv mems
  void $ pistoreaM n $ catMaybes mProvs
  return d

  where
    inferMemsProv m@(Lifted    Provides mn mt meOpt mas) = inferMemberProv m True  mn mt meOpt mas
    inferMemsProv m@(Attribute Provides mn mt meOpt mas) = inferMemberProv m False mn mt meOpt mas
    inferMemsProv _ = return Nothing

    inferMemberProv mem lifted mn mt meOpt mas = do
      u  <- memUID mem mas
      mp <- case find isDProvenance mas of
              Just (DProvenance prv) -> either return return prv
              _ -> maybe (provOfType [] mt) inferProvenance meOpt
      return $ Just (mn,u,mp,lifted)

inferDeclProv d = return d

-- | Compute a provenance tree in a single pass, tracking expression-provenance associations.
--   Then, apply a second pass to attach associated provenances to each expression node.
inferExprProvenance :: K3 Expression -> PInfM (K3 Expression)
inferExprProvenance expr = inferProvenance expr >> substituteProvenance expr

-- | Computes a single extended provenance tree for an expression.
--   This includes an extra child at each PApply indicating the return value provenance
--   of the apply. The provenance associated with each subexpression is stored as a
--   separate relation in the environment rather than directly attached to each node.
inferProvenance :: K3 Expression -> PInfM (K3 Provenance)
inferProvenance expr = mapIn1RebuildTree topdown sideways inferWithRule expr
  where
    topdown _ e@(tag -> ELambda i) = piexteM i (pfvar i) >> pushClosure e
    topdown _ _ = iu

    sideways p e@(tag -> ELetIn  i) = uidOf e >>= \u -> return [freshM i u p]
    sideways p e@(tag -> ECaseOf i) = do
      u <- uidOf e
      return [freshM i u $ poption p, pilkupeM i >>= pmv >>= pipushcaseM >> pideleM i]

    sideways p e@(tag -> EBindAs b) = do
      u <- uidOf e
      case b of
        BIndirection i -> return [freshM i u $ pindirect p]
        BTuple is      -> return . (:[]) $ mapM_ (\(i,n) -> freshM n u $ ptuple i p) $ zip [0..length is -1] is
        BRecord ivs    -> return . (:[]) $ mapM_ (\(src,dest) -> freshM dest u $ precord src p) ivs

    sideways _ (children -> ch) = return $ replicate (length ch - 1) iu

    -- Provenance deduction logging.
    inferWithRule :: [K3 Provenance] -> K3 Expression -> PInfM (K3 Provenance)
    inferWithRule ch e = do
      u  <- uidOf e
      (ruleTag, rp) <- infer ch e
      localLog $ T.unpack $ showPInfRule ruleTag ch rp u
      return rp

    -- Provenance computation
    infer :: [K3 Provenance] -> K3 Expression -> PInfM (String, K3 Provenance)
    infer _ e@(tag -> EConstant _) = rt "const" e ptemp
    infer _ e@(tag -> EVariable i) = varErr e (pilkupeM i >>= rt "var" e)

    infer [p] e@(tag -> ELambda i) = popClosure e >> pideleM i >> rt "lambda" e (plambda i p)

    -- Return a papply with three children: the lambda, argument, and return value provenance.
    infer [lp, argp] e@(tag -> EOperate OApp) = do
      p <- simplifyApplyM (Just e) lp argp
      rt "apply" e p

    infer [psrc] e@(tnc -> (EProject i, [esrc])) =
      case esrc @~ isEType of
        Just (EType t) ->
          case tag t of
            TCollection -> collectionMemberProvenance i psrc esrc t >>= rt "cproject" e
            TRecord ids ->
              case tnc psrc of
                (PData _, pdch) -> do
                  idx <- maybe (memErr i esrc) return $ elemIndex i ids
                  rt "rproject" e $ pproject i psrc $ Just $ pdch !! idx
                (_,_) -> rt "project" e $ pproject i psrc Nothing
            _ -> prjErr esrc
        _ -> prjErr esrc

    infer pch   e@(tag -> EIfThenElse)   = rt "if-then-else" e $ pset $ tail pch
    infer pch   e@(tag -> EOperate OSeq) = rt "seq"          e $ last pch
    infer [p]   e@(tag -> EAssign i)     = rt "assign"       e $ passign i p
    infer [_,p] e@(tag -> EOperate OSnd) = rt "send"         e $ psend p

    infer [_,lb] e@(tag -> ELetIn i) = do
      mv <- pilkupeM i >>= pmv
      void $ pideleM i
      void $ piextmM e $ pmaterialize [mv] lb
      return ("let-in", lb)

    infer [_,bb] e@(tag -> EBindAs b) = do
      mvs <- mapM pilkupeM (bindingVariables b) >>= mapM pmv
      void $ mapM_ pideleM $ bindingVariables b
      void $ piextmM e $ pmaterialize mvs bb
      return ("bind-as", bb)

    infer [_,s,n] e@(tag -> ECaseOf _) = do
      casemv <- pipopcaseM ()
      void $ piextmM e $ pset [pmaterialize [casemv] s, n]
      return ("case-of", pset [s,n])

    -- Data constructors.
    infer pch e@(tag -> ESome)       = rt "some"   e $ pdata Nothing pch
    infer pch e@(tag -> EIndirect)   = rt "ind"    e $ pdata Nothing pch
    infer pch e@(tag -> ETuple)      = rt "tuple"  e $ pdata Nothing pch
    infer pch e@(tag -> ERecord ids) = rt "record" e $ pdata (Just ids) pch

    -- Operators and untracked primitives.
    infer pch e@(tag -> EOperate op) = rt (show op) e $ derived pch
    infer pch e@(tag -> EAddress)    = rt "address" e $ derived pch

    -- TODO: ESelf
    infer _ e = inferErr e

    rt tg e p = piextmM e p >> return (tg, p)

    -- | Closure variable management
    pushClosure e@((@~ isEUID) -> Just (EUID u@(UID i))) = pilkupcM i >>= mapM_ (liftClosureVar e u)
    pushClosure e = errorM $ PT.boxToString $ [T.pack "Invalid UID on "] %+ PT.prettyLines e

    popClosure e@((@~ isEUID) -> Just (EUID (UID i))) = pilkupcM i >>= mapM_ (lowerClosureVar e)
    popClosure e = errorM $ PT.boxToString $ [T.pack "Invalid UID on "] %+ PT.prettyLines e

    liftClosureVar  _ u n = pilkupeM n >>= \p -> pideleM n >> freshM n u p
    lowerClosureVar   e n = pilkupeM n >>= \p -> unwrapClosure e p >>= \p' -> pideleM n >> piexteM n p'

    unwrapClosure _ (tag -> PBVar mv) = pilkuppM (pmvptr mv)
    unwrapClosure e p = errorM $ PT.boxToString $ [T.pack "Invalid closure variable "] %+ PT.prettyLines p
                                               %$ [T.pack "at expr:"] %$ PT.prettyLines e

    freshM i u p = void $ pifreshM i u p

    derived ch = let ntch = filter (not . isPTemporary) ch
                 in if null ntch then ptemp else pderived ntch

    iu = return ()

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = errorM $ PT.boxToString $ [T.pack "No uid found for pexprinf on "] %+ PT.prettyLines e

    pmv (tag -> PBVar mv) = return mv
    pmv p = errorM $ PT.boxToString $ [T.pack "Invalid provenance bound var: "] %$ PT.prettyLines p

    isPTemporary (tag -> PTemporary) = True
    isPTemporary _ = False

    inferErr e = errorM $ PT.boxToString $ [T.pack "Could not infer provenance for "] %$ PT.prettyLines e
    varErr e   = reasonM (\s -> T.unlines [s, T.pack "Variable access on", PT.pretty e])
    prjErr e   = errorM $ PT.boxToString $ [T.pack "Invalid type on "] %+ PT.prettyLines e

    memErr i e = errorM $ PT.boxToString $  [T.unwords $ map T.pack ["Failed to project", i, "from"]]
                                         %$ PT.prettyLines e

    showPInfRule rtag ch p euid =
      PT.boxToString $ (rpsep %+ premise) %$ separator %$ (rpsep %+ conclusion)
      where rprefix        = T.pack $ unwords [rtag, show euid]
            (rplen, rpsep) = (T.length rprefix, [T.pack $ replicate rplen ' '])
            premise        = if null ch then [T.pack "<empty>"]
                             else (PT.intersperseBoxes [T.pack " , "] $ map PT.prettyLines ch)
            premLens       = map T.length premise
            headWidth      = if null premLens then 4 else maximum premLens
            separator      = [T.append rprefix $ T.pack $ replicate headWidth '-']
            conclusion     = PT.prettyLines p


substituteProvenance :: K3 Expression -> PInfM (K3 Expression)
substituteProvenance expr = modifyTree injectProvenance expr
  where injectProvenance e = pilkupmM e >>= \p -> return (e @+ (EProvenance p))

-- | Construct the provenance of a collection field member projected from the given source.
--   TODO: this implementation currently recomputes a merged realization environment on every
--   call. This should be lifted to a cached realization environment.
collectionMemberProvenance :: Identifier -> K3 Provenance -> K3 Expression -> K3 Type -> PInfM (K3 Provenance)
collectionMemberProvenance i psrc e t =
  let annIds = namedTAnnotations $ annotations t in do
    memsEnv <- mapM pilkupasM annIds >>= return . Map.unions
    (mp, lifted) <- maybe memErr return $ Map.lookup i memsEnv
    if not lifted then attrErr else return $ pproject i psrc $ Just mp

  where memErr  = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Unknown projection of ", i, "on"]]           %+ PT.prettyLines e
        attrErr = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Invalid attribute projection of ", i, "on"]] %+ PT.prettyLines e

provOfType :: [Identifier] -> K3 Type -> PInfM (K3 Provenance)
provOfType args t | isTFunction t =
   case tnc t of
    (TForall _, [ch])      -> provOfType args ch
    (TFunction, [_, retT]) -> let a = mkArg (length args + 1)
                              in provOfType (args++[a]) retT >>= return . plambda a
    _ -> errorM $ PT.boxToString $ [T.pack "Invalid function type"] %+ PT.prettyLines t
  where mkArg i = "__arg" ++ show i

provOfType [] _   = return ptemp
provOfType args _ = return $ pderived $ map pfvar args

-- | Simplifies applies on all provenance trees attached to an expression.
simplifyProgramProvenance :: K3 Declaration -> PInfM (K3 Declaration)
simplifyProgramProvenance d = mapExpression simplifyExprProvenance d

simplifyExprProvenance :: K3 Expression -> PInfM (K3 Expression)
simplifyExprProvenance expr = modifyTree simplifyProvAnn expr
  where
    simplifyProvAnn e@(tag &&& (@~ isEProvenance) -> (EOperate OApp, Just (EProvenance p@(tag -> PApply mvOpt)))) = do
      np <- simplifyProvenance p
      return $ (e @<- (filter (not . isEProvenance) $ annotations e))
                  @+ (EProvenance $ maybe np (\mv -> pmaterialize [mv] np) mvOpt)

    simplifyProvAnn e@((@~ isEProvenance) -> Just (EProvenance p)) = do
      np <- simplifyProvenance p
      return $ (e @<- (filter (not . isEProvenance) $ annotations e)) @+ (EProvenance np)

    simplifyProvAnn e = return e

-- | Replaces PApply provenance nodes with their return value (instead of a triple of lambda, arg, rv).
--   Also, simplifies PSet and PDerived nodes on these applies after substitution has been performed.
simplifyProvenance :: K3 Provenance -> PInfM (K3 Provenance)
simplifyProvenance p = modifyTree simplify p
  where simplify (tnc -> (PApply _, [_,_,r])) = return r
        -- Rebuilding PSet and PDerived will filter all temporaries.
        simplify (tnc -> (PSet, ch))     = return $ pset ch
        simplify (tnc -> (PDerived, ch)) = return $ pderived ch
        simplify p' = return p'


{- Provenance environment pretty printing -}
instance Pretty PIEnv where
  prettyLines (PIEnv c p e a cl ep _) =
    [T.pack $ "PCnt: " ++ show c] ++
    [T.pack "PEnv: "  ]  %$ (PT.indent 2 $ PT.prettyLines e)  ++
    [T.pack "PPEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines p)  ++
    [T.pack "PAEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines a)  ++
    [T.pack "PLCEnv: "]  %$ (PT.indent 2 $ PT.prettyLines cl) ++
    [T.pack "EPMap: " ]  %$ (PT.indent 2 $ PT.prettyLines ep)

instance Pretty (IntMap (K3 Provenance)) where
  prettyLines pp = IntMap.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] pp

instance Pretty PEnv where
  prettyLines pe = Map.foldlWithKey (\acc k v -> acc ++ prettyFrame k v) [] pe
    where prettyFrame k v = concatMap prettyPair $ flip zip v $ replicate (length v) k

instance Pretty PAEnv where
  prettyLines pa = Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] pa

instance Pretty PMEnv where
  prettyLines pm = Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k, fst v)) [] pm

instance Pretty PLCEnv where
  prettyLines plc = IntMap.foldlWithKey (\acc k v -> acc ++ [T.pack $ show k ++ " => " ++ show v]) [] plc

prettyPair :: (Show a, Pretty b) => (a,b) -> [Text]
prettyPair (a,b) = [T.pack $ show a ++ " => "] %+ PT.prettyLines b
