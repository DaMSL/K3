{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}

-- | Effect analysis for K3 programs.
module Language.K3.Analysis.SEffects.Inference where

import Control.Applicative
import Control.Arrow hiding ( left )
import Control.Monad.State
import Control.Monad.Trans.Except
import Data.Functor.Identity

import Data.Binary ( Binary )
import Data.Serialize ( Serialize )

import Data.Either
import Data.List
import Data.Maybe
import Data.Tree
import Data.Monoid

import Data.IntMap           ( IntMap )
import qualified Data.IntMap as IntMap

import Debug.Trace

import GHC.Generics ( Generic )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Analysis.Data.BindingEnv ( BindingEnv, BindingStackEnv )
import qualified Language.K3.Analysis.Data.BindingEnv as BEnv

import Language.K3.Analysis.Core

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Constructors
import Language.K3.Analysis.Provenance.Inference ( PIEnv, piBindings )

import Language.K3.Analysis.SEffects.Core
import Language.K3.Analysis.SEffects.Constructors

import Language.K3.Utils.Logger

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
type FEnv = BindingStackEnv (K3 Effect)

-- | An effect "pointer" environment for bound effect variables.
type FPEnv = IntMap (K3 Effect)

-- | A provenance bindings environment, built from existing provenance annotations.
type FPBEnv = BindingStackEnv (K3 Provenance)

-- | A provenance "pointer" environment
type FPPEnv = IntMap (K3 Provenance)

-- | A effect bindings environment for annotations,
--   indexed by annotation and attribute name.
type FAEnv = BindingEnv FMEnv
type FMEnv = BindingEnv (K3 Effect, Bool)

-- | A lambda closure environment: ELambda UID => identifiers of closure variables.
type FLCEnv = IntMap [Identifier]

-- | A mapping from expression ids to a pair of effect and effect structure.
type EFMap = IntMap (K3 Effect, K3 Effect)

data EffectErrorCtxt = EffectErrorCtxt { ftoplevelExpr :: Maybe (K3 Expression)
                                       , fcurrentExpr  :: Maybe (K3 Expression) }
                      deriving (Eq, Read, Show, Generic)

instance Monoid EffectErrorCtxt where
  mempty = EffectErrorCtxt Nothing Nothing
  mappend (EffectErrorCtxt t c) (EffectErrorCtxt t' c') = EffectErrorCtxt (t <|> t') (c <|> c')

-- | An effect inference environment.
data FIEnv = FIEnv {
               fcnt     :: ParGenSymS,
               fpenv    :: FPEnv,
               fenv     :: FEnv,
               faenv    :: FAEnv,
               fpbenv   :: FPBEnv,
               fppenv   :: FPPEnv,
               flcenv   :: FLCEnv,
               efmap    :: EFMap,
               fcase    :: [(FMatVar, PMatVar)],  -- Temporary storage stack for case variables.
               ferrctxt :: EffectErrorCtxt
            }
            deriving (Eq, Read, Show, Generic)

mergeFIEnv :: Maybe Identifier -> FIEnv -> FIEnv -> FIEnv
mergeFIEnv d agg new =
  FIEnv { fcnt     = rewindsymS (fcnt agg) (fcnt new)
        , fpenv    = fpenv agg <> fpenv new
        , fenv     = maybe (fenv agg)   (\d' -> BEnv.mergeIntoWith [d'] (fenv agg)   (fenv new))   d
        , faenv    = maybe (faenv agg)  (\d' -> BEnv.mergeIntoWith [d'] (faenv agg)  (faenv new))  d
        , fpbenv   = maybe (fpbenv agg) (\d' -> BEnv.mergeIntoWith [d'] (fpbenv agg) (fpbenv new)) d
        , fppenv   = fppenv agg <> fppenv new
        , flcenv   = flcenv agg <> flcenv new
        , efmap    = efmap agg <> efmap new
        , fcase    = fcase agg <> fcase new
        , ferrctxt = EffectErrorCtxt { ftoplevelExpr = ftoplevelExpr (ferrctxt agg) <|> ftoplevelExpr (ferrctxt new)
                                     , fcurrentExpr = fcurrentExpr (ferrctxt agg) <|> fcurrentExpr (ferrctxt new)
                                     }
        }

-- | The effects inference monad
type FInfM = ExceptT Text (State FIEnv)

-- | User-defined external inference function.
type ExtInferF a = K3 Effect -> a -> FIEnv -> K3 Effect

{- Effect instances -}
instance Binary EffectErrorCtxt
instance Binary FIEnv

instance Serialize EffectErrorCtxt
instance Serialize FIEnv

{- Data.Text helpers -}
mkErr :: String -> Except Text a
mkErr msg = throwE $ T.pack msg

mkErrP :: PT.Pretty a => String -> a -> Except Text b
mkErrP msg a = throwE $ T.unlines [T.pack msg, PT.pretty a]

{- FEnv helpers -}
fenv0 :: FEnv
fenv0 = BEnv.empty

flkup :: FEnv -> Identifier -> Except Text (K3 Effect)
flkup env x = BEnv.slookup env x

flkupAll :: FEnv -> Identifier -> Except Text [K3 Effect]
flkupAll env x = BEnv.lookup env x

fext :: FEnv -> Identifier -> K3 Effect -> FEnv
fext env x f = BEnv.push env x f

fsetAll :: FEnv -> Identifier -> [K3 Effect] -> FEnv
fsetAll env x l = BEnv.set env x l

fdel :: FEnv -> Identifier -> FEnv
fdel env x = BEnv.pop env x

fmem :: FEnv -> Identifier -> Except Text Bool
fmem env x = BEnv.member env x


{- FPEnv helpers -}
fpenv0 :: FPEnv
fpenv0 = IntMap.empty

fplkup :: FPEnv -> Int -> Except Text (K3 Effect)
fplkup env x = maybe err return $ IntMap.lookup x env
  where err = mkErrP msg env
        msg = "Unbound variable in effect pointer environment: " ++ show x

fpext :: FPEnv -> Int -> K3 Effect -> FPEnv
fpext env x p = IntMap.insert x p env

fpdel :: FPEnv -> Int -> FPEnv
fpdel env x = IntMap.delete x env


{- FAEnv helpers -}
faenv0 :: FAEnv
faenv0 = BEnv.empty

fmenv0 :: FMEnv
fmenv0 = BEnv.empty

falkup :: FAEnv -> Identifier -> Identifier -> Except Text (K3 Effect)
falkup env x y = BEnv.lookup env x >>= \menv -> BEnv.lookup menv y >>= return . fst

faext :: FAEnv -> Identifier -> Identifier -> K3 Effect -> Bool -> FAEnv
faext env x y fOpt l = BEnv.pushWith env BEnv.union x (BEnv.fromList [(y,(fOpt,l))])

falkups :: FAEnv -> Identifier -> Except Text FMEnv
falkups env x = BEnv.lookup env x

faexts :: FAEnv -> Identifier -> FMEnv -> FAEnv
faexts env x af = BEnv.pushWith env BEnv.union x af


{- EFMap helpers -}
efmap0 :: EFMap
efmap0 = IntMap.empty

eflkup :: EFMap -> K3 Expression -> Except Text (K3 Effect, K3 Effect)
eflkup efm e@((@~ isEUID) -> Just (EUID (UID uid))) = maybe lookupErr return $ IntMap.lookup uid efm
  where lookupErr = throwE $ PT.boxToString $ msg %$ PT.prettyLines e
        msg = [T.unwords $ map T.pack ["No effects found for", show uid]]

eflkup _ e = throwE $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e

efext :: EFMap -> K3 Expression -> K3 Effect -> K3 Effect -> Except Text EFMap
efext efm e f s = case e @~ isEUID of
  Just (EUID (UID i)) -> return $ IntMap.insert i (f,s) efm
  _ -> throwE $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e


{- FPBEnv helpers -}
fpbenv0 :: FPBEnv
fpbenv0 = BEnv.empty

fpblkup :: FPBEnv -> Identifier -> Except Text (K3 Provenance)
fpblkup env x = BEnv.slookup env x

fpbext :: FPBEnv -> Identifier -> K3 Provenance -> FPBEnv
fpbext env x p = BEnv.push env x p

fpbdel :: FPBEnv -> Identifier -> FPBEnv
fpbdel env x =  BEnv.pop env x


{- FPPEnv helpers -}
fppenv0 :: FPPEnv
fppenv0 = IntMap.empty

fpplkup :: FPPEnv -> Int -> Except Text (K3 Provenance)
fpplkup env x = maybe err return $ IntMap.lookup x env
  where err = mkErrP msg env
        msg = "Unbound pointer in lineage environment during effects: " ++ show x


{- FLCEnv helpers -}
flcenv0 :: FLCEnv
flcenv0 = IntMap.empty

flclkup :: FLCEnv -> Int -> Except Text [Identifier]
flclkup env x = maybe err return $ IntMap.lookup x env
  where err = mkErr $ "Unbound UID in closure environment: " ++ show x

{- Error context helpers -}
err0 :: EffectErrorCtxt
err0 = EffectErrorCtxt Nothing Nothing

{- FIEnv helpers -}
fienv0 :: Maybe ParGenSymS -> FPPEnv -> FLCEnv -> FIEnv
fienv0 symSOpt ppenv lcenv =
  FIEnv (maybe contigsymS id symSOpt) fpenv0 fenv0 faenv0 fpbenv0 ppenv lcenv efmap0 [] err0

-- | Modifiers.
mfiee :: (FEnv -> FEnv) -> FIEnv -> FIEnv
mfiee f env = env {fenv = f $ fenv env}

mfiep :: (FPEnv -> FPEnv) -> FIEnv -> FIEnv
mfiep f env = env {fpenv = f $ fpenv env}

filkupe :: FIEnv -> Identifier -> Except Text (K3 Effect)
filkupe env x = flkup (fenv env) x

filkupalle :: FIEnv -> Identifier -> Except Text [K3 Effect]
filkupalle env x = flkupAll (fenv env) x

fiexte :: FIEnv -> Identifier -> K3 Effect -> FIEnv
fiexte env x f = env {fenv=fext (fenv env) x f}

fisetalle :: FIEnv -> Identifier -> [K3 Effect] -> FIEnv
fisetalle env x fl = env {fenv=fsetAll (fenv env) x fl}

fidele :: FIEnv -> Identifier -> FIEnv
fidele env i = env {fenv=fdel (fenv env) i}

fimeme :: FIEnv -> Identifier -> Except Text Bool
fimeme env i = fmem (fenv env) i

filkupep :: FIEnv -> Identifier -> Except Text (K3 Provenance)
filkupep env x = fpblkup (fpbenv env) x

fiextep :: FIEnv -> Identifier -> K3 Provenance -> FIEnv
fiextep env x f = env {fpbenv=fpbext (fpbenv env) x f}

fidelep :: FIEnv -> Identifier -> FIEnv
fidelep env i = env {fpbenv=fpbdel (fpbenv env) i}

filkupp :: FIEnv -> Int -> Except Text (K3 Effect)
filkupp env x = fplkup (fpenv env) x

fiextp :: FIEnv -> Int -> K3 Effect -> FIEnv
fiextp env x f = env {fpenv=fpext (fpenv env) x f}

fidelp :: FIEnv -> Int -> FIEnv
fidelp env i = env {fpenv=fpdel (fpenv env) i}

filkupa :: FIEnv -> Identifier -> Identifier -> Except Text (K3 Effect)
filkupa env x y = falkup (faenv env) x y

filkupas :: FIEnv -> Identifier -> Except Text FMEnv
filkupas env x = falkups (faenv env) x

fiextas :: FIEnv -> Identifier -> FMEnv -> FIEnv
fiextas env x f = env {faenv=faexts (faenv env) x f}

filkupm :: FIEnv -> K3 Expression -> Except Text (K3 Effect, K3 Effect)
filkupm env e = eflkup (efmap env) e

fiextm :: FIEnv -> K3 Expression -> K3 Effect -> K3 Effect -> Except Text FIEnv
fiextm env e f s = efext (efmap env) e f s >>= \nep -> return ( env {efmap=nep} )

filkuppp :: FIEnv -> Int -> Except Text (K3 Provenance)
filkuppp env x = fpplkup (fppenv env) x

filkupc :: FIEnv -> Int -> Except Text [Identifier]
filkupc env i = flclkup (flcenv env) i

fipopcase :: FIEnv -> Except Text ((FMatVar, PMatVar), FIEnv)
fipopcase env = case fcase env of
  [] -> throwE $ T.pack "Uninitialized case matvar"
  h:t -> return (h, env {fcase=t})

fipushcase :: FIEnv -> (FMatVar, PMatVar) -> FIEnv
fipushcase env mvs = env {fcase=mvs:(fcase env)}

fierrtle :: FIEnv -> Maybe (K3 Expression)
fierrtle = ftoplevelExpr . ferrctxt

fierrce :: FIEnv -> Maybe (K3 Expression)
fierrce = fcurrentExpr . ferrctxt

fiseterrtle :: FIEnv -> Maybe (K3 Expression) -> FIEnv
fiseterrtle env eOpt = env {ferrctxt = (ferrctxt env) {ftoplevelExpr = eOpt}}

fiseterrce :: FIEnv -> Maybe (K3 Expression) -> FIEnv
fiseterrce env eOpt = env {ferrctxt = (ferrctxt env) {fcurrentExpr = eOpt}}


{- Fresh pointer and binding construction. -}

-- | Self-referential effect pointer construction
fifreshfp :: FIEnv -> Identifier -> UID -> (K3 Effect, FIEnv)
fifreshfp fienv i u =
  let (nfcnt,j) = gensym $ fcnt fienv
      f = fbvar $ FMatVar i u j
  in (f, fiextp (fienv {fcnt=nfcnt}) j f)

fifreshbp :: FIEnv -> Identifier -> UID -> K3 Effect -> (K3 Effect, FIEnv)
fifreshbp fienv i u f =
  let (nfcnt,j) = gensym $ fcnt fienv
      f' = fbvar $ FMatVar i u j
  in (f', fiextp (fienv {fcnt=nfcnt}) j f)

-- | Self-referential effect pointer construction
--   This adds a new named pointer to both the named and pointer environments.
fifreshs :: FIEnv -> Identifier -> UID -> (K3 Effect, FIEnv)
fifreshs fienv n u =
  let (f, nenv) = fifreshfp fienv n u in (f, fiexte nenv n f)

-- | Effect linked pointer construction.
fifresh :: FIEnv -> Identifier -> UID -> K3 Effect -> (K3 Effect, FIEnv)
fifresh fienv n u f =
  let (f', nenv) = fifreshbp fienv n u f in (f', fiexte nenv n f')

fifreshAs :: FIEnv -> Identifier -> [(Identifier, UID, Bool, Bool)] -> (FMEnv, FIEnv)
fifreshAs fienv n memN =
  let mkMemF lacc l f                = lacc++[(f,l)]
      extMemF (lacc,eacc) (i,u,l,fn) = if fn then first (mkMemF lacc l) $ fifreshfp eacc i u
                                             else (lacc++[(fnone,l)], eacc)
      (memF, nfienv)                 = foldl extMemF ([], fienv) memN
      memNF                          = BEnv.fromList $ zip (map (\(a,_,_,_) -> a) memN) memF
  in (memNF, fiextas nfienv n memNF)


{- Effect pointer helpers -}

-- | Retrieves the effect value referenced by a named pointer
fiload :: FIEnv -> Identifier -> Except Text (K3 Effect)
fiload fienv n = do
  f <- filkupe fienv n
  case tag f of
    FBVar mv -> filkupp fienv $ fmvptr mv
    _ -> throwE $ PT.boxToString $ [T.pack "Invalid load on pointer"] %$ PT.prettyLines f

-- | Sets the effect value referenced by a named pointer
fistore :: FIEnv -> Identifier -> UID -> K3 Effect -> Except Text FIEnv
fistore fienv n u f = do
  f' <- filkupe fienv n
  case tag f' of
    FBVar mv | (fmvn mv, fmvloc mv) == (n,u) -> return $ fiextp fienv (fmvptr mv) f
    _ -> throwE $ PT.boxToString $ [T.pack "Invalid store on pointer"] %$ PT.prettyLines f'

fistorea :: FIEnv -> Identifier -> [(Identifier, UID, K3 Effect, Bool)] -> Except Text FIEnv
fistorea fienv n memF = do
  fmenv <- filkupas fienv n
  foldM (storemem fmenv) fienv memF

  where storemem fmenv eacc (i,u,f,_) = BEnv.lookup fmenv i >>= \(f',_) -> store eacc i u f' f
        store eacc i u (tag -> FBVar mv) f
          | (fmvn mv, fmvloc mv) == (i,u) = return $ fiextp eacc (fmvptr mv) f
        store eacc _ _ _ _ = return eacc

        invalidMem i = mkErr $ "Invalid store on annotation member" ++ i

-- | Traverses all pointers until reaching a non-pointer.
--   This function stops on any cycles detected.
fichase :: FIEnv -> K3 Effect -> Except Text (K3 Effect)
fichase fienv cf = aux [] cf
  where aux path f@(tag -> FBVar (fmvptr -> i)) | i `elem` path = return f
                                                | otherwise = filkupp fienv i >>= aux (i:path)
        aux _ f = return f

{- Substitution helpers -}
chaseProvenance :: K3 Provenance -> Except Text (K3 Provenance)
chaseProvenance (tnc -> (PApply _, [_,_,r])) = chaseProvenance r
chaseProvenance (tnc -> (PMaterialize _, [r])) = chaseProvenance r
chaseProvenance p = return p

-- Capture-avoiding substitution of any free variable with the given identifier.
fisub :: FIEnv -> Maybe (ExtInferF a, a) -> Bool -> Identifier -> K3 Effect -> K3 Effect -> K3 Provenance
      -> Except Text (K3 Effect, FIEnv)
fisub fienv extInfOpt asStructure i df sf p = do
  (renv, _, rf) <- debugAcyclicSub fienv sf $ acyclicSub fienv asStructure emptyPtrSubs [] sf
  return (rf, renv)

  where
    extInferM env f = maybe (return f) (\(infF, infSt) -> return $ infF f infSt env) extInfOpt

    acyclicSub env _ subs _ (tag -> FFVar j) | i == j = return (env, subs, df)

    acyclicSub env asStruc subs path f@(tag -> FBVar mv@(fmvptr -> j))
      | j `elem` path   = return (env, subs, f)
      | isPtrSub subs j = getPtrSub subs j >>= return . (env, subs,)
      | otherwise       = filkupp fienv j >>= subPtrIfDifferent env asStruc subs (j:path) mv

    acyclicSub env _ subs _ (tag -> FRead (tag -> PFVar j _)) | j == i = do
      p' <- chaseProvenance p
      f' <- extInferM env $ fread p'
      return (env, subs, f')

    acyclicSub env _ subs _ (tag -> FWrite (tag -> PFVar j _)) | j == i = do
      p' <- chaseProvenance p
      f' <- extInferM env $ fwrite p'
      return (env, subs, f')

    -- TODO: we can short-circuit descending into the body if we
    -- are willing to stash the expression uid in a FLambda, using
    -- this uid to lookup i's presence in our precomputed closures.
    acyclicSub env _ subs _ f@(tag -> FLambda j) | i == j = return (env, subs, f)

    -- Avoid descent into materialization points of shadowed variables.
    acyclicSub env _ subs _ f@(tag -> FScope mvs) | i `elem` map fmvn mvs = return (env, subs, f)

    -- Handle higher-order function application (two-place FApply nodes) where a FFVar
    -- in the lambda position is replaced by a FLambda
    acyclicSub env asStructure subs path f@(tnc -> (FApply Nothing, [lrf@(tag -> FFVar _), argrf])) = do
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) [(lrf, True), (argrf, True)]
      case nch of
        [lrf', argrf'] -> do
          (nef, nrf, nenv') <- simplifyApply nenv extInfOpt True Nothing [] lrf' argrf'
          let nrf' = if asStructure then nrf else case tnc nrf of { (FApply Nothing, [_,_]) -> nrf; _ -> nef }
          return $ debugFFApp lrf argrf lrf' argrf' nef nrf $ (nenv', nsubs, nrf')
        _ -> appSubErr f

      where
        debugFFApp a b c d e f r = if True then r else
          flip trace r (T.unpack $ PT.boxToString $ [T.pack $ "Sub FFVar app " ++ i]
                                %$ PT.prettyLines a
                                %$ PT.prettyLines b
                                %$ PT.prettyLines c
                                %$ PT.prettyLines d
                                %$ PT.prettyLines e
                                %$ PT.prettyLines f)


    acyclicSub env asStruc subs path n@(Node _ ch) = do
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) $ zip ch $ subAsStructure asStruc n
      return (nenv, nsubs, replaceCh n nch)

    rcr path (eacc,sacc,chacc) (c, asStruc) = acyclicSub eacc asStruc sacc path c >>= \(e,s,nc) -> return (e,s,chacc++[nc])

    subAsStructure _ f@(tnc -> (FData _, ch))           = replicate (length ch) True
    subAsStructure _ f@(tnc -> (FScope _, [a,b,c,d]))   = [False, False, False, True]
    subAsStructure _ f@(tnc -> (FLambda _, [a,b,c]))    = [False, False, True]
    subAsStructure _ f@(tnc -> (FApply Nothing, [a,b])) = [True, True]
    subAsStructure _ f@(tnc -> (FApply _, [a,b,c,d,e])) = [True, True, False, False, True]
    subAsStructure _ f@(tnc -> (FApply _, [a]))         = [True]
    subAsStructure s f@(tnc -> (FSet, ch))              = replicate (length ch) s
    subAsStructure _ f@(tnc -> (FSeq, ch))              = replicate (length ch) False
    subAsStructure _ f@(tnc -> (FLoop, ch))             = replicate (length ch) False
    subAsStructure _ _ = []

    subPtrIfDifferent env asStruc subs path mv f = do
      (nenv, nsubs, nf) <- acyclicSub env asStruc subs path f
      if f == nf then return (nenv, nsubs, fbvar mv) else addPtrSub nenv nsubs mv nf

    -- Pointer substitutions, as a map of old PPtr => new PMatVar
    emptyPtrSubs   = IntMap.empty
    isPtrSub  fs j = IntMap.member j fs
    getPtrSub fs j = maybe (lookupErr j) (return . fbvar) $ IntMap.lookup j fs
    addPtrSub env fs mv f =
      let (mvi, mviE) = (fmvn mv, runExcept $ filkupalle env $ fmvn mv)
          (f', env') = fifreshbp env mvi (fmvloc mv) f
          (nf, nenv) = case mviE of
                         Left _  -> (f', env')
                         Right l -> (f',) $ fisetalle env' mvi $ flip map l $ \ef -> case tag ef of
                           FBVar imv | imv == mv -> f'
                           _ -> ef
      in
      case tag nf of
        FBVar nmv -> return (nenv, IntMap.insert (fmvptr mv) nmv fs, nf)
        _ -> freshErr nf

    debugAcyclicSub denv f r = if True then r else
      flip trace r $ T.unpack $ PT.boxToString $ [T.pack "acyclicSub"]
                                              %$ PT.prettyLines denv
                                              %$ PT.prettyLines f

    lookupErr j = throwE $ T.pack $ "Could not find pointer substitution for " ++ show j
    freshErr  f = throwE $ PT.boxToString $ [T.pack "Invalid fresh FBVar result "] %$ PT.prettyLines f
    appSubErr f = throwE $ PT.boxToString $ [T.pack "Invalid apply substitution at: "] %$ PT.prettyLines f


{- Apply simplification -}
chaseAppArg :: K3 Effect -> Except Text (K3 Effect)
chaseAppArg (tnc -> (FApply _, [sf])) = chaseAppArg sf
chaseAppArg (tnc -> (FApply _, [_,_,_,_,sf])) = chaseAppArg sf
chaseAppArg (tnc -> (FScope _, [_,_,_,sf])) = chaseAppArg sf
chaseAppArg sf = return sf

chaseLambda :: FIEnv -> Maybe (ExtInferF a, a) -> [Text] -> [FPtr] -> K3 Effect -> Except Text [K3 Effect]
chaseLambda env extInfOpt msg path f@(tnc -> (FApply _, [lf, af])) = do
  (_, nrf, nenv) <- simplifyApply env extInfOpt False Nothing [] lf af
  if nrf == f then return [f] else chaseLambda nenv extInfOpt msg path nrf

chaseLambda env _ msg path f = chaseApplied env msg path f
  where chaseApplied _ _ _ f@(tag -> FLambda _) = return [f]
        chaseApplied _ _ _ f@(tnc -> (FApply _, [_,_])) = return [f]
        chaseApplied _ _ _ f@(tag -> FFVar _)   = return [f]
        chaseApplied _ _ _ f@(tag -> FNone)     = return [f]
          -- For partial application of externals and forward declarations.

        chaseApplied env msg path f@(tag -> FBVar (fmvptr -> i))
          | i `elem` path = return [f]
          | otherwise     = fichase env f >>= chaseApplied env msg (i:path)

        chaseApplied env msg path (tnc -> (FApply _, [sf]))         = chaseApplied env msg path sf
        chaseApplied env msg path (tnc -> (FApply _, [_,_,_,_,sf])) = chaseApplied env msg path sf
        chaseApplied env msg path (tnc -> (FSet, rfl)) = mapM (chaseApplied env msg path) rfl >>= return . concat
        chaseApplied _ msg _ f = throwE $ PT.boxToString $ fErr f %$ (if null msg then [] else [T.pack "on"]) %$ msg
          where fErr f' = [T.pack "Invalid application or lambda: "] %$ PT.prettyLines f'

simplifyApply :: FIEnv -> Maybe (ExtInferF a, a) -> Bool -> Maybe (K3 Expression) -> [Maybe (K3 Effect)] -> K3 Effect -> K3 Effect
              -> Except Text (K3 Effect, K3 Effect, FIEnv)
simplifyApply fienv extInfOpt defer eOpt ef lrf arf = do
  upOpt            <- uidP eOpt
  arf'             <- chaseAppArg arf
  manyLrf          <- chaseLambda fienv extInfOpt (maybe [] PT.prettyLines eOpt) [] lrf
  (manyLerf, nenv) <- foldM (doSimplify upOpt arf') ([], fienv) manyLrf

  case manyLerf of
    []          -> applyLambdaErr "return" lrf
    [(nef,nrf)] -> return (nef, nrf, nenv)
    _           -> let (efl, rfl) = unzip manyLerf
                   in return (fset efl, fset rfl, nenv)

  where
    doSimplify upOpt arf' (facc, eacc) lrf' =
      if defer && (case tag arf' of {FFVar _ -> True; _ -> False})
      then let appef = fromJust $ fexec ef
           in return (facc ++ [(appef, fapplyExt lrf arf)], eacc)
      else
      case tnc lrf' of
        (FLambda i, [_,bef,brf]) -> case upOpt of
          Just (uid, p) -> do
            let (ifbv, neacc) = fifreshbp eacc i uid arf'
            imv  <- fmv ifbv
            (nbef,n2eacc) <- fisub neacc  extInfOpt False i ifbv bef p
            (nbrf,n3eacc) <- fisub n2eacc extInfOpt True  i ifbv brf p
            let appef = fromJust $ fexec $ ef ++ [Just nbef]
            let apprf = fapplyRT (Just imv) nbrf
            return (facc++[(appef, apprf)], n3eacc)

          Nothing -> do
            -- This case handles application of external lambdas as direct inlining
            -- of the argument effect structure without lifting.
            -- Also, we replace any provenance symbols for the argument with temporaries
            -- since we have no further information from effect signatures.
            (nbef,neacc)  <- fisub eacc  extInfOpt False i arf' bef ptemp
            (nbrf,n2eacc) <- fisub neacc extInfOpt True  i arf' brf ptemp
            let appef = fromJust $ fexec $ ef ++ [Just nbef]
            let apprf = fapplyRT Nothing nbrf
            return (facc++[(appef,apprf)], n2eacc)

        -- Handle recursive functions and forward declarations by using an opaque return value.
        (FBVar _, _) ->
          let appef = fromJust $ fexec ef
              apprf = fapplyRT Nothing fnone
          in return (facc ++ [(appef, apprf)], eacc)

        -- Handle higher-order and external functions as an unsimplified apply.
        (FFVar _, _) -> let appef = fromJust $ fexec ef
                        in return (facc ++ [(appef, fapplyExt lrf arf)], eacc)

        (FApply _, [_,_]) -> let appef = fromJust $ fexec ef
                             in return (facc ++ [(appef, fapplyExt lrf arf)], eacc)

        -- For partial application of externals and forward declarations.
        (FNone, _) ->
          let appef = fromJust $ fexec ef
              apprf = fapplyRT Nothing fnone
          in return (facc ++ [(appef, apprf)], eacc)

        _ -> applyLambdaErr "match" lrf

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = throwE $ PT.boxToString $ [T.pack "No uid found for fsimplifyapp on "] %+ PT.prettyLines e

    provOf  e = maybe (provErr e) (\case {(EProvenance p) ->  return p; _ -> provErr e}) $ e @~ isEProvenance
    provErr e = throwE $ PT.boxToString $ [T.pack "No provenance found on "] %+ PT.prettyLines e

    uidP eOpt' = case eOpt' of
      Nothing -> return Nothing
      Just e  -> (\a bOpt -> bOpt >>= return . (a,)) <$> uidOf e <*> argP e

    argP e = provOf e >>= \case
                            (tag -> PApply Nothing)    -> return Nothing
                            (tag -> PApply (Just mv))  -> return $ Just $ pbvar mv
                            (tag -> PMaterialize [mv]) -> return $ Just $ pbvar mv
                            _ -> argPErr e

    fexec ef' = Just $ fseq $ catMaybes ef'

    fmv (tag -> FBVar mv) = return mv
    fmv f = throwE $ PT.boxToString $ [T.pack "Invalid effect bound var: "] %$ PT.prettyLines f

    exprErr = maybe [] (\e -> PT.prettyLines e) eOpt
    argPErr e = throwE $ PT.boxToString $ [T.pack "No argument provenance found on:"] %$ PT.prettyLines e

    applyLambdaErr :: forall a. String -> K3 Effect -> Except Text a
    applyLambdaErr tg f = throwE $ PT.boxToString $ [T.pack $ "Invalid apply lambda effect (" ++ tg ++ "): "]
                             %$ exprErr %$ [T.pack "Effect:"] %$ PT.prettyLines f

simplifyApplyM :: Maybe (ExtInferF a, a) -> Bool -> Maybe (K3 Expression) -> [Maybe (K3 Effect)] -> K3 Effect -> K3 Effect
               -> FInfM (K3 Effect, K3 Effect)
simplifyApplyM extInfOpt defer eOpt ef lrf argrf = do
  env <- get
  (nef, nrf, nenv) <- liftExceptM $ simplifyApply env extInfOpt defer eOpt ef lrf argrf
  void $ put nenv
  return (nef, nrf)


{- FInfM helpers -}

runFInfM :: FIEnv -> FInfM a -> (Either Text a, FIEnv)
runFInfM env m = flip runState env $ runExceptT m

runFInfE :: FIEnv -> FInfM a -> Either Text (a, FIEnv)
runFInfE env m = let (a,b) = runFInfM env m in a >>= return . (,b)

runFInfES :: FIEnv -> FInfM a -> Either String (a, FIEnv)
runFInfES env m = either (Left . T.unpack) Right $ runFInfE env m

reasonM :: (Text -> Text) -> FInfM a -> FInfM a
reasonM errf = mapExceptT $ \m -> m >>= \case
  Left  err -> get >>= \env -> (return . Left $ errf $ T.unlines [err, T.pack "Effect environment:", PT.pretty env])
  Right r   -> return $ Right r

errorM :: Text -> FInfM a
errorM msg = reasonM id $ throwE msg

liftExceptM :: Except Text a -> FInfM a
liftExceptM = mapExceptT contextualizeErr
  where contextualizeErr (runIdentity -> Left msg) = do
          ctxt <- get >>= return . ferrctxt
          let tle = maybe [T.pack "<nothing>"] PT.prettyLines $ ftoplevelExpr ctxt
          let cre = maybe [T.pack "<nothing>"] PT.prettyLines $ fcurrentExpr ctxt
          let ctxtmsg = PT.boxToString $ [msg] %$ [T.pack "On"] %$ cre %$ [T.pack "Toplevel"] %$ tle
          return $ Left ctxtmsg

        contextualizeErr (runIdentity -> Right r) = return $ Right r

liftEitherM :: Either Text a -> FInfM a
liftEitherM = either throwE return

fifreshbpM :: Identifier -> UID -> K3 Effect -> FInfM (K3 Effect)
fifreshbpM n u f = get >>= return . (\env -> fifreshbp env n u f) >>= \(f',nenv) -> put nenv >> return f'

fifreshsM :: Identifier -> UID -> FInfM (K3 Effect)
fifreshsM n u = get >>= return . (\env -> fifreshs env n u) >>= \(f,env) -> put env >> return f

fifreshM :: Identifier -> UID -> K3 Effect -> FInfM (K3 Effect)
fifreshM n u f = get >>= return . (\env -> fifresh env n u f) >>= \(f',env) -> put env >> return f'

fifreshAsM :: Identifier -> [(Identifier, UID, Bool, Bool)] -> FInfM FMEnv
fifreshAsM n mems = get >>= return . (\env -> fifreshAs env n mems) >>= \(f,env) -> put env >> return f

filkupeM :: Identifier -> FInfM (K3 Effect)
filkupeM n = get >>= liftExceptM . flip filkupe n

fiexteM :: Identifier -> K3 Effect -> FInfM ()
fiexteM n f = get >>= \env -> return (fiexte env n f) >>= put

fideleM :: Identifier -> FInfM ()
fideleM n = get >>= \env -> return (fidele env n) >>= put

fimemeM :: Identifier -> FInfM Bool
fimemeM n = get >>= liftExceptM . flip fimeme n

filkupepM :: Identifier -> FInfM (K3 Provenance)
filkupepM n = get >>= liftExceptM . flip filkupep n

fiextepM :: Identifier -> K3 Provenance -> FInfM ()
fiextepM n f = get >>= \env -> return (fiextep env n f) >>= put

fidelepM :: Identifier -> FInfM ()
fidelepM n = get >>= \env -> return (fidelep env n) >>= put

filkupaM :: Identifier -> Identifier -> FInfM (K3 Effect)
filkupaM n m = get >>= liftExceptM . (\env -> filkupa env n m)

filkupasM :: Identifier -> FInfM FMEnv
filkupasM n = get >>= liftExceptM . flip filkupas n

filkupmM :: K3 Expression -> FInfM (K3 Effect, K3 Effect)
filkupmM e = get >>= liftExceptM . flip filkupm e

fiextmM :: K3 Expression -> K3 Effect -> K3 Effect -> FInfM ()
fiextmM e f s = get >>= liftExceptM . (\env -> fiextm env e f s) >>= put

filoadM :: Identifier -> FInfM (K3 Effect)
filoadM n = get >>= liftExceptM . flip fiload n

fistoreM :: Identifier -> UID -> K3 Effect -> FInfM ()
fistoreM n u f = get >>= liftExceptM . (\env -> fistore env n u f) >>= put

fistoreaM :: Identifier -> [(Identifier, UID, K3 Effect, Bool)] -> FInfM ()
fistoreaM n memF = get >>= liftExceptM . (\env -> fistorea env n memF) >>= put

fichaseM :: K3 Effect -> FInfM (K3 Effect)
fichaseM f = get >>= liftExceptM . flip fichase f

fisubM :: Maybe (ExtInferF a, a) -> Bool -> Identifier -> K3 Effect -> K3 Effect -> K3 Provenance -> FInfM (K3 Effect)
fisubM extInfOpt asStructure i ref f p = get >>= liftExceptM . (\env -> fisub env extInfOpt asStructure i ref f p) >>= \(f', nenv) -> put nenv >> return f'

filkupppM :: Int -> FInfM (K3 Provenance)
filkupppM n = get >>= liftExceptM . flip filkuppp n

filkupcM :: Int -> FInfM [Identifier]
filkupcM n = get >>= liftExceptM . flip filkupc n

fipopcaseM :: () -> FInfM (FMatVar, PMatVar)
fipopcaseM _ = get >>= liftExceptM . fipopcase >>= \(cmvs,env) -> put env >> return cmvs

fipushcaseM :: (FMatVar, PMatVar) -> FInfM ()
fipushcaseM mv = get >>= return . flip fipushcase mv >>= put

fierrtleM :: FInfM (Maybe (K3 Expression))
fierrtleM = get >>= return . fierrtle

fierrceM :: FInfM (Maybe (K3 Expression))
fierrceM = get >>= return . fierrce

fiseterrtleM :: Maybe (K3 Expression) -> FInfM ()
fiseterrtleM eOpt = modify $ flip fiseterrtle eOpt

fiseterrceM :: Maybe (K3 Expression) -> FInfM ()
fiseterrceM eOpt = modify $ flip fiseterrce eOpt


-- | Misc. initialization helpers.
uidOfD :: K3 Declaration -> FInfM UID
uidOfD d = maybe uidErr (\case {(DUID u) -> return u ; _ ->  uidErr}) $ d @~ isDUID
  where uidErr = errorM $ PT.boxToString $ [T.pack "No uid found for fdeclinf on "] %+ PT.prettyLines d

memUID :: AnnMemDecl -> [Annotation Declaration] -> FInfM UID
memUID memDecl as = case filter isDUID as of
                      [DUID u] -> return u
                      _ -> memUIDErr
  where memUIDErr = errorM $ T.append (T.pack "Invalid member UID") $ PT.pretty memDecl

unlessHasEffect :: K3 Declaration -> (() -> FInfM (K3 Declaration)) -> FInfM (K3 Declaration)
unlessHasEffect d f = case d @~ isDEffect of
  Nothing -> f ()
  _ -> return d

unlessMemHasEffect :: AnnMemDecl -> [Annotation Declaration] -> (() -> FInfM AnnMemDecl) -> FInfM AnnMemDecl
unlessMemHasEffect mem as f = case find isDEffect as of
  Nothing -> f ()
  _ -> return mem

effectOf :: K3 Expression -> FInfM (K3 Effect)
effectOf e = maybe effectErr effectOfA $ e @~ isESEffect
  where
    effectOfA a = case a of {(ESEffect f') -> return f'; _ -> effectErr}
    effectErr   = errorM $ PT.boxToString $ [T.pack "No effect found on "] %+ PT.prettyLines e

-- | Attaches any inferred effect in the effect environment to a declaration.
markGlobalEffect :: K3 Declaration -> FInfM (K3 Declaration)
markGlobalEffect d@(tag -> DGlobal  n _ eOpt) = unlessHasEffect d $ \_ -> do
  f' <- maybe (filkupeM n) effectOf eOpt
  return (d @+ (DEffect $ Right f'))

markGlobalEffect d@(tag -> DTrigger _ _ e) = unlessHasEffect d $ \_ -> do
  f' <- effectOf e
  return (d @+ (DEffect $ Right f'))

markGlobalEffect d@(tag -> DDataAnnotation n tvars mems) = do
  nmems <- mapM (markMemsEffect n) mems
  return (replaceTag d $ DDataAnnotation n tvars nmems)

  where
    markMemsEffect an m@(Lifted Provides mn mt meOpt mas) = unlessMemHasEffect m mas $ \_ -> do
      f' <- maybe (filkupaM an mn) effectOf meOpt
      return (Lifted Provides mn mt meOpt $ mas ++ [DEffect $ Right f'])

    markMemsEffect an m@(Attribute Provides mn mt meOpt mas) = unlessMemHasEffect m mas $ \_ -> do
      f' <- maybe (filkupaM an mn) effectOf meOpt
      return (Attribute Provides mn mt meOpt $ mas ++ [DEffect $ Right f'])

    markMemsEffect _ m = return m

markGlobalEffect d = return d


{- Analysis entrypoint -}
inferProgramEffects :: Maybe (ExtInferF a, a)
                    -> Maybe ParGenSymS -> FPPEnv -> K3 Declaration
                    -> Either String (K3 Declaration, FIEnv)
inferProgramEffects extInfOpt symSOpt ppenv prog =  do
  lcenv <- lambdaClosures prog
  runFInfES (fienv0 symSOpt ppenv lcenv) $ doInference prog

  where
    doInference p = do
      np   <- globalsEff p
      np'  <- inferPlain np {- inferWithSimplify np -}
      markGlobals np'

    inferPlain        np = mapExpression (inferExprEffects extInfOpt) np
    inferWithSimplify np = inferPlain np >>= simplifyProgramEffects

    -- Globals cannot be captured in closures, so we elide them from the
    -- effect provenance bindings environment.
    globalsEff :: K3 Declaration -> FInfM (K3 Declaration)
    globalsEff p = inferAllRcrDecls p >>= inferAllDataAnnotationDecls >>= inferAllDecls

    inferAllRcrDecls            p = mapProgram initializeRcrDeclEffect return return Nothing p
    inferAllDataAnnotationDecls p = mapProgram (inferDataAnnotationDeclEffect extInfOpt) return return Nothing p
    inferAllDecls               p = mapProgram (inferDeclEffect extInfOpt) return return Nothing p
    markGlobals                 p = mapProgram markGlobalEffect return return Nothing p


-- | Repeat provenance inference on a global with an initializer.
reinferProgDeclEffects :: Maybe (ExtInferF a, a) -> FIEnv -> Identifier -> K3 Declaration
                       -> Either String (K3 Declaration, FIEnv)
reinferProgDeclEffects extInfOpt env dn prog = runFInfES env inferNamedDecl
  where
    inferNamedDecl = mapProgramWithDecl onNamedDecl (const return) (const return) Nothing prog

    onNamedDecl d@(tag -> DGlobal  n _ (Just e)) | dn == n = inferDecl n d e
    onNamedDecl d@(tag -> DTrigger n _ e)        | dn == n = inferDecl n d e
    onNamedDecl d = return d

    inferDecl n d e = do
      present <- fimemeM n
      nd   <- if present then return d else initializeRcrDeclEffect d
      nd'  <- inferDeclEffect extInfOpt nd
      ne   <- inferPlain e {- inferWithSimplify e -}
      nd'' <- rebuildDecl ne nd'
      markGlobalEffect nd''

    inferPlain        e = inferExprEffects extInfOpt e
    inferWithSimplify e = inferPlain e >>= simplifyExprEffects

    rebuildDecl e d@(tnc -> (DGlobal  n t (Just _), ch)) = return $ Node (DGlobal  n t (Just e) :@: annotations d) ch
    rebuildDecl e d@(tnc -> (DTrigger n t _, ch))        = return $ Node (DTrigger n t e        :@: annotations d) ch
    rebuildDecl _ d = return d


-- | Declaration effect "blind" initialization.
--   This adds a self-referential effect pointers to a global for cyclic scope.
initializeRcrDeclEffect :: K3 Declaration -> FInfM (K3 Declaration)
initializeRcrDeclEffect decl = case tag decl of
    DGlobal  n _ _ -> uidOfD decl >>= \u -> fifreshsM n u >> declProv decl n >> return decl
    DTrigger n _ _ -> uidOfD decl >>= \u -> fifreshsM n u >> declProv decl n >> return decl
    DDataAnnotation n _ mems -> mapM freshMems mems >>= fifreshAsM n . catMaybes >> return decl
    _ -> return decl

  where
    freshMems m@(Lifted      Provides mn mt _ mas) = memUID m mas >>= \u -> return (Just (mn, u, True,  isTFunction mt))
    freshMems m@(Attribute   Provides mn mt _ mas) = memUID m mas >>= \u -> return (Just (mn, u, False, isTFunction mt))
    freshMems _ = return Nothing

    declProv d n = void $ provOf d >>= fiextepM n

    provOf    d = maybe (provErr d) (provOfA d) $ d @~ isDProvenance
    provOfA d a = case a of {(DProvenance p') -> either return return p'; _ -> provErr d}
    provErr   d = errorM $ PT.boxToString $ [T.pack "No provenance found on "] %+ PT.prettyLines d

-- | Structure-based declaration effect initialization.
--   Infer based on initializer, and update effect pointer.
inferDeclEffect :: Maybe (ExtInferF a, a) -> K3 Declaration -> FInfM (K3 Declaration)
inferDeclEffect extInfOpt d@(tag -> DGlobal n t eOpt) = do
  u <- uidOfD d
  f <- case d @~ isDEffect of
          Just (DEffect eff) -> either return return eff
          _ -> maybe (effectsOfType [] t) (inferEffects extInfOpt) eOpt
  void $ fistoreM n u f
  return d

inferDeclEffect extInfOpt d@(tag -> DTrigger n _ e) = do
  u <- uidOfD d
  f <- case d @~ isDEffect of
          Just (DEffect eff) -> either return return eff
          _ -> inferEffects extInfOpt e
  void $ fistoreM n u f
  return d

inferDeclEffect extInfOpt d@(tag -> DDataAnnotation n _ mems) = do
  mEffs <- mapM inferMems mems
  void $ fistoreaM n $ catMaybes mEffs
  return d

  where
    inferMems m@(Lifted      Provides mn mt meOpt mas) = inferMember m True  mn mt meOpt mas
    inferMems m@(Attribute   Provides mn mt meOpt mas) = inferMember m False mn mt meOpt mas
    inferMems _ = return Nothing

    inferMember mem lifted mn mt meOpt mas = do
      u  <- memUID mem mas
      mf <- case find isDEffect mas of
              Just (DEffect eff) -> either return return eff
              _ -> maybe (effectsOfType [] mt) (inferEffects extInfOpt) meOpt
      return $ Just (mn, u, mf, lifted)

inferDeclEffect _ d = return d

inferDataAnnotationDeclEffect :: Maybe (ExtInferF a, a) -> K3 Declaration -> FInfM (K3 Declaration)
inferDataAnnotationDeclEffect extInfOpt d@(tag -> DDataAnnotation n _ mems) = do
  mEffs <- mapM inferMems mems
  void $ fistoreaM n $ catMaybes mEffs
  return d

  where
    inferMems m@(Lifted      Provides mn mt meOpt mas) = inferMember m True  mn mt meOpt mas
    inferMems m@(Attribute   Provides mn mt meOpt mas) = inferMember m False mn mt meOpt mas
    inferMems _ = return Nothing

    inferMember mem lifted mn mt meOpt mas = do
      u  <- memUID mem mas
      mf <- case find isDEffect mas of
              Just (DEffect eff) -> either return return eff
              _ -> maybe (effectsOfType [] mt) (inferEffects extInfOpt) meOpt
      return $ Just (mn, u, mf, lifted)

inferDataAnnotationDeclEffect _ d = return d

-- | Expression effect inference.
inferExprEffects :: Maybe (ExtInferF a, a) -> K3 Expression -> FInfM (K3 Expression)
inferExprEffects extInfOpt expr = inferEffects extInfOpt expr >> substituteEffects expr

inferEffects :: Maybe (ExtInferF a, a) -> K3 Expression -> FInfM (K3 Effect)
inferEffects extInfOpt expr = do
  fiseterrtleM $ Just expr
  (_, r) <- biFoldMapIn1RebuildTree topdown sideways inferWithRule iu (Nothing, []) expr
  return r

  where
    iu = return ()
    srt = return . (iu,)

    extInferM f = maybe (return f) (\(infF, infSt) -> get >>= return . infF f infSt) extInfOpt

    topdown m _ e@(tag -> ELambda i) = m >> uidOf e >>= \u -> fiexteM i (ffvar i) >> fiextepM i (pfvar i $ Just u) >> ppushClosure u e >> return iu
    topdown m _ _ = m >> return iu

    -- Effect bindings reference lambda effects where necessary to ensure
    -- defferred effect inlining for function values.
    -- Bindings are introduced based on types.
    sideways :: FInfM () -> (Maybe (K3 Effect), [PMatVar]) -> K3 Effect -> K3 Expression
             -> FInfM (FInfM (), [FInfM ()])
    sideways m _ rf e@(tag -> ELetIn i) = m >> do
      case (head $ children e) @~ isEType of
        Just (EType t) -> uidOf e >>= \u -> srt [freshM False e i u t rf]
        _ -> tAnnErr e

    sideways m _ rf e@(tag -> ECaseOf i) = m >> do
      u <- uidOf e
      case (head $ children e) @~ isEType of
        Just (EType t) -> srt [freshOptM e i u t rf, setcaseM i >> popVars u i]
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

    -- Effect deduction logging.
    inferWithRule :: FInfM () -> [(Maybe (K3 Effect), [PMatVar])] -> [K3 Effect] -> K3 Expression
                  -> FInfM ((Maybe (K3 Effect), [PMatVar]), K3 Effect)
    inferWithRule m efch rfch e = do
      fiseterrceM $ Just e
      u <- uidOf e
      (ruleTag, r) <- infer m efch rfch e
      localLog $ T.unpack $ showFInfRule ruleTag efch rfch r u
      return r

    infer :: FInfM () -> [(Maybe (K3 Effect), [PMatVar])] -> [K3 Effect] -> K3 Expression
          -> FInfM (String, ((Maybe (K3 Effect), [PMatVar]), K3 Effect))
    infer m (onSub -> (_, mv)) _ e@(tag -> EConstant _) = m >> rt "const" False e mv (Nothing, fnone)
    infer m (onSub -> (_, mv)) _ e@(tag -> EVariable i) = m >> do
      f  <- filkupeM i
      p  <- filkupepM i
      p' <- liftExceptM $ chaseProvenance p
      ef <- extInferM $ fread p'
      rt "var" False e mv (Just ef, f)

    infer m (onSub -> (ef, mv)) rf e@(tag -> ESome)        = m >> rt "some"   False e mv (fexec ef,     fdata Nothing    rf)
    infer m (onSub -> (ef, mv)) rf e@(tag -> EIndirect)    = m >> rt "ind"    False e mv (fexec ef,     fdata Nothing    rf)
    infer m (onSub -> (_,  mv)) _  e@(tnc -> (ETuple, [])) = m >> rt "unit"   False e mv (Just $ fnone, fnone)
    infer m (onSub -> (ef, mv)) rf e@(tag -> ETuple)       = m >> rt "tuple"  False e mv (fexec ef,     fdata Nothing    rf)
    infer m (onSub -> (ef, mv)) rf e@(tag -> ERecord ids)  = m >> rt "record" False e mv (fexec ef,     fdata (Just ids) rf)

    infer m (onSub -> (ef, mv)) [rf] e@(tag -> ELambda i) = m >> do
      UID u    <- uidOf e
      clv      <- filkupcM u
      clf      <- mapM filkupepM clv >>= mapM (liftExceptM . chaseProvenance) >>= mapM (extInferM . fread)
      ppopClosure (UID u) e
      popVars (UID u) i
      rt "lambda" False e mv (Just fnone, flambda i (fseq clf) (fseq $ catMaybes ef) rf)

    infer m (onSub -> (ef, mv)) [lrf,arf] e@(tag -> EOperate OApp) = m >> do
      env <- get
      (appef, apprf) <- simplifyApplyM extInfOpt False (Just e) ef lrf arf
      appmv <- pmvOf e
      let nmv = appmv ++ mv
      pappef <- pruneAndSimplify "appef" (Just $ PT.prettyLines lrf %$ PT.prettyLines arf) False nmv $ Just appef
      Just papprf <- simplifyAppCh mv $ Just apprf
      debugAppRF env nmv appef pappef apprf papprf $ rt "apply" True e nmv (pappef, papprf)

      where debugAppRF denv x a a' b b' c = if True then c else do
              Just nb' <- pruneAndSimplify "apprf" Nothing True x $ Just b'
              flip trace c (T.unpack $ PT.boxToString
                $ [T.pack "AppEF/AppRF"]
               %$ PT.prettyLines lrf
               %$ [T.pack "Chased lambda:"]
               %$ (either (:[]) (concatMap PT.prettyLines) $ runExcept $ chaseLambda denv extInfOpt [] [] lrf)
               %$ PT.prettyLines arf
               %$ PT.prettyLines a
               %$ (maybe [T.pack "No PAppEF"] PT.prettyLines a')
               %$ PT.prettyLines b
               %$ PT.prettyLines b'
               %$ PT.prettyLines nb'
               %$ PT.prettyLines e)

    infer m (onSub -> (ef, mv)) [rf] e@(tnc -> (EProject i, [esrc])) = m >> do
      psrc <- provOf esrc
      case esrc @~ isEType of
        Just (EType t) ->
          case tag t of
            TCollection -> collectionMemberEffect extInfOpt i ef rf esrc t psrc >>= rt "cproject" False e mv
            TRecord ids ->
              case tnc rf of
                (FData _, fdch) -> do
                  idx <- maybe (memErr i esrc) return $ elemIndex i ids
                  rt "rproject" False e mv (fexec ef, fdch !! idx)
                (_,_) -> rt "project" False e mv (fexec ef, fnone)
            _ -> prjErr esrc
        _ -> prjErr esrc

    infer m (onSub -> (ef, mv)) _ e@(tag -> EAssign i) = m >> do
      p   <- filkupepM i
      p'  <- liftExceptM $ chaseProvenance p
      aef <- extInferM $ fwrite p'
      rt "assign" False e mv (fexec $ ef ++ [Just aef], fnone)

    infer m (onSub -> (ef, mv)) _      e@(tag -> EOperate OSnd) = m >> rt "send"    False e mv (fexec $ ef ++ [Just fio], fnone)
    infer m (onSub -> (ef, mv)) [_,rf] e@(tag -> EOperate OSeq) = m >> rt "seq"     False e mv (fexec ef, rf)
    infer m (onSub -> (ef, mv)) _      e@(tag -> EOperate op)   = m >> rt (show op) False e mv (fexec ef, fnone)

    infer m (onSub -> ([pe,te,ee], mv)) [_,tr,er] e@(tag -> EIfThenElse) =
      m >> rt "if-then-else" False e mv (fexec [pe, Just $ fset $ catMaybes [te, ee]], fset [tr,er])

    infer m (onSub -> ([initef,bef], submv)) [_,rf] e@(tag -> ELetIn  i) = m >> do
      u   <- uidOf e
      smv <- pmvOf e
      mv  <- filkupeM i >>= fmv
      popVars u i
      let nmv = smv ++ submv
      let nrf = fscope [mv] (fromJust $ fexec [initef]) (fromJust $ fexec [bef]) fnone rf
      nef <- pruneAndSimplify "letef" Nothing False nmv $ fexec [initef, bef]
      rt "let-in" True e nmv (nef, nrf)

    infer m (onSub -> ([initef,bef], mv)) [_,rf] e@(tag -> EBindAs b) = m >> do
      u     <- uidOf e
      initp <- provOf $ head $ children e
      smvs  <- pmvOf e
      fmvs  <- mapM filkupeM (bindingVariables b) >>= mapM fmv
      pbvs  <- mapM filkupepM (bindingVariables b)
      ps    <- mapM pmv pbvs >>= mapM (filkupppM . pmvptr)
      mapM_ (popVars u) (bindingVariables b)
      let nmv  = smvs ++ mv
      let nief = fromJust $ fexec [initef]
      let nbef = fromJust $ fexec [bef]
      npief <- (liftExceptM $ chaseProvenance initp) >>= extInferM . fwrite
      npef <- mapM (liftExceptM . chaseProvenance) ps >>= mapM (extInferM . fwrite) >>= return . fseq
      symcat <- liftEitherM $ categorizeLocalEffects nbef
      nef <- pruneAndSimplify "bindef" Nothing False nmv $ fexec $ map Just $
               [nief, nbef] ++ (if all (flip noWritesOnProvenanceC symcat) pbvs then [] else [npief])
      let nrf  = fscope fmvs nief nbef npef rf
      rt "bind-as" True e nmv (nef, nrf)

    infer m (onSub -> ([oef,sef,nef], mv)) [_,snf,rnf] e@(tag -> ECaseOf _) = m >> do
      initp <- provOf $ head $ children e
      (cfmv, cpmv) <- fipopcaseM ()
      let nmv  = [cpmv] ++ mv
      let nief = fromJust $ fexec [oef]
      let nbef = fset $ catMaybes [sef, nef]
      npief <- (liftExceptM $ chaseProvenance initp) >>= extInferM . fwrite
      npef <- extInferM (fwrite $ pbvar cpmv) >>= \spf -> return (fset [spf, fnone])
      symcat <- maybe (return emptyCategories) (liftEitherM . categorizeLocalEffects) sef
      nsef <- pruneAndSimplify "casesef" Nothing False nmv $ fexec $
                [sef] ++ (if noWritesOnProvenanceC (pbvar cpmv) symcat then [] else [Just npief])
      let nef' = fexec $ map Just [nief, fset [maybe fnone id nsef, maybe fnone id nef]]
      let nrf  = fscope [cfmv] nief nbef npef (fset [snf, rnf])
      rt "case-of" True e nmv (nef', nrf)

    infer m (onSub -> (_, mv)) _ e@(tag -> EAddress) = m >> rt "address" False e mv (Nothing, fnone)

    -- TODO: unhandled cases: ESelf, EImperative
    infer m _ _ e = m >> inferErr e

    rt tg prune e mv r = fiextmM e (maybe fnone id $ fst r) (snd r) >> rtp tg prune mv r
    rtp tg prune mv r = do
      let r' = Just $ snd r
      Just nr <- if prune then pruneAndSimplify "rtsf" Nothing True mv r' else return r'
      return (tg, ((fst r, mv), nr))

    onSub efmv = let (x,y) = unzip efmv in (x, foldl union [] y)

    fexec ef = Just $ fseq $ catMaybes ef
    popVars u i = fideleM i >> fidelepM i

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
          void $ maybe (return ()) (\mv -> fiextepM (pmvn mv) (pbvar mv)) $ find ((== i) . pmvn) mvs
          forceLambdaEff t f >>= void . fifreshM i u

        Just (EProvenance (tnc -> (PSet, (safeHead -> Just (tag -> PMaterialize mvs))))) | asCase -> do
          void $ maybe (return ()) (\mv -> fiextepM (pmvn mv) (pbvar mv)) $ find ((== i) . pmvn) mvs
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

    -- | Closure variable management
    ppushClosure (UID i) e = filkupcM i >>= \cli -> (provOf e >>= lambdaClmvs) >>= \clmvs -> (mapM_ liftClosureVar $ zip cli clmvs)
    ppopClosure  (UID i) e = filkupcM i >>= mapM_ (lowerClosureVar e)

    lambdaClmvs (tag -> PLambda _ clmvs) = return clmvs
    lambdaClmvs _ = return []

    liftClosureVar (n, mv) = filkupepM n >>= \p -> fidelepM n >> fiextepM n (pbvar mv)
    lowerClosureVar e n = filkupepM n >>= \p -> unwrapClosure e p >>= \p' -> fidelepM n >> fiextepM n p'

    unwrapClosure _ (tag -> PBVar mv) = filkupppM (pmvptr mv)
    unwrapClosure e p = errorM $ PT.boxToString
                               $ [T.pack "Invalid closure variable "] %+ PT.prettyLines p
                              %$ [T.pack "at expr:"] %$ PT.prettyLines e



    -- Simlutaneously removes any effects on the given provenance symbols,
    -- and reduces effect structures
    pruneAndSimplify :: Identifier -> Maybe [Text] -> Bool -> [PMatVar] -> Maybe (K3 Effect) -> FInfM (Maybe (K3 Effect))
    pruneAndSimplify _ _ _ _ Nothing = return Nothing
    pruneAndSimplify ptag errExtra asStructure pmvl (Just pf) = transform asStructure pmvl pf >>= return . Just
      where transform False mvl (tag -> FRead  (tag -> PBVar mv')) | mv' `elem` mvl = return fnone
            transform False mvl (tag -> FWrite (tag -> PBVar mv')) | mv' `elem` mvl = return fnone

            -- Structure node transformation
            transform False _ (tnc -> (FLambda _, [clf, _, _])) = return clf

            transform True mvl (tnc -> (FLambda i, [clf, bef, rf])) = do
              nbef <- transform False mvl bef
              nrf  <- transform True mvl rf
              return $ flambda i clf nbef nrf

            -- Note a 3-child variant cannot exist yet since we have not performed simplification.
            transform False mvl (tnc -> (FApply (Just _), [_, _, ief, bef, _])) = do
              nief <- transform False mvl ief
              nbef <- transform False mvl bef
              return $ fseq [nief, nbef]

            transform True mvl (tnc -> (FApply (Just _), [_, _, _, _, rf])) = transform True mvl rf

            transform False mvl (tnc -> (FApply (Just _), [rf])) = return fnone
            transform True mvl (tnc -> (FApply (Just _), [rf])) = transform True mvl rf

            -- Try to simplify any external FApply.
            transform asStructure mvl (tnc -> (FApply Nothing, [l, a, ief, bef, r])) = do
              nl <- transform True mvl l
              na <- transform True mvl a
              nief <- transform False mvl ief
              nbef <- transform False mvl bef
              nr <- transform True mvl r
              return $ fapply Nothing nl na nief nbef nr

            transform asStructure mvl (tnc -> (FApply Nothing, [l, a])) = do
              nl <- transform True mvl l
              na <- transform True mvl a
              (nef, nrf) <- simplifyApplyM extInfOpt True Nothing [] nl na
              return $ debugXfApp nl na nef nrf $ if asStructure then nrf else
                case tnc nrf of
                  (FApply Nothing, [_,_]) -> nrf
                  _ -> nef
              where debugXfApp a b c d r = if True then r else
                      flip trace r $ T.unpack $ PT.boxToString $ [T.pack $ "Transform " ++ show asStructure]
                                             %$ PT.prettyLines a
                                             %$ PT.prettyLines b
                                             %$ PT.prettyLines c
                                             %$ PT.prettyLines d
                                             %$ PT.prettyLines r

            transform asStructure mvl (tnc -> (FApply Nothing, [r])) = do
              nr <- transform True mvl r
              return $ fapplyRT Nothing nr

            transform False mvl (tnc -> (FScope _, [ief, bef, pef, _])) = do
              nief <- transform False mvl ief
              nbef <- transform False mvl bef
              npef <- transform False mvl pef
              return $ fseq [nief, nbef, npef]

            transform True mvl (tnc -> (FScope _, [_, _, _, rf])) = transform True mvl rf

            transform _ _   f@(tag -> FNone)    = return f
            transform _ _   f@(tag -> FFVar _)  = return f
            transform s mvl f@(tag -> FBVar _)  = do
              f' <- fichaseM f
              if f == f' then return f else transform s mvl f'

            transform False _ f@(tag -> FRead  _) = return f
            transform False _ f@(tag -> FWrite _) = return f
            transform False _ f@(tag -> FIO)      = return f

            transform False mvl (tnc -> (FSeq, ch)) = mapM (transform False mvl) ch >>= return . fseq
            transform False mvl (tnc -> (FLoop, [ch])) = do
              nch <- transform False mvl ch
              return $ debugXfLoop ch $ floop nch
              where debugXfLoop a b = if True then b else
                      flip trace b $ T.unpack $ PT.boxToString $ [T.pack "Transform loop"]
                                             %$ PT.prettyLines a
                                             %$ PT.prettyLines b

            transform True mvl (tnc -> (FData idOpt, ch)) = mapM (transform True mvl) ch >>= return . fdata idOpt

            transform s mvl (tnc -> (FSet, ch)) = mapM (transform s mvl) ch >>= return . fset

            transform s _ f = errorM $ PT.boxToString $ [T.pack $ errmsg s]
                                    %$ PT.prettyLines f
                                    %$ [T.pack "applied on:"]
                                    %$ PT.prettyLines pf
                                    %$ (maybe [] id errExtra)

            errmsg s = "Invalid pruneAndSimplify ("++ ptag ++ ", asStructure=" ++ show s ++ "): "

    simplifyAppCh :: [PMatVar] -> Maybe (K3 Effect) -> FInfM (Maybe (K3 Effect))
    simplifyAppCh _ Nothing = return Nothing
    simplifyAppCh pmvl (Just pf) = do
      let origChOpt = map Just $ children pf
      let chStructure = case tnc pf of
                          (FApply _, ch@[lf, af, ief, bef, sf]) -> Just $ zip [True, True, False, False, True] ch
                          (FApply _, ch@[lf, af]) -> Just $ zip [True, True] ch
                          (FApply _, ch@[rf]) -> Just $ zip [True] ch
                          _ -> Nothing
      nchOpt <- maybe (return origChOpt) (mapM (\(s,c) -> pruneAndSimplify "appchsf" Nothing s pmvl $ Just c)) chStructure
      return $ Just $ replaceCh pf $ catMaybes nchOpt

    fmv (tag -> FBVar mv) = return mv
    fmv f = errorM $ PT.boxToString $ [T.pack "Invalid effect bound var: "] %$ PT.prettyLines f

    pmv (tag -> PBVar mv) = return mv
    pmv p = errorM $ PT.boxToString $ [T.pack "Invalid provenance bound var: "] %$ PT.prettyLines p

    pmvOf  e = maybe (pmvErr e) (\case {EProvenance p -> pmvOfT e p; _ -> pmvErr e}) $ e @~ isEProvenance
    pmvErr e = errorM $ PT.boxToString $ [T.pack "No pmv found on "] %+ PT.prettyLines e

    pmvOfT _ (tag -> PApply mvOpt) = return $ maybe [] (:[]) mvOpt
    pmvOfT _ (tag -> PMaterialize mvl) = return mvl
    pmvOfT e _ = pmvErr e

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = errorM $ PT.boxToString $ [T.pack "No uid found for fexprinf on "] %$ PT.prettyLines e

    provOf  e = maybe (provErr e) (\case {(EProvenance p) ->  return p; _ -> provErr e}) $ e @~ isEProvenance
    provErr e = errorM $ PT.boxToString $ [T.pack "No provenance found on "] %+ PT.prettyLines e

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

    showFInfRule rtag efch rfch ((refOpt, _), rrf) euid =
      PT.boxToString $ (rpsep %+ premise) %$ separator %$ (rpsep %+ conclusion)
      where commaT         = T.pack ", "
            rprefix        = T.pack $ unwords [rtag, show euid]
            (rplen, rpsep) = (T.length rprefix, [T.pack $ replicate rplen ' '])
            premise        = let ch = zip (map fst efch) rfch in
                             if null ch then [T.pack "<empty>"]
                             else (PT.intersperseBoxes [commaT] $ map (uncurry prettyER) ch)
            premLens       = map T.length premise
            headWidth      = if null premLens then 4 else maximum premLens
            separator      = [T.append rprefix $ T.pack $ replicate headWidth '-']
            conclusion     = prettyER refOpt rrf
            prettyER ef rf = maybe [T.pack "<no exec>"] PT.prettyLines ef %+ [commaT] %+ PT.prettyLines rf

substituteEffects :: K3 Expression -> FInfM (K3 Expression)
substituteEffects expr = modifyTree injectEffects expr
  where injectEffects e = filkupmM e >>= \(f,s) -> return ((e @+ ESEffect f) @+ EFStructure s)

effectsOfType :: [Identifier] -> K3 Type -> FInfM (K3 Effect)
effectsOfType args t | isTFunction t =
   case tnc t of
    (TForall _, [ch])      -> effectsOfType args ch
    (TFunction, [_, retT]) -> let a = mkArg (length args + 1)
                              in effectsOfType (args++[a]) retT
    _ -> errorM $ PT.boxToString $ [T.pack "Invalid function type"] %+ PT.prettyLines t
  where mkArg i = "__arg" ++ show i

effectsOfType [] _   = return fnone
effectsOfType args _ = return $ foldl lam (flambda (last args) fnone ef fnone) $ init args
  where
    lam rfacc a = flambda a fnone fnone rfacc
    ef = floop $ fseq $ concatMap (\i -> [fread $ pfvar i Nothing, fwrite $ pfvar i Nothing]) args ++ [fio]


-- | Computes execution effects and effect structure for a collection field member.
--   TODO: much like its counterpart with provenance, this method recomputes the effect rather
--   using a cached result.
collectionMemberEffect :: Maybe (ExtInferF a, a) -> Identifier -> [Maybe (K3 Effect)] -> K3 Effect
                       -> K3 Expression -> K3 Type -> K3 Provenance
                       -> FInfM (Maybe (K3 Effect), K3 Effect)
collectionMemberEffect extInfOpt i ef sf esrc t psrc =
  let annIds = namedTAnnotations $ annotations t in do
    memsEnv <- mapM filkupasM annIds >>= return . BEnv.unions
    (mrf, lifted) <- liftExceptM $ BEnv.lookup memsEnv i
    mrfs  <- fisubM extInfOpt True "self" sf mrf psrc
    mrfsc <- fisubM extInfOpt True "content" fnone mrfs ptemp
    if not lifted then attrErr else return $ (Just $ fseq $ catMaybes ef, mrfsc)

  where
    memErr  = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Unknown projection of ", i, "on"]]           %+ PT.prettyLines esrc
    attrErr = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Invalid attribute projection of ", i, "on"]] %+ PT.prettyLines esrc


-- | Replaces 5-place FApply effect nodes with only their initial execution and
--   deferred body effects, as well as their effect structure.
simplifyEffects :: Bool -> K3 Effect -> FInfM (K3 Effect)
simplifyEffects removeTopLevel f =
  biFoldRebuildTree remove simplify removeTopLevel () f >>= return . snd
  where
    remove rm    (tag -> FScope _)  = return (rm, [True, True, True, False])
    remove rm    (tag -> FLambda _) = return (rm, [True, True, False])
    remove rm f'@(tag -> FApply _)  = case length $ children f' of
                                        5 -> return (rm, [False, False, True, True, False])
                                        3 -> return (rm, [True, True, False])
                                        2 -> return (rm, [False, False])
                                        1 -> return (rm, [False])
                                        _ -> errorM $ PT.boxToString $ [T.pack "Invalid app effect"] %$ PT.prettyLines f
    remove rm f' = return (rm, replicate (length $ children f') rm)

    simplify rm _ [_,_,ef,bf,r] f'@(tag -> FApply mvOpt) =
          rt $ (mkApp rm mvOpt ef bf r) @<- annotations f'

    -- Rebuilding FSeq, FLoop will filter all fnones.
    simplify _ _ ch   (tag -> FSet)  = rt $ fset ch
    simplify _ _ ch   (tag -> FSeq)  = rt $ fseq ch
    simplify _ _ [cf] (tag -> FLoop) = rt $ floop cf
    simplify _ _ ch f' = rt $ replaceCh f' ch

    -- Flatten applies nested in return structures.
    mkApp rm mvOpt ef bf (tnc -> (FApply Nothing, [ef', bf', r'])) =
      mkApp rm mvOpt ef (fseq [bf, ef', bf']) r'

    mkApp rm mvOpt ef bf r =
      if rm then fseq [ef, bf]
      else Node (FApply mvOpt :@: []) [ef, bf, r]

    rt = return . ((),)

-- | Simplifies applies on all effect trees attached to an expression.
simplifyProgramEffects :: K3 Declaration -> FInfM (K3 Declaration)
simplifyProgramEffects d = mapExpression simplifyExprEffects d

simplifyExprEffects :: K3 Expression -> FInfM (K3 Expression)
simplifyExprEffects expr = modifyTree simplifyEffAnns expr
  where
    simplifyEffAnns e = do
      (Just (ESEffect f1), Just (EFStructure f2)) <- annsOf e
      nf1 <- simplifyEffects True f1
      nf2 <- simplifyEffects False f2
      return $ ((e @<- (filter (not . isEffectAnn) $ annotations e)) @+ (ESEffect nf1)) @+ (EFStructure nf2)

    annsOf e = return (e @~ isESEffect, e @~ isEFStructure)

    isEffectAnn (ESEffect _) = True
    isEffectAnn (EFStructure _) = True
    isEffectAnn _ = False


{- Effect queries -}
type EffectMap = IntMap (K3 Effect)

-- | Single-pass, bottom-up extraction of default effects at all expressions in a program.
inferDefaultEffects :: K3 Declaration -> Either Text EffectMap
inferDefaultEffects p = foldExpression foldDefaultExprEffects IntMap.empty p >>= return . fst

-- | Accumulating default effect computation for a specific expression.
--   Default effects only require extraction of the execution effects at an expression, since
--   inference without an external function assumes call-by-value semantics for all bindings.
foldDefaultExprEffects :: EffectMap -> K3 Expression -> Either Text (EffectMap, K3 Expression)
foldDefaultExprEffects accqr expr = do
  nqr <- foldMapTree extract emptyQR expr
  return (unionQR accqr nqr, expr)

  where
    extract :: [EffectMap] -> K3 Expression -> Either Text EffectMap
    extract (unionsQR -> qr) e = fromEffect e (e @~ isESEffect) >>= rt qr e

    rt :: EffectMap -> K3 Expression -> K3 Effect -> Either Text EffectMap
    rt qr e f = uidOf e >>= \u -> return (putQR qr u f)

    fromEffect _ (Just (ESEffect f)) = return f
    fromEffect e _ = effectErr e

    emptyQR  = IntMap.empty
    unionQR  = IntMap.union
    unionsQR = IntMap.unions
    putQR qr (UID i) f = IntMap.insert i f qr

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = Left $ PT.boxToString $ [T.pack "No uid found for fdefault on "] %+ PT.prettyLines e

    effectErr e = Left $ PT.boxToString $ [T.pack "No execution effects found on expression: "] %$ PT.prettyLines e
                                       %$ [T.pack "On argument: "] %$ PT.prettyLines expr

-- | Default effect computation for a specific expression.
inferDefaultExprEffects :: K3 Expression -> Either Text EffectMap
inferDefaultExprEffects e = foldDefaultExprEffects IntMap.empty e >>= return . fst

-- Categorizes symbols by whether they participate in reads, writes, function application or scopes.
data SymbolCategories = SymbolCategories { readSyms    :: [K3 Provenance]
                                         , writeSyms   :: [K3 Provenance]
                                         , doesIO      :: Bool
                                         }

instance Pretty SymbolCategories where
  prettyLines (SymbolCategories r w io) =
         [T.pack "Reads"]   %$ concatMap PT.prettyLines r
      %$ [T.pack "Writes"]  %$ concatMap PT.prettyLines w
      %$ [T.pack "Does IO "] %+ [T.pack $ show io]

emptyCategories :: SymbolCategories
emptyCategories = SymbolCategories [] [] False

addCategories :: SymbolCategories -> SymbolCategories -> SymbolCategories
addCategories (SymbolCategories r w io1) (SymbolCategories r2 w2 io2) =
  SymbolCategories (nub $ r++r2) (nub $ w++w2) (io1 || io2)

-- | Effect categorization
type SymCatMap = IntMap SymbolCategories

categorizeProgramEffects :: K3 Declaration -> Either Text SymCatMap
categorizeProgramEffects p = foldExpression categorizeExpr emptyCatMap p >>= return . fst
  where
    categorizeExpr cmacc e = (\f -> foldTree f cmacc e >>= return . (,e)) $ \cmacc' e' -> do
      UID u <- uidOf e'
      sc    <- categorizeExprEffects e'
      return $ addCatMap cmacc' u sc

    emptyCatMap = IntMap.empty
    addCatMap cm u c = IntMap.insert u c cm

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = Left $ PT.boxToString $ [T.pack "No uid found for fcategorizedecl on "] %+ PT.prettyLines e

categorizeExprEffects :: K3 Expression -> Either Text SymbolCategories
categorizeExprEffects e = do
  UID u <- uidOf e
  df    <- inferDefaultExprEffects e
  maybe (lookupErr e) categorizeLocalEffects $ IntMap.lookup u df

  where

    uidOf  e' = maybe (uidErr e') (\case {(EUID u) -> return u ; _ ->  uidErr e'}) $ e' @~ isEUID
    uidErr e' = Left $ PT.boxToString $ [T.pack "No uid found for fcategorizeexpr on "] %+ PT.prettyLines e'

    lookupErr e' = Left $ PT.boxToString $ [T.pack "No effects found for "] %+ PT.prettyLines e'

-- | Returns effects present locally at an expression with effect targets categorized into reads, writes and IO.
--   Note: we do not need to chase FBVars here for categorization.
--   All effects from FBVar targets will already have been lifted during default
--   effect inference, or applied during regular effect inference.
categorizeLocalEffects :: K3 Effect -> Either Text SymbolCategories
categorizeLocalEffects (tag -> FRead  p)  = return $ emptyCategories {readSyms  = [p]}
categorizeLocalEffects (tag -> FWrite p)  = return $ emptyCategories {writeSyms = [p]}
categorizeLocalEffects (tag -> FIO)       = return $ emptyCategories {doesIO    = True}
categorizeLocalEffects (tag -> FLambda _) = return $ emptyCategories
categorizeLocalEffects (Node _ ch)        = foldM (\a c -> categorizeLocalEffects c >>= return . addCategories a) emptyCategories ch


{- Effect queries -}

readOnlyC :: SymbolCategories -> Bool
readOnlyC (SymbolCategories _ w io) = null w && not io

readOnlyOnProvenanceC :: K3 Provenance -> SymbolCategories -> Bool
readOnlyOnProvenanceC p (SymbolCategories _ w io) = not io && p `notElem` w

noWritesC :: SymbolCategories -> Bool
noWritesC (SymbolCategories _ w _) = null w

noWritesOnProvenanceC :: K3 Provenance -> SymbolCategories -> Bool
noWritesOnProvenanceC p (SymbolCategories _ w _) = p `notElem` w

noWritesOnBinding :: PIEnv -> Identifier -> UID -> SymbolCategories -> Either Text Bool
noWritesOnBinding env i u (SymbolCategories _ w _) = runExcept $ do
  b <- mapM (piBindings env) w >>= return . nub . concat
  return $ (i, u) `notElem` (rights b)


-- | Returns whether the given expression has only read effects.
--   The first argument indicates whether lambda effects should be considered in terms of their body's effects.
readOnly :: Bool -> K3 Expression -> Either String Bool
readOnly True (tnc -> (ELambda _, [b])) = readOnly False b
readOnly _ e = either (Left . T.unpack) Right $ do
  symcat <- categorizeExprEffects e
  return $ readOnlyC symcat

-- | Returns whether the given expression has no write effects.
--   The first argument indicates whether lambda effects should be considered in terms of their body's effects.
noWrites :: Bool -> K3 Expression -> Either String Bool
noWrites True (tnc -> (ELambda _, [b])) = noWrites False b
noWrites _ e = either (Left . T.unpack) Right $ do
  symcat <- categorizeExprEffects e
  return $ noWritesC symcat

-- | Returns where the given expression has any writes on an identifier, UID pair.
--   The first argument indicates whether lambda effects should be considered in terms of their body's effects.
noWritesOn :: Bool -> PIEnv -> Identifier -> UID -> K3 Expression -> Either String Bool
noWritesOn True env i u (tnc -> (ELambda _, [b])) = noWritesOn False env i u b
noWritesOn _ env i u e = either (Left . T.unpack) Right $ do
  symcat <- categorizeExprEffects e
  noWritesOnBinding env i u symcat


{- Pattern synonyms for inference -}
pattern PTOption et <- Node (TOption :@: _) [et]
pattern PTIndirection et <- Node (TIndirection :@: _) [et]
pattern PTTuple tt <- Node (TTuple :@: _) tt
pattern PTRecord rt <- Node (TRecord _ :@: _) rt

{- Effect environment pretty printing -}
instance Pretty FIEnv where
  prettyLines (FIEnv c p e a pb _ cl ef _ errc) =
    [T.pack $ "FCnt: " ++ show c] ++
    [T.pack "FEnv: "  ]  %$ (PT.indent 2 $ PT.prettyLines e)  ++
    [T.pack "FPEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines p)  ++
    [T.pack "FAEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines a)  ++
    [T.pack "FPBEnv: "]  %$ (PT.indent 2 $ PT.prettyLines pb) ++
    [T.pack "FLCEnv: "]  %$ (PT.indent 2 $ PT.prettyLines cl) ++
    [T.pack "EFMap: " ]  %$ (PT.indent 2 $ PT.prettyLines ef) ++
    [T.pack "FErrCtxt: "]
      %$ (PT.indent 2 $ [T.pack "Toplevel:"] %$ (maybe [T.pack "<nothing>"] PT.prettyLines $ ftoplevelExpr errc)
                     %$ [T.pack "Current:"]  %$ (maybe [T.pack "<nothing>"] PT.prettyLines $ fcurrentExpr errc))

instance Pretty (IntMap (K3 Effect)) where
  prettyLines fp = IntMap.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] fp

instance Pretty (IntMap (K3 Effect, K3 Effect)) where
  prettyLines fp = IntMap.foldlWithKey (\acc k (u,v) -> acc ++ prettyTriple k u v) [] fp

instance Pretty (IntMap SymbolCategories) where
  prettyLines cm = IntMap.foldlWithKey (\acc k sc -> acc ++ prettyPair (k,sc)) [] cm

instance Pretty FEnv where
  prettyLines fe = BEnv.foldl (\acc k v -> acc ++ prettyFrame k v) [] fe
    where prettyFrame k v = concatMap prettyPair $ flip zip v $ replicate (length v) k

instance Pretty FAEnv where
  prettyLines fa = BEnv.foldl (\acc k v -> acc ++ prettyPair (k,v)) [] fa

instance Pretty FMEnv where
  prettyLines fm = BEnv.foldl (\acc k v -> acc ++ prettyPair (k, fst v)) [] fm

prettyPair :: (Show a, Pretty b) => (a,b) -> [Text]
prettyPair (a,b) = [T.pack $ show a ++ " => "] %+ PT.prettyLines b

prettyTriple :: (Show a, Pretty b, Pretty c) => a -> b -> c -> [Text]
prettyTriple a b c =
  [T.pack $ show a ++ " "]
    %+ (PT.intersperseBoxes [T.pack " , "] [PT.prettyLines b, PT.prettyLines c])
