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

import Data.IntMap              ( IntMap )
import qualified Data.IntMap as IntMap

import Data.Vector.Unboxed           ( Vector )
import qualified Data.Vector.Unboxed as Vector

import Debug.Trace

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

import Language.K3.Utils.Logger

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
type PEnv = BindingStackEnv (K3 Provenance)

-- | A provenance bindings environment for annotations,
--   indexed by annotation and attribute name.
type PAEnv = BindingEnv PMEnv
type PMEnv = BindingEnv (K3 Provenance, Bool)

-- | A provenance "pointer" environment
type PPEnv = IntMap (K3 Provenance)

-- | A lambda closure environment: ELambda UID => identifiers of closure variables.
type PLCEnv = IntMap [Identifier]
type PVPEnv = VarPosEnv

-- | A mapping of expression ids and provenance ids.
type EPMap = IntMap (K3 Provenance)

data ProvErrorCtxt = ProvErrorCtxt { ptoplevelExpr :: Maybe (K3 Expression)
                                   , pcurrentExpr  :: Maybe (K3 Expression) }

-- | A provenance inference environment.
data PIEnv = PIEnv {
               pcnt     :: Int,
               ppenv    :: PPEnv,
               penv     :: PEnv,
               paenv    :: PAEnv,
               pvpenv   :: PVPEnv,
               epmap    :: EPMap,
               pcase    :: [PMatVar],   -- Temporary storage stack for case variables.
               perrctxt :: ProvErrorCtxt,
               ptienv   :: AIVEnv
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
penv0 = BEnv.empty

plkup :: PEnv -> Identifier -> Either Text (K3 Provenance)
plkup env x = BEnv.slookup env x

plkupAll :: PEnv -> Identifier -> Either Text [K3 Provenance]
plkupAll env x = BEnv.lookup env x

pext :: PEnv -> Identifier -> K3 Provenance -> PEnv
pext env x p = BEnv.push env x p

psetAll :: PEnv -> Identifier -> [K3 Provenance] -> PEnv
psetAll env x l = BEnv.set env x l

pdel :: PEnv -> Identifier -> PEnv
pdel env x = BEnv.pop env x

pmem :: PEnv -> Identifier -> Either Text Bool
pmem env x = BEnv.member env x

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
paenv0 = BEnv.empty

pmenv0 :: PMEnv
pmenv0 = BEnv.empty

palkup :: PAEnv -> Identifier -> Identifier -> Either Text (K3 Provenance)
palkup env x y = BEnv.lookup env x >>= \menv -> BEnv.lookup menv y >>= return . fst

paext :: PAEnv -> Identifier -> Identifier -> K3 Provenance -> Bool -> PAEnv
paext env x y p l = BEnv.pushWith env BEnv.union x (BEnv.fromList [(y,(p,l))])

palkups :: PAEnv -> Identifier -> Either Text PMEnv
palkups env x = BEnv.lookup env x

paexts :: PAEnv -> Identifier -> PMEnv -> PAEnv
paexts env x ap' = BEnv.pushWith env BEnv.union x ap'


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

{- Error context helpers -}
perr0 :: ProvErrorCtxt
perr0 = ProvErrorCtxt Nothing Nothing

{- PIEnv helpers -}
pienv0 :: PVPEnv -> PIEnv
pienv0 vpenv = PIEnv 0 ppenv0 penv0 paenv0 vpenv epmap0 [] perr0 aivenv0

-- | Modifiers.
{-
mpiee :: (PEnv -> PEnv) -> PIEnv -> PIEnv
mpiee f env = env {penv = f $ penv env}

mpiep :: (PPEnv -> PPEnv) -> PIEnv -> PIEnv
mpiep f env = env {ppenv = f $ ppenv env}
-}

pilkupe :: PIEnv -> Identifier -> Either Text (K3 Provenance)
pilkupe env x = plkup (penv env) x

pilkupalle :: PIEnv -> Identifier -> Either Text [K3 Provenance]
pilkupalle env x = plkupAll (penv env) x

piexte :: PIEnv -> Identifier -> K3 Provenance -> PIEnv
piexte env x p = env {penv=pext (penv env) x p}

pisetalle :: PIEnv -> Identifier -> [K3 Provenance] -> PIEnv
pisetalle env x pl = env {penv=psetAll (penv env) x pl}

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
pilkupc env i = vplkuplc (pvpenv env) i

pilkupsc :: PIEnv -> Int -> Either Text IndexedScope
pilkupsc env i = vplkupsc (pvpenv env) i

pilkupvu :: PIEnv -> Int -> Either Text (UID, BVector)
pilkupvu env i = vplkupvu (pvpenv env) i

pipopcase :: PIEnv -> Either Text (PMatVar, PIEnv)
pipopcase env = case pcase env of
  [] -> Left $ T.pack "Uninitialized case matvar stack"
  h:t -> return (h, env {pcase=t})

pipushcase :: PIEnv -> PMatVar -> PIEnv
pipushcase env mv = env {pcase=mv:(pcase env)}

pierrtle :: PIEnv -> Maybe (K3 Expression)
pierrtle = ptoplevelExpr . perrctxt

pierrce :: PIEnv -> Maybe (K3 Expression)
pierrce = pcurrentExpr . perrctxt

piseterrtle :: PIEnv -> Maybe (K3 Expression) -> PIEnv
piseterrtle env eOpt = env {perrctxt = (perrctxt env) {ptoplevelExpr = eOpt}}

piseterrce :: PIEnv -> Maybe (K3 Expression) -> PIEnv
piseterrce env eOpt = env {perrctxt = (perrctxt env) {pcurrentExpr = eOpt}}

pilkupt :: PIEnv -> Int -> Either Text (TrIndex, Maybe Int)
pilkupt env x = aivlkup (ptienv env) x

piextt :: PIEnv -> TrIndex -> K3 Provenance -> K3 Provenance -> Either Text PIEnv
piextt env ti (tag -> PBVar (pmvptr -> i)) (tag -> PBVar (pmvptr -> j)) =
  Right $ env {ptienv = aivext (ptienv env) i ti (Just j)}

piextt env ti (tag -> PBVar (pmvptr -> i)) _ =
  Right $ env {ptienv = aivext (ptienv env) i ti Nothing}

piextt _ _ sp _ = Left $ PT.boxToString $ msg %$ PT.prettyLines sp
  where msg = [T.pack "Invalid pti env extension source:"]

pidelt :: PIEnv -> Int -> PIEnv
pidelt env i = env {ptienv=aivdel (ptienv env) i}

pimemt :: PIEnv -> Int -> Bool
pimemt env i = aivmem (ptienv env) i

picleart :: PIEnv -> PIEnv
picleart env = env {ptienv=aivenv0}

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

pifreshas :: PIEnv -> Identifier -> [(Identifier, UID, Bool)] -> (PMEnv, PIEnv)
pifreshas pienv n memN =
  let mkMemP lacc l p             = lacc++[(p,l)]
      extMemP (lacc,eacc) (i,u,l) = first (mkMemP lacc l) $ pifreshfp eacc i u
      (memP, npienv)              = foldl extMemP ([], pienv) memN
      memNP                       = BEnv.fromList $ zip (map (\(a,_,_) -> a) memN) memP
  in (memNP, piextas npienv n memNP)

pifreshats :: PIEnv -> Identifier -> [(Identifier, UID, Bool, TrIndex)] -> (PMEnv, PIEnv)
pifreshats pienv n memN =
  let mkMemP lacc l p               = lacc++[(p,l)]
      extMemP (lacc,eacc) (i,u,l,_) = first (mkMemP lacc l) $ pifreshfp eacc i u
      extTi         eacc (ti,(p,_)) = either (error "extTi") id $ piextt eacc ti p p
      (memP, npienv)                = foldl extMemP ([], pienv) memN
      npienv'                       = foldl extTi npienv $ zip (map (\(_,_,_,ti) -> ti) memN) memP
      memNP                       = BEnv.fromList $ zip (map (\(a,_,_,_) -> a) memN) memP
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
    _ -> Left $ PT.boxToString
              $ [T.pack $ unwords ["Invalid store on pointer", show n, show u]]
             %$ PT.prettyLines p'

pistoret :: PIEnv -> Identifier -> UID -> (TrIndex, K3 Provenance) -> Either Text PIEnv
pistoret pienv n u (ti, p) = do
  p' <- pilkupe pienv n
  case tag p' of
    PBVar mv | (pmvn mv, pmvloc mv) == (n,u) ->
      do
        nenv <- piextt pienv ti p' p
        return $ piextp nenv (pmvptr mv) p

    _ -> Left $ PT.boxToString
              $ [T.pack $ unwords ["Invalid store on pointer", show n, show u]]
             %$ PT.prettyLines p'


pistorea :: PIEnv -> Identifier -> [(Identifier, UID, K3 Provenance, Bool)] -> Either Text PIEnv
pistorea pienv n memP = do
  pmenv <- pilkupas pienv n
  foldM (storemem pmenv) pienv memP

  where storemem pmenv eacc (i,u,p,_) = BEnv.lookup pmenv i >>= \(p',_) -> store eacc i u p' p
        store eacc i u (tag -> PBVar mv) p
          | (pmvn mv, pmvloc mv) == (i,u) = return $ piextp eacc (pmvptr mv) p
        store _ i u p _ = invalidStore i u p

        invalidMem   i     = mkErr $ "Invalid store on annotation member" ++ i
        invalidStore i u p = Left $ PT.boxToString $ storeMsg i u %$ PT.prettyLines p
        storeMsg     i u   = [T.unwords $ map T.pack ["Invalid store on pointer", "@loc", show (i,u)]]

pistoreat :: PIEnv -> Identifier -> [(Identifier, UID, K3 Provenance, Bool, TrIndex)]
          -> Either Text PIEnv
pistoreat pienv n memP = do
  pmenv  <- pilkupas pienv n
  foldM (storemem pmenv) pienv memP

  where storemem pmenv eacc (i,u,p,_,ti) =
          BEnv.lookup pmenv i >>= \(p',_) -> store eacc i u ti p' p

        store eacc i u ti p'@(tag -> PBVar mv) p
          | (pmvn mv, pmvloc mv) == (i,u) = do
            neacc <- piextt eacc ti p' p
            return $ piextp neacc (pmvptr mv) p

        store _ i u _ p _ = invalidStore i u p

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

pichaset :: PIEnv -> Int -> Either Text TrIndex
pichaset env i = pilkupt env i >>= aux []
  where aux path (ti, Just ni) | ni `elem` path = return ti
                               | otherwise = pilkupt env ni >>= aux (ni:path)
        aux path (ti, Nothing) = return ti

piIndexChase :: PIEnv -> BVector -> TrIndex -> K3 Provenance -> Either Text (TrIndex, K3 Provenance)
piIndexChase env mask ti p@(tag -> PBVar pmv) = chasePtr [] ti pmv
  where chasePtr path ti pmv | (pmvptr pmv) `elem` path = return (ti, pbvar pmv)
        chasePtr path ti pmv@(pmvptr -> i) = do
          let npath = i:path
          p' <- pilkupp env i
          t' <- pilkupt env i
          case (t', tag p') of
            ((ti', Just j), PBVar pmv') | j == pmvptr pmv' -> chasePtr npath ti' pmv'
            ((ti', Nothing), _) -> chaseTree npath ti' p'
            (_,_) -> Left $ PT.boxToString $ [T.pack "Inconsistent index and provenance "]
                         %$ PT.prettyLines p'

        chaseTree path ti p = {-debugChaseTree ti p $-} indexMapRebuildTree (chaseProv path) mask ti p
        chaseProv path _ _ ti p@(tag -> PBVar pmv) = chasePtr path ti pmv
        chaseProv _ tich ch _ p = return (orti tich, replaceCh p ch)

        debugChaseTree ti p v = flip trace v $ T.unpack $ PT.boxToString
                                  $ [T.pack "indexMapRebuildTree chase"]
                                  %$ PT.prettyLines ti %$ PT.prettyLines p

piIndexChase _ _ _ p = Left $ PT.boxToString
                      $ [T.pack "Invalid index chase pointer "] %+ PT.prettyLines p

-- Capture-avoiding substitution of any free variable with the given identifier.
pisub :: PIEnv -> Identifier -> K3 Provenance -> K3 Provenance -> K3 Provenance -> TrIndex -> TrIndex
      -> Either Text ((K3 Provenance, K3 Provenance, TrIndex), PIEnv)
pisub pienv i dp dip sp dti sti = do
    (renv, _, rp, rip, rti) <- acyclicSub pienv emptyPtrSubs [] sti sp
    return ((rp, rip, rti), renv)
  where
    acyclicSub env subs _ _ (tag -> PFVar j) | i == j = return (env, subs, dp, dip, dti)

    acyclicSub env subs path ti p@(tag -> PBVar mv@(pmvptr -> j))
      | j `elem` path   = return (env, subs, p, p, ti)
      | isPtrSub subs j = getPtrSub subs j >>= \(p',ip',ti') -> return (env, subs, p', ip', ti')
      | otherwise       = do
                            sp'  <- pilkupp env j
                            (sti',_) <- pilkupt env j
                            subPtrIfDifferent env subs (j:path) mv sti' sp'

    -- TODO: we can short-circuit descending into the body if we
    -- are willing to stash the expression uid in a PLambda, using
    -- this uid to lookup i's presence in our precomputed closures.
    acyclicSub env subs _ ti p@(tag -> PLambda j) | i == j
      = return (env, subs, p, p, ti)

    -- Avoid descent into materialization points of shadowed variables.
    acyclicSub env subs _ ti p@(tag -> PMaterialize mvs) | i `elem` map pmvn mvs
      = return (env, subs, p, p, ti)

    -- Handle higher-order function application (two-place PApply nodes) where a PFVar
    -- in the lambda position is replaced by a PLambda
    acyclicSub env subs path ti p@(tnc -> (PApply Nothing, [lp@(tag -> PFVar _),argp])) = do
      let subp = [(head $ children ti, lp), (((children ti) !! 2), argp)]
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) subp
      case nch of
        [(lp',_,lti'), (argp',_,argti')] -> do
          ((p',ip',ti'),nenv') <- simplifyApply nenv Nothing lp' argp' lti' argti'
          return (nenv', nsubs, p', ip', ti')
        _ -> appSubErr p

    acyclicSub env subs path ti n@(Node _ ch) = do
      (nenv,nsubs,nch) <- foldM (rcr path) (env, subs, []) $ zip (children ti) ch
      let (npch, nipch, ntich) = unzip3 nch
      return (nenv, nsubs, replaceCh n npch, replaceCh n nipch, replaceCh ti ntich)

    rcr path (eacc,sacc,chacc) (cti, c) = do
      (e,s,nc,nic,nti) <- acyclicSub eacc sacc path cti c
      return (e,s,chacc++[(nc,nic,nti)])

    -- Preserve original pointer p unless any substitution actually occurred.
    -- We preserve sharing for the original pointer when substituting in deep structures containing PFVar i.
    -- This is implemented by creating a new pointer environment entry for the substitution, and
    -- then to track and apply pointer substitutions for PBVars referring to the old entry.
    -- This method is not tail-recursive.
    subPtrIfDifferent env subs path mv ti p = do
      (nenv, nsubs, np, nip, nti) <- acyclicSub env subs path ti p
      if p == np then return (nenv, nsubs, pbvar mv, nip, ti)
                 else addPtrSub nenv nsubs mv np nip nti

    -- Pointer substitutions, as a map of old PPtr => new PMatVar
    emptyPtrSubs   = IntMap.empty
    isPtrSub  ps j = IntMap.member j ps
    getPtrSub ps j = maybe (lookupErr j) (\(mv,ip,ti) -> return (pbvar mv, ip, ti)) $ IntMap.lookup j ps
    addPtrSub env ps mv p ip ti =
      let subp np op   = case tag op of
                           PBVar imv | imv == mv -> np
                           _ -> op
          (mvi, mviE)  = (pmvn mv, pilkupalle env $ pmvn mv)
          (p', env')   = pifreshbp env mvi (pmvloc mv) p
      in do
          env'' <- piextt env' ti p' p
          let (np, nenv) = case mviE of
                             Left _  -> (p', env'')
                             Right l -> (p',) $ pisetalle env'' mvi $ map (subp p') l
          case tag np of
            PBVar nmv -> return (nenv, IntMap.insert (pmvptr mv) (nmv, ip, ti) ps, np, ip, ti)
            _ -> freshErr np

    lookupErr j = Left $ T.pack $ "Could not find pointer substitution for " ++ show j
    freshErr  p = Left $ PT.boxToString $ [T.pack "Invalid fresh PBVar result "] %$ PT.prettyLines p
    appSubErr p = Left $ PT.boxToString $ [T.pack "Invalid apply substitution at: "] %$ PT.prettyLines p


{- Apply simplification -}

chaseLambda :: PIEnv -> [PPtr] -> TrIndex -> K3 Provenance -> Either Text [(K3 Provenance, TrIndex)]
chaseLambda _ _ ti p@(tag -> PLambda _) = return [(p, ti)]
chaseLambda _ _ ti p@(tag -> PFVar _)   = return [(p, ti)]

chaseLambda env path ti p@(tag -> PBVar (pmvptr -> i))
  | i `elem` path = return [(p, ti)]
  | otherwise     = do
                      cp <- pichase env p
                      cti <- pichaset env i
                      chaseLambda env (i:path) cti cp

chaseLambda env path ti (tnc -> (PApply _, [_,_,r]))   = chaseLambda env path ((children ti) !! 2) r
chaseLambda env path ti (tnc -> (PMaterialize _, [r])) = chaseLambda env path (head $ children ti) r
chaseLambda env path ti (tnc -> (PProject _, [_,r]))   = chaseLambda env path ((children ti) !! 1) r
chaseLambda env path ti (tnc -> (PGlobal _, [r]))      = chaseLambda env path (head $ children ti) r
chaseLambda env path ti (tnc -> (PChoice, r:_))        = chaseLambda env path (head $ children ti) r -- TODO: for now we use the first choice.

chaseLambda env path ti (tnc -> (PSet, rl)) = do
  cl <- mapM (\(cp,cti) -> chaseLambda env path cti cp) (zip rl $ children ti)
  return $ concat cl

chaseLambda env _ ti p = Left $ PT.boxToString $ [T.pack "Invalid application or lambda: "]
                             %$ PT.prettyLines p %$ [T.pack "Env:"] %$ PT.prettyLines env

chaseAppArg :: TrIndex -> K3 Provenance -> Either Text (K3 Provenance, TrIndex)
chaseAppArg ti (tnc -> (PApply _, [_,_,r])) = chaseAppArg ((children ti) !! 2) r
chaseAppArg ti (tnc -> (PMaterialize _, [r])) = chaseAppArg (head $ children ti) r
chaseAppArg ti p = return (p, ti)

simplifyApply :: PIEnv -> Maybe (K3 Expression) -> K3 Provenance -> K3 Provenance -> TrIndex -> TrIndex
              -> Either Text ((K3 Provenance, K3 Provenance, TrIndex), PIEnv)
simplifyApply pienv eOpt lp argp lti argti = do
  let debugChase lp' argp' = flip trace lp' (T.unpack $ PT.boxToString
                                 $ [T.pack "chaseLambda"]
                                %$ [T.pack "Expr:"]  %$ (maybe [] PT.prettyLines eOpt)
                                %$ [T.pack "LProv:"] %$ PT.prettyLines lp'
                                %$ [T.pack "AProv:"] %$ PT.prettyLines argp')
  uOpt              <- maybe (return Nothing) (\e -> uidOf e >>= return . Just) eOpt
  (argp', argti')   <- chaseAppArg argti argp
  manyLpti          <- chaseLambda pienv [] {-(debugChase lp argp)-} lti lp
  (manyLpti', nenv) <- foldM (doSimplify uOpt argp' argti') ([], pienv) manyLpti
  case manyLpti' of
    []          -> appLambdaErr lp
    [(p,ip,ti)] -> return ((p,ip,ti), nenv)
    _           -> let (pl, ipl, til) = unzip3 manyLpti'
                   in return ((pset pl, pset ipl, orti til), nenv)

  where
    doSimplify uOpt argp' argti' (chacc, eacc) (lp', lti') =
      case tnc lp' of
        (PLambda i, [bp]) ->
          case uOpt of
            Just uid -> do
              let (ip, neacc) = pifreshbp eacc i uid argp'
              imv <- pmv ip
              neacc' <- piextt neacc argti' ip argp'
              bti <- bodyTi lp' lti'
              ((rp, rip, rti), reacc) <- pisub neacc' i ip argp' bp argti' bti
              let mkapp p = papply (Just imv) lp argp p
              let rti' = tinode (rootbv $ rti) [lti, argti, rti]
              return (chacc ++ [(mkapp rp, mkapp rip, rti')], reacc)

            Nothing -> do
              bti <- bodyTi lp' lti'
              ((rp, rip, rti), reacc) <- pisub eacc i argp' argp' bp argti' bti
              let mkapp p = papply Nothing lp argp p
              let rti' = tinode (rootbv $ rti) [lti, argti, rti]
              return (chacc ++ [(mkapp rp, mkapp rip, rti')], reacc)

        -- Handle recursive functions, and forward declarations
        -- by using an opaque return value.
        (PBVar _, _) ->
          let rp  = papply Nothing lp argp ptemp
              rti = tileaf $ bzerobv $ Vector.length $ rootbv lti'
          in return (chacc ++ [(rp, rp, rti)], eacc)

        -- Handle higher-order and external functions as an unsimplified apply.
        (PFVar _, _) ->
          let rp = papplyExt lp argp
              rti = orti [lti', argti']
          in return (chacc ++ [(rp, rp, rti)], eacc)

        _ -> appLambdaErr lp'

    bodyTi _ (children -> [bti]) = return bti
    bodyTi p lti = Left $ PT.boxToString $ [T.pack "Invalid lambda ti "]
                        %$ PT.prettyLines p %$ PT.prettyLines lti

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = Left $ PT.boxToString $ [T.pack "No uid found for psimplifyapp on "] %$ PT.prettyLines e

    pmv (tag -> PBVar mv) = return mv
    pmv p = Left $ PT.boxToString $ [T.pack "Invalid provenance bound var: "] %$ PT.prettyLines p

    exprErr = flip (maybe []) eOpt $ \e -> PT.prettyLines e
    appLambdaErr p = Left $ PT.boxToString $ [T.pack "Invalid function provenance on:"]
                          %$ exprErr %$ [T.pack "Provenance:"] %$ PT.prettyLines p


simplifyApplyM :: Maybe (K3 Expression) -> K3 Provenance -> K3 Provenance -> TrIndex -> TrIndex
               -> PInfM (K3 Provenance, K3 Provenance, TrIndex)
simplifyApplyM eOpt lp argp lti argti = do
  env <- get
  ((p', ip', appti), nenv) <- liftEitherM $ simplifyApply env eOpt lp argp lti argti
  void $ put nenv
  return (p', ip', appti)


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
liftEitherM = either contextualizeErr return
  where contextualizeErr msg = do
          ctxt <- get >>= return . perrctxt
          let tle = maybe [T.pack "<nothing>"] PT.prettyLines $ ptoplevelExpr ctxt
          let cre = maybe [T.pack "<nothing>"] PT.prettyLines $ pcurrentExpr ctxt
          let ctxtmsg = PT.boxToString $ [msg] %$ [T.pack "On"] %$ cre %$ [T.pack "Toplevel"] %$ tle
          left ctxtmsg

{-
pifreshbpM :: Identifier -> UID -> K3 Provenance -> PInfM (K3 Provenance)
pifreshbpM n u p = get >>= return . (\env -> pifreshbp env n u p) >>= \(p',nenv) -> put nenv >> return p'
-}

pifreshsM :: Identifier -> UID -> PInfM (K3 Provenance)
pifreshsM n u = get >>= return . (\env -> pifreshs env n u) >>= \(p,env) -> put env >> return p

pifreshM :: Identifier -> UID -> K3 Provenance -> PInfM (K3 Provenance)
pifreshM n u p = get >>= return . (\env -> pifresh env n u p) >>= \(p',env) -> put env >> return p'

pifreshasM :: Identifier -> [(Identifier, UID, Bool)] -> PInfM PMEnv
pifreshasM n mems = get >>= return . (\env -> pifreshas env n mems) >>= \(p,env) -> put env >> return p

pifreshatsM :: Identifier -> [(Identifier, UID, Bool, TrIndex)] -> PInfM PMEnv
pifreshatsM n mems = get >>= return . (\env -> pifreshats env n mems) >>= \(p,env) -> put env >> return p

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

pistoretM :: Identifier -> UID -> (TrIndex, K3 Provenance) -> PInfM ()
pistoretM n u tip = get >>= liftEitherM . (\env -> pistoret env n u tip) >>= put

pistoreaM :: Identifier -> [(Identifier, UID, K3 Provenance, Bool)] -> PInfM ()
pistoreaM n memP = get >>= liftEitherM . (\env -> pistorea env n memP) >>= put

pistoreatM :: Identifier -> [(Identifier, UID, K3 Provenance, Bool, TrIndex)] -> PInfM ()
pistoreatM n memP = get >>= liftEitherM . (\env -> pistoreat env n memP) >>= put

pichaseM :: K3 Provenance -> PInfM (K3 Provenance)
pichaseM p = get >>= liftEitherM . flip pichase p

piIndexChaseM :: BVector -> TrIndex -> K3 Provenance -> PInfM (TrIndex, K3 Provenance)
piIndexChaseM mask ti p = get >>= \env -> liftEitherM (piIndexChase env mask ti p)

{-
pisubM :: Identifier -> K3 Provenance -> K3 Provenance -> PInfM (K3 Provenance, K3 Provenance)
pisubM i rep p = get >>= liftEitherM . (\env -> pisub env i rep p) >>= \((p',ip'),nenv) -> put nenv >> return (p', ip')
-}

pilkupcM :: Int -> PInfM [Identifier]
pilkupcM n = get >>= liftEitherM . flip pilkupc n

pilkupscM :: Int -> PInfM IndexedScope
pilkupscM n = get >>= liftEitherM . flip pilkupsc n

pilkupvuM :: Int -> PInfM (UID, BVector)
pilkupvuM n = get >>= liftEitherM . flip pilkupvu n

pipopcaseM :: () -> PInfM PMatVar
pipopcaseM _ = get >>= liftEitherM . pipopcase >>= \(mv,env) -> put env >> return mv

pipushcaseM :: PMatVar -> PInfM ()
pipushcaseM mv = get >>= return . flip pipushcase mv >>= put

pierrtleM :: PInfM (Maybe (K3 Expression))
pierrtleM = get >>= return . pierrtle

pierrceM :: PInfM (Maybe (K3 Expression))
pierrceM = get >>= return . pierrce

piseterrtleM :: Maybe (K3 Expression) -> PInfM ()
piseterrtleM eOpt = modify $ flip piseterrtle eOpt

piseterrceM :: Maybe (K3 Expression) -> PInfM ()
piseterrceM eOpt = modify $ flip piseterrce eOpt

pilkuptM :: Int -> PInfM (TrIndex, Maybe Int)
pilkuptM n = get >>= liftEitherM . flip pilkupt n

piexttM :: TrIndex -> K3 Provenance -> K3 Provenance -> PInfM ()
piexttM ti sp dp = get >>= \env -> liftEitherM (piextt env ti sp dp) >>= put

pideltM :: Int -> PInfM ()
pideltM n = get >>= \env -> return (pidelt env n) >>= put

pimemtM :: Int -> PInfM Bool
pimemtM n = get >>= return . flip pimemt n

picleartM :: () -> PInfM ()
picleartM _ = get >>= return . picleart >>= put


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
  vpenv <- variablePositions prog
  liftEitherTM $ runPInfE (pienv0 vpenv) $ doInference prog

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
initializeRcrDeclProv d@(tag -> DGlobal  n _ _) = uidOfD d >>= \u -> freshProvWithTI n u >> return d
initializeRcrDeclProv d@(tag -> DTrigger n _ _) = uidOfD d >>= \u -> freshProvWithTI n u >> return d
initializeRcrDeclProv d@(tag -> DDataAnnotation n _ mems) = mapM freshMems mems >>= pifreshatsM n . catMaybes >> return d
  where
    freshMems m@(Lifted      Provides mn _ _ mas) = memUID m mas >>= \u -> return (Just (mn, u, True, emptyti))
    freshMems m@(Attribute   Provides mn _ _ mas) = memUID m mas >>= \u -> return (Just (mn, u, False, emptyti))
    freshMems _ = return Nothing

initializeRcrDeclProv d = return d

freshProvWithTI :: Identifier -> UID -> PInfM ()
freshProvWithTI n u = do
  freshp <- pifreshsM n u
  piexttM emptyti freshp freshp

declProvWithTI :: Either (K3 Provenance) (K3 Provenance) -> PInfM (TrIndex, K3 Provenance)
declProvWithTI p = either fromUser fromInferred p
  where fromUser p' = tiOfExternalProv p' >>= return . (,p')
        fromInferred p'@(tag -> PBVar pmv) = pilkuptM (pmvptr pmv) >>= \(ti,_) -> return (ti,p')
        fromInferred p' = errorM $ PT.boxToString $ [T.pack "Invalid declaration provenance on"]
                                %$ PT.prettyLines p'

initProvWithTI :: K3 Type -> Maybe (K3 Expression) -> PInfM (TrIndex, K3 Provenance)
initProvWithTI t eOpt = maybe fromType fromExpr eOpt
  where fromType = provOfType [] t
        fromExpr e = inferProvenance e

-- | Structure-based declaration initialization.
--   Infer based on initializer, and update provenance pointer.
inferDeclProv :: K3 Declaration -> PInfM (K3 Declaration)
inferDeclProv d@(tag -> DGlobal n t eOpt) = do
  u <- uidOfD d
  (ti, p') <- case d @~ isDProvenance of
                Just (DProvenance prv) -> declProvWithTI prv
                _ -> initProvWithTI t eOpt
  void $ pistoretM n u $ (tisdup ti, pglobal n p')
  return d

inferDeclProv d@(tag -> DTrigger n _ e) = do
  u <- uidOfD d
  (ti, p') <- case d @~ isDProvenance of
                Just (DProvenance prv) -> declProvWithTI prv
                _ -> inferProvenance e
  void $ pistoretM n u $ (tisdup ti, pglobal n p')
  return d

inferDeclProv d@(tag -> DDataAnnotation n _ mems) = do
  mProvs <- mapM inferMemsProv mems
  void $ pistoreatM n $ catMaybes mProvs
  return d

  where
    inferMemsProv m@(Lifted    Provides mn mt meOpt mas) = inferMemberProv m True  mn mt meOpt mas
    inferMemsProv m@(Attribute Provides mn mt meOpt mas) = inferMemberProv m False mn mt meOpt mas
    inferMemsProv _ = return Nothing

    inferMemberProv mem lifted mn mt meOpt mas = do
      u  <- memUID mem mas
      (ti, mp) <- case find isDProvenance mas of
                    Just (DProvenance prv) -> declProvWithTI prv
                    _ -> initProvWithTI mt meOpt
      return $ Just (mn,u,mp,lifted,ti)

inferDeclProv d = return d

-- | Compute a provenance tree in a single pass, tracking expression-provenance associations.
--   Then, apply a second pass to attach associated provenances to each expression node.
inferExprProvenance :: K3 Expression -> PInfM (K3 Expression)
inferExprProvenance expr = inferProvenance expr >> substituteProvenance expr

-- | Computes a single extended provenance tree for an expression.
--   This includes an extra child at each PApply indicating the return value provenance
--   of the apply. The provenance associated with each subexpression is stored as a
--   separate relation in the environment rather than directly attached to each node.
data InfTD = InfTD { scuid :: Int, scszt :: Int, tdm :: PInfM () }

inferProvenance :: K3 Expression -> PInfM (TrIndex, K3 Provenance)
inferProvenance expr = do
  piseterrtleM $ Just expr
  tip <- biFoldMapIn1RebuildTree topdown sideways inferWithRule td0 emptyti expr
  return tip

  where
    td0 = InfTD (-1) 0 iu

    iu = return ()
    trt td u szd a = tdm td >> a >> return (InfTD u (scszt td + szd) iu)
    srt td udl ml = return (td, map (\((u,szd),m) -> InfTD u (scszt td + szd) m) $ zip udl ml)

    topdown :: InfTD -> K3 Expression -> K3 Expression -> PInfM (InfTD)
    topdown td _ e@(tag -> ELambda i) = do
      u <- uidOf e
      trt td (uidInt u) 1 (piexteM i (pfvar i) >> pushClosure (scuid td) u e)

    topdown td _ _ = trt td (scuid td) 0 iu

    sideways :: InfTD -> TrIndex -> K3 Provenance -> K3 Expression -> PInfM (InfTD, [InfTD])
    sideways td pti p e@(tag -> ELetIn  i) = do
      tdm td
      u <- uidOf e
      srt td [(uidInt u, 1)] [freshM i u pti p]

    sideways td pti p e@(tag -> ECaseOf i) = tdm td >> do
      u <- uidOf e
      srt td
          [(uidInt u, 1), (scuid td, 0)]
          [freshM i u (tisdup pti) $ poption p, pilkupeM i >>= pmv >>= pipushcaseM >> pideleM i]

    sideways td pti p e@(tag -> EBindAs b) = tdm td >> do
      u <- uidOf e
      case b of
        BIndirection i -> srt td [(uidInt u, 1)] [freshM i u (tisdup pti) $ pindirect p]

        BTuple is      -> srt td [(uidInt u, length is)] . (:[])
                            $ mapM_ (\(i,n) -> freshM n u (tisdup pti) $ ptuple i p) $ zip [0..length is -1] is

        BRecord ivs    -> srt td [(uidInt u, length ivs)] . (:[])
                            $ mapM_ (\(src,dest) -> freshM dest u (tisdup pti) $ precord src p) ivs

    sideways td _ _ (children -> ch) = tdm td >> srt td ul ml
      where (ul, ml) = unzip $ replicate (length ch - 1) ((scuid td, 0), iu)

    -- Provenance deduction logging.
    inferWithRule :: InfTD -> [TrIndex] -> [K3 Provenance] -> K3 Expression -> PInfM (TrIndex, K3 Provenance)
    inferWithRule td tich ch e = tdm td >> do
      piseterrceM $ Just e
      u  <- uidOf e
      (ruleTag, rp) <- infer td tich ch e
      localLog $ T.unpack $ showPInfRule ruleTag ch (snd rp) u
      return rp
      --where debugInfer tg a b = trace (T.unpack $ PT.boxToString $ [T.pack tg] %$ PT.prettyLines a) b

    -- Provenance computation
    infer :: InfTD -> [TrIndex] -> [K3 Provenance] -> K3 Expression -> PInfM (String, (TrIndex, K3 Provenance))
    infer td _ _ e@(tag -> EConstant _) = constti (scszt td) >>= \cti -> nrt "const" e cti ptemp
    infer td _ _ e@(tag -> EVariable i) = varErr e $ do
      vp <- pilkupeM i
      vti <- varti (scuid td) vp
      nrt "var" e vti vp

    infer _ [ti] [p] e@(tag -> ELambda i) = do
      u <- uidOf e
      popClosure u e
      pideleM i
      nrt "lambda" e (tising (truncateLast $ rootbv ti) ti) $ plambda i p

    -- Return a papply with three children: the lambda, argument, and return value provenance.
    infer _ [lti,argti] [lp, argp] e@(tag -> EOperate OApp) = do
      (p, ip, appti) <- simplifyApplyM (Just e) lp argp lti argti
      void $ piextmM e p
      return ("apply", (appti, ip))

    infer _ [ti] [psrc] e@(tnc -> (EProject i, [esrc])) =
      case esrc @~ isEType of
        Just (EType t) ->
          case tag t of
            TCollection -> do
              (mti, mp) <- collectionMemberProvenance i psrc esrc t
              orrt "cproject" e [ti, mti] mp

            TRecord ids ->
              case tnc psrc of
                (PData _, pdch) -> do
                  idx <- maybe (memErr i esrc) return $ elemIndex i ids
                  let tich = [ti, (children ti) !! idx]
                  orrt "rproject" e tich $ pproject i psrc $ Just $ pdch !! idx
                (_,_) -> strt "project" e ti $ pproject i psrc Nothing

            _ -> prjErr esrc
        _ -> prjErr esrc

    infer td tich pch e@(tag -> EIfThenElse) =
      let (rti, rp) = psetTI (szbv $ scszt td) (tail tich) $ tail pch
      in nrt "if-then-else" e rti rp

    infer _ tich   pch   e@(tag -> EOperate OSeq) = nrt "seq" e (last tich) $ last pch
    infer _ [ti]   [p]   e@(tag -> EAssign i)     = strt "assign" e ti $ passign i p
    infer _ [_,ti] [_,p] e@(tag -> EOperate OSnd) = strt "send"   e ti $ psend p

    infer td [_,bti] [_,lb] e@(tag -> ELetIn i) = do
      mv <- pilkupeM i >>= pmv
      void $ pideleM i
      void $ piextmM e $ pmaterialize [mv] lb
      let sz = scszt td + 1
      let mask = lastbv sz
      (nbti,nlb) <- pruneEscapes mask bti lb
      return ("let-in", (nbti, nlb))

    infer td [_,bti] [_,bb] e@(tag -> EBindAs b) = do
      let sz = scszt td
      let localsz = length $ bindingVariables b
      let mask = suffixbv sz (sz + localsz)
      mvs <- mapM pilkupeM (bindingVariables b) >>= mapM pmv
      void $ mapM_ pideleM $ bindingVariables b
      void $ piextmM e $ pmaterialize mvs bb
      (nbti,nbb) <- pruneEscapes mask bti bb
      return ("bind-as", (nbti, nbb))

    infer td [_,sti,nti] [_,s,n] e@(tag -> ECaseOf _) = do
      casemv <- pipopcaseM ()
      let sz = scszt td + 1
      let mask = lastbv sz
      void $ piextmM e $ pset [pmaterialize [casemv] s, n]
      (nsti,ns) <- pruneEscapes mask sti s
      let (rti, rp) = psetTI (szbv sz) [nsti,nti] [ns,n]
      return ("case-of", (rti, rp))

    -- Data constructors.
    infer _ tich pch e@(tag -> ESome)       = orrt "some"   e tich $ pdata Nothing pch
    infer _ tich pch e@(tag -> EIndirect)   = orrt "ind"    e tich $ pdata Nothing pch
    infer _ tich pch e@(tag -> ETuple)      = orrt "tuple"  e tich $ pdata Nothing pch
    infer _ tich pch e@(tag -> ERecord ids) = orrt "record" e tich $ pdata (Just ids) pch

    -- Operators and untracked primitives.
    infer td tich pch e@(tag -> EOperate op) = uncurry (nrt (show op) e) $ pderivedTI (scszt td) tich pch
    infer td tich pch e@(tag -> EAddress)    = uncurry (nrt "address" e) $ pderivedTI (scszt td) tich pch

    -- TODO: ESelf
    infer _ _ _ e = inferErr e

    nrt tg e ti p = piextmM e p >> return (tg, (ti, p))
    strt tg e ti p = piextmM e p >> return (tg, (unaryti ti, p))
    orrt tg e tich p = piextmM e p >> return (tg, (orti tich, p))

    -- | Closure variable management
    pushClosure su u@(UID i) e = pilkupcM i >>= mapM_ (liftClosureVar su u e)
    popClosure       (UID i) e = pilkupcM i >>= mapM_ (lowerClosureVar e)

    liftClosureVar su u _ n = pilkupeM n >>= \p -> pideleM n >> varti su p >>= \pti -> freshM n u pti p
    lowerClosureVar e n = pilkupeM n >>= \p -> unwrapClosure e p >>= \p' -> pideleM n >> piexteM n p'

    unwrapClosure _ (tag -> PBVar mv) = pilkuppM (pmvptr mv)
    unwrapClosure e p = errorM $ PT.boxToString
                           $ [T.pack "Invalid closure variable "] %+ PT.prettyLines p
                          %$ [T.pack "at expr:"] %$ PT.prettyLines e

    freshM i u ti p = pifreshM i u p >>= \sp -> piexttM ti sp p

    pruneEscapes :: BVector -> TrIndex -> K3 Provenance -> PInfM (TrIndex, K3 Provenance)
    pruneEscapes mask ti p = {-debugPrune ti p $-} indexMapRebuildTree prune mask ti p
      where prune _ _ ti p@(tag -> PBVar pmv) = piIndexChaseM mask ti p
            prune tich ch _ p = return (orti tich, replaceCh p ch)
            debugPrune ti p v = flip trace v $ T.unpack $ PT.boxToString
                                  $ [T.pack "indexMapRebuildTree prune"]
                                  %$ PT.prettyLines ti %$ PT.prettyLines p

    {-
    opruneEscapes :: [PMatVar] -> K3 Provenance -> PInfM (K3 Provenance)
    opruneEscapes mvl p = modifyTree prune p
      where prune p@(tag -> PBVar pmv) | pmv `elem` mvl = do
              p' <- pichaseM p
              if p == p' then return p else opruneEscapes mvl p'
            prune p = return p

    opruneApplyCh :: K3 Provenance -> PInfM (K3 Provenance)
    opruneApplyCh (tnc -> (PApply (Just pmv), [f, a, r])) = do
      nr <- opruneEscapes [pmv] r
      return $ papply (Just pmv) f a nr

    opruneApplyCh (tnc -> (PSet, ch)) = mapM opruneApplyCh ch >>= return . pset
    opruneApplyCh p = return p
    -}

    {- TrIndex helpers -}
    constti varSz = return . tileaf $ zerobv varSz

    varti su (tag -> PBVar pmv) = svarti su (pmvn pmv)
    varti su (tag -> PFVar n) = svarti su n
    varti _ p = vartiKindErr p

    svarti su n = pilkupscM su >>= \idxsc ->
      case n `elemIndex` (scids idxsc)  of
        Just i -> return . tileaf $ singbv i $ scsz idxsc
        Nothing -> return . tileaf $ zerobv $ scsz idxsc

    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = errorM $ PT.boxToString $ [T.pack "No uid found for pexprinf on "] %+ PT.prettyLines e

    uidInt (UID i) = i
    uidInt _ = error "Invalid UID for uidInt"

    pmv (tag -> PBVar mv) = return mv
    pmv p = errorM $ PT.boxToString $ [T.pack "Invalid provenance bound var: "] %$ PT.prettyLines p

    isPTemporary (tag -> PTemporary) = True
    isPTemporary _ = False

    inferErr e = errorM $ PT.boxToString $ [T.pack "Could not infer provenance for "] %$ PT.prettyLines e
    varErr e   = reasonM (\s -> T.unlines [s, T.pack "Variable access on", PT.pretty e])
    prjErr e   = errorM $ PT.boxToString $ [T.pack "Invalid type on "] %+ PT.prettyLines e

    memErr i e = errorM $ PT.boxToString $  [T.unwords $ map T.pack ["Failed to project", i, "from"]]
                                         %$ PT.prettyLines e

    vartiKindErr p = errorM $ PT.boxToString $ [T.pack "Invalid closure variable binding "]
                           %$ PT.prettyLines p

    {-
    svartiErr su i l p =
      errorM $ PT.boxToString $
        [T.pack $ "No index for " ++ i ++ " in " ++ show l ++ " at scope " ++ show su]
        %$ PT.prettyLines p
    -}

    diffErr msg p1 p2 = errorM $ PT.boxToString $ [T.pack $ "Different prunings:" ++ msg]
                          %$ PT.prettyLines p1
                          %$ PT.prettyLines p2

    diffErrMTI msg mask ti p1 p2 =
      errorM $ PT.boxToString $ [T.pack $ "Different prunings:" ++ msg]
            %$ [T.pack $ showbv mask]
            %$ PT.prettyLines ti
            %$ PT.prettyLines p1
            %$ PT.prettyLines p2

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
collectionMemberProvenance :: Identifier -> K3 Provenance -> K3 Expression -> K3 Type
                           -> PInfM (TrIndex, K3 Provenance)
collectionMemberProvenance i psrc e t =
  let annIds = namedTAnnotations $ annotations t in do
    memsEnv <- mapM pilkupasM annIds >>= return . BEnv.unions
    (mp, lifted) <- liftEitherM $ BEnv.lookup memsEnv i
    case tag mp of
      PBVar pmv -> if not lifted then attrErr
                   else do
                     (ti,_) <- pilkuptM (pmvptr pmv)
                     return (ti, pproject i psrc $ Just mp)
      _ -> attrErr

  where memErr  = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Unknown projection of ", i, "on"]]           %+ PT.prettyLines e
        attrErr = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Invalid attribute projection of ", i, "on"]] %+ PT.prettyLines e

provOfType :: [Identifier] -> K3 Type -> PInfM (TrIndex, K3 Provenance)
provOfType args t | isTFunction t =
   case tnc t of
    (TForall _, [ch])      -> provOfType args ch
    (TFunction, [_, retT]) -> let a = mkArg (length args + 1) in do
                                (ti,p) <- provOfType (args++[a]) retT
                                return (tising (zerobv $ length args) ti, plambda a p)
    _ -> errorM $ PT.boxToString $ [T.pack "Invalid function type"] %+ PT.prettyLines t
  where mkArg i = "__arg" ++ show i

provOfType [] _   = return (emptyti, ptemp)
provOfType args _ = return (tinode v0 $ map (const $ tileaf v0) args, pderived $ map pfvar args)
  where v0 = zerobv $ length args

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

tiOfExternalProv :: K3 Provenance -> PInfM TrIndex
tiOfExternalProv p = aux 0 p
  where aux sz (tnc -> (PLambda _, [b])) = sing sz b
        aux sz (tnc -> (PMaterialize _, [b])) = sing sz b
        aux sz (Node _ ch) = mapM (aux sz) ch >>= return . tinode (zerobv sz)

        sing sz n = aux (sz+1) n >>= return . tising (zerobv sz)

{- Provenance environment pretty printing -}
instance Pretty PIEnv where
  prettyLines (PIEnv c p e a vp ep _ errc _) =
    [T.pack $ "PCnt: " ++ show c] ++
    [T.pack "PEnv: "  ]  %$ (PT.indent 2 $ PT.prettyLines e)  ++
    [T.pack "PPEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines p)  ++
    [T.pack "PAEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines a)  ++
    [T.pack "PLCEnv: "]  %$ (PT.indent 2 $ PT.prettyLines $ lcenv vp) ++
    [T.pack "EPMap: " ]  %$ (PT.indent 2 $ PT.prettyLines ep) ++
    [T.pack "PErrCtxt: "]
      %$ (PT.indent 2 $ [T.pack "Toplevel:"] %$ (maybe [T.pack "<nothing>"] PT.prettyLines $ ptoplevelExpr errc)
                     %$ [T.pack "Current:"]  %$ (maybe [T.pack "<nothing>"] PT.prettyLines $ pcurrentExpr errc))

instance Pretty (IntMap (K3 Provenance)) where
  prettyLines pp = IntMap.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] pp

instance Pretty PEnv where
  prettyLines pe = BEnv.foldl (\acc k v -> acc ++ prettyFrame k v) [] pe
    where prettyFrame k v = concatMap prettyPair $ flip zip v $ replicate (length v) k

instance Pretty PAEnv where
  prettyLines pa = BEnv.foldl (\acc k v -> acc ++ prettyPair (k,v)) [] pa

instance Pretty PMEnv where
  prettyLines pm = BEnv.foldl (\acc k v -> acc ++ prettyPair (k, fst v)) [] pm

instance Pretty PLCEnv where
  prettyLines plc = IntMap.foldlWithKey (\acc k v -> acc ++ [T.pack $ show k ++ " => " ++ show v]) [] plc

prettyPair :: (Show a, Pretty b) => (a,b) -> [Text]
prettyPair (a,b) = [T.pack $ show a ++ " => "] %+ PT.prettyLines b
