{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TupleSections #-}

-- | Machinery for making decisions about C++ level materialization for K3.
module Language.K3.Codegen.CPP.Materialization where

import Prelude hiding (concat, mapM, mapM_, or, and)

import Control.Arrow

import Control.Monad.Identity (Identity(..), runIdentity)
import Control.Monad.State (StateT(..), MonadState(..), modify, gets)
import Control.Monad.Reader
import Control.Monad.Writer

import Language.K3.Analysis.Core

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Inference (PIEnv(..))

import qualified Language.K3.Analysis.Provenance.Constructors as P

import Language.K3.Analysis.SEffects.Core
import Language.K3.Analysis.SEffects.Inference (FIEnv(..))

import Language.K3.Codegen.CPP.Materialization.Hints

import Data.Foldable

import Data.Maybe (fromMaybe, maybeToList, fromJust)
import Data.Ord (comparing)
import Data.List (elemIndex, sortBy, tails, zip4)
import Data.Tree

import qualified Data.Map as M
import qualified Data.IntMap as I

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Common hiding (getUID)

import Text.Printf

-- Types

-- Basic operating monad. Report entries are emitted through a writer for deferred printing, scoped
-- state is stored in a reader, while unscoped state is stored in state. Errors are reported through
-- except.
type MaterializationM = WriterT [(Int, MReport)] (ReaderT MScope (StateT MState (ExceptT MError Identity))

-- Reporting
data MReport
  = MRLambda { currentlyGlobal :: Bool
             , argReadOnly :: Bool
             , nrvoRequired :: Bool
             , argD :: Decision
             , closureReport :: [(Provenance, Provenance, Bool, Bool, Decision)]
             , downstreamsConsidered :: [Downstream]
             }
  | MRApply
  | MRBind
  | MRLet
  | MRCase
  | MRSend
  | MRRecord [(Identifier, Bool, [Downstream], Decision)]
 deriving (Eq, Read, Show)

dumpMReporteport :: Bool
dumpMReporteport = False

formatMR :: [(Int, MReport)] -> IO ()
formatMR r = putStrLn "Materialization Report" >> mapM_ (uncurry formatR) r
  where
   formatR :: Int -> MReport -> IO ()
   formatR u t = case t of
     MRLambda cg aro nr ad cr dc -> do
       printf "Materialized lambda at UID %d\n" u
       printf "  Lambda is part of currently global chain: %s\n" (show cg)
       printf "  Argument is read-only inside lambda: %s\n" (show aro)
       printf "  Lambda requires manual NRVO: %s\n" (show nr)
       printf "  Incoming decision for argument: %s\n" (show $ inD ad)
       printf "  Outgoing decision for argument: %s\n" (show $ outD ad)
       printf "  Downstreams considered for all decisions: %s\n" (show $ map (getUID . fst) dc)
       unless (null cr) $ do
         printf "  Closure Variables:\n"
         forM_ cr $ \(ip, op, hw, b, d) -> do
           printf "    Variable: %s\n" $ case ip of
             PBVar (PMatVar { pmvn }) -> pmvn
             _ -> error "Incorrect Closure Provenance"
           printf "      Captured from: %s\n" $ case op of
             PBVar (PMatVar { pmvloc = (UID i) }) -> "identifier bound at " ++ show i
             PFVar _ -> "free variable in parent scope."
           printf "      Is written to inside lambda: %s\n" (show hw)
           printf "      Moveable: %s\n" (show b)
           printf "      Decision: %s\n" (show $ inD d)
     MRRecord imnds -> do
       printf "Materialized record at UID %d\n" u
       forM_ imnds $ \(i, mn, ds, d) -> do
         printf "  Field %s:\n" i
         printf "    Moveable: %s\n" (show mn)
         printf "      Downstreams considered: %s\n" (show $ map (getUID . fst) ds)
         printf "    Decision: %s\n" (show $ inD d)

reportAt :: Int -> MReport -> MaterializationM ()
reportAt u r = tell [(u, r)]

-- Scoping
data MScope = MScope { downstreams :: [Downstream]
                     , nearestBind :: Int

                     , pEnv :: PIEnv
                     , fEnv :: FIEnv
                     }

type Contextual a = (a, Maybe Int)

-- A downstream is a technically just an expression, but we need to augment with the UID of the
-- nearest bind point above the expression in context. This indicates the expression which was the
-- binding point of any free variable in the downstream whose binding point itself was not in the
-- downstream.
type Downstream = Contextual (K3 Expression)

-- Run a materialization with an additional set of downstreams. Any expression with more than one
-- subexpression should use this to make the materialization system aware of order-of-execution.
withDownstreams :: [Downstream] -> MaterializationM a -> MaterializationM a
withDownstreams ds = local (\s -> s { downstreams = ds ++ downstreams s })

-- Run a materialization computation with a different nearest binding expression. This is almost
-- always done at lambdas, by setting the nearest bind to the lambda's own UID.
withNearestBind :: Int -> MaterializationM a -> MaterializationM a
withNearestBind bi = local (\s -> s { nearestBind = bi })

chasePPtr :: PPtr -> MaterializationM (K3 Provenance)
chasePPtr p = gets pEnv >>= fromMaybe (throwE "Invalid pointer in provenance chase") . I.lookup p

-- State
data MState = MState { constraints :: CTable, dependencies :: RTable }

-- Generic type of table keyed on expression UID and identifier at that expression.
type Table = I.IntMap (M.Map (Contextual Identifier))

insertN :: Int -> Contextual Identifier -> a -> Table a -> Table a
insertN u ci a = I.insertWith (M.insert ci a) (M.insert ci a M.empty)

-- Table of concrete decisions.
type DTable = Table Decision

-- Table of constrained decisions.
type CTable = Table (DTable -> Decision)

-- Table of running constraint satisfaction.
type RTable = Table (S.Set (Int, Identifier))

-- Lodge a constrained decision, given the location and name of the decision target, a list of
-- dependencies, and the constraint itself.
lodge :: Int -> Identifier -> [(Int, Identifier)] -> (DTable -> Decision) -> MaterializationM ()
lodge u i (S.fromList -> ds) f = do
  nb <- asks nearestBind
  let upI = M.insert (i, nb)
  modify $ \s ->
    s { constraints = insertN u (i, nb) f (constraints s)
      , dependencies = insertN u (i, nb) ds (dependencies s)
      }

-- Lodge an independent decision.
lodgeI Int -> Identifier -> Decision -> MaterializationM ()
lodgeI u i d = lodge u i [] (const d)

-- Errors
newtype MError = MError String

-- Expression accessors

eUID :: K3 Expression -> MaterializationM Int
eUID e = let EUID (UID u)
                 = fromMaybe (throwE "Attempted to get UID of expression " ++ show e) (e @~ isEUID)
         in u

ePrv :: K3 Expression -> MaterializationM (K3 Provenance)
ePrv e = let (EProvenance p)
                 = fromMaybe (eUID e >>= \u throwE "Attempted to get provenance of expression " ++ show u)
                             (e @~ isEProvenance)
         in p

eEff :: K3 Expression -> MaterializationM (K3 Effect)
eEff e = let ESEffect f
                 = fromMaybe (eUID e >>= \u -> throwE "Attempted to get effects of expression " ++ show u)
                             (e @~ isESEffect)
         in f

eEfs :: K3 Expression -> MaterializationM (K3 Effect)
eEfs e = let EFStructure f
                 = fromMaybe (eUID e >>= \u -> throwE "Attempted to get effect structure of expression " ++ show u)
                             (e @~ isEFStructure)
         in f

pmvloc' :: PMatVar -> Int
pmvloc' pmv = let UID u = pmvloc pmv in u

type ProxyProvenance = (K3 Provenance, Maybe (Identifier, Int))

makeCurrentPP :: K3 Provenance -> MaterializationM ProxyProvenance
makeCurrentPP p = (p,) <$> gets currentActivePFVar

-- Table Construction/Attachment

runMaterializationM :: MaterializationM a -> MState -> ((a, [(Int, MReport)]), MState)
runMaterializationM m s = runIdentity $ runStateT (RrunWriterT m) s

optimizeMaterialization :: (PIEnv, FIEnv) -> K3 Declaration -> IO (K3 Declaration)
optimizeMaterialization (p, f) d = do
  let ((nd, report), _) = runMaterializationM (materializationD d) (MState I.empty p f [] True Nothing M.empty)
  when dumpMReporteport $ formatMR report
  return nd

materializationD :: K3 Declaration -> MaterializationM (K3 Declaration)
materializationD (Node (d :@: as) cs)
  = case d of
      DGlobal i t me -> do
        setCurrentGlobal True
        me' <- traverse materializationE me
        Node (DGlobal i t me' :@: as) <$> cs'
      DTrigger i t e -> materializationE e >>= \e' -> Node (DTrigger i t e' :@: as) <$> cs'
      DRole i -> Node (DRole i :@: as) <$> cs'
      _ -> Node (d :@: as) <$> cs'
 where
   cs' = mapM materializationD cs

materializationE :: K3 Expression -> MaterializationM (K3 Expression)
materializationE e@(Node (t :@: as) cs)
  = case t of
      ERecord is -> do
        let decisionForField c ds = withLocalDS ds $ do
              rf <- materializationE c
              mn <- makeCurrentPP (getProvenance c) >>= isMoveableNow
              return (if mn then defaultDecision { inD = Moved } else defaultDecision, rf, mn)

        let cds = reverse $ zip cs (tail $ tails cs)
        (decisions, fs, mns) <- unzip3 . reverse <$> mapM (uncurry decisionForField) cds
        zipWithM_ (setDecision (getUID e)) is decisions
        ds <- dLookupAll (getUID e)
        -- let (is', cs') = unzip . sortBy (comparing fst) $ zip is cs
        -- (decisions, fs, mns) <- unzip3 <$> zipWithM decisionForField (reverse cs') (reverse $ tail $ tails cs')
        -- zipWithM_ (setDecision (getUID e)) is' decisions
        -- ds <- dLookupAll (getUID e)
        -- let fs' = map (\i -> fs !! (fromJust $ elemIndex i $ reverse is')) is
        -- cs'' <- zip cs' . repeat <$> gets currentActivePFVar
        -- say (getUID e) $ MRRecord $ zip4 is' mns (map (eds ++) $ tail $ tails cs'') decisions
        return (Node (t :@: (EMaterialization ds:as)) fs)

      EOperate OApp -> do
        let [f, x] = cs
        x'' <- materializationE x
        f' <- withLocalDS [x] $ materializationE f

        -- Manually reset external move capture decisions for fold lambdas.
        when (tag f' == EProject "fold") $ do
          case tag x'' of
            ELambda i -> do
              mFD <- dLookupAllWithBindings (getUID x'')
              void $ flip M.traverseWithKey mFD $ \(u, mi) -> \d -> do
                when (u /= i && inD d == Moved) $ setFullDecision (getUID x'') (u, mi) (d { inD = Copied })

        ds' <- dLookupAll (getUID x'')
        let x' = let Node (t' :@: as') cs' = x'' in Node (t' :@: (EMaterialization ds':as')) cs'

        let applicationEffects = getFStructure e
        let executionEffects = getEffects e
        let formalParameter =
              case applicationEffects of
                (tag -> FApply (Just fmv)) -> fmv
                _ -> error "Invalid effect structure"

        conservativeDoMoveLocal <- hasWriteInIF (fmvn formalParameter) executionEffects

        conservativeDoMoveReturn <-
          case f' of
            (tag &&& children -> (ELambda i, [f''])) ->
              case f'' of
                (tag -> ELambda _) -> do
                  let f''id = getUID f''
                  f''d <- dLookup f''id i
                  return $ inD f''d == Moved
                _ -> return False
            _ -> return False

        moveable <- makeCurrentPP (getProvenance x) >>= isMoveableNow
        referenceable <- makeCurrentPP (getProvenance x) >>= isReferenceableNow

        let applicationDecision d =
              if (conservativeDoMoveLocal || conservativeDoMoveReturn) && moveable
                then d { inD = Moved }
                else if (not conservativeDoMoveLocal && referenceable)
                       then d { inD = Referenced }
                       else d

        setDecision (getUID e) "" $ applicationDecision defaultDecision

        decisions <- dLookupAll (getUID e)

        return (Node (t :@: (EMaterialization decisions:as)) [f', x'])

      EOperate OSnd -> do
        let [h, m] = cs
        m' <- materializationE m
        h' <- withLocalDS [m] $ materializationE h

        moveable <- makeCurrentPP (getProvenance m') >>= isMoveableNow
        let decision = if moveable then defaultDecision { inD = Moved } else defaultDecision
        setDecision (getUID e) "" decision
        ds <- dLookupAll (getUID e)

        return (Node (t :@: (EMaterialization ds:as)) [h', m'])

      EOperate _ -> do
        case cs of
          [x] -> do
            x' <- materializationE x
            return $ (Node (t :@: as)) [x']
          [x, y] -> do
            y' <- materializationE y
            x' <- withLocalDS [y] $ materializationE x
            return $ (Node (t :@: as)) [x', y']
          _ -> error "Invalid argument form for operator."

      ELambda x -> do
        cg <- currentGlobal <$> get
        let fp = getProvenance e
        let closureSymbols = case tag fp of
              PLambda _ mvs -> mvs
              _ -> error "Invalid provenance on lambda form."

        when cg $ case tag (head cs) of
                    ELambda _ -> return ()
                    _ -> setCurrentGlobal False

        [b] <- withLocalBindings (x : map pmvn closureSymbols) (getUID e) $
               withActivePFVar x (getUID e) $ mapM materializationE cs

        setCurrentGlobal cg

        let returnedProvenance =
              case getProvenance e of
                (tag &&& children -> (PLambda _ _, [rp])) -> rp
                _ -> error "Invalid provenance structure"

        readOnly <- not <$> hasWriteInP (P.pfvar x) b

        let nrvoProvenance q =
              case q of
                (tag -> PFVar _) -> return True
                (tag -> PBVar _) -> not <$> isGlobalP q
                (tag -> PSet) -> anyM nrvoProvenance (children q)
                _ -> return False

        nrvo <- nrvoProvenance returnedProvenance

        let readOnlyDecision d = if readOnly then d { inD = ConstReferenced } else d
        let nrvoDecision d = if nrvo then d { outD = Moved } else d

        setDecision (getUID e) x $ readOnlyDecision $ nrvoDecision $ defaultDecision

        cr <- forM closureSymbols $ \s -> do
          let innerProxyProvenance = P.pbvar s
          outerProxyProvenance <- fromMaybe (error "Dangling closure provenance.") <$> pLookup (pmvptr s)
          closureHasWrite <- hasWriteInP innerProxyProvenance b
          moveable <- makeCurrentPP outerProxyProvenance >>= isMoveableNow
          let closureDecision d =
                if closureHasWrite
                  then if moveable
                         then d { inD = Moved }
                         else d { inD = Copied }
                  else if cg
                         then d { inD = Moved }
                         else d { inD = Referenced }

          let decisionSetter = case tag outerProxyProvenance of
                PBVar _ -> setDecision
                PFVar _ -> setNullDecision

          decisionSetter (getUID e) (pmvn s) $ closureDecision defaultDecision

          return ( tag innerProxyProvenance
                 , tag outerProxyProvenance
                 , closureHasWrite
                 , moveable
                 , closureDecision defaultDecision
                 )
        decisions <- dLookupAll (getUID e)

        eds <- downstreams <$> get
        say (getUID e) $ MRLambda cg readOnly nrvo (M.findWithDefault defaultDecision x decisions) cr eds
        return $ (Node (t :@: (EMaterialization decisions:as)) [b])

      EBindAs b -> do
        let [x, y] = cs
        let newBindings = case b of { BIndirection i -> [i]; BTuple is -> is; BRecord iis -> snd (unzip iis) }
        y' <- withLocalBindings newBindings (getUID e) $ materializationE y
        x' <- withLocalDS [y] (materializationE x)

        let xp = getProvenance x
        writeMention <- hasWriteInP xp y'

        let referenceBind d = if not writeMention then d { inD = Referenced, outD = Referenced } else d

        case b of
          BIndirection i -> setDecision (getUID e) i $ referenceBind defaultDecision
          BTuple is -> mapM_ (\i -> setDecision (getUID e) i $ referenceBind defaultDecision) is
          BRecord iis -> mapM_ (\(_, i) -> setDecision (getUID e) i $ referenceBind defaultDecision) iis

        decisions <- dLookupAll (getUID e)
        return (Node (t :@: (EMaterialization decisions:as)) [x', y'])

      ECaseOf i -> do
        let [x, s, n] = cs
        n' <- materializationE n
        s' <- withLocalBindings [i] (getUID e) $ materializationE s
        x' <- withLocalDS [s, n] (materializationE x)

        let xp = getProvenance x

        -- TODO: Slightly conservative, although it takes reasonably unusual code to trigger
        -- those cases.
        noMention <- do
          sMention <- (||) <$> hasReadInP xp s' <*> hasWriteInP xp s'
          nMention <- (||) <$> hasReadInP xp n' <*> hasWriteInP xp n'

          return $ not (sMention || nMention)

        let referenceBind d = if noMention then d { inD = Referenced, outD = Referenced } else d

        setDecision (getUID e) i $ referenceBind defaultDecision
        decisions <- dLookupAll (getUID e)
        return (Node (t :@: (EMaterialization decisions:as)) [x', s', n'])

      ELetIn i -> do
        let [x, b] = cs
        b' <- withLocalBindings [i] (getUID e) $ materializationE b
        (x', d) <- withLocalDS [b] $ do
          x'' <- materializationE x
          m <- makeCurrentPP (getProvenance x'') >>= isMoveableNow
          return (x'', if m then defaultDecision { inD = Moved } else defaultDecision)

        setDecision (getUID e) i d
        decisions <- dLookupAll (getUID e)
        return (Node (t :@: (EMaterialization decisions:as)) [x', b'])

      _ -> (Node (t :@: as)) <$> mapM materializationE cs

-- Queries

anyM :: (Functor m, Applicative m, Monad m) => (a -> m Bool) -> [a] -> m Bool
anyM f xs = or <$> mapM f xs

allM :: (Functor m, Applicative m, Monad m) => (a -> m Bool) -> [a] -> m Bool
allM f xs = and <$> mapM f xs

-- Determine if a piece of provenance 'occurs in' another. The answer can be influenced by 'width
-- flag', determining whether or not the provenance of superstructure occurs in the provenance of
-- its substructure.
occursIn :: Bool -> K3 Provenance -> K3 Provenance -> MaterializationM Bool
occursIn wide a b
  = case tag b of

      -- Everything occurs in itself.
      _ | a =*= b -> return True

      -- Something occurs in a bound variable if it occurs in anything that was used to initialize
      -- that bound variable, and that bound variable was initialized using a non-isolating method.
      PBVar mv -> do
             decision <- dLookup (pmvloc' mv) (pmvn mv)
             if inD decision == Referenced || inD decision == ConstReferenced
               then pLookup (pmvptr mv) >>=
                    maybe (error "Attempted to occurs-check a dangling provenance pointer.") (occursIn wide a)
               else return False

      -- Something occurs in substructure if it occurs in any superstructure, and wide effects are
      -- set.
      POption | wide -> anyM (occursIn wide a) (children b)
      PIndirection | wide -> anyM (occursIn wide a) (children b)
      PTuple _ | wide -> anyM (occursIn wide a) (children b)
      PProject _ | wide -> anyM (occursIn wide a) (children b)
      PRecord _ | wide -> anyM (occursIn wide a) (children b)

      -- TODO: Add more intelligent handling of substructure + PData combinations.

      _ -> return False

isReadIn :: K3 Provenance -> K3 Expression -> MaterializationM Bool
isReadIn x f = isReadInF x (getEffects f)

isReadInF :: K3 Provenance -> K3 Effect -> MaterializationM Bool
isReadInF xp ff =
  case ff of
    (tag -> FRead yp) -> occursIn False xp yp

    (tag -> FScope _) -> anyM (isReadInF xp) (children ff)
    (tag -> FSeq) -> anyM (isReadInF xp) (children ff)
    (tag -> FSet) -> anyM (isReadInF xp) (children ff)

    _ -> return False

isWrittenIn :: K3 Provenance -> K3 Expression -> MaterializationM Bool
isWrittenIn x f = isWrittenInF x (getEffects f)

isWrittenInF :: K3 Provenance -> K3 Effect -> MaterializationM Bool
isWrittenInF xp ff =
  case ff of
    (tag -> FWrite yp) -> occursIn True xp yp

    (tag -> FScope _) -> anyM (isWrittenInF xp) (children ff)
    (tag -> FSeq) -> anyM (isWrittenInF xp) (children ff)
    (tag -> FSet) -> anyM (isWrittenInF xp) (children ff)

    _ -> return False

hasWriteInIF :: Identifier -> K3 Effect -> MaterializationM Bool
hasWriteInIF ident effect =
  case effect of
    (tag -> FWrite (tag -> PFVar i)) | i == ident -> return True
    (tag -> FWrite (tag -> PBVar m)) | pmvn m == ident -> return True

    (tag -> FScope _) -> anyM (hasWriteInIF ident) (children effect)
    (tag -> FSeq) -> anyM (hasWriteInIF ident) (children effect)
    (tag -> FSet) -> anyM (hasWriteInIF ident) (children effect)

    _ -> return False

hasWriteInI :: Identifier -> K3 Expression -> MaterializationM Bool
hasWriteInI ident expr =
  case expr of
    (tag -> ELambda i) | i == ident -> return False
    (tag &&& children -> (ELambda _, [body])) -> do
       lambdaDecisions <- dLookupAll (getUID expr)
       case M.lookup ident lambdaDecisions of
         Nothing -> return False
         Just cd ->
           case inD cd of
             ConstReferenced -> return False
             Referenced -> hasWriteInI ident body
             Moved -> return True
             Copied -> return False

    (tag &&& children -> (ELetIn i, [e, _])) | i == ident -> hasWriteInI ident e
    (tag &&& children -> (ELetIn _, [e, b])) -> do
      eHasWriteInI <- hasWriteInI ident e
      bHasWriteInI <- hasWriteInI ident b
      (||) <$> hasWriteInI ident e <*> hasWriteInI ident b

    -- TODO: Other shadow cases.

    _ -> do
      moveDecisions <- dLookupAll (getUID expr)
      let localHasWriteByMove = maybe False (\d -> inD d == Moved) (M.lookup ident moveDecisions)
      localHasWrite <- hasWriteInIF ident (getEffects expr)
      childHasWrite <- anyM (hasWriteInI ident) (children expr)
      return (localHasWriteByMove || localHasWrite || childHasWrite)

pBindInfo :: K3 Provenance -> Maybe (Identifier, Maybe Int)
pBindInfo p =
  case p of
    (tag -> PFVar j) -> Just (j, Nothing)
    (tag -> PBVar (PMatVar { pmvn = n, pmvloc = (UID u) })) -> Just (n, Just u)
    _ -> Nothing

hasWriteInP :: K3 Provenance -> K3 Expression -> MaterializationM Bool
hasWriteInP prov expr =
  case expr of
    (tag -> ELambda x) | (PFVar x) == tag prov -> return False
    (tag &&& children -> (ELambda _, [b])) -> do
      closureDecisions <- dLookupAllWithBindings (getUID expr)
      let writeInClosure = fromMaybe False $ do
            (j, mb) <- pBindInfo prov
            d <- M.lookup (j, mb) closureDecisions
            return $ inD d == Moved
      childHasWrite <- hasWriteInP prov b
      return (writeInClosure || childHasWrite)

    (tag &&& children -> (EOperate OApp, [f, x])) -> do
      let argProv = getProvenance x
      argOccurs <- occursIn True prov argProv
      appDecision <- dLookup (getUID expr) ""

      functionHasWrite <- hasWriteInP prov f
      argHasWrite <- hasWriteInP prov x
      let appHasWrite = inD appDecision == Moved && argOccurs

      appHasIntrinsicWrite <- isWrittenInF prov (getEffects expr)

      return (functionHasWrite || argHasWrite || appHasWrite || appHasIntrinsicWrite)

    (tag &&& children -> (EOperate OSnd, [_, x])) -> do
      let messageProv = getProvenance x
      messageOccurs <- occursIn True prov messageProv
      sendDecision <- dLookup (getUID expr) ""
      messageHasWrite <- hasWriteInP prov x
      let sendHasWrite = inD sendDecision == Moved && messageOccurs
      return (messageHasWrite || sendHasWrite)

    (tag &&& children -> (ERecord is, cs)) -> do
      childrenHaveWrite <- anyM (hasWriteInP prov) cs

      moveDecisions <- dLookupAll (getUID expr)
      let f i c = do
            let currentDecision = M.findWithDefault defaultDecision i moveDecisions
            if inD currentDecision == Moved
               then occursIn True prov (getProvenance c)
               else return False
      constructorsHaveMoveWrite <- or <$> zipWithM f is cs
      return (constructorsHaveMoveWrite || childrenHaveWrite)

    _ -> do
      genericHasWrite <- isWrittenInF prov (getEffects expr)
      childHasWrite <- anyM (hasWriteInP prov) (children expr)
      return (genericHasWrite || childHasWrite)

hasReadInP :: K3 Provenance -> K3 Expression -> MaterializationM Bool
hasReadInP prov expr =
  case expr of
    (tag &&& children -> (ELambda _, [b])) -> do
      closureDecisions <- dLookupAllWithBindings (getUID expr)
      let readInClosure = fromMaybe False $ do
            (j, mb) <- pBindInfo prov
            d <- M.lookup (j, mb) closureDecisions
            return $ inD d == Copied || inD d == Moved

      childHasRead <- hasReadInP prov b
      return (readInClosure || childHasRead)

    (tag &&& children -> (EOperate OApp, [f, x])) -> do
      let argProv = getProvenance x
      argOccurs <- occursIn True prov argProv
      appDecision <- dLookup (getUID expr) ""

      functionHasRead <- hasReadInP prov f
      argHasRead <- hasReadInP prov x
      let appHasRead = inD appDecision == Moved && argOccurs

      return (functionHasRead || argHasRead || appHasRead)

    _ -> (||) <$> isReadInF prov (getEffects expr) <*> anyM (hasReadInP prov) (children expr)

isGlobalP :: K3 Provenance -> MaterializationM Bool
isGlobalP ep =
  case ep of
    (tag -> PGlobal _) -> return True
    (tag -> PBVar pmv) -> pLookup (pmvptr pmv) >>= maybe (return False) isGlobalP

    (tag &&& children -> (PProject _, [pp])) -> isGlobalP pp

    _ -> return False

isMoveable :: K3 Provenance -> MaterializationM Bool
isMoveable p = case tag p of
                 PLambda _ _ -> return False
                 _ -> not <$> isGlobalP p

isMoveableIn :: ProxyProvenance -> Downstream -> MaterializationM Bool
isMoveableIn (p, m) (d, n) =
  case tag p of
    PFVar i -> case m of
      Just (j, u) | j == i -> case n of
        Just (k, v) | (j, u) == (k, v) -> isMoveableIn' p d
        _ -> return True
      _ -> error "Found an impossible free variable."
    _ -> isMoveableIn' p d


isMoveableIn' :: K3 Provenance -> K3 Expression -> MaterializationM Bool
isMoveableIn' x c = do
  isRead <- hasReadInP x c
  isWritten <- hasWriteInP x c
  return $ not (isRead || isWritten)

isMoveableNow :: ProxyProvenance -> MaterializationM Bool
isMoveableNow p = do
  ds <- downstreams <$> get
  isMoveable1 <- isMoveable (fst p)
  allMoveable <- allM (isMoveableIn p) ds
  return $ isMoveable1 && allMoveable

isReferenceableIn' :: K3 Provenance -> K3 Expression -> MaterializationM Bool
isReferenceableIn' x c = do
  isWritten <- hasWriteInP x c
  return $ not isWritten

isReferenceableIn :: ProxyProvenance -> Downstream -> MaterializationM Bool
isReferenceableIn (p, m) (d, n) =
  case tag p of
    PFVar i -> case m of
      Just (j, u) | j == i -> case n of
        Just (k, v) | (j, u) == (k, v) -> isReferenceableIn' p d
        _ -> return True
      _ -> error "Found an impossible free variable."
    _ -> isReferenceableIn' p d

isReferenceableNow :: ProxyProvenance -> MaterializationM Bool
isReferenceableNow p = do
  ds <- downstreams <$> get
  allReferenceable <- allM (isReferenceableIn p) ds
  return allReferenceable

(=*=) :: K3 Provenance -> K3 Provenance -> Bool
a =*= b = case (tag a, tag b) of
            (PBVar mva, PBVar mvb) -> pmvn mva == pmvn mvb && pmvloc mva == pmvloc mvb
            (PFVar ia, PFVar ib) -> ia == ib
            (PGlobal ia, PGlobal ib) -> ia == ib

            -- TODO: Handle more cases.
            _ -> a == b
