{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ViewPatterns #-}

-- | Machinery for making decisions about C++ level materialization for K3.
module Language.K3.Codegen.CPP.Materialization where

import Prelude hiding (concat, mapM, mapM_, or, and)

import Control.Applicative
import Control.Arrow

import Control.Monad.Identity (Identity(..), runIdentity)
import Control.Monad.State (StateT(..), MonadState(..), modify, runState)

import Language.K3.Analysis.Core

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Inference (PIEnv(..))

import qualified Language.K3.Analysis.Provenance.Constructors as P

import Language.K3.Analysis.SEffects.Core
import Language.K3.Analysis.SEffects.Inference (FIEnv(..))

import Language.K3.Codegen.CPP.Materialization.Hints

import Data.Functor
import Data.Traversable
import Data.Foldable

import Data.Maybe (fromMaybe, maybeToList)
import Data.Tree

import qualified Data.Map as M
import qualified Data.IntMap as I

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Common hiding (getUID)

import Control.Monad

type Table = I.IntMap (M.Map Identifier Decision)

data MaterializationS = MaterializationS { decisionTable :: Table
                                         , pienv :: PIEnv
                                         , fienv :: FIEnv
                                         , downstreams :: [K3 Expression]
                                         , currentGlobal :: Bool
                                         }

type MaterializationM = StateT MaterializationS Identity

-- State Accessors

dLookup :: Int -> Identifier -> MaterializationM Decision
dLookup u i = decisionTable <$> get >>= \t -> return $ fromMaybe defaultDecision (I.lookup u t >>= M.lookup i)

dLookupAll :: Int -> MaterializationM (M.Map Identifier Decision)
dLookupAll u = decisionTable <$> get >>= \t -> return (I.findWithDefault M.empty u t)

pLookup :: PPtr -> MaterializationM (K3 Provenance)
pLookup p = pienv <$> get >>= \e -> return (fromMaybe (error "Dangling provenance pointer") (I.lookup p (ppenv e)))

pLookupDeep :: PPtr -> MaterializationM (K3 Provenance)
pLookupDeep p = pLookup p >>= \case
  (tag -> PBVar (PMatVar { pmvptr })) -> pLookupDeep pmvptr
  p' -> return p'

-- A /very/ rough approximation of ReaderT's ~local~ for StateT.
withLocalDS :: [K3 Expression] -> MaterializationM a -> MaterializationM a
withLocalDS nds m = do
  s <- get
  put (s { downstreams = nds ++ (downstreams s)})
  r <- m
  s' <- get
  put (s' { downstreams = downstreams s})
  return r

getUID :: K3 Expression -> Int
getUID e = let EUID (UID u) = fromMaybe (error "No UID on expression.")
                        (e @~ \case { EUID _ -> True; _ -> False }) in u

getProvenance :: K3 Expression -> K3 Provenance
getProvenance e = let EProvenance p = fromMaybe (error "No provenance on expression.")
                                      (e @~ \case { EProvenance _ -> True; _ -> False}) in p


getEffects :: K3 Expression -> K3 Effect
getEffects e = let ESEffect f = fromMaybe (error "No effects on expression.")
                                (e @~ \case { ESEffect _ -> True; _ -> False }) in f

getFStructure :: K3 Expression -> K3 Effect
getFStructure e = let EFStructure f = fromMaybe (error "No effects on expression.")
                                      (e @~ \case { EFStructure _ -> True; _ -> False }) in f


setDecision :: Int -> Identifier -> Decision -> MaterializationM ()
setDecision u i d = modify $ \s -> s { decisionTable = I.insertWith M.union u (M.singleton i d) (decisionTable s)}

getClosureSymbols :: Int -> MaterializationM [Identifier]
getClosureSymbols i = (pvpenv . pienv) <$> get >>= \e -> return $ concat $ maybeToList (I.lookup i $ lcenv e)

pmvloc' :: PMatVar -> Int
pmvloc' pmv = let UID u = pmvloc pmv in u

setCurrentGlobal :: Bool -> MaterializationM ()
setCurrentGlobal b = modify $ \s -> s { currentGlobal = b }

-- Table Construction/Attachment

runMaterializationM :: MaterializationM a -> MaterializationS -> (a, MaterializationS)
runMaterializationM m s = runIdentity $ runStateT m s

optimizeMaterialization :: (PIEnv, FIEnv) -> K3 Declaration -> K3 Declaration
optimizeMaterialization (p, f) d = fst $ runMaterializationM (materializationD d) (MaterializationS I.empty p f [] True)

materializationD :: K3 Declaration -> MaterializationM (K3 Declaration)
materializationD (Node (d :@: as) cs)
  = case d of
      DGlobal i t me -> setCurrentGlobal True >> traverse materializationE me >>= \me' -> Node (DGlobal i t me' :@: as) <$> cs'
      DTrigger i t e -> materializationE e >>= \e' -> Node (DTrigger i t e' :@: as) <$> cs'
      DRole i -> Node (DRole i :@: as) <$> cs'
      _ -> Node (d :@: as) <$> cs'
 where
   cs' = mapM materializationD cs

materializationE :: K3 Expression -> MaterializationM (K3 Expression)
materializationE e@(Node (t :@: as) cs)
  = case t of
      ERecord is -> do
        fs <- mapM materializationE cs

        let moveDecision i x = isMoveableNow x >>= \m -> return $ if m then defaultDecision { inD = Moved } else defaultDecision

        decisions <- zipWithM moveDecision is fs
        zipWithM_ (setDecision (getUID e)) is decisions
        ds <- dLookupAll (getUID e)

        return (Node (t :@: (EMaterialization ds:as)) fs)

      EOperate OApp -> do
             [f, x] <- mapM materializationE cs

             let applicationEffects = getFStructure e
             let executionEffects = getEffects e
             let (returnEffects, formalParameter) =
                   case applicationEffects of
                     (tag &&& children -> (FApply (Just fmv), [returnEffects])) -> (returnEffects, fmv)
                     _ -> error "Invalid effect structure"
             {-
             let (executionEffects, returnEffects, formalParameter) =
                   case applicationEffects of
                     (tag &&& children -> (FApply (Just fmv), [_, executionEffects, returnEffects])) ->
                         (executionEffects, returnEffects, fmv)
                     _ -> error "Invalid effect structure"
             -}

             conservativeDoMoveLocal <- hasWriteInIF (fmvn formalParameter) executionEffects

             conservativeDoMoveReturn <-
               case f of
                 (tag &&& children -> (ELambda i, [f'])) ->
                   case f' of
                     (tag -> ELambda _) -> do
                       let f'id = getUID f'
                       f'd <- dLookup f'id i
                       return $ inD f'd == Moved
                     _ -> return False
                 _ -> return False

             moveable <- isMoveableNow x

             let applicationDecision d =
                   if (conservativeDoMoveLocal || conservativeDoMoveReturn) && moveable
                     then d { inD = Moved }
                     else d

             setDecision (getUID e) "" $ applicationDecision defaultDecision

             decisions <- dLookupAll (getUID e)

             return (Node (t :@: (EMaterialization decisions:as)) [f, x])

      EOperate OSnd -> do
        [target, message] <- mapM materializationE cs
        moveable <- isMoveableNow message
        let decision = if moveable then defaultDecision { inD = Moved } else defaultDecision
        setDecision (getUID e) "" decision
        ds <- dLookupAll (getUID e)
        return (Node (t :@: (EMaterialization ds:as)) [target, message])

      ELambda x -> do
             cg <- currentGlobal <$> get
             when cg $ case tag (head cs) of
                         ELambda _ -> return ()
                         otherwise -> setCurrentGlobal False

             [b] <- mapM materializationE cs

             setCurrentGlobal cg -- Probably not necessary to restore.

             let lambdaEffects = getEffects e
             let (deferredEffects, returnedEffects)
                   = case lambdaEffects of
                       (tag &&& children -> (FLambda _, [_, deferredEffects, returnedEffects]))
                         -> (deferredEffects, returnedEffects)
                       _ -> error "Invalid effect structure"

             let lambdaProvenance = getProvenance e

             let returnedProvenance =
                   case getProvenance e of
                     (tag &&& children -> (PLambda _, [returnedProvenance])) -> returnedProvenance
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

             closureSymbols <- getClosureSymbols (getUID e)

             forM_ closureSymbols $ \s -> do
               closureHasWrite <- hasWriteInI s b
               moveable <- return True
               let closureDecision d =
                     if closureHasWrite
                       then if moveable
                              then d { inD = Moved }
                              else d { inD = Copied }
                       else if cg
                              then d { inD = Moved }
                              else d { inD = Referenced }
               setDecision (getUID e) s $ closureDecision defaultDecision
             decisions <- dLookupAll (getUID e)
             return $ (Node (t :@: (EMaterialization decisions:as)) [b])

      EBindAs b -> do
             let [x, y] = cs
             x' <- withLocalDS [y] (materializationE x)
             y' <- materializationE y

             let xp = getProvenance x
             mention <- (||) <$> hasReadInP xp y' <*> hasWriteInP xp y'

             let referenceBind d = if not mention then d { inD = Referenced, outD = Referenced } else d

             case b of
               BIndirection i -> setDecision (getUID e) i $ referenceBind defaultDecision
               BTuple is -> mapM_ (\i -> setDecision (getUID e) i $ referenceBind defaultDecision) is
               BRecord iis -> mapM_ (\(_, i) -> setDecision (getUID e) i $ referenceBind defaultDecision) iis

             decisions <- dLookupAll (getUID e)
             return (Node (t :@: (EMaterialization decisions:as)) [x', y'])

      ECaseOf i -> do
             let [x, s, n] = cs
             x' <- withLocalDS [s, n] (materializationE x)
             s' <- materializationE s
             n' <- materializationE n

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
             x' <- withLocalDS [b] (materializationE x)
             b' <- materializationE b

             setDecision (getUID e) i defaultDecision
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
      _ | a == b -> return True

      -- Something occurs in a bound variable if it occurs in anything that was used to initialize
      -- that bound variable, and that bound variable was initialized using a non-isolating method.
      PBVar mv -> do
             decision <- dLookup (pmvloc' mv) (pmvn mv)
             if inD decision == Referenced || inD decision == ConstReferenced
               then pLookup (pmvptr mv) >>= occursIn wide a
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

isReadIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isReadIn x f =
  case f of
    _ -> isReadInF (getProvenance x) (getEffects f)

isReadInF :: K3 Provenance -> K3 Effect -> MaterializationM Bool
isReadInF xp ff =
  case ff of
    (tag -> FRead yp) -> occursIn False xp yp

    (tag -> FScope _) -> anyM (isReadInF xp) (children ff)
    (tag -> FSeq) -> anyM (isReadInF xp) (children ff)
    (tag -> FSet) -> anyM (isReadInF xp) (children ff)

    _ -> return False

isWrittenIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isWrittenIn x f =
  case f of
    _ -> isWrittenInF (getProvenance x) (getEffects f)

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
    (tag &&& children -> (ELetIn j, [e, b])) -> do
      eHasWriteInI <- hasWriteInI ident e
      bHasWriteInI <- hasWriteInI ident b
      (||) <$> hasWriteInI ident e <*> hasWriteInI ident b

    -- TODO: Other shadow cases.

    _ -> do
      localHasWrite <- hasWriteInIF ident (getEffects expr)
      childHasWrite <- anyM (hasWriteInI ident) (children expr)
      return (localHasWrite || childHasWrite)

pVarName :: K3 Provenance -> Maybe Identifier
pVarName p =
  case p of
    (tag -> PFVar j) -> Just j
    (tag -> PBVar pmv) -> Just $ pmvn pmv
    _ -> Nothing

hasWriteInP :: K3 Provenance -> K3 Expression -> MaterializationM Bool
hasWriteInP prov expr =
  case expr of
    (tag &&& children -> (ELambda i, [b])) -> do
      closureDecisions <- dLookupAll (getUID expr)
      let writeInClosure = maybe False (\j -> maybe False (\d -> inD d == Moved) $ M.lookup j closureDecisions)
                           (pVarName prov)
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

    _ -> (||) <$> isWrittenInF prov (getEffects expr) <*> anyM (hasWriteInP prov) (children expr)

hasReadInP :: K3 Provenance -> K3 Expression -> MaterializationM Bool
hasReadInP prov expr =
  case expr of
    (tag &&& children -> (ELambda i, [b])) -> do
      closureDecisions <- dLookupAll (getUID expr)
      let readInClosure = maybe False (\j -> maybe False (\d -> inD d == Copied) $ M.lookup j closureDecisions)
                           (pVarName prov)
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
    (tag -> PBVar pmv) -> pLookup (pmvptr pmv) >>= isGlobalP

    (tag &&& children -> (PProject _, [pp])) -> isGlobalP pp

    _ -> return False

isMoveable :: K3 Expression -> MaterializationM Bool
isMoveable e =
  case e of
    (tag -> ELambda _) -> return False
    _ -> not <$> isGlobalP (getProvenance e)

isMoveableIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isMoveableIn x c = do
  isRead <- isReadIn x c
  isWritten <- isWrittenIn x c
  return $ not (isRead || isWritten)

isMoveableNow :: K3 Expression -> MaterializationM Bool
isMoveableNow x = do
  ds <- downstreams <$> get
  isMoveable1 <- isMoveable x
  allMoveable <- allM (isMoveableIn x) ds
  return $ isMoveable1 && allMoveable
