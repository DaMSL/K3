{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

-- | Machinery for making decisions about C++ level materialization for K3.
module Language.K3.Codegen.CPP.Materialization where

import Prelude hiding (concat, mapM, mapM_, or)

import Control.Applicative
import Control.Arrow

import Control.Monad.Identity (Identity(..), runIdentity)
import Control.Monad.State (StateT(..), MonadState(..), modify, runState)

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Inference (PIEnv(..))

import Language.K3.Analysis.SEffects.Core

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

type Table = I.IntMap (M.Map Identifier Decision)

type MaterializationS = (Table, PIEnv, [K3 Expression])
type MaterializationM = StateT MaterializationS Identity

-- State Accessors

dLookup :: Int -> Identifier -> MaterializationM Decision
dLookup u i = get >>= \(t, _, _) -> return $ fromMaybe defaultDecision (I.lookup u t >>= M.lookup i)

dLookupAll :: Int -> MaterializationM (M.Map Identifier Decision)
dLookupAll u = get >>= \(t, _, _) -> return (I.findWithDefault M.empty u t)

pLookup :: PPtr -> MaterializationM (K3 Provenance)
pLookup p = get >>= \(_, e, _) -> return (fromMaybe (error "Dangling provenance pointer") (I.lookup p (ppenv e)))

-- A /very/ rough approximation of ReaderT's ~local~ for StateT.
withLocalDS :: [K3 Expression] -> MaterializationM a -> MaterializationM a
withLocalDS nds m = do
  (t, e, ds) <- get
  put (t, e, (nds ++ ds))
  r <- m
  (t', e', _) <- get
  put (t', e', ds)
  return r

getUID :: K3 Expression -> Int
getUID e = let EUID (UID u) = fromMaybe (error "No UID on expression.")
                        (e @~ \case { EUID _ -> True; _ -> False }) in u

getProvenance :: K3 Expression -> K3 Provenance
getProvenance e = let EProvenance p = fromMaybe (error "No provenance on expression.")
                                      (e @~ \case { EProvenance _ -> True; _ -> False}) in p

getEffects :: K3 Expression -> K3 Effect
getEffects e = let ESEffect f = fromMaybe (error "No effects on expression.")
                                (e @~ \case { EEffect _ -> True; _ -> False }) in f

setDecision :: Int -> Identifier -> Decision -> MaterializationM ()
setDecision u i d = modify $ \(t, e, ds) -> (I.insertWith M.union u (M.singleton i d) t, e, ds)

getClosureSymbols :: Int -> MaterializationM [Identifier]
getClosureSymbols i = get >>= \(_, plcenv -> e, _) -> return $ concat $ maybeToList (I.lookup i e)

pmvloc' :: PMatVar -> Int
pmvloc' pmv = let UID u = pmvloc pmv in u

-- Table Construction/Attachment

runMaterializationM :: MaterializationM a -> MaterializationS -> (a, MaterializationS)
runMaterializationM m s = runIdentity $ runStateT m s

optimizeMaterialization :: (PIEnv, ()) -> K3 Declaration -> K3 Declaration
optimizeMaterialization (p, _) d = fst $ runMaterializationM (materializationD d) (I.empty, p, [])

materializationD :: K3 Declaration -> MaterializationM (K3 Declaration)
materializationD (Node (d :@: as) cs)
  = case d of
      DGlobal i t me -> traverse materializationE me >>= \me' -> Node (DGlobal i t me' :@: as) <$> cs'
      DTrigger i t e -> materializationE e >>= \e' -> Node (DTrigger i t e' :@: as) <$> cs'
      DRole i -> Node (DRole i :@: as) <$> cs'
      _ -> Node (d :@: as) <$> cs'
 where
   cs' = mapM materializationD cs

materializationE :: K3 Expression -> MaterializationM (K3 Expression)
materializationE e@(Node (t :@: as) cs)
  = case t of
      EOperate OApp -> do
             [f, x] <- mapM materializationE cs

             let pf = getProvenance f

             setDecision (getUID e) "" defaultDecision

             decisions <- dLookupAll (getUID e)

             return (Node (t :@: (EMaterialization decisions:as)) [f, x])

      ELambda x -> do
             [b] <- mapM materializationE cs
             closureSymbols <- getClosureSymbols (getUID e)
             setDecision (getUID e) x defaultDecision
             forM_ closureSymbols $ \s -> setDecision (getUID e) s defaultDecision
             decisions <- dLookupAll (getUID e)
             return (Node (t :@: (EMaterialization decisions:as)) [b])

      EBindAs b -> do
             let [x, y] = cs
             x' <- withLocalDS [y] (materializationE x)
             y' <- materializationE y

             case b of
               BIndirection i -> setDecision (getUID e) i defaultDecision
               BTuple is -> mapM_ (\i -> setDecision (getUID e) i defaultDecision) is
               BRecord iis -> mapM_ (\(_, i) -> setDecision (getUID e) i defaultDecision) iis

             decisions <- dLookupAll (getUID e)
             return (Node (t :@: (EMaterialization decisions:as)) [x', y'])

      ECaseOf i -> do
             let [x, s, n] = cs
             x' <- withLocalDS [s, n] (materializationE x)
             s' <- materializationE s
             n' <- materializationE n

             setDecision (getUID e) i defaultDecision
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

-- Determine if a piece of provenance 'occurs in' another. The answer can be influenced by 'width
-- flag', determining whether or not the provenance of superstructure occurs in the provenance of
-- its substructure.
occursIn :: Bool -> K3 Provenance -> K3 Provenance -> MaterializationM Bool
occursIn wide b a
  = case a of

      -- Everything occurs in itself.
      _ | a == b -> return True

      -- Something occurs in a bound variable if it occurs in anything that was used to initialize
      -- that bound variable, and that bound variable was initialized using a non-isolating method.
      (tag -> PBVar mv) -> do
             decision <- dLookup (pmvloc' mv) (pmvn mv)
             if inD decision == Referenced || inD decision == ConstReferenced
               then pLookup (pmvptr mv) >>= occursIn wide b
               else return False

      -- Something occurs in substructure if it occurs in any superstructure, and wide effects are
      -- set.
      (tag -> POption) | wide -> anyM (occursIn wide b) (children a)
      (tag -> PIndirection) | wide -> anyM (occursIn wide b) (children a)
      (tag -> PTuple _) | wide -> anyM (occursIn wide b) (children a)
      (tag -> PProject _) | wide -> anyM (occursIn wide b) (children a)
      (tag -> PRecord _) | wide -> anyM (occursIn wide b) (children a)

      -- TODO: Add more intelligent handling of substructure + PData combinations.

      _ -> return False

isReadIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isReadIn = undefined

isWrittenIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isWrittenIn = undefined

isMoveableIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isMoveableIn x c = undefined

isMoveableNow :: K3 Expression -> MaterializationM Bool
isMoveableNow x = get >>= \(_, _, ds) -> anyM (isMoveableIn x) ds
