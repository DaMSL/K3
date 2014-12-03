-- | Machinery for making decisions about C++ level materialization for K3.
module Language.K3.Codegen.CPP.Materialization where

import Control.Monad.Identity (Identity(..), runIdentity)
import Control.Monad.State (StateT(..), MonadState(..), runState)

import qualified Data.Map as M
import qualified Data.IntMap as I

import Language.K3.Core.Common

data Method
  = ConstReferenced
  | Referenced
  | Moved
  | Copied
 deriving (Eq, Read, Show)

-- Decisions as pertaining to an identifier at a given expression specify how the binding is to be
-- populated (initialized), and how it is to be written back (finalized, if necessary).
data Decision = Decision { inD :: Method, outD :: Method } deriving (Eq, Read, Show)

-- The most conservative decision is to initialize a new binding by copying its initializer, and to
-- finalize it by copying it back. There might be a strictly better strategy, but this is the
-- easiest to write.
defaultDecision :: Decision
defaultDecision = Decision { inD = Copied, outD = Copied }

type Table = I.Map (M.Map Identifier Decision)

type MaterializationS = (Table, PIEnv, [K3 Expression])
type MaterializationM = StateT MaterializationS Identity

-- State Accessors

dLookup :: UID -> Identifier -> MaterializationM Decision
dLookup u i = get >>= \(t, _, _) -> fromMaybe defaultDecision (I.lookup u t >>= M.lookup i)

dLookupAll :: UID -> Identifier -> MaterializationM (M.Map Identifier Decision)
dLookupAll u = get >>= \(t, _, _) -> I.findWithDefault M.empty u t

pLookup :: PPtr -> MaterializationM (K3 Provenance)
pLookup p = get >>= \(_, e, _) -> fromMaybe (error "Dangling provenance pointer") (I.lookup p e)

-- A /very/ rough approximation of ReaderT's ~local~ for StateT.
withLocalDS :: [K3 Expression] -> MaterializationM a -> MaterializationM a
withLocalDS nds m = do
  (t, e, ds) <- get
  put (t, e, (nds ++ ds))
  r <- m
  (t', e', _) <- get
  put (t', e', ds)
  return r

getUID :: K3 Expression -> UID
getUID e = let EUID u = fromMaybe (error "No UID on expression.")
                        (e @~ \case { EUID _ -> True; _ -> False }) in u

getProvenance :: K3 Expression -> K3 Provenance
getProvenance e = let EProvenance p = fromMaybe (error "No provenance on expression.")
                                      (e @~ \case { EProvenance _ -> True; _ -> False}) in p

getEffects :: K3 Expression -> K3 Effect
getEffects e = let EEffect ff = fromMaybe (error "No effects on expression.")
                                (e @~ \case { EEffect _ -> True; _ -> False }) in f

setDecision :: UID -> Identifier -> Decision -> MaterializationM ()
setDecision u i d = modify $ \(t, e, ds) -> (I.insertWith (M.insert i d) u t, e, ds)

-- Table Construction/Attachment

runMaterializationM :: MaterializationM a -> MaterializationS -> (a, MaterializationS)
runMaterializationM = runIdentity . runStateT

materializationD :: K3 Declaration -> MaterializationM (K3 Declaration)
materializationD = undefined

materializationE :: K3 Expression -> MaterializationM (K3 Expression)
materializationE e
  = case e of
      (tag &&& children -> (EOperate OApp, [f, x])) -> do
             f' <- withLocalDS [x] (materializationE f)
             x' <- materializationE x

             let argIdent = case tag (getProvenance f') of
                              PLambda p -> p
                              _ -> error "Unexpected provenance on function."

             fDecision <- dLookup (getUID f') argIdent

             decision <- if inD fDecision == Referenced || inD fDecision == ConstReferenced
                           then return fDecision
                           else do
                             moveable <- isMoveableNow x'

                             return $ if moveable then (Decision Moved Moved) else (Decision Copied Copied)

             setDecision (getUID e) argIdent decision

             allDecisions <- dLookupAll
             return $ Node (tag e :@: (EMaterialization allDecisions : annotations e)) [f', x']

      _  -> Node (tag e :@: annotations e) <$> mapM materializationE (children e)

-- Queries

anyM :: (Functor m, Applicative m, Monad m) => (a -> m Bool) -> [a] -> m Bool
anyM f xs = or <$> mapM xs

-- Determine if a piece of provenance 'occurs in' another. The answer can be influenced by 'width
-- flag', determining whether or not the provenance of superstructure occurs in the provenance of
-- its substructure.
occursIn :: Bool -> K3 Provenance -> K3 Provenance -> MaterializationM Bool
occursIn wide b a
  = case a of

      -- Everything occurs in itself.
      _ | a == b = True

      -- Something occurs in a bound variable if it occurs in anything that was used to initialize
      -- that bound variable, and that bound variable was initialized using a non-isolating method.
      (tag -> PBVar mv) -> do
             decision <- dLookup (pmvloc mv) (pmvn mv)
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

      _ -> False

isReadIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isReadIn = undefined

isWrittenIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isWrittenIn = undefined

isMoveableIn :: K3 Expression -> K3 Expression -> MaterializationM Bool
isMoveableIn x c = undefined

isMoveableNow :: K3 Expression -> MaterializationM Bool
isMoveableNow x = get >>= \(_, _, ds) -> anyM (isMoveableIn x) ds
