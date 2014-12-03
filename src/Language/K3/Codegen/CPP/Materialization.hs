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

pLookup :: PPtr -> MaterializationM (K3 Provenance)
pLookup p = get >>= \(_, e, _) -> fromMaybe (error "Dangling provenance pointer") (I.lookup p e)

-- Table Construction/Attachment

runMaterializationM :: MaterializationM a -> MaterializationS -> (a, MaterializationS)
runMaterializationM = runIdentity . runStateT

materializationD :: K3 Declaration -> MaterializationM (K3 Declaration)
materializationD = undefined

materializationE :: K3 Expression -> MaterializationM (K3 Expression)
materializationE = undefined

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
