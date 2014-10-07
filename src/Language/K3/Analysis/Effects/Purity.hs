-- | Purity Analysis.
module Language.K3.Analysis.Effects.Purity where

import qualified Data.Set as S

import Language.K3.Analysis.Effects.Core

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

runPurity :: K3 Declaration -> K3 Declaration
runPurity = runPurityD

runPurityD :: K3 Declaration -> K3 Declaration
runPurityD (Node (DGlobal i t me :@: as) cs) = Node (DGlobal i t (runPurityE <$> me)) cs
runPurityD (Node (DTrigger i t e :@: as) cs) = Node (DTrigger i t (runPurityE e) :@: as) cs
runPurityD (Node (DRole n :@: as) cs) = Node (DRole n :@: as) (map runPurityD cs)
runPurityD d = d

runPurityE :: K3 Expression -> K3 Expression
runPurityE e@(Node (ELambda x :@: as) cs) = (if isPure then e @+ (EProperty "Pure" Nothing) else e)
  where
    EEffect (Node (FScope [binding] closure :@: _) effects) = fromJust $ e @~ isEEffect

    isPure = noGlobalReads && noGlobalWrites && noIndirections && readOnlyNonLocalScalars

    nonLocals = let (cRead, cWritten, cApplied) = closure in S.insert binding $ S.unions [cRead, cWritten, cApplied]

    noGlobalReads = any isGlobal $ readSet effects
    noGlobalWrites = any isGlobal $ writeSet effects

    noIndirections = any isIndirection nonLocals
    readOnlyNonLocalScalars = all isScalar $ S.intersection nonLocals (writeSet effects)
runPurityE (Node (t :@: as) cs) = Node (t :@: as) (map runPurityE cs)
