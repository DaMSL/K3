{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | Data types for K3 dataflow provenance
module Language.K3.Analysis.Provenance.Core where

import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Utils.Pretty

type PPtr = Int

data Provenance =
    -- Atoms
      PFVar        Identifier
    | PBVar        PPtr
    | PTemporary   -- A local leading to no lineage of interest

    -- Terms
    | PGlobal      Identifier
    | PRecord      Identifier Identifier
    | PTuple       Identifier Integer
    | PIndirection Identifier
    | PProject     Identifier -- Created by projections
    | PLet         Identifier
    | PCase        Identifier
    | PLambda      Identifier
    | PClosure
    | PApply       -- A symbol application only extracts the child symbols
    | PSet         -- Non-deterministic (if-then-else or case)
    | PChoice      -- One of the cases must be chosen ie. they're exclusive
    | PDerived     -- A symbol derived from its children e.g. x + y ==> [x;y]
    | PDirect      -- A temporary representation that's a direct path to its child
  deriving (Eq, Ord, Read, Show)

data instance Annotation Provenance = PID PPtr
                                    | PDeclared (K3 Provenance)
                                    deriving (Eq, Ord, Read, Show)

isPID :: Annotation Provenance -> Bool
isPID (PID _) = True
isPID _ = False

isPDeclared :: Annotation Provenance -> Bool
isPDeclared (PDeclared _) = True
isPDeclared _ = False

instance Pretty (K3 Provenance) where
  prettyLines (Node (tg :@: as) ch) =
    let (aStr, chAStr) = drawPAnnotations as
    in [show tg ++ aStr]
         ++ (let (p,l) = if null ch then ("`- ", "   ") else ("+- ", "|  ")
             in shift p l chAStr)
         ++ drawSubTrees ch

drawPAnnotations :: [Annotation Provenance] -> (String, [String])
drawPAnnotations as =
  let (pdeclAnns, anns) = partition isPDeclared as
      prettyPrAnns      = concatMap drawPDeclAnnotation pdeclAnns
  in (drawAnnotations anns, prettyPrAnns)

  where drawPDeclAnnotation (PDeclared p) = ["PDeclared "] %+ prettyLines p
        drawPDeclAnnotation _ = error "Invalid provenance annotation"
