{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | Data types for K3 dataflow provenance
module Language.K3.Analysis.Provenance.Core where

import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Utils.Pretty

import Data.Text ( Text )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

type PPtr    = Int
data PMatVar = PMatVar {pmvn :: Identifier, pmvloc :: UID, pmvptr :: PPtr}
             deriving (Eq, Ord, Read, Show)

data Provenance =
    -- Atoms
      PFVar        Identifier
    | PBVar        PMatVar
    | PTemporary   -- A local leading to no lineage of interest

    -- Terms
    | PGlobal      Identifier
    | PSet                              -- Non-deterministic (if-then-else or case)
    | PChoice                           -- One of the cases must be chosen ie. they're exclusive
    | PDerived                          -- A value derived from named children e.g. x + y ==> [x;y]
    | PData        (Maybe [Identifier]) -- A value derived from a data constructor (optionally with named comoonents)
    | PRecord      Identifier
    | PTuple       Int
    | PIndirection
    | POption
    | PLambda      Identifier
    | PClosure
    | PApply       (Maybe PMatVar) -- The lambda, argument, and return value provenances of the application.
                                   -- The apply also tracks any provenance variable binding needed for the lambda.
    | PMaterialize [PMatVar]       -- A materialized return value scope, denoting provenance varibles bound in the child.
    | PProject     Identifier      -- The source of the projection, and the provenance of the projected value if available.
    | PAssign      Identifier      -- The provenance of the expression used for assignment.
    | PSend                        -- The provenance of the value being sent.
  deriving (Eq, Ord, Read, Show)

data instance Annotation Provenance = PDeclared (K3 Provenance)
                                    deriving (Eq, Ord, Read, Show)

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


instance PT.Pretty (K3 Provenance) where
  prettyLines (Node (tg :@: as) ch) =
    let (aTxt, chATxt) = drawPAnnotationsT as
    in [T.append (T.pack $ show tg) aTxt]
         ++ (let (p,l) = if null ch then (T.pack "`- ", T.pack "   ")
                                    else (T.pack "+- ", T.pack "|  ")
             in PT.shift p l chATxt)
         ++ PT.drawSubTrees ch

drawPAnnotationsT :: [Annotation Provenance] -> (Text, [Text])
drawPAnnotationsT as =
  let (pdeclAnns, anns) = partition isPDeclared as
      prettyPrAnns      = concatMap drawPDeclAnnotation pdeclAnns
  in (PT.drawAnnotations anns, prettyPrAnns)

  where drawPDeclAnnotation (PDeclared p) = [T.pack "PDeclared "] PT.%+ PT.prettyLines p
        drawPDeclAnnotation _ = error "Invalid provenance annotation"
