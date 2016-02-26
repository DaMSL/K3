{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | Data types for the K3 effect system
module Language.K3.Analysis.SEffects.Core where

import Control.DeepSeq
import GHC.Generics (Generic)

import Data.Binary
import Data.Serialize

import Data.List
import Data.Tree
import Data.Typeable

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Utils.Pretty

import Data.Text ( Text )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

import Language.K3.Analysis.Provenance.Core

type FPtr    = Int
data FMatVar = FMatVar {fmvn :: !Identifier, fmvloc :: !UID, fmvptr :: !FPtr}
             deriving (Eq, Ord, Read, Show, Typeable, Generic)

data Effect
    = FFVar        !Identifier
    | FBVar        !FMatVar
    | FRead        !(K3 Provenance)
    | FWrite       !(K3 Provenance)
    | FIO
    | FData        !(Maybe [Identifier]) -- An effect container, to support structural matching for lambda effects.

    | FScope       ![FMatVar]   -- Materialization point for the given FMatVars. This has four children:
                               -- initialization execution effects (i.e., pre-body effects), body execution effects
                               -- post-body execution effects, and result effect structure.

    | FLambda      !Identifier
                          -- Lambda effects have three effect children: closure construction effects,
                          -- deferred execution effects and deferred effect structure.

    | FApply       !(Maybe FMatVar)
                          -- Application effect nodes have either two, three or five children:
                          -- i. 5-child variant:
                          --    lambda effect structure, arg effect structure,
                          --    initializer execution effects, result execution effects,
                          --    and a result effect structure.
                          -- ii. 2-child variant:
                          --     lambda effect structure, and arg effect structure.
                          -- iii. 1-child variant: result effect structure
                          --
                          -- Note after simplification, we introduce a 3-child variant as a
                          -- simplified form of the 5-child version:
                          -- iv. initializer execution effects, result execution effects,
                          --     and a result effect structure.

    | FSet                     -- Set of effects, all of which are possible.
    | FSeq
    | FLoop                    -- A repetitive effect. Can only happen in a foreign function
    | FNone                    -- Null effect
    deriving (Eq, Ord, Read, Show, Typeable, Generic)

data instance Annotation Effect = FDeclared !(K3 Effect)
                                deriving (Eq, Ord, Read, Show, Generic)

{- SEffect instances -}
instance NFData FMatVar
instance NFData Effect
instance NFData (Annotation Effect)

instance Binary FMatVar
instance Binary Effect
instance Binary (Annotation Effect)

instance Serialize FMatVar
instance Serialize Effect
instance Serialize (Annotation Effect)

{- Annotation extractors -}
isFDeclared :: Annotation Effect -> Bool
isFDeclared (FDeclared _) = True

instance Pretty (K3 Effect) where
  prettyLines (Node (FRead  p :@: as) _) =
    let (aStr, chAStr) = drawFAnnotations as
    in ["FRead " ++ aStr] %+ prettyLines p ++ shift "`- " "   " chAStr

  prettyLines (Node (FWrite p :@: as) _) =
    let (aStr, chAStr) = drawFAnnotations as
    in ["FWrite " ++ aStr] %+ prettyLines p ++ shift "`- " "   " chAStr

  prettyLines (Node (tg :@: as) ch) =
    let (aStr, chAStr) = drawFAnnotations as
    in [show tg ++ aStr]
         ++ (let (p,l) = if null ch then ("`- ", "   ") else ("+- ", "|  ")
             in shift p l chAStr)
         ++ drawSubTrees ch

drawFAnnotations :: [Annotation Effect] -> (String, [String])
drawFAnnotations as =
  let (fdeclAnns, anns) = partition isFDeclared as
      prettyEffAnns      = concatMap drawFDeclAnnotation fdeclAnns
  in (drawAnnotations anns, prettyEffAnns)

  where drawFDeclAnnotation (FDeclared f) = ["FDeclared "] %+ prettyLines f


instance PT.Pretty (K3 Effect) where
  prettyLines (Node (FRead  p :@: as) _) =
    let (aTxt, chATxt) = drawFAnnotationsT as
    in [T.append (T.pack "FRead ") aTxt] PT.%+ PT.prettyLines p
       ++ (PT.shift (T.pack "`- ") (T.pack "   ") chATxt)

  prettyLines (Node (FWrite p :@: as) _) =
    let (aTxt, chATxt) = drawFAnnotationsT as
    in [T.append (T.pack "FWrite ") aTxt] PT.%+ PT.prettyLines p
       ++ (PT.shift (T.pack "`- ") (T.pack "   ") chATxt)

  prettyLines (Node (tg :@: as) ch) =
    let (aTxt, chATxt) = drawFAnnotationsT as
    in [T.append (T.pack $ show tg) aTxt]
         ++ (let (p,l) = if null ch then (T.pack "`- ", T.pack "   ")
                                    else (T.pack "+- ", T.pack "|  ")
             in PT.shift p l chATxt)
         ++ PT.drawSubTrees ch

drawFAnnotationsT :: [Annotation Effect] -> (Text, [Text])
drawFAnnotationsT as =
  let (fdeclAnns, anns) = partition isFDeclared as
      prettyEffAnns      = concatMap drawFDeclAnnotation fdeclAnns
  in (PT.drawAnnotations anns, prettyEffAnns)

  where drawFDeclAnnotation (FDeclared f) = [T.pack "FDeclared "] PT.%+ PT.prettyLines f
