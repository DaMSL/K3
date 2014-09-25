{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

module Language.K3.Core.Metaprogram where

import Data.Map ( Map )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Utils.Pretty

data MetaDeclaration
data MPDeclaration

type SpliceEnv     = Map Identifier SpliceReprEnv
type SpliceReprEnv = Map Identifier SpliceValue
type SpliceContext = [SpliceEnv]

data SpliceValue
data SpliceResult (m :: * -> *)

instance Eq   MetaDeclaration
instance Ord  MetaDeclaration
instance Read MetaDeclaration
instance Show MetaDeclaration

instance Eq   MPDeclaration
instance Ord  MPDeclaration
instance Read MPDeclaration
instance Show MPDeclaration

instance Eq   SpliceValue
instance Ord  SpliceValue
instance Read SpliceValue
instance Show SpliceValue

instance Pretty (K3 MetaDeclaration)
instance Pretty MPDeclaration

{- Splice context accessors -}
lookupSCtxt      :: Identifier -> Identifier -> SpliceContext -> Maybe SpliceValue
addSCtxt         :: Identifier -> SpliceReprEnv -> SpliceContext -> SpliceContext
removeSCtxt      :: Identifier -> SpliceContext -> SpliceContext
removeSCtxtFirst :: Identifier -> SpliceContext -> SpliceContext
pushSCtxt        :: SpliceEnv -> SpliceContext -> SpliceContext
popSCtxt         :: SpliceContext -> SpliceContext

{- Splice environment helpers -}
spliceVIdSym :: Identifier
spliceVTSym :: Identifier
spliceVESym :: Identifier

lookupSpliceE      :: Identifier -> Identifier -> SpliceEnv -> Maybe SpliceValue
addSpliceE         :: Identifier -> SpliceReprEnv -> SpliceEnv -> SpliceEnv
emptySpliceReprEnv :: SpliceReprEnv
mkSpliceReprEnv    :: [(Identifier, SpliceValue)] -> SpliceReprEnv
emptySpliceEnv     :: SpliceEnv
mkSpliceEnv        :: [(Identifier, SpliceReprEnv)] -> SpliceEnv
mergeSpliceEnv     :: SpliceEnv -> SpliceEnv -> SpliceEnv
