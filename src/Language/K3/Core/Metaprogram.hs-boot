{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

module Language.K3.Core.Metaprogram where

import Data.Map ( Map )

import Language.K3.Core.Common
import Language.K3.Utils.Pretty

data SpliceValue
data SpliceType
type TypedSpliceVar = (SpliceType, Identifier)
data SpliceResult (m :: * -> *)

data MPDeclaration

type SpliceEnv     = Map Identifier SpliceValue
type SpliceContext = [SpliceEnv]

instance Eq   MPDeclaration
instance Ord  MPDeclaration
instance Read MPDeclaration
instance Show MPDeclaration

instance Eq   SpliceValue
instance Ord  SpliceValue
instance Read SpliceValue
instance Show SpliceValue

instance Pretty MPDeclaration

{- Splice value and type constructors -}
spliceRecord  :: [(Identifier, SpliceValue)] -> SpliceValue
spliceSet     :: [SpliceValue] -> SpliceValue
spliceRecordT :: [(Identifier, SpliceType)] -> SpliceType
spliceSetT    :: SpliceType -> SpliceType

spliceRecordField :: SpliceValue -> Identifier -> Maybe SpliceValue

{- Splice context accessors -}
lookupSCtxt      :: Identifier -> SpliceContext -> Maybe SpliceValue
addSCtxt         :: Identifier -> SpliceValue -> SpliceContext -> SpliceContext
removeSCtxt      :: Identifier -> SpliceContext -> SpliceContext
removeSCtxtFirst :: Identifier -> SpliceContext -> SpliceContext
pushSCtxt        :: SpliceEnv -> SpliceContext -> SpliceContext
popSCtxt         :: SpliceContext -> SpliceContext

{- Splice environment helpers -}
spliceVIdSym :: Identifier
spliceVTSym :: Identifier
spliceVESym :: Identifier

lookupSpliceE      :: Identifier -> SpliceEnv -> Maybe SpliceValue
addSpliceE         :: Identifier -> SpliceValue -> SpliceEnv -> SpliceEnv
emptySpliceEnv     :: SpliceEnv
mkSpliceEnv        :: [(Identifier, SpliceValue)] -> SpliceEnv
mergeSpliceEnv     :: SpliceEnv -> SpliceEnv -> SpliceEnv
