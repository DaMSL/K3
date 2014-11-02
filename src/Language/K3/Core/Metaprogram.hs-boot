{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

module Language.K3.Core.Metaprogram where

import Data.Map ( Map )
import Data.Typeable

import Language.K3.Core.Common
import Language.K3.Utils.Pretty

data SpliceValue
data SpliceType
type TypedSpliceVar = (SpliceType, Identifier)
data SpliceResult (m :: * -> *)

data MPDeclaration
data MPAnnMemDecl

type SpliceEnv     = Map Identifier SpliceValue
type SpliceContext = [SpliceEnv]

instance Eq       SpliceValue
instance Ord      SpliceValue
instance Read     SpliceValue
instance Show     SpliceValue
instance Typeable SpliceValue

instance Eq       SpliceType
instance Ord      SpliceType
instance Read     SpliceType
instance Show     SpliceType
instance Typeable SpliceType

instance Eq       MPDeclaration
instance Ord      MPDeclaration
instance Read     MPDeclaration
instance Show     MPDeclaration
instance Typeable MPDeclaration

instance Pretty SpliceValue
instance Pretty MPAnnMemDecl
instance Pretty MPDeclaration

{- Splice value and type constructors -}
spliceRecord  :: [(Identifier, SpliceValue)] -> SpliceValue
spliceList    :: [SpliceValue] -> SpliceValue
spliceRecordT :: [(Identifier, SpliceType)] -> SpliceType
spliceListT   :: SpliceType -> SpliceType

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
