{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

-- | Data types for Hindley-Milner inference.
--   We use a separate tree data type to ensure no mixing of type systems.
module Language.K3.Analysis.HMTypes.DataTypes where

import Control.DeepSeq
import GHC.Generics (Generic)

import Data.List
import Data.Tree
import Data.Typeable

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Utils.Pretty

import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

type QTVarId = Int

data QPType = QPType [QTVarId] (K3 QType)
                deriving (Eq, Ord, Read, Show, Typeable, Generic)

data QType
        = QTBottom
        | QTPrimitive QTBase
        | QTCon       QTData
        | QTVar       QTVarId
        | QTOperator  QTOp
        | QTContent
        | QTFinal
        | QTSelf
        | QTTop
      deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Primitive types.
--   Note this class derives an enum instance which we use to determine precedence.
--   Hence the ordering of the constructors should not be changed lightly.
data QTBase
        = QTBool
        | QTByte
        | QTReal
        | QTInt
        | QTString
        | QTAddress
        | QTNumber
      deriving (Enum, Eq, Ord, Read, Show, Typeable, Generic)

data QTData
        = QTFunction
        | QTOption
        | QTIndirection
        | QTTuple
        | QTRecord      [Identifier]
        | QTCollection  [Identifier]
        | QTTrigger
        | QTSource
        | QTSink
      deriving (Eq, Ord, Read, Show, Typeable, Generic)

data QTOp = QTLower deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Annotations on types are the mutability qualifiers.
data instance Annotation QType
    = QTMutable
    | QTImmutable
    | QTWitness
  deriving (Eq, Ord, Read, Show, Generic)

{- QType-related instances -}
instance NFData QPType
instance NFData QType
instance NFData QTBase
instance NFData QTData
instance NFData QTOp
instance NFData (Annotation QType)

-- | Type constructors
tleaf :: QType -> K3 QType
tleaf t = Node (t :@: []) []

tdata :: QTData -> [K3 QType] -> K3 QType
tdata d c = Node (QTCon d :@: []) c

tprim :: QTBase -> K3 QType
tprim b = tleaf $ QTPrimitive b

tvar :: QTVarId -> K3 QType
tvar i = Node (QTVar i :@: []) []

ttop :: K3 QType
ttop = tleaf QTTop

tbot :: K3 QType
tbot = tleaf QTBottom

tcontent :: K3 QType
tcontent = tleaf QTContent

tfinal :: K3 QType
tfinal = tleaf QTFinal

tself :: K3 QType
tself = tleaf QTSelf


-- | Datatype constructors
tfun :: K3 QType -> K3 QType -> K3 QType
tfun a r = tdata QTFunction [a,r]

topt :: K3 QType -> K3 QType
topt c = tdata QTOption [c]

tind :: K3 QType -> K3 QType
tind c = tdata QTIndirection [c]

ttup :: [K3 QType] -> K3 QType
ttup c = tdata QTTuple c

trec :: [(Identifier, K3 QType)] -> K3 QType
trec idt = let (ids,ts) = unzip idt in tdata (QTRecord ids) ts

tcol :: K3 QType -> [Identifier] -> K3 QType
tcol ct annIds = tdata (QTCollection annIds) [ct]

ttrg :: K3 QType -> K3 QType
ttrg t = tdata QTTrigger [t]

tsrc :: K3 QType -> K3 QType
tsrc t = tdata QTSource [t]

tsnk :: K3 QType -> K3 QType
tsnk t = tdata QTSink [t]


-- | Primitive type constructors

tbool :: K3 QType
tbool = tprim QTBool

tbyte :: K3 QType
tbyte = tprim QTByte

tint :: K3 QType
tint = tprim QTInt

treal :: K3 QType
treal = tprim QTReal

tstr :: K3 QType
tstr = tprim QTString

taddr :: K3 QType
taddr = tprim QTAddress

tnum :: K3 QType
tnum = tprim QTNumber

tunit :: K3 QType
tunit = ttup []


-- | Operator constructors
tlower :: [K3 QType] -> K3 QType
tlower ch = Node (QTOperator QTLower :@: []) $ nub ch


-- | Annotation predicates
isQTQualified :: Annotation QType -> Bool
isQTQualified QTImmutable = True
isQTQualified QTMutable   = True
isQTQualified _ = False

isQTNumeric :: K3 QType -> Bool
isQTNumeric (tag -> QTPrimitive p1) | p1 `elem` [QTInt, QTReal, QTNumber] = True
                                    | otherwise = False
isQTNumeric _ = False

isQTVar :: K3 QType -> Bool
isQTVar (tag -> QTVar _) = True
isQTVar _ = False

isQTLower :: K3 QType -> Bool
isQTLower (tag -> QTOperator QTLower) = True
isQTLower _ = False

instance Pretty QTVarId where
  prettyLines x = [show x]

instance Pretty QTBase where
  prettyLines x = [show x]

instance Pretty QTData where
  prettyLines x = [show x]

instance Pretty (K3 QType) where
  prettyLines (Node (t :@: as) ts) = (show t ++ drawAnnotations as) : drawSubTrees ts

instance Pretty QPType where
  prettyLines (QPType tvars qt) = [unwords ["QPT", show tvars] ++ " "] %+ (prettyLines qt)

instance PT.Pretty QTVarId where
  prettyLines x = [T.pack $ show x]

instance PT.Pretty QTBase where
  prettyLines x = [T.pack $ show x]

instance PT.Pretty QTData where
  prettyLines x = [T.pack $ show x]

instance PT.Pretty (K3 QType) where
  prettyLines (Node (t :@: as) ts) = (T.append (T.pack $ show t) $ PT.drawAnnotations as) : PT.drawSubTrees ts

instance PT.Pretty QPType where
  prettyLines (QPType tvars qt) = [T.pack $ unwords ["QPT", show tvars] ++ " "] PT.%+ (PT.prettyLines qt)
