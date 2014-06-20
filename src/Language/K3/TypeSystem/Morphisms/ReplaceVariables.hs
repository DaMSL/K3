{-# LANGUAGE TemplateHaskell, TypeSynonymInstances, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, UndecidableInstances, ConstraintKinds #-}

{-|
  This module defines an operation which substitutes variables for other
  variables throughout a given construct.
-}
module Language.K3.TypeSystem.Morphisms.ReplaceVariables
( ReplaceVariables(..)
, replaceVariables
, VariableReplacable
) where

import Control.Applicative
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Utils.TemplateHaskell.Transform
import Language.K3.TypeSystem.Data

type VariableReplacable a = (Transform ReplaceVariables a)

-- |The data type defining the transformation.
data ReplaceVariables = ReplaceVariables (Map QVar QVar) (Map UVar UVar)

-- |A function to extract variables from type system data.
replaceVariables :: (Transform ReplaceVariables a)
                 => Map QVar QVar -> Map UVar UVar -> a -> a
replaceVariables qvarMap uvarMap = transform (ReplaceVariables qvarMap uvarMap)

instance Transform ReplaceVariables QVar where
  transform (ReplaceVariables m _) qa = fromMaybe qa $ Map.lookup qa m

instance Transform ReplaceVariables UVar where
  transform (ReplaceVariables _ m) a = fromMaybe a $ Map.lookup a m

instance Transform ReplaceVariables ConstraintSet where
  transform trans cs = csFromList $ map (transform trans) $ csToList cs

$(
  concat <$> mapM (defineHomInstance ''ReplaceVariables)
                [ ''Constraint
                , ''BinaryOperator
                , ''Coproduct
                , ''TQual
                , ''ShallowType
                , ''OpaqueID
                , ''OpaqueOrigin
                , ''OpaqueVar
                , ''QuantType
                , ''AnyTVar
                , ''AnnType
                , ''AnnBodyType
                , ''AnnMemType
                , ''TPolarity
                ]
 )

$(concat <$> mapM (defineTransformIdentityInstance ''ReplaceVariables)
                [ ''UID
                , ''MorphismArity
                , ''K3
                ]
 )

$(
  concat <$> mapM (defineTransformFunctorInstance ''ReplaceVariables)
                [ [t|Map Identifier|]
                , [t|Map TEnvId|]
                ]
 )

$(defineCommonHomInstances ''ReplaceVariables)
