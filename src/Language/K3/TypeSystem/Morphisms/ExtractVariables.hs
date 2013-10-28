{-# LANGUAGE TemplateHaskell, FlexibleContexts, FlexibleInstances, MultiParamTypeClasses, UndecidableInstances, ConstraintKinds #-}

{-|
  This module defines an operation which extracts all variables which "appear"
  within a given construct.
-}

module Language.K3.TypeSystem.Morphisms.ExtractVariables
( ExtractVariables(..)
, extractVariables
, VariableExtractable
) where

import Control.Applicative
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.TypeDecision.Data
import Language.K3.Utils.TemplateHaskell.Reduce

type VariableExtractable a = (Reduce ExtractVariables a VariableReduction)

type VariableReduction = Set AnyTVar

-- |A function to extract variables from type system data.
extractVariables :: (VariableExtractable a) => a -> Set AnyTVar
extractVariables = reduce ExtractVariables

-- |A transformation (as in @Language.K3.Utils.TemplateHaskell.Transform@) for
--  retrieving the type variables which appear in type system data structures.
data ExtractVariables = ExtractVariables

instance Reduce ExtractVariables UVar VariableReduction where
  reduce ExtractVariables a = Set.singleton $ SomeUVar a
  
instance Reduce ExtractVariables QVar VariableReduction where
  reduce ExtractVariables a = Set.singleton $ SomeQVar a
  
instance Reduce ExtractVariables ConstraintSet VariableReduction where
  reduce ExtractVariables cs = reduce ExtractVariables $ csToList cs

$(
  concat <$> mapM (defineCatInstance [t|VariableReduction|] ''ExtractVariables)
                [ ''Constraint
                , ''BinaryOperator
                , ''Coproduct
                , ''TQual
                , ''ShallowType
                , ''OpaqueID
                , ''OpaqueOrigin
                , ''OpaqueVar
                , ''TPolarity
                , ''AnnType
                , ''AnnBodyType
                , ''AnnMemType
                , ''StubbedConstraintSet
                ]
 )
 
$(concat <$> mapM (defineReduceEmptyInstance [t|VariableReduction|]
                      ''ExtractVariables)
                [ ''UID
                , ''Stub
                , ''MorphismArity
                ]                
 )
 
$(
  concat <$> mapM (defineReduceFoldInstance [t|VariableReduction|]
                      ''ExtractVariables)
                [ [t|Map Identifier|]
                , [t|Map TEnvId|]
                ]
 )

$(defineCommonCatInstances [t|VariableReduction|] ''ExtractVariables)
