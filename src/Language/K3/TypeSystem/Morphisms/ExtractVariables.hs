{-# LANGUAGE TemplateHaskell, FlexibleContexts, FlexibleInstances, MultiParamTypeClasses, UndecidableInstances #-}

module Language.K3.TypeSystem.Morphisms.ExtractVariables
( ExtractVariables
, extractVariables
) where

import Control.Applicative
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Map (Map)

import Language.K3.Core.Common
import Language.K3.TemplateHaskell.Reduce
import Language.K3.TypeSystem.Data

type VariableReduction = Set AnyTVar

-- |A function to extract variables from type system data.
extractVariables :: (Reduce ExtractVariables a VariableReduction)
                 => a -> Set AnyTVar
extractVariables = reduce ExtractVariables

-- |A transformation (as in @Language.K3.TemplateHaskell.Transform@) for
--  retrieving the type variables which appear in type system data structures.
data ExtractVariables = ExtractVariables

instance Reduce ExtractVariables UVar VariableReduction where
  reduce _ a = Set.singleton $ SomeUVar a
  
instance Reduce ExtractVariables QVar VariableReduction where
  reduce _ a = Set.singleton $ SomeQVar a

$(
  concat <$> mapM (defineCatInstance [t|VariableReduction|] ''ExtractVariables)
                [ ''ConstraintSet
                , ''Constraint
                , ''BinaryOperator
                , ''Coproduct
                , ''TQual
                , ''ShallowType
                ]
 )
 
$(
  concat <$> mapM (defineReduceFoldInstance [t|VariableReduction|]
                      ''ExtractVariables)
                [ [t|Map Identifier|]
                ]
 )

$(defineCommonCatInstances [t|VariableReduction|] ''ExtractVariables)
