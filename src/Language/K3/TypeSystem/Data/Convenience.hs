{-# LANGUAGE TemplateHaskell, MultiParamTypeClasses, FlexibleInstances #-}

{-|
  A module containing convenience functions for users of the type system.
-}
module Language.K3.TypeSystem.Data.Convenience
( emptyAnnotation
, ConstraintConstructor2(..)
, (<:)
) where

import Control.Applicative
import qualified Data.Map as Map
import Data.Set (Set)

import Language.K3.TypeSystem.Data.ConstraintSet
import Language.K3.TypeSystem.Data.TypesAndConstraints
import Language.K3.TypeSystem.Data.Utils

-- |A value defining the empty annotation.
emptyAnnotation :: AnnType
emptyAnnotation = AnnType Map.empty (AnnBodyType [] []) csEmpty
  
-- |A typeclass with convenience instances for constructing constraints.  This
--  constructor only works on 2-ary constraint constructors (which most
--  constraints have).
class ConstraintConstructor2 a b where
  constraint :: a -> b -> Constraint
  
-- |An infix synonym for @constraint@.
infix 7 <:
(<:) :: (ConstraintConstructor2 a b) => a -> b -> Constraint
(<:) = constraint

{-
  The following Template Haskell creates various instances for the constraint
  function.  In each case of a TypeOrVar or QualOrVar, three variations are
  produced: one which takes the left side, one which takes the right, and one
  which takes the actual Either structure.
-}
$(
  -- The instances variable contains 5-tuples describing the varying positions
  -- in the typeclass instance template below.
  let instances =
        let typeOrVar = [ ([t|ShallowType|], [|CLeft|])
                        , ([t|UVar|], [|CRight|])
                        , ([t|TypeOrVar|], [|id|])
                        ]
            qualOrVar = [ ([t|Set TQual|], [|CLeft|])
                        , ([t|QVar|], [|CRight|])
                        , ([t|QualOrVar|], [|id|])
                        ]
        in
        -- Each of the following lists represent the 5-tuples for one constraint
        -- constructor.
        [ (t1, t2, [|IntermediateConstraint|], f1, f2)
        | (t1,f1) <- typeOrVar
        , (t2,f2) <- typeOrVar
        ]
        ++
        [ (t1, [t|QVar|], [|QualifiedLowerConstraint|], f1, [|id|])
        | (t1, f1) <- typeOrVar
        ]
        ++
        [ ([t|QVar|], t2, [|QualifiedUpperConstraint|], [|id|], f2)
        | (t2, f2) <- typeOrVar
        ]
        ++
        [ (t1, t2, [|QualifiedIntermediateConstraint|], f1, f2)
        | (t1, f1) <- qualOrVar
        , (t2, f2) <- qualOrVar
        ]
  in
  -- This function takes the 5-tuples and feeds them into the typeclass instance
  -- template, the actual instances.
  let mkInstance (t1, t2, cons, f1, f2) =
        [d|
          instance ConstraintConstructor2 $t1 $t2 where
            constraint a b = $cons ($f1 a) ($f2 b)
        |]
  in
  -- Rubber, meet road.
  concat <$> mapM mkInstance instances
 )
  