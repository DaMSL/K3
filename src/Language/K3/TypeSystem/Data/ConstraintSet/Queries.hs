{-# LANGUAGE TemplateHaskell, LambdaCase #-}

{-|
  A module defining the query descriptors for a constraint set.
-}
module Language.K3.TypeSystem.Data.ConstraintSet.Queries
( constraintSetQueryDescriptors
) where

import Data.Set (Set)

import Language.K3.TypeSystem.Data.Constraints
import Language.K3.TypeSystem.Data.Coproduct
import Language.K3.TypeSystem.Data.Types
import Language.K3.Utils.IndexedSet

constraintSetQueryDescriptors :: [QueryDescriptor]
constraintSetQueryDescriptors =

    [ QueryDescriptor
        "AllTypesLowerBoundingAnyVars"
        [t| () |]
        [t| (ShallowType,AnyTVar) |]
        [| \case
              IntermediateConstraint (CLeft t) (CRight a) ->
                [ ( () , (t,someVar a) ) ]
              QualifiedLowerConstraint (CLeft t) qa ->
                [ ( () , (t,someVar qa) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllTypesUpperBoundingAnyVars"
        [t| () |]
        [t| (AnyTVar,ShallowType) |]
        [| \case
              IntermediateConstraint (CRight a) (CLeft t) ->
                [ ( () , (someVar a,t) ) ]
              QualifiedUpperConstraint qa (CLeft t) ->
                [ ( () , (someVar qa,t) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllTypesLowerBoundingTypes"
        [t| () |]
        [t| (ShallowType, ShallowType) |]
        [| \case
              IntermediateConstraint (CLeft t) (CLeft t') ->
                [ ( () , (t,t') ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllQualOrVarLowerBoundingQVar"
        [t| () |]
        [t| (QualOrVar, QVar) |]
        [| \case
              QualifiedIntermediateConstraint qv (CRight qa) ->
                [ ( () , (qv,qa) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllTypeOrVarLowerBoundingQVar"
        [t| () |]
        [t| (TypeOrVar, QVar) |]
        [| \case
              QualifiedLowerConstraint ta qa ->
                [ ( () , (ta,qa) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllQVarLowerBoundingQVar"
        [t| () |]
        [t| (QVar, QVar) |]
        [| \case
              QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) ->
                [ ( (), (qa1, qa2) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllUVarLowerBoundingQVar"
        [t| () |]
        [t| (UVar, QVar) |]
        [| \case
              QualifiedLowerConstraint (CRight a) qa ->
                [ ( (), (a, qa) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllQVarLowerBoundingUVar"
        [t| () |]
        [t| (QVar, UVar) |]
        [| \case
              QualifiedUpperConstraint qa (CRight a) ->
                [ ( (), (qa, a) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllUVarLowerBoundingUVar"
        [t| () |]
        [t| (UVar, UVar) |]
        [| \case
              IntermediateConstraint (CRight a1) (CRight a2) ->
                [ ( (), (a1, a2) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllMonomorphicQualifiedUpperConstraint"
        [t| () |]
        [t| (QVar, Set TQual) |]
        [| \case
              MonomorphicQualifiedUpperConstraint qa qs ->
                [ ( (), (qa, qs) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllOpaqueLowerBoundedConstraints"
        [t| () |]
        [t| (OpaqueVar, ShallowType) |]
        [| \case
              IntermediateConstraint (CLeft (SOpaque oa)) (CLeft t) ->
                [ ( (), (oa, t) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "AllOpaqueUpperBoundedConstraints"
        [t| () |]
        [t| (ShallowType, OpaqueVar) |]
        [| \case
              IntermediateConstraint (CLeft t) (CLeft (SOpaque oa)) ->
                [ ( (), (t, oa) ) ]
              _ -> []
        |]
        
    , QueryDescriptor
        "TypeOrAnyVarByAnyVarLowerBound"
        [t| AnyTVar |]
        [t| UVarBound |]
        [| \case
              IntermediateConstraint (CRight a) ta ->
                [ (someVar a, CLeft ta) ]
              QualifiedLowerConstraint (CRight a) qa ->
                [ (someVar a, CRight qa) ]
              QualifiedUpperConstraint qa ta ->
                [ (someVar qa, CLeft ta) ]
              QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) ->
                [ (someVar qa1, CRight qa2) ]
              _ -> []
        |]

    , QueryDescriptor
        "TypeOrAnyVarByAnyVarUpperBound"
        [t| AnyTVar |]
        [t| UVarBound |]
        [| \case
              IntermediateConstraint ta (CRight a) ->
                [ (someVar a, CLeft ta) ]
              QualifiedUpperConstraint qa (CRight a) ->
                [ (someVar a, CRight qa) ]
              QualifiedLowerConstraint ta qa ->
                [ (someVar qa, CLeft ta) ]
              QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) ->
                [ (someVar qa2, CRight qa1) ]
              _ -> []
        |]

    , QueryDescriptor
        "TypeByUVarLowerBound"
        [t| UVar |]
        [t| ShallowType |]
        [| \case
              IntermediateConstraint (CRight a) (CLeft t) -> [ (a, t) ]
              _ -> []
        |]

    , QueryDescriptor
        "TypeByUVarUpperBound"
        [t| UVar |]
        [t| ShallowType |]
        [| \case
              IntermediateConstraint (CLeft t) (CRight a) -> [ (a, t) ]
              _ -> []
        |]

    , QueryDescriptor
        "TypeByQVarLowerBound"
        [t| QVar |]
        [t| ShallowType |]
        [| \case
              QualifiedUpperConstraint qa (CLeft t) -> [ (qa, t) ]
              _ -> []
        |]

    , QueryDescriptor
        "TypeByQVarUpperBound"
        [t| QVar |]
        [t| ShallowType |]
        [| \case
              QualifiedLowerConstraint (CLeft t) qa -> [ (qa, t) ]
              _ -> []
        |]

    , QueryDescriptor
        "QualOrVarByQVarLowerBound"
        [t| QVar |]
        [t| QualOrVar |]
        [| \case
              QualifiedIntermediateConstraint (CRight qa) qv -> [ (qa, qv) ]
              _ -> []
        |]

    , QueryDescriptor
        "TypeOrVarByQVarLowerBound"
        [t| QVar |]
        [t| TypeOrVar |]
        [| \case
              QualifiedUpperConstraint qa ta -> [ (qa, ta) ]
              _ -> []
        |]

    , QueryDescriptor
        "TypeOrVarByQVarUpperBound"
        [t| QVar |]
        [t| TypeOrVar |]
        [| \case
              QualifiedLowerConstraint ta qa -> [ (qa, ta) ]
              _ -> []
        |]

    , QueryDescriptor
        "TQualSetByQVarLowerBound"
        [t| QVar |]
        [t| Set TQual |]
        [| \case
              QualifiedIntermediateConstraint (CRight qa) (CLeft qs) ->
                [ (qa, qs) ]
              _ -> []
        |]

    , QueryDescriptor
        "TQualSetByQVarUpperBound"
        [t| QVar |]
        [t| Set TQual |]
        [| \case
              QualifiedIntermediateConstraint (CLeft qs) (CRight qa) ->
                [ (qa, qs) ]
              _ -> []
        |]

    , QueryDescriptor
        "BoundingConstraintsByUVar"
        [t| UVar |]
        [t| Constraint |]
        [| \c -> (`zip` repeat c) $ case c of
              IntermediateConstraint (CRight a1) (CRight a2) -> [a1, a2]
              IntermediateConstraint (CRight a) _ -> [a]
              IntermediateConstraint _ (CRight a) -> [a]
              _ -> []
        |]

    , QueryDescriptor
        "BoundingConstraintsByQVar"
        [t| QVar |]
        [t| Constraint |]
        [| \c -> (`zip` repeat c) $ case c of
              QualifiedLowerConstraint _ qa -> [qa]
              QualifiedUpperConstraint qa _ -> [qa]
              QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) ->
                [qa1, qa2]
              QualifiedIntermediateConstraint (CRight qa) _ -> [qa]
              QualifiedIntermediateConstraint _ (CRight qa) -> [qa]
              _ -> []
        |]

    , QueryDescriptor
        "PolyLineageByOrigin"
        [t| QVar |]
        [t| QVar |]
        [| \case
              PolyinstantiationLineageConstraint qa1 qa2 -> [ (qa2, qa1) ]
              _ -> []
        |]

    , QueryDescriptor
        "OpaqueBounds"
        [t| OpaqueVar |]
        [t| (TypeOrVar, TypeOrVar) |]
        [| \case
              OpaqueBoundConstraint oa lb ub -> [ (oa, (lb, ub)) ]
              _ -> []
        |]
    
    ]
