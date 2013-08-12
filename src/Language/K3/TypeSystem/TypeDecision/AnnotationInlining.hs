{-# LANGUAGE ScopedTypeVariables, TupleSections #-}

{-|
  This module defines the routines necessary to inline member annotation
  declarations for the purpose of constructing an initial environment.
  
  Inlining proceeds by extracting the annotations from the AST and representing
  them in a form more amenable to duplicate detection.  This form is essentially
  a dictionary of dictionary pairs.  The outermost layer maps identifiers to the
  annotations bound under them.  Each annotation is represented as a pair of
  dictionaries which map identifiers to the binding lifted attributes and
  attributes, respectively.  In this form, annotations also track the member
  annotation declarations they contain as well as a set of them which has been
  processed; this helps to resolve cyclic dependencies and reduces duplicate
  work.
-}
module Language.K3.TypeSystem.TypeDecision.AnnotationInlining
( FlatAnnotation
, FlatAnnotationDecls
, inlineAnnotations
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Maybe
import Data.Monoid
import qualified Data.Set as Set
import Data.Set (Set)
import qualified Data.Traversable as Trav
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Utils
import Language.K3.TypeSystem.Utils.K3Tree

-- |The internal representation of annotations during and after inlining.
data AnnRepr = AnnRepr
                  { liftedAttMap :: Map Identifier AnnMemDecl
                  , schemaAttMap :: Map Identifier AnnMemDecl
                  , memberAnnDecls :: Set (Identifier, TPolarity)
                  , processedMemberAnnDecls :: Set (Identifier, TPolarity)
                  }

-- |A data type which tracks information about an @AnnRepr@ in addition to the
--  @AnnRepr@ itself.
type TaggedAnnRepr = (AnnRepr, Span, K3 Declaration)

-- |A type alias describing the representation of annotations internal to the
--  type decision procedure.  The first list of annotations describes the
--  lifted attributes; the second list describes the schema attributes.
type FlatAnnotation = ([AnnMemDecl], [AnnMemDecl])
-- |A type alias describing the representation of a group of declared
--  annotations internal to the type decision procedure.  The declaration
--  included here is solely for error reporting purposes; it indicates the
--  original declaration (pre-inlining) from which the @FlatAnnotation@ was
--  derived.
type FlatAnnotationDecls = Map Identifier (FlatAnnotation, K3 Declaration)

emptyRepr :: AnnRepr
emptyRepr = AnnRepr Map.empty Map.empty Set.empty Set.empty

appendRepr :: forall m. (TypeErrorI m, Monad m)
           => AnnRepr -> AnnRepr -> m AnnRepr
appendRepr (AnnRepr lam sam mad umad) (AnnRepr lam' sam' mad' umad') = do
  overlapCheck lam lam'
  overlapCheck sam sam'
  return $ AnnRepr (lam `mappend` lam') (sam `mappend` sam')
                   (mad `mappend` mad') (umad `mappend` umad')
  where
    overlapCheck :: Map Identifier AnnMemDecl -> Map Identifier AnnMemDecl
                 -> m ()
    overlapCheck m1 m2 =
      let ks = Set.toList $ Set.fromList (Map.keys m1) `Set.intersection`
                            Set.fromList (Map.keys m2) in
      unless (null ks) $
        let i = head ks in
        typeError $ MultipleAnnotationBindings i [m1 Map.! i, m2 Map.! i]
        
concatReprs :: forall m. (TypeErrorI m, Monad m)
           => [AnnRepr] -> m AnnRepr
concatReprs = foldM appendRepr emptyRepr

-- |Converts a single annotation into internal representation.
convertAnnotationToRepr :: forall m. (TypeErrorI m, Monad m)
                        => [AnnMemDecl] -> m AnnRepr
convertAnnotationToRepr mems =
  concatReprs =<< mapM convertMemToRepr mems
  where
    convertMemToRepr :: AnnMemDecl -> m AnnRepr
    convertMemToRepr mem = case mem of
      Lifted _ i _ _ _ ->
        return $ AnnRepr (Map.singleton i mem) Map.empty Set.empty Set.empty
      Attribute _ i _ _ _ ->
        return $ AnnRepr Map.empty (Map.singleton i mem) Set.empty Set.empty
      MAnnotation pol i _ ->
        let s = Set.singleton (i,typeOfPol pol) in
        return $ AnnRepr Map.empty Map.empty s Set.empty

-- |Given a role AST, converts all annotations contained within to the internal
--  form.
convertAstToRepr :: forall m. (TypeErrorI m, Monad m, Functor m)
                 => K3 Declaration -> m (Map Identifier TaggedAnnRepr)
convertAstToRepr ast =
  case tag ast of
    DRole _ ->
      let decls = subForest ast in
      Map.fromList <$> catMaybes <$> mapM declToRepr decls
    _ -> internalTypeError $ TopLevelDeclarationNonRole ast
  where
    declToRepr :: K3 Declaration -> m (Maybe (Identifier, TaggedAnnRepr))
    declToRepr decl = case tag decl of
      DAnnotation i mems -> do
        repr <- convertAnnotationToRepr mems
        s <- spanOf decl
        return $ Just (i, (repr, s, decl))
      _ -> return Nothing

-- |Given a map of internal annotation representations, performs closure over
--  their member annotation declarations until they have all been processed.
--  Because closure may reveal overlapping bindings, this process may fail.
--  If it does not, every member annotation will have been processed.
closeReprs :: forall m. (TypeErrorI m, Monad m, Functor m, Applicative m)
           => Map Identifier TaggedAnnRepr
                -- ^The current representation of annotations
           -> m (Map Identifier TaggedAnnRepr)
closeReprs dict = do
  -- FIXME: This isn't quite an accurate representation of the scenario.  The
  --        annotations from the external environment (in theory obtained from
  --        other modules) do not appear here.  They should be in the dictionary
  --        passed to updateRepr below.
  computed <- Trav.sequence $ Map.map (updateRepr dict) dict
  let result = Map.map fst computed
  let progress = any (snd . snd) (Map.toList computed)
  (if progress then closeReprs else return) result
  where
    updateRepr :: Map Identifier TaggedAnnRepr -> TaggedAnnRepr
               -> m (TaggedAnnRepr, Bool)
    updateRepr current (repr,s,decl) =
      let unproc = memberAnnDecls repr Set.\\ processedMemberAnnDecls repr in
      if Set.null unproc then return ((repr,s,decl), False) else
        do
          let (i,pol) = Set.findMin unproc
          (repr',_,_) <- 
              fromMaybe <$> typeError (UnboundTypeEnvironmentIdentifier s $
                                          TEnvIdentifier i)
                        <*> return (Map.lookup i current)
          let repr'' = case pol of
                          Positive -> repr'
                          Negative -> negatize repr'
          newRepr <- appendRepr repr repr''
          let newRepr' = newRepr { processedMemberAnnDecls =
                                      Set.insert (i,pol) $
                                        processedMemberAnnDecls newRepr }
          return ((newRepr',s,decl),True) 
    negatize :: AnnRepr -> AnnRepr
    negatize (AnnRepr lam sam mad umad) =
      AnnRepr (negMap lam) (negMap sam) (negSet mad) (negSet umad)
      where
        negMap = Map.map negDecl
        negSet = Set.map (second $ const Negative)
        negDecl mem = case mem of
                        Lifted _ i tExpr mexpr s ->
                          Lifted Requires i tExpr mexpr s
                        Attribute _ i tExpr mexpr s ->
                          Attribute Requires i tExpr mexpr s
                        MAnnotation _ i s -> MAnnotation Requires i s

-- |A routine which performs annotation inlining.  The input arrives in the form
--  of a top-level K3 AST.  The result is a mapping from identifiers to
--  annotation representations.  Each representation is a pair of maps from
--  identifiers to relevant member declarations; the first represents lifted
--  attributes while the second represents schema attributes.
inlineAnnotations :: (TypeErrorI m, Monad m, Functor m, Applicative m)
                  => K3 Declaration
                  -> m FlatAnnotationDecls
inlineAnnotations decl = do
  m <- convertAstToRepr decl
  m' <- closeReprs m
  return $ Map.map f m'
  where
    f (repr,_,decl') =
        ( ( Map.elems $ liftedAttMap repr
          , Map.elems $ schemaAttMap repr)
        , decl')
