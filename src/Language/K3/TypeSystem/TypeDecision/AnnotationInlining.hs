{-# LANGUAGE ScopedTypeVariables, TupleSections, TemplateHaskell #-}

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
( AnnMemRepr(..)
, TypeParameterContext
, TypeParameterContextEntry
, FlatAnnotation
, FlatAnnotationDecls
, inlineAnnotations
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Data.List
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
import Language.K3.Core.Type
import Language.K3.Utils.Pretty
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Utils
import Language.K3.TypeSystem.Utils.K3Tree
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |An internal repersentation of an annotation member.  This representation
--  carries additional metadata which applies to the member in particular; this
--  allows inlining to occur without confusing the context in which the member
--  was declared.  In particular, @typeParamCxt@ describes the context of
--  declared type parameters on this annotation member and @reprNative@
--  indicates whether or not the member is "native" (e.g. was not inlined from
--  another source).
data AnnMemRepr = AnnMemRepr
                    { reprMem      :: AnnMemDecl
                    , typeParamCxt :: TypeParameterContext
                    , reprNative   :: Bool
                    }
  deriving (Show)

-- |A type alias for type parameter contexts.  Each entry maps the name of a
--  declared type parameter to a pair of type variables chosen for it as well as
--  an upper bounding type expression when one was provided.
type TypeParameterContext = Map Identifier TypeParameterContextEntry
type TypeParameterContextEntry = (UVar,QVar,Maybe (K3 Type))

-- |The internal representation of annotations during and after inlining.  The
--  type parameter map relates declared type variable identifiers to the type
--  variables which represent them.  This map only includes type variables from
--  the current annotation and not those from included annotations (via the
--  "requires annotation" or "provides annotation" syntax) since it is only
--  necessary to test the new parametricity.
data AnnRepr = AnnRepr
                  { liftedAttMap :: Map Identifier AnnMemRepr
                  , schemaAttMap :: Map Identifier AnnMemRepr
                  , memberAnnDecls :: Set (Identifier, TPolarity)
                  , processedMemberAnnDecls :: Set (Identifier, TPolarity)
                  }

instance Pretty AnnRepr where
  prettyLines (AnnRepr lam sam mad umad) =
    ["〈 "] %+ prettyAttMap lam %$
    [", "] %+ prettyAttMap sam %$
    [", "] %+ intersperseBoxes [", "]
                (map (prettyTriple . rearrange) $ Set.toList $
                  mad Set.\\ umad)
    +% [" 〉"]
    where
      prettyAttMap m = intersperseBoxes [", "] $ map prettyEntry $ Map.toList m
      rearrange (i,pol) = (pol,i,Nothing)
      prettyEntry (_,memRepr) = prettyMem $ reprMem memRepr
      prettyMem mem =
        prettyTriple $ case mem of
          Lifted pol i _ _ anns    -> (typeOfPol pol,i,uidFromAnns anns)
          Attribute pol i _ _ anns -> (typeOfPol pol,i,uidFromAnns anns)
          MAnnotation pol i anns   -> (typeOfPol pol,i,uidFromAnns anns)
      prettyTriple (pol,i,mu) = [i ++ pStr pol ++ ": UID#" ++
        maybe "?" (\(UID u) -> show u) mu]
      pStr pol = case pol of { Positive -> "+"; Negative -> "-" }
      uidFromAnns anns = maybe Nothing extractUID $ find isDUID anns
      extractUID (DUID u) = Just u
      extractUID _        = Nothing

-- |A data type which tracks information about an @AnnRepr@ in addition to the
--  @AnnRepr@ itself.
type TaggedAnnRepr = (AnnRepr, UID, K3 Declaration)

-- |A type alias describing the representation of annotations internal to the
--  type decision procedure.  The first list of annotations describes the
--  lifted attributes; the second list describes the schema attributes.
type FlatAnnotation = (TypeParameterContext, [AnnMemRepr], [AnnMemRepr])
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
appendRepr (AnnRepr lam sam mad umad) (AnnRepr lam' sam' mad' umad') =
  do
    overlapCheck lam lam'
    overlapCheck sam sam'
    return $ AnnRepr (lam `mappend` lam') (sam `mappend` sam')
                     (mad `mappend` mad') (umad `mappend` umad')
  where
    overlapCheck :: Map Identifier AnnMemRepr -> Map Identifier AnnMemRepr
                 -> m ()
    overlapCheck m1 m2 =
      let ks = Set.toList $ Set.fromList (Map.keys m1) `Set.intersection`
                            Set.fromList (Map.keys m2) in
      unless (null ks) $
        let i = head ks in
        typeError $ MultipleAnnotationBindings i [ reprMem $ m1 Map.! i
                                                 , reprMem $ m2 Map.! i]

concatReprs :: forall m. (TypeErrorI m, Monad m)
           => [AnnRepr] -> m AnnRepr
concatReprs = foldM appendRepr emptyRepr

-- |Converts a single annotation into internal representation.
convertAnnotationToRepr :: forall m. (TypeErrorI m, Monad m)
                        => TypeParameterContext -> [AnnMemDecl]
                        -> m AnnRepr
convertAnnotationToRepr typeParams mems =
  concatReprs =<< mapM convertMemToRepr mems
  where
    convertMemToRepr :: AnnMemDecl -> m AnnRepr
    convertMemToRepr mem =
      let repr = AnnMemRepr mem typeParams True in
      case mem of
        Lifted _ i _ _ _ ->
          return $ AnnRepr (Map.singleton i repr) Map.empty Set.empty Set.empty
        Attribute _ i _ _ _ ->
          return $ AnnRepr Map.empty (Map.singleton i repr) Set.empty Set.empty
        MAnnotation pol i _ ->
          let s = Set.singleton (i,typeOfPol pol) in
          return $ AnnRepr Map.empty Map.empty s Set.empty

-- |Given a role AST, converts all annotations contained within to the internal
--  form.
convertAstToRepr :: forall m. ( TypeErrorI m, FreshVarI m, Monad m
                              , Applicative m, Functor m)
                 => K3 Declaration
                 -> m ( Map Identifier TypeParameterContext
                      , Map Identifier TaggedAnnRepr)
convertAstToRepr ast =
  case tag ast of
    DRole _ -> do
      let decls = subForest ast
      m' <- Map.fromList <$> catMaybes <$> mapM declToRepr decls
      return (Map.map fst m', Map.map snd m')
    _ -> internalTypeError $ TopLevelDeclarationNonRole ast
  where
    declToRepr :: K3 Declaration -> m (Maybe (Identifier, (TypeParameterContext, TaggedAnnRepr)))
    declToRepr decl = case tag decl of
      DDataAnnotation i _ vdecls mems -> do
        u <- uidOf decl
        -- TODO: for parametricity, also include the bounding expression(s)
        --       in the type params map
        typeParams <- Map.fromList <$> mapM contextEntryForVDecl vdecls
        repr <- convertAnnotationToRepr typeParams mems
        _debug $ boxToString $ ["Annotation " ++ i ++ " representation: "] %$
                                  indent 2 (prettyLines repr)
        return $ Just (i, (typeParams, (repr, u, decl)))
      _ -> return Nothing
      where
        contextEntryForVDecl :: TypeVarDecl
                             -> m (Identifier, TypeParameterContextEntry)
        contextEntryForVDecl (TypeVarDecl i' mlbtExpr mubtExpr) = do
          u <- uidOf decl
          let origin = TVarAnnotationDeclaredParamOrigin u i'
          a <- freshUVar origin
          qa <- freshQVar origin
          when (isJust mlbtExpr) $
            error "Type decision does not support declared lower bounds!"
          return (i',(a,qa,mubtExpr))

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
              maybe (typeError (UnboundTypeEnvironmentIdentifier s $ TEnvIdentifier i))
                    return $ Map.lookup i current
          let repr'' = case pol of
                          Positive -> repr'
                          Negative -> negatize repr'
          newRepr <- appendRepr repr $ foreignize repr''
          let newRepr' = newRepr { processedMemberAnnDecls =
                                      Set.insert (i,pol) $
                                        processedMemberAnnDecls newRepr }
          return ((newRepr',s,decl),True)
    foreignize :: AnnRepr -> AnnRepr
    foreignize (AnnRepr lam sam mad umad) =
      AnnRepr (Map.map f lam) (Map.map f sam) mad umad
      where f (AnnMemRepr mem tps _) = AnnMemRepr mem tps False
    negatize :: AnnRepr -> AnnRepr
    negatize (AnnRepr lam sam mad umad) =
      AnnRepr (negMap lam) (negMap sam) (negSet mad) (negSet umad)
      where
        negMap = Map.map negRepr
        negSet = Set.map (second $ const Negative)
        negRepr (AnnMemRepr mem tps ntv) = AnnMemRepr (negMem mem) tps ntv
        negMem mem = case mem of
            Lifted _ i tExpr _ anns    -> Lifted Requires i tExpr Nothing anns
            Attribute _ i tExpr _ anns -> Attribute Requires i tExpr Nothing anns
            MAnnotation _ i anns       -> MAnnotation Requires i anns

-- |A routine which performs annotation inlining.  The input arrives in the form
--  of a top-level K3 AST.  The result is a mapping from identifiers to
--  annotation representations.  Each representation is a pair of maps from
--  identifiers to relevant member declarations; the first represents lifted
--  attributes while the second represents schema attributes.
inlineAnnotations :: ( FreshVarI m, TypeErrorI m, Monad m, Functor m
                     , Applicative m)
                  => K3 Declaration
                  -> m FlatAnnotationDecls
inlineAnnotations decl = do
  (cxtm, reprm) <- convertAstToRepr decl
  reprm' <- closeReprs reprm
  return $ Map.mapWithKey (f cxtm) reprm'
  where
    f cxtm i (repr,UID u,decl') =
        let ans = ( ( cxtm Map.! i
                    , Map.elems $ liftedAttMap repr
                    , Map.elems $ schemaAttMap repr)
                  , decl')
        in _debugI
              ( boxToString $ ["Inlined form of UID " ++ show u ++ ":"] %$
                  indent 2 (prettyLines repr) )
              ans
