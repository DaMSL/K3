{-# LANGUAGE ViewPatterns #-}
module Language.K3.Metaprogram.Primitives.Values where
import Control.Monad.Identity
import Control.Arrow ( (&&&) )

import Data.List
import Data.Maybe
import qualified Data.Map as Map

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal
import Language.K3.Core.Utils

import Language.K3.Core.Constructor.Type       as TC
import Language.K3.Core.Constructor.Literal    as LC
import Language.K3.Core.Constructor.Expression as EC

import Language.K3.Core.Metaprogram

{- Splice value extractors -}
idOfSLabel :: SpliceValue -> Maybe Identifier
idOfSLabel (SLabel i) = Just i
idOfSLabel _ = Nothing

tyOfSType :: SpliceValue -> Maybe (K3 Type)
tyOfSType (SType t) = Just t
tyOfSType _ = Nothing

expOfSExpr :: SpliceValue -> Maybe (K3 Expression)
expOfSExpr (SExpr e) = Just e
expOfSExpr _ = Nothing

elemsOfSList :: SpliceValue -> Maybe [SpliceValue]
elemsOfSList (SList svs) = Just svs
elemsOfSList _ = Nothing


{- Splice value operations -}
concatLabel :: SpliceValue -> SpliceValue -> SpliceValue
concatLabel (SLabel a) (SLabel b) = SLabel $ a ++ b
concatLabel _ _ = error "Invalid splice labels"

concatLabels :: SpliceValue -> SpliceValue
concatLabels (SList vs) = foldl concatLabel (SLabel "") vs
concatLabels _ = error "Invalid splice value container for concatLabels"

mkRecord :: SpliceValue -> SpliceValue
mkRecord (SList vs) = maybe err (SType . TC.record) $ mapM mkRecField vs
  where mkRecField v = do
          (SLabel n) <- ltLabel v
          (SType t)  <- ltType v
          return (n,t)
        err = error "Invalid splice container elements for mkRecord"

mkRecord _ = error "Invalid splice value container for mkRecord"

mkTuple :: SpliceValue -> SpliceValue
mkTuple (SList vs) = maybe err (SType . TC.tuple) $ mapM asType vs
  where asType (SType t) = Just t
        asType _ = Nothing
        err = error "Invalid splice container elements for mkTuple"

mkTuple _ = error "Invalid splice value container for mkTuple"

mkEmpty :: SpliceValue -> SpliceValue
mkEmpty (SType ct@(tnc -> (TCollection, [t]))) = SExpr $ EC.empty t @<- (map EAnnotation $ namedTAnnotations $ annotations ct)
mkEmpty _ = error "Invalid container type for constructing an empty collection."

mkEmptyByElem :: Identifier -> SpliceValue -> SpliceValue
mkEmptyByElem n (SType t) = SExpr $ EC.empty t @+ EAnnotation n
mkEmptyByElem _ _ = error "Invalid element type for constructing an empty collection."

listLabels :: SpliceValue -> SpliceValue
listLabels (SList vs) = maybe err SList $ mapM ltLabel vs
  where err = error "Invalid splice container elements for listLabels"

listLabels _ = error "Invalid splice value container for listLabels"

listTypes :: SpliceValue -> SpliceValue
listTypes (SList vs) = maybe err SList $ mapM ltType vs
  where err = error "Invalid splice container elements for listTypes"

listTypes _ = error "Invalid splice value container for listTypes"

{- Conversions -}
labelExpr :: SpliceValue -> SpliceValue
labelExpr (SExpr (tag -> EConstant (CString i))) = SLabel i
labelExpr (SExpr (tag -> EConstant (CInt i)))    = SLabel $ show i
labelExpr (SExpr (tag -> EConstant (CBool b)))   = SLabel $ show b
labelExpr _ = error "Invalid splice expression for labelExpr"

labelLiteral :: SpliceValue -> SpliceValue
labelLiteral (SLiteral (tag -> LString i)) = SLabel i
labelLiteral (SLiteral (tag -> LInt i))    = SLabel $ show i
labelLiteral (SLiteral (tag -> LBool b))   = SLabel $ show b
labelLiteral _ = error "Invalid splice literal for labelLiteral"

literalLabel :: SpliceValue -> SpliceValue
literalLabel (SLabel i) = SLiteral $ LC.string i
literalLabel _ = error "Invalid splice label for literalLabel"

literalType :: SpliceValue -> SpliceValue
literalType (SType t) = SLiteral . LC.string $ show t
literalType _ = error "Invalid splice label for literalType"

exprLabel :: SpliceValue -> SpliceValue
exprLabel (SLabel i) = SExpr $ EC.constant $ CString i
exprLabel _ = error "Invalid splice label for exprLabel"

exprType :: SpliceValue -> SpliceValue
exprType (SType t) = SExpr  $ EC.constant $ CString $ show t
exprType _ =  error "Invalid splice type for exprType"

{- Map specialization helpers. -}
specializeMapTypeByKV :: SpliceValue -> SpliceValue
specializeMapTypeByKV (SType t@(tnc -> (TRecord ["key", "value"], [(tag -> kt), _]))) =
  case kt of
    TInt    -> SType $ (TC.collection t) @+ TAnnotation "IntMap"
    TString -> SType $ (TC.collection t) @+ TAnnotation "StrMap"
    _       -> SType $ (TC.collection t) @+ TAnnotation "Map"

specializeMapTypeByKV sv = error $ "Invalid key-value record element for specializeMapTypeByKV: " ++ show sv

specializeMapEmptyByKV :: SpliceValue -> SpliceValue
specializeMapEmptyByKV (SType t@(tnc -> (TRecord ["key", "value"], [(tag -> kt), _]))) =
  case kt of
    TInt    -> SExpr $ (EC.empty t) @+ EAnnotation "IntMap"
    TString -> SExpr $ (EC.empty t) @+ EAnnotation "StrMap"
    _       -> SExpr $ (EC.empty t) @+ EAnnotation "Map"

specializeMapEmptyByKV sv = error $ "Invalid key-value record element for specializeMapEmptyByKV " ++ show sv

{- Index extractor spec contruction helpers. -}
mkIndexExtractor :: SpliceValue -> SpliceValue -> SpliceValue
mkIndexExtractor (SList keyLT) (SList specLL) =
  if length matches /= length keyLT
    then error "Invalid index extractor specifiers"
    else SLiteral $ LC.string $ intercalate ";" $ map extractor matches

  where extractor (SLabel k, _, SLiteral pathlit) =
          case tag pathlit of
            LString path -> k ++ "=" ++ path
            _ -> error "Invalid extractor field path"

        extractor _ = error "Invalid extractor spec"

        matches = join (map extractLT keyLT) (map extractLL specLL)

        join lt ll = catMaybes $ map (\(lbl, t) -> lookup lbl ll >>= \lit -> return (lbl, t, lit)) lt

        extractLL (SRecord fields) =
          case (Map.lookup spliceVIdSym fields, Map.lookup spliceVLSym fields) of
            (Just lbl, Just lit) -> (lbl, lit)
            (_, _) -> error "Invalid index extractor"

        extractLT (SRecord fields) =
          case (Map.lookup spliceVIdSym fields, Map.lookup spliceVTSym fields) of
            (Just lbl, Just typ) -> (lbl, typ)
            (_, _) -> error "Invalid index key"

mkIndexExtractor _ _ = error "Invalid index extraction arguments"

{- Distributed join constructors -}

equijoinMapType :: SpliceValue -> SpliceValue -> SpliceValue
equijoinMapType (SType (tnc -> (TFunction, [_, kt]))) (SType vt) = SType $ (TC.collection $ TC.record [("key", kt), ("value", vt)]) @+ TAnnotation "Map"
equijoinMapType _ _ = error "Invalid key/value types for equijoin map type"

equijoinEmptyMap :: SpliceValue -> SpliceValue -> SpliceValue
equijoinEmptyMap (SType (tnc -> (TFunction, [_, kt]))) (SType vt) = SExpr $ (EC.empty $ TC.record [("key", kt), ("value", vt)]) @+ EAnnotation "Map"
equijoinEmptyMap _ _ = error "Invalid key/value types for equijoin empty map"

broadcastJoinLHSVar :: SpliceValue -> SpliceValue -> SpliceValue
broadcastJoinLHSVar (SLabel lbl) (SExpr e) = case tag e of
    EVariable _ -> SExpr e
    _ -> SExpr $ EC.variable $ lbl ++ "_lhs"

broadcastJoinLHSVar _ _ = error "Invalid broadcast join lhsvar arguments"

broadcastjoinMaterialize :: SpliceValue -> SpliceValue -> SpliceValue
broadcastjoinMaterialize (SLabel lbl) (SExpr e) = case tag e of
    EVariable _ -> SExpr $ EC.unit
    _ -> SExpr $ EC.assign (lbl ++ "_lhs") e

broadcastjoinMaterialize _ _ = error "Invalid broadcast join materialization arguments"


{- Column-store helpers -}
columnJoinUpperBound :: Int
columnJoinUpperBound = 14

joinRange :: SpliceValue -> SpliceValue
joinRange (SLiteral (tag -> LInt i)) = SList $ map (SLiteral . LC.int) [1..(columnJoinUpperBound - i)]
joinRange _ = error "Invalid integer literal in joinRange"

{- BulkFlatCollection helpers -}
baseTableBFC :: SpliceValue -> K3 Expression -> SpliceValue
baseTableBFC (SExpr e) var = runIdentity $ mapTree replaceNode e >>= return . SExpr
  where
    replaceNode :: [K3 Expression] -> K3 Expression -> Identity (K3 Expression)
    replaceNode cs n@(annotations -> anns) = return . ((flip replaceCh) cs) $ if any hasProperty anns then var else n

    hasProperty :: Annotation Expression -> Bool
    hasProperty (EProperty (ePropertyName -> name)) = name == "BaseTable"
    hasProperty _ = False
baseTableBFC _ _ = error "Invalid expression in baseTableBFC"

extractPathBFC :: SpliceValue -> SpliceValue
extractPathBFC (SExpr e) = maybe (error "No path found in BaseTable property") (SExpr . EC.variable) (runIdentity $ foldTree nodeFn Nothing e)
  where
    nodeFn :: Maybe (String) -> K3 Expression -> Identity (Maybe String)
    nodeFn acc n@(annotations -> anns) = return $ case (foldl extract Nothing anns) of
                                                    Nothing -> acc
                                                    Just x -> Just x

    extract :: Maybe String -> Annotation Expression -> Maybe String
    extract acc (EProperty ( (ePropertyName &&& ePropertyValue) -> ("BaseTable", Just (tag -> LString s)) )) = Just s
    extract acc _ = acc
extractPathBFC _ = error "Invalid expression in extractPathBFC"

collectionContentType :: SpliceValue -> SpliceValue
collectionContentType (SType (tag &&& children -> (TCollection , (t:_) ))) = SType t
collectionContentType _ = error "Invalid type in collectionContentType"
