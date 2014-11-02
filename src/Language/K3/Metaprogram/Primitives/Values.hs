module Language.K3.Metaprogram.Primitives.Values where

import Language.K3.Core.Constructor.Type       as TC
import Language.K3.Core.Constructor.Literal    as LC
import Language.K3.Core.Constructor.Expression as EC

import Language.K3.Core.Metaprogram

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

listLabels :: SpliceValue -> SpliceValue
listLabels (SList vs) = maybe err SList $ mapM ltLabel vs
  where err = error "Invalid splice container elements for listLabels"

listLabels _ = error "Invalid splice value container for listLabels"

listTypes :: SpliceValue -> SpliceValue
listTypes (SList vs) = maybe err SList $ mapM ltType vs
  where err = error "Invalid splice container elements for listTypes"

listTypes _ = error "Invalid splice value container for listTypes"

literalLabel :: SpliceValue -> SpliceValue
literalLabel (SLabel i) = SLiteral $ LC.string i
literalLabel _ = error "Invalid splice label for literalLabel"

literalType :: SpliceValue -> SpliceValue
literalType (SType t) = SLiteral . LC.string $ show t
literalType _ = error "Invalid splice label for literalType"

exprLabel :: SpliceValue -> SpliceValue
exprLabel (SLabel i) = SExpr $ EC.constant $ CString i
exprLabel _ = error "Invalid splice label for exprLabel"
