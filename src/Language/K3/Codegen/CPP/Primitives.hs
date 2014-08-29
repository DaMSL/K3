{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Primitives where

import Data.Char
import Data.Functor
import Data.List (sort)
import Data.Maybe (maybeToList)

import Control.Arrow ((&&&))

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Types

import qualified Language.K3.Codegen.CPP.Representation as R

-- | Generate a (potentially templated) C++ function definition.
genCFunction :: Maybe [CPPGenR] -> CPPGenR -> CPPGenR -> [CPPGenR] -> CPPGenR -> CPPGenR
genCFunction mta rt f args body = vsep $ tl ++ [rt <+> f <> tupled args <+> hangBrace body]
  where tl = maybeToList $ genCTemplateDecl <$> mta

-- | Generate a (potentially templated) C++ function call.
genCCall :: CPPGenR -> Maybe [CPPGenR] -> [CPPGenR] -> CPPGenR
genCCall f ts as = f <> (maybe empty $ \ts' -> angles (hcat $ punctuate comma ts')) ts <> tupled as

-- | Generate a C++ namespace qualification.
genCQualify :: CPPGenR -> CPPGenR -> CPPGenR
genCQualify namespace name = namespace <> text "::" <> name

-- | Generate a C++ declaration, with optional initializer.
genCDecl :: CPPGenR -> CPPGenR -> Maybe CPPGenR -> CPPGenR
genCDecl t n Nothing = t <+> n <> semi
genCDecl t n (Just e) = t <+> n <+> equals <+> e <> semi

-- | Generate a C++ assignment.
genCAssign :: CPPGenR -> CPPGenR -> CPPGenR
genCAssign a b = a <+> equals <+> b

-- | Generate a template declaration from a list of template variables.
genCTemplateDecl :: [CPPGenR] -> CPPGenR
genCTemplateDecl ta = text "template" <+> angles (hcat $ punctuate comma [text "class" <+> a | a <- ta])

genCType :: K3 Type -> CPPGenM R.Type
genCType (tag -> TBool) = return $ R.Primitive R.PBool
genCType (tag -> TByte) = return R.Byte
genCType (tag -> TInt) = return $ R.Primitive R.PInt
genCType (tag -> TReal) = return $ R.Primitive R.PDouble
genCType (tag -> TNumber) = return $ R.Primitive R.PDouble
genCType (tag -> TString) = return $ R.Primitive R.PString
genCType (tag &&& children -> (TOption, [t])) = R.Pointer <$> genCType t
genCType (tag &&& children -> (TIndirection, [t])) = R.Pointer <$> genCType t
genCType (tag &&& children -> (TTuple, [])) = return R.Unit
genCType (tag &&& children -> (TTuple, ts)) = R.Tuple <$> mapM genCType ts
genCType t@(tag -> TRecord ids) = do
  let sig = recordSignature ids
  let children' = snd . unzip . sort $ zip ids (children t)
  addRecord sig (zip ids (children t))
  templateVars <- mapM genCType children'
  return $ R.Named (R.Specialized templateVars $ R.Name sig)
genCType (tag -> TDeclaredVar t) = return $ R.Named (R.Name t)
genCType (tag &&& children &&& annotations -> (TCollection, ([et], as))) = do
    ct <- genCType et
    case annotationComboIdT as of
        Nothing -> return $ R.Named (R.Specialized [ct] $ R.Name "Collection")
        Just i' -> return $ R.Named (R.Specialized [ct] $ R.Name i')
genCType (tag -> TAddress) = return R.Address
genCType (tag &&& children -> (TFunction, [_, _])) = return R.Inferred

genCType (tag &&& children -> (TForall _, [t])) = genCType t
genCType (tag -> TDeclaredVar i) = return $ R.Named (R.Name $ map toUpper i)
genCType t = throwE $ CPPGenE $ "Invalid Type Form " ++ show t

genCBind :: CPPGenR -> CPPGenR -> Int -> CPPGenR
genCBind f x n = genCQualify (text "std") (text "bind") <> tupled ([f, x] ++ placeholderList)
  where placeholderList = [ genCQualify (genCQualify (text "std") (text "placeholders")) (text "_" <> int i)
                          | i <- [1 .. n - 2]
                          ]

genCInferredType :: K3 Type -> CPPGenM R.Type
genCInferredType (tag -> TDeclaredVar _) = return R.Inferred
genCInferredType t = genCType t

-- | Get the K3 Type of an expression. Relies on type-manifestation to have attached an EType
-- annotation to the expression ahead of time.
getKType :: K3 Expression -> CPPGenM (K3 Type)
getKType e = case e @~ \case { EType _ -> True; _ -> False } of
    Just (EType t) -> return t
    _ -> throwE $ CPPGenE $ "Absent type at " ++ show e
