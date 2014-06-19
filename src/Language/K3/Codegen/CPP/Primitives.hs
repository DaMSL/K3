{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Primitives where

import Data.Functor
import Data.Maybe (maybeToList)

import Control.Arrow ((&&&))

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Types

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

genCType :: K3 Type -> CPPGenM CPPGenR
genCType (tag -> TBool) = return (text "bool")
genCType (tag -> TByte) = return (text "unsigned char")
genCType (tag -> TInt) = return (text "int")
genCType (tag -> TReal) = return (text "double")
genCType (tag -> TString) = return (text "string")
genCType (tag &&& children -> (TOption, [t])) = (text "std::shared_ptr" <>) . angles <$> genCType t
genCType (tag &&& children -> (TIndirection, [t])) = (text "shared_ptr" <>) . angles <$> genCType t
genCType (tag &&& children -> (TTuple, [])) = return (text "unit_t")
genCType (tag &&& children -> (TTuple, ts))
    = (text "tuple" <>) . angles . sep . punctuate comma <$> mapM genCType ts
genCType t@(tag -> TRecord ids) = signature t >>= \sig -> addRecord sig (zip ids (children t)) >> return (text sig)
genCType (tag -> TDeclaredVar t) = return $ text t
genCType (tag &&& children &&& annotations -> (TCollection, ([et], as))) = do
    ct <- genCType et
    case annotationComboIdT as of
        Nothing -> return $ text "Collection" <> angles ct
        Just i' -> return $ text i' <> angles ct
genCType (tag -> TAddress) = return $ text "Address"
genCType (tag &&& children -> (TFunction, [ta, tr])) = do
    cta <- genCType ta
    ctr <- genCType tr
    return $ genCQualify (text "std") $ text "function" <> angles (ctr <> parens cta)

genCType t = throwE $ CPPGenE $ "Invalid Type Form " ++ show t

genCBind :: CPPGenR -> CPPGenR -> Int -> CPPGenR
genCBind f x n = genCQualify (text "std") (text "bind") <> tupled ([f, x] ++ placeholderList)
  where placeholderList = [ genCQualify (genCQualify (text "std") (text "placeholders")) (text "_" <> int i)
                          | i <- [1 .. n - 2]
                          ]

-- | Get the K3 Type of an expression. Relies on type-manifestation to have attached an EType
-- annotation to the expression ahead of time.
getKType :: K3 Expression -> CPPGenM (K3 Type)
getKType e = case e @~ \case { EType _ -> True; _ -> False } of
    Just (EType t) -> return t
    _ -> throwE $ CPPGenE $ "Absent type at " ++ show e
