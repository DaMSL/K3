{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Declaration where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Functor

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Collections
import Language.K3.Codegen.CPP.Expression
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

declaration :: K3 Declaration -> CPPGenM CPPGenR
declaration (tag -> DGlobal i _ _) | "register" `L.isPrefixOf` i = return empty
declaration (tag -> DGlobal _ (tag -> TSource) _) = return empty
declaration (tag -> DGlobal name t@(tag -> TFunction) Nothing) | any (\y -> y `L.isSuffixOf` name) source_builtins = genSourceBuiltin t name
                                                               | otherwise = return empty
declaration (tag -> DGlobal i t Nothing) = cDecl t i
declaration (tag -> DGlobal i t@(tag &&& children -> (TFunction, [ta, tr]))
            (Just (tag &&& children -> (ELambda x, [b])))) = do
    newF <- cDecl t i
    addForward newF
    body <- reify RReturn b
    cta <- genCType ta
    ctr <- genCType tr
    return $ ctr <+> text i <> parens (cta <+> text x) <+> hangBrace body

declaration (tag -> DGlobal i t (Just e)) = do
    newI <- reify (RName i) e
    modify (\s -> s { initializations = initializations s <//> newI })
    cDecl t i

-- The generated code for a trigger is the same as that of a function with corresponding ()
-- return-type.
declaration (tag -> DTrigger i t e) = do
    addTrigger i
    dispClass <- dispatchClass i t
    addForward dispClass
    declaration (D.global i (T.function t T.unit) (Just e))

declaration (tag &&& children -> (DRole _, cs)) = do
    subDecls <- vsep . punctuate line <$> mapM declaration cs
    currentS <- get
    i <- genCType T.unit >>= \ctu ->
        return $ ctu <+> text "initGlobalDecls" <> parens empty <+> hangBrace (initializations currentS <> text "return unit_t();")
    let amp = annotationMap currentS
    compositeDecls <- forM (S.toList $ S.filter (not . S.null) $ composites currentS) $ \(S.toList -> als) ->
        composite (annotationComboId als) [(a, M.findWithDefault [] a amp) | a <- als]
    recordDecls <- forM (M.toList $ recordMap currentS) $ (\(_, (unzip -> (ids, _))) -> record ids)
    tablePop <- generateDispatchPopulation

    newS <- get

    return $ vsep $ punctuate line $
               [text "using K3::Collection;"]
            ++ forwards newS
            ++ compositeDecls
            ++ recordDecls
            ++ [subDecls, i, tablePop]

declaration (tag -> DAnnotation i _ amds) = addAnnotation i amds >> return empty
declaration _ = return empty

-- | Generates a function which populates the trigger dispatch table.
generateDispatchPopulation :: CPPGenM CPPGenR
generateDispatchPopulation = do
    triggerS <- triggers <$> get
    dispatchStatements <- mapM genDispatch (S.toList triggerS)
    return $ genCFunction Nothing (text "void") (text "populate_dispatch") [] (vsep dispatchStatements)
  where
    genDispatch tName =
      let className = genDispatchClassName tName in
      return $ text $ "dispatch_table[\"" ++ tName ++ "\"] = make_shared<" ++ className ++ ">();"

-- Generate a trigger-wrapper Dispatcher class
dispatchClass :: Identifier -> K3 Type -> CPPGenM CPPGenR
dispatchClass i t = do
  cType <- genCType t
  let className = genDispatchClassName i
      -- If we have a primitive type, we treat it somewhat differently
      (sharedCType, ptrDeref, primDeref) =
        if primitiveType t then (cType, text "", text "*")
        else (text "shared_ptr" <> angles cType, text "*", text "")
  return $
    vsep [
      text $ "class " ++ className ++ " : public Dispatcher {",
      text   "  public:",
      text   "    " <> text className <> parens (sharedCType <+> text "arg") <+> text ": _arg(arg) {}",
      text   "    " <> text className <> text "()" <+> text "{}",
      text   "",
      text   "    void dispatch() const {",
      text   "        " <> text i <> parens (ptrDeref <> text "_arg") <> semi,
      text   "    }",
      text   "",
      text   "    void unpack(const string &msg) {",
      text   "        _arg =" <+> primDeref <> text "BoostSerializer::unpack<" <> cType <> text ">(msg);",
      text   "    }",
      text   "",
      text   "    string pack() const {",
      text   "        return BoostSerializer::pack<" <> cType <> text ">(" <> ptrDeref <> text "_arg);",
      text   "    }",
      text   "",
      text   "  private:",
      text   "    " <> sharedCType <+> text "_arg;",
      text   "};"
    ]

-- Generated Builtins
-- Interface for source builtins.
-- Map special builtin suffix to a function that will generate the builtin.
source_builtin_map :: [(String, (String -> K3 Type -> String -> CPPGenM CPPGenR))]
source_builtin_map = [("HasRead", genHasRead), ("Read", genDoRead)]

source_builtins :: [String]
source_builtins = map fst source_builtin_map

-- Grab the generator function from the map, currying the key of the builtin to be generated.
getSourceBuiltin :: String -> (K3 Type -> String -> CPPGenM CPPGenR)
getSourceBuiltin k =
  case filter (\(x,_) -> k == x) source_builtin_map of
    []         -> error $ "Could not find builtin with name" ++ k
    ((_,f):_) -> f k

genHasRead :: String -> K3 Type -> String -> CPPGenM CPPGenR
genHasRead suf _ name = do
  source_name <- return $ stripSuffix suf name
  body  <- return $ text "return engine.hasRead" <> parens (dquotes $ text source_name) <> semi
  forward <- return $ text "bool" <+> text name <> parens (text "unit_t") <> semi
  addForward forward
  return $ genCFunction Nothing (text "bool") (text name) [text "unit_t"] body

genDoRead :: String -> K3 Type -> String -> CPPGenM CPPGenR
genDoRead suf typ name = do
    ret_type  <- genCType $ last $ children typ
    source_name <- return $ stripSuffix suf name
    res_decl <- return $ ret_type <+> text "result" <> semi
    doRead <- return $ text "*engine.doReadExternal" <> parens (dquotes $ text source_name)
    doPatch <- return $ text "do_patch" <> angles ret_type <> parens (doRead <> comma <> text "result") <> semi
    ret <- return $ text "return result;"
    body <- return $ vsep $ [res_decl, doPatch, ret]
    forward <- return $ ret_type <+> text name <> parens (text "unit_t") <> semi
    addForward forward
    return $ genCFunction Nothing ret_type (text name) [text "unit_t"] body

stripSuffix :: String -> String -> String
stripSuffix suffix name = maybe (error "not a suffix!") reverse $ L.stripPrefix (reverse suffix) (reverse name)

genSourceBuiltin :: K3 Type -> Identifier -> CPPGenM CPPGenR
genSourceBuiltin typ name = do
  suffix <- return $ head $ filter (\y -> y `L.isSuffixOf` name) source_builtins
  f <- return $ getSourceBuiltin suffix
  f typ name
