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
-- return-type. Additionally however, we must generate a trigger-wrapper function to perform
-- deserialization.
declaration (tag -> DTrigger i t e) = do
    addTrigger i
    d <- declaration (D.global i (T.function t T.unit) (Just e))
    w <- triggerWrapper i t
    return $ d <$$> w

declaration (tag &&& children -> (DRole _, cs)) = do
    subDecls <- vsep . punctuate line <$> mapM declaration cs
    currentS <- get
    i <- genCType T.unit >>= \ctu ->
        return $ ctu <+> text "initGlobalDecls" <> parens empty <+> hangBrace (initializations currentS <> text "return unit_t();")
    let amp = annotationMap currentS
    compositeDecls <- forM (S.toList $ S.filter (not . S.null) $ composites currentS) $ \(S.toList -> als) ->
        composite (annotationComboId als) [(a, M.findWithDefault [] a amp) | a <- als]
    recordDecls <- forM (M.toList $ recordMap currentS) $ uncurry record
    tablePop <- generateDispatchPopulation
    let tableDecl = text "TriggerDispatch" <+> text "dispatch_table" <> semi

    newS <- get

    return $ vsep $ punctuate line $
               [text "using K3::Collection;"]
            ++ forwards newS
            ++ compositeDecls
            ++ recordDecls
            ++ [subDecls, i, tableDecl, tablePop]

declaration (tag -> DAnnotation i _ amds) = addAnnotation i amds >> return empty
declaration _ = return empty

-- | Generates a function which populates the trigger dispatch table.
generateDispatchPopulation :: CPPGenM CPPGenR
generateDispatchPopulation = do
    triggerS <- triggers <$> get
    dispatchStatements <- mapM genDispatch (S.toList triggerS)
    return $ genCFunction Nothing (text "void") (text "populate_dispatch") [] (vsep dispatchStatements)
  where
    genDispatch tName = return $
        text ("dispatch_table[\"" ++ tName ++ "\"] = " ++ genDispatchName tName) <> semi

genDispatchName :: Identifier -> Identifier
genDispatchName i = i ++ "_dispatch"

-- | Generate a trigger-wrapper function, which performs deserialization of an untyped message
-- (using Boost serialization) and call the appropriate trigger.
triggerWrapper :: Identifier -> K3 Type -> CPPGenM CPPGenR
triggerWrapper i t = do
    tmpDecl <- cDecl t "arg"
    tmpType <- genCType t
    let triggerDispatch = text i <> parens (text "arg") <> semi
    let unpackCall = text "arg" <+> equals <+> text "*" <> genCCall (text "unpack") (Just [tmpType]) [text "msg"] <> semi
    return $ genCFunction Nothing (text "void") (text i <> text "_dispatch") [text "string msg"] $ hangBrace (
            vsep [
                tmpDecl,
                unpackCall,
                triggerDispatch,
                text "return;"
            ])

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
