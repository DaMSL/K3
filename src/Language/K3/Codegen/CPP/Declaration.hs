{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Declaration where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Maybe

import qualified Data.List as L

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Codegen.CPP.Expression
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import qualified Language.K3.Codegen.CPP.Representation as R

declaration :: K3 Declaration -> CPPGenM [R.Definition]
declaration (tag -> DGlobal _ (tag -> TSource) _) = return []

-- Global functions without implementations -- Built-Ins.
declaration (tag -> DGlobal name t@(tag -> TFunction) Nothing) | any (`L.isSuffixOf` name) source_builtins = genSourceBuiltin t name >>= return . replicate 1
                                                               | otherwise = return []

-- Global monomorphic function with direct implementations.
declaration (tag -> DGlobal i (tag &&& children -> (TFunction, [ta, tr]))
                        (Just (tag &&& children -> (ELambda x, [body])))) = do
    cta <- genCType ta
    ctr <- genCType tr

    cbody <- reify RReturn body

    addForward $ R.FunctionDecl (R.Name i) [cta] ctr

    return [R.FunctionDefn (R.Name i) [(x, cta)] (Just ctr) [] cbody]

-- Global polymorphic functions with direct implementations.
declaration (tag -> DGlobal i (tag &&& children -> (TForall _, [tag &&& children -> (TFunction, [ta, tr])]))
                        (Just (tag &&& children -> (ELambda x, [body])))) = do
    returnType <- genCInferredType tr
    (argumentType, template) <- case tag ta of
                      TDeclaredVar t -> return (R.Named (R.Name t), Just t)
                      _ -> genCType ta >>= \cta -> return (cta, Nothing)

    let templatize = if isJust template then R.TemplateDefn [(fromJust template, Nothing)] else id

    addForward $ (if isJust template then R.TemplateDecl [(fromJust template, Nothing)] else id)
                 (R.FunctionDecl (R.Name i) [argumentType] returnType)

    body' <- reify RReturn body
    return [templatize $ R.FunctionDefn (R.Name i) [(x, argumentType)] (Just returnType) [] body']

-- Global scalars.
declaration (tag -> DGlobal i t me) = do
    globalType <- genCType t
    globalInit <- maybe (return []) (reify $ RName i) me

    addInitialization globalInit
    when (tag t == TCollection) $ addComposite (namedTAnnotations $ annotations t)

    return [R.GlobalDefn $ R.Forward $ R.ScalarDecl (R.Name i) globalType Nothing]

-- Triggers are implementationally identical to functions returning unit, except they also generate
-- dispatch wrappers.
declaration (tag -> DTrigger i t e) = declaration (D.global i (T.function t T.unit) (Just e))
declaration (tag -> DRole _) = throwE $ CPPGenE "Roles below top-level are deprecated."
declaration _ = return []

--declaration (tag -> DGlobal i _ _) | "register" `L.isPrefixOf` i = return []

-- declaration (tag &&& children -> (DRole _, cs)) = do
--     subDecls <- vsep . punctuate line <$> mapM declaration cs
--     currentS <- get
--     i <- genCType T.unit >>= \ctu ->
--         return $ ctu <+> text "initGlobalDecls" <> parens empty <+> hangBrace (initializations currentS <> text "return unit_t();")
--     let amp = annotationMap currentS
--     compositeDecls <- forM (S.toList $ S.filter (not . S.null) $ composites currentS) $ \(S.toList -> als) ->
--         composite (annotationComboId als) [(a, M.findWithDefault [] a amp) | a <- als]
--     recordDecls <- forM (M.toList $ recordMap currentS) $
--                      \(_, unzip -> (ids, _)) -> record ids
--     tablePop <- generateDispatchPopulation

--     newS <- get

--     return $ vsep $ punctuate line $
--                [text "using K3::Collection;"]
--             ++ forwards newS
--             ++ compositeDecls
--             ++ recordDecls
--             ++ [subDecls, i, tablePop]

-- declaration (tag -> DDataAnnotation i _ _ amds) = addAnnotation i amds >> return empty
-- declaration _ = return empty


-- -- Generated Builtins
-- -- Interface for source builtins.
-- -- Map special builtin suffix to a function that will generate the builtin.
source_builtin_map :: [(String, (String -> K3 Type -> String -> CPPGenM R.Definition))]
source_builtin_map = [("HasRead", genHasRead),
                      ("Read", genDoRead),
                      ("Loader",genLoader)]

source_builtins :: [String]
source_builtins = map fst source_builtin_map

stripSuffix :: String -> String -> String
stripSuffix suffix name = maybe (error "not a suffix!") reverse $ L.stripPrefix (reverse suffix) (reverse name)

genSourceBuiltin :: K3 Type -> Identifier -> CPPGenM R.Definition
genSourceBuiltin typ name = do
    suffix <- return $ head $ filter (\y -> y `L.isSuffixOf` name) source_builtins
    f <- return $ getSourceBuiltin suffix
    f typ name

-- Grab the generator function from the map, currying the key of the builtin to be generated.
getSourceBuiltin :: String -> K3 Type -> String -> CPPGenM R.Definition
getSourceBuiltin k =
    case filter (\(x,_) -> k == x) source_builtin_map of
        []         -> error $ "Could not find builtin with name" ++ k
        ((_,f):_) -> f k

genHasRead :: String -> K3 Type -> String -> CPPGenM R.Definition
genHasRead suf _ name = do
    let source_name = stripSuffix suf name
    let e_has_r = R.Project (R.Variable $ R.Name "engine") (R.Name "hasRead")
    let body = R.Return $ R.Call e_has_r [R.Literal $ R.LString source_name]
    return $ R.FunctionDefn (R.Name $ source_name ++ suf) [("_", R.Named $ R.Name "unit_t")] (Just $ R.Primitive R.PBool) [] [body]

genDoRead :: String -> K3 Type -> String -> CPPGenM R.Definition
genDoRead suf typ name = do
    ret_type    <- genCType $ last $ children typ
    let source_name =  stripSuffix suf name
    let result_dec  = R.Forward $ R.ScalarDecl (R.Name "result") ret_type Nothing
    let read_result = R.Dereference $ R.Call (R.Project (R.Variable $ R.Name "engine") (R.Name "doReadExternal")) []
    let do_patch    = R.Ignore $ R.Call (R.Variable $ R.Name "do_patch") [read_result, R.Variable $ R.Name "result"]
    return $ R.FunctionDefn (R.Name $ source_name ++ suf) [("_", R.Named $ R.Name "unit_t")] (Just ret_type) [] [result_dec, do_patch, R.Return $ R.Variable $ R.Name "result"]

-- TODO: Loader is not quite valid K3. The collection should be passed by indirection so we are not working with a copy
-- (since the collection is technically passed-by-value)
genLoader :: String -> K3 Type -> String -> CPPGenM R.Definition
genLoader suf (children -> [_,f]) name = do
 (colType, recType) <- return $ getColType f
 cColType <- genCType colType
 cRecType <- genCType recType
 fields   <- getRecFields recType
 let coll_name = stripSuffix suf name
 let result_dec = R.Forward $ R.ScalarDecl (R.Name "rec") cRecType Nothing
 let projs = [R.Project (R.Variable $ R.Name "rec") (R.Name i) | i <- fields]
 let parse = R.Call
               (R.Variable $ R.Qualified (R.Name "strtk" ) (R.Name "parse"))
               ( [R.Variable $ R.Name "str", R.Literal $ R.LString ","] ++ projs)
 let insert = R.Call (R.Project (R.Variable $ R.Name "c") (R.Name "insert")) [R.Variable $ R.Name "rec"]
 let err    = R.Binary "<<" (R.Variable $ R.Qualified (R.Name "std") (R.Name "cout")) (R.Literal $ R.LString "Failed to parse a row!\\n")
 let ite = R.IfThenElse parse [R.Ignore insert] [R.Ignore err]

 let lamb = R.Lambda [R.RefCapture (Just ("rec", Nothing)), R.RefCapture (Just ("c", Nothing))] [("str", R.Const $ R.Reference $ R.Named $ R.Qualified (R.Name "std") (R.Name "string"))] Nothing [ite]
 let foreachline = R.Call (R.Variable $ R.Qualified (R.Name "strtk") (R.Name "for_each_line")) [R.Variable $ R.Name "file", lamb]
 let ret = R.Return $ R.Initialization (R.Named $ R.Name "unit_t") []
 return $ R.FunctionDefn (R.Name $ coll_name ++ suf) [("file", R.Named $ R.Name "string"),("c", R.Reference cColType)]
            (Just $ R.Named $ R.Name "unit_t")
            [] [result_dec, R.Ignore foreachline, ret]
 where
    getColType = case children f of
                  ([c,_])  -> case children c of
                                [r] -> return (c, r)
                                _   -> type_mismatch
                  _        -> type_mismatch

    getRecFields (tag -> TRecord ids)  = return ids
    getRecFields _ = error "Cannot get fields for non-record type"

    type_mismatch = error "Invalid type for Loader function. Should Be String -> Collection R -> ()"

genLoader _ _ _ =  error "Invalid type for Loader function."

