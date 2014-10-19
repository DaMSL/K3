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

import Language.K3.Transform.Hints

declaration :: K3 Declaration -> CPPGenM [R.Definition]
declaration (tag -> DGlobal _ (tag -> TSource) _) = return []

-- Global functions without implementations -- Built-Ins.
declaration (tag -> DGlobal name t@(tag -> TFunction) Nothing) | any (`L.isSuffixOf` name) source_builtins = genSourceBuiltin t name >>= return . replicate 1
                                                               | otherwise = return []

-- Global polymorphic functions without implementations -- Built-Ins
declaration (tag -> DGlobal _ (tag &&& children -> (TForall _, [tag &&& children -> (TFunction, [_, _])]))
                        Nothing) = return []

-- Global monomorphic function with direct implementations.
declaration (tag -> DGlobal i (tag &&& children -> (TFunction, [ta, tr]))
                      (Just e@(tag &&& children -> (ELambda x, [body])))) = do
    cta <- genCType ta
    ctr <- genCType tr

    cbody <- reify RReturn body

    addForward $ R.FunctionDecl (R.Name i) [cta] ctr

    let (EOpt (FuncHint readOnly)) = fromMaybe (EOpt (FuncHint False))
                                     (e @~ \case { EOpt (FuncHint _) -> True; _ -> False})

    -- processRole always gets generated as const ref because that's our built-in signature
    let cta' = if readOnly || i == "processRole" then R.Const (R.Reference cta) else cta

    return [R.FunctionDefn (R.Name i) [(x, cta')] (Just ctr) [] False cbody]

-- Global polymorphic functions with direct implementations.
declaration (tag -> DGlobal i (tag &&& children -> (TForall _, [tag &&& children -> (TFunction, [ta, tr])]))
                      (Just e@(tag &&& children -> (ELambda x, [body])))) = do

    returnType <- genCInferredType tr
    (argumentType, template) <- case tag ta of
                      TDeclaredVar t -> return (R.Named (R.Name t), Just t)
                      _ -> genCType ta >>= \cta -> return (cta, Nothing)

    let templatize = maybe id (\t -> R.TemplateDefn [(t, Nothing)]) template

    addForward $ maybe id (\t -> R.TemplateDecl [(t, Nothing)]) template $
                   R.FunctionDecl (R.Name i) [argumentType] returnType

    let (EOpt (FuncHint readOnly)) = fromMaybe (EOpt (FuncHint False))
                                     (e @~ \case { EOpt (FuncHint _) -> True; _ -> False})

    let argumentType' = if readOnly then R.Const (R.Reference argumentType) else argumentType

    body' <- reify RReturn body
    return [templatize $ R.FunctionDefn (R.Name i) [(x, argumentType')] (Just returnType) [] False body']

-- Global scalars.
declaration d@(tag -> DGlobal i t me) = do
    globalType <- genCType t
    globalInit <- maybe (return []) (reify $ RName i) me

    addInitialization globalInit
    when (tag t == TCollection) $ addComposite (namedTAnnotations $ annotations t)

    let pinned = isJust $ d @~ (\case { DProperty "Pinned" Nothing -> True; _ -> False })
    if pinned then modify (\s -> s { staticGlobals = (i, t) : (staticGlobals s) } ) else return ()

    let globalType' = if pinned then R.Static globalType else globalType

    return [R.GlobalDefn $ R.Forward $ R.ScalarDecl (R.Name i) globalType' Nothing]

-- Triggers are implementationally identical to functions returning unit, except they also generate
-- dispatch wrappers.
declaration (tag -> DTrigger i t e) = declaration (D.global i (T.function t T.unit) (Just e))
declaration (tag -> DDataAnnotation i _ amds) = addAnnotation i amds >> return []
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

-- declaration (tag -> DDataAnnotation i _ amds) = addAnnotation i amds >> return empty
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
    let e_has_r = R.Project (R.Variable $ R.Name "__engine") (R.Name "hasRead")
    let body = R.Return $ R.Call e_has_r [R.Literal $ R.LString source_name]
    return $ R.FunctionDefn (R.Name $ source_name ++ suf) [("_", R.Named $ R.Name "unit_t")]
      (Just $ R.Primitive R.PBool) [] False [body]

genDoRead :: String -> K3 Type -> String -> CPPGenM R.Definition
genDoRead suf typ name = do
    ret_type    <- genCType $ last $ children typ
    let source_name =  stripSuffix suf name
    let result_dec  = R.Forward $ R.ScalarDecl (R.Name "result") ret_type Nothing
    let read_result = R.Dereference $ R.Call (R.Project (R.Variable $ R.Name "__engine") (R.Name "doReadExternal")) [R.Literal $ R.LString source_name]
    let do_patch    = R.Ignore $ R.Call (R.Variable $ R.Name "do_patch") [read_result, R.Variable $ R.Name "result"]
    return $ R.FunctionDefn (R.Name $ source_name ++ suf) [("_", R.Named $ R.Name "unit_t")]
      (Just ret_type) [] False [result_dec, do_patch, R.Return $ R.Variable $ R.Name "result"]

-- TODO: Loader is not quite valid K3. The collection should be passed by indirection so we are not working with a copy
-- (since the collection is technically passed-by-value)
genLoader :: String -> K3 Type -> String -> CPPGenM R.Definition
genLoader suf (children -> [_,f]) name = do
 (colType, recType) <- return $ getColType f
 cColType <- genCType colType
 cRecType <- genCType recType
 fields   <- getRecFields recType
 let coll_name = stripSuffix suf name
 let bufferDecl = [R.Forward $ R.ScalarDecl (R.Name "tmp_buffer")
                        (R.Named $ R.Qualified (R.Name "std") (R.Name "string")) Nothing]

 let readField f t = [ R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "getline"))
                                [ R.Variable (R.Name "in")
                                , R.Variable (R.Name "tmp_buffer")
                                , R.Literal (R.LChar "\\'")
                                ]
                   , R.Assignment (R.Project (R.Variable $ R.Name "record") (R.Name f))
                                  (typeMap t $ R.Variable $ R.Name "tmp_buffer")
                   ]
 let recordDecl = [R.Forward $ R.ScalarDecl (R.Name "record") cRecType Nothing]

 let recordGetLines = bufferDecl ++ recordDecl ++ concat [readField field ft | (field, ft)  <- uncurry zip fields]
                      ++ [R.Return $ R.Variable $ R.Name "record"]

 let readRecordFn = R.Lambda [] [("in", (R.Reference $ R.Named $ R.Qualified (R.Name "std") (R.Name "istream")))]
                    False Nothing recordGetLines

 return $ R.FunctionDefn (R.Name $ coll_name ++ suf)
            [("file", R.Named $ R.Name "string"),("c", R.Reference cColType)]
            (Just $ R.Named $ R.Name "unit_t") [] False
            [ R.Forward $ R.ScalarDecl (R.Name "_in")
                            (R.Named $ R.Qualified (R.Name "std") (R.Name "ifstream")) Nothing
            , R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "_in") (R.Name "open")) [R.Variable $ R.Name "file"]
            , R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "K3") $ R.Name "read_records")
                           [ R.Variable $ R.Name "_in"
                           , R.Variable $ R.Name "c"
                           , readRecordFn
                           ]
            ]
 where
   typeMap :: K3 Type -> R.Expression -> R.Expression
   typeMap (tag -> TInt) e = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "atoi"))
                             [R.Call (R.Project e (R.Name "c_str")) []]
   typeMap (tag -> TReal) e = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "atof"))
                              [R.Call (R.Project e (R.Name "c_str")) []]
   typeMap (tag -> _) x = x

   getColType = case children f of
                  ([c,_])  -> case children c of
                                [r] -> return (c, r)
                                _   -> type_mismatch
                  _        -> type_mismatch

   getRecFields (tag &&& children -> (TRecord ids, cs))  = return (ids, cs)
   getRecFields _ = error "Cannot get fields for non-record type"

   type_mismatch = error "Invalid type for Loader function. Should Be String -> Collection R -> ()"

genLoader _ _ _ =  error "Invalid type for Loader function."

