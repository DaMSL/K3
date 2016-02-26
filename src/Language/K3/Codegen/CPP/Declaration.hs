{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ParallelListComp #-}

module Language.K3.Codegen.CPP.Declaration where

import Control.Arrow ((&&&))
import Control.Applicative
import Control.Monad.State

import Data.Maybe

import qualified Data.List as L
import qualified Data.Map as M

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Codegen.CPP.Expression
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import Language.K3.Codegen.CPP.Materialization.Hints

import qualified Language.K3.Codegen.CPP.Representation as R

import Language.K3.Utils.Pretty

-- Builtin names to explicitly skip.
skip_builtins :: [String]
skip_builtins = ["hasRead", "doRead", "doBlockRead", "hasWrite", "doWrite"]

declaration :: K3 Declaration -> CPPGenM [R.Definition]
declaration (tna -> ((DGlobal n (tnc -> (TSource, [t])) _), as)) = return []

-- Sinks with a valid body are handled in the same way as triggers.
declaration d@(tna -> (DGlobal i (tnc -> (TSink, [t])) (Just e), as)) = do
  declaration $ D.global i (T.function t T.unit) $ Just e

declaration (tag -> DGlobal i (tag -> TSink) Nothing) =
  throwE $ CPPGenE $ unwords ["Invalid sink trigger", i, "(missing body)"]

-- Global functions without implementations -- Built-Ins.
declaration (tag -> DGlobal name t@(tag -> TFunction) Nothing)
  | name `elem` skip_builtins = return []
  | any (`L.isSuffixOf` name) source_builtins = genSourceBuiltin t name >>= return . replicate 1
  | otherwise = return []

-- Global polymorphic functions without implementations -- Built-Ins
declaration (tag -> DGlobal _ (tag &&& children -> (TForall _, [tag &&& children -> (TFunction, [_, _])]))
                        Nothing) = return []

-- Global monomorphic function with direct implementations.
declaration (tag -> DGlobal i t@(tag &&& children -> (TFunction, [ta, tr]))
                      (Just e@(tag &&& children -> (ELambda x, [body]))))
  | i == "processRole" = do
      ([], R.Lambda _ mits _ _ body) <- inline e
      return $ [R.FunctionDefn (R.Name i) [(Nothing, R.Reference $ R.Const $ R.Unit)] (Just R.Unit) [] True body]
  | otherwise = do
      ([], e') <- inline e
      ct <- R.flattenFnType <$> genCType t
      return [R.GlobalDefn $ R.Forward $ R.ScalarDecl (R.Name i) ct $ Just e']

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

    -- mtrlzns <- case e @~ isEMaterialization of
    --              Just (EMaterialization ms) -> return ms
    --              Nothing -> return $ M.fromList [(x, defaultDecision)]

    let argMtrlznType = case getInMethodFor x e of
                          ConstReferenced -> R.Reference (R.Const argumentType)
                          Referenced -> R.Reference argumentType
                          Forwarded -> R.RValueReference argumentType
                          _ -> argumentType

    body' <- reify (RReturn False) body
    return [templatize $ R.FunctionDefn (R.Name i) [(Just x, argMtrlznType)] (Just returnType) [] False body']

-- Global scalars.
declaration d@(tag -> DGlobal i t me) = do
    globalType <- genCType t
    let pinned = isJust $ d @~ (\case { DProperty (dPropertyV -> ("Pinned", Nothing)) -> True; _ -> False })
    let globalType' = if pinned then R.Static globalType else globalType

    -- Need to declare static members outside of class scope
    let staticGlobalDecl = [R.Forward $ R.ScalarDecl
                             (R.Qualified (R.Name "__global_context") (R.Name i))
                             globalType
                             Nothing
                           | pinned]

    addStaticDeclaration staticGlobalDecl

    -- Initialize the variable.
    let rName = RName (R.Variable $ R.Name $ if pinned then "__global_context::" ++ i else i) Nothing
    globalInit <- maybe (return []) (liftM (addSetCheck pinned i) . reify rName) me

    -- Add to proper initialization list
    let addFn = if pinned then addStaticInitialization else addGlobalInitialization
    addFn globalInit

    -- Add any annotation to the state
    when (tag t == TCollection) $ addComposite (namedTAnnotations $ annotations t) (head $ children t)

    -- Return the class-scope-declaration including the set variable if needed
    let setOp = if False then [] else
                  [R.GlobalDefn $ R.Forward $ R.ScalarDecl
                    (R.Name $ setName i) (R.Primitive R.PBool) (Just $ R.Literal $ R.LBool False)]

    return $ (R.GlobalDefn $ R.Forward $ R.ScalarDecl (R.Name i) globalType' Nothing):setOp
      where
        setName n = "__"++n++"_set__"
        addSetCheck pinned n f = if pinned then f else
          [R.IfThenElse
            (R.Unary "!" $ R.Variable $ R.Name $ setName n)
            (f ++ [R.Assignment (R.Variable $ R.Name $ setName n) (R.Literal $ R.LBool True)])
            []]


-- Triggers are implementationally identical to functions returning unit, except they also generate
-- dispatch wrappers.
declaration (tag -> DTrigger i t e) = declaration (D.global i (T.function t T.unit) (Just e))
declaration (tag -> DDataAnnotation i _ amds) = addAnnotation i amds >> return []
declaration (tag -> DRole _) = throwE $ CPPGenE "Roles below top-level are deprecated."
declaration _ = return []

-- Generated Builtins
-- Interface for source builtins.
-- Map special builtin suffix to a function that will generate the builtin.
-- These suffixes are taken from L.K3.Parser.ProgramBuilder.hs
source_builtin_map :: [(String, (String -> K3 Type -> String -> CPPGenM R.Definition))]
source_builtin_map = [("MuxHasRead",   genHasRead True     Nothing),
                      ("MuxRead",      genDoRead  True     Nothing),
                      ("PDataHasRead", genHasRead True     Nothing),
                      ("PDataRead",    genDoRead  True     Nothing),
                      ("POrdHasRead",  genHasRead False  $ Just "order"),
                      ("POrdRead",     genDoRead  False  $ Just "order"),
                      ("HasRead",      genHasRead False    Nothing),
                      ("Read",         genDoRead  False    Nothing),
                      ("HasWrite",     genHasWrite),
                      ("Write",        genDoWrite)]
                     ++ extraSuffixes

        -- These suffixes are for data loading hacks.
	-- TODO this needs refactoring, big time
  where extraSuffixes = [("Loader",       genLoader False False False False False False ","),
                         ("LoaderC",      genLoader False False True  False False False ","),
                         ("LoaderF",      genLoader False True  False False False False ","),
                         ("LoaderFC",     genLoader False True  True  False False False ","),
                         ("LoaderE",      genLoader True  False False False False False ","),
                         ("LoaderP",      genLoader False False False False False False "|"),
                         ("LoaderPC",     genLoader False False True  False False False "|"),
                         ("LoaderPF",     genLoader False True  False False False False "|"),
                         ("LoaderPFC",    genLoader False True  True  False False False "|"),
                         ("LoaderRP",     genLoader False False False True  False False "|"),
                         ("LoaderPE",     genLoader True  False False False False False "|"),
                         ("LoaderMPC",    genLoader False False True  False True  False "|"),
                         ("LoaderMosaic", genLoader False False False False False True  "|"),
                         ("Logger",    genLogger)]

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

genHasRead :: Bool -> Maybe String -> String -> K3 Type -> String -> CPPGenM R.Definition
genHasRead asMux chanIdSuffixOpt suf _ name = do
    concatOp        <- binarySymbol OConcat
    let source_name = stripSuffix suf name
    let e_has_r     = R.Variable $ R.Name "hasRead"
    let source_pfx  = source_name ++ if asMux || isJust chanIdSuffixOpt then "_" else ""

    let chan_id_e   = case (chanIdSuffixOpt, asMux) of
                        (Just chan_suf, _) -> R.Literal $ R.LString $ source_pfx ++ chan_suf

                        (Nothing, True) -> R.Binary concatOp (R.Literal $ R.LString source_pfx) $
                                             R.Call (R.Variable $ R.Name "itos")
                                               [R.Variable $ R.Name "muxid"]

                        (_, _) -> R.Literal $ R.LString source_pfx

    let body = R.Return $ R.Call e_has_r [R.Variable $ R.Name "me", chan_id_e]
    let args = if asMux then [(Just "muxid", R.Primitive R.PInt)]
                        else [(Just "_", R.Named $ R.Name "unit_t")]

    return $ R.FunctionDefn (R.Name name) args (Just $ R.Primitive R.PBool) [] False [body]

genDoRead :: Bool -> Maybe String -> String -> K3 Type -> String -> CPPGenM R.Definition
genDoRead asMux chanIdSuffixOpt suf typ name = do
    concatOp <- binarySymbol OConcat
    ret_type <- genCType $ last $ children typ
    let source_name = stripSuffix suf name
    let source_pfx  = source_name ++ if asMux || isJust chanIdSuffixOpt then "_" else ""

    let chan_id_e   = case (chanIdSuffixOpt, asMux) of
                        (Just chan_suf, _) -> R.Literal $ R.LString $ source_pfx ++ chan_suf

                        (Nothing, True) -> R.Binary concatOp (R.Literal $ R.LString source_pfx) $
                                             R.Call (R.Variable $ R.Name "itos")
                                               [R.Variable $ R.Name "muxid"]

                        (_, _) -> R.Literal $ R.LString source_pfx

    let body = R.Return $ (R.Call (R.Variable (R.Specialized [ret_type] $ R.Name "doRead"))
                            [R.Variable $ R.Name "me", chan_id_e])

    let args = if asMux then [(Just "muxid", R.Primitive R.PInt)]
                        else [(Just "_", R.Named $ R.Name "unit_t")]

    return $ R.FunctionDefn (R.Name name) args (Just ret_type) [] False [body]

genHasWrite :: String -> K3 Type -> String -> CPPGenM R.Definition
genHasWrite suf _ name = do
    let sink_name = stripSuffix suf name
    let e_has_w = R.Variable (R.Name "hasWrite")
    let body = R.Return $ R.Call e_has_w [R.Variable $ R.Name "me", R.Literal $ R.LString sink_name]
    return $ R.FunctionDefn (R.Name $ sink_name ++ suf) [(Just "_", R.Named $ R.Name "unit_t")]
      (Just $ R.Primitive R.PBool) [] False [body]

genDoWrite :: String -> K3 Type -> String -> CPPGenM R.Definition
genDoWrite suf typ name = do
    val_type    <- genCType $ head $ children typ
    let sink_name =  stripSuffix suf name
    let write_expr = R.Call (R.Variable $ (R.Specialized [val_type] $ R.Name "doWrite"))
                            [R.Variable $ R.Name "me", R.Literal $ R.LString sink_name, R.Variable $ R.Name "v"]
    return $ R.FunctionDefn (R.Name $ sink_name ++ suf) [(Just "v", R.Reference $ R.Const val_type)]
      (Just $ R.Named $ R.Name "unit_t") [] False
      ([R.Ignore write_expr, R.Return $ R.Initialization R.Unit []])

-- TODO: Loader is not quite valid K3. The collection should be passed by indirection so we are not working with a copy
-- (since the collection is technically passed-by-value)
genLoader :: Bool -> Bool -> Bool -> Bool -> Bool -> Bool -> String -> String -> K3 Type -> String -> CPPGenM R.Definition
genLoader elemWrap fixedSize projectedLoader asReturn addMeta addMultiplicity sep suf ft@(children -> [_,f]) name = do
 void (genCType ft) -- Force full type to generate potential record/collection variants.
 (colType, recType, fullRecTypeOpt) <- return $ getColType f
 cColType      <- genCType colType
 cRecType      <- genCType recType
 cfRecType     <- maybe (return Nothing) (\t -> genCType t >>= return . Just) fullRecTypeOpt
 fields        <- getRecFields recType
 fullFieldsOpt <- maybe (return Nothing) (\frt -> getRecFields frt >>= return . Just) fullRecTypeOpt
 br <- gets (boxRecords . flags)
 let bw = if br then R.Dereference else id
 let coll_name = stripSuffix suf name
 let bufferDecl = [R.Forward $ R.ScalarDecl (R.Name "tmp_buffer")
                        (R.Named $ R.Qualified (R.Name "std") (R.Name "string")) Nothing]

 let readField f t skip b = [ R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "getline")) $
                                     [ R.Variable (R.Name "in")
                                     , R.Variable (R.Name "tmp_buffer")
                                     ] ++ [R.Literal (R.LChar sep) | not b]
                            ] ++
                            (if skip then []
                             else [ R.Assignment (R.Project (bw $ R.Variable $ R.Name "record") (R.Name f))
                                           (typeMap t $ R.Variable $ R.Name "tmp_buffer")
                                  ])

 let recordDecl = [R.Forward $ R.ScalarDecl (R.Name "record") cRecType Nothing]

 let fts = uncurry zip fields
 let fullfts = fullFieldsOpt >>= return . uncurry zip

 let ftsWSkip = maybe (map (\(x,y) -> (x, y, False)) fts)
                      (map (\(x,y) -> (x, y, (x /= "meta") && (x `notElem` (map fst fts)))))
                      fullfts

 let containerDecl = R.Forward $ R.ScalarDecl (R.Name "c2") cColType Nothing
 let container = R.Variable $ R.Name (if asReturn then "c2" else "c")

 let setMeta = R.Assignment (R.Project (bw $ R.Variable $ R.Name "record") (R.Name "meta")) (R.Call (R.Variable $ R.Name "mf") [R.Initialization R.Unit [] ])
 let setMult = R.Assignment (R.Project (R.Variable $ R.Name "record") (R.Name . last . fst $ fields)) (R.Literal $ R.LInt 1)

 let ftsToRead = if addMultiplicity then init ftsWSkip else ftsWSkip
 let recordGetLines = recordDecl
                      ++ concat [readField field ft skip False | (field, ft, skip)  <- init ftsToRead]
                      ++ (\(a,b,c) -> readField a b c True) (last ftsToRead)
		      ++ (if addMeta then [setMeta] else [])
		      ++ (if addMultiplicity then [setMult] else [])
                      ++ [R.Return $ R.Variable $ R.Name "record"]
 let extraCaptures = if addMeta then [R.RefCapture (Just ("mf", Nothing))] else []

 let readRecordFn = R.Lambda ([R.ThisCapture] ++ extraCaptures)
                    [ (Just "in", (R.Reference $ R.Named $ R.Qualified (R.Name "std") (R.Name "istream")))
                    , (Just "tmp_buffer", (R.Reference $ R.Named $ (R.Qualified (R.Name "std") (R.Name "string"))))
                    ] False Nothing recordGetLines

 let rrm = if br then (++ "_boxed") else id

 let readRecordsCall = if asReturn
                       then R.Call (R.Variable $ R.Qualified (R.Name "K3") $ R.Name $ rrm "read_records_into_container")
                              [ R.Variable $ R.Name "paths"
                              , container
                              , readRecordFn ]
                       else
                        (if fixedSize
                         then R.Call (R.Variable $ R.Qualified (R.Name "K3") $ R.Name "read_records_with_resize")
                                [ R.Variable $ R.Name "size"
                                , R.Variable $ R.Name "paths"
                                , container
                                , readRecordFn
                                ]
                         else R.Call (R.Variable $ R.Qualified (R.Name "K3") $ R.Name $ rrm "read_records")
                                [ R.Variable $ R.Name "paths"
                                , container
                                , readRecordFn
                                ])

 pathsType <- genCType ((T.collection $ T.record [("path", T.string)]) @+ TAnnotation "Collection")
 let defaultArgs = [(Just "paths", pathsType)]

 let args = defaultArgs
              ++ [(Just "c", R.Reference $ (if asReturn then R.Const else id) cColType)]
              ++ (if projectedLoader then [(Just "_rec", R.Reference $ fromJust cfRecType)] else [])
              ++ (if fixedSize       then [(Just "size", R.Primitive R.PInt)] else [])
	      ++ (if addMeta         then [(Just "mf", R.Named $ R.Name "MetaFn")] else [])

 let returnType   = if asReturn then cColType else R.Named $ R.Name "unit_t"
 let functionBody = if asReturn
                      then [ containerDecl, R.Return $ readRecordsCall ]
                      else [ R.Ignore $ readRecordsCall, R.Return $ R.Initialization R.Unit [] ]

 let defaultDefn = R.FunctionDefn (R.Name $ coll_name ++ suf) args (Just $ returnType) [] False functionBody
 return $ if addMeta then R.TemplateDefn [("MetaFn", Nothing)] defaultDefn else defaultDefn

 where
   typeMap :: K3 Type -> R.Expression -> R.Expression
   typeMap (tag &&& (@~ isTDateInt) -> (TInt, Just _)) e =
     R.Call (R.Variable $ R.Name "tpch_date") [R.Call (R.Project e (R.Name "c_str")) []]

   typeMap (tag -> TInt) e = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "atoi"))
                             [R.Call (R.Project e (R.Name "c_str")) []]
   typeMap (tag -> TReal) e = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "atof"))
                              [R.Call (R.Project e (R.Name "c_str")) []]
   typeMap (tag -> _) x = x

   isTDateInt :: Annotation Type -> Bool
   isTDateInt (TProperty (tPropertyName -> "TPCHDate")) = True
   isTDateInt _ = False

   getColType = case fnArgs [] f of
                  [c, fr, sz] | projectedLoader && fixedSize -> colRecOfType c >>= \(x,y) -> return (x, y, Just fr)
                  [c, fr]     | projectedLoader              -> colRecOfType c >>= \(x,y) -> return (x, y, Just fr)
                  [c, fr, f]  | projectedLoader              -> colRecOfType c >>= \(x,y) -> return (x, y, Just fr)
                  [c, _]      | fixedSize                    -> colRecOfType c >>= \(x,y) -> return (x, y, Nothing)
                  [c]                                        -> colRecOfType c >>= \(x,y) -> return (x, y, Nothing)
                  _                                          -> type_mismatch

   fnArgs acc t@(tnc -> (TFunction, [a,r])) = fnArgs (acc++[a]) r
   fnArgs acc _ = acc

   colRecOfType c@(tnc -> (TCollection, [r])) | elemWrap  = return (c, removeElem r)
   colRecOfType c@(tnc -> (TCollection, [r])) | otherwise = return (c, r)
   colRecOfType _ = type_mismatch

   removeElem (tag &&& children -> (TRecord [_], [inner])) = inner
   removeElem _ = error "Invalid record structure for elemLoader"

   getRecFields (tag &&& children -> (TRecord ids, cs))  = return (ids, cs)
   getRecFields _ = error "Cannot get fields for non-record type"

   type_mismatch = error "Invalid type for Loader function. Should Be String -> Collection R -> ()"

genLoader _ _ _ _ _ _ _ _ _ _ =  error "Invalid type for Loader function."

genLogger :: String -> K3 Type -> String -> CPPGenM R.Definition
genLogger _ (children -> [_,f]) name = do
  (colType, recType) <- getColType
  let (fields,_) = getRecFields recType
  let fieldLogs = map (log . proj) fields
  let allLogs = L.intersperse (seperate $ R.Variable $ R.Name "sep") fieldLogs
  cRecType <- genCType recType
  cColType <- genCType colType
  let printRecordFn = R.Lambda []
                    [ (Just "file", R.Reference $ R.Named $ R.Qualified (R.Name "std") (R.Name "ofstream"))
                    , (Just "elem", R.Reference $ R.Const cRecType)
                    , (Just "sep", R.Reference $ R.Const $ R.Named $ R.Name "string")
                    ] False Nothing (map R.Ignore allLogs)

  return $ R.FunctionDefn (R.Name name)
             [ (Just "file", R.Named $ R.Name "string")
             , (Just "c", R.Reference cColType)
             , (Just "sep", R.Reference $ R.Const $ R.Named $ R.Name "string")]
             (Just $ R.Named $ R.Name "unit_t") [] False
             [ R.Return $ R.Call (R.Variable $ R.Name "logHelper")
                            [ R.Variable $ R.Name "file"
                            , R.Variable $ R.Name "c"
                            , printRecordFn
                            , R.Variable $ R.Name "sep"
                            ]
             ]

  where
   proj i = R.Project (R.Variable $ R.Name "elem") (R.Name i)
   log = R.Binary "<<" (R.Variable $ R.Name "file")
   seperate s = log s
   getColType = case children f of
                  ([c,_])  -> case children c of
                                [r] -> return (c, r)
                                _   -> type_mismatch
                  _        -> type_mismatch

   getRecFields (tag &&& children -> (TRecord ids, cs))  = (ids, cs)
   getRecFields _ = error "Cannot get fields for non-record type"

   type_mismatch = error "Invalid type for Logger function. Must be a flat-record of ints, reals, and strings"

genLogger _ _ _ = error "Error: Invalid type for Logger function. Must be a flat-record of ints, reals, and strings"


genCsvParser :: K3 Type -> CPPGenM (Maybe R.Expression)
genCsvParser t@(tag &&& children -> (TTuple, ts)) = genCsvParserImpl t ts get >>= (return . Just)
  where
    get exp i = R.Call
               (R.Variable $ R.Qualified (R.Name "std") (R.Specialized [R.Named $ R.Name (show i)] (R.Name "get")))
               [exp]
genCsvParser t@(tag &&& children -> (TRecord ids, ts)) = genCsvParserImpl t ts project >>= (return . Just)
  where
    project exp i = R.Project exp (R.Name (ids L.!! i))
genCsvParser _ = error "Can't generate CsvParser. Only works for flat records and tuples"

genCsvParserImpl :: K3 Type -> [K3 Type] -> (R.Expression -> Int -> R.Expression) -> CPPGenM R.Expression
genCsvParserImpl elemType childTypes accessor = do
  et  <- genCType elemType
  let fields = concatMap (uncurry readField) (zip childTypes [0,1..])
  return $ R.Lambda
               []
               [(Just "str", R.Reference $ R.Const $ R.Named $ R.Qualified (R.Name "std") (R.Name "string"))]
               False
               Nothing
               ( [iss_decl, iss_str, tup_decl et, token_decl] ++ fields ++ [R.Return tup])
  where

   iss_decl = R.Forward $ R.ScalarDecl (R.Name "iss") (R.Named $ R.Qualified (R.Name "std") (R.Name "istringstream")) Nothing
   iss_str  = R.Ignore $ R.Call (R.Project iss (R.Name "str")) [R.Variable $ R.Name "str"]
   token_decl = R.Forward $ R.ScalarDecl (R.Name "token") (R.Named $ R.Qualified (R.Name "std") (R.Name "string")) Nothing
   iss = R.Variable $ R.Name "iss"
   token = R.Variable $ R.Name "token"
   tup_decl et = R.Forward $ R.ScalarDecl (R.Name "tup") et Nothing
   tup = R.Variable $ R.Name "tup"

   readField :: K3 Type -> Int -> [R.Statement]
   readField t i = [ R.Ignore getline
                   , R.Assignment (accessor tup i) (typeMap t cstr)
                   ]

   cstr = R.Call (R.Project token (R.Name "c_str")) []

   getline = R.Call
               (R.Variable $ R.Qualified (R.Name "std") (R.Name "getline"))
               [iss, token, R.Literal $ R.LChar "|"]

   typeMap :: K3 Type -> R.Expression -> R.Expression
   typeMap (tag -> TInt) e = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "atoi"))
                             [e]
   typeMap (tag -> TReal) e = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "atof"))
                              [e]
   typeMap (tag -> TString) e = R.Call (R.Variable $ R.Name "string_impl") [e]
   typeMap (tag -> _) x = x
