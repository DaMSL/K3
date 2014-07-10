{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Declaration where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Functor

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import Data.Tree

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
declaration (tag -> DGlobal i (tag &&& children -> (TForall _, _)) (Just e)) = do
    te@(tag &&& children -> (TFunction, [ta, tr])) <- getKType e
    let tvs = L.nub $ map (\(TDeclaredVar j :@: _) -> j)
                    $ filter (\case { (TDeclaredVar _ :@: _) -> True; _ -> False })
                    $ flatten te
    let td = genCTemplateDecl $ map text tvs
    let (tag &&& children -> (ELambda x, [b])) = e
    newF <- cDecl te i
    addForward $ td <+> newF
    body <- reify RReturn b
    cta <- genCType ta
    ctr <- genCType tr
    return $ td <$$> ctr <+> text i <> parens (cta <+> text x) <+> hangBrace body

declaration (tag -> DGlobal i t (Just e)) = do
    newI <- reify (RName i) e
    modify (\s -> s { initializations = initializations s <//> newI })
    cDecl t i

-- The generated code for a trigger is the same as that of a function with corresponding ()
-- return-type.
declaration (tag -> DTrigger i t e) = 
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
            ++ [(text "#include" <+> (dquotes (text "Builtins.hpp")))]
            ++ [subDecls, i, tablePop]

declaration (tag -> DAnnotation i _ amds) = addAnnotation i amds >> return empty
declaration _ = return empty

-- | Generates a function which populates the trigger dispatch table.
generateDispatchPopulation :: CPPGenM CPPGenR
generateDispatchPopulation = do
    triggerS <- triggers <$> get
    dispatchStatements <- mapM genDispatch triggerS
    let dispatchInit = text "dispatch_table.resize" <> parens(int $ length triggerS) <> semi
    return $ genCFunction Nothing (text "void") (text "populate_dispatch") [] $
             vsep $ dispatchInit:dispatchStatements
  where
    genDispatch (tName, (tType, tNum)) = do
      kType <- genCType tType
      let className = text "DispatcherImpl" <> angles kType
      return $ text "dispatch_table[" <> int tNum <> text "] =" <+>
        text "make_tuple" <> parens (
          text "make_shared" <> angles className <> parens (text tName) <> comma <+> dquotes (text tName))
        <> semi

-- Generated Builtins
-- Interface for source builtins.
-- Map special builtin suffix to a function that will generate the builtin.
source_builtin_map :: [(String, (String -> K3 Type -> String -> CPPGenM CPPGenR))]
source_builtin_map = [("HasRead", genHasRead),
                      ("Read", genDoRead),
                      ("Loader",genLoader),
                      ("LoaderJSON",genJSONLoader),
                      ("LoaderVector", genVectorLoader),
                      ("LoaderVectorLabel", genVectorLabelLoader)]

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
  body        <- return $ text "return engine.hasRead" <> parens (dquotes $ text source_name) <> semi
  forward     <- return $ text "bool" <+> text name <> parens (text "unit_t") <> semi
  addForward forward
  return $ genCFunction Nothing (text "bool") (text name) [text "unit_t"] body

genDoRead :: String -> K3 Type -> String -> CPPGenM CPPGenR
genDoRead suf typ name = do
    ret_type    <- genCType $ last $ children typ
    source_name <- return $ stripSuffix suf name
    res_decl    <- return $ ret_type <+> text "result" <> semi
    doRead      <- return $ text "*engine.doReadExternal" <> parens (dquotes $ text source_name)
    doPatch     <- return $ text "do_patch" <> angles ret_type <> parens (doRead <> comma <> text "result") <> semi
    ret         <- return $ text "return result;"
    body        <- return $ vsep $ [res_decl, doPatch, ret]
    forward     <- return $ ret_type <+> text name <> parens (text "unit_t") <> semi
    addForward forward
    return $ genCFunction Nothing ret_type (text name) [text "unit_t"] body

genLoader :: String -> K3 Type -> String -> CPPGenM CPPGenR
genLoader _ (children -> [_,f]) name = do
    rec      <- getRecordType
    rec_type <- genCType rec
    fields   <- getRecFields rec
    proj_str <- return $ text $ concat $ L.intersperse "," $ map (\q -> "rec." ++ q) fields
    c_type   <- return $ text "K3::Collection<" <> rec_type <> text">"
    header1  <- return $ text "F<unit_t(" <> c_type <> text "&)>"<> text name <> text "(string filepath)"
    header2  <- return $ text "F<unit_t(" <> c_type <> text "&)> r = [filepath] (" <> c_type <> text" & c)"
    ifs      <- return $ text "if (strtk::parse(str,\",\"," <> proj_str <> text "))" <> (hangBrace (text "c.insert(rec)" <> semi))
    elses    <- return $ text "else" <> (hangBrace $ text "std::cout << \"Failed to parse a row\" << std::endl;")
    body     <- return $ vsep [rec_type <+> text "rec" <> semi,
                                text "strtk::for_each_line(filepath,",
                                text "[&](const std::string& str)" <> (hangBrace (vsep [ifs,elses]) <> text ");"),
                                text "return unit_t();"]
    return $ header1 <> (hangBrace $ (header2 <> vsep [hangBrace body <> semi, text "return r;"]))
  where
    getRecordType = case children f of
                     ([c,_])  -> (case children c of
                                   [r] -> return r
                                   _  -> type_mismatch)
                     _        ->  type_mismatch

    getRecFields r = case tag r of
                     (TRecord ids) -> return ids
                     _             -> type_mismatch

    type_mismatch = error "Invalid type for Loader function. Should Be String -> Collection R -> ()"


genLoader _ _ _ =  error "Invalid type for Loader function."

-- Generate a JSON Loader builtin for a collection with a specified type
genJSONLoader :: String -> K3 Type -> String -> CPPGenM CPPGenR
genJSONLoader _ (children -> [_,f]) name = do
    rec      <- getRecordType
    rec_type <- genCType rec
    c_type   <- return $ text "K3::Collection<" <> rec_type <> text">"
    -- Function definition
    header1  <- return $ text "F<unit_t(" <> c_type <> text "&)>"<> text name <> text "(string filepath)"
    header2  <- return $ text "F<unit_t(" <> c_type <> text "&)> r = [filepath] (" <> c_type <> text" & c)"
    inits    <- return $ vsep [text "using namespace json_spirit;",
                               text "using json_spirit::Value;",
                               text "std::string line;",
                               text "std::ifstream infile(filepath);"]
    loader   <- parseJSON "val" rec
    loop     <- return $ vsep [text "std::istringstream iss(line);",
                               text "Value val;",
                               text "read_stream ( iss, val );",
                               text "c.insert" <> parens (loader) <> semi]
    body     <- return $ vsep [inits,
                               text "while (std::getline(infile, line))" <> hangBrace (loop),
                               text "return unit_t();"]

    return $ header1 <> (hangBrace $ (header2 <> vsep [hangBrace body <> semi, text "return r;"]))
  where
    getRecordType = case children f of
                     ([c,_])  -> (case children c of
                                   [r] -> return r
                                   _   -> type_mismatch)
                     _        ->  type_mismatch

    type_mismatch = error "Invalid type for JSON Loader function. Should Be String -> Collection R -> ()"

genJSONLoader _ _ _ = error "Invalid type for JSON Loader function. Should Be String -> Collection R -> ()"


-- | Generate CPP code to parse a JSON object into a value of the given K3 Type
parseJSON :: String -> K3 Type -> CPPGenM CPPGenR
parseJSON value base_t =
    case base_t of
      -- non-nested:
      (tag -> TBool)     -> return $ text value <> text ".get_bool()"
      (tag -> TInt)      -> return $ text value <> text ".get_int()"
      (tag -> TReal)     -> return $ text value <> text ".get_real()"
      (tag -> TString)   -> return $ text value <> text ".get_str()"
      (tag -> TFunction) -> unsupported "Function"
      (tag -> TAddress)  -> unsupported "Address"
      (tag -> TByte)     -> unsupported "Byte"
      -- nested:
      ((tag &&& children) -> (TRecord ids, ts))   -> parse_json_record base_t value ids ts
      ((tag &&& children) -> (TCollection, [rt])) -> parse_json_collection base_t value rt
      ((tag &&& children) -> (TOption, [_]))      -> unsupported "Option"
      ((tag &&& children) -> (TIndirection, [_])) -> unsupported "Indirection"
      ((tag &&& children) -> (TTuple, _))         -> unsupported "Tuple"
      _                                           -> unsupported "Trigger"
  where
    unsupported t = error $ "Unsupported K3 Type for JSON parsing: " ++ t

    rname = text "rec"

    new_val v = v ++ "_"

    parse_json_record rt val ids ts = do
      rtyp <- genCType rt
      rdef <- return $ rtyp <+> rname <> semi
      e    <- return $ text "if" <> parens (text val <> text ".get_obj().size() == 0") <+> text "{std::cout << \"Failed to Parse a Row\" << std::endl;}"
      for  <- return $ text "for(Object::size_type i =0;i!=" <> text val <> text ".get_obj().size();++i)"
      p    <- return $ text "const Pair& pair =" <+> text val <> text ".get_obj()[i];"
      n    <- return $ text "const string&" <+> text (new_val val) <> text "name = pair.name_;"
      v    <- return $ text "const Value&" <+> text (new_val val) <+> text " = pair.value_;"
      cs   <- sequence $ zipWith (get_field val) ids ts
      err  <- return $ text "assert(false);"
      loop <- return $ for <> hangBrace (vsep ([p,n,v]++cs++[err]))
      ret  <- return $ text "return" <+> rname <> semi
      return $ text "[&] ()" <> (hangBrace $ vsep [e,rdef, loop, ret]) <> text "()"

    parse_json_collection ct val rt = do
      ctyp <- genCType ct
      decl <- return $ ctyp <+> text "c = " <> ctyp <> parens empty <> semi
      for  <- return $ text "for(Array::size_type i =0;i!=" <> text val <> text ".get_array().size();++i)"
      v    <- return $ text "const Value&" <+> text (new_val val) <+> text "=" <+> text val <> text ".get_array()[i];"
      c    <- parseJSON (new_val val) rt
      ins  <- return $ text "c.insert" <> parens (c) <> semi
      loop <- return $ for <> hangBrace (vsep [v,ins])
      body <- return $ vsep [decl, loop, text "return c;"]
      return $ text "[&] ()" <> hangBrace body <> text "()"

    get_field val i t = do
      iff <- return $ text "if" <> parens (text (new_val val) <> text "name ==" <+> dquotes (text i))
      c   <- parseJSON (new_val val) t
      bod <- return $ rname <> dot <> text i <> text "=" <+> c <> semi
      return $ iff <> hangBrace (vsep [bod, text "continue;"])

genVectorLoader :: String -> K3 Type -> String -> CPPGenM CPPGenR
genVectorLoader _ (children -> [_,f]) name = do
    rec      <- getRecordType
    rec_type <- genCType rec
    c_type   <- return $ text "K3::Collection<" <> rec_type <> text">"
    -- Function definition
    header1  <- return $ text "F<unit_t(" <> c_type <> text "&)>"<> text name <> text "(string filepath)"
    header2  <- return $ text "F<unit_t(" <> c_type <> text "&)> r = [filepath] (" <> c_type <> text" & c)"
    inits    <- return $ vsep  [text "std::string line;",
                               text "std::ifstream infile(filepath);"]
    loop     <- return $ vsep [ text "char * pch;",
                                text "pch = strtok (&line[0],\",\");",
                                text "_Collection<R_elem<double>> c2 = _Collection<R_elem<double>>();",
                                text "while (pch != NULL)" <> hangBrace (vsep [text "R_elem<double> rec;",
                                                                              text "rec.elem = std::atof(pch);",
                                                                              text "c2.insert(rec);",
                                                                              text "pch = strtok (NULL,\",\");"]),
                                text "R_elem<_Collection<R_elem<double>>> rec2;",
                                text "rec2.elem = c2;",
                                text "c.insert(rec2);"]
    body     <- return $ vsep [inits,
                               text "while (std::getline(infile, line))" <> hangBrace (loop),
                               text "return unit_t();"]

    return $ header1 <> (hangBrace $ (header2 <> vsep [hangBrace body <> semi, text "return r;"]))
  where
    getRecordType = case children f of
                     ([c,_])  -> (case children c of
                                   [r] -> return r
                                   _   -> type_mismatch)
                     _        ->  type_mismatch
    type_mismatch = error "Invalid type for Vector Loader function."
genVectorLoader _ _ _ = error "Invalid type for Vector Loader"

genVectorLabelLoader :: String -> K3 Type -> String -> CPPGenM CPPGenR
genVectorLabelLoader _ (children -> [_,f]) name = do
    rec      <- getRecordType
    rec_type <- genCType rec
    c_type   <- return $ text "K3::Collection<" <> rec_type <> text">"
    -- Function definition
    header1  <- return $ text "F<unit_t(" <> c_type <> text "&)>"<> text name <> text "(string filepath)"
    header2  <- return $ text "F<unit_t(" <> c_type <> text "&)> r = [filepath] (" <> c_type <> text" & c)"
    inits    <- return $ vsep  [text "std::string line;",
                               text "std::ifstream infile(filepath);"]
    loop     <- return $ vsep [ text "char * pch;",
                                text "pch = strtok (&line[0],\",\");",
                                text "_Collection<R_elem<double>> c2 = _Collection<R_elem<double>>();",
                                text "double d;",
                                text "while (pch != NULL)" <> hangBrace (vsep [text "R_elem<double> rec;",
                                                                              text "i++;",
                                                                              text "d = std::atof(pch);",
                                                                               (text "if (i > 1)" <> (hangBrace $ vsep [
                                                                                text "rec.elem = d;",
                                                                                text "c2.insert(rec);"])),
                                                                              text "pch = strtok (NULL,\",\");"]),
                                text "R_elem_label<_Collection<R_elem<double>>, double> rec2;",
                                text "rec2.elem = c2;",
                                text "rec2.label = d;",
                                text "c.insert(rec2);"]
    body     <- return $ vsep [inits,
                               text "while (std::getline(infile, line))" <> hangBrace (loop),
                               text "return unit_t();"]

    return $ header1 <> (hangBrace $ (header2 <> vsep [hangBrace body <> semi, text "return r;"]))
  where
    getRecordType = case children f of
                     ([c,_])  -> (case children c of
                                   [r] -> return r
                                   _   -> type_mismatch)
                     _        ->  type_mismatch
    type_mismatch = error "Invalid type for Vector Loader function."
genVectorLabelLoader _ _ _ = error "Invalid type for Vector Label Loader"

stripSuffix :: String -> String -> String
stripSuffix suffix name = maybe (error "not a suffix!") reverse $ L.stripPrefix (reverse suffix) (reverse name)

genSourceBuiltin :: K3 Type -> Identifier -> CPPGenM CPPGenR
genSourceBuiltin typ name = do
  suffix <- return $ head $ filter (\y -> y `L.isSuffixOf` name) source_builtins
  f <- return $ getSourceBuiltin suffix
  f typ name
