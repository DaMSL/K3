{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
module Language.K3.Codegen.CPP.Program where

import Control.Arrow ((&&&))
import Control.Monad.State

import qualified Data.List as L
import Data.Functor

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type

import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Declaration
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

-- Top-level program generation.
--  - Process the __main role.
--  - Generate include directives.
--  - Generate namespace use directives.
program :: K3 Declaration -> CPPGenM CPPGenR
program d = do
    s <- showGlobals
    staticGlobals' <- staticGlobals
    program' <- declaration d
    genNamespaces <- namespaces >>= \ns -> return [text "using" <+> text n <> semi | n <- ns]
    genIncludes <- includes >>= \is -> return [text "#include" <+> dquotes (text f) | f <- is]
    main <- genKMain
    return $ vsep $ punctuate line [
            vsep genIncludes,
            vsep genNamespaces,
            vsep genAliases,
            staticGlobals',
            program',
            s,
            main
        ]
  where
    genAliases = [text "using" <+> text new <+> equals <+> text old <> semi | (new, old) <- aliases]

matchersDecl :: CPPGenM CPPGenR
matchersDecl = do
    let mapDecl = genCDecl
                   (text "map" <> angles (cat $ punctuate comma [text "string", text "std::function<void(string)>"]))
                   (text "matchers")
                   Nothing
    mapInit <- map populateMatchers . patchables <$> get
    return $ vsep $ mapDecl : mapInit
  where
    populateMatchers :: Identifier -> CPPGenR
    populateMatchers f = text "matchers"
                            <> brackets (dquotes $ text f)
                            <+> equals
                            <+> brackets empty
                            <+> parens (text "string _s")
                            <+> braces (genCCall (text "do_patch") Nothing [text "_s", text f] <> semi)
                            <> semi

genKMain :: CPPGenM CPPGenR
genKMain = do
    matchers <- matchersDecl
    return $ genCFunction Nothing (text "int") (text "main") [text "int argc", text "char** argv"] $ vsep [
            genCDecl (text "Options") (text "opt") Nothing,
            genCCall (text "opt.parse") Nothing [text "argc", text "argv"] <> semi,
            genCCall (text "populate_dispatch") Nothing [] <> semi,
            matchers,
            genCDecl (text "string") (text "parse_arg") (Just $ text "opt.peer_strings[0]") <> semi,
            genCDecl (text "map" <> angles (cat $ punctuate comma [text "string", text "string"]))
              (text "bindings") (Just $ genCCall (text "parse_bindings") Nothing
              [text "parse_arg"]),
            genCCall (text "match_patchers") Nothing [text "bindings", text "matchers"] <> semi,
            genCCall (text "processRole") Nothing [text "unit_t()"] <> semi,
            genCDecl (text "DispatchMessageProcessor") (text "dmp") (Just $
              genCCall (text "DispatchMessageProcessor") Nothing 
                [text "dispatch_table", text showGlobalsName]) <> semi,
            genCCall (text "engine.configure") Nothing
              [text "opt.simulation", text "se", 
               text "make_shared<DefaultInternalCodec>(DefaultInternalCodec())"] <> semi,
            text "engine.runEngine(make_shared<DispatchMessageProcessor>(dmp))" <> semi,
            text "return 0" <> semi
        ]

includes :: CPPGenM [Identifier]
includes = return [
        -- Standard Library
        "memory",
        "sstream",
        "string",

        -- Boost
        "boost/archive/text_iarchive.hpp",

        -- K3 Runtime
        "Collections.hpp",
        "Common.hpp",
        "Dispatch.hpp",
        "Engine.hpp",
        "Literals.hpp",
        "MessageProcessor.hpp",
        "Serialization.hpp"
    ]

namespaces :: CPPGenM [Identifier]
namespaces = do
    serializationNamespace <- serializationMethod <$> get >>= \case
        BoostSerialization -> return "namespace K3::BoostSerializer"
    return [
            "namespace std",
            "namespace K3",
            serializationNamespace,
            "std::begin", "std::end"
        ]

aliases :: [(Identifier, Identifier)]
aliases = [
        ("unit_t", "struct {}")
    ]

staticGlobals :: CPPGenM CPPGenR
staticGlobals = return $ vsep [
            text "SystemEnvironment se = defaultEnvironment()" <> semi,
            text "Engine engine = Engine()" <> semi
        ]

-- | Generate a function to help print the current environment (global vars and their values).
-- Currently, this function returns a map from string (variable name) to string (string representation of value)
showGlobalsName :: String
showGlobalsName = "show_globals"

showGlobals :: CPPGenM CPPGenR
showGlobals = do
    currentS <- get
    body    <- gen_body $ showables currentS
    return $ genCFunction Nothing result_type fun_name [] body
  where
    result_type  = text "map<string,string>"
    fun_name     = text showGlobalsName
    result_var   = text "result"

    gen_body  :: [(Identifier, K3 Type)] -> CPPGenM CPPGenR
    gen_body n_ts = do
      result_decl <- return $ result_type <+> result_var <> semi
      inserts     <- gen_inserts n_ts
      return_st   <- return $ text "return" <+> result_var <> semi
      return $ vsep $ (result_decl : inserts) ++ [return_st]

    gen_inserts :: [(Identifier, K3 Type)] -> CPPGenM [CPPGenR]
    gen_inserts n_ts = do
      names      <- return $ map fst n_ts
      name_strs  <- return $ map (dquotes . text) names
      val_strs   <- mapM (\(n,t) -> showVar t n) n_ts
      kvs        <- return $  zip name_strs val_strs
      mk_insert  <- return $ \(k,v) -> result_var <> brackets k <> text " = " <> v <> semi
      return $ map mk_insert kvs

-- | Generate CPP code to show the a global variable of given K3-Type and name
showVar :: K3 Type -> String -> CPPGenM CPPGenR
showVar base_t name =
  case base_t of
    -- non-nested:
    (tag -> TBool)     -> return $ std_to_string name
    (tag -> TByte)     -> return $ std_to_string name
    (tag -> TInt)      -> return $ std_to_string name
    (tag -> TReal)     -> return $ std_to_string name
    (tag -> TString)   -> return $ text name
    (tag -> TAddress)  -> return $ text "addressAsString" <> parens (text name)
    (tag -> TFunction) -> return $ str "<fun>"
    -- nested:
    ((tag &&& children) -> (TOption, [t]))      -> opt_to_string t name
    ((tag &&& children) -> (TIndirection, [t])) -> ind_to_string t name
    ((tag &&& children) -> (TTuple, ts))        -> tup_to_string ts name
    ((tag &&& children) -> (TRecord ids, ts))   -> rec_to_string ids ts name
    ((tag &&& children) -> (TCollection, [et])) -> coll_to_string base_t et name
    _                                           -> return $ str "Cant Show!"
  where
    -- Utils
    str = dquotes . text
    deref = (++) "*"
    getTup i n = "get<" ++ (show i) ++ ">(" ++ n ++ ")"
    project field n = n ++ "." ++ field
    inner_comma = text "+" <+> str "," <+> text "+"
    -- Use std::to_string for basic types
    std_to_string n = text "to_string" <> parens (text n)
    -- Option
    opt_to_string ct n = do
        inner <- showVar ct (deref n)
        let i = text "if" <+> parens (text n) <> hangBrace (str "Some" <+> text "+" <+> inner)
            e = text "else" <> hangBrace (str "None")
            in return $ vsep [i,e]
    -- Indirection
    ind_to_string ct n = do
        inner <- showVar ct (deref n)
        return $ str "Ind" <+> text "+" <+> inner
    -- Tuple
    tup_to_string cts n = do
        ct_is <- return $ zip cts ([0..] :: [Integer])
        cs    <- mapM (\(ct,i) -> showVar ct (getTup i n)) ct_is
        done  <- return $ L.intersperse inner_comma cs
        return $ vsep $ [str "(", text "+", (hsep done), text "+", str ")"]
    -- Record
    rec_to_string ids cts n = do
        ct_ids <- return $ zip cts ids
        cs     <- mapM (\(ct,field) -> showVar ct (project field n) >>= \v -> return $ str (field ++ ":") <+> text "+" <+>  v) ct_ids
        done   <- return $ L.intersperse inner_comma cs
        return $ str "{" <+> text "+" <+> parens (hsep done <+> text "+" <+> str "}")
    -- Collection
    coll_to_string t et n = do
        rvar <- return $ text "ostringstream oss;"
        t_n  <- genCType t
        et_n <- genCType et
        v    <- showVar et "elem"
        fun  <- return $ text "auto f = [&]" <+> parens (et_n <+> text "elem") <+> braces (text "oss <<" <+> v <+> text "<< \",\";") <> semi
        iter <- return $ text "coll" <> dot <> text "iterate" <> parens (text "f") <> semi
        result <- return $ text "return" <+> str "[" <+> text "+" <+> text "oss.str()" <+> text "+" <+> str "]" <> semi
        -- wrap in lambda, then call it
        return $ parens $ text "[]" <+> parens (t_n <+> text "coll") <+> hangBrace (vsep [rvar,fun,iter,result]) <> parens (text n)
