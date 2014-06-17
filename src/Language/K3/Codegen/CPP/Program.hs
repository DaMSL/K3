{-# LANGUAGE LambdaCase #-}
module Language.K3.Codegen.CPP.Program where

import Control.Monad.State

import Data.Functor

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration

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
            s,
            program',
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
            genCDecl (text "string") (text "parse_arg") (Just $ text "opt.peer[0]") <> semi,
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
    return $ genCFunction Nothing result_type fun_name [] (gen_body (patchables currentS))
  where
    result_type  = text "map<string,string>"
    fun_name     = text showGlobalsName
    result_var   = text "result"
    var_to_str v = text "K3::show" <> parens (text v)

    gen_body  :: [Identifier] -> CPPGenR
    gen_body names = let
      result_decl = result_type <> space <> result_var <> semi
      inserts     = gen_inserts names
      return_st   = text "return" <> space <> result_var <> semi
      in vsep $ (result_decl : inserts) ++ [return_st]

    gen_inserts :: [Identifier] -> [CPPGenR]
    gen_inserts names = let
      name_strs = map (dquotes . text) names -- C++ string literals
      val_strs   = map var_to_str names
      kvs        = zip name_strs val_strs
      mk_insert (k,v) = result_var <> brackets k <> text " = " <> v <> semi
      in map mk_insert kvs
