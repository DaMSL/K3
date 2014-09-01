{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Program where

import Control.Arrow ((&&&), first)
import Control.Monad.State

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S

import Data.Functor

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Collections
import Language.K3.Codegen.CPP.Declaration
import Language.K3.Codegen.CPP.Preprocessing
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types
import qualified Language.K3.Codegen.Imperative as I

import qualified Language.K3.Codegen.CPP.Representation as R

-- | Copy state elements from the imperative transformation to CPP code generation.
-- | Also mangle the lists for C++
transitionCPPGenS :: I.ImperativeS -> CPPGenS
transitionCPPGenS is = defaultCPPGenS
    { globals    = convert $ I.globals is
    , patchables = map mangleReservedId $ I.patchables is
    , showables  = convert $ I.showables is
    , triggers   = add_numbers $ convert $ I.triggers is
    }
  where
    convert = map (first mangleReservedId)
    add_numbers l = zipWith (\(x,t) i -> (x,(t,i))) l [0..length l]

stringifyProgram :: K3 Declaration -> CPPGenM Doc
stringifyProgram d = vsep . map R.stringify <$> program d

-- Top-level program generation.
-- publics <- concat <$> mapM declaration cs
program :: K3 Declaration -> CPPGenM [R.Definition]
program (tag &&& children -> (DRole name, decls)) = do
    -- Process the program, accumulate global state.
    programDefns <- concat <$> mapM declaration decls

    -- Generate program preamble.
    includeDefns <- map R.IncludeDefn <$> requiredIncludes
    aliasDefns <- map (R.GlobalDefn . R.Forward . uncurry R.UsingDecl) <$> requiredAliases
    forwardDefns <- map (R.GlobalDefn . R.Forward) . forwards <$> get
    compositeDefns <- do
        currentComposites <- composites <$> get
        currentAnnotations <- annotationMap <$> get
        forM (S.toList $ S.filter (not . S.null) currentComposites) $ \(S.toList -> als) ->
            composite (annotationComboId als) [(a, M.findWithDefault [] a currentAnnotations) | a <- als]
    records <- map (map fst) . snd . unzip . M.toList . recordMap <$> get
    recordDefns <- mapM record records

    let contextName = R.Name $ name ++ "_context"

    inits <- initializations <$> get

    let contextConstructor = R.FunctionDefn contextName [] Nothing
                             [R.Call (R.Variable $ R.Name "__program_context") []] inits

    let contextDefns = [contextConstructor] ++ forwardDefns ++ programDefns
    let contextClassDefn = R.ClassDefn contextName [R.Named $ R.Name "__program_context"] [] contextDefns [] []

    mainFn <- main

    -- Return all top-level definitions.
    return $ includeDefns ++ aliasDefns ++ concat recordDefns ++ concat compositeDefns ++ [contextClassDefn] ++ mainFn

program _ = throwE $ CPPGenE "Top-level declaration construct must be a Role."

main :: CPPGenM [R.Definition]
main = return [
        R.FunctionDefn (R.Name "main") [("argc", R.Primitive R.PInt), ("argv", R.Named (R.Name "char**"))]
             (Just $ R.Primitive R.PInt) [] []
       ]

requiredAliases :: CPPGenM [(Either R.Name R.Name, Maybe R.Name)]
requiredAliases = return
                  [ (Right (R.Qualified "K3" $ R.Name "unit_t"), Nothing)
                  , (Right (R.Qualified "K3" $ R.Name "Address"), Nothing)]

requiredIncludes :: CPPGenM [Identifier]
requiredIncludes = return
                   [ "string"
                   , "tuple"

                   , "Common.hpp"
                   ]

-- program :: K3 Declaration -> CPPGenM CPPGenR
-- program d = do
--     let preprocessed = mangleReservedNames d
--     s <- showGlobals
--     staticGlobals' <- staticGlobals
--     program' <- declaration preprocessed
--     genNamespaces <- namespaces  >>= \ns -> return [text "using" <+> text n <> semi | n <- ns]
--     genIncludes2  <- sysIncludes >>= \is -> return [text "#include" <+> angles (text f) | f <- is]
--     genIncludes   <- includes    >>= \is -> return [text "#include" <+> dquotes (text f) | f <- is]
--     main <- genKMain
--     return $ vsep $ punctuate line [
--             text "#define MAIN_PROGRAM",
--             vsep genIncludes2,
--             vsep genIncludes,
--             vsep genNamespaces,
--             vsep genAliases,
--             staticGlobals',
--             program',
--             s,
--             main
--         ]
--   where
--     genAliases = [text "using" <+> text new <+> equals <+> text old <> semi | (new, old) <- aliases]

-- matchersDecl :: CPPGenM CPPGenR
-- matchersDecl = do
--     let mapDecl = genCDecl
--                    (text "map" <> angles (cat $ punctuate comma [text "string", text "std::function<void(string)>"]))
--                    (text "matchers")
--                    Nothing
--     mapInit <- map populateMatchers . patchables <$> get
--     return $ vsep $ mapDecl : mapInit
--   where
--     populateMatchers :: Identifier -> CPPGenR
--     populateMatchers f = text "matchers"
--                             <> brackets (dquotes $ text f)
--                             <+> equals
--                             <+> brackets empty
--                             <+> parens (text "string _s")
--                             <+> braces (genCCall (text "do_patch") Nothing [text "_s", text f] <> semi)
--                             <> semi

-- genKMain :: CPPGenM CPPGenR
-- genKMain = do
--     matchers <- matchersDecl
--     return $ genCFunction Nothing (text "int") (text "main") [text "int argc", text "char** argv"] $ vsep [
--             genCCall (text "initGlobalDecls") Nothing [] <> semi,
--             genCDecl (text "Options") (text "opt") Nothing,
--             (text "if ") <>
--               parens (genCCall (text "opt.parse") Nothing [text "argc", text "argv"])
--               <> text " return 0" <> semi,
--             genCCall (text "populate_dispatch") Nothing [] <> semi,
--             matchers,
--             genCDecl (text "string") (text "parse_arg") (Just $ text "opt.peer_strings[0]") <> semi,
--             genCDecl (text "map" <> angles (cat $ punctuate comma [text "string", text "string"]))
--               (text "bindings") (Just $ genCCall (text "parse_bindings") Nothing
--               [text "parse_arg"]),
--             genCCall (text "match_patchers") Nothing [text "bindings", text "matchers"] <> semi,
--             text "list<Address> addr_l;",
--             text "addr_l.push_back(me);",
--             text "SystemEnvironment se = defaultEnvironment(addr_l);",
--             genCCall (text "engine.configure") Nothing
--               [text "opt.simulation", text "se",
--                text "make_shared<DefaultInternalCodec>(DefaultInternalCodec())",
--                text "opt.log_level"] <> semi,
--             genCCall (text "processRole") Nothing [text "unit_t()"] <> semi,
--             genCDecl (text "DispatchMessageProcessor") (text "dmp") (Just $
--               genCCall (text "DispatchMessageProcessor") Nothing
--                 [text showGlobalsName]) <> semi,
--             text "engine.runEngine(make_shared<DispatchMessageProcessor>(dmp))" <> semi,
--             text "return 0" <> semi
--         ]

-- sysIncludes :: CPPGenM [Identifier]
-- sysIncludes = return [
--         -- Standard Library
--         "functional",
--         "memory",
--         "sstream",
--         "string",

--         -- Strtk
--         "external/strtk.hpp",

--         -- JSON
--         "external/json_spirit_reader_template.h"
--       ]

-- includes :: CPPGenM [Identifier]
-- includes = return [
--         -- K3 Runtime
--         "BaseTypes.hpp",
--         "Common.hpp",
--         "dataspace/Dataspace.hpp",
--         "BaseCollections.hpp",
--         "Dispatch.hpp",
--         "Engine.hpp",
--         "Literals.hpp",
--         "MessageProcessor.hpp",
--         "Serialization.hpp",
--         "Builtins.hpp"
--       ]

-- namespaces :: CPPGenM [Identifier]
-- namespaces = do
--     serializationNamespace <- serializationMethod <$> get >>= \case
--         BoostSerialization -> return "namespace K3::BoostSerializer"
--     return [
--             "namespace std",
--             "namespace K3",
--             serializationNamespace,
--             "std::begin", "std::end"
--         ]

-- aliases :: [(Identifier, Identifier)]
-- aliases = [
--     ]

-- staticGlobals :: CPPGenM CPPGenR
-- staticGlobals = return $ text "K3::Engine engine;"

 --| Generate a function to help print the current environment (global vars and their values).
 -- Currently, this function returns a map from string (variable name) to string (string representation of value)
--showGlobalsName :: R.Name
--showGlobalsName = R.Name "prettify"

--showGlobals :: CPPGenM R.Definition
--showGlobals = do
--   currentS <- get
--   body    <- gen_body $ showables currentS
--   return $ R.FunctionDefn showGlobalsName [] (Just result_type) [] body
-- where
--   p_string = R.Primitive R.PString
--   result_type  = R.Named $ R.Specialized [p_string, p_string] (R.Name "map")
--   result  = "result"

--   gen_body  :: [(Identifier, K3 Type)] -> CPPGenM [R.Statement]
--   gen_body n_ts = do
--     result_decl <- return $ R.Forward $ R.ScalarDecl (R.Name result) result_type Nothing
--     inserts     <- gen_inserts n_ts
--     return_st   <- return $ R.Return $ R.Variable $ R.Name result
--     return $ (result_decl : inserts) ++ [return_st]

--   -- Insert a key-value pair into the map
--   gen_inserts :: [(Identifier, K3 Type)] -> CPPGenM [R.Statement]
--   gen_inserts n_ts = do
--     names      <- return $ map fst n_ts
--     name_vars  <- return $ map (R.Variable . R.Name) names
--     new_nts    <- return $ zip name_vars $ map snd n_ts
--     lhs_exprs  <- return $ map (\x -> R.Variable $ R.Name $ result ++ "[\"" ++ x ++ "\"]") names
--     rhs_exprs  <- mapM (\(n,t) -> showVar t n) new_nts
--     return $ zipWith (R.Assignment) lhs_exprs rhs_exprs

---- | Generate an expression that represents a variable as a string
--showVar :: K3 Type -> R.Expression -> CPPGenM R.Expression
--showVar base_t e =
-- case base_t of
--   -- non-nested:
--   (tag -> TBool)     -> return $ to_string e
--   (tag -> TByte)     -> return $ to_string e
--   (tag -> TInt)      -> return $ to_string e
--   (tag -> TReal)     -> return $ to_string e
--   (tag -> TString)   -> return $ e
--   (tag -> TAddress)  -> return $ R.Call (R.Variable $ R.Name "addressAsString") [e]
--   (tag -> TFunction) -> return $ lit_string "<opaque_function>"
--   -- nested:
--   ((tag &&& children) -> (TOption, [t]))      -> opt t e
--   ((tag &&& children) -> (TIndirection, [t])) -> ind_to_string t e
--   ((tag &&& children) -> (TTuple, ts))        -> tup_to_string ts e
--   ((tag &&& children) -> (TRecord ids, ts))   -> rec_to_string ids ts name
--n
--   _                                           -> return $ lit_string "Cant Show!"
-- where
--   -- Utils
--   singleton = replicate 1

--   var_by_name = R.Variable . R.Name
--   lit_string  = R.Literal . R.LString
--   wrap stmnts e = R.Call (R.Lambda [] [("x", R.Reference $ R.Named $ R.Name "auto")] Nothing stmnts) [e]
--   to_string e = R.Call (R.Variable $ R.Qualified "std" $ R.Name "to_string") [e]
--   concat = R.Binary "+"
--   get_tup i e = R.Call (R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")) [e]
--   str = dquotes . text
--   deref = (++) "*"
--   project field n = R.Project (R.Variable $ R.Name field) (R.Name n)
--   inner_comma = line <> text "+" <+> str "," <> line <> text "+"

--   -- Option
--   opt ct e = do
--       inner <- showVar ct (R.Dereference (R.Variable $ R.Name "x"))
--       return $ wrap $ singleton $ R.IfThenElse e [(R.Return $ concat (lit_string "Some ") inner)] [(R.Return $ lit_string "None")] $ e
--   -- Indirection
--   ind_to_string ct e = do
--       inner <- showVar ct (R.Dereference e)
--       return $ concat (lit_string "Ind ") inner
--   -- Tuple
--   tup_to_string cts e = do
--       ct_is  <- return $ zip cts ([0..] :: [Integer])
--       cs     <- mapM (\(ct,i) -> showVar ct (get_tup i e)) ct_is --show each element in tuple
--       commad <- return $ L.intersperse (lit_string ",") cs -- comma seperate
--       return $ concat (foldl concat (lit_string "(") commad) (lit_string ")") -- concat
--   ---- Record
--   rec_to_string ids cts n = do
--       ct_ids <- return $ zip cts ids
--       cs     <- mapM (\(ct,field) -> showVar ct (project field n) >>= \v -> return $ concat (lit_string $ field ++ ":") v) ct_ids
--       done   <- return $ L.intersperse (lit_string ",") cs
--       return $ concat (foldl concat (lit_string "{") done) (lit_string "}")
--   ---- Collection
--   coll_to_string t et n = do
--       rvar <- return $ R.ScalarDecl (R.Name "oss") (R.Named $ R.Name "ostringstream")
--       t_n  <- genCType t
--       et_n <- genCType et
--       e_name <- return $ R.Name "elem"
--       v    <- showVar et (R.Variable e_name)
--       lambda_body <- return $ R.Block $ [R.Ignore $ R.Binary "<<" (R.Variable e_name) (concat v $ lit_string ","), R.Return $ R.Initialization (R.Named $ R.Name "unit_t") []]
--       fun <- return $ R.Lambda [] ("elem", R.Reference $ R.Named $ R.Name "auto") Nothing lambda_body
--       iter <- return $ R.Call (R.Project (R.Variable $ R.Name "x") "iterate") [fun]
--       result <- return $ R.Return $ concat (lit_string "[") (R.Call (R.Project (R.Variable $ R.Name "oss") "str") [])
--       -- wrap in lambda, then call it
--       return $ wrap (R.Block [R.Ignore iterate, result]) n
