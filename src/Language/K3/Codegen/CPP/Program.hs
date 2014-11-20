{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Program where

import Control.Arrow ((&&&), first)
import Control.Monad.State

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import Data.Maybe (isJust)

import Data.Functor

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Collections
import Language.K3.Codegen.CPP.Declaration
import Language.K3.Codegen.CPP.Preprocessing
import Language.K3.Codegen.CPP.Primitives (genCType)
import Language.K3.Codegen.CPP.Types

import qualified Language.K3.Codegen.Imperative as I

import qualified Language.K3.Codegen.CPP.Representation as R

-- | Copy state elements from the imperative transformation to CPP code generation.
-- | Also mangle the lists for C++
transitionCPPGenS :: I.ImperativeS -> CPPGenS
transitionCPPGenS is = defaultCPPGenS
    { globals    = convert $ I.globals is
    , patchables = convert $ I.patchables is
    , showables  = convert $ I.showables is
    , triggers   = convert $ I.triggers is
    }
  where
    convert = map (first mangleReservedId)

stringifyProgram :: K3 Declaration -> CPPGenM Doc
stringifyProgram d = vsep . map R.stringify <$> program d

-- Top-level program generation.
-- publics <- concat <$> mapM declaration cs
program :: K3 Declaration -> CPPGenM [R.Definition]
program (mangleReservedNames -> (tag &&& children -> (DRole name, decls))) = do
    -- Process the program, accumulate global state.
    programDefns <- concat <$> mapM declaration decls

    globalInits <- globalInitializations <$> get
    let initDeclDefn = R.FunctionDefn (R.Name "initDecls") [("_", R.Unit)] (Just R.Unit) [] False
                         (globalInits ++ [R.Return (R.Initialization R.Unit [])])
    -- Generate program preamble.
    includeDefns <- map R.IncludeDefn <$> requiredIncludes
    aliasDefns   <- map (R.GlobalDefn . R.Forward . uncurry R.UsingDecl) <$> requiredAliases
    compositeDefns <- do
        currentComposites <- composites <$> get
        currentAnnotations <- annotationMap <$> get
        forM (S.toList $ S.filter (not . S.null) currentComposites) $ \(S.toList -> als) ->
            composite (annotationComboId als) [(a, M.findWithDefault [] a currentAnnotations) | a <- als]
    records <- map (map fst) . snd . unzip . M.toList . recordMap <$> get
    recordDefns <- mapM record records

    let contextName = R.Name $ name ++ "_context"

    inits <- initializations <$> get

    prettify <- genPrettify
    jsonify <- genJsonify

    patchables' <- patchables <$> get

    patchables'' <- forM patchables' $ \(i, (t, f)) -> genCType t >>= \ct -> return (i, (ct, f))

    let yamlStructDefn = R.NamespaceDefn "YAML"
                         [ R.TemplateDefn [] $ R.ClassDefn (R.Name "convert") [R.Named contextName] []
                             [ R.FunctionDefn (R.Name "encode")
                                 [("context", R.Const $ R.Reference $ R.Named contextName)]
                                 (Just $ R.Static $ R.Named $ R.Name "Node") [] False
                                 ([R.Forward $ R.ScalarDecl (R.Name "_node") (R.Named $ R.Name "Node") Nothing] ++
                                  [R.Assignment (R.Subscript
                                                      (R.Variable $ R.Name "_node")
                                                      (R.Literal $ R.LString field))
                                                (R.Call
                                                      (R.Variable $ R.Qualified
                                                            (R.Specialized [fieldType] (R.Name "convert"))
                                                            (R.Name "encode"))
                                                      [R.Project (R.Variable $ R.Name "context") (R.Name field)])
                                  | (field, (fieldType, _)) <- patchables''
                                  ] ++ [R.Return (R.Variable $ R.Name "_node")])
                             , R.FunctionDefn (R.Name "decode")
                                 [ ("node", R.Const $ R.Reference $ R.Named $ R.Name "Node")
                                 , ("context", R.Reference $ R.Named contextName)
                                 ] (Just $ R.Static $ R.Primitive $ R.PBool) [] False
                                 ([ R.IfThenElse (R.Unary "!" $ R.Call (R.Project
                                                                            (R.Variable $ R.Name "node")
                                                                            (R.Name "IsMap")) [])
                                     [R.Return $ R.Literal $ R.LBool False] []
                                 ] ++
                                 [ R.IfThenElse (R.Subscript
                                                      (R.Variable $ R.Name "node")
                                                      (R.Literal $ R.LString field))
                                     ([ R.Assignment
                                         (R.Project (R.Variable $ R.Name "context") (R.Name field))
                                         (R.Call (R.Project
                                                   (R.Subscript
                                                         (R.Variable $ R.Name "node")
                                                         (R.Literal $ R.LString field))
                                                   (R.Specialized [fieldType] $ R.Name "as")) [])
                                     ] ++
                                     [ R.Assignment (R.Project (R.Variable $ R.Name "context")
                                                          (R.Name $ "__" ++ field ++ "_set__"))
                                                    (R.Literal $ R.LBool True)
                                     | setF
                                     ]) []
                                 | (field, (fieldType, setF)) <- patchables''
                                 ] ++ [R.Return $ R.Literal $ R.LBool True])
                             ]
                             [] []
                         ]

    dispatchPop <- generateDispatchPopulation

    let contextConstructor = R.FunctionDefn contextName [("__engine", R.Reference $ R.Named (R.Name "Engine"))]
                             Nothing
                             [ R.Call
                                 (R.Variable $ R.Qualified (R.Name "K3") $ R.Name "__standard_context")
                                 [R.Variable $ R.Name "__engine"]
                             -- builtin mixins:
                             , R.Call
                                 (R.Variable $ R.Qualified (R.Name "K3") $ R.Name "__string_context")
                                 []

                             , R.Call
                                 (R.Variable $ R.Qualified (R.Name "K3") $ R.Name "__time_context")
                                 []
                             ]
                             False
                             (inits ++ dispatchPop)

    let dispatchDecl = R.FunctionDefn (R.Name "__dispatch")
                       [("trigger_id", R.Primitive R.PInt), ("payload", R.Named $ R.Name "void*")]
                       (Just R.Void) [] False
                       [R.Ignore $ R.Call
                         (R.Subscript (R.Variable $ R.Name "dispatch_table") (R.Variable $ R.Name "trigger_id"))
                         [R.Variable $ R.Name "payload"]
                       ]
    let dispatchTableDecl  = R.GlobalDefn $ R.Forward $ R.ScalarDecl
                     (R.Name "dispatch_table")
                     (R.Named $ R.Qualified (R.Name "std") $ R.Specialized
                           [R.Primitive R.PInt, R.Function [R.Named $ R.Name "void*"] R.Void] (R.Name "map"))
                     Nothing

    let patchFn = R.FunctionDefn (R.Qualified contextName (R.Name "__patch"))
                  [("s", R.Named $ R.Qualified (R.Name "std") (R.Name "string"))]
                  (Just R.Void) [] False
                  [ R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "YAML") $ R.Qualified (R.Specialized
                                                         [R.Named contextName]
                                                         (R.Name "convert"))
                                       (R.Name "decode"))
                               [ R.Call (R.Variable $ R.Qualified (R.Name "YAML") (R.Name "Load"))
                                            [R.Variable $ R.Name "s"]
                               , R.Dereference (R.Variable $ R.Name "this")]
                  ]

    let patchFnDecl = R.GlobalDefn $ R.Forward $ R.FunctionDecl (R.Name "__patch")
                      [R.Named $ R.Qualified (R.Name "std") (R.Name "string")] R.Void

    let contextDefns = [contextConstructor] ++ programDefns  ++ [initDeclDefn] ++
                       [patchFnDecl, prettify, jsonify, dispatchDecl]

    let contextClassDefn = R.ClassDefn contextName []
                             [ R.Named $ R.Qualified (R.Name "K3") $ R.Name "__standard_context"
                             , R.Named $ R.Qualified (R.Name "K3") $ R.Name "__string_context"
                             , R.Named $ R.Qualified (R.Name "K3") $ R.Name "__time_context"
                             ]
                             contextDefns [] [dispatchTableDecl]

    pinned <- (map R.GlobalDefn) <$> definePinnedGlobals
    mainFn <- main

    -- Return all top-level definitions.
    return $ includeDefns ++ aliasDefns ++ concat recordDefns ++ concat compositeDefns ++
               [contextClassDefn] ++ pinned ++ [yamlStructDefn, patchFn] ++ mainFn

program _ = throwE $ CPPGenE "Top-level declaration construct must be a Role."

main :: CPPGenM [R.Definition]
main = do
    let optionDecl = R.Forward $ R.ScalarDecl (R.Name "opt") (R.Named $ R.Name "Options") Nothing
    let optionCall = R.IfThenElse (R.Call (R.Project (R.Variable $ R.Name "opt") (R.Name "parse"))
                                    [R.Variable $ R.Name "argc", R.Variable $ R.Name "argv"])
                     [R.Return (R.Literal $ R.LInt 0)] []

    staticContextMembersPop <- generateStaticContextMembers


    let runProgram = R.Ignore $ R.Call
                       (R.Variable $ R.Specialized [R.Named $ R.Name "__global_context"] (R.Name "runProgram"))
                       [ (R.Project (R.Variable $ R.Name "opt") (R.Name "peer_strings"))
                       , (R.Project (R.Variable $ R.Name "opt") (R.Name "simulation"))
                       , (R.Project (R.Variable $ R.Name "opt") (R.Name "log_level") )
                       ]

    return [
        R.FunctionDefn (R.Name "main") [("argc", R.Primitive R.PInt), ("argv", R.Named (R.Name "char**"))]
             (Just $ R.Primitive R.PInt) [] False
             (
             staticContextMembersPop ++
             [ optionDecl
             , optionCall
             , runProgram
             ]
             )
       ]

requiredAliases :: CPPGenM [(Either R.Name R.Name, Maybe R.Name)]
requiredAliases = return
                  [ (Right (R.Qualified (R.Name "K3" )$ R.Name "Address"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "Engine"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "string_impl"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "Options"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "ValDispatcher"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "Dispatcher"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "virtualizing_message_processor"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "make_address"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "__k3_context"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "runProgram"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "SystemEnvironment"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "processRoles"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "defaultEnvironment"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "createContexts"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "getAddrs"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "DefaultInternalCodec"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "currentTime"), Nothing)
                  , (Right (R.Qualified (R.Name "std")$ R.Name "make_tuple" ), Nothing)
                  , (Right (R.Qualified (R.Name "std")$ R.Name "make_shared" ), Nothing)
                  , (Right (R.Qualified (R.Name "std")$ R.Name "shared_ptr" ), Nothing)
                  , (Right (R.Qualified (R.Name "std")$ R.Name "get" ), Nothing)
                  , (Right (R.Qualified (R.Name "std")$ R.Name "map"), Nothing)
                  , (Right (R.Qualified (R.Name "std")$ R.Name "list"), Nothing)
                  , (Right (R.Qualified (R.Name "std")$ R.Name "ostringstream"), Nothing)
                  ]

requiredIncludes :: CPPGenM [Identifier]
requiredIncludes = return
                   [ "functional"
                   , "map"
                   , "memory"
                   , "sstream"
                   , "string"
                   , "tuple"

                   , "boost/multi_index_container.hpp"
                   , "boost/multi_index/ordered_index.hpp"
                   , "boost/multi_index/member.hpp"
                   , "boost/multi_index/composite_key.hpp"

                   , "BaseTypes.hpp"
                   , "BaseString.hpp"
                   , "Common.hpp"
                   , "Context.hpp"
                   , "Dispatch.hpp"
                   , "Engine.hpp"
                   , "MessageProcessor.hpp"
                   , "Literals.hpp"
                   , "Serialization.hpp"
                   , "serialization/yaml.hpp"
                   , "serialization/json.hpp"
                   , "Builtins.hpp"
                   , "Run.hpp"
                   , "Prettify.hpp"

                   , "dataspace/Dataspace.hpp"

                   , "yaml-cpp/yaml.h"
                   ]


definePinnedGlobals :: CPPGenM [R.Statement]
definePinnedGlobals = staticDeclarations <$> get

idOfTrigger :: Identifier -> Identifier
idOfTrigger t = "__" ++ unmangleReservedId t ++ "_tid"

generateStaticContextMembers :: CPPGenM [R.Statement]
generateStaticContextMembers = do
  triggerS <- triggers <$> get
  initializations <- staticInitializations <$> get
  names <- mapM assignTrigName triggerS
  dispatchers <- mapM assignClonableDispatcher triggerS
  return $ initializations ++ names ++ dispatchers
  where
    assignTrigName (tName, _) = do
      let i = R.Variable $ R.Qualified (R.Name "__global_context") (R.Name (idOfTrigger tName))
      let nameStr = R.Literal $ R.LString tName
      let table = R.Variable $ R.Qualified (R.Name "__k3_context") (R.Name "__trigger_names")
      return $ R.Assignment (R.Subscript table i) nameStr

    assignClonableDispatcher (tName, tType) = do
      kType <- genCType tType
      let i = R.Variable $ R.Qualified (R.Name "__global_context") (R.Name (idOfTrigger tName))
      let table = R.Variable $ R.Qualified (R.Name "__k3_context") (R.Name "__clonable_dispatchers")
      let dispatcher = R.Call
                         (R.Variable $ R.Specialized [R.Named $ R.Specialized [kType] (R.Name "ValDispatcher")] (R.Name "make_shared"))
                         []
      return $ R.Assignment (R.Subscript table i) dispatcher


generateDispatchPopulation :: CPPGenM [R.Statement]
generateDispatchPopulation = do
  triggerS <- triggers <$> get
  mapM genDispatch triggerS
  where
     table = R.Variable $ R.Name "dispatch_table"
     genDispatch (tName, tType) = do
       kType <- genCType tType

       let i = R.Variable $ R.Name (idOfTrigger tName)

       let engine = R.Project (R.Dereference $ R.Variable $ R.Name "this") (R.Name "__engine")
       let ctDecl = R.Forward $ R.ScalarDecl (R.Name "ct") R.Inferred
                      (Just $ R.Call (R.Variable $ R.Name "currentTime") [])
       let encoded_payload = R.Call (R.Variable $ R.Specialized [kType] (R.Name "K3::serialization::json::encode"))
                               [R.Variable $ R.Name "v"]
       let dispatchWrapper = R.Lambda
                             [R.ValueCapture $ Just ("this", Nothing)]
                             [("payload", R.Named $ R.Name "void*")] False Nothing
                             [ ctDecl
                             , R.Forward $ R.ScalarDecl (R.Name "v") R.Inferred
                                 (Just $ R.Dereference $ R.Call (R.Variable $ R.Specialized [R.Pointer kType] $
                                                             R.Name "static_cast") [R.Variable $ R.Name "payload"])
                             , R.Ignore $ R.Call (R.Variable $ R.Name tName)
                                   [R.Variable $ R.Name "v"]
                             , R.IfThenElse (R.Call (R.Project engine (R.Name "logEnabled")) [])
                                 [ R.Ignore $ R.Call ((R.Project engine) (R.Name "logJson"))
                                    [ R.Variable $ R.Name "ct"
                                    , R.Variable $ R.Name "me"
                                    , R.Literal $ R.LString tName
                                    , encoded_payload
                                    , R.Call (R.Variable $ R.Name "__jsonify") []
                                    ]
                                 ]
                                 []
                             ]

       return $ R.Assignment (R.Subscript table i) dispatchWrapper


genJsonify :: CPPGenM R.Definition
genJsonify = do
   currentS <- get
   body    <- genBody $ showables currentS
   --let body = [R.Return $ R.Initialization result_type []]
   return $ R.FunctionDefn (R.Name "__jsonify") [] (Just result_type) [] False body
  where
   genBody  :: [(Identifier, K3 Type)] -> CPPGenM [R.Statement]
   genBody n_ts = do
     result_decl <- return $ R.Forward $ R.ScalarDecl (R.Name result) result_type Nothing
     inserts     <- genInserts n_ts
     return_st   <- return $ R.Return $ R.Variable $ R.Name result
     return $ (result_decl : inserts) ++ [return_st]

   -- Insert key-value pairs into the map
   genInserts :: [(Identifier, K3 Type)] -> CPPGenM [R.Statement]
   genInserts n_ts' = do
     -- Don't include unit vars
     let n_ts      = filter (not . isTUnit . snd) n_ts'
         names     = map fst n_ts
         name_vars = map (R.Variable . R.Name) names
         new_nts   = zip name_vars $ map snd n_ts
         lhs_exprs = map (\x -> R.Subscript (R.Variable $ R.Name result) (R.Literal $ R.LString x)) names
     rhs_exprs  <- mapM (\(n,t) -> jsonifyExpr t n) new_nts
     return $ zipWith R.Assignment lhs_exprs rhs_exprs
     where
       isTUnit (tnc -> (TTuple, [])) = True
       isTUnit _ = False

       jsonifyExpr t n = do
         cType <- genCType t
         return $ R.Call (R.Variable $ R.Specialized [cType] (R.Name "K3::serialization::json::encode")) [n]

   p_string = R.Named $ R.Qualified (R.Name "std") (R.Name "string")
   result_type  = R.Named $ R.Qualified (R.Name "std") (R.Specialized [p_string, p_string] (R.Name "map"))
   result  = "__result"

-- Generate a function to help print the current environment (global vars and their values).
-- Currently, this function returns a map from string (variable name) to string (string representation of value)
prettifyName :: R.Name
prettifyName = R.Name "__prettify"

genPrettify :: CPPGenM R.Definition
genPrettify = do
   currentS <- get
   body    <- genBody $ showables currentS
   --let body = [R.Return $ R.Initialization result_type []]
   return $ R.FunctionDefn prettifyName [] (Just result_type) [] False body
 where
   p_string = R.Named $ R.Qualified (R.Name "std") (R.Name "string")
   result_type  = R.Named $ R.Qualified (R.Name "std") (R.Specialized [p_string, p_string] (R.Name "map"))
   result  = "__result"

   genBody  :: [(Identifier, K3 Type)] -> CPPGenM [R.Statement]
   genBody n_ts = do
     result_decl <- return $ R.Forward $ R.ScalarDecl (R.Name result) result_type Nothing
     inserts     <- genInserts n_ts
     return_st   <- return $ R.Return $ R.Variable $ R.Name result
     return $ (result_decl : inserts) ++ [return_st]

   -- Insert key-value pairs into the map
   genInserts :: [(Identifier, K3 Type)] -> CPPGenM [R.Statement]
   genInserts n_ts' = do
     -- Don't include unit vars
     let n_ts      = filter (not . isTUnit . snd) n_ts'
         names     = map fst n_ts
         name_vars = map (R.Variable . R.Name) names
         new_nts   = zip name_vars $ map snd n_ts
         lhs_exprs = map (\x -> R.Subscript (R.Variable $ R.Name result) (R.Literal $ R.LString x)) names
     rhs_exprs  <- mapM (\(n,t) -> prettifyExpr t n) new_nts
     return $ zipWith R.Assignment lhs_exprs rhs_exprs
     where
       isTUnit (tnc -> (TTuple, [])) = True
       isTUnit _ = False

-- | Generate an expression that represents an expression of the given type as a string
prettifyExpr :: K3 Type -> R.Expression -> CPPGenM R.Expression
prettifyExpr base_t e =
 case base_t of
   -- non-nested:
   (tag -> TBool)     -> return $ call_prettify "bool" [e]
   (tag -> TByte)     -> return $ call_prettify "byte" [e]
   (tag -> TInt)      -> return $ call_prettify "int" [e]
   (tag -> TReal)     -> return $ call_prettify "real" [e]
   (tag -> TString)   -> return $ call_prettify "string" [e]
   (tag -> TAddress)  -> return $ call_prettify "address" [e]
   (tag -> TFunction) -> return $ call_prettify "function" [e]
   -- nested:
   ((tag &&& children) -> (TOption, [t]))      -> opt t
   ((tag &&& children) -> (TIndirection, [t])) -> ind_to_string t
   ((tag &&& children) -> (TTuple, ts))        -> tup_to_string ts
   ((tag &&& children) -> (TRecord ids, ts))   -> rec_to_string ids ts
   ((tag &&& children) -> (TCollection, [et])) -> coll_to_string base_t et
   _                                           -> return $ lit_string "Cant Show!"
 where
   -- Utils
   call_prettify x exp =  R.Call (R.Variable (R.Qualified (R.Name "K3") (R.Name $ "prettify_" ++ x))) exp
   wrap_inner t = do
     cType <- genCType t
     inner <- prettifyExpr t (R.Variable $ R.Name "x")
     return $ R.Lambda [] [("x", cType)] False Nothing [R.Return $ inner]

   oss_decl = R.Forward $ R.ScalarDecl (R.Name "oss") (R.Named $ R.Name "ostringstream") Nothing
   oss_concat = R.Binary "<<"
   oss = (R.Variable $ R.Name "oss")

   lit_string  = R.Literal . R.LString
   std_string s = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "string")) [lit_string s]
   project field n = R.Project n (R.Name field)

   -- Option
   opt ct = do
       f <- wrap_inner ct
       return $ call_prettify "option" [e, f]

   -- Indirection
   ind_to_string ct = do
       f <- wrap_inner ct
       return $ call_prettify "indirection" [e, f]

   -- TODO
   -- Tuple
   tup_to_string _ = return $ call_prettify "tuple" [e]
   -- Record
   rec_to_string ids cts = do
       cType  <- genCType base_t
       let ct_ids = zip cts ids
       let x = R.Variable $ R.Name "x"
       cs     <- mapM (\(ct,field) -> prettifyExpr ct (project field x) >>= \v -> return $ (oss_concat oss (oss_concat (std_string $ field ++ ":") v ))) ct_ids
       done   <- return $ map R.Ignore $ L.intersperse (oss_concat oss (lit_string  ",")) cs
       let front = R.Ignore $ oss_concat oss (lit_string "{")
       let end = R.Ignore $ oss_concat oss (lit_string "}")
       let str = R.Call (R.Project oss (R.Name "str")) []
       let ret = R.Return $ R.Call (R.Variable $ R.Name "string_impl") [str]
       let body = [oss_decl, front] ++ done ++ [end, ret]
       let f =  R.Lambda [] [("x", cType)] False Nothing body
       return $ call_prettify "record" [e, f]

   -- Collection
   coll_to_string t et = do
       f <- wrap_inner et
       return $ call_prettify "collection" [e, f]
