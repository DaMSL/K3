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
program (mangleReservedNames -> (tag &&& children -> (DRole name, decls))) = do
    -- Process the program, accumulate global state.
    programDefns <- concat <$> mapM declaration decls
    -- Generate program preamble.
    includeDefns <- map R.IncludeDefn <$> requiredIncludes
    aliasDefns <- map (R.GlobalDefn . R.Forward . uncurry R.UsingDecl) <$> requiredAliases
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

    patchables' <- patchables <$> get

    let popPatch p = R.IfThenElse (R.Binary ">"
                                        (R.Call (R.Project (R.Variable $ R.Name "bindings") (R.Name "count"))
                                          [R.Literal $ R.LString p])
                                        (R.Literal $ R.LInt 0))
                     [R.Ignore $ R.Call (R.Variable $ R.Name "do_patch")
                       [R.Subscript (R.Variable $ R.Name "bindings") (R.Literal $ R.LString p), R.Variable $ R.Name p]]
                     []
    let patchDecl = R.FunctionDefn (R.Name "__patch")
                    [("bindings", R.Named $ R.Qualified (R.Name "std") $ R.Specialized
                                    [R.Primitive R.PString, R.Primitive R.PString] $ R.Name "map")]
                    (Just R.Void) [] False (map popPatch patchables')

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

    let contextDefns = [contextConstructor] ++ programDefns  ++ [prettify, patchDecl, dispatchDecl]
    let contextClassDefn = R.ClassDefn contextName []
                             [ R.Named $ R.Qualified (R.Name "K3") $ R.Name "__standard_context"
                             , R.Named $ R.Qualified (R.Name "K3") $ R.Name "__string_context"
                             , R.Named $ R.Qualified (R.Name "K3") $ R.Name "__time_context"
                             ]
                             contextDefns [] [dispatchTableDecl]
    mainFn <- main

    -- Return all top-level definitions.
    return $ includeDefns ++ aliasDefns ++ concat recordDefns ++ concat compositeDefns ++ [contextClassDefn] ++ mainFn

program _ = throwE $ CPPGenE "Top-level declaration construct must be a Role."

main :: CPPGenM [R.Definition]
main = do
    let optionDecl = R.Forward $ R.ScalarDecl (R.Name "opt") (R.Named $ R.Name "Options") Nothing
    let optionCall = R.IfThenElse (R.Call (R.Project (R.Variable $ R.Name "opt") (R.Name "parse"))
                                    [R.Variable $ R.Name "argc", R.Variable $ R.Name "argv"])
                     [R.Return (R.Literal $ R.LInt 0)] []

    staticContextMembersPop <- R.Block <$> generateStaticContextMembers


    let runProgram = R.Ignore $ R.Call
                       (R.Variable $ R.Specialized [R.Named $ R.Name "__global_context"] (R.Name "runProgram"))
                       [ (R.Project (R.Variable $ R.Name "opt") (R.Name "peer_strings"))
                       , (R.Project (R.Variable $ R.Name "opt") (R.Name "simulation"))
                       , (R.Project (R.Variable $ R.Name "opt") (R.Name "log_level") )
                       ]

    return [
        R.FunctionDefn (R.Name "main") [("argc", R.Primitive R.PInt), ("argv", R.Named (R.Name "char**"))]
             (Just $ R.Primitive R.PInt) [] False
             [ optionDecl
             , optionCall
             , staticContextMembersPop
             , runProgram
             ]
       ]

requiredAliases :: CPPGenM [(Either R.Name R.Name, Maybe R.Name)]
requiredAliases = return
                  [ (Right (R.Qualified (R.Name "K3" )$ R.Name "unit_t"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "Address"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "Engine"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "Options"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "ValDispatcher"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "Dispatcher"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "virtualizing_message_processor"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "make_address"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "__k3_context"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "runProgram"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "SystemEnvironment"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "processRoles"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "do_patch"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "defaultEnvironment"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "createContexts"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "getAddrs"), Nothing)
                  , (Right (R.Qualified (R.Name "K3" )$ R.Name "DefaultInternalCodec"), Nothing)
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
                   , "Common.hpp"
                   , "Context.hpp"
                   , "Dispatch.hpp"
                   , "Engine.hpp"
                   , "MessageProcessor.hpp"
                   , "Literals.hpp"
                   , "Serialization.hpp"
                   , "Builtins.hpp"
                   , "Run.hpp"

                   , "dataspace/Dataspace.hpp"

                   , "strtk.hpp"
                   ]


generateStaticContextMembers :: CPPGenM [R.Statement]
generateStaticContextMembers = do
  triggerS <- triggers <$> get
  names <- mapM assignTrigName triggerS
  dispatchers <- mapM assignClonableDispatcher triggerS
  return $ names ++ dispatchers;
  where
    assignTrigName (tName, (_, tNum)) = do
      let i = R.Literal $ R.LInt tNum
      let nameStr = R.Literal $ R.LString tName
      let table = R.Variable $ R.Qualified (R.Name "__k3_context") (R.Name "__trigger_names")
      return $ R.Assignment (R.Subscript table i) nameStr
    assignClonableDispatcher (_, (tType, tNum)) = do
      kType <- genCType tType
      let i = R.Literal $ R.LInt tNum
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
     genDispatch (tName, (tType, tNum)) = do
       kType <- genCType tType

       let i = R.Literal $ R.LInt tNum

       let dispatchWrapper = R.Lambda
                             [R.ValueCapture $ Just ("this", Nothing)]
                             [("payload", R.Named $ R.Name "void*")] False Nothing
                             [R.Ignore $ R.Call (R.Variable $ R.Name tName)
                                   [R.Dereference $ R.Call (R.Variable $ R.Specialized [R.Pointer kType] $
                                                             R.Name "static_cast")
                                    [R.Variable $ R.Name "payload"]]]

       return $ R.Assignment (R.Subscript table i) dispatchWrapper

-- Generate a function to help print the current environment (global vars and their values).
-- Currently, this function returns a map from string (variable name) to string (string representation of value)
prettifyName :: R.Name
prettifyName = R.Name "__prettify"

genPrettify :: CPPGenM R.Definition
genPrettify = do
   currentS <- get
   body    <- genBody $ showables currentS
   return $ R.FunctionDefn prettifyName [] (Just result_type) [] False body
 where
   p_string = R.Primitive R.PString
   result_type  = R.Named $ R.Qualified (R.Name "std") (R.Specialized [p_string, p_string] (R.Name "map"))
   result  = "result"

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
   (tag -> TBool)     -> return to_string
   (tag -> TByte)     -> return to_string
   (tag -> TInt)      -> return to_string
   (tag -> TReal)     -> return to_string
   (tag -> TString)   -> return e
   (tag -> TAddress)  -> return $ R.Call (R.Variable $ R.Qualified (R.Name "K3") (R.Name "addressAsString")) [e]
   (tag -> TFunction) -> return $ lit_string "<opaque_function>"
   -- nested:
   ((tag &&& children) -> (TOption, [t]))      -> opt t
   ((tag &&& children) -> (TIndirection, [t])) -> ind_to_string t
   ((tag &&& children) -> (TTuple, ts))        -> tup_to_string ts
   ((tag &&& children) -> (TRecord ids, ts))   -> rec_to_string ids ts
   ((tag &&& children) -> (TCollection, [et])) -> coll_to_string base_t et
   _                                           -> return $ lit_string "Cant Show!"
 where
   -- Utils
   singleton = replicate 1
   lit_string  = R.Literal . R.LString
   std_string s = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "string")) [lit_string s]
   wrap stmnts cap expr t = R.Call (R.Lambda cap [("x", t)] False Nothing stmnts) [expr]
   to_string = R.Call (R.Variable (R.Qualified (R.Name "std") (R.Name "to_string"))) [e]
   stringConcat = R.Binary "+"
   ossConcat = R.Binary "<<"
   get_tup i expr = R.Call (R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")) [expr]
   project field n = R.Project n (R.Name field)

   -- Option
   opt ct = do
       cType <- genCType base_t
       inner <- prettifyExpr ct (R.Dereference (R.Variable $ R.Name "x"))
       return $ wrap (singleton $ R.IfThenElse (R.Variable $ R.Name "x") [R.Return $ stringConcat (std_string "Some ") inner] [R.Return $ std_string "None"]) [] e (R.Const $ R.Reference cType)
   -- Indirection
   ind_to_string ct = do
       inner <- prettifyExpr ct (R.Dereference e)
       return $ stringConcat (lit_string "Ind ") inner
   -- Tuple
   tup_to_string cts = do
       ct_is  <- return $ zip cts ([0..] :: [Integer])
       cs     <- mapM (\(ct,i) -> prettifyExpr ct (get_tup i e)) ct_is --show each element in tuple
       commad <- return $ L.intersperse (lit_string ",") cs -- comma seperate
       return $ stringConcat (foldl stringConcat (lit_string "(") commad) (lit_string ")") -- stringConcat
   -- Record
   rec_to_string ids cts = do
       ct_ids <- return $ zip cts ids
       cs     <- mapM (\(ct,field) -> prettifyExpr ct (project field e) >>= \v -> return $ stringConcat (std_string $ field ++ ":") v) ct_ids
       done   <- return $ L.intersperse (lit_string ",") cs
       return $ stringConcat (foldl stringConcat (lit_string "{") done) (lit_string "}")
   -- Collection
   coll_to_string t et = do
       cColType <- genCType t
       cEType <- genCType et
       rvar <- return $ R.ScalarDecl (R.Name "oss") (R.Named $ R.Qualified (R.Name "std") (R.Name "ostringstream")) Nothing
       e_name <- return $ R.Name "elem"
       v    <- prettifyExpr et (R.Variable e_name)
       svar <- return $ R.ScalarDecl (R.Name "s") (R.Primitive R.PString) (Just v)
       lambda_body <- return [R.Forward svar, R.Ignore $ R.Binary "<<" (R.Variable $ R.Name "oss") (ossConcat (R.Variable $ R.Name "s") $ lit_string ","), R.Return $ R.Initialization (R.Named $ R.Name "unit_t") []]
       fun <- return $ R.Lambda
                [R.RefCapture (Just ("oss", Nothing))]
                [("elem", R.Const $ R.Reference cEType)] False Nothing lambda_body
       iter <- return $ R.Call (R.Project (R.Variable $ R.Name "x") (R.Name "iterate")) [fun]
       result <- return $ R.Return $ stringConcat (lit_string "[") (R.Call (R.Project (R.Variable $ R.Name "oss") (R.Name "str")) [])
       -- wrap in lambda, then call it
       return $ wrap [R.Forward rvar, R.Ignore iter, result] [] e (cColType)
