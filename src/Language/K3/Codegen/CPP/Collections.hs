{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Collections where

import Data.List (intercalate, partition, sort, isInfixOf)

import Language.K3.Core.Common
import Language.K3.Core.Declaration

import Language.K3.Codegen.CPP.Types
import Language.K3.Codegen.CPP.MultiIndex (indexes)

import qualified Language.K3.Codegen.CPP.Representation as R

-- | An Annotation Combination Composite should contain the following:
--  - Inlined implementations for all provided methods.
--  - Declarations for all provided data members.
--  - Declaration for the dataspace of the collection.
--  - Implementations for at least the following constructors:
--      - Default constructor, which creates an empty collection.
--      - Dataspace constructor, which creates a collection from an empty dataspace (e.g. vector).
--          - Additionally a move dataspace constructor which uses a temporary dataspace?
--      - Copy constructor.
--      - Superclass constructor.
--  - Serialization function, which should proxy the dataspace serialization.
composite :: Identifier -> [(Identifier, [AnnMemDecl])] -> CPPGenM [R.Definition]
composite name ans = do
    let (ras, as) = partition (\(aname, _) -> aname `elem` reservedAnnotations) ans

    -- Inlining is only done for provided (positive) declarations.
    -- let positives = filter isPositiveDecl (concat . snd $ unzip nras)

    -- Split data and method declarations, for access specifiers.
    -- let (dataDecls, methDecls) = partition isDataDecl positives

    -- When dealing with Indexes, we need to specialize the MultiIndex class on each index type
    (indexTypes, indexDefns) <- indexes name as

    let addnSpecializations n = if "MultiIndex" `isInfixOf` n  then indexTypes else []

    let baseClass (n,_) = R.Qualified (R.Name "K3") (R.Specialized ((R.Named $ R.Name "__CONTENT") :  addnSpecializations n ) (R.Name n) )
    let baseClasses = map baseClass ras

    let selfType = R.Named $ R.Specialized [R.Named $ R.Name "__CONTENT"] $ R.Name name

    let defaultConstructor = R.FunctionDefn (R.Name name) [] Nothing [R.Call (R.Variable b) [] | b <- baseClasses] []
    let copyConstructor = R.FunctionDefn (R.Name name) [("__other", R.Const $ R.Reference selfType)] Nothing
                          [R.Call (R.Variable b) [R.Variable $ R.Name "__other"] | b <- baseClasses] []

    let superConstructor = R.FunctionDefn (R.Name name)
                             [("__other" ++ show i, R.Const $ R.Reference $ R.Named b)  | (b,i) <- zip baseClasses ([1..] :: [Integer]) ]
                             Nothing
                             [R.Call (R.Variable b) [R.Variable $ R.Name $ "__other" ++ show i] | (b,i) <- zip baseClasses ([1..] :: [Integer]) ]                                []

    let superMoveConstructor = R.FunctionDefn (R.Name name)
                             [("__other" ++ show i, R.RValueReference $ R.Named b)  | (b,i) <- zip baseClasses ([1..] :: [Integer]) ]
                             Nothing
                             [R.Call
                               (R.Variable b)
                               [R.Call
                                   (R.Variable $ R.Qualified (R.Name "std") (R.Name "move"))
                                   [R.Variable $ R.Name $ "__other" ++ show i]
                               ]
                             | (b,i) <- zip baseClasses ([1..] :: [Integer])
                             ]
                             []
    let serializeParent p = R.Ignore $ R.Binary "&" (R.Variable $ R.Name "_archive")
                          (R.Call
                              (R.Variable $
                                R.Specialized [R.Named p]
                                  (R.Qualified (R.Name "boost") $
                                    R.Qualified (R.Name "serialization") $ R.Name "base_object"))
                              [R.Variable $ R.Name "*this"])

    let serializeStatements = map serializeParent baseClasses

    let serializeFn = R.TemplateDefn [("archive", Nothing)]
                      (R.FunctionDefn (R.Name "serialize")
                            [ ("_archive", R.Reference (R.Parameter "archive"))
                            , ("_version", R.Const $ R.Named (R.Name "unsigned int"))
                            ]
                       (Just $ R.Named $ R.Name "void")
                       [] serializeStatements)

    let patcherFnDefn
            = R.FunctionDefn (R.Name "patch") [("_input", R.Primitive R.PString), ("_c", R.Reference selfType)]
              (Just $ R.Named $ R.Name "static void") []
              [R.Ignore $
                R.Call (
                  R.Variable $ R.Qualified
                  (R.Specialized [R.Parameter name, R.Parameter "__CONTENT"] (R.Name "collection_patcher"))
                  (R.Name "patch"))
                     [R.Variable $ R.Name "_input", R.Variable $ R.Name "_c"]
              ]

    let patcherStructDefn
            = R.NamespaceDefn "K3" [
                      R.TemplateDefn [("__CONTENT", Nothing)]
                           (R.ClassDefn (R.Name "patcher") [selfType] [] [patcherFnDefn] [] [])
                     ]

    let addnDefns = indexDefns
    let methods = [defaultConstructor, copyConstructor, superConstructor, superMoveConstructor, serializeFn] ++ addnDefns

    let collectionClassDefn = R.TemplateDefn [("__CONTENT", Nothing)]
             (R.ClassDefn (R.Name name) [] (map R.Named baseClasses) methods [] [])

    return [collectionClassDefn, patcherStructDefn]

record :: [Identifier] -> CPPGenM [R.Definition]
record (sort -> ids) = do
    let recordName = "R_" ++ intercalate "_" ids
    let templateVars = ["_T" ++ show n | _ <- ids | n <- [0..] :: [Int]]
    let formalVars = ["_" ++ i | i <- ids]

    let recordType = R.Named $ R.Specialized [R.Named $ R.Name t | t <- templateVars] $ R.Name recordName

    let defaultConstructor
            = R.FunctionDefn (R.Name recordName) [] Nothing
              [R.Call (R.Variable $ R.Name i) [] | i <- ids] []

    -- Forwarding constructor. One should be sufficient to handle all field-based constructions.

    let forwardTemplateVars = map ('_':) templateVars

    let initArgsEnableIf fvs tvs
            = case (fvs, tvs) of
                ([fv], [tv]) -> [(fv, R.RValueReference $ R.Named $ R.Qualified (R.Name "std")
                      (R.Specialized [R.Named $ R.Name tv, recordType] (R.Name "is_unrelated_type")))]
                _ -> [(fv, R.RValueReference $ R.Named $ R.Name tv) | fv <- fvs | tv <- tvs]

    let initConstructor
            = R.TemplateDefn (zip forwardTemplateVars $ repeat Nothing) $
              R.FunctionDefn (R.Name recordName) (initArgsEnableIf formalVars forwardTemplateVars) Nothing
              [ R.Call (R.Variable $ R.Name i)
                           [R.Call (R.Variable $ R.Qualified (R.Name "std")
                                         (R.Specialized [R.Named $ R.Name t] (R.Name "forward")))
                            [(R.Variable $ R.Name f)]]
              | i <- ids
              | f <- formalVars
              | t <- forwardTemplateVars
              ] []

    let copyConstructor
            = R.FunctionDefn (R.Name recordName)
              [("__other", R.Const $ R.Reference recordType)] Nothing
              [R.Call (R.Variable $ R.Name i) [R.Project (R.Variable $ R.Name "__other") (R.Name i)] | i <-  ids] []

    let equalityOperator
            = R.FunctionDefn (R.Name "operator==")
              [("__other", R.Const $ R.Reference recordType)] (Just $ R.Primitive R.PBool) []
              [R.Return $ foldr1 (R.Binary "&&")
                    [ R.Binary "==" (R.Variable $ R.Name i) (R.Project (R.Variable $ R.Name "__other") (R.Name i))
                    | i <- ids
                    ]]

    let tieSelf  = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "tie")) [R.Variable $ R.Name i | i <- ids]
    let tieOther n = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "tie")) [R.Project (R.Variable $ R.Name n) (R.Name i) | i <- ids]

    let inequalityOperator
            = R.FunctionDefn (R.Name "operator!=")
              [("__other", R.Const $ R.Reference recordType)] (Just $ R.Primitive R.PBool) []
              [R.Return $ R.Binary "!=" tieSelf (tieOther "__other")]

    let lessOperator
            = R.FunctionDefn (R.Name "operator<")
              [("__other", R.Const $ R.Reference recordType)] (Just $ R.Primitive R.PBool) []
              [R.Return $ R.Binary "<" tieSelf (tieOther "__other")]

    let greaterOperator
            = R.FunctionDefn (R.Name "operator>")
              [("__other", R.Const $ R.Reference recordType)] (Just $ R.Primitive R.PBool) []
              [R.Return $ R.Binary ">" tieSelf (tieOther "__other")]

    let fieldDecls = [ R.GlobalDefn (R.Forward $ R.ScalarDecl (R.Name i) (R.Named $ R.Name t) Nothing)
                     | i <- ids
                     | t <- templateVars
                     ]
    let serializeMember m = R.Ignore $ R.Binary "&" (R.Variable $ R.Name "_archive") (R.Variable $ R.Name m)

    let serializeStatements = map serializeMember ids

    let serializeFn = R.TemplateDefn [("archive", Nothing)]
                      (R.FunctionDefn (R.Name "serialize")
                            [ ("_archive", R.Reference (R.Parameter "archive"))
                            , ("_version", R.Const $ R.Named (R.Name "unsigned int"))
                            ]
                       (Just $ R.Named $ R.Name "void")
                       [] serializeStatements)

    let operators = [equalityOperator, inequalityOperator, lessOperator, greaterOperator]
    let members = [defaultConstructor, initConstructor, copyConstructor, serializeFn] ++ operators ++ fieldDecls

    let recordStructDefn
            = R.GuardedDefn ("K3_" ++ recordName ++ "_hash_value") $
                R.TemplateDefn (zip templateVars (repeat Nothing)) $ R.ClassDefn (R.Name recordName) [] [] members [] []

    let shallowDecl = R.Forward $ R.ScalarDecl (R.Name "_shallow")
                      (R.Named $ R.Specialized [R.Named $ R.Qualified (R.Name "string") (R.Name "iterator")]
                            (R.Name "shallow")) Nothing

    let doPatchInvocation f = R.Call (R.Variable $ R.Name "do_patch")
                              [R.Variable $ R.Name "_partial", R.Project (R.Variable $ R.Name "_record") (R.Name f)]

    let oneFieldParserDefn f
            = foldl1 (R.Binary ">>")
              [ R.Call (R.Variable $ R.Qualified (R.Name "qi") (R.Name "lit")) [R.Literal $ R.LString f]
              , R.Literal (R.LChar ':')
              , R.Variable (R.Name "_shallow")
              ]

    let oneFieldParserAction f
            = R.Lambda [R.RefCapture (Just  ("_record", Nothing))]
              [("_partial", R.Primitive R.PString)] False Nothing
              [R.Ignore $ doPatchInvocation f]

    let oneFieldParserDecl f
            = R.Forward $ R.ScalarDecl (R.Name $ "_" ++ f)
              (R.Named $ R.Specialized
                    [ R.Named $ R.Qualified (R.Name "string") (R.Name "iterator")
                    , R.Named $ R.Qualified (R.Name "qi") (R.Name "space_type")
                    ]
               (R.Qualified (R.Name "qi") (R.Name "rule")))
              (Just $ R.Subscript (oneFieldParserDefn f) (oneFieldParserAction f))

    let allFieldParserDecls = map oneFieldParserDecl ids

    let fieldParserDecl
            = R.Forward $ R.ScalarDecl (R.Name "_field")
              (R.Named $ R.Specialized
                    [ R.Named $ R.Qualified (R.Name "string") (R.Name "iterator")
                    , R.Named $ R.Qualified (R.Name "qi") (R.Name "space_type")
                    ]
               (R.Qualified (R.Name "qi") (R.Name "rule")))
              (Just $ foldl1 (R.Binary "|") [R.Variable (R.Name $ "_" ++ f) | f <- ids])

    let recordParserDecl
            = R.Forward $ R.ScalarDecl (R.Name "_parser")
              (R.Named $ R.Specialized
                    [ R.Named $ R.Qualified (R.Name "string") (R.Name "iterator")
                    , R.Named $ R.Qualified (R.Name "qi") (R.Name "space_type")
                    ]
               (R.Qualified (R.Name "qi") (R.Name "rule")))
              (Just $ foldl1 (R.Binary ">>")
                                   [ R.Literal (R.LChar '{')
                                   , R.Binary "%" (R.Variable $ R.Name "_field") (R.Literal $ R.LChar ',')
                                   , R.Literal (R.LChar '}')
                                   ])

    let parseInvocation
            = R.Call (R.Variable $ R.Qualified (R.Name "qi") (R.Name "phrase_parse"))
              [ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "begin")) [R.Variable $ R.Name "_input"]
              , R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [R.Variable $ R.Name "_input"]
              , R.Variable (R.Name "_parser")
              , R.Variable (R.Qualified (R.Name "qi") $ R.Name "space")
              ]

    let patcherFnDefn
            = R.FunctionDefn (R.Name "patch") [("_input", R.Primitive R.PString), ("_record", R.Reference recordType)]
              (Just $ R.Named $ R.Name "static void") []
              ([shallowDecl] ++ allFieldParserDecls ++ [fieldParserDecl, recordParserDecl, R.Ignore parseInvocation])

    let patcherStructDefn
            = R.NamespaceDefn "K3" [
                        R.TemplateDefn (zip templateVars (repeat Nothing))
                             (R.ClassDefn (R.Name "patcher") [recordType] [] [patcherFnDefn] [] [])
                             ]
    let hashStructDefn
            = R.GuardedDefn ("K3_" ++ recordName) $ R.TemplateDefn (zip templateVars (repeat Nothing)) $
                R.FunctionDefn (R.Name "hash_value")
                  [("r", R.Const $ R.Reference recordType)] (Just $ R.Named $ R.Qualified (R.Name "std") (R.Name "size_t")) []
                      [R.Forward $ R.ScalarDecl (R.Name "hasher")
                        (R.Named $ R.Qualified (R.Name "boost") (R.Specialized [R.Tuple [R.Named $ R.Name t | t <- templateVars]] (R.Name "hash"))) Nothing,
                       R.Return $ R.Call (R.Variable $ R.Name "hasher") [tieOther "r"]]


    return [recordStructDefn, patcherStructDefn, hashStructDefn]

reservedAnnotations :: [Identifier]
reservedAnnotations = ["Collection", "External", "Seq", "Set", "Sorted", "Map", "Vector", "MultiIndex"]
