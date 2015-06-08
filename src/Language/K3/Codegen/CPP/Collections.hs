{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Collections where

import Data.Char
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

    let baseClass (n,_) = R.Qualified (R.Name "K3")
                          (R.Specialized ((R.Named $ R.Name "__CONTENT"): addnSpecializations n) (R.Name n))

    let baseClasses = map baseClass ras

    let selfType = R.Named $ R.Specialized [R.Named $ R.Name "__CONTENT"] $ R.Name name

    let defaultConstructor
            = R.FunctionDefn (R.Name name) [] Nothing [R.Call (R.Variable b) [] | b <- baseClasses] False []

    let superConstructor = R.FunctionDefn (R.Name name)
                           [ ("__other" ++ show i, R.Const $ R.Reference $ R.Named b)
                           | (b,i) <- zip baseClasses ([1..] :: [Integer])
                           ] Nothing
                           [ R.Call (R.Variable b)
                             [ R.Variable $ R.Name $ "__other" ++ show i]
                             | (b,i) <- zip baseClasses ([1..] :: [Integer])
                           ] False []

    let superMoveConstructor
            = R.FunctionDefn (R.Name name)
              [ ("__other" ++ show i, R.RValueReference $ R.Named b)
              | (b,i) <- zip baseClasses ([1..] :: [Integer])
              ] Nothing
              [ R.Call
                (R.Variable b)
                [ R.Call
                      (R.Variable $ R.Qualified (R.Name "std") (R.Name "move"))
                      [R.Variable $ R.Name $ "__other" ++ show i]
                ]
              | (b,i) <- zip baseClasses ([1..] :: [Integer])
              ] False []

    let mkXmlTagName s = map (\c -> if isAlphaNum c || c `elem` ['-','_','.'] then c else '_') s
    let serializationName asYas n =
          if asYas then R.Qualified (R.Name "yas") n
          else R.Qualified (R.Name "boost") $ R.Qualified (R.Name "serialization") n

    let serializeParent asYas (p, (q, _)) =
          let nvp_wrap e = if asYas then e
                           else R.Call (R.Variable $ serializationName asYas $ R.Name "make_nvp")
                                  [ R.Literal $ R.LString $ mkXmlTagName q, e ]
          in
          R.Ignore $ R.Binary "&" (R.Variable $ R.Name "_archive")
            (nvp_wrap $ R.Call (R.Variable $ serializationName asYas $ R.Specialized [R.Named p] $ R.Name "base_object")
              [R.Dereference $ R.Variable $ R.Name "this"])

    let serializeStatements asYas = map (serializeParent asYas) $ zip baseClasses ras

    let serializeFn asYas =
          R.TemplateDefn [("archive", Nothing)]
            (R.FunctionDefn (R.Name "serialize")
              ([("_archive", R.Reference (R.Parameter "archive"))]
               ++ (if asYas then [] else [ ("_version", R.Const $ R.Named (R.Name "unsigned int")) ]))
             (Just $ R.Named $ R.Name "void")
             [] False $ serializeStatements asYas)

    let methods = [defaultConstructor, superConstructor, superMoveConstructor, serializeFn False, serializeFn True] ++ indexDefns

    let collectionClassDefn = R.TemplateDefn [("__CONTENT", Nothing)]
             (R.ClassDefn (R.Name name) [] (map R.Named baseClasses) methods [] [])

    let parent = head baseClasses

    let compactSerializationDefn
            = R.NamespaceDefn "boost" [ R.NamespaceDefn "serialization" [
                R.TemplateDefn [("__CONTENT", Nothing)] $
                R.ClassDefn (R.Name "implementation_level") [selfType] []
                [ R.TypeDefn (R.Named $ R.Qualified (R.Name "mpl") $ R.Name "integral_c_tag") "tag"
                , R.TypeDefn (R.Named $ R.Qualified (R.Name "mpl")
                    $ R.Specialized [R.Named $ R.Name "object_serializable"] $ R.Name "int_") "type"
                , R.GlobalDefn $ R.Ignore $ R.Call (R.Variable $ R.Name "BOOST_STATIC_CONSTANT")
                    [ R.Variable $ R.Name "int"
                    , R.Binary "=" (R.Variable $ R.Name "value")
                        (R.Variable $ R.Qualified (R.Name "implementation_level") $ R.Qualified (R.Name "type") $ R.Name "value")]
                ]
                [] []
              ]]

    let yamlStructDefn = R.NamespaceDefn "YAML"
                         [ R.TemplateDefn [("__CONTENT", Nothing)] $
                            R.ClassDefn (R.Name "convert") [selfType] []
                             [ R.FunctionDefn (R.Name "encode")
                                 [("c", R.Const $ R.Reference $ selfType)]
                                 (Just $ R.Static $ R.Named $ R.Name "Node") [] False
                                 [R.Return $ R.Call
                                       (R.Variable $ R.Qualified
                                             (R.Specialized [R.Named $ parent] (R.Name "convert"))
                                             (R.Name "encode"))
                                       [R.Variable $ R.Name "c"]]
                             , R.FunctionDefn (R.Name "decode")
                                 [ ("node", R.Const $ R.Reference $ R.Named $ R.Name "Node")
                                 , ("c", R.Reference selfType)
                                 ] (Just $ R.Static $ R.Primitive $ R.PBool) [] False
                                 [R.Return $ R.Call
                                       (R.Variable $ R.Qualified
                                             (R.Specialized [R.Named $ parent] (R.Name "convert"))
                                             (R.Name "decode"))
                                       [R.Variable $ R.Name "node", R.Variable $ R.Name "c"]]
                             ]
                             [] []
                         ]

    let jsonStructDefn = R.NamespaceDefn "JSON"
                         [ R.TemplateDefn [("__CONTENT", Nothing)] $
                            R.ClassDefn (R.Name "convert") [selfType] []
                             [ R.TemplateDefn [("Allocator", Nothing)] $
                               R.FunctionDefn (R.Name "encode")
                                 [("c", R.Const $ R.Reference $ selfType)
                                 ,("al", R.Reference $ R.Named $ R.Name "Allocator")
                                 ]
                                 (Just $ R.Static $ R.Named $ R.Name "rapidjson::Value") [] False
                                 [R.Return $ R.Call
                                       (R.Variable $ R.Qualified
                                             (R.Specialized [R.Named $ parent] (R.Name "convert"))
                                             (R.Name "encode"))
                                       [R.Variable $ R.Name "c",
                                        R.Variable $ R.Name "al"]
                                ]
                             ]
                             [] []
                         ]
    return [collectionClassDefn, compactSerializationDefn, yamlStructDefn, jsonStructDefn]

record :: [Identifier] -> CPPGenM [R.Definition]
record (sort -> ids) = do
    let recordName = "R_" ++ intercalate "_" ids
    let templateVars = ["_T" ++ show n | _ <- ids | n <- [0..] :: [Int]]
    let formalVars = ["_" ++ i | i <- ids]

    let recordType = R.Named $ R.Specialized [R.Named $ R.Name t | t <- templateVars] $ R.Name recordName

    let defaultConstructor
            = R.FunctionDefn (R.Name recordName) [] Nothing
              [R.Call (R.Variable $ R.Name i) [] | i <- ids] False []

    -- Forwarding constructor. One should be sufficient to handle all field-based constructions.

    let forwardTemplateVars = map ('_':) templateVars

    let init1Const fv tv i = R.FunctionDefn (R.Name recordName) [(fv, R.Const $ R.Reference $ R.Named $ R.Name tv)]
                             Nothing [R.Call (R.Variable $ R.Name i) [R.Variable $ R.Name fv]] False []

    let init1Move fv tv i = R.FunctionDefn (R.Name recordName) [(fv, R.RValueReference $ R.Named $ R.Name tv)]
                            Nothing [R.Call (R.Variable $ R.Name i)
                                          [R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "move"))
                                                [R.Variable $ R.Name fv]]] False []

    let initConstructor
            = R.TemplateDefn (zip forwardTemplateVars $ repeat Nothing) $
              R.FunctionDefn (R.Name recordName)
              [(fv, R.RValueReference $ R.Named $ R.Name tv) | fv <- formalVars | tv <- forwardTemplateVars] Nothing
              [ R.Call (R.Variable $ R.Name i)
                           [R.Call (R.Variable $ R.Qualified (R.Name "std")
                                         (R.Specialized [R.Named $ R.Name t] (R.Name "forward")))
                            [R.Variable $ R.Name f]]
              | i <- ids
              | f <- formalVars
              | t <- forwardTemplateVars
              ] False []

    let initConstructors = case (formalVars, templateVars, ids) of
                             ([fv], [tv], [i]) -> [init1Const fv tv i, init1Move fv tv i]
                             _ -> [initConstructor]
    let equalityOperator
            = R.FunctionDefn (R.Name "operator==")
              [("__other", R.Const $ R.Reference recordType)] (Just $ R.Primitive R.PBool) []
              True
              [R.Return $ foldr1 (R.Binary "&&")
                    [ R.Binary "==" (R.Variable $ R.Name i) (R.Project (R.Variable $ R.Name "__other") (R.Name i))
                    | i <- ids
                    ]]

    let tieSelf  = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "tie")) [R.Variable $ R.Name i | i <- ids]
    let tieOther n = R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "tie"))
                     [R.Project (R.Variable $ R.Name n) (R.Name i) | i <- ids]

    let logicOp op
            = R.FunctionDefn (R.Name $ "operator"++op)
              [("__other", R.Const $ R.Reference recordType)] (Just $ R.Primitive R.PBool)
              [] True
              [R.Return $ R.Binary op tieSelf (tieOther "__other")]

    let fieldDecls = [ R.GlobalDefn (R.Forward $ R.ScalarDecl (R.Name i) (R.Named $ R.Name t) Nothing)
                     | i <- ids
                     | t <- templateVars
                     ]

    let serializeMember asYas m =
          R.Ignore $ R.Binary "&" (R.Variable $ R.Name "_archive")
            (if asYas then R.Variable $ R.Name m
             else R.Call (R.Variable $ R.Name "BOOST_SERIALIZATION_NVP") [R.Variable $ R.Name m])

    let serializeStatements asYas = map (serializeMember asYas) ids

    let serializeFn asYas =
          R.TemplateDefn [("archive", Nothing)]
            (R.FunctionDefn (R.Name "serialize")
                  ([ ("_archive", R.Reference (R.Parameter "archive")) ]
                   ++ if asYas then [] else [ ("_version", R.Const $ R.Named (R.Name "unsigned int")) ])
                  (Just $ R.Named $ R.Name "void")
                  [] False $ serializeStatements asYas)

    let constructors = (defaultConstructor:initConstructors)
    let comparators = [equalityOperator, logicOp "!=", logicOp "<", logicOp ">", logicOp "<=", logicOp ">="]
    let members = constructors ++ comparators ++ [serializeFn False, serializeFn True] ++ fieldDecls

    let recordStructDefn
            = R.GuardedDefn ("K3_" ++ recordName) $
                R.TemplateDefn (zip templateVars (repeat Nothing)) $
                 R.ClassDefn (R.Name recordName) [] [] members [] []

    let compactSerializationDefn
            = R.GuardedDefn ("K3_" ++ recordName ++ "_srimpl_lvl") $ R.NamespaceDefn "boost" [ R.NamespaceDefn "serialization" [
                R.TemplateDefn (zip templateVars (repeat Nothing)) $
                R.ClassDefn (R.Name "implementation_level") [recordType] []
                [ R.TypeDefn (R.Named $ R.Qualified (R.Name "mpl") $ R.Name "integral_c_tag") "tag"
                , R.TypeDefn (R.Named $ R.Qualified (R.Name "mpl")
                    $ R.Specialized [R.Named $ R.Name "object_serializable"] $ R.Name "int_") "type"
                , R.GlobalDefn $ R.Ignore $ R.Call (R.Variable $ R.Name "BOOST_STATIC_CONSTANT")
                    [ R.Variable $ R.Name "int"
                    , R.Binary "=" (R.Variable $ R.Name "value")
                        (R.Variable $ R.Qualified (R.Name "implementation_level") $ R.Qualified (R.Name "type") $ R.Name "value")]
                ]
                [] []
              ]]

    {-
    let noTrackingDefn
            = R.GuardedDefn ("K3_" ++ recordName ++ "_srtrck_lvl") $ R.NamespaceDefn "boost" [ R.NamespaceDefn "serialization" [
                R.TemplateDefn (zip templateVars (repeat Nothing)) $
                R.ClassDefn (R.Name "tracking_level") [recordType] []
                [ R.TypeDefn (R.Named $ R.Qualified (R.Name "mpl") $ R.Name "integral_c_tag") "tag"
                , R.TypeDefn (R.Named $ R.Qualified (R.Name "mpl")
                    $ R.Specialized [R.Named $ R.Name "track_never"] $ R.Name "int_") "type"
                , R.GlobalDefn $ R.Ignore $ R.Call (R.Variable $ R.Name "BOOST_STATIC_CONSTANT")
                    [ R.Variable $ R.Name "int"
                    , R.Binary "=" (R.Variable $ R.Name "value")
                        (R.Variable $ R.Qualified (R.Name "tracking_level") $ R.Qualified (R.Name "type") $ R.Name "value")]
                ]
                [] []
              ]]

    let bitwiseSerializableDefn
            = R.GuardedDefn ("K3_" ++ recordName ++ "_srbitwise") $ R.NamespaceDefn "boost" [ R.NamespaceDefn "serialization" [
                R.TemplateDefn (zip templateVars (repeat Nothing)) $
                R.ClassDefn (R.Name "is_bitwise_serializable")
                [recordType] [R.Named $ R.Qualified (R.Name "mpl") $ R.Name "true_"]
                [] [] []
              ]]
    -}

    let hashStructDefn
            = R.GuardedDefn ("K3_" ++ recordName ++ "_hash_value") $ R.TemplateDefn (zip templateVars (repeat Nothing)) $
                R.FunctionDefn (R.Name "hash_value")
                  [("r", R.Const $ R.Reference recordType)]
                  (Just $ R.Named $ R.Qualified (R.Name "std") (R.Name "size_t"))
                  [] False [ R.Forward $ R.ScalarDecl (R.Name "hasher")
                             (R.Named $ R.Qualified (R.Name "boost")
                              (R.Specialized [R.Tuple [R.Named $ R.Name t | t <- templateVars]]
                                    (R.Name "hash"))) Nothing
                           , R.Return $ R.Call (R.Variable $ R.Name "hasher") [tieOther "r"]
                           ]

    let yamlStructDefn = R.NamespaceDefn "YAML"
                         [ R.TemplateDefn (zip templateVars (repeat Nothing)) $
                            R.ClassDefn (R.Name "convert") [recordType] []
                             [ R.FunctionDefn (R.Name "encode")
                                 [("r", R.Const $ R.Reference $ recordType)]
                                 (Just $ R.Static $ R.Named $ R.Name "Node") [] False
                                 ([R.Forward $ R.ScalarDecl (R.Name "node") (R.Named $ R.Name "Node") Nothing] ++
                                  [R.Assignment (R.Subscript
                                                      (R.Variable $ R.Name "node")
                                                      (R.Literal $ R.LString field))
                                                (R.Call
                                                      (R.Variable $ R.Qualified (R.Specialized [R.Named $ R.Name fieldType] $ R.Name "convert") (R.Name "encode"))
                                                      [R.Project (R.Variable $ R.Name "r") (R.Name field)])
                                  | field <- ids | fieldType <- templateVars
                                  ] ++ [R.Return $ R.Variable $ R.Name "node"])
                             , R.FunctionDefn (R.Name "decode")
                                 [ ("node", R.Const $ R.Reference $ R.Named $ R.Name "Node")
                                 , ("r", R.Reference recordType)
                                 ] (Just $ R.Static $ R.Primitive $ R.PBool) [] False
                                 ([ R.IfThenElse (R.Unary "!" $ R.Call (R.Project
                                                                            (R.Variable $ R.Name "node")
                                                                            (R.Name "IsMap")) [])
                                     [R.Return $ R.Literal $ R.LBool False] []
                                 ] ++
                                 [ R.IfThenElse (R.Subscript
                                                      (R.Variable $ R.Name "node")
                                                      (R.Literal $ R.LString field))
                                     [ R.Assignment
                                         (R.Project (R.Variable $ R.Name "r") (R.Name field))
                                         (R.Call (R.Project
                                                   (R.Subscript
                                                         (R.Variable $ R.Name "node")
                                                         (R.Literal $ R.LString field))
                                                   (R.Specialized [R.Named $ R.Name templateVar] $ R.Name "as")) [])
                                     ] []
                                 | field <- ids | templateVar <- templateVars
                                 ] ++ [R.Return $ R.Literal $ R.LBool True])
                             ]
                             [] []
                         ]

    let addField (name,typ) = R.Call (R.Project (R.Variable $ R.Name "inner") (R.Name "AddMember"))
                                [ R.Literal $ R.LString name
                                , R.Call (R.Variable $ R.Qualified (R.Specialized [R.Named $ R.Name typ] (R.Name "convert")) (R.Name "encode"))
                                   [R.Project (R.Variable $ R.Name "r") (R.Name name), R.Variable $ R.Name "al"]
                                , R.Variable $ R.Name "al"
                                ]


    let jsonStructDefn =   R.NamespaceDefn "JSON"
                           [R.TemplateDefn (zip templateVars (repeat Nothing)) $
                           R.ClassDefn (R.Name "convert") [recordType] []
                           [
                           R.TemplateDefn [("Allocator", Nothing)] (R.FunctionDefn (R.Name "encode")
                           [("r", R.Const $ R.Reference recordType)
                           ,("al", R.Reference $ R.Named $ R.Name "Allocator")
                           ]
                           (Just $ R.Static $ R.Named $ R.Name "rapidjson::Value") [] False
                           (
                            [R.Forward $ R.UsingDecl (Left (R.Name "rapidjson")) Nothing] ++
                            [R.Forward $ R.ScalarDecl (R.Name "val") (R.Named $ R.Name "Value") Nothing] ++
                            [R.Forward $ R.ScalarDecl (R.Name "inner") (R.Named $ R.Name "Value") Nothing ] ++
                            [R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "val") (R.Name "SetObject")) [] ] ++
                            [R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "inner") (R.Name "SetObject")) [] ] ++
                            [R.Ignore $ addField tup | tup <- zip ids templateVars] ++
                            [R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "val") (R.Name "AddMember"))
                              [R.Literal $ R.LString "type"
                              , R.Call (R.Variable $ R.Name "Value")
                                [R.Literal $ R.LString "record"]
                              , R.Variable $ R.Name "al"
                              ]
                            ] ++
                            [R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "val") (R.Name "AddMember"))
                              [R.Literal $ R.LString "value"
                              , R.Call
                                  (R.Project (R.Variable $ R.Name "inner") (R.Name "Move"))
                                  []
                              , R.Variable $ R.Name "al"
                              ]
                            ] ++
                            [R.Return $ R.Variable $ R.Name "val"]
                           ))
                            ] [] []
                            ]
    return [ recordStructDefn, compactSerializationDefn {-, noTrackingDefn, bitwiseSerializableDefn-}
           , jsonStructDefn, yamlStructDefn, hashStructDefn]

reservedAnnotations :: [Identifier]
reservedAnnotations = ["Collection", "External", "Seq", "Set", "Sorted", "Map", "IntMap", "StrMap", "Vector", "MultiIndex", "VMap"]
