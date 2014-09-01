{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Collections where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Functor
import Data.List (intercalate, nub, partition, sort)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Codegen.Common

import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Expression
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

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
--  - Serialization function, which should proxy the dataspace serialization.
composite :: Identifier -> [(Identifier, [AnnMemDecl])] -> CPPGenM [R.Definition]
composite name ans = do
    let (ras, _) = partition (\(aname, _) -> aname `elem` reservedAnnotations) ans

    -- Inlining is only done for provided (positive) declarations.
    -- let positives = filter isPositiveDecl (concat . snd $ unzip nras)

    -- Split data and method declarations, for access specifiers.
    -- let (dataDecls, methDecls) = partition isDataDecl positives

    let baseClasses = map (R.Specialized [R.Named $ R.Name "__CONTENT"] . R.Name . fst) ras

    let selfType = R.Named $ R.Specialized [R.Named $ R.Name "__CONTENT"] $ R.Name name

    let defaultConstructor = R.FunctionDefn (R.Name name) [] Nothing [R.Call (R.Variable b) [] | b <- baseClasses] []
    let copyConstructor = R.FunctionDefn (R.Name name) [("__other", R.Const $ R.Reference selfType)] Nothing
                          [R.Call (R.Variable b) [R.Variable $ R.Name "__other"] | b <- baseClasses] []

    let methods = [defaultConstructor, copyConstructor]

    return [R.TemplateDefn [("__CONTENT", Nothing)]
             (R.ClassDefn (R.Name name) [] (map R.Named baseClasses) methods [] [])]
  where

--     let parentSerializeCall p = text "_archive" <+> text "&" <+>
--                                 genCQualify (genCQualify (text "boost") (text "serialization"))
--                                   (genCCall (text "base_object")
--                                   (Just [genCQualify (text "K3") (text p) <> angles (text "CONTENT")])
--                                   [text "*this"]) <> semi
--     let dataSerialize = vsep $ map (parentSerializeCall . fst) ras

--     -- Generate a serialization method based on engine preferences.
--     serializeBody <- serializationMethod <$> get >>= \case
--         BoostSerialization -> return $ genCBoostSerialize $ map (\(Lifted _ i _ _ _) -> i) dataDecls

--     let serializeDefn = genCFunction
--                           (Just [text "archive"])
--                           (text "void")
--                           (text "serialize")
--                           [text "archive& _archive", text "const unsigned int"]
--                           (serializeBody <$$> dataSerialize)

--     let ps = punctuate comma $ map (\(fst -> p) -> genCQualify (text "K3") $ text p <> angles (text "CONTENT")) ras

--     constructors' <- mapM ($ ps) constructors

--     let constructors'' = constructors' ++ [sc p | p <- take 1 ps, sc <- superConstructors]

--     aoperators' <- mapM ($ ps) assignOperators

--     pubDecls <- mapM annMemDecl methDecls
--     prvDecls <- mapM annMemDecl dataDecls

--     let classBody = text "class" <+> text className <> colon <+> hsep (map (text "public" <+>) ps)
--             <+> hangBrace (text "public:"
--             <$$> indent 4 (vsep $ punctuate line $ constructors'' ++ aoperators' ++ [serializeDefn] ++ pubDecls)
--             <$$> vsep [text "private:" <$$> indent 4 (vsep $ punctuate line prvDecls) | not (null prvDecls)]
--             ) <> semi

--     let classDefn = genCTemplateDecl [text "CONTENT"] <$$> classBody

--     addForward $ genCTemplateDecl [text "CONTENT"] <+> text "class" <+> text className <> semi

--     return $ defineProtect classDefn <$$> patcherSpec

--   where
--     sigil = hcat (punctuate (text "_") [text "K3", text className])
--     defineProtect t = text "#ifndef" <+> sigil <$$> text "#define" <+> sigil <$$> t <$$> text "#endif //" <+> sigil

--     isPositiveDecl :: AnnMemDecl -> Bool
--     isPositiveDecl (Lifted Provides _ _ _ _) = True
--     isPositiveDecl (Attribute Provides _ _ _ _) = True
--     isPositiveDecl (MAnnotation Provides _ _) = True
--     isPositiveDecl _ = False

--     isDataDecl :: AnnMemDecl -> Bool
--     isDataDecl (Lifted _ _ (tag -> TFunction) _ _) = False
--     isDataDecl (Attribute _ _ (tag -> TFunction) _ _) = False
--     isDataDecl _ = True

--     defaultConstructor ps = return $
--             text className
--          <> parens empty
--          <> colon
--         <+> hsep (punctuate comma $ map (<> parens empty) ps) -- initialize superclasses
--         <+> braces empty

--     -- TODO: Generate copy statements for remaining data members.
--     copyConstructor ps = return $
--             text className
--          <> parens (text $ "const " ++ className ++ "& c")
--          <> colon
--         <+> hsep (punctuate comma $ map (<> parens (text "c")) ps) <+> braces empty

--     moveConstructor ps = return $
--             text className
--          <> parens (text $ className ++ "&& c")
--          <> colon
--         <+> hsep (punctuate comma $ map (<> parens (text "std::move(c)")) ps) <+> braces empty

--     superCopyConstructor p =
--             text className
--          <> parens (text "const" <+> p <> text "& c")
--          <> colon
--         <+> p <> parens (text "c") <+> braces empty

--     superMoveConstructor p =
--             text className
--          <> parens (p <> text "&& c")
--          <> colon
--         <+> p <> parens (text "std::move(c)") <+> braces empty

--     constructors = [defaultConstructor, copyConstructor, moveConstructor]
--     superConstructors = [superCopyConstructor, superMoveConstructor]

--     assignOperator ps = return $
--             text (className ++ "&")
--         <+> text "operator="
--          <> parens (text $ "const " ++ className ++ "& other")
--          <> hangBrace (vsep $
--               (map (<> text "::operator=(other);") ps)
--               ++ [text "return *this;"]
--             )

--     moveAssignOperator ps = return $
--             text (className ++ "&")
--         <+> text "operator="
--          <> parens (text $ "const " ++ className ++ "&& other")
--          <> hangBrace (vsep $
--               (map (<> text "::operator=(std::move(other));") ps)
--               ++ [text "return *this;"]
--             )

--     assignOperators = [assignOperator, moveAssignOperator]

--     patcherSpec :: CPPGenR
--     patcherSpec = text "namespace K3" <+> (hangBrace $ genCTemplateDecl [text "E"] <$$> patcherSpecStruct)

--     patcherSpecStruct :: CPPGenR
--     patcherSpecStruct = text "struct patcher"
--                      <> angles (text className <> angles (text "E")) <+> hangBrace patcherSpecMeth <> semi

--     patcherSpecMeth :: CPPGenR
--     patcherSpecMeth = text "static" <+> text "void" <+> text "patch"
--                    <> parens (cat $ punctuate comma [
--                               text "string s",
--                               text className <> angles (text "E") <> text "&" <+> text "c"
--                          ])
--                   <+> hangBrace subPatcherCall

--     subPatcherCall :: CPPGenR
--     subPatcherCall = genCQualify patcherStruct patcherCall <> semi

--     patcherStruct :: CPPGenR
--     patcherStruct = text "collection_patcher" <> angles (cat $ punctuate comma [text className, text "E"])

--     patcherCall = genCCall (text "patch") Nothing [text "s", text "c"]


-- dataspaceType :: CPPGenR -> CPPGenM CPPGenR
-- dataspaceType eType = return $ text "vector" <> angles eType

-- annMemDecl :: AnnMemDecl -> CPPGenM CPPGenR
-- annMemDecl (Lifted _ i t me _)  = do
--     memDefinition <- definition i t me
--     return $ genCTemplateDecl (map text typeVars) <$$> memDefinition
--   where
--     findTypeVars :: K3 Type -> [Identifier]
--     findTypeVars (tag -> TDeclaredVar v) = [v]
--     findTypeVars (children -> []) = []
--     findTypeVars (children -> ts) = concatMap findTypeVars ts

--     typeVars = nub $ findTypeVars t

-- annMemDecl (Attribute _ i _ _ _) = return $ text i
-- annMemDecl (MAnnotation _ i _) = return $ text i
    let serializeParent p = R.Ignore $ R.Binary "&" (R.Variable $ R.Name "_archive")
                          (R.Call
                              (R.Variable $
                                R.Specialized [R.Named $ R.Qualified (R.Name "K3") p]
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
                  (R.Specialized [R.Parameter name, R.Parameter "__CONTENT"] (R.Name "patch"))
                  (R.Name "collection_patcher"))
                     [R.Variable $ R.Name "_input", R.Variable $ R.Name "_c"]
              ]

    let patcherStructDefn
            = R.NamespaceDefn "K3" [
                      R.TemplateDefn [("__CONTENT", Nothing)]
                           (R.ClassDefn (R.Name "patcher") [selfType] [] [patcherFnDefn] [] [])
                     ]

    let methods = [defaultConstructor, copyConstructor, serializeFn]

    let collectionClassDefn = R.TemplateDefn [("__CONTENT", Nothing)]
             (R.ClassDefn (R.Name name) [] (map R.Named baseClasses) methods [] [])

    return [collectionClassDefn, patcherStructDefn]

record :: [Identifier] -> CPPGenM [R.Definition]
record (sort -> ids) = do
    let recordName = "R_" ++ intercalate "_" ids
    let templateVars = ["_T" ++ show n | _ <- ids | n <- [0..] :: [Int]]
    let formalVars = ["_" ++ i | i <- ids]

    let defaultConstructor
            = R.FunctionDefn (R.Name recordName) [] Nothing
              [R.Call (R.Variable $ R.Name i) [] | i <- ids] []

    let initConstructor
            = R.FunctionDefn (R.Name recordName)
              [(fv, R.Named $ R.Name tv) | fv <- formalVars | tv <- templateVars]
              Nothing [R.Call (R.Variable $ R.Name i) [R.Variable $ R.Name f] | i <- ids | f <- formalVars] []

    let recordType = R.Named $ R.Specialized [R.Named $ R.Name t | t <- templateVars] $ R.Name recordName

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

    let members = [defaultConstructor, initConstructor, copyConstructor, equalityOperator, serializeFn] ++ fieldDecls

    let recordStructDefn
            = R.TemplateDefn (zip templateVars (repeat Nothing)) $ R.ClassDefn (R.Name recordName) [] [] members [] []

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
            = R.Lambda [("_record", R.Call (R.Variable (R.Qualified (R.Name "std") (R.Name "ref")))
                                      [R.Variable $ R.Name "_record"])]
              [("_partial", R.Primitive R.PString)] Nothing
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
              (Just $ foldl1 (R.Binary "|") [R.Variable (R.Name $ "_" ++ f) | f <- "x": ids])

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

    return [recordStructDefn, patcherStructDefn]

--     serializer <- serializeDefn

--     let publicDefs = vsep $ constructors ++ [equalityOperator, serializer]
--     let privateDefs = vsep fields
--     let recordDefs = text "public:" <$$> indent 4 (publicDefs <$$> privateDefs)

--     let templateDecl = genCTemplateDecl templateVars
--     let recordStructDef = templateDecl <$$> text "class" <+> recordName <+> hangBrace recordDefs <> semi

--     recordPatcherDef <- patcherSpec

--     -- addForward $ templateDecl <+> text "class" <+> recordName <> semi

--     return $ defineProtect recordStructDef <$$> recordPatcherDef
--   where
--     sigil = hcat (punctuate (text "_") [text "K3", recordName])
--     defineProtect t = text "#ifndef" <+> sigil <$$> text "#define" <+> sigil <$$> t <$$> text "#endif //" <+> sigil

--     recordName = text $ recordSignature ids

--     oneFieldParser :: Identifier -> CPPGenM CPPGenR
--     oneFieldParser i = do
--         let keyParser = text "qi::lit" <> parens (dquotes $ text i)
--         let valParser = text "_shallow"
--         let fieldParser = parens $ keyParser <+> text ">>" <+> squotes (char ':') <+> text ">>" <+> valParser
--         let actCapture = text "&r"
--         let actArgs = text "string _string"
--         let actBody = genCCall (text "do_patch") Nothing [text "_string", text "r" <> dot <> text i] <> semi
--         let fieldAction = brackets $ parens $ brackets actCapture <+> parens actArgs <+> braces actBody

--         return $ genCDecl
--           (text "qi::rule<string::iterator, qi::space_type>")
--           (text "_" <> text i)
--           (Just $ fieldParser <> fieldAction)

--     anyFieldParser :: [Identifier] -> CPPGenM CPPGenR
--     anyFieldParser is
--       = return $ genCDecl (text "qi::rule<string::iterator, qi::space_type>")
--                  (text "_field")
--                  (Just $ cat $ punctuate (text " | ") [text "_" <> text i | i <- is])

--     allFieldParser :: CPPGenM CPPGenR
--     allFieldParser
--       = return $ genCDecl (text "qi::rule<string::iterator, qi::space_type>")
--                           (text "_parser")
--                           (Just $ text "'{' >> (_field % ',') >> '}'")

--     parserInvocation :: CPPGenM CPPGenR
--     parserInvocation
--       = return $ genCCall (text "qi::phrase_parse") Nothing
--                  [text "std::begin(s)", text "std::end(s)", text "_parser", text "qi::space"] <> semi

--     patcherSpec :: CPPGenM CPPGenR
--     patcherSpec = do
--         fps <- vsep <$> mapM oneFieldParser ids
--         afp <- anyFieldParser ids
--         lfp <- allFieldParser
--         piv <- parserInvocation
--         let templateVars = [text "_T" <> int n | _ <- ids | n <- [0..]]
--         let shallowDecl = genCDecl (text "shallow<string::iterator>") (text "_shallow") Nothing
--         let patchFn = text "static" <+> text "void" <+> text "patch"
--                    <> parens (cat $ punctuate comma [text "string s", recordName
--                        <> angles (hsep $ punctuate comma templateVars) <> text "&" <+> text "r"])
--                   <+> hangBrace (vsep [shallowDecl <> semi, fps, afp, lfp, piv])
--         return $ text "namespace K3" <+> (hangBrace $ genCTemplateDecl templateVars
--                    <$$> text "struct patcher"
--                             <> angles (recordName <> angles (hsep $ punctuate comma templateVars))
--                     <+> hangBrace patchFn <> semi)

--     serializeDefn = do
--         body <- serializeBody
--         return $ genCFunction
--                  (Just [text "archive"])
--                  (text "void")
--                  (text "serialize")
--                  [text "archive& _archive", text "const unsigned int"]
--                  (body <$$> parentSerialize)

--     serializeBody = serializationMethod <$> get >>= \case
--         BoostSerialization -> return $ genCBoostSerialize ids

--     parentSerialize = empty

-- genCBoostSerialize :: [Identifier] -> CPPGenR
-- genCBoostSerialize dataDecls = vsep $ map serializeMember dataDecls
--   where
--     serializeMember dataDecl = text "_archive" <+> text "&" <+> text dataDecl <> semi

-- definition :: Identifier -> K3 Type -> Maybe (K3 Expression) -> CPPGenM CPPGenR
-- definition i (tag &&& children -> (TFunction, [ta, tr])) (Just (tag &&& children -> (ELambda x, [b]))) = do
--     body <- reify RReturn b
--     cta <- genCType ta
--     ctr <- genCType tr
--     return $ ctr <+> text i <> parens (cta <+> text x) <+> hangBrace body
-- definition i t (Just e) = do
--     newI <- reify (RName i) e
--     d <- cDecl t i
--     return $ d <//> newI
-- definition i t Nothing = cDecl t i

reservedAnnotations :: [Identifier]
reservedAnnotations = ["Collection", "External", "Seq", "Set", "Sorted", "Map"]
