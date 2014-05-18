{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}

-- This module provides the machinery necessary to generate C++ code from K3 programs. The resulting
-- code can be compiled using a C++ compiler and linked against the K3 runtime library to produce a
-- binary.
module Language.K3.Codegen.CPP (
    module Language.K3.Codegen.CPP.Types,

    program
) where

import Control.Arrow ((&&&))

import Control.Monad.State hiding (forM)

import Data.Function
import Data.Functor
import Data.List (nub, sortBy)
import Data.Traversable (forM)

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))
import qualified Text.PrettyPrint.ANSI.Leijen as PL

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

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Type as T

-- C++ Primitive Generators

declaration :: K3 Declaration -> CPPGenM CPPGenR
declaration (tag -> DGlobal i _ _) | "register" `L.isPrefixOf` i = return empty
declaration (tag -> DGlobal _ (tag -> TSource) _) = return empty
declaration (tag -> DGlobal i t Nothing) = cDecl t i
declaration (tag -> DGlobal i t@(tag &&& children -> (TFunction, [ta, tr]))
            (Just (tag &&& children -> (ELambda x, [b])))) = do
    newF <- cDecl t i
    modify (\s -> s { forwards = forwards s PL.<$$> newF })
    body <- reify RReturn b
    cta <- genCType ta
    ctr <- genCType tr
    return $ ctr <+> text i <> parens (cta <+> text x) <+> hangBrace body

declaration (tag -> DGlobal i t (Just e)) = do
    newI <- reify (RName i) e
    modify (\s -> s { initializations = initializations s PL.<//> newI })
    cDecl t i

-- The generated code for a trigger is the same as that of a function with corresponding ()
-- return-type. Additionally however, we must generate a trigger-wrapper function to perform
-- deserialization.
declaration (tag -> DTrigger i t e) = do
    addTrigger i
    d <- declaration (D.global i (T.function t T.unit) (Just e))
    w <- triggerWrapper i t
    return $ d PL.<$$> w

declaration (tag &&& children -> (DRole n, cs)) = do
    subDecls <- vsep . punctuate line <$> mapM declaration cs
    currentS <- get
    i <- genCType T.unit >>= \ctu ->
        return $ ctu <+> text "initGlobalDecls" <> parens empty <+> hangBrace (initializations currentS)
    let amp = annotationMap currentS
    compositeDecls <- forM (S.toList $ composites currentS) $ \(S.toList -> als) ->
        composite (annotationComboId als) [(a, M.findWithDefault [] a amp) | a <- als]
    recordDecls <- forM (M.toList $ recordMap currentS) $ uncurry record
    tablePop <- generateDispatchPopulation
    let tableDecl = text "TriggerDispatch" <+> text "dispatch_table" <> semi

    put defaultCPPGenS
    return $ text "namespace" <+> text n <+> hangBrace (
        vsep $ punctuate line $ [forwards currentS]
            ++ compositeDecls
            ++ recordDecls
            ++ [subDecls, i, tableDecl, tablePop])

declaration (tag -> DAnnotation i _ amds) = addAnnotation i amds >> return empty
declaration _ = return empty

-- | Generate a trigger-wrapper function, which performs deserialization of an untyped message
-- (using Boost serialization) and call the appropriate trigger.
triggerWrapper :: Identifier -> K3 Type -> CPPGenM CPPGenR
triggerWrapper i t = do
    tmpDecl <- cDecl t "arg"
    tmpType <- genCType t
    let triggerDispatch = text i <> parens (text "arg") <> semi
    let unpackCall = text "arg" <+> equals <+> text "*" <> genCCall (text "unpack") (Just [tmpType]) [text "msg"] <> semi
    return $ genCFunction Nothing (text "void") (text i <> text "_dispatch") [text "string msg"] $ hangBrace (
            vsep [
                tmpDecl,
                unpackCall,
                triggerDispatch,
                text "return;"
            ])

-- | Generates a function which populates the trigger dispatch table.
generateDispatchPopulation :: CPPGenM CPPGenR
generateDispatchPopulation = do
    triggerS <- triggers <$> get
    dispatchStatements <- mapM genDispatch (S.toList triggerS)
    return $ genCFunction Nothing (text "void") (text "populate_dispatch") [] (vsep dispatchStatements)
  where
    genDispatch tName = return $
        text ("dispatch_table[\"" ++ tName ++ "\"] = " ++ genDispatchName tName) <> semi

genDispatchName :: Identifier -> Identifier
genDispatchName i = i ++ "_dispatch"

reserved :: [Identifier]
reserved = ["openBuiltin"]

templateLine :: [Doc] -> Doc
templateLine [] = empty
templateLine ts = text "template" <+> angles (sep $ punctuate comma [text "class" <+> td | td <- ts])

genCBoostSerialize :: [Identifier] -> CPPGenR
genCBoostSerialize dataDecls = serializeTemplateLine PL.<$$> serializeMethod
  where
    serializeTemplateLine = templateLine [text "archive"]
    serializeMethod = genCFunction
                          (Just [text "archive"])
                          (text "void")
                          (text "serialize")
                          [text "archive _archive"]
                          serializeBody
    serializeBody = vsep $ map serializeMember dataDecls
    serializeMember dataDecl = text "_archive" <+> text "&" <+> text dataDecl <> semi

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
composite :: Identifier -> [(Identifier, [AnnMemDecl])] -> CPPGenM CPPGenR
composite className ans = do

    -- Inlining is only done for provided (positive) declarations.
    let positives = filter isPositiveDecl (concat . snd $ unzip ans)

    -- Split data and method declarations, for access specifiers.
    let (dataDecls, methDecls) = L.partition isDataDecl positives

    -- Generate a serialization method based on engine preferences.
    serializationDefn <- serializationMethod <$> get >>= \case
        BoostSerialization -> return $ genCBoostSerialize $ map (\(Lifted _ i _ _ _) -> i) dataDecls

    constructors' <- sequence constructors

    let parentList = text "Collection"

    pubDecls <- mapM annMemDecl methDecls
    prvDecls <- mapM annMemDecl dataDecls

    let classBody = text "class" <+> text className <> colon <+> parentList
            <+> hangBrace (text "public:"
            PL.<$$> indent 4 (vsep $ punctuate line $ constructors' ++ [serializationDefn] ++ pubDecls)
            PL.<$$> vsep [text "private:" PL.<$$> indent 4 (vsep $ punctuate line prvDecls) | not (null prvDecls)]
            ) <> semi

    let classDefn = templateLine [text "CONTENT"] PL.<$$> classBody

    return classDefn
  where

    isPositiveDecl :: AnnMemDecl -> Bool
    isPositiveDecl (Lifted Provides _ _ _ _) = True
    isPositiveDecl (Attribute Provides _ _ _ _) = True
    isPositiveDecl (MAnnotation Provides _ _) = True
    isPositiveDecl _ = False

    isDataDecl :: AnnMemDecl -> Bool
    isDataDecl (Lifted _ _ (tag -> TFunction) _ _) = False
    isDataDecl (Attribute _ _ (tag -> TFunction) _ _) = False
    isDataDecl _ = True

    defaultConstructor = return $ text className <> parens empty <> colon
        <+> text "Collection" <> parens empty <+> braces empty

    dataspaceConstructor = do
        let dsType = text "chunk" <> angles (text "CONTENT")
        return $ text className <> parens ( dsType <+> text "v")
            <> colon <+> text "Collection" <> parens (text "v") <+> braces empty

    -- TODO: Generate copy statements for remaining data members.
    copyConstructor = return $ text className <> parens (text $ "const " ++ className ++ "& c") <> colon
        <+> text "Collection" <> parens (text "c") <+> braces empty

    constructors = [defaultConstructor, dataspaceConstructor, copyConstructor]

dataspaceType :: CPPGenR -> CPPGenM CPPGenR
dataspaceType eType = return $ text "vector" <> angles eType

annMemDecl :: AnnMemDecl -> CPPGenM CPPGenR
annMemDecl (Lifted _ i t me _)  = do
    memDefinition <- definition i t me
    return $ templateLine (map text typeVars) PL.<$$> memDefinition
  where
    findTypeVars :: K3 Type -> [Identifier]
    findTypeVars (tag -> TDeclaredVar v) = [v]
    findTypeVars (children -> []) = []
    findTypeVars (children -> ts) = concatMap findTypeVars ts

    typeVars = nub $ findTypeVars t

annMemDecl (Attribute _ i _ _ _) = return $ text i
annMemDecl (MAnnotation _ i _) = return $ text i

-- Generate a struct definition for an anonymous record type.
record :: Identifier -> [(Identifier, K3 Type)] -> CPPGenM CPPGenR
record rName idts = do
    members <- vsep <$> mapM (\(i, t) -> definition i t Nothing) (sortBy (compare `on` fst) idts)
    sD <- serializeDefn
    return $ text "struct" <+> text rName <+> hangBrace (members PL.<$$> sD) <> semi
  where
    serializeDefn = serializationMethod <$> get >>= \case
        BoostSerialization -> return $ genCBoostSerialize (fst $ unzip idts)

definition :: Identifier -> K3 Type -> Maybe (K3 Expression) -> CPPGenM CPPGenR
definition i (tag &&& children -> (TFunction, [ta, tr])) (Just (tag &&& children -> (ELambda x, [b]))) = do
    body <- reify RReturn b
    cta <- genCType ta
    ctr <- genCType tr
    return $ ctr <+> text i <> parens (cta <+> text x) <+> hangBrace body
definition i t (Just e) = do
    newI <- reify (RName i) e
    d <- cDecl t i
    return $ d PL.<//> newI
definition i t Nothing = cDecl t i

-- Top-level program generation.
--  - Process the __main role.
--  - Generate include directives.
--  - Generate namespace use directives.
program :: K3 Declaration -> CPPGenM CPPGenR
program d = do
    staticGlobals' <- staticGlobals
    program' <- declaration d
    genNamespaces <- namespaces >>= \ns -> return [text "using namespace" <+> text n <> semi | n <- ns]
    genIncludes <- includes >>= \is -> return [text "#include" <+> dquotes (text f) | f <- is]
    main <- genKMain
    return $ vsep $ punctuate line [
            vsep genIncludes,
            vsep genNamespaces,
            vsep genAliases,
            staticGlobals',
            program',
            main
        ]
  where
    genAliases = [text "using" <+> text new <+> equals <+> text old <> semi | (new, old) <- aliases]

genKMain :: CPPGenM CPPGenR
genKMain = return $ genCFunction Nothing (text "int") (text "main") [text "int", text "char**"] $ vsep [
        genCQualify (text "__global") (genCCall (text "populate_dispatch") Nothing []) <> semi,
        genCQualify (text "__global") (genCCall (text "processRole") Nothing [text "unit_t()"]) <> semi,
        text "DispatchMessageProcessor dmp = DispatchMessageProcessor(__global::dispatch_table);",
        text "engine.runEngine(make_shared<DispatchMessageProcessor>(dmp));"
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
        "MessageProcessor.hpp",
        "Serialization.hpp"
    ]

namespaces :: CPPGenM [Identifier]
namespaces = do
    serializationNamespace <- serializationMethod <$> get >>= \case
        BoostSerialization -> return "K3::BoostSerializer"
    return ["std", "K3", serializationNamespace]

aliases :: [(Identifier, Identifier)]
aliases = [
        ("unit_t", "struct {}")
    ]

staticGlobals :: CPPGenM CPPGenR
staticGlobals = return $ vsep [
            text "SystemEnvironment se = defaultEnvironment()" <> semi,
            text "Engine engine = Engine(true, se, make_shared<DefaultInternalCodec>(DefaultInternalCodec()))" <> semi
        ]
