{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Collections where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Function (on)
import Data.Functor
import Data.List (nub, partition, sortBy)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Expression
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

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

    let (ras, nras) = partition (\(aname, _) -> aname `elem` reservedAnnotations) ans

    -- Inlining is only done for provided (positive) declarations.
    let positives = filter isPositiveDecl (concat . snd $ unzip nras)

    -- Split data and method declarations, for access specifiers.
    let (dataDecls, methDecls) = partition isDataDecl positives

    -- Generate a serialization method based on engine preferences.
    serializationDefn <- serializationMethod <$> get >>= \case
        BoostSerialization -> return $ genCBoostSerialize $ map (\(Lifted _ i _ _ _) -> i) dataDecls

    let ps = punctuate comma $ map (\(fst -> p) -> genCQualify (text "K3") $ text p <> angles (text "CONTENT")) ras

    constructors' <- mapM ($ ps) constructors

    pubDecls <- mapM annMemDecl methDecls
    prvDecls <- mapM annMemDecl dataDecls

    let classBody = text "class" <+> text className <> colon <+> hsep (map (text "public" <+>) ps)
            <+> hangBrace (text "public:"
            <$$> indent 4 (vsep $ punctuate line $ constructors' ++ [serializationDefn] ++ pubDecls)
            <$$> vsep [text "private:" <$$> indent 4 (vsep $ punctuate line prvDecls) | not (null prvDecls)]
            ) <> semi

    let classDefn = genCTemplateDecl [text "CONTENT"] <$$> classBody <$$> patcherSpec

    addForward $ genCTemplateDecl [text "CONTENT"] <+> text "class" <+> text className <> semi

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

    engineConstructor ps = return $
            text className
         <> parens empty
         <> colon
        <+> hsep (punctuate comma $ map (<> parens (text "&engine")) ps)
        <+> braces empty

    -- TODO: Generate copy statements for remaining data members.
    copyConstructor ps = return $
            text className
         <> parens (text $ "const " ++ className ++ "& c")
         <> colon
        <+> hsep (punctuate comma $ map (<> parens (text "c")) ps) <+> braces empty

    constructors = [engineConstructor, copyConstructor]

    patcherSpec :: CPPGenR
    patcherSpec = genCTemplateDecl [text "E"] <$$> patcherSpecStruct

    patcherSpecStruct :: CPPGenR
    patcherSpecStruct = text "struct patcher"
                     <> angles (text className <> angles (text "E")) <+> hangBrace patcherSpecMeth <> semi

    patcherSpecMeth :: CPPGenR
    patcherSpecMeth = text "static" <+> text "void" <+> text "patch"
                   <> parens (cat $ punctuate comma [
                              text "string s",
                              text className <> angles (text "E") <> text "&" <+> text "c"
                         ])
                  <+> hangBrace subPatcherCall

    subPatcherCall :: CPPGenR
    subPatcherCall = genCQualify patcherStruct patcherCall <> semi

    patcherStruct :: CPPGenR
    patcherStruct = text "collection_patcher" <> angles (cat $ punctuate comma [text className, text "E"])

    patcherCall = genCCall (text "patch") Nothing [text "s", text "c"]

dataspaceType :: CPPGenR -> CPPGenM CPPGenR
dataspaceType eType = return $ text "vector" <> angles eType

annMemDecl :: AnnMemDecl -> CPPGenM CPPGenR
annMemDecl (Lifted _ i t me _)  = do
    memDefinition <- definition i t me
    return $ genCTemplateDecl (map text typeVars) <$$> memDefinition
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
    let structDefn = text "struct" <+> text rName <+> hangBrace (members <$$> sD) <> semi
    patcherDefn <- patcherSpec
    addForward $ text "struct" <+> text rName <> semi
    return $ vsep [structDefn, patcherDefn]
  where
    oneFieldParser :: Identifier -> CPPGenM CPPGenR
    oneFieldParser i = do
        let keyParser = text "qi::lit" <> parens (dquotes $ text i)
        let valParser = text "_shallow"
        let fieldParser = parens $ keyParser <+> text ">>" <+> squotes (char ':') <+> text ">>" <+> valParser
        let actCapture = text "&r"
        let actArgs = text "string _string"
        let actBody = genCCall (text "do_patch") Nothing [text "_string", text "r" <> dot <> text i] <> semi
        let fieldAction = brackets $ parens $ brackets actCapture <+> parens actArgs <+> braces actBody

        return $ genCDecl
          (text "qi::rule<string::iterator, qi::space_type>")
          (text "_" <> text i)
          (Just $ fieldParser <> fieldAction)

    anyFieldParser :: [Identifier] -> CPPGenM CPPGenR
    anyFieldParser is
      = return $ genCDecl (text "qi::rule<string::iterator, qi::space_type>")
                 (text "_field")
                 (Just $ cat $ punctuate (text " | ") [text "_" <> text i | i <- is])

    allFieldParser :: CPPGenM CPPGenR
    allFieldParser
      = return $ genCDecl (text "qi::rule<string::iterator, qi::space_type>")
                          (text "_parser")
                          (Just $ text "'{' >> (_field % ',') >> '}'")

    parserInvocation :: CPPGenM CPPGenR
    parserInvocation
      = return $ genCCall (text "qi::phrase_parse") Nothing
                 [text "std::begin(s)", text "std::end(s)", text "_parser", text "qi::space"] <> semi

    patcherSpec :: CPPGenM CPPGenR
    patcherSpec = do
        let ids = fst $ unzip idts
        fps <- vsep <$> mapM oneFieldParser ids
        afp <- anyFieldParser ids
        lfp <- allFieldParser
        piv <- parserInvocation
        let shallowDecl = genCDecl (text "shallow<string::iterator>") (text "_shallow") Nothing
        let patchFn = text "static" <+> text "void" <+> text "patch"
                   <> parens (cat $ punctuate comma [text "string s", text rName <> text "&" <+> text "r"])
                  <+> hangBrace (vsep [shallowDecl <> semi, fps, afp, lfp, piv])
        return $ genCTemplateDecl [] <$$> text "struct patcher" <> angles (text rName) <+> hangBrace patchFn <> semi

    serializeDefn = serializationMethod <$> get >>= \case
        BoostSerialization -> return $ genCBoostSerialize (fst $ unzip idts)

genCBoostSerialize :: [Identifier] -> CPPGenR
genCBoostSerialize dataDecls = genCFunction
    (Just [text "archive"])
    (text "void")
    (text "serialize")
    [text "archive _archive"]
    serializeBody
  where
    serializeBody = vsep $ map serializeMember dataDecls
    serializeMember dataDecl = text "_archive" <+> text "&" <+> text dataDecl <> semi

definition :: Identifier -> K3 Type -> Maybe (K3 Expression) -> CPPGenM CPPGenR
definition i (tag &&& children -> (TFunction, [ta, tr])) (Just (tag &&& children -> (ELambda x, [b]))) = do
    body <- reify RReturn b
    cta <- genCType ta
    ctr <- genCType tr
    return $ ctr <+> text i <> parens (cta <+> text x) <+> hangBrace body
definition i t (Just e) = do
    newI <- reify (RName i) e
    d <- cDecl t i
    return $ d <//> newI
definition i t Nothing = cDecl t i

reservedAnnotations :: [Identifier]
reservedAnnotations = ["Collection", "External", "Seq", "Set", "Sorted"]
