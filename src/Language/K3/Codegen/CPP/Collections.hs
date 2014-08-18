{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Collections where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Functor
import Data.List (nub, partition, sort)

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

    let parentSerializeCall p = text "_archive" <+> text "&" <+>
                                genCQualify (genCQualify (text "boost") (text "serialization"))
                                  (genCCall (text "base_object")
                                  (Just [genCQualify (text "K3") (text p) <> angles (text "CONTENT")])
                                  [text "*this"]) <> semi
    let dataSerialize = vsep $ map (parentSerializeCall . fst) ras

    -- Generate a serialization method based on engine preferences.
    serializeBody <- serializationMethod <$> get >>= \case
        BoostSerialization -> return $ genCBoostSerialize $ map (\(Lifted _ i _ _ _) -> i) dataDecls

    let serializeDefn = genCFunction
                          (Just [text "archive"])
                          (text "void")
                          (text "serialize")
                          [text "archive& _archive", text "const unsigned int"]
                          (serializeBody <$$> dataSerialize)

    let ps = punctuate comma $ map (\(fst -> p) -> genCQualify (text "K3") $ text p <> angles (text "CONTENT")) ras

    constructors' <- mapM ($ ps) constructors

    let constructors'' = constructors' ++ [sc p | p <- take 1 ps, sc <- superConstructors]

    aoperators' <- mapM ($ ps) assignOperators

    pubDecls <- mapM annMemDecl methDecls
    prvDecls <- mapM annMemDecl dataDecls

    let classBody = text "class" <+> text className <> colon <+> hsep (map (text "public" <+>) ps)
            <+> hangBrace (text "public:"
            <$$> indent 4 (vsep $ punctuate line $ constructors'' ++ aoperators' ++ [serializeDefn] ++ pubDecls)
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

    defaultConstructor ps = return $
            text className
         <> parens empty
         <> colon
        <+> hsep (punctuate comma $ map (<> parens empty) ps) -- initialize superclasses
        <+> braces empty

    -- TODO: Generate copy statements for remaining data members.
    copyConstructor ps = return $
            text className
         <> parens (text $ "const " ++ className ++ "& c")
         <> colon
        <+> hsep (punctuate comma $ map (<> parens (text "c")) ps) <+> braces empty

    moveConstructor ps = return $
            text className
         <> parens (text $ className ++ "&& c")
         <> colon
        <+> hsep (punctuate comma $ map (<> parens (text "std::move(c)")) ps) <+> braces empty

    superCopyConstructor p =
            text className
         <> parens (text "const" <+> p <> text "& c")
         <> colon
        <+> p <> parens (text "c") <+> braces empty

    superMoveConstructor p =
            text className
         <> parens (p <> text "&& c")
         <> colon
        <+> p <> parens (text "std::move(c)") <+> braces empty

    constructors = [defaultConstructor, copyConstructor, moveConstructor]
    superConstructors = [superCopyConstructor, superMoveConstructor]

    assignOperator ps = return $
            text (className ++ "&")
        <+> text "operator="
         <> parens (text $ "const " ++ className ++ "& other")
         <> hangBrace (vsep $
              (map (<> text "::operator=(other);") ps)
              ++ [text "return *this;"]
            )

    moveAssignOperator ps = return $
            text (className ++ "&")
        <+> text "operator="
         <> parens (text $ "const " ++ className ++ "&& other")
         <> hangBrace (vsep $
              (map (<> text "::operator=(std::move(other));") ps)
              ++ [text "return *this;"]
            )

    assignOperators = [assignOperator, moveAssignOperator]

    patcherSpec :: CPPGenR
    patcherSpec = text "namespace K3" <+> (hangBrace $ genCTemplateDecl [text "E"] <$$> patcherSpecStruct)

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

record :: [Identifier] -> CPPGenM CPPGenR
record (sort -> ids) = do
    let templateVars = [text "_T" <> int n | _ <- ids | n <- [0..]]
    let formalVars = [text "_" <> text i | i <- ids]

    let defaultConstructor
            = recordName <> parens empty <+> braces empty

    let initConstructor
            = recordName <> tupled (zipWith (<+>) templateVars formalVars) <> colon
                        <+> hsep (punctuate comma $ [text i <> parens f | i <- ids | f <- formalVars])
                        <+> braces empty

    let copyConstructor
            = recordName <> parens (text "const" <+> recordName
                                      <> angles (hsep $ punctuate comma templateVars) <> text "&" <+> text "_r")
                                      <> colon
                        <+> hsep (punctuate comma [text i <> parens (text "_r" <> dot <> text i) | i <- ids])
                        <+> braces empty

    let constructors = [defaultConstructor, initConstructor, copyConstructor]

    let fieldEqs = text "if"
          <+> parens (hsep $ punctuate (text "&&") [text i <+> text "==" <+> text "_r" <> dot <> text i | i <- ids])
          <$$> indent 4 (text "return true;") <$$> text "return false;"

    let equalityOperator = genCFunction Nothing (text "bool") (text "operator==") [recordName <+> text "_r"] fieldEqs

    let fields = [t <+> text i <> semi | t <- templateVars | i <- ids]

    serializer <- serializeDefn

    let publicDefs = vsep $ constructors ++ [equalityOperator, serializer]
    let privateDefs = vsep fields
    let recordDefs = text "public:" <$$> indent 4 (publicDefs <$$> privateDefs)

    let templateDecl = genCTemplateDecl templateVars
    let recordStructDef = templateDecl <$$> text "class" <+> recordName <+> hangBrace recordDefs <> semi

    recordPatcherDef <- patcherSpec

    addForward $ templateDecl <+> text "class" <+> recordName <> semi

    return $ defineProtect recordStructDef <$$> recordPatcherDef
  where
    sigil = hcat (punctuate (text "_") [text "K3", recordName])
    defineProtect t = text "#ifndef" <+> sigil <$$> text "#define" <+> sigil <$$> t <$$> text "#endif //" <+> sigil

    recordName = text $ recordSignature ids

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
        fps <- vsep <$> mapM oneFieldParser ids
        afp <- anyFieldParser ids
        lfp <- allFieldParser
        piv <- parserInvocation
        let templateVars = [text "_T" <> int n | _ <- ids | n <- [0..]]
        let shallowDecl = genCDecl (text "shallow<string::iterator>") (text "_shallow") Nothing
        let patchFn = text "static" <+> text "void" <+> text "patch"
                   <> parens (cat $ punctuate comma [text "string s", recordName
                       <> angles (hsep $ punctuate comma templateVars) <> text "&" <+> text "r"])
                  <+> hangBrace (vsep [shallowDecl <> semi, fps, afp, lfp, piv])
        return $ text "namespace K3" <+> (hangBrace $ genCTemplateDecl templateVars
                   <$$> text "struct patcher"
                            <> angles (recordName <> angles (hsep $ punctuate comma templateVars))
                    <+> hangBrace patchFn <> semi)

    serializeDefn = do
        body <- serializeBody
        return $ genCFunction
                 (Just [text "archive"])
                 (text "void")
                 (text "serialize")
                 [text "archive& _archive", text "const unsigned int"]
                 (body <$$> parentSerialize)

    serializeBody = serializationMethod <$> get >>= \case
        BoostSerialization -> return $ genCBoostSerialize ids

    parentSerialize = empty

genCBoostSerialize :: [Identifier] -> CPPGenR
genCBoostSerialize dataDecls = vsep $ map serializeMember dataDecls
  where
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
reservedAnnotations = ["Collection", "External", "Seq", "Set", "Sorted", "Map"]
