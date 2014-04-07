{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}

-- | K3 -> C++ Code Generation.
--
-- This module provides the machinery necessary to generate C++ code from K3 programs. The resulting
-- code can be compiled using a C++ compiler and linked against the K3 runtime library to produce a
-- binary.
module Language.K3.Codegen.CPP where

import Control.Arrow ((&&&))

import Control.Monad.State hiding (forM)
import Control.Monad.Trans.Either

import Data.Function
import Data.Functor
import Data.List (nub, sortBy)
import Data.Maybe (isJust)
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

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Type as T

-- | The C++ code generation monad. Provides access to various configuration values and error
-- reporting.
type CPPGenM a = EitherT CPPGenE (State CPPGenS) a

-- | Run C++ code generation action using a given initial state.
runCPPGenM :: CPPGenS -> CPPGenM a -> (Either CPPGenE a, CPPGenS)
runCPPGenM s = flip runState s . runEitherT

-- | Error messages thrown by C++ code generation.
data CPPGenE = CPPGenE String deriving (Eq, Read, Show)

-- | Throw a code generation error.
throwE :: CPPGenE -> CPPGenM a
throwE = left

-- | All generated code is produced in the form of pretty-printed blocks.
type CPPGenR = Doc

-- | State carried around during C++ code generation.
data CPPGenS = CPPGenS {
        -- | UUID counter for generating identifiers.
        uuid :: Int,

        -- | Code necessary to initialize global declarations.
        initializations :: CPPGenR,

        -- | Forward declarations for constructs as a result of cyclic scope.
        forwards :: CPPGenR,

        -- | Mapping of record signatures to corresponding record structure, for generation of
        -- record classes.
        recordMap :: M.Map Identifier [(Identifier, K3 Type)],

        -- | Mapping of annotation class names to list of member declarations, for eventual
        -- declaration of composite classes.
        annotationMap :: M.Map Identifier [AnnMemDecl],

        -- | Set of annotation combinations actually encountered during the program.
        composites :: S.Set (S.Set Identifier),

        -- | The set of triggers declared in a program, used to populate the dispatch table.
        triggers :: S.Set Identifier,

        -- | The serialization method to use.
        serializationMethod :: SerializationMethod
    } deriving Show

-- | The default code generation state.
defaultCPPGenS :: CPPGenS
defaultCPPGenS = CPPGenS 0 empty empty M.empty M.empty S.empty S.empty BoostSerialization

-- | Generate a new unique symbol, required for temporary reification.
genSym :: CPPGenM Identifier
genSym = do
    current <- uuid <$> get
    modify (\s -> s { uuid = succ (uuid s) })
    return $ '_':  show current

-- | Add an annotation to the code generation state.
addAnnotation :: Identifier -> [AnnMemDecl] -> CPPGenM ()
addAnnotation i amds = modify (\s -> s { annotationMap = M.insert i amds (annotationMap s) })

-- | Add a new composite specification to the code generation state.
addComposite :: [Identifier] -> CPPGenM ()
addComposite is = modify (\s -> s { composites = S.insert (S.fromList is) (composites s) })

-- | Add a new record specification to the code generation state.
addRecord :: Identifier -> [(Identifier, K3 Type)] -> CPPGenM ()
addRecord i its = modify (\s -> s { recordMap = M.insert i its (recordMap s) })

-- | Add a new trigger specification to the code generation state.
addTrigger :: Identifier -> CPPGenM ()
addTrigger i = modify (\s -> s { triggers = S.insert i (triggers s) })

data SerializationMethod
    = BoostSerialization
  deriving (Eq, Read, Show)

-- | The reification context passed to an expression determines how the result of that expression
-- will be stored in the generated code.
data RContext

    -- | Indicates that the calling context will ignore the callee's result.
    = RForget

    -- | Indicates that the calling context is a C++ function, in which case the result may be
    -- 'returned' from the callee.
    | RReturn

    -- | Indicates that the calling context requires the callee's result to be stored in a variable
    -- of a pre-specified name.
    | RName Identifier

    -- | A free-form reification context, for special cases.
    | RSplice ([CPPGenR] -> CPPGenR)

instance Show RContext where
    show RForget = "RForget"
    show RReturn = "RReturn"
    show (RName i) = "RName \"" ++ i ++ "\""
    show (RSplice _) = "RSplice <opaque>"

-- | Realization of unary operators.
unarySymbol :: Operator -> CPPGenM CPPGenR
unarySymbol ONot = return $ text "!"
unarySymbol ONeg = return $ text "-"
unarySymbol u = throwE $ CPPGenE $ "Invalid Unary Operator " ++ show u

-- | Realization of binary operators.
binarySymbol :: Operator -> CPPGenM CPPGenR
binarySymbol OAdd = return $ text "+"
binarySymbol OSub = return $ text "-"
binarySymbol OMul = return $ text "*"
binarySymbol ODiv = return $ text "/"
binarySymbol OMod = return $ text "%" -- TODO: type based selection of % vs fmod
binarySymbol OEqu = return $ text "=="
binarySymbol ONeq = return $ text "!="
binarySymbol OLth = return $ text "<"
binarySymbol OLeq = return $ text "<="
binarySymbol OGth = return $ text ">"
binarySymbol OGeq = return $ text ">="
binarySymbol OAnd = return $ text "&&"
binarySymbol OOr = return $ text "||"
binarySymbol b = throwE $ CPPGenE $ "Invalid Binary Operator " ++ show b

-- | Realization of constants.
constant :: Constant -> CPPGenM CPPGenR
constant (CBool True) = return $ text "true"
constant (CBool False) = return $ text "false"
constant (CInt i) = return $ int i
constant (CReal d) = return $ double d
constant (CString s) = return $ text "string" <> (parens . text $ show s)
constant (CNone _) = return $ text "null"
constant c = throwE $ CPPGenE $ "Invalid Constant Form " ++ show c

-- | Reaization of types.
-- This utility is used in a number of places where the generated code makes use of an expression's
-- type. The most prominent place is where a temporary variable must be declared to store the result
-- of an expression, and must be of the appropriate type.
cType :: K3 Type -> CPPGenM CPPGenR
cType (tag -> TBool) = return $ text "bool"
cType (tag -> TByte) = return $ text "unsigned char"
cType (tag -> TInt) = return $ text "int"
cType (tag -> TReal) = return $ text "double"
cType (tag -> TString) = return $ text "string"
cType (tag &&& children -> (TOption, [t])) = (text "shared_ptr" <>) . angles <$> cType t
cType (tag &&& children -> (TIndirection, [t])) = (text "shared_ptr" <>) . angles <$> cType t
cType (tag &&& children -> (TTuple, [])) = return $ text "unit_t"
cType (tag &&& children -> (TTuple, ts))
    = (text "tuple" <>) . angles . sep . punctuate comma <$> mapM cType ts

-- TODO: Is a call to addRecord really needed here?
cType t@(tag -> TRecord ids) = signature t >>= \sig -> addRecord sig (zip ids (children t)) >> return (text sig)
cType (tag -> TDeclaredVar t) = return $ text t

-- TODO: Three pieces of information necessary to generate a collection type:
--  1. The list of named annotations on the collections.
--  2. The types provided to each annotation to fulfill their type variable requirements.
--  3. The content type.
cType (tag &&& children &&& annotations -> (TCollection, ([et], as))) = do
    ct <- cType et
    case annotationComboIdT as of
        Nothing -> return $ text "Collection" <> angles ct
        Just i' -> return $ text i' <> angles ct

cType (tag -> TAddress) = return $ text "Address"

-- TODO: Are these all the cases that need to be handled?
cType (tag -> TNumber) = return $ text "double"
cType t = throwE $ CPPGenE $ "Invalid Type Form " ++ show t

-- TODO: This isn't really C++ specific.
canonicalType :: K3 Expression -> CPPGenM (K3 Type)
canonicalType (tag -> EConstant (CEmpty t)) = return $ T.collection t
canonicalType e = case lowerBoundType e of
    Just x -> return x
    Nothing -> throwE $ CPPGenE $ "Invalid Lower Bound for " ++ show e

-- | Get the lower bound type of an expression.
lowerBoundType :: K3 Expression -> Maybe (K3 Type)
lowerBoundType e = case e @~ (\case (ETypeLB _) -> True; _ -> False) of
    Just (ETypeLB t) -> Just t
    _ -> Nothing

-- | Generate a C++ declaration for a value of a given type.
cDecl :: K3 Type -> Identifier -> CPPGenM CPPGenR
cDecl (tag &&& children -> (TFunction, [ta, tr])) i = do
    ctr <- cType tr
    cta <- cType ta
    return $ ctr <+> text i <> parens cta <> semi

-- TODO: As with cType/addRecord, is a call to addComposite really needed here?
cDecl t@(tag &&& annotations -> (TCollection, as)) i = case annotationComboIdT as of
    Nothing -> return $ text "Collection" <+> text i
    Just _ -> addComposite (namedTAnnotations as) >> cType t >>= \ct -> return $ ct <+> text i <> semi
cDecl t i = cType t >>= \ct -> return $ ct <+> text i <> semi

inline :: K3 Expression -> CPPGenM (CPPGenR, CPPGenR)
inline e@(tag &&& annotations -> (EConstant (CEmpty t), as)) = case annotationComboIdE as of
    Nothing -> throwE $ CPPGenE $ "No Viable Annotation Combination for Empty " ++ show e
    Just ac -> cType t >>= \ct -> addComposite (namedEAnnotations as) >> return (empty, text ac <> angles ct)

inline (tag -> EConstant c) = (empty,) <$> constant c

-- If a variable was declared as mutable it's been reified as a shared_ptr, and must be
-- dereferenced.
inline e@(tag -> EVariable v) = return $ if isJust $ e @~ (\case { EMutable -> True; _ -> False })
        then (empty, text "*" <> text v)
        else (empty, text v)

inline (tag &&& children -> (t', [c])) | t' == ESome || t' == EIndirect = do
    (e, v) <- inline c
    ct <- canonicalType c
    t <- cType ct
    return (e, text "shared_ptr" <> angles t <> parens (text "new" <+> t <> parens v))
inline (tag &&& children -> (ETuple, [])) = return (empty, text "unit_t" <> parens empty)
inline (tag &&& children -> (ETuple, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    return (vsep es, text "make_tuple" <> tupled vs)
inline e@(tag &&& children -> (ERecord _, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    t <- canonicalType e
    case t of
        (tag &&& children -> (TRecord ids, ts)) -> do
            sig <- signature t
            addRecord sig (zip ids ts)
            return (vsep es, text sig <> tupled vs)
        _ -> throwE $ CPPGenE $ "Invalid Record Type " ++ show t

inline (tag &&& children -> (EOperate uop, [c])) = do
    (ce, cv) <- inline c
    usym <- unarySymbol uop
    return (ce, usym <> cv)
inline (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    (be, bv) <- inline b
    return (ae PL.<$> be, bv)
inline e@(tag &&& children -> (ELambda arg, [body])) = do
    (ta, tr) <- canonicalType e >>= \case
        (tag &&& children -> (TFunction, [ta, tr])) -> do
            ta' <- cType ta
            tr' <- cType tr
            return (ta', tr')
        _ -> throwE $ CPPGenE "Invalid Function Form"
    let fvs = map text . L.delete arg $ freeVariables body
    body' <- reify RReturn body
    return (empty, list fvs <+> parens (ta <+> text arg) <+> hangBrace body')
inline (tag &&& children -> (EOperate OApp, [f, a])) = do
    (ae, av) <- inline a
    case f of
        (tag -> EVariable v) -> return (ae, text v <> parens av)
        (tag -> EProject _) -> do
            (pe, pv) <- inline f
            return (ae PL.<//> pe, pv <> parens av)
        (tag -> ELambda _) -> do
            (fe, fv) <- inline f
            return (ae PL.<//> fe, fv <> parens av)
        _ -> throwE $ CPPGenE $ "Invalid Function Form " ++ show f
inline (tag &&& children -> (EOperate OSnd, [(tag &&& children -> (ETuple, [t, a])), v])) = do
    (te, tv) <- inline t
    (ae, av) <- inline a
    (ve, vv) <- inline v
    return (te PL.<//> ae PL.<//> ve , text "engine.send" <> tupled [av, tv, vv])
inline (tag &&& children -> (EOperate bop, [a, b])) = do
    (ae, av) <- inline a
    (be, bv) <- inline b
    bsym <- binarySymbol bop
    return (ae PL.<//> be, av <+> bsym <+> bv)
inline (tag &&& children -> (EProject v, [e])) = do
    (ee, ev) <- inline e
    return (ee, ev <> dot <> text v)

inline (tag &&& children -> (EAssign x, [e])) = (,text "unit_t" <> parens empty) <$> reify (RName x) e

inline e = do
    k <- genSym
    ct <- canonicalType e
    decl <- cDecl ct k
    effects <- reify (RName k) e
    return (decl PL.<//> effects, text k)

-- | The generic function to generate code for an expression whose result is to be reified. The
-- method of reification is indicated by the @RContext@ argument.
reify :: RContext -> K3 Expression -> CPPGenM CPPGenR

-- TODO: Is this the fix we need for the unnecessary reification issues?
reify RForget e@(tag -> EOperate OApp) = do
    (ee, ev) <- inline e
    return $ ee PL.<//> ev <> semi

reify r (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    be <- reify r b
    return $ ae PL.<$> be

reify r (tag &&& children -> (ELetIn x, [e, b])) = do
    ct <- canonicalType e
    d <- cDecl ct x
    ee <- reify (RName x) e
    be <- reify r b
    return $ hangBrace $ vsep [d, ee, be]

reify r (tag &&& children -> (ECaseOf x, [e, s, n])) = do
    ct <- canonicalType e
    d <- cDecl (head $ children ct) x
    (ee, ev) <- inline e
    se <- reify r s
    ne <- reify r n
    return $ ee PL.<//>
        text "if" <+> parens (ev <+> text "==" <+> text "null") <+>
        hangBrace (d PL.<$> text x <+> equals <+> text "*" <> ev <> semi PL.<//> se) <+> text "else" <+>
        hangBrace ne

reify r (tag &&& children -> (EBindAs b, [a, e])) = do
    (ae, g) <- case a of
        (tag -> EVariable _) -> inline a
        _ -> do
            g' <- genSym
            ae' <- reify (RName g') a
            return (ae', text g')

    ta <- canonicalType a

    bindInit <- case b of
            BIndirection i -> do
                let (tag &&& children -> (TIndirection, [ti])) = ta
                di <- cDecl ti i
                return $ di PL.<$> (text i <+> equals <+> text "*" <> g <> semi)
            BTuple is -> do
                let (tag &&& children -> (TTuple, ts)) = ta
                ds <- zipWithM cDecl ts is
                return $ vsep ds PL.<$> genCCall (text "tie") Nothing (map text is) <+> equals <+> g <> semi
            BRecord iis -> return $ vsep
                [text i <+> equals <+> g <> dot <> text v <> semi | (i, v) <- iis]

    let bindWriteback = case b of
            BIndirection i -> g <+> equals <+> genCCall (text "make_shared") Nothing [text i]
            BTuple is -> vcat (zipWith (genTupleAssign g) [0..] is)
            BRecord iis -> vcat (map (uncurry $ genRecordAssign g) iis)

    (bindBody, k) <- case r of
        RReturn -> do
            g' <- genSym
            te <- canonicalType e
            de <- cDecl te g'
            re <- reify (RName g') e
            return (de PL.<$> re, Just g')
        _ -> (,Nothing) <$> reify r e

    let bindCleanUp = maybe empty (\k' -> text "return" <+> text k' <> semi) k

    return $ ae PL.<$> hangBrace (bindInit PL.<$> bindBody PL.<$> bindWriteback PL.<$> bindCleanUp)
    where
    genTupleAssign g n i = genCCall (text "get") (Just $ [int n]) [g] <+> equals <+> text i <> semi
    genRecordAssign g k v = g <> dot <> text k <+> equals <+> text v <> semi

reify r (tag &&& children -> (EIfThenElse, [p, t, e])) = do
    (pe, pv) <- inline p
    te <- reify r t
    ee <- reify r e
    return $ pe PL.<//> text "if" <+> parens pv <+> hangBrace te PL.</> text "else" <+> hangBrace ee

reify r e = do
    (effects, value) <- inline e
    reification <- case r of
        RForget -> return empty
        RName k -> return $ text k <+> equals <+> value <> semi
        RReturn -> return $ text "return" <+> value <> semi
        RSplice f -> return $ f [value] <> semi
    return $ effects PL.<//> reification

declaration :: K3 Declaration -> CPPGenM CPPGenR
declaration (tag -> DGlobal i _ _) | "register" `L.isPrefixOf` i = return empty
declaration (tag -> DGlobal _ (tag -> TSource) _) = return empty
declaration (tag -> DGlobal i t Nothing) = cDecl t i
declaration (tag -> DGlobal i t@(tag &&& children -> (TFunction, [ta, tr]))
            (Just (tag &&& children -> (ELambda x, [b])))) = do
    newF <- cDecl t i
    modify (\s -> s { forwards = forwards s PL.<$$> newF })
    body <- reify RReturn b
    cta <- cType ta
    ctr <- cType tr
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
    i <- cType T.unit >>= \ctu ->
        return $ ctu <+> text "initGlobalDecls" <> parens empty <+> hangBrace (initializations currentS)
    let amp = annotationMap currentS
    compositeDecls <- forM (S.toList $ composites currentS) $ \(S.toList -> als) ->
        composite (annotationComboId als) [(a, M.findWithDefault [] a amp) | a <- als]
    recordDecls <- forM (M.toList $ recordMap currentS) $ \(k, idts) -> record k idts
    tablePop <- generateDispatchPopulation
    tableDecl <- return $ text "TriggerDispatch" <+> text "dispatch_table" <> semi

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
    tmpType <- cType t
    let triggerDispatch = text i <> parens (text "arg") <> semi
    let unpackCall = text "arg" <+> equals <+> text "engine.valueFormat->unpack"
            <> angles tmpType <> parens (text "msg")
    return $ genCFunction (text "void") (text i <> text "_dispatch") [text "string msg"] $ hangBrace (
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
    return $ genCFunction (text "void") (text "populate_dispatch") [] (vsep dispatchStatements)
  where
    genDispatch tName = return $
        text ("dispatch_table[\"" ++ tName ++ "\"] = " ++ genDispatchName tName) <> semi

genDispatchName :: Identifier -> Identifier
genDispatchName i = i ++ "_dispatch"

reserved :: [Identifier]
reserved = ["openBuiltin"]

hangBrace :: Doc -> Doc
hangBrace d = text "{" PL.<$$> indent 4 d PL.<$$> text "}"

templateLine :: [Doc] -> Doc
templateLine [] = empty
templateLine ts = text "template" <+> angles (sep $ punctuate comma [text "class" <+> td | td <- ts])

genCFunction :: Doc -> Doc -> [Doc] -> Doc -> Doc
genCFunction rt f args body = rt <+> f <> tupled args <+> hangBrace body

genCCall :: Doc -> Maybe [Doc] -> [Doc] -> Doc
genCCall f ts as = f <> (maybe empty $ \ts' -> angles (hcat $ punctuate comma ts')) ts <> tupled as

genCBoostSerialize :: [Identifier] -> CPPGenR
genCBoostSerialize dataDecls = serializeTemplateLine PL.<$$> serializeMethod
  where
    serializeTemplateLine = templateLine [(text "archive")]
    serializeMethod = genCFunction (text "void") (text "serialize") [(text "archive _archive")] serializeBody
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

    publicDecls <- mapM annMemDecl methDecls
    privateDecls <- mapM annMemDecl dataDecls

    let classBody = text "class" <+> text className <> colon <+> parentList
            <+> hangBrace (vsep $
                    if not (null publicDecls)
                        then [text "public:" PL.<$$> (indent 4 $ vsep $ punctuate line $
                            constructors' ++ [serializationDefn] ++ publicDecls)]
                        else []
                    ++ if not (null privateDecls)
                        then [text "private:" PL.<$$> (indent 4 $ vsep $ punctuate line privateDecls)]
                        else []
                )
            <> semi

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
    cta <- cType ta
    ctr <- cType tr
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
    globals' <- globals
    program' <- declaration d
    return $ vsep $ punctuate line [
            vsep genIncludes,
            vsep genNamespaces,
            vsep genAliases,
            globals',
            program'
        ]
  where
    genIncludes = [text "#include" <+> dquotes (text f) | f <- includes]
    genNamespaces = [text "using namespace" <+> (text n) <> semi | n <- namespaces]
    genAliases = [text "using" <+> text new <+> equals <+> text old <> semi | (new, old) <- aliases]

includes :: [Identifier]
includes = [
        -- Standard Library
        "memory",
        "sstream",
        "string",

        -- Boost
        "boost/archive/text_iarchive.hpp",

        -- K3 Runtime
        "runtime/cpp/Collections.hpp",
        "runtime/cpp/Dispatch.hpp"
    ]

namespaces :: [Identifier]
namespaces = ["std", "K3"]

aliases :: [(Identifier, Identifier)]
aliases = [
        ("unit_t", "struct {}")
    ]

globals :: CPPGenM CPPGenR
globals = do
    return $ text "Engine engine" <> semi
