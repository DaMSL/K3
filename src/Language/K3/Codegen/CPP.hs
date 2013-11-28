{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Language.K3.Codegen.CPP where

import Control.Arrow ((&&&))

import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Functor
import Data.Maybe
import Data.List (nub)

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
        composites :: S.Set (S.Set Identifier)
    } deriving Show

-- | Error messages thrown by C++ code generation.
data CPPGenE = CPPGenE String deriving (Eq, Read, Show)

-- | The C++ code generation monad.
type CPPGenM a = EitherT CPPGenE (State CPPGenS) a

-- | Reification context, used to determine how an expression should reify its result.
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

type CPPGenR = Doc

throwE :: CPPGenE -> CPPGenM a
throwE = left

runCPPGenM :: CPPGenS -> CPPGenM a -> (Either CPPGenE a, CPPGenS)
runCPPGenM s = flip runState s . runEitherT

defaultCPPGenS :: CPPGenS
defaultCPPGenS = CPPGenS 0 empty empty M.empty M.empty S.empty

genSym :: CPPGenM Identifier
genSym = do
    current <- uuid <$> get
    modify (\s -> s { uuid = succ (uuid s) })
    return $ '_':  show current

addAnnotation :: Identifier -> [AnnMemDecl] -> CPPGenM ()
addAnnotation i amds = modify (\s -> s { annotationMap = M.insert i amds (annotationMap s) })

addComposite :: [Identifier] -> CPPGenM ()
addComposite is = modify (\s -> s { composites = S.insert (S.fromList is) (composites s) })

addRecord :: Identifier -> [(Identifier, K3 Type)] -> CPPGenM ()
addRecord i its = modify (\s -> s { recordMap = M.insert i its (recordMap s) })

unarySymbol :: Operator -> CPPGenM CPPGenR
unarySymbol ONot = return $ text "!"
unarySymbol ONeg = return $ text "-"
unarySymbol u = throwE $ CPPGenE $ "Invalid Unary Operator " ++ show u

binarySymbol :: Operator -> CPPGenM CPPGenR
binarySymbol OAdd = return $ text "+"
binarySymbol OSub = return $ text "-"
binarySymbol OMul = return $ text "*"
binarySymbol ODiv = return $ text "/"
binarySymbol OEqu = return $ text "=="
binarySymbol ONeq = return $ text "!="
binarySymbol OLth = return $ text "<"
binarySymbol OLeq = return $ text "<="
binarySymbol OGth = return $ text ">"
binarySymbol OGeq = return $ text ">="
binarySymbol OAnd = return $ text "&&"
binarySymbol OOr = return $ text "||"
binarySymbol b = throwE $ CPPGenE $ "Invalid Binary Operator " ++ show b

constant :: Constant -> CPPGenM CPPGenR
constant (CBool True) = return $ text "true"
constant (CBool False) = return $ text "false"
constant (CInt i) = return $ int i
constant (CReal d) = return $ double d
constant (CString s) = return $ text "string" <> (parens . text $ show s)
constant (CNone _) = return $ text "null"
constant c = throwE $ CPPGenE $ "Invalid Constant Form " ++ show c

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
cType t@(tag -> TRecord _) = text <$> signature t
cType (tag -> TDeclaredVar t) = return $ text t

-- TODO: Three pieces of information necessary to generate a collection type:
--  1. The list of named annotations on the collections.
--  2. The types provided to each annotation to fulfill their type variable requirements.
--  3. The content type.
cType t@(tag &&& children &&& annotations -> (TCollection, ([et], as))) = do
    ct <- cType et
    case annotationComboIdT as of
        Nothing -> throwE $ CPPGenE $ "Invalid Annotation Combination for " ++ show t
        Just i' -> return $ text i' <> angles ct

-- TODO: Are these all the cases that need to be handled?
cType t = throwE $ CPPGenE $ "Invalid Type Form " ++ show t

-- TODO: This really needs to go somewhere else.
-- The correct procedure:
--  - Take the lower bound for everything except functions.
--  - For functions, take the upper bound for the argument type and lower bound for the return type,
--  put them together in a TFunction for the correct type.
canonicalType :: K3 Expression -> CPPGenM (K3 Type)
canonicalType (tag -> EConstant (CEmpty t)) = return $ T.collection t
canonicalType e@(tag -> ELambda _) = do
    ETypeLB elb <- case (e @~ (\case (ETypeLB _) -> True; _ -> False)) of
        Just x -> return x
        Nothing -> throwE $ CPPGenE $ "Invalid Lower Bound for " ++ show e
    ETypeUB eub <- case (e @~ (\case (ETypeUB _) -> True; _ -> False)) of
        Just x -> return x
        Nothing -> throwE $ CPPGenE $ "Invalid Upper Bound for " ++ show e

    aub <- case eub of
        (tag &&& children -> (TFunction, [a, _])) -> return a
        _ -> throwE $ CPPGenE $ "Invalid Function Type " ++ show eub

    rlb <- case elb of
        (tag &&& children -> (TFunction, [_, r])) -> return r
        _ -> throwE $ CPPGenE $ "Invalid Function Type " ++ show elb

    return $ T.function aub rlb

canonicalType e = case (e @~ (\case (ETypeLB _) -> True; _ -> False)) of
    Just (ETypeLB t) -> return t
    _ -> throwE $ CPPGenE $ "Unknown Lower Bound for type of " ++ show e

cDecl :: K3 Type -> Identifier -> CPPGenM CPPGenR
cDecl (tag &&& children -> (TFunction, [ta, tr])) i = do
    ctr <- cType tr
    cta <- cType ta
    return $ ctr <+> text i <> parens cta <> semi
cDecl t@(tag &&& annotations -> (TCollection, as)) i = case annotationComboIdT as of
    Nothing -> throwE $ CPPGenE $ "No Viable Annotation Combination for Declaration " ++ i
    Just _ -> addComposite (namedTAnnotations as) >> cType t >>= \ct -> return $ ct <+> text i <> semi
cDecl t i = cType t >>= \ct -> return $ ct <+> text i <> semi

inline :: K3 Expression -> CPPGenM (CPPGenR, CPPGenR)
inline e@(tag &&& annotations -> (EConstant (CEmpty t), as)) = case annotationComboIdE as of
    Nothing -> throwE $ CPPGenE $ "No Viable Annotation Combination for Empty " ++ show e
    Just ac -> cType t >>= \ct -> addComposite (namedEAnnotations as) >> return (empty, text ac <> angles ct)

inline (tag -> EConstant c) = (empty,) <$> constant c
inline (tag -> EVariable v) = return (empty, text v)
inline (tag &&& children -> (t', [c])) | t' == ESome || t' == EIndirect = do
    (e, v) <- inline c
    ct <- canonicalType c
    t <- cType ct
    return (e, text "shared_ptr" <> angles t <> parens v)
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
    return (ae PL.<//> be, bv)
inline (tag &&& children -> (EOperate OApp, [f, a])) = do
    (ae, av) <- inline a
    case f of
        (tag -> EVariable v) -> return (ae, text v <> parens av)
        (tag -> EProject _) -> do
            (pe, pv) <- inline f
            return (ae PL.<//> pe, pv <> parens av)
        _ -> throwE $ CPPGenE $ "Invalid Function Form " ++ show f
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

reify :: RContext -> K3 Expression -> CPPGenM CPPGenR

-- TODO: Is this the fix we need for the unnecessary reification issues?
reify RForget e@(tag -> EOperate OApp) = do
    (ee, ev) <- inline e
    return $ ee PL.<//> ev
reify r (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    be <- reify r b
    return $ ae PL.<//> be
reify r (tag &&& children -> (ELetIn x, [e, b])) = do
    ct <- canonicalType e
    d <- cDecl ct x
    ee <- reify (RName x) e
    be <- reify r b
    return $ braces $ vsep [d, ee, be]
reify r (tag &&& children -> (ECaseOf x, [e, s, n])) = do
    ct <- canonicalType e
    d <- cDecl (head $ children ct) x
    (ee, ev) <- inline e
    se <- reify r s
    ne <- reify r n
    return $ ee PL.<//>
        text "if" <+> parens (ev <+> text "==" <+> text "null") <+>
        braces (d PL.<//> text x <+> equals <+> text "*" <> ev <> semi PL.<//> se) <+> text "else" <+>
        braces ne
reify r (tag &&& children -> (EBindAs b, [a, e])) = do
    (ae, g) <- case a of
        (tag -> EVariable v) -> return (empty, text v)
        _ -> do
            g' <- genSym
            ae' <- reify (RName g') a
            return (ae', text g')
    ee <- reify r e
    return $ (ae PL.<//>) . braces . (PL.<//> ee) $ case b of
        BIndirection i -> text i <+> equals <+> text "*" <> g <> semi
        BTuple is -> vsep
            [text i <+> equals <+> text "get" <> angles (int x) <> parens g <> semi | (i, x) <- zip is [0..]]
        BRecord iis -> vsep
            [text i <+> equals <+> g <> dot <> text v <> semi | (i, v) <- iis]
reify r (tag &&& children -> (EIfThenElse, [p, t, e])) = do
    (pe, pv) <- inline p
    te <- reify r t
    ee <- reify r e
    return $ pe PL.<//> hang 4 (text "if" <+> parens pv <+> braces te <+> text "else" <+> braces ee)
reify r e = do
    (effects, value) <- inline e
    return $ effects PL.<//> case r of
        RForget -> empty
        RName k -> text k <+> equals <+> value <> semi
        RReturn -> text "return" <+> value <> semi
        RSplice f -> f [value]

declaration :: K3 Declaration -> CPPGenM CPPGenR
declaration d | isJust (d @~ isGeneratedSpan) = return empty
  where
    isGeneratedSpan (DSpan (GeneratedSpan _)) = True
    isGeneratedSpan _ = False
declaration (tag -> DGlobal _ (tag -> TSource) _) = return empty
declaration (tag -> DGlobal i t Nothing) = cDecl t i
declaration (tag -> DGlobal i t@(tag &&& children -> (TFunction, [ta, tr]))
            (Just (tag &&& children -> (ELambda x, [b])))) = do
    newF <- cDecl t i
    modify (\s -> s { forwards = forwards s PL.<//> newF })
    body <- reify RReturn b
    cta <- cType ta
    ctr <- cType tr
    return $ ctr <+> text i <> parens (cta <+> text x) <+> braces body
declaration (tag -> DGlobal i t (Just e)) = do
    newI <- reify (RName i) e
    modify (\s -> s { initializations = initializations s PL.<//> newI })
    cDecl t i
declaration (tag -> DTrigger i t e) = declaration (D.global i (T.function t T.unit) (Just e))
declaration (tag &&& children -> (DRole n, cs)) = do
    subDecls <- vsep <$> mapM declaration cs
    return $ text "namespace" <+> text n <+> braces subDecls
declaration (tag -> DAnnotation i _ amds) = addAnnotation i amds >> return empty
declaration _ = return empty

reserved :: [Identifier]
reserved = ["openBuiltin"]

composite :: Identifier -> [(Identifier, [AnnMemDecl])] -> CPPGenM CPPGenR
composite cName ans = do
    members <- vsep <$> mapM annMemDecl positives
    return $ text "class" <+> text cName <+> braces members
  where
    onlyPositives :: AnnMemDecl -> Bool
    onlyPositives (Lifted Provides _ _ _ _) = True
    onlyPositives (Attribute Provides _ _ _ _) = True
    onlyPositives (MAnnotation Provides _ _) = True
    onlypositives _ = False

    positives = filter onlyPositives (concat . snd $ unzip ans)

annMemDecl :: AnnMemDecl -> CPPGenM CPPGenR
annMemDecl (Lifted _ i t me _)  = do
    memDefinition <- definition i t me
    return $ templateLine PL.<$$> memDefinition
  where
    findTypeVars :: K3 Type -> [Identifier]
    findTypeVars (tag -> TDeclaredVar v) = [v]
    findTypeVars (children -> []) = []
    findTypeVars (children -> ts) = concatMap findTypeVars ts

    typeVars = nub $ findTypeVars t

    templateLine = if null typeVars
        then empty
        else (text "template" <> angles (sep $ punctuate comma $ map text typeVars))

annMemDecl (Attribute _ i _ _ _) = return $ text i
annMemDecl (MAnnotation _ i _) = return $ text i

definition :: Identifier -> K3 Type -> Maybe (K3 Expression) -> CPPGenM CPPGenR
definition i t@(tag &&& children -> (TFunction, [ta, tr])) (Just (tag &&& children -> (ELambda x, [b]))) = do
    body <- reify RReturn b
    return $ body
    cta <- cType ta
    ctr <- cType tr
    return $ ctr <+> text i <> parens (cta <+> text x) <+> braces body
definition i t (Just e) = do
    newI <- reify (RName i) e
    d <- cDecl t i
    return $ newI PL.<//> d
definition i t Nothing = cDecl t i

program :: K3 Declaration -> CPPGenM CPPGenR
program d = do
    p <- declaration d
    currentS <- get
    i <- cType T.unit >>= \ctu ->
        return $ ctu <+> text "initGlobalDecls" <> parens empty <+> braces (initializations currentS)
    let amp = annotationMap currentS
    compositeDecls <- forM (S.toList $ composites currentS) $ \(S.toList -> als) ->
        composite (annotationComboId als) [(a, M.findWithDefault [] a amp) | a <- als]
    return $ vsep $ compositeDecls ++ [forwards currentS, i, p]
