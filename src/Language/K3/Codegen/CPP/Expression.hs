{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Expression where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Functor
import Data.List (nub, (\\))
import Data.Maybe

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Common
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

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
constant (CNone _) = return $ text "nullptr"
constant c = throwE $ CPPGenE $ "Invalid Constant Form " ++ show c

cDecl :: K3 Type -> Identifier -> CPPGenM CPPGenR
cDecl (tag &&& children -> (TFunction, [ta, tr])) i = do
    ctr <- genCType tr
    cta <- genCType ta
    return $ ctr <+> text i <> parens cta <> semi
cDecl t i = do
    when (tag t == TCollection) $ addComposite (namedTAnnotations $ annotations t)
    ct <- genCType t
    ci <- return $ text i
    return $ genCDecl ct ci Nothing

inline :: K3 Expression -> CPPGenM (CPPGenR, CPPGenR)
inline e@(tag &&& annotations -> (EConstant (CEmpty t), as)) = case annotationComboIdE as of
    Nothing -> throwE $ CPPGenE $ "No Viable Annotation Combination for Empty " ++ show e
    Just ac -> do
        ct <- genCType t
        addComposite (namedEAnnotations as)
        return (empty, text ac <> angles ct <> parens empty)

inline (tag -> EConstant c) = (empty,) <$> constant c

-- If a variable was declared as mutable it's been reified as a shared_ptr, and must be
-- dereferenced.
inline e@(tag -> EVariable v) = return $ if isJust $ e @~ (\case { EMutable -> True; _ -> False })
        then (empty, text "*" <> text v)
        else (empty, text v)

inline (tag &&& children -> (t', [c])) | t' == ESome || t' == EIndirect = do
    (e, v) <- inline c
    ct <- getKType c
    t <- genCType ct
    return (e, text "shared_ptr" <> angles t <> parens (text "new" <+> t <> parens v))
inline (tag &&& children -> (ETuple, [])) = return (empty, text "unit_t" <> parens empty)
inline (tag &&& children -> (ETuple, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    return (vsep es, text "make_tuple" <> tupled vs)
inline e@(tag &&& children -> (ERecord _, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    t <- getKType e
    case t of
        (tag &&& children -> (TRecord _, _)) -> do
            sig <- genCType t
            return (vsep es, sig <> braces (cat $ punctuate comma vs))
        _ -> throwE $ CPPGenE $ "Invalid Record Type " ++ show t

inline (tag &&& children -> (EOperate uop, [c])) = do
    (ce, cv) <- inline c
    usym <- unarySymbol uop
    return (ce, usym <> cv)
inline (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    (be, bv) <- inline b
    return (ae <$$> be, bv)
inline e@(tag &&& children -> (ELambda arg, [body])) = do
    (ta, _) <- getKType e >>= \case
        (tag &&& children -> (TFunction, [ta, tr])) -> do
            ta' <- genCType ta
            tr' <- genCType tr
            return (ta', tr')
        _ -> throwE $ CPPGenE "Invalid Function Form"
    exc <- globals <$> get
    let fvs = nub $ filter (/= arg) $ freeVariables body
    body' <- reify RReturn body
    return (empty, list (map text $ fvs \\ exc) <+> parens (ta <+> text arg) <+> hangBrace body')
inline (tag &&& children -> (EOperate OApp, [f, a])) = do
    -- Inline both function and argument for call.
    (fe, fv) <- inline f
    (ae, av) <- inline a

    return (fe <$$> ae, fv <> parens av)
inline (tag &&& children -> (EOperate OSnd, [tag &&& children -> (ETuple, [trig@(tag -> EVariable trigNm), addr]), val])) = do
    (te, tv)  <- inline trig
    (ae, av) <- inline addr
    (ve, vv) <- inline val
    let className = genDispatchClassName trigNm
        classInst = genCCall (text $ "auto d = boost::make_shared<"++className++">") Nothing [vv]
    return (
            vsep [te, ae, ve,
                  classInst <> semi,
                  text "engine.send" <> tupled [av, dquotes tv, text "d"]] <> semi,
            text "unit_t()"
        )

inline (tag &&& children -> (EOperate bop, [a, b])) = do
    (ae, av) <- inline a
    (be, bv) <- inline b
    bsym <- binarySymbol bop
    return (ae <//> be, av <+> bsym <+> bv)

inline (tag &&& children -> (EProject v, [e])) = do
    (ee, ev) <- inline e
    return (ee, ev <> dot <> text v)

inline (tag &&& children -> (EAssign x, [e])) = (,text "unit_t" <> parens empty) <$> reify (RName x) e

inline (tag &&& children -> (EAddress, [h, p])) = do
    (he, hv) <- inline h
    (pe, pv) <- inline p
    return (he <$$> pe, genCCall (text "make_address") Nothing [hv, pv])

inline e = do
    k <- genSym
    ct <- getKType e
    decl <- cDecl ct k
    effects <- reify (RName k) e
    return (decl <//> effects, text k)

-- | The generic function to generate code for an expression whose result is to be reified. The
-- method of reification is indicated by the @RContext@ argument.
reify :: RContext -> K3 Expression -> CPPGenM CPPGenR

-- TODO: Is this the fix we need for the unnecessary reification issues?
reify RForget e@(tag -> EOperate OApp) = do
    (ee, ev) <- inline e
    return $ ee <//> ev <> semi

reify r (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    be <- reify r b
    return $ ae <$$> be

reify r (tag &&& children -> (ELetIn x, [e, b])) = do
    ct <- getKType e
    d <- cDecl ct x
    ee <- reify (RName x) e
    be <- reify r b
    return $ hangBrace $ vsep [d, ee, be]

reify r (tag &&& children -> (ECaseOf x, [e, s, n])) = do
    ct <- getKType e
    d <- cDecl (head $ children ct) x
    g <- genSym
    p <- cDecl ct g
    ee <- reify (RName g) e
    se <- reify r s
    ne <- reify r n
    return $ p <$$> ee <$$>
        text "if" <+> parens (text g) <+>
        hangBrace (d <$$> text x <+> equals <+> text "*" <> text g <> semi <//> se) <+> text "else" <+>
        hangBrace ne

reify r (tag &&& children -> (EBindAs b, [a, e])) = do
    (ae, g) <- case a of
        (tag -> EVariable _) -> inline a
        _ -> do
            g' <- genSym
            ae' <- reify (RName g') a
            return (ae', text g')

    ta <- getKType a

    bindInit <- case b of
            BIndirection i -> do
                let (tag &&& children -> (TIndirection, [ti])) = ta
                di <- cDecl ti i
                return $ di <$$> (text i <+> equals <+> text "*" <> g <> semi)
            BTuple is -> do
                let (tag &&& children -> (TTuple, ts)) = ta
                ds <- zipWithM cDecl ts is
                return $ vsep ds <$$> genCCall (text "tie") Nothing (map text is) <+> equals <+> g <> semi
            BRecord iis -> return $ vsep
                [text i <+> equals <+> g <> dot <> text v <> semi | (i, v) <- iis]

    let bindWriteback = case b of
            BIndirection i -> g <+> equals <+> genCCall (text "make_shared") Nothing [text i]
            BTuple is -> vcat (zipWith (genTupleAssign g) [0..] is)
            BRecord iis -> vcat (map (uncurry $ genRecordAssign g) iis)

    (bindBody, k) <- case r of
        RReturn -> do
            g' <- genSym
            te <- getKType e
            de <- cDecl te g'
            re <- reify (RName g') e
            return (de <$$> re, Just g')
        _ -> (,Nothing) <$> reify r e

    let bindCleanUp = maybe empty (\k' -> text "return" <+> text k' <> semi) k

    return $ ae <$$> hangBrace (bindInit <$$> bindBody <$$> bindWriteback <$$> bindCleanUp)
    where
    genTupleAssign g n i = genCCall (text "get") (Just [int n]) [g] <+> equals <+> text i <> semi
    genRecordAssign g k v = g <> dot <> text k <+> equals <+> text v <> semi

reify r (tag &&& children -> (EIfThenElse, [p, t, e])) = do
    (pe, pv) <- inline p
    te <- reify r t
    ee <- reify r e
    return $ pe <//> text "if" <+> parens pv <+> hangBrace te </> text "else" <+> hangBrace ee

reify r e = do
    (effects, value) <- inline e
    reification <- case r of
        RForget -> return empty
        RName k -> return $ text k <+> equals <+> value <> semi
        RReturn -> return $ text "return" <+> value <> semi
        RSplice f -> return $ f [value] <> semi
    return $ effects <//> reification
