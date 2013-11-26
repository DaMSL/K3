{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP where

import Control.Arrow ((&&&))

import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Functor

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

data CPPGenS = CPPGenS { uuid :: Int, initializations :: CPPGenR, forwards :: CPPGenR } deriving Show
data CPPGenE = CPPGenE deriving (Eq, Read, Show)

type CPPGenM a = EitherT CPPGenE (State CPPGenS) a

data RContext
    = RForget
    | RReturn
    | RName Identifier
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
defaultCPPGenS = CPPGenS 0 empty empty

genSym :: CPPGenM Identifier
genSym = do
    current <- uuid <$> get
    modify (\s -> s { uuid = succ (uuid s) })
    return $ '_':  show current

include :: String -> CPPGenM ()
include = undefined

namespace :: String -> CPPGenM ()
namespace = undefined

unarySymbol :: Operator -> CPPGenM CPPGenR
unarySymbol ONot = return $ text "!"
unarySymbol ONeg = return $ text "-"
unarySymbol _ = throwE CPPGenE

unary :: Operator -> K3 Expression -> CPPGenM Doc
unary op a = do
    a' <- expression a
    us <- unarySymbol op
    return $ us <> parens a'

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
binarySymbol _ = throwE CPPGenE

binary :: Operator -> K3 Expression -> K3 Expression -> CPPGenM Doc
binary op a b = do
    a' <- expression a
    b' <- expression b
    bs <- binarySymbol op
    return $ parens $ a' <+> bs <+> b'

constant :: Constant -> CPPGenM CPPGenR
constant (CBool True) = return $ text "true"
constant (CBool False) = return $ text "false"
constant (CInt i) = return $ int i
constant (CReal d) = return $ double d
constant (CString s) = return $ text "string" <> (parens . text $ show s)
constant (CNone _) = return $ text "null"
constant _ = throwE CPPGenE

cType :: K3 Type -> CPPGenM CPPGenR
cType (tag -> TBool) = return $ text "bool"
cType (tag -> TByte) = return $ text "unsigned char"
cType (tag -> TInt) = return $ text "int"
cType (tag -> TReal) = return $ text "double"
cType (tag -> TString) = return $ text "string"
cType (tag &&& children -> (TOption, [t])) = (text "shared_ptr" <>) . angles <$> cType t
cType (tag &&& children -> (TIndirection, [t])) = (text "shared_ptr" <>) . angles <$> cType t
cType (tag &&& children -> (TTuple, ts))
    = (text "tuple" <>) . angles . sep . punctuate comma <$> mapM cType ts
cType (tag &&& children -> (TRecord ids, ts)) = return . text . recordName $ zip ids ts

-- TODO: Three pieces of information necessary to generate a collection type:
--  1. The list of named annotations on the collections.
--  2. The types provided to each annotation to fulfill their type variable requirements.
--  3. The content type.
cType (tag &&& children &&& annotations -> (TCollection, (_, _))) = throwE CPPGenE

-- TODO: Are these all the cases that need to be handled?
cType _ = throwE CPPGenE

-- TODO: This really needs to go somewhere else.
canonicalType :: K3 Expression -> K3 Type
canonicalType = undefined

cDecl :: K3 Type -> Identifier -> CPPGenM CPPGenR
cDecl (tag &&& children -> (TFunction, [ta, tr])) i = do
    ctr <- cType tr
    cta <- cType ta
    return $ ctr <+> text i <> parens cta <> semi
cDecl t i = cType t >>= \ct -> return $ ct <+> text i <> semi

inline :: K3 Expression -> CPPGenM (CPPGenR, CPPGenR)
inline (tag -> EConstant c) = (empty,) <$> constant c
inline (tag -> EVariable v) = return (empty, text v)
inline (tag &&& children -> (t', [c])) | t' == ESome || t' == EIndirect = do
    (e, v) <- inline c
    t <- cType (canonicalType c)
    return (e, text "shared_ptr" <> angles t <> parens v)
inline (tag &&& children -> (ETuple, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    return (vsep es, text "make_tuple" <> tupled vs)
inline (tag &&& children -> (ERecord ids, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    return (vsep es, text (recordName $ zip ids (map canonicalType cs)) <> tupled vs)
inline (tag &&& children -> (EOperate uop, [c])) = do
    (ce, cv) <- inline c
    usym <- unarySymbol uop
    return (ce, usym <> cv)
inline (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    (be, bv) <- inline b
    return (ae PL.<$> be, bv)
inline (tag &&& children -> (EOperate OApp, [f, a])) = do
    (ae, av) <- inline a
    case f of
        (tag -> EVariable v) -> return $ (ae, text v <> parens av)
        (tag -> EProject _) -> do
            (pe, pv) <- inline f
            return (ae PL.<$> pe, pv <> parens av)
        _ -> throwE CPPGenE
inline (tag &&& children -> (EOperate bop, [a, b])) = do
    (ae, av) <- inline a
    (be, bv) <- inline b
    bsym <- binarySymbol bop
    return (ae PL.<$> be, av <+> bsym <+> bv)
inline (tag &&& children -> (EProject v, [e])) = do
    (ee, ev) <- inline e
    return (ee, ev <> dot <> text v)
inline (tag &&& children -> (EAssign x, [e])) = (,text "null") <$> reify (RName x) e
inline e = do
    k <- genSym
    decl <- cDecl (canonicalType e) k
    effects <- reify (RName k) e
    return (decl PL.<$> effects, text k)

reify :: RContext -> K3 Expression -> CPPGenM CPPGenR

-- TODO: Is this the fix we need for the unnecessary reification issues?
reify r (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    be <- reify r b
    return $ ae PL.<$> be
reify r (tag &&& children -> (ELetIn x, [e, b])) = do
    d <- cDecl (canonicalType e) x
    ee <- reify (RName x) e
    be <- reify r b
    return $ braces $ vsep [d, ee, be]
reify r (tag &&& children -> (ECaseOf x, [e, s, n])) = do
    d <- cDecl (head . children $ canonicalType e) x
    (ee, ev) <- inline e
    se <- reify r s
    ne <- reify r n
    return $ ee PL.<$>
        text "if" <+> parens (ev <+> text "==" <+> text "null") <+>
        braces (d PL.<$> text x <+> equals <+> text "*" <> ev <> semi PL.<$> se) <+> text "else" <+>
        braces ne
reify r (tag &&& children -> (EBindAs b, [a, e])) = do
    (ae, g) <- case a of
        (tag -> EVariable v) -> return (empty, text v)
        _ -> do
            g' <- genSym
            ae' <- reify (RName g') a
            return (ae', text g')
    ee <- reify r e
    return $ (ae PL.<$>) . braces . (PL.<$> ee) $ case b of
        BIndirection i -> text i <+> equals <+> text "*" <> g <> semi
        BTuple is -> vsep
            [text i <+> equals <+> text "get" <> angles (int x) <> parens g <> semi | (i, x) <- zip is [0..]]
        BRecord iis -> vsep
            [text i <+> equals <+> g <> dot <> text v <> semi | (i, v) <- iis]
reify r (tag &&& children -> (EIfThenElse, [p, t, e])) = do
    (pe, pv) <- inline p
    te <- reify r t
    ee <- reify r e
    return $ pe PL.<$> hang 4 (text "if" <+> parens pv <+> braces te <+> text "else" <+> braces ee)
reify r e = do
    (effects, value) <- inline e
    return $ effects PL.<$> case r of
        RForget -> empty
        RName k -> text k <+> equals <+> value <> semi
        RReturn -> text "return" <+> value <> semi
        RSplice f -> f [value]

expression :: K3 Expression -> CPPGenM Doc
expression _ = throwE CPPGenE

declaration :: K3 Declaration -> CPPGenM CPPGenR
declaration (tag -> DGlobal i t Nothing) = cDecl t i
declaration (tag -> DGlobal i t@(tag &&& children -> (TFunction, [ta, tr]))
            (Just (tag &&& children -> (ELambda x, [b])))) = do
    newF <- cDecl t i
    modify (\s -> s { forwards = forwards s PL.<$> newF })
    body <- reify RReturn b
    cta <- cType ta
    ctr <- cType tr
    return $ ctr <+> text i <> parens (cta <+> text x) <+> braces body
declaration (tag -> DGlobal i t (Just e)) = do
    newI <- reify (RName i) e
    modify (\s -> s { initializations = initializations s PL.<$> newI })
    cDecl t i
declaration (tag -> DTrigger i t e) = declaration (D.global i t (Just e))
declaration (tag &&& children -> (DRole n, cs)) = do
    subDecls <- vsep <$> mapM declaration cs
    return $ text "namespace" <+> text n <+> braces subDecls
declaration _ = return empty

program :: K3 Declaration -> CPPGenM CPPGenR
program d = do
    p <- declaration d
    currentS <- get
    i <- cType T.unit >>= \ctu -> return $ ctu <+> text "atInit" <> parens ctu <+> braces (initializations currentS)
    return $ vsep [forwards currentS, i, p]
