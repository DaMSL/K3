{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP where

import Control.Arrow ((&&&))

import Control.Monad.State
import Control.Monad.Trans.Either

import qualified Data.Set as S

import Text.PrettyPrint.ANSI.Leijen

import Language.K3.Core.Annotation
import Language.K3.Core.Expression

type CPPGenS = ()
data CPPGenE = CPPGenE

type CPPGenM a = EitherT CPPGenE (State CPPGenS) a

data RContext
    = RForget
    | RReturn
    | RName Identifier
    | RSplice ([CPPGenR] -> CPPGenR)

instance Show RContext where
    show RForget = "RForget"
    show RReturn = "RReturn"
    show RName i = "RName \"" ++ i ++ "\""
    show RSplice f = "RSplice <opaque>"

newtype CPPGenR = Doc

throwE :: CPPGenE -> CPPGenM a
throwE = left

runCPPGenM :: CPPGenS -> CPPGenM a -> (Either CPPGenE a, CPPGenS)
runCPPGenM s = flip runState s . runEitherT

unarySymbol :: Operator -> CPPGenM CPPGenR
unarySymbol ONot = return $ text "!"
unarySymbol ONeg = return $ text "-"
unarySymbol _ = throwE CPPGenE

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

constant :: Constant -> CPPGenM CPPGenR
constant (CBool True) = return $ text "true"
constant (CBool False) = return $ text "false"
constant (CInt i) = return $ int i
constant (CReal d) = return $ double d
constant (CString s) = return $ text "string" <> (parens . text $ show s)
constant (CNone _) = return $ text "null"

cType :: K3 Tyype -> CPPGenM CPPGenR
cType (tag -> TBool) = return $ text "bool"
cType (tag -> TByte) = return $ text "unsigned char"
cType (tag -> TInt) = return $ text "int"
cType (tag -> TReal) = return $ text "double"
cType (tag &&& children -> (TOption, [t])) = return . text $ "shared_ptr" <> angles (cType t)
cType (tag &&& children -> (TIndirection, [t])) = return $ "shared_ptr" <> angles (cType t)
cType (tag &&& children -> (TTuple, ts)) = return $ "tuple" <> angles . sep . punctuate comma $ map cType ts
cType (tag &&& children -> (TRecord ids, ts)) = return . text . recordName $ zip ids ts

-- TODO: Three pieces of information necessary to generate a collection type:
--  1. The list of named annotations on the collections.
--  2. The types provided to each annotation to fulfill their type variable requirements.
--  3. The content type.
cType (tag &&& children &&& annotations -> (TCollection, (cs, as))) = throwE CPPGenE

-- TODO: Are these all the cases that need to be handled?
cType _ = throwE CPPGenE

expression :: K3 Expression -> CPPGenM Doc
expression (tag -> EConstant c) = constant c
expression (tag &&& children -> (EIfThenElse, [p, t, e])) = do
    p' <- expression p
    t' <- expression t
    e' <- expression e
    return $ text "if" <+> parens p' <+> braces t' <+> text "else" <+> braces e'

expression (tag &&& children -> (EOperate op, [a])) = unary op a
expression (tag &&& children -> (EOperate op, [a, b])) = binary op a b

unary :: Operator -> K3 Expression -> CPPGenM Doc
unary op a = do
    a' <- expression a
    us <- unarySymbol op
    return $ text us <> parens a'

binary :: Operator -> K3 Expression -> K3 Expression -> CPPGenM Doc
binary op a b = do
    a' <- expression a
    b' <- expression b
    bs <- binarySymbol op
    return $ parens $ a' <+> text bs <+> b'
