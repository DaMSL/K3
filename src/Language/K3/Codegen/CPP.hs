{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP where

import Control.Arrow ((&&&))

import Control.Monad.State
import Control.Monad.Trans.Either

import Text.PrettyPrint.ANSI.Leijen

import Language.K3.Core.Annotation
import Language.K3.Core.Expression

type CPPGenS = ()
type CPPGenE = ()

type CPPGenM a = EitherT CPPGenE (State CPPGenS) a

throwE = left

runCPPGenM :: CPPGenS -> CPPGenM a -> (Either CPPGenE a, CPPGenS)
runCPPGenM s = flip runState s . runEitherT

constant :: Constant -> CPPGenM Doc
constant (CBool True) = return $ text "true"
constant (CBool False) = return $ text "false"
constant (CInt i) = return $ int i
constant (CReal d) = return $ double d

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

binarySymbol :: Operator -> CPPGenM String
binarySymbol OAdd = return "+"
binarySymbol OSub = return "-"
binarySymbol OMul = return "*"
binarySymbol ODiv = return "/"
binarySymbol OEqu = return "=="
binarySymbol ONeq = return "!="
binarySymbol OLth = return "<"
binarySymbol OLeq = return "<="
binarySymbol OGth = return ">"
binarySymbol OGeq = return ">="
binarySymbol OAnd = return "&&"
binarySymbol OOr = return "||"
binarySymbol _ = throwE ()

unarySymbol :: Operator -> CPPGenM String
unarySymbol ONot = return "!"
unarySymbol ONeg = return "-"
unarySymbol _ = throwE ()
