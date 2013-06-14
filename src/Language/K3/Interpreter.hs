{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

-- | The K3 Interpreter
module Language.K3.Interpreter (
    -- | Types
    Value(..),

    Interpretation,
    InterpretationError,

    IEnvironment,
    ILog,

    -- | Interpreters
    runInterpretation,

    expression,
    program
) where

import Control.Applicative
import Control.Arrow

import Control.Monad.Identity
import Control.Monad.IO.Class
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Function
import Data.IORef
import Data.List
import Data.Ord
import Data.Tree
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Type
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

-- | K3 Values
data Value
    = VBool Bool
    | VByte Word8
    | VInt Int
    | VReal Double
    | VString String
    | VOption (Maybe Value)
    | VTuple [Value]
    | VRecord [(Identifier, Value)]
    | VCollection [Value]
    | VIndirection (IORef Value)
    | VFunction (Value -> Interpretation Value)

-- We can't deriving Show because IORefs aren't showable.
instance Show Value where
    show (VBool b) = "VBool " ++ show b
    show (VByte b) = "VByte " ++ show b
    show (VInt i) = "VInt " ++ show i
    show (VReal r) = "VReal " ++ show r
    show (VString s) = "VString " ++ show s
    show (VOption m) = "VOption " ++ show m
    show (VTuple t) = "VTuple " ++ show t
    show (VRecord r) = "VRecord " ++ show r
    show (VCollection c) = "VCollection " ++ show c
    show (VIndirection i) = "VIndirection <opaque>"
    {- show (VFunction f i e) = "VFunction <function>" -}

-- | Interpretation event log.
type ILog = [String]

-- | Interpretation Environment.
type IEnvironment = [(Identifier, Value)]

-- | Errors encountered during interpretation.
data InterpretationError
    = RunTimeInterpretationError String
    | RunTimeTypeError String
  deriving (Eq, Read, Show)

-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IEnvironment (WriterT ILog IO))

-- | Run an interpretation to get a value or error, resulting environment and event log.
runInterpretation :: IEnvironment -> Interpretation a -> IO ((Either InterpretationError a, IEnvironment), ILog)
runInterpretation e = runWriterT . flip runStateT e . runEitherT

-- | Raise an error inside an interpretation. The error will be captured alongside the event log
-- till date, and the current state.
throwE :: InterpretationError -> Interpretation a
throwE = Control.Monad.Trans.Either.left

-- | Look-up the environment, with a thrown error if unsuccessful.
lookupE :: Identifier -> Interpretation Value
lookupE n = get >>= maybe (throwE $ RunTimeTypeError $ "Unknown Variable: '" ++ n ++ "'") return . lookup n

children = subForest

-- | Interpretation of Constants.
constant :: Constant -> Interpretation Value
constant (CBool b) = return $ VBool b
constant (CInt i) = return $ VInt i
constant (CByte w) = return $ VByte w
constant (CReal r) = return $ VReal r
constant (CString s) = return $ VString s
constant CNone = return $ VOption Nothing
constant CEmpty = return $ VCollection []

-- | Common Numeric-Operation handling, with casing for int/real promotion.
numeric :: (forall a. Num a => a -> a -> a) -> K3 Expression -> K3 Expression -> Interpretation Value
numeric op a b = do
    a' <- expression a
    b' <- expression b
    case (a', b') of
        (VInt x, VInt y) -> return $ VInt $ op x y
        (VInt x, VReal y) -> return $ VReal $ op (fromIntegral x) y
        (VReal x, VInt y) -> return $ VReal $ op x (fromIntegral y)
        (VReal x, VReal y) -> return $ VReal $ op x y

-- | Common boolean operation handling.
logic :: (Bool -> Bool -> Bool) -> K3 Expression -> K3 Expression -> Interpretation Value
logic op a b = do
    a' <- expression a
    b' <- expression b

    case (a', b') of
        (VBool x, VBool y) -> return $ VBool $ op x y
        _ -> throwE $ RunTimeTypeError "Invalid Boolean Operation"

-- | Common comparison operation handling.
comparison :: (forall a. Ord a => a -> a -> Bool) -> K3 Expression -> K3 Expression -> Interpretation Value
comparison op a b = do
    a' <- expression a
    b' <- expression b

    case (a', b') of
        (VBool x, VBool y) -> return $ VBool $ op x y
        (VInt x, VInt y) -> return $ VBool $ op x y
        (VReal x, VReal y) -> return $ VBool $ op x y
        (VString x, VString y) -> return $ VBool $ op x y

-- | Interpretation of unary operators.
unary :: Operator -> K3 Expression -> Interpretation Value

-- | Interpretation of unary negation of numbers.
unary ONeg a = expression a >>= \case
    VInt i -> return $ VInt (negate i)
    VReal r -> return $ VReal (negate r)
    _ -> throwE $ RunTimeTypeError "Invalid Negation"

-- | Interpretation of unary negation of booleans.
unary ONot a = expression a >>= \case
    VBool b -> return $ VBool (not b)
    _ -> throwE $ RunTimeTypeError "Invalid Complement"

unary _ _ = throwE $ RunTimeTypeError "Invalid Unary Operator"

-- | Interpretation of binary operators.
binary :: Operator -> K3 Expression -> K3 Expression -> Interpretation Value

-- | Standard numeric operators.
binary OAdd = numeric (+)
binary OSub = numeric subtract
binary OMul = numeric (*)

-- | Division handled similarly, but accounting zero-division errors.
binary ODiv = \a b -> do
    a' <- expression a
    b' <- expression b

    case b' of
        VInt 0 -> throwE $ RunTimeInterpretationError $ "Division by Zero"
        VReal 0 -> throwE $ RunTimeInterpretationError $ "Division by Zero"

    case (a', b') of
        (VInt x, VInt y) -> return $ VInt $ x `div` y
        (VInt x, VReal y) -> return $ VReal $ fromIntegral x / y
        (VReal x, VInt y) -> return $ VReal $ x / (fromIntegral y)
        (VReal x, VReal y) -> return $ VReal $ x / y

-- | Logical Operators
binary OAnd = logic (&&)
binary OOr  = logic (||)

-- | Function Application
binary OApp = \f x -> do
    f' <- expression f
    x' <- expression x

    case f' of
        VFunction b -> b x'
        _ -> throwE $ RunTimeTypeError "Invalid Function Application"

-- | Comparison Operators
binary OEqu = comparison (==)
binary ONeq = comparison (/=)
binary OLth = comparison (<)
binary OLeq = comparison (<=)
binary OGth = comparison (>)
binary OGeq = comparison (>=)

-- | Message Passing
binary OSnd = binary OApp

-- | Interpretation of Expressions
expression :: K3 Expression -> Interpretation Value

-- | Interpretation of constant expressions.
expression (tag -> EConstant c) = constant c

-- | Interpretation of variable lookups.
expression (tag -> EVariable i) = lookupE i

-- | Interpretation of option type construction expressions.
expression (tag &&& children -> (ESome, [x])) = expression x >>= return . VOption . Just
expression (tag -> ESome) = throwE $ RunTimeTypeError "Invalid Construction of Option"

-- | Interpretation of indirection type construction expressions.
expression (tag &&& children -> (EIndirect, [x])) = expression x >>= liftIO . newIORef >>= return . VIndirection
expression (tag -> EIndirect) = throwE $ RunTimeTypeError "Invalid Construction of Indirection"

-- | Interpretation of tuple construction expressions.
expression (tag &&& children -> (ETuple, cs)) = mapM expression cs >>= return . VTuple

-- | Interpretation of record construction expressions.
expression (tag &&& children -> (ERecord is, cs)) = mapM expression cs >>= return . VRecord . zip is

-- | Interpretation of function construction.
expression (tag &&& children -> (ELambda i, [b])) = return $ VFunction $ \v -> modify ((i, v):) >> expression b

-- | Interpretation of unary/binary operators.
expression (tag &&& children -> (EOperate otag, cs))
    | otag `elem` [ONeg, ONot], [a] <- cs = unary otag a
    | otherwise, [a, b] <- cs = binary otag a b
    | otherwise = undefined

-- | Interpretation of Record Projection.
expression (tag &&& children -> (EProject i, [r])) = expression r >>= \case
    VRecord vr -> maybe (throwE $ RunTimeTypeError "Unknown Record Field") return $ lookup i vr
    _ -> throwE $ RunTimeTypeError "Invalid Record Projection"
expression (tag -> EProject _) = throwE $ RunTimeTypeError "Invalid Record Projection"

-- | Interpretation of Let-In Constructions.
expression (tag &&& children -> (ELetIn i, [e, b])) = expression e >>= modify . (:) . (i,) >> expression b
expression (tag -> ELetIn _) = throwE $ RunTimeTypeError "Invalid LetIn Construction"

-- | Interpretation of Assignment.
expression (tag &&& children -> (EAssign i, [e])) = lookupE i >>= \case
    VIndirection r -> expression e >>= liftIO . writeIORef r >> return (VTuple [])
    _ -> throwE $ RunTimeTypeError "Assignment to Non-Reference"
expression (tag -> EAssign _) = throwE $ RunTimeTypeError "Invalid Assignment"

-- | Interpretation of Case-Matches.
expression (tag &&& children -> (ECaseOf i, [e, s, n])) = expression e >>= \case
    VOption (Just v) -> modify ((i, v):) >> expression s
    VOption (Nothing) -> expression n
    _ -> throwE $ RunTimeTypeError "Invalid Argument to Case-Match"
expression (tag -> ECaseOf _) = throwE $ RunTimeTypeError "Invalid Case-Match"

-- | Interpretation of Binding.
expression (tag &&& children -> (EBindAs b, [e, f])) = expression e >>= \b' -> case (b, b') of
    (BIndirection i, VIndirection r) -> (modify . (:) $ (i, VIndirection r)) >> expression f
    (BTuple ts, VTuple vs) -> (modify . (++) $ zip ts vs) >> expression f
    (BRecord ids, VRecord ivs) -> do
        let (idls, idbs) = unzip $ sortBy (compare `on` fst) ids
        let (ivls, ivvs) = unzip $ sortBy (compare `on` fst) ivs
        if idls == ivls
            then modify ((++) (zip idbs ivvs)) >> expression f
            else throwE $ RunTimeTypeError "Invalid Bind-Pattern"
expression (tag -> EBindAs _) = throwE $ RunTimeTypeError "Invalid Bind Construction"

-- | Interpretation of If-Then-Else constructs.
expression (tag &&& children -> (EIfThenElse, [p, t, e])) = expression p >>= \case
    VBool True -> expression t
    VBool False -> expression e
    _ -> throwE $ RunTimeTypeError "Invalid Conditional Predicate"

global      :: Identifier -> (K3 Type) -> (Maybe (K3 Expression)) -> Interpretation Value
global      = undefined

bind        :: Identifier -> Identifier -> Interpretation Value
bind        = undefined

selector    :: Identifier -> Interpretation Value
selector    = undefined

role        :: Identifier -> [K3 Declaration] -> Interpretation Value
role        = undefined

annotation  :: Identifier -> [AnnMemDecl] -> Interpretation Value
annotation  = undefined

declaration :: K3 Declaration -> Interpretation Value
declaration d = case tag d of 
                  DGlobal     n t e_opt -> global n t e_opt
                  DBind       src dest  -> bind src dest
                  DSelector   n         -> selector n
                  DRole       r         -> role r $ children d
                  DAnnotation n members -> annotation n members
  where children (Node _ x) = x
        tag (Node (x :@: _) _) = x

program :: K3 Declaration -> IO IEnvironment
program p = runInterpretation [] (declaration p) >>= \case
              ((Right _, env), _) -> return env
              ((Left x, _), _) -> return []
