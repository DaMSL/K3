{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | The K3 Interpreter
module Language.K3.Interpreter (
    Interpretation,
    InterpretationError,

    IEnvironment, ILog,
    expression,
    declaration,
    program
) where

import Control.Monad.Identity
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.List
import Data.Tree
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Type
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

-- | K3 Values
data Value
    = VBool        Bool
    | VByte        Word8
    | VInt         Int
    | VFloat       Float
    | VString      String
    | VOption      (Maybe Value)
    | VTuple       [Value]
    | VRecord      [(Identifier, Value)]
    | VCollection  [Value]
    | VIndirection Value
    | VFunction    IEnvironment Identifier (K3 Expression)
  deriving (Read, Show)

-- | Interpretation event log.
type ILog = [String]

-- | Interpretation Environment.
type IEnvironment = [(Identifier, Value)]

-- | Errors encountered during interpretation.
data InterpretationError
    = UnknownVariable    Identifier 
    | TypeError          String      Value        -- type mismaatch w/ expected type and supplied value
    | OperatorError      Operator    [Value]      -- invalid operator application
    | BindError          Value       Binder       -- invalid binding identifiers
    | ProjectError       Value       Identifier   -- invalid project request
    | ApplyError         Value                    -- invalid function application
    | InvalidDestination Value
  deriving (Read, Show)

-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IEnvironment (WriterT ILog Identity))

-- | Run an interpretation to get a value or error, resulting environment and event log.
runInterpretation :: IEnvironment -> Interpretation a -> ((Either InterpretationError a, IEnvironment), ILog)
runInterpretation e = runIdentity . runWriterT . flip runStateT e . runEitherT

-- | Raise an error inside an interpretation. The error will be captured alongside the event log
-- till date, and the current state.
throwE :: InterpretationError -> Interpretation a
throwE = left

{- Value helpers -}
valueType v = case v of
  VBool _             -> "bool"
  VByte _             -> "byte"
  VInt _              -> "int"
  VFloat _            -> "float"
  VString _           -> "string"
  VOption v_opt       -> "option"
  VTuple subv         -> "tuple"
  VRecord subv        -> "record"
  VCollection subv    -> "collection"
  VIndirection v      -> "ind"
  VFunction _ _ _     -> "function"

{- Environment helpers -}
addEnv env n v = env ++ [(n,v)]
deleteEnv env n = filter ((n ==) . fst) env
lookupEnv env n = case lookup n env of
    Just v -> Right v
    Nothing -> Left $ UnknownVariable n

replaceEnv env n v = addEnv (deleteEnv env n) n v
bindEnv env n v = case lookup n env of
                    Just a -> replaceEnv env n v
                    Nothing -> addEnv env n v


{- Interpreter implementation -}
vUnit  = VTuple []
vTrue  = VBool True
vFalse = VBool False


constant :: Constant -> Interpretation Value
constant c = right $ case c of
              CBool b   -> VBool b
              CInt i    -> VInt i
              CByte w   -> VByte w
              CFloat f  -> VFloat f
              CString s -> VString s
              CNone     -> VOption Nothing
              CEmpty    -> VCollection []

-- TODO: current send targets are parsed as variables,
-- and will fail on this lookup
variable :: Identifier -> Interpretation Value
variable n = do
  env <- get
  case lookupEnv env n of
    Left e -> throwE e
    Right v -> right v

some :: Interpretation Value -> Interpretation Value
some v = v >>= right . VOption . Just

indirect :: Interpretation Value -> Interpretation Value
indirect v = v >>= right . VIndirection

tuple :: [Interpretation Value] -> Interpretation Value
tuple fields = sequence fields >>= right . VTuple

record :: [Identifier] -> [Interpretation Value] -> Interpretation Value
record ids vals = sequence vals >>= right . VRecord . zip ids

lambda :: Identifier -> K3 Expression -> Interpretation Value
lambda a b = do
  env <- get
  return $ VFunction env a b

apply :: Interpretation Value -> Interpretation Value -> Interpretation Value
apply f arg = f >>= performApply
  where performApply (VFunction env n body) = do
          -- TODO: this does not correctly handle updates to globals
          -- by the applied function
          savedEnv <- get
          v <- arg
          put $ bindEnv env n v
          result <- expression body
          put savedEnv
          return result
        performApply f = throwE $ ApplyError f

send :: Interpretation Value -> Interpretation Value -> Interpretation Value
send dest arg = dest >>= performSend
  where performSend (VTuple [_, VString addr, VInt port]) = undefined
        performSend x = throwE $ InvalidDestination x

project :: Identifier -> Interpretation Value -> Interpretation Value
project x e = e >>= applyProject
  where applyProject (VRecord idvs) =
          case lookup x idvs of
            Just v  -> right v
            Nothing -> throwE $ ProjectError (VRecord idvs) x
        applyProject v = throwE $ ProjectError v x

letIn :: Identifier -> Interpretation Value -> K3 Expression -> Interpretation Value
letIn n iv b = (do
  v <- iv
  env <- get
  put $ bindEnv env n v
  return vUnit) >> (expression b)

assign :: Identifier -> Interpretation Value -> Interpretation Value
assign n iv = do
  v <- iv
  env <- get
  put $ bindEnv env n v
  return vUnit

caseOf :: Interpretation Value -> Identifier -> K3 Expression -> K3 Expression -> Interpretation Value
caseOf dv n se ne = dv >>= applyCase
  where applyCase (VOption Nothing) = expression ne
        applyCase (VOption (Just v)) = (do
          env <- get
          put $ bindEnv env n v
          return vUnit) >> (expression se)

bindAs :: Interpretation Value -> Binder -> K3 Expression -> Interpretation Value
bindAs bv binder e = bv >>= applyBind binder
  where applyBind (BIndirection n) (VIndirection v) = (do
          env <- get
          put $ bindEnv env n v
          return vUnit) >> (expression e)          
        applyBind (BTuple ids) (VTuple subv) = 
          if length subv /= length ids then
            throwE $ BindError (VTuple subv) (BTuple ids)
          else (do
            env <- get
            put $ foldl (uncurry . bindEnv) env $ zip ids subv
            return vUnit) >> expression e
        applyBind (BRecord rebinds) (VRecord idvs) = 
          if length idvs /= length rebinds then
            throwE $ BindError (VRecord idvs) (BRecord rebinds)
          else (do
            env <- get
            case foldl (rename idvs) (Right env) rebinds of
              Left bindErr -> throwE bindErr
              Right nEnv -> (put nEnv >> return vUnit)) >> expression e
        applyBind b v = throwE $ BindError v b
        rename record acc (new, old) = case (acc, lookup old record) of
          (Left err, _)       -> Left err
          (Right env, Just v) -> Right $ bindEnv env new v
          (Right env, _)      -> Left $ BindError (VRecord record) (BRecord [(new,old)])


ifThenElse :: Interpretation Value -> K3 Expression -> K3 Expression -> Interpretation Value
ifThenElse pv t e = pv >>= (\v -> 
    case v of 
      VBool b -> if b then expression t else expression e
      _ -> throwE $ TypeError (valueType vTrue) v)

unOp :: Operator -> Interpretation Value -> Interpretation Value
unOp op iv = iv >>= (\v -> case (op, v) of
  (ONeg, VInt i)   -> right $ VInt (negate i)
  (ONeg, VFloat f) -> right $ VFloat (negate f)
  (ONot, VBool b)  -> right $ VBool (not b)
  _ -> throwE $ OperatorError op [v])


binOp :: Operator -> Interpretation Value -> Interpretation Value -> Interpretation Value
binOp op liv riv = (liftM2 applyOp liv riv) >>= hoistEither
  where
    applyOp lv rv =
      let err = Left $ OperatorError op [lv,rv] in case (op, lv, rv) of 
      (x, VInt l, VInt r)     | x `elem` (map fst intOps) -> dispatchIntOp err VInt x l r
      (x, VInt l, VFloat r)   | x `elem` (map fst floatOps) -> dispatchFloatOp err VFloat x (promote l) r
      (x, VFloat l, VInt r)   | x `elem` (map fst floatOps) -> dispatchFloatOp err VFloat x l (promote r)
      (x, VBool l, VBool r)   | x `elem` (map fst boolOps) -> dispatchBoolOp err VBool x l r
      _ -> err
    dispatch opList err cstr op l r = case op `elemIndex` (map fst opList) of
      Just i -> Right $ cstr $ (snd $ opList !! i) l r
      Nothing -> err
    dispatchIntOp = dispatch intOps
    dispatchFloatOp = dispatch floatOps
    dispatchBoolOp = dispatch boolOps
    promote i = fromIntegral i :: Float
    intOps = [(OAdd, (+)), (OSub, (-)), (OMul, (*)), (ODiv, div)]
    floatOps = [(OAdd, (+)), (OSub, (-)), (OMul, (*)), (ODiv, (/))]
    boolOps = [(OEqu, (==)), (ONeq, (/=)), (OLth, (<)), (OLeq, (<=)), (OGth, (>)), (OGeq, (>=)), (OAnd, (&&)), (OOr, (||))]


operator :: Operator -> [Interpretation Value] -> Interpretation Value
operator op args =
  case op of 
    OSeq -> sequence args >>= right . last
    OApp -> pass apply args
    OSnd -> pass send args
    _ | op `elem` [ONeg, ONot] -> unOp op $ head args
      | otherwise -> pass (binOp op) args
  where pass f l = uncurry f ((head l), l !! 1)

expression :: K3 Expression -> Interpretation Value
expression e =
  case tag e of 
    EConstant   c        -> constant c
    EVariable   n        -> variable n
    ESome                -> some . head $ subvals
    EIndirect            -> indirect . head $ subvals
    ETuple               -> tuple subvals
    ERecord     ids      -> record ids subvals 
    ELambda     x        -> lambda x $ head subexprs
    EOperate    op       -> operator op subvals
    EProject    memberId -> project memberId $ head subvals
    ELetIn      x        -> letIn x (head subvals) (last subexprs)
    EAssign     lhs      -> assign lhs (head subvals)
    ECaseOf     x        -> caseOf (head subvals) x (subexprs !! 1) (subexprs !! 2)
    EBindAs     binder   -> bindAs (head subvals) binder (last subexprs)
    EIfThenElse          -> ifThenElse (head subvals) (subexprs !! 1) (subexprs !! 2)
  where children (Node _ x) = x
        tag (Node (x :@: _) _) = x
        subexprs = children e
        subvals = map expression subexprs

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

program :: K3 Declaration -> IEnvironment
program p = case runInterpretation [] $ declaration p of
              ((Right _, env), _) -> env
              ((Left x, _), _) -> []
