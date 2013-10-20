{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TemplateHaskell #-}
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

  runExpression,
  runExpression_,

  runNetwork,
  runProgram,

  packValueSyntax,
  unpackValueSyntax,

  valueWD,
  syntaxValueWD

) where

import Control.Applicative
import Control.Arrow hiding ( (+++) )
import Control.Concurrent (ThreadId)
import Control.Concurrent.MVar
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Function
import Data.IORef
import Data.List
import Data.Tree
import Data.Word (Word8)

import Text.Read hiding (get, lift)
import qualified Text.Read          as TR (lift)
import Text.ParserCombinators.ReadP as P (skipSpaces)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Runtime.Common ( PeerBootstrap, SystemEnvironment, defaultSystem )
import Language.K3.Runtime.Dispatch
import Language.K3.Runtime.Engine

import Language.K3.Utils.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["Dispatch", "RegisterGlobal"])

-- | K3 Values
data Value
    = VBool        Bool
    | VByte        Word8
    | VInt         Int
    | VReal        Double
    | VString      String
    | VOption      (Maybe Value)
    | VTuple       [Value]
    | VRecord      [(Identifier, Value)]
    | VCollection  (MVar (Collection Value))
    | VIndirection (IORef Value)
    | VFunction    (Value -> Interpretation Value, Closure Value)
    | VAddress     Address
    | VTrigger     (Identifier, Maybe (Value -> Interpretation Value))

-- | Identifiers for global declarations
type Globals = [Identifier]

-- | Function closures that capture free variable bindings.
type Closure v = IEnvironment v

-- | Interpretation event log.
type ILog = [String]

-- | Interpretation Environment.
type IEnvironment v = [(Identifier, v)]

-- | Errors encountered during interpretation.
data InterpretationError
    = RunTimeInterpretationError String
    | RunTimeTypeError String
  deriving (Eq, Read, Show)

-- | Type synonym for interpreter engine
type IEngine = Engine Value

-- | Type declaration for an Interpretation's state.
type IState = (Globals, IEnvironment Value, AEnvironment Value)

-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IState (WriterT ILog (EngineM Value)))

-- | An evaluated value type, produced from running an interpretation.
type IResult a = ((Either InterpretationError a, IState), ILog)

-- | Pairing of errors and environments for debugging output.
type EnvOnError = (InterpretationError, IEnvironment Value)


{- Collections and annotations -}

-- | Collection implementation.
--   The namespace contains lifted members, the dataspace contains final
--   records, and the extension identifier is the instance's annotation signature.
data Collection v = Collection { namespace   :: CollectionNamespace v
                               , dataspace   :: [Value]
                               , extensionId :: Identifier }

-- | Two-level namespacing of collection constituents. Collections have two levels of named values:
--   i. global names, comprised of unambiguous annotation member names.
--   ii. annotation-specific names, comprised of overlapping named annotation members.
--   
-- TODO: for now, we assume names are unambiguous and keep everything as a global name.
-- Check with Zach on the typechecker status for annotation-specific names.
data CollectionNamespace v = 
        CollectionNamespace { collectionNS :: IEnvironment v
                            , annotationNS :: [(Identifier, IEnvironment v)] }
     deriving (Read, Show)

-- | Annotation environment, for lifted attributes. This contains two mappings:
--  i. annotation ids => lifted attribute ids, lifted attribute value
--  ii. combined annotation ids => combination namespace
-- 
--  The second mapping is used to store concrete annotation combinations used at
--  collection instances (once for all instances), and defines namespaces containing
--  bindings that are introduced to the interpretation environment when invoking members.
data AEnvironment v = 
  AEnvironment { definitions  :: AnnotationDefinitions v
               , realizations :: AnnotationCombinations v }

type AnnotationDefinitions v  = [(Identifier, IEnvironment v)]
type AnnotationCombinations v = [(Identifier, CollectionBinders v)]

type CollectionBinders v = (CInitializer v, CCopyConstructor v)

-- | A collection initializer that populates default lifted attributes.
type CInitializer v = () -> Interpretation (MVar (Collection v))

-- | A copy constructor that takes a collection, copies its non-function fields,
--   and rebinds its member functions to lift/lower bindings to/from the new collection.
type CCopyConstructor v = Collection v -> Interpretation (MVar (Collection v))


{- Misc. helpers-}

details :: K3 a -> (a, [K3 a], [Annotation a])
details (Node (tg :@: anns) ch) = (tg, ch, anns)


{- State and result accessors -}

emptyState :: IState
emptyState = ([], [], AEnvironment [] [])

getGlobals :: IState -> Globals
getGlobals (x,_,_) = x

getEnv :: IState -> IEnvironment Value
getEnv (_,x,_) = x

getAnnotEnv :: IState -> AEnvironment Value
getAnnotEnv (_,_,x) = x

modifyStateEnv :: (IEnvironment Value -> IEnvironment Value) -> IState -> IState
modifyStateEnv f (w, x, y) = (w, f x, y)

modifyStateAEnv :: (AEnvironment Value -> AEnvironment Value) -> IState -> IState
modifyStateAEnv f (w, x, y) = (w, x, f y)

getResultState :: IResult a -> IState
getResultState ((_, x), _) = x

getResultVal :: IResult a -> Either InterpretationError a
getResultVal ((x, _), _) = x

{- Interpretation Helpers -}

-- | Run an interpretation to get a value or error, resulting environment and event log.
runInterpretation :: Engine Value -> IState -> Interpretation a -> IO (Either EngineError (IResult a))
runInterpretation e s = flip runEngineM e . runWriterT . flip runStateT s . runEitherT

runInterpretation' :: IState -> Interpretation a -> EngineM Value (IResult a)
runInterpretation' s = runWriterT . flip runStateT s . runEitherT

-- | Run an interpretation and extract its value.
valueOfInterpretation :: IState -> Interpretation a -> EngineM Value (Maybe a)
valueOfInterpretation s i = runInterpretation' s i >>= return . either (const $ Nothing) Just . fst . fst

-- | Raise an error inside an interpretation. The error will be captured alongside the event log
-- till date, and the current state.
throwE :: InterpretationError -> Interpretation a
throwE = Control.Monad.Trans.Either.left

-- | Lift an engine computation to an interpretation.
liftEngine :: EngineM Value b -> Interpretation b
liftEngine = lift . lift . lift

-- | Checks the result of running an interpretation for an error, and 
--   lifts any error present to the engine monad.
liftError :: String -> IResult a -> EngineM b (IResult a)
liftError msg r = either rethrow pass' $ getResultVal r 
  where pass'   = const $ return r
        rethrow = throwEngineError . EngineError . (++ (" " ++ msg)) . show

-- | Test if a variable is defined in the current interpretation environment.
elemE :: Identifier -> Interpretation Bool
elemE n = get >>= return . maybe False (const True) . find ((n == ) . fst) . getEnv

-- | Environment lookup, with a thrown error if unsuccessful.
lookupE :: Identifier -> Interpretation Value
lookupE n = get >>= maybe err return . lookup n . getEnv
  where err = throwE $ RunTimeTypeError $ "Unknown Variable: '" ++ n ++ "'"

-- | Environment modification
modifyE :: (IEnvironment Value -> IEnvironment Value) -> Interpretation ()
modifyE f = modify $ modifyStateEnv f

-- | Environment binding removal
removeE :: (Identifier, Value) -> a -> Interpretation a
removeE (n,v) r = modifyE (deleteBy ((==) `on` fst) (n,v)) >> return r

-- | Environment binding replacement
replaceE :: Identifier -> Value -> Interpretation ()
replaceE n v = modifyE $ map (\(n',v') -> if n == n' then (n,v) else (n',v'))


lookupADef :: Identifier -> Interpretation (IEnvironment Value)
lookupADef n = get >>= maybe err return . lookup n . definitions . getAnnotEnv
  where err = throwE $ RunTimeTypeError $ "Unknown annotation definition: '" ++ n ++ "'"

lookupACombo :: Identifier -> Interpretation (CollectionBinders Value)
lookupACombo n = tryLookupACombo n >>= maybe err return
  where err = throwE $ RunTimeTypeError $ "Unknown annotation combination: '" ++ n ++ "'"

tryLookupACombo :: Identifier -> Interpretation (Maybe (CollectionBinders Value))
tryLookupACombo n = get >>= return . lookup n . realizations . getAnnotEnv

-- | Annotation environment modification
modifyA :: (AEnvironment Value -> AEnvironment Value) -> Interpretation ()
modifyA f = modify $ modifyStateAEnv f

modifyADefs :: (AnnotationDefinitions Value -> AnnotationDefinitions Value) -> Interpretation ()
modifyADefs f = modifyA (\aEnv -> AEnvironment (f $ definitions aEnv) (realizations aEnv))

modifyACombos :: (AnnotationCombinations Value -> AnnotationCombinations Value) -> Interpretation ()
modifyACombos f = modifyA (\aEnv -> AEnvironment (definitions aEnv) (f $ realizations aEnv))

-- | Monadic message passing primitive for the interpreter.
sendE :: Address -> Identifier -> Value -> Interpretation ()
sendE addr n val = liftEngine $ send addr n val


{- Constants -}
vunit :: Value
vunit = VTuple []


{- Identifiers -}
collectionAnnotationId :: Identifier
collectionAnnotationId = "Collection"

annotationSelfId :: Identifier
annotationSelfId = "self"
  -- This is a keyword in the language, thus no binding to it can exist beforehand.

annotationDataId :: Identifier
annotationDataId = "data"
  -- TODO: make this a keyword in the syntax.

annotationComboId :: [Identifier] -> Identifier
annotationComboId annIds = intercalate ";" annIds

annotationComboIdT :: [Annotation Type] -> Maybe Identifier
annotationComboIdT (namedTAnnotations -> [])  = Nothing
annotationComboIdT (namedTAnnotations -> ids) = Just $ annotationComboId ids

annotationComboIdE :: [Annotation Expression] -> Maybe Identifier
annotationComboIdE (namedEAnnotations -> [])  = Nothing
annotationComboIdE (namedEAnnotations -> ids) = Just $ annotationComboId ids

annotationComboIdL :: [Annotation Literal] -> Maybe Identifier
annotationComboIdL (namedLAnnotations -> [])  = Nothing
annotationComboIdL (namedLAnnotations -> ids) = Just $ annotationComboId ids

{- Collection initialization -}

emptyCollectionNamespace :: CollectionNamespace Value
emptyCollectionNamespace = CollectionNamespace [] []

initialCollectionBody :: Identifier -> [Value] -> Collection Value
initialCollectionBody n vals = Collection emptyCollectionNamespace vals n

emptyCollectionBody :: Identifier -> Collection Value
emptyCollectionBody n = initialCollectionBody n []

defaultCollectionBody :: [Value] -> Collection Value
defaultCollectionBody vals = initialCollectionBody collectionAnnotationId vals

initialCollection :: [Value] -> Interpretation Value
initialCollection vals = liftIO (newMVar $ initialCollectionBody "" vals) >>= return . VCollection

emptyCollection :: Interpretation Value
emptyCollection = initialCollection []


-- | These methods create a valid namespace by performing a lookup based on the annotation combo id.
initialAnnotatedCollectionBody :: Identifier -> [Value] -> Interpretation (MVar (Collection Value))
initialAnnotatedCollectionBody comboId vals = do
    (initF, _) <- lookupACombo comboId
    cmv        <- initF ()
    void $ liftIO (modifyMVar_ cmv (\(Collection ns _ cid) -> return $ Collection ns vals cid))
    return cmv

initialAnnotatedCollection :: Identifier -> [Value] -> Interpretation Value
initialAnnotatedCollection comboId vals =
  initialAnnotatedCollectionBody comboId vals >>= return . VCollection

emptyAnnotatedCollection :: Identifier -> Interpretation Value
emptyAnnotatedCollection comboId = initialAnnotatedCollection comboId []


{- Interpretation -}

-- | Default values for specific types
defaultValue :: K3 Type -> Interpretation Value
defaultValue (tag -> TBool)       = return $ VBool False
defaultValue (tag -> TByte)       = return $ VByte 0
defaultValue (tag -> TInt)        = return $ VInt 0
defaultValue (tag -> TReal)       = return $ VReal 0.0
defaultValue (tag -> TString)     = return $ VString ""
defaultValue (tag -> TOption)     = return $ VOption Nothing
defaultValue (tag -> TAddress)    = return $ VAddress defaultAddress

defaultValue (tag &&& children -> (TIndirection, [x])) = defaultValue x >>= liftIO . newIORef >>= return . VIndirection
defaultValue (tag &&& children -> (TTuple, ch))        = mapM defaultValue ch >>= return . VTuple
defaultValue (tag &&& children -> (TRecord ids, ch))   = mapM defaultValue ch >>= return . VRecord . zip ids
defaultValue
 (tag &&& annotations -> (TCollection, anns)) = 
  (getComposedAnnotationT anns) >>= maybe emptyCollection emptyAnnotatedCollection

{- TODO: 
  TSource
  TSink
  TTrigger
  TBuiltIn TypeBuiltIn
  TForall [TypeVarDecl]
  TDeclaredVar Identifier
-}
defaultValue t = throwE . RunTimeTypeError $ "Cannot create default value for " ++ show t

-- | Interpretation of Constants.
constant :: Constant -> [Annotation Expression] -> Interpretation Value
constant (CBool b)   _ = return $ VBool b
constant (CInt i)    _ = return $ VInt i
constant (CByte w)   _ = return $ VByte w
constant (CReal r)   _ = return $ VReal r
constant (CString s) _ = return $ VString s
constant (CNone _)   _ = return $ VOption Nothing
constant (CEmpty _) as = (getComposedAnnotationE as) >>= maybe emptyCollection emptyAnnotatedCollection

-- | Common Numeric-Operation handling, with casing for int/real promotion.
numeric :: (forall a. Num a => a -> a -> a) -> K3 Expression -> K3 Expression -> Interpretation Value
numeric op a b = do
  a' <- expression a
  b' <- expression b
  case (a', b') of
      (VInt x, VInt y)   -> return $ VInt  $ op x y
      (VInt x, VReal y)  -> return $ VReal $ op (fromIntegral x) y
      (VReal x, VInt y)  -> return $ VReal $ op x (fromIntegral y)
      (VReal x, VReal y) -> return $ VReal $ op x y
      _ -> throwE $ RunTimeTypeError "Arithmetic Type Mis-Match"

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
      (VBool x, VBool y)     -> return $ VBool $ op x y
      (VInt x, VInt y)       -> return $ VBool $ op x y
      (VReal x, VReal y)     -> return $ VBool $ op x y
      (VString x, VString y) -> return $ VBool $ op x y
      _ -> throwE $ RunTimeTypeError "Comparison Type Mis-Match"

-- | Interpretation of unary operators.
unary :: Operator -> K3 Expression -> Interpretation Value

-- | Interpretation of unary negation of numbers.
unary ONeg a = expression a >>= \case
  VInt i   -> return $ VInt  (negate i)
  VReal r  -> return $ VReal (negate r)
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
binary OSub = numeric (-)
binary OMul = numeric (*)

-- | Division handled similarly, but accounting zero-division errors.
binary ODiv = \a b -> do
  a' <- expression a
  b' <- expression b

  void $ case b' of
      VInt 0  -> throwE $ RunTimeInterpretationError "Division by Zero"
      VReal 0 -> throwE $ RunTimeInterpretationError "Division by Zero"
      _ -> return ()

  case (a', b') of
      (VInt x, VInt y)   -> return $ VInt $ x `div` y
      (VInt x, VReal y)  -> return $ VReal $ fromIntegral x / y
      (VReal x, VInt y)  -> return $ VReal $ x / (fromIntegral y)
      (VReal x, VReal y) -> return $ VReal $ x / y
      _ -> throwE $ RunTimeTypeError "Arithmetic Type Mis-Match"

-- | Logical Operators
binary OAnd = logic (&&)
binary OOr  = logic (||)

-- | Comparison Operators
binary OEqu = comparison (==)
binary ONeq = comparison (/=)
binary OLth = comparison (<)
binary OLeq = comparison (<=)
binary OGth = comparison (>)
binary OGeq = comparison (>=)

-- | Function Application
binary OApp = \f x -> do
  f' <- expression f
  x' <- expression x

  case f' of
      VFunction (b, cl) -> withClosure cl $ b x'
      _ -> throwE $ RunTimeTypeError $ "Invalid Function Application\n" ++ pretty f

  where withClosure cl doApp = modifyE (cl ++) >> doApp >>= flip (foldM $ flip removeE) cl

-- | Message Passing
binary OSnd = \target x -> do
  target'  <- expression target
  x'       <- expression x

  case target' of
    VTuple [VTrigger (n, _), VAddress addr] -> sendE addr n x' >> return vunit
    _ -> throwE $ RunTimeTypeError "Invalid Trigger Target"

-- | Sequential expressions
binary OSeq = \e1 e2 -> expression e1 >> expression e2

binary _ = const . const $ throwE $ RunTimeInterpretationError "Unreachable"

-- | Interpretation of Expressions
expression :: K3 Expression -> Interpretation Value

-- | Interpretation of constant expressions.
expression (tag &&& annotations -> (EConstant c, as)) = constant c as

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
expression (tag &&& children -> (ELambda i, [b])) =
  mkFunction $ \v -> modifyE ((i,v):) >> expression b >>= removeE (i,v)
  where mkFunction f = closure >>= \cl -> return $ VFunction . (, cl) $ f

        closure :: Interpretation (Closure Value)
        closure = do
          globals <- get >>= return . getGlobals
          vars    <- return $ filter (\n -> n /= i && not (elem n globals)) $ freeVariables b
          vals    <- mapM lookupE vars
          return $ zip vars vals

-- | Interpretation of unary/binary operators.
expression (tag &&& children -> (EOperate otag, cs))
    | otag `elem` [ONeg, ONot], [a] <- cs = unary otag a
    | otherwise, [a, b] <- cs = binary otag a b
    | otherwise = undefined

-- | Interpretation of Record Projection.
expression (tag &&& children -> (EProject i, [r])) = expression r >>= \case
    VRecord vr -> maybe (unknownField i) return $ lookup i vr

    VCollection cmv -> do
      Collection ns _ extId <- liftIO $ readMVar cmv
      if null extId then unannotatedCollection else case lookup i $ collectionNS ns of
        Nothing -> unknownCollectionMember i (map fst $ collectionNS ns)
        Just v' -> return v'

    _ -> throwE $ RunTimeTypeError "Invalid Record Projection"
  
  where unknownField i'              = throwE $ RunTimeTypeError $ "Unknown record field " ++ i'
        unannotatedCollection        = throwE $ RunTimeTypeError $ "Invalid projection on an unannotated collection"
        unknownCollectionMember i' n = throwE $ RunTimeTypeError $ "Unknown collection member " ++ i' ++ "(valid " ++ show n ++ ")"

expression (tag -> EProject _) = throwE $ RunTimeTypeError "Invalid Record Projection"

-- | Interpretation of Let-In Constructions.
expression (tag &&& children -> (ELetIn i, [e, b])) =
    expression e >>= (\v -> modifyE ((i,v):) >> expression b >>= removeE (i,v))
expression (tag -> ELetIn _) = throwE $ RunTimeTypeError "Invalid LetIn Construction"

-- | Interpretation of Assignment.
expression (tag &&& children -> (EAssign i, [e])) =
  lookupE i >>= (\_ -> expression e)
            >>= modifyE . (\new -> map (\(n,v) -> if i == n then (i,new) else (n,v)))
            >> return vunit
expression (tag -> EAssign _) = throwE $ RunTimeTypeError "Invalid Assignment"

-- | Interpretation of Case-Matches.
expression (tag &&& children -> (ECaseOf i, [e, s, n])) = expression e >>= \case
    VOption (Just v) -> modifyE ((i, v):) >> expression s >>= removeE (i,v)
    VOption (Nothing) -> expression n
    _ -> throwE $ RunTimeTypeError "Invalid Argument to Case-Match"
expression (tag -> ECaseOf _) = throwE $ RunTimeTypeError "Invalid Case-Match"

-- | Interpretation of Binding.
expression (tag &&& children -> (EBindAs b, [e, f])) = expression e >>= \b' -> case (b, b') of
    (BIndirection i, VIndirection r) -> 
      (modifyE . (:) $ (i, VIndirection r)) >> expression f >>= refreshBinding >>= removeE (i, VIndirection r)
    
    (BTuple ts, VTuple vs) ->
      (modifyE . (++) $ zip ts vs) >> expression f >>= refreshBinding >>= removeAllE (zip ts vs)
    
    (BRecord ids, VRecord ivs) -> do
        let (idls, idbs) = unzip $ sortBy (compare `on` fst) ids
        let (ivls, ivvs) = unzip $ sortBy (compare `on` fst) ivs
        if idls == ivls
            then modifyE ((++) (zip idbs ivvs)) >> expression f >>= refreshBinding >>= removeAllE (zip idbs ivvs)
            else throwE $ RunTimeTypeError "Invalid Bind-Pattern"
    _ -> throwE $ RunTimeTypeError "Bind Mis-Match"
  
  -- TODO: support run-time determination of refresh targets, e.g., 
  -- bind (if predicate then x else y) as ind x in ...
  -- TODO: support aliased bindings, e.g, bind x as ind y in (bind x as ind z in y = y + z)
  where bindId = case tag e of
                  EVariable i -> Just i
                  _           -> Nothing

        removeAllE = flip (foldM (flip removeE))

        refreshBinding r = const (return r) =<< case (bindId, b) of
          (Just i, BIndirection j) -> modifyIndirection =<< (,) <$> lookupE i <*> lookupE j
          (Just i, BTuple ts)      -> mapM lookupE ts >>= replaceE i . VTuple
          (Just i, BRecord ids)    -> mapM (\(s,t) -> lookupE t >>= return . (s,)) ids >>= replaceE i . VRecord
          (_,_)                    -> return ()

        modifyIndirection (VIndirection r, v) = liftIO $ writeIORef r v
        modifyIndirection _ = throwE $ RunTimeTypeError "Invalid indirection value"

expression (tag -> EBindAs _) = throwE $ RunTimeTypeError "Invalid Bind Construction"

-- | Interpretation of If-Then-Else constructs.
expression (tag &&& children -> (EIfThenElse, [p, t, e])) = expression p >>= \case
    VBool True -> expression t
    VBool False -> expression e
    _ -> throwE $ RunTimeTypeError "Invalid Conditional Predicate"

expression (tag &&& children -> (EAddress, [h, p])) = do
  hv <- expression h
  pv <- expression p
  case (hv, pv) of
    (VString host, VInt port) -> return $ VAddress $ Address (host, port)
    _ -> throwE $ RunTimeTypeError "Invalid address"

expression (tag -> ESelf) = lookupE annotationSelfId

expression _ = throwE $ RunTimeInterpretationError "Invalid Expression"


{- Literal interpretation -}

literal :: K3 Literal -> Interpretation Value
literal (tag -> LBool b)   = return $ VBool b
literal (tag -> LByte b)   = return $ VByte b
literal (tag -> LInt b)    = return $ VInt b
literal (tag -> LReal b)   = return $ VReal b
literal (tag -> LString b) = return $ VString b
literal (tag -> LNone _)   = return $ VOption Nothing

literal (tag &&& children -> (LSome, [x])) = literal x >>= return . VOption . Just
literal (tag -> LSome) = throwE $ RunTimeTypeError "Invalid option literal"

literal (tag &&& children -> (LIndirect, [x])) = literal x >>= liftIO . newIORef >>= return . VIndirection
literal (tag -> LIndirect) = throwE $ RunTimeTypeError "Invalid indirection literal"

literal (tag &&& children -> (LTuple, ch)) = mapM literal ch >>= return . VTuple
literal (tag -> LTuple) = throwE $ RunTimeTypeError "Invalid tuple literal"

literal (tag &&& children -> (LRecord ids, ch)) = mapM literal ch >>= return . VRecord . zip ids
literal (tag -> LRecord _) = throwE $ RunTimeTypeError "Invalid record literal"

literal (details -> (LEmpty _, [], anns)) =
  getComposedAnnotationL anns >>= maybe emptyCollection initEmptyCollection
  where initEmptyCollection comboId = lookupACombo comboId >>= ($ ()) . fst >>= return . VCollection

literal (tag -> LEmpty _) = throwE $ RunTimeTypeError "Invalid empty literal"

literal (details -> (LCollection _, elems, anns)) = do
  cElems <- mapM literal elems
  getComposedAnnotationL anns
    >>= maybe (initialCollection cElems) (flip initialAnnotatedCollection cElems)

literal (details -> (LAddress, [h,p], _)) = mapM literal [h,p] >>= \case 
  [VString a, VInt b] -> return . VAddress $ Address (a,b)
  _     -> throwE $ RunTimeTypeError "Invalid address literal"

literal (tag -> LAddress) = throwE $ RunTimeTypeError "Invalid address literal" 

literal _ = throwE $ RunTimeTypeError "Invalid literal"

{- Declaration interpretation -}

replaceTrigger :: Identifier -> Value -> Interpretation()
replaceTrigger n (VFunction (f,[])) = modifyE (\env -> replaceAssoc env n (VTrigger (n, Just f)))
replaceTrigger _ _                  = throwE $ RunTimeTypeError "Invalid trigger body"

global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> Interpretation ()
global n (tag -> TSink) (Just e)      = expression e >>= replaceTrigger n
global _ (tag -> TSink) Nothing       = throwE $ RunTimeInterpretationError "Invalid sink trigger"
global _ (tag -> TSource) _           = return ()
global _ (tag -> TFunction) _         = return () -- Functions have already been initialized.

global n t@(tag -> TCollection) eOpt = elemE n >>= \case
  True  -> void . getComposedAnnotationT $ annotations t
  False -> (getComposedAnnotationT $ annotations t) >>= initializeCollection . maybe "" id
  where
    initializeCollection comboId = case eOpt of
      Nothing | not (null comboId) -> lookupACombo comboId >>= \(initC,_) -> initC () >>= modifyE . (:) . (n,) . VCollection
      Just e  | not (null comboId) -> expression e >>= verifyInitialCollection comboId

      -- TODO: error on these cases. All collections must have at least the builtin Collection annotation.
      Nothing -> emptyCollection >>= modifyE . (:) . (n,)
      Just e  -> expression e >>= modifyE . (:) . (n,)

    verifyInitialCollection comboId = \case
      VCollection cmv -> do
          Collection _ _ extId <- liftIO $ readMVar cmv
          if comboId /= extId then collInitError
          else modifyE ((n, VCollection cmv):)
      
      _ -> collValError

    collInitError = throwE . RunTimeTypeError $ "Invalid annotations on collection initializer for " ++ n
    collValError  = throwE . RunTimeTypeError $ "Invalid collection value " ++ n

global n t eOpt = elemE n >>= \case
  True  -> return ()
  False -> maybe (defaultValue t) expression eOpt >>= modifyE . (:) . (n,)


-- TODO: qualify names?
role :: Identifier -> [K3 Declaration] -> Interpretation ()
role _ subDecls = mapM_ declaration subDecls


declaration :: K3 Declaration -> Interpretation ()
declaration (tag &&& children -> (DGlobal n t eO, ch)) =
    debugDecl n t $ global n t eO >> mapM_ declaration ch

declaration (tag &&& children -> (DTrigger n t e, cs)) =
    debugDecl n t $ (expression e >>= replaceTrigger n) >> mapM_ declaration cs

declaration (tag &&& children -> (DRole r, ch)) = role r ch
declaration (tag -> DAnnotation n vdecls members)      =
    annotation n vdecls members

declaration _ = undefined

{- Annotations -}

annotation :: Identifier -> [TypeVarDecl] -> [AnnMemDecl] -> Interpretation ()
annotation n vdecls memberDecls = do
  -- TODO: consider: does anything need to be done with "vdecls", the declared
  --       type variables for this annotation?
  bindings <- foldM initializeMembers [] [liftedAttrFuns, liftedAttrs, attrFuns, attrs]
  void $ foldM (flip removeE) () bindings
  void $ modifyADefs $ (:) (n,bindings)

  where initializeMembers initEnv spec = foldM (memberWithBindings spec) initEnv memberDecls

        memberWithBindings (isLifted, matchF) accEnv mem =
          annotationMember n isLifted matchF mem 
            >>= maybe (return accEnv) (\nv -> modifyE (nv:) >> return (nv:accEnv))

        (liftedAttrFuns, liftedAttrs) = ((True, matchFunction), (True, not . matchFunction))
        (attrFuns, attrs)             = ((False, matchFunction), (False, not . matchFunction))

        matchFunction (tag -> TFunction) = True
        matchFunction _ = False


annotationMember :: Identifier -> Bool -> (K3 Type -> Bool) -> AnnMemDecl 
                    -> Interpretation (Maybe (Identifier, Value))
annotationMember annId matchLifted matchF annMem = case (matchLifted, annMem) of
  (True,  Lifted    Provides n t (Just e) _)   | matchF t -> interpretExpr n e
  (False, Attribute Provides n t (Just e) _)   | matchF t -> interpretExpr n e
  (True,  Lifted    Provides n t Nothing  uid) | matchF t -> builtinLiftedAttribute annId n t uid
  (False, Attribute Provides n t Nothing  uid) | matchF t -> builtinAttribute annId n t uid
  _ -> return Nothing
  where interpretExpr n e = expression e >>= return . Just . (n,)


{- Interpretation utility functions -}

-- | Annotation composition retrieval and registration.
getComposedAnnotationT :: [Annotation Type] -> Interpretation (Maybe Identifier)
getComposedAnnotationT anns = getComposedAnnotation (annotationComboIdT anns, namedTAnnotations anns)

getComposedAnnotationE :: [Annotation Expression] -> Interpretation (Maybe Identifier)
getComposedAnnotationE anns = getComposedAnnotation (annotationComboIdE anns, namedEAnnotations anns) 

getComposedAnnotationL :: [Annotation Literal] -> Interpretation (Maybe Identifier)
getComposedAnnotationL anns = getComposedAnnotation (annotationComboIdL anns, namedLAnnotations anns) 

getComposedAnnotation :: (Maybe Identifier, [Identifier]) -> Interpretation (Maybe Identifier)
getComposedAnnotation (comboIdOpt, annNames) = case comboIdOpt of
  Nothing      -> return Nothing
  Just comboId -> tryLookupACombo comboId 
                    >>= (\cOpt -> initializeComposition comboId cOpt >> return (Just comboId))
  where
    initializeComposition comboId = \case
      Nothing -> mapM (\x -> lookupADef x >>= return . (x,)) annNames
                   >>= addComposedAnnotation comboId
      Just _  -> return ()

    addComposedAnnotation comboId namedAnnDefs = 
      modifyACombos . (:) . (comboId,) $ mkCBinder comboId namedAnnDefs

    mkCBinder comboId namedAnnDefs =
      (mkInitializer comboId namedAnnDefs, mkConstructor namedAnnDefs)

    mkInitializer :: Identifier -> [(Identifier, IEnvironment Value)] -> CInitializer Value
    mkInitializer comboId namedAnnDefs = const $ do
      newCMV <- liftIO . newMVar $ emptyCollectionBody comboId
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      void $ mapM_ (bindAnnotationDef newCMV) namedAnnDefs
      return newCMV

    mkConstructor :: [(Identifier, IEnvironment Value)] -> CCopyConstructor Value
    mkConstructor namedAnnDefs = \coll -> do
      newCMV <- liftIO (newMVar coll)
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      return newCMV

    bindAnnotationDef :: MVar (Collection Value) -> (Identifier, IEnvironment Value) -> Interpretation ()
    bindAnnotationDef cmv (n, env) = mapM_ (bindMember cmv n) env

    bindMember _ _ (_, VFunction _) = return ()
    bindMember cmv annId (n, v) =
      liftIO $ modifyMVar_ cmv (\(Collection (CollectionNamespace cns ans) ds extId) ->
        let (cns', ans') = extendNamespace cns ans annId n v
        in return $ Collection (CollectionNamespace cns' ans') ds extId)

    rebindFunctionsInEnv :: MVar (Collection Value) -> (Identifier, IEnvironment Value) -> Interpretation ()
    rebindFunctionsInEnv cmv (n, env) = mapM_ (rebindFunction cmv n) env
    
    rebindFunction cmv annId (n, VFunction f) =
      liftIO $ modifyMVar_ cmv (\(Collection (CollectionNamespace cns ans) ds extId) ->
        let newF         = contextualizeFunction cmv f
            (cns', ans') = extendNamespace cns ans annId n newF
        in return $ Collection (CollectionNamespace cns' ans') ds extId)
    
    rebindFunction _ _ _ = return ()

    extendNamespace cns ans annId n v =
      let cns'   = replaceAssoc cns n v
          annEnv = maybe Nothing (\env -> Just $ replaceAssoc env n v) $ lookup annId ans
          ans'   = maybe ans (replaceAssoc ans annId) annEnv
      in (cns', ans')

contextualizeFunction :: MVar (Collection Value) -> (Value -> Interpretation Value, Closure Value) -> Value
contextualizeFunction cmv (f, cl) = VFunction . (, cl) $ \x -> do
      bindings <- liftCollection
      result   <- f x >>= return . contextualizeResult
      lowerCollection bindings result

  where
    contextualizeResult (VFunction f') = contextualizeFunction cmv f'
    contextualizeResult r = r

    -- TODO:
    -- i. lift/lower data segment.
    -- ii. handle aliases, e.g., 'self.x' and 'x'. 
    --     Also, this needs to be done more generally for bind as expressions.
    -- iii. handle lowering of annotation-specific namespaces
    liftCollection = do
      Collection (CollectionNamespace cns _) _ _ <- liftIO $ readMVar cmv
      bindings <- return $ cns ++ [(annotationSelfId, VCollection cmv)]
      void $ modifyE (bindings ++)
      return bindings

    lowerCollection bindings result = do
        newNsInfo <- lowerBindings bindings
        void $ liftIO $ modifyMVar_ cmv $ \(Collection ns ds extId) -> 
          return (Collection (rebuildNamespace ns newNsInfo) ds extId)
        foldM (flip removeE) result bindings

    lowerBindings env =
      mapM (\(n,_) -> lookupE n >>= return . (n,)) env 
        >>= return . partition ((annotationSelfId /= ) . fst)

    -- TODO: rebind annotation-specific namespace
    rebuildNamespace ns (newGlobalNS, _) =
      CollectionNamespace newGlobalNS $ annotationNS ns


{- Built-in functions -}

ignoreFn :: Value -> Value
ignoreFn = VFunction . (,[]) . const . return

vfun :: (Value -> Interpretation Value) -> Value
vfun = VFunction . (,[])

ivfun :: (Value -> Interpretation Value) -> Interpretation Value
ivfun = return . vfun

builtin :: Identifier -> K3 Type -> Interpretation ()
builtin n t = genBuiltin n t >>= modifyE . (:) . (n,)

genBuiltin :: Identifier -> K3 Type -> Interpretation Value

-- parseArgs :: () -> ([String], [(String, String)])
genBuiltin "parseArgs" _ =
  emptyCollection >>= \x -> return $ ignoreFn $ VTuple [x,x]


-- TODO: error handling on all open/close/read/write methods.
-- TODO: argument for initial endpoint bindings for open method as a list of triggers
-- TODO: correct element type (rather than function type sig) for openFile / openSocket

-- type ChannelId = String

-- openBuilting :: ChannelId -> String -> ()
genBuiltin "openBuiltin" _ =
  ivfun $ \(VString cid) -> 
    ivfun $ \(VString builtinId) ->
      ivfun $ \(VString format) ->
        liftEngine (openBuiltin cid builtinId (wireDesc format)) >> return vunit

-- openFile :: ChannelId -> String -> String -> String -> ()
genBuiltin "openFile" t =
  ivfun $ \(VString cid) ->
    ivfun $ \(VString path) ->
      ivfun $ \(VString format) ->
        ivfun $ \(VString mode) ->
          liftEngine (openFile cid path (wireDesc format) (Just t) mode) >> return vunit

-- openSocket :: ChannelId -> Address -> String -> String -> ()
genBuiltin "openSocket" t =
  ivfun $ \(VString cid) ->
    ivfun $ \(VAddress addr) ->
      ivfun $ \(VString format) ->
        ivfun $ \(VString mode) ->
          liftEngine (openSocket cid addr (wireDesc format) (Just t) mode) >> return vunit

-- close :: ChannelId -> ()
genBuiltin "close" _ = ivfun $ \(VString cid) -> liftEngine (close cid) >> return vunit

-- TODO: deregister methods
-- register*Trigger :: ChannelId -> TTrigger () -> ()
genBuiltin "registerFileDataTrigger"     _ = registerNotifier "data"
genBuiltin "registerFileCloseTrigger"    _ = registerNotifier "close"

genBuiltin "registerSocketAcceptTrigger" _ = registerNotifier "accept"
genBuiltin "registerSocketDataTrigger"   _ = registerNotifier "data"
genBuiltin "registerSocketCloseTrigger"  _ = registerNotifier "close"

-- <source>HasRead :: () -> Bool
genBuiltin (channelMethod -> ("HasRead", Just n)) _ = ivfun $ \_ -> checkChannel
  where checkChannel = liftEngine (hasRead n) >>= maybe invalid (return . VBool)
        invalid = throwE $ RunTimeInterpretationError $ "Invalid source \"" ++ n ++ "\""

-- <source>Read :: () -> t
genBuiltin (channelMethod -> ("Read", Just n)) _ = ivfun $ \_ -> liftEngine (doRead n) >>= throwOnError
  where throwOnError (Just v) = return v
        throwOnError Nothing =
          throwE $ RunTimeInterpretationError $ "Invalid next value from source \"" ++ n ++ "\""

-- <sink>HasWrite :: () -> Bool
genBuiltin (channelMethod -> ("HasWrite", Just n)) _ = ivfun $ \_ -> checkChannel
  where checkChannel = liftEngine (hasWrite n) >>= maybe invalid (return . VBool)
        invalid = throwE $ RunTimeInterpretationError $ "Invalid sink \"" ++ n ++ "\""

-- <sink>Write :: t -> ()
genBuiltin (channelMethod -> ("Write", Just n)) _ =
  ivfun $ \arg -> liftEngine (doWrite n arg) >> return vunit

genBuiltin n _ = throwE $ RunTimeTypeError $ "Invalid builtin \"" ++ n ++ "\""

channelMethod :: String -> (String, Maybe String)
channelMethod x =
  case find (flip isSuffixOf x) ["HasRead", "Read", "HasWrite", "Write"] of
    Just y -> (y, stripSuffix y x)
    Nothing -> (x, Nothing)
  where stripSuffix sfx lst = maybe Nothing (Just . reverse) $ stripPrefix (reverse sfx) (reverse lst)

registerNotifier :: Identifier -> Interpretation Value
registerNotifier n =
  ivfun $ \cid -> ivfun $ \target -> attach cid n target >> return vunit

  where attach (VString cid) _ (targetOfValue -> (addr, tid, v)) = 
          liftEngine $ attachNotifier_ cid n (addr, tid, v)
        attach _ _ _ = undefined

        targetOfValue (VTuple [VTrigger (m, _), VAddress addr]) = (addr, m, vunit)
        targetOfValue _ = error "Invalid notifier target"


{- Builtin annotation members -}
providesError :: String -> Identifier -> a
providesError kind n = error $ 
  "Invalid " ++ kind ++ " definition for " ++ n ++ ": no initializer expression"

builtinLiftedAttribute :: Identifier -> Identifier -> K3 Type -> UID
                          -> Interpretation (Maybe (Identifier, Value))
builtinLiftedAttribute annId n _ _ 
  | annId == "Collection" && n == "peek"    = return $ Just (n, peekFn)
  | annId == "Collection" && n == "insert"  = return $ Just (n, insertFn)
  | annId == "Collection" && n == "delete"  = return $ Just (n, deleteFn)
  | annId == "Collection" && n == "update"  = return $ Just (n, updateFn)
  | annId == "Collection" && n == "combine" = return $ Just (n, combineFn)
  | annId == "Collection" && n == "split"   = return $ Just (n, splitFn)
  | annId == "Collection" && n == "iterate" = return $ Just (n, iterateFn)
  | annId == "Collection" && n == "map"     = return $ Just (n, mapFn)
  | annId == "Collection" && n == "filter"  = return $ Just (n, filterFn)
  | annId == "Collection" && n == "fold"    = return $ Just (n, foldFn)
  | annId == "Collection" && n == "groupBy" = return $ Just (n, groupByFn)
  | annId == "Collection" && n == "ext"     = return $ Just (n, extFn)
  | otherwise = providesError "lifted attribute" n

  where 
        copy newC = do
          (_, copyCstr) <- lookupACombo $ extensionId newC
          c'            <- copyCstr newC
          return $ VCollection c'

        -- | Collection accessor implementation
        peekFn = valWithCollection $ \_ (Collection _ ds _) -> 
          case ds of
            []    -> return $ VOption Nothing
            (h:_) -> return . VOption $ Just h

        -- | Collection modifier implementation
        insertFn = valWithCollectionMV $ \el cmv -> modifyCollection cmv (insertCollection el)
        deleteFn = valWithCollectionMV $ \el cmv -> modifyCollection cmv (deleteCollection el)
        updateFn = valWithCollectionMV $ \old cmv -> ivfun $ \new -> modifyCollection cmv (updateCollection old new)

        modifyCollection cmv f = liftIO (modifyMVar_ cmv f) >> return vunit

        insertCollection v    (Collection ns ds extId) = return $ Collection ns (ds++[v]) extId
        deleteCollection v    (Collection ns ds extId) = return $ Collection ns (delete v ds) extId
        updateCollection v v' (Collection ns ds extId) = return $ Collection ns ((delete v ds)++[v']) extId

        -- | Collection effector implementation
        iterateFn = valWithCollection $ \f (Collection _ ds _) -> 
          flip (matchFunction iterateFnError) f $
          \f' -> mapM_ (withClosure f') ds >> return vunit

        -- | Collection transformer implementation
        combineFn = valWithCollection $ \other (Collection ns ds extId) ->
          flip (matchCollection collectionError) other $ 
            \(Collection _ ds' extId') ->
              if extId /= extId' then combineError
              else copy $ Collection ns (ds++ds') extId

        splitFn = valWithCollection $ \_ (Collection ns ds extId) ->
          let (l, r) = splitImpl ds in do
            lc <- copy (Collection ns l extId)
            rc <- copy (Collection ns r extId)
            return $ VTuple [lc, rc]

        mapFn = valWithCollection $ \f (Collection _ ds _) ->
          flip (matchFunction mapFnError) f $ 
            \f'  -> mapM (withClosure f') ds >>= 
            \ds' -> copy (defaultCollectionBody ds')

        filterFn = valWithCollection $ \f (Collection ns ds extId) ->
          flip (matchFunction filterFnError) f $
            \f'  -> filterM (\v -> withClosure f' v >>= matchBool filterValError) ds >>=
            \ds' -> copy (Collection ns ds' extId)

        foldFn = valWithCollection $ \f (Collection _ ds _) ->
          flip (matchFunction foldFnError) f $
            \f' -> ivfun $ \accInit -> foldM (curryFoldFn f') accInit ds

        curryFoldFn f' acc v = withClosure f' acc >>= (matchFunction curryFnError) (flip withClosure v)

        -- TODO: replace assoc lists with a hashmap.
        groupByFn = valWithCollection $ \gb (Collection _ ds _) ->
          flip (matchFunction partitionFnError) gb $ \gb' -> ivfun $ \f -> 
          flip (matchFunction foldFnError) f $ \f' -> ivfun $ \accInit ->
            do
              kvPairs   <- foldM (groupByElement gb' f' accInit) [] ds
              kvRecords <- return $ map (\(k,v) -> VRecord [("key", k), ("value", v)]) kvPairs
              copy (defaultCollectionBody kvRecords)

        groupByElement gb' f' accInit acc v = do
          k <- withClosure gb' v
          case lookup k acc of
            Nothing         -> curryFoldFn f' accInit v    >>= return . (acc++) . (:[]) . (k,)
            Just partialAcc -> curryFoldFn f' partialAcc v >>= return . replaceAssoc acc k 

        extFn = valWithCollection $ \f (Collection _ ds _) -> 
          flip (matchFunction extError) f $ 
          \f' -> do
                    vals <- mapM (withClosure f') ds
                    case vals of
                      []    -> copy (defaultCollectionBody [])
                      (h:t) -> foldM combine' (Just h) (map Just t) >>= maybe combineError return

        combine' :: Maybe Value -> Maybe Value -> Interpretation (Maybe Value)
        combine' Nothing _ = return Nothing
        combine' _ Nothing = return Nothing

        -- TODO: make more efficient by avoiding intermediate MVar construction.
        combine' (Just acc) (Just cv) =
          flip (matchCollection $ return Nothing) acc $ \(Collection ns1 ds1 extId1) -> 
          flip (matchCollection $ return Nothing) cv  $ \(Collection _ ds2 extId2) -> 
          if extId1 /= extId2 then return Nothing
          else copy (Collection ns1 (ds1++ds2) extId1) >>= return . Just


        -- | Collection implementation helpers.
        withClosure :: (Value -> Interpretation Value, Closure Value) -> Value -> Interpretation Value
        withClosure (f, cl) arg = modifyE (cl ++) >> f arg >>= flip (foldM $ flip removeE) cl

        valWithCollection :: (Value -> Collection Value -> Interpretation Value) -> Value
        valWithCollection f = vfun $ \arg -> 
          lookupE annotationSelfId >>= matchCollection collectionError (f arg)

        valWithCollectionMV :: (Value -> MVar (Collection Value) -> Interpretation Value) -> Value
        valWithCollectionMV f = vfun $ \arg -> 
          lookupE annotationSelfId >>= matchCollectionMV collectionError (f arg)

        matchCollection :: Interpretation a -> (Collection Value -> Interpretation a) -> Value -> Interpretation a
        matchCollection _ f (VCollection cmv) = liftIO (readMVar cmv) >>= f
        matchCollection err _  _ =  err

        matchCollectionMV :: Interpretation a -> (MVar (Collection Value) -> Interpretation a) -> Value -> Interpretation a
        matchCollectionMV _ f (VCollection cmv) = f cmv
        matchCollectionMV err _ _ = err

        matchFunction :: a -> ((Value -> Interpretation Value, Closure Value) -> a) -> Value -> a
        matchFunction _ f (VFunction f') = f f'
        matchFunction err _ _ = err

        matchBool :: Interpretation Bool -> Value -> Interpretation Bool
        matchBool _ (VBool b) = return b
        matchBool err _ = err


        splitImpl l = if length l <= threshold then (l, []) else splitAt (length l `div` 2) l
        threshold = 10

        collectionError  = throwE $ RunTimeTypeError "Invalid collection"
        combineError     = throwE $ RunTimeTypeError "Mismatched collection types for combine"
        iterateFnError   = throwE $ RunTimeTypeError "Invalid iterate function"
        mapFnError       = throwE $ RunTimeTypeError "Invalid map function"
        filterFnError    = throwE $ RunTimeTypeError "Invalid filter function"
        filterValError   = throwE $ RunTimeTypeError "Invalid filter function result"
        foldFnError      = throwE $ RunTimeTypeError "Invalid fold function"
        partitionFnError = throwE $ RunTimeTypeError "Invalid grouping function"
        curryFnError     = throwE $ RunTimeTypeError "Invalid curried function"
        extError         = throwE $ RunTimeTypeError "Invalid function argument for ext"


builtinAttribute :: Identifier -> Identifier -> K3 Type -> UID
                 -> Interpretation (Maybe (Identifier, Value))
builtinAttribute _ n _ _ = providesError "attribute" n


{- Program initialization methods -}

initEnvironment :: K3 Declaration -> IState -> EngineM Value IState
initEnvironment decl st =
  let declGState  = foldTree registerDecl st decl
  in initDecl declGState decl
  where 
    initDecl st' (tag &&& children -> (DGlobal n t eOpt, ch)) = initGlobal st' n t eOpt >>= flip (foldM initDecl) ch
    initDecl st' (tag &&& children -> (DTrigger n _ _, ch))   = initTrigger st' n >>= flip (foldM initDecl) ch
    initDecl st' (tag &&& children -> (DRole _, ch))          = foldM initDecl st' ch
    initDecl st' _                                            = return st'

    -- | Global initialization for cyclic dependencies.
    --   This partially initializes sinks and functions (to their defining lambda expression).
    initGlobal :: IState -> Identifier -> K3 Type -> Maybe (K3 Expression) -> EngineM Value IState
    initGlobal st' n (tag -> TSink) _          = initTrigger st' n
    initGlobal st' n t@(tag -> TFunction) eOpt = initFunction st' n t eOpt
    initGlobal st' _ _ _                       = return st'

    initTrigger st' n = return $ modifyStateEnv ((:) $ (n, VTrigger (n, Nothing))) st'
        
    -- Functions create lambda expressions, thus they are safe to initialize during
    -- environment construction. This way, all global functions can be mutually recursive.
    -- Note that since we only initialize functions, no declaration can force function
    -- evaluation during initialization (e.g., as would be the case with variable initializers).
    initFunction st' n _ (Just e) = initializeExpr st' n e
    initFunction st' n t Nothing  = initializeBinding st' $ builtin n t

    initializeExpr st' n e = initializeBinding st' (expression e >>= modifyE . (:) . (n,))
    initializeBinding st' interp = runInterpretation' st' interp
                                     >>= liftError "(initializing environment)"
                                     >>= return . getResultState

    -- | Global identifier registration
    registerGlobal :: Identifier -> IState -> IState
    registerGlobal n (w,x,y) = (n:w, x, y)

    registerDecl :: IState -> K3 Declaration -> IState
    registerDecl st' (tag -> DGlobal n _ _)  = _debugI_RegisterGlobal ("Registering global "++n) registerGlobal n st'
    registerDecl st' (tag -> DTrigger n _ _) = _debugI_RegisterGlobal ("Registering global "++n) registerGlobal n st'
    registerDecl st' _                       = _debugI_RegisterGlobal ("Skipping global registration") st'


initState :: K3 Declaration -> EngineM Value IState
initState prog = initEnvironment prog emptyState

initMessages :: IResult () -> EngineM Value (IResult Value)
initMessages = \case
    ((Right _, s), ilog)
      | Just (VFunction (f, [])) <- lookup "atInit" $ getEnv s -> runInterpretation' s (f vunit)
      | otherwise                                              -> return ((unknownTrigger, s), ilog)
    ((Left err, s), ilog)                                      -> return ((Left err, s), ilog)
  where unknownTrigger = Left $ RunTimeTypeError "Could not find atInit trigger"

initBootstrap :: PeerBootstrap -> EngineM Value (IEnvironment Value)
initBootstrap bootstrap = flip mapM bootstrap (\(n,l) ->
    runInterpretation' emptyState (literal l) 
      >>= liftError "(initializing bootstrap)"
      >>= either invalidErr (return . (n,)) . getResultVal)
  where invalidErr = const $ throwEngineError $ EngineError "Invalid result"

initProgram :: PeerBootstrap -> K3 Declaration -> EngineM Value (IResult Value)
initProgram bootstrap prog = do
    env <- initBootstrap bootstrap
    st  <- initState prog
    st' <- return $ injectBootstrap st env
    r   <- runInterpretation' st' (declaration prog)
    initMessages r
  where injectBootstrap (globals, vEnv, annEnv) bootEnv = 
          (globals, map (\(n,v) -> maybe (n,v) (n,) $ lookup n bootEnv) vEnv, annEnv)

finalProgram :: IState -> EngineM Value (IResult Value)
finalProgram st = runInterpretation' st $ maybe unknownTrigger runFinal $ lookup "atExit" $ getEnv st
  where runFinal (VFunction (f,[])) = f vunit
        runFinal _                  = throwE $ RunTimeTypeError "Invalid atExit trigger"
        unknownTrigger              = throwE $ RunTimeTypeError "Could not find atExit trigger"


{- Standalone (i.e., single peer) evaluation -}

standaloneInterpreter :: (IEngine -> IO a) -> IO a
standaloneInterpreter f = simulationEngine defaultSystem syntaxValueWD >>= f

runExpression :: K3 Expression -> IO (Maybe Value)
runExpression e = standaloneInterpreter (withEngine')
  where
    withEngine' engine = flip runEngineM engine (valueOfInterpretation emptyState (expression e))
        >>= return . either (const Nothing) id

runExpression_ :: K3 Expression -> IO ()
runExpression_ e = runExpression e >>= putStrLn . show


{- Distributed program execution -}

-- | Single-machine system simulation.
runProgram :: SystemEnvironment -> K3 Declaration -> IO (Either EngineError ())
runProgram systemEnv prog = do
    engine <- simulationEngine systemEnv syntaxValueWD
    flip runEngineM engine $ runEngine virtualizedProcessor prog

-- | Single-machine network deployment.
--   Takes a system deployment and forks a network engine for each peer.
runNetwork :: SystemEnvironment -> K3 Declaration -> IO [Either EngineError (Address, Engine Value, ThreadId)]
runNetwork systemEnv prog =
  let nodeBootstraps = map (:[]) systemEnv in do
    engines       <- mapM (flip networkEngine syntaxValueWD) nodeBootstraps
    namedEngines  <- return . map pairWithAddress $ zip engines nodeBootstraps
    engineThreads <- mapM fork namedEngines
    return engineThreads
  where
    pairWithAddress (engine, bootstrap) = (fst . head $ bootstrap, engine)
    fork (addr, engine) = do
      threadId <- flip runEngineM engine $ forkEngine virtualizedProcessor prog
      return $ either Left (Right . (addr, engine,)) threadId


{- Message processing -}

runTrigger :: IResult Value -> Identifier -> Value -> Value -> EngineM Value (IResult Value)
runTrigger r n a = \case
    (VTrigger (_, Just f)) -> runInterpretation' (getResultState r) (f a)
    (VTrigger _)           -> return . iError $ "Uninitialized trigger " ++ n
    _                      -> return . tError $ "Invalid trigger or sink value for " ++ n

  where iError = mkError r . RunTimeInterpretationError
        tError = mkError r . RunTimeTypeError
        mkError ((_,st), ilog) v = ((Left v, st), ilog)

uniProcessor :: MessageProcessor (K3 Declaration) Value (IResult Value) (IResult Value)
uniProcessor = MessageProcessor {
    initialize = initUP,
    process = processUP,
    status = statusUP,
    finalize = finalizeUP,
    report = reportUP
} where
    initUP prog = ask >>= flip initProgram prog . uniBootstrap . deployment
    uniBootstrap [] = []
    uniBootstrap ((_,is):_) = is

    statusUP res   = either (\_ -> Left res) (\_ -> Right res) $ getResultVal res
    finalizeUP res = either (\_ -> return res) (\_ -> finalProgram $ getResultState res) $ getResultVal res

    reportUP (Left err)  = showIResult err >>= liftIO . putStr
    reportUP (Right res) = showIResult res >>= liftIO . putStr

    processUP (_, n, args) r = maybe (return $ unknownTrigger r n) (run r n args) $
        lookup n $ getEnv $ getResultState r

    run r n args trig = logTrigger defaultAddress n args r >> runTrigger r n args trig

    unknownTrigger ((_,st), ilog) n = ((Left . RunTimeTypeError $ "Unknown trigger " ++ n, st), ilog)

virtualizedProcessor :: MessageProcessor (K3 Declaration) Value [(Address, IResult Value)] [(Address, IResult Value)]
virtualizedProcessor = MessageProcessor {
    initialize = initializeVP,
    process = processVP,
    status = statusVP,
    finalize = finalizeVP,
    report = reportVP
} where
    initializeVP program = do
        engine <- ask
        sequence [initNode node program (deployment engine) | node <- nodes engine]

    initNode node program systemEnv = do
        initEnv <- return $ maybe [] id $ lookup node systemEnv
        iProgram <- initProgram initEnv program
        logResult "INIT " node iProgram
        return (node, iProgram)

    processVP (addr, name, args) ps = fmap snd $ flip runDispatchT ps $ do
        dispatch addr (\s -> logTrigger addr name args s >> runTrigger' s name args)

    runTrigger' s n a = case lookup n $ getEnv $ getResultState s of
        Nothing -> return (Just (), unknownTrigger s n)
        Just ft -> fmap (Just (),) $ runTrigger s n a ft

    unknownTrigger ((_,st), ilog) n = ((Left . RunTimeTypeError $ "Unknown trigger " ++ n, st), ilog)

    -- TODO: Fix status computation to use rest of list.
    statusVP [] = Left []
    statusVP is@(p:_) = case sStatus p of
        Left _ -> Left is
        Right _ -> Right is

    sStatus (node, res) = either (const $ Left (node, res)) (const $ Right (node, res)) $ getResultVal res

    finalizeVP = mapM sFinalize

    sFinalize (node, res) = do
        res' <- either (const $ return res) (const $ finalProgram $ getResultState res) $ getResultVal res
        return (node, res')

    reportVP (Left err)  = mapM_ reportNodeIResult err
    reportVP (Right res) = mapM_ reportNodeIResult res

    reportNodeIResult (addr, r) = do
      void $ liftIO (putStrLn ("[" ++ show addr ++ "]"))
      void $ prettyIResult r >>= liftIO . putStr . unlines . indent 2 


{- Wire descriptions -}

valueWD :: WireDesc Value
valueWD = WireDesc (return . show) (return . Just . read) $ Delimiter "\n"

syntaxValueWD :: WireDesc Value
syntaxValueWD = WireDesc (packValueSyntax True) (\s -> unpackValueSyntax s >>= return . Just) $ Delimiter "\n"

wireDesc :: String -> WireDesc Value
wireDesc "k3" = syntaxValueWD
wireDesc fmt  = error $ "Invalid format " ++ fmt


{- Pretty printing -}
prettyValue :: Value -> IO String
prettyValue = packValueSyntax False

prettyResultValue :: Either InterpretationError Value -> IO [String]
prettyResultValue (Left err)  = return ["Error: " ++ show err]
prettyResultValue (Right val) = prettyValue val >>= return . (:[]) . ("Value: " ++)

prettyEnv :: IEnvironment Value -> IO [String]
prettyEnv env = do
    nWidth   <- return . maximum $ map (length . fst) env
    bindings <- mapM (prettyEnvEntry nWidth) $ sortBy (on compare fst) env
    return $ ["Environment:"] ++ concat bindings 
  where 
    prettyEnvEntry w (n,v) =
      prettyValue v >>= return . shift (prettyName w n) (prefixPadTo (w+4) " .. ") . wrap 70

    prettyName w n    = (suffixPadTo w n) ++ " => "
    suffixPadTo len n = n ++ replicate (max (len - length n) 0) ' '
    prefixPadTo len n = replicate (max (len - length n) 0) ' ' ++ n

prettyIResult :: IResult Value -> EngineM Value [String]
prettyIResult ((res, st), _) =
  liftIO ((++) <$> prettyResultValue res <*> prettyEnv (getEnv st))

showIResult :: IResult Value -> EngineM Value String
showIResult r = prettyIResult r >>= return . boxToString


{- Debugging helpers -}

debugDecl :: (Show a, Pretty b) => a -> b -> c -> c
debugDecl n t = _debugI . boxToString $
  [concat ["Declaring ", show n, " : "]] ++ (indent 2 $ prettyLines t)

showResult :: String -> IResult Value -> EngineM Value [String]
showResult str r = 
  prettyIResult r >>= return . ([str ++ " { "] ++) . (++ ["}"]) . indent 2

showDispatch :: Address -> Identifier -> Value -> IResult Value -> EngineM Value [String]
showDispatch addr name args r =
    wrap' <$> liftIO (prettyValue args)
         <*> (showResult "BEFORE" r >>= return . indent 2)
  where
    wrap' arg res =  ["", "TRIGGER " ++ name ++ " " ++ show addr ++ " { "]
                  ++ ["  Args: " ++ arg] 
                  ++ res ++ ["}"]

logResult :: String -> Address -> IResult Value -> EngineM Value ()
logResult tag' addr r = do
    msg <- showResult (tag' ++ show addr) r 
    void $ _notice_Dispatch $ boxToString msg

logTrigger :: Address -> Identifier -> Value -> IResult Value -> EngineM Value ()
logTrigger addr n args r = do
    msg <- showDispatch addr n args r
    void $ _notice_Dispatch $ boxToString msg


{- Instances -}
instance Pretty IState where
  prettyLines (_, vEnv, aEnv) =
         ["Environment:"] ++ (indent 2 $ map prettyEnvEntry $ sortBy (on compare fst) vEnv)
      ++ ["Annotations:"] ++ (indent 2 $ lines $ show aEnv)
    where
      prettyEnvEntry (n,v) = n ++ replicate (maxNameLength - length n) ' ' ++ " => " ++ show v
      maxNameLength        = maximum $ map (length . fst) vEnv

instance (Pretty a) => Pretty (IResult a) where
    prettyLines ((r, st), _) = ["Status: "] ++ either ((:[]) . show) prettyLines r ++ prettyLines st

instance (Pretty a) => Pretty [(Address, IResult a)] where
    prettyLines l = concatMap (\(x,y) -> [""] ++ prettyLines x ++ (indent 2 $ prettyLines y)) l

{- Value equality -}
instance Eq Value where
  VBool v        == VBool v'        = v == v'
  VByte b        == VByte b'        = b == b'
  VInt  v        == VInt  v'        = v == v'
  VReal v        == VReal v'        = v == v'
  VString v      == VString v'      = v == v'
  VOption v      == VOption v'      = v == v'
  VTuple v       == VTuple v'       = all (uncurry (==)) $ zip v v'
  VRecord v      == VRecord v'      = all (uncurry (==)) $ zip v v'
  VAddress v     == VAddress v'     = v == v'  
  VCollection  v == VCollection v'  = v == v'
  VIndirection v == VIndirection v' = v == v'  
  _              == _               = False

{- Show/print methods -}
showsPrecTag :: Show a => String -> Int -> a -> ShowS
showsPrecTag s d v = showsPrecTagF s d $ showsPrec (appPrec+1) v
  where appPrec = 10

showsPrecTagF :: String -> Int -> ShowS -> ShowS
showsPrecTagF s d showF =
  showParen (d > appPrec) $ showString (s++" ") . showF
  where appPrec = 10

-- | Verbose stringification of values through show instance.
--   This produces <tag> placeholders for unshowable values (IORefs and functions)
instance Show Value where
  showsPrec d (VBool v)        = showsPrecTag "VBool" d v
  showsPrec d (VByte v)        = showsPrecTag "VByte" d v
  showsPrec d (VInt v)         = showsPrecTag "VInt" d v
  showsPrec d (VReal v)        = showsPrecTag "VReal" d v
  showsPrec d (VString v)      = showsPrecTag "VString" d v
  showsPrec d (VOption v)      = showsPrecTag "VOption" d v
  showsPrec d (VTuple v)       = showsPrecTag "VTuple" d v
  showsPrec d (VRecord v)      = showsPrecTag "VRecord" d v
  showsPrec d (VAddress v)     = showsPrecTag "VAddress" d v
  
  showsPrec d (VCollection _)  = showsPrecTagF "VCollection"  d $ showString "<opaque>"
  showsPrec d (VIndirection _) = showsPrecTagF "VIndirection" d $ showString "<opaque>"
  showsPrec d (VFunction _)    = showsPrecTagF "VFunction"    d $ showString "<function>"
  showsPrec d (VTrigger (_, Nothing)) = showsPrecTagF "VTrigger" d $ showString "<uninitialized>"
  showsPrec d (VTrigger (_, Just _))  = showsPrecTagF "VTrigger" d $ showString "<function>"

-- | Verbose stringification of values through read instance.
--   This errors on attempting to read unshowable values (IORefs and functions)
instance Read Value where
  readPrec = parens $ 
        (prec app_prec $ do
          Ident "VBool" <- lexP
          v <- step readPrec
          return (VBool v))

    +++ (prec app_prec $ do
          Ident "VByte" <- lexP
          v <- step readPrec
          return (VByte v))

    +++ (prec app_prec $ do
          Ident "VInt" <- lexP
          v <- step readPrec
          return (VInt v))

    +++ (prec app_prec $ do
          Ident "VReal" <- lexP
          v <- step readPrec
          return (VReal v))

    +++ (prec app_prec $ do
          Ident "VString" <- lexP
          v <- step readPrec
          return (VString v))

    +++ (prec app_prec $ do
          Ident "VOption" <- lexP
          v <- step readPrec
          return (VOption v))

    +++ (prec app_prec $ do
          Ident "VTuple" <- lexP
          v <- step readPrec
          return (VTuple v))

    +++ (prec app_prec $ do
          Ident "VRecord" <- lexP
          v <- step readPrec
          return (VRecord v))

    +++ (prec app_prec $ do
          Ident "VCollection" <- lexP
          Ident "<opaque>" <- step readPrec
          error "Cannot read collections")

    +++ (prec app_prec $ do
          Ident "VIndirection" <- lexP
          Ident "<opaque>" <- step readPrec
          error "Cannot read indirections")

    +++ (prec app_prec $ do
          Ident "VFunction" <- lexP
          Ident "<function>" <- step readPrec
          error "Cannot read functions")

    +++ (prec app_prec $ do
          Ident "VAddress" <- lexP
          v <- step readPrec
          return (VAddress v))

    +++ (prec app_prec $ do
          Ident "VTrigger" <- lexP
          Ident _ <- lexP
          Ident "<uninitialized>" <- step readPrec
          error "Cannot read triggers")

    +++ (prec app_prec $ do
          Ident "VTrigger" <- lexP
          Ident _ <- lexP
          Ident "<function>" <- step readPrec
          error "Cannot read triggers")

    where app_prec = 10

  readListPrec = readListPrecDefault

-- TODO
instance Pretty Value where
  prettyLines v = [show v]


-- | Syntax-oriented stringification of values.
packValueSyntax :: Bool -> Value -> IO String
packValueSyntax forTransport v = packValue 0 v >>= return . ($ "")
  where
    rt = return
    packValue :: Int -> Value -> IO ShowS
    packValue d = \case
      VBool v'     -> rt $ showsPrec d v'
      VByte v'     -> rt $ showChar 'B' . showParen True (showsPrec appPrec1 v')
      VInt v'      -> rt $ showsPrec d v'
      VReal v'     -> rt $ showsPrec d v'
      VString v'   -> rt $ showsPrec d v'
      VOption vOpt -> packOpt d vOpt
      VTuple v'    -> parens' (packValue $ d+1) v'
      VRecord v'   -> packNamedValues (d+1) v'
      
      VCollection c  -> packCollectionPrec (d+1) c      
      VIndirection r -> readIORef r >>= (\v' -> (.) <$> rt (showChar 'I') <*> packValue (d+1) v')
      VAddress v'    -> rt $ showsPrec d v'

      VFunction _    -> (forTransport ? error $ (rt . showString)) funSym
      VTrigger (n,_) -> (forTransport ? error $ (rt . showString)) $ trigSym n

    funSym    = "<function>"
    trigSym n = "<trigger " ++ n ++ " >"

    parens'  = packCustomList "(" ")" ","
    braces   = packCustomList "{" "}" ","
    brackets = packCustomList "[" "]" ","

    packOpt d vOpt =
      maybe (rt ("Nothing "++)) 
            (\v' -> packValue appPrec1 v' >>= \showS -> rt (showParen (d > appPrec) ("Just " ++) . showS))
            vOpt

    packCollectionPrec d cmv = do
      Collection (CollectionNamespace cns ans) v' extId <- readMVar cmv 
      wrap' "{" "}" ((\a b c d' -> a . b . c . d')
        <$> packCollectionNamespace (d+1) (cns, ans)
        <*> rt (showChar ',')
        <*> packCollectionDataspace (d+1) v'
        <*> rt (showChar ',' . showString extId))

    packCollectionNamespace d (cns, ans) =
      (\a b c d' e -> a . b . c . d' . e)
        <$> rt (showString "CNS=") <*> packNamedValues d cns
        <*> rt (showChar ',')
        <*> rt (showString "ANS=") <*> braces (packDoublyNamedValues d) ans
    
    packCollectionDataspace d v' =
      (\a b -> a . b) <$> (rt $ showString "DS=") <*> brackets (packValue d) v'
    
    packDoublyNamedValues d (n, nv)  = (.) <$> rt (showString $ n ++ "=") <*> packNamedValues d nv

    packNamedValues d nv        = braces (packNamedValuePrec d) nv
    packNamedValuePrec d (n,v') = (.) <$> rt (showString n . showChar '=') <*> packValue d v'

    packCustomList :: String -> String -> String -> (a -> IO ShowS) -> [a] -> IO ShowS
    packCustomList lWrap rWrap _ _ []             = rt $ \s -> lWrap ++ rWrap ++ s
    packCustomList lWrap rWrap sep packF (x:xs)   = (\a b -> (lWrap ++) . a . b) <$> packF x <*> packl xs
      where packl []     = rt $ \s -> rWrap ++ s
            packl (y:ys) = (\a b -> (sep++) . a . b) <$> packF y <*> packl ys

    wrap' lWrap rWrap packx =
      (\a b c -> a . b . c) <$> rt (showString lWrap) <*> packx <*> rt (showString rWrap)

    appPrec  = 10
    appPrec1 = 11  

    True  ? x = const x  
    False ? _ = id


unpackValueSyntax :: String -> IO Value
unpackValueSyntax = readSingleParse unpackValue
  where
    rt :: a -> IO a
    rt = return

    unpackValue :: ReadPrec (IO Value)
    unpackValue = parens $ 
          (readPrec  >>= return . rt . VInt)
      <++ ((readPrec >>= return . rt . VBool)
      +++ (readPrec  >>= return . rt . VReal)
      +++ (readPrec  >>= return . rt . VString)

      +++ (do
            Ident "B" <- lexP
            v <- prec appPrec1 readPrec
            return . rt $ VByte v)

      +++ (do
            Ident "Just" <- lexP
            v <- prec appPrec1 unpackValue
            return (v >>= rt . VOption . Just))

      +++ (do
            Ident "Nothing" <- lexP
            return . rt $ VOption Nothing)

      +++ (prec appPrec $ do
            v <- step $ readParens unpackValue
            return (sequence v >>= rt . VTuple))

      +++ (do
            nv <- readNamedValues unpackValue
            return (nv >>= rt . VRecord))

      +++ (do
            Ident "I" <- lexP
            v <- step unpackValue
            return (v >>= newIORef >>= rt . VIndirection))

      +++ readCollectionPrec
      
      +++ (readPrec >>= return . rt . VAddress))

    readCollectionPrec = parens $
      (prec appPrec $ do
        Punc "{" <- lexP
        ns       <- step $ readCollectionNamespace
        Char ',' <- lexP
        v        <- step $ readCollectionDataspace
        Char ',' <- lexP
        Ident n  <- lexP
        Punc "}" <- lexP
        return $ (do
          x   <- ns
          y   <- v
          cmv <- newMVar $ Collection x y n
          rt $ VCollection cmv))

    readCollectionNamespace :: ReadPrec (IO (CollectionNamespace Value))
    readCollectionNamespace = parens $
      (prec appPrec $ do
        void      $ readExpectedName "CNS"
        cns      <- readNamedValues unpackValue
        Char ',' <- lexP
        void      $ readExpectedName "ANS"
        ans      <- readDoublyNamedValues
        return $ (do
          cns' <- cns
          ans' <- ans
          rt $ CollectionNamespace cns' ans'))

    readCollectionDataspace :: ReadPrec (IO [Value])
    readCollectionDataspace = parens $
      (prec appPrec $ do
        void $ readExpectedName "DS"
        v <- readBrackets unpackValue
        return $ sequence v)

    readDoublyNamedValues :: ReadPrec (IO [(String, [(String, Value)])])
    readDoublyNamedValues = parens $
      (prec appPrec $ do
        v <- readBraces $ readNamedPrec $ readNamedValues unpackValue
        return $ sequence v)

    readNamedValues :: ReadPrec (IO a) -> ReadPrec (IO [(String, a)])
    readNamedValues readF = parens $
      (prec appPrec $ do
        v <- readBraces $ readNamedPrec readF
        return $ sequence v)

    readNamedPrec :: ReadPrec (IO a) -> ReadPrec (IO (String, a))
    readNamedPrec readF = parens $ 
      (prec appPrec $ do
        n <- readName 
        v <- step readF
        return $ v >>= rt . (n,))

    readExpectedName :: String -> ReadPrec ()
    readExpectedName expected = do
      Ident n  <- lexP
      Punc "=" <- lexP
      if n == expected then return () else pfail

    readName :: ReadPrec String
    readName = do
      Ident n  <- lexP
      Punc "=" <- lexP
      return n

    readParens   = readCustomList "(" ")" ","
    readBraces   = readCustomList "{" "}" ","
    readBrackets = readCustomList "[" "]" ","
    
    readSingleParse :: Show a => ReadPrec (IO a) -> String -> IO a
    readSingleParse readP s =
      case [ x | (x,"") <- readPrec_to_S read' minPrec s ] of
        [x] -> x
        []  -> error "Interpreter.unpackValue: no parse"
        l   -> error $ "Interpreter.unpackValue: ambiguous parse (" ++ (show $ length l) ++ " variants)"
      where read' = do
              x <- readP
              TR.lift P.skipSpaces
              return x

    readCustomList :: String -> String -> String -> ReadPrec (IO a) -> ReadPrec [IO a]
    readCustomList lWrap rWrap sep readx =
      parens
      ( do Punc c <- lexP
           if c == lWrap 
           then (listRest False +++ listNext) else pfail
      )
     where
      listRest started =
        do Punc c <- lexP
           if c == rWrap then return []
           else if c == sep && started then listNext
           else pfail
      
      listNext =
        do x  <- reset readx
           xs <- listRest True
           return (x:xs)

    appPrec = 10
    appPrec1 = 11  


instance (Show v) => Show (AEnvironment v) where
  show (AEnvironment defs _) = show defs
