{-# LANGUAGE CPP #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}

-- | The K3 Interpreter
module Language.K3.Interpreter (
  -- | Types
  Value(..),

  Interpretation,
  InterpretationError(..),

  IEnvironment,
  ILog,

  -- | Interpreters
  runInterpretation,

  runExpression,
  runExpression_,

  runNetwork,
  runProgram,

  emptyState,
  getResultVal,

  packValueSyntax,
  unpackValueSyntax,

  valueWD,
  syntaxValueWD,

  emptyStaticEnv

-- #ifdef TEST
-- TODO When the ifdef is uncommented, the dataspace tests don't compile
  , throwE
-- #endif
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
import Data.Maybe
import Data.Tree
import Data.Word (Word8)

import Debug.Trace

import Text.Read hiding (get, lift)
import qualified Text.Read          as TR (lift)
import Text.ParserCombinators.ReadP as P (skipSpaces)

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Transform.Interpreter.BindAlias ( labelBindAliases )

import Language.K3.Runtime.Common ( PeerBootstrap, SystemEnvironment, defaultSystem )
import Language.K3.Runtime.Dispatch
import Language.K3.Runtime.Engine
import Language.K3.Runtime.Dataspace
import Language.K3.Runtime.FileDataspace

import Language.K3.Utils.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["Dispatch", "RegisterGlobal", "BindPath"])

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
data IState = IState { getGlobals    :: Globals
                     , getEnv        :: IEnvironment Value
                     , getAnnotEnv   :: AEnvironment Value
                     , getStaticEnv  :: SEnvironment Value
                     , getBindStack  :: BindPathStack }

-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IState (WriterT ILog (EngineM Value)))

-- | An evaluated value type, produced from running an interpretation.
type IResult a = ((Either InterpretationError a, IState), ILog)

-- | Pairing of errors and environments for debugging output.
type EnvOnError = (InterpretationError, IEnvironment Value)

{- Bind writeback support -}
type BindPath = [BindStep]

data BindStep
    = Named Identifier
    | Temporary Identifier
    | Indirection
    | TupleField Int
    | RecordField Identifier
  deriving (Eq, Show, Read)

type BindPathStack = [Maybe BindPath]


{- Collections and annotations -}

data CollectionDataspace v = InMemoryDS [v] | ExternalDS (FileDataspace v)

-- | Collection implementation.
--   The namespace contains lifted members, the dataspace contains final
--   records, and the extension identifier is the instance's annotation signature.
data Collection v = Collection { namespace   :: CollectionNamespace v
                               , dataspace   :: CollectionDataspace v
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

data CollectionBinders v =
  CollectionBinders { emptyCtor   :: CEmptyConstructor v
                    , initialCtor :: CInitialConstructor v
                    , copyCtor    :: CCopyConstructor v
                    , emplaceCtor :: CEmplaceConstructor v }

-- | A collection initializer that populates default lifted attributes.
type CEmptyConstructor v = () -> Interpretation (MVar (Collection v))

-- | A collection initializer that takes a list of values and builds
--   a collection populated with those values.
type CInitialConstructor v = [v] -> Interpretation (MVar (Collection v))

-- | A copy constructor that takes a collection, copies its non-function fields,
--   and rebinds its member functions to lift/lower bindings to/from the new collection.
type CCopyConstructor v = Collection v -> Interpretation (MVar (Collection v))

type CEmplaceConstructor v = CollectionDataspace v -> Interpretation (MVar (Collection v))

-- | An environment for rebuilding static values after their serialization.
--   The static value can either be a value (e.g., a global function) or a 
--   collection namespace associated with an annotation combination id.
type SEnvironment v = (IEnvironment v, AEnvironment v)


{- Misc. helpers-}

details :: K3 a -> (a, [K3 a], [Annotation a])
details (Node (tg :@: anns) ch) = (tg, ch, anns)


{- State and result accessors -}

modifyStateGlobals :: (Globals -> Globals) -> IState -> IState
modifyStateGlobals f (IState g x y z bind_stack) = IState (f g) x y z bind_stack

modifyStateEnv :: (IEnvironment Value -> IEnvironment Value) -> IState -> IState
modifyStateEnv f (IState w x y z bindStack) = IState w (f x) y z bindStack

modifyStateAEnv :: (AEnvironment Value -> AEnvironment Value) -> IState -> IState
modifyStateAEnv f (IState w x y z bindStack) = IState w x (f y) z bindStack

modifyStateSEnv :: (SEnvironment Value -> SEnvironment Value) -> IState -> IState
modifyStateSEnv f (IState g e a s bindStack) = IState g e a (f s) bindStack

modifyStateBinds :: (BindPathStack -> BindPathStack) -> IState -> IState
modifyStateBinds f (IState v w x y bindStack) = (IState v w x y (f bindStack))

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

-- | Annotation environment helpers.
lookupADef :: Identifier -> Interpretation (IEnvironment Value)
lookupADef n = get >>= maybe err return . lookup n . definitions . getAnnotEnv
  where err = throwE $ RunTimeTypeError $ "Unknown annotation definition: '" ++ n ++ "'"

lookupACombo :: Identifier -> Interpretation (CollectionBinders Value)
lookupACombo n = tryLookupACombo n >>= maybe err return
  where err = throwE $ RunTimeTypeError $ "Unknown annotation combination: '" ++ n ++ "'"

tryLookupACombo :: Identifier -> Interpretation (Maybe (CollectionBinders Value))
tryLookupACombo n = get >>= return . lookup n . realizations . getAnnotEnv

modifyAnnEnv :: (AEnvironment Value -> AEnvironment Value) -> Interpretation ()
modifyAnnEnv f = modify $ modifyStateAEnv f

modifyADefs :: (AnnotationDefinitions Value -> AnnotationDefinitions Value) -> Interpretation ()
modifyADefs f = modifyAnnEnv (\aEnv -> AEnvironment (f $ definitions aEnv) (realizations aEnv))

modifyACombos :: (AnnotationCombinations Value -> AnnotationCombinations Value) -> Interpretation ()
modifyACombos f = modifyAnnEnv (\aEnv -> AEnvironment (definitions aEnv) (f $ realizations aEnv))

-- | Bind stack helpers
getBindPath :: Interpretation (Maybe BindPath)
getBindPath = get >>= return . (\x -> if x == [] then Nothing else last x) . getBindStack

modifyBindStack :: (BindPathStack -> BindPathStack) -> Interpretation ()
modifyBindStack f = modify $ modifyStateBinds f

modifyBindStack_ :: (BindPathStack -> Interpretation BindPathStack) -> Interpretation ()
modifyBindStack_ f =
  do { st <- get;
      newBindStack <- f (getBindStack st);
      put $ modifyStateBinds (\_ -> newBindStack) st }

pushBindFrame :: Interpretation ()
pushBindFrame = modifyBindStack (\bs -> bs++[Nothing])

popBindFrame :: Interpretation ()
popBindFrame = modifyBindStack (\bs -> init bs)

appendAlias :: BindStep -> Interpretation ()
appendAlias n = modifyBindStack_ pushBindAlias
  where pushBindAlias [] = return []
        pushBindAlias bs = case last bs of
          Nothing -> return $ init bs ++ [Just [n]]
          Just _  -> throwE $ RunTimeInterpretationError "Duplicate bind alias"

appendAliasExtension :: Identifier -> Interpretation ()
appendAliasExtension n = modifyBindStack_ pushBindAliasExtension
  where pushBindAliasExtension [] = return []
        pushBindAliasExtension bs = case last bs of
          Just bindpath -> return $ init bs ++ [Just $ bindpath++[RecordField n]]
          Nothing       -> throwE $ RunTimeInterpretationError "No bind alias found for extension"

-- | Monadic message passing primitive for the interpreter.
sendE :: Address -> Identifier -> Value -> Interpretation ()
sendE addr n val = liftEngine $ send addr n val


{- Constants -}
vunit :: Value
vunit = VTuple []

{- Collection operations -}
emptyCollectionNamespace :: CollectionNamespace Value
emptyCollectionNamespace = CollectionNamespace [] []

-- TODO any dataspace
-- Used to interpret literals, so InMemory?
initialCollection :: [Value] -> Interpretation Value
initialCollection vals = liftIO (newMVar $ initialCollectionBody "" vals) >>= return . VCollection
  where
    initialCollectionBody :: Identifier -> [Value] -> Collection Value
    initialCollectionBody n vals = Collection emptyCollectionNamespace (InMemoryDS vals) n
    

{- generalize on value, move to Runtime/Dataspace.hs -}
instance (Monad m) => Dataspace m [Value] Value where
  emptyDS _        = return []
  initialDS vals _ = return vals
  copyDS ls        = return ls
  peekDS ls        = case ls of
    []    -> return Nothing
    (h:_) -> return $ Just h
  insertDS ls v    = return $ ls ++ [v]
  deleteDS v ls    = return $ delete v ls
  updateDS v v' ls = return $ (delete v ls) ++ [v']
  foldDS           = foldM
  mapDS            = mapM
  mapDS_           = mapM_
  filterDS         = filterM
  combineDS l r    = return $ l ++ r
  splitDS l        = return $ if length l <= threshold then (l, []) else splitAt (length l `div` 2) l
    where
      threshold = 10

{- TODO have the engine delete all the collection files in it's cleanup
 - generalize Value -> b (in EngineM b), and monad -> engine
 -}
instance Dataspace Interpretation (FileDataspace Value) Value where
  emptyDS _        = liftEngine $ emptyFile ()
  initialDS vals _ = initialFile liftEngine vals
  copyDS old_id    = liftEngine $ copyFile old_id
  peekDS ext_id    = peekFile liftEngine ext_id
  insertDS         = insertFile liftEngine
  deleteDS         = deleteFile liftEngine
  updateDS         = updateFile liftEngine
  foldDS           = foldFile liftEngine
  -- Takes the file id of an external collection, then runs the second argument on
  -- each argument, then returns the identifier of the new collection
  mapDS            = mapFile liftEngine
  mapDS_           = mapFile_ liftEngine
  filterDS         = filterFile liftEngine
  combineDS        = combineFile liftEngine
  splitDS          = splitFile liftEngine

instance Dataspace Interpretation (CollectionDataspace Value) Value where
  emptyDS maybeHint =
    case maybeHint of
      Nothing ->
        (emptyDS Nothing) >>= return . InMemoryDS
      Just (InMemoryDS ls) ->
        (emptyDS (Just ls)) >>= return . InMemoryDS
      Just (ExternalDS ext) ->
        (emptyDS (Just ext)) >>= return . ExternalDS
  initialDS vals maybeHint =
    case maybeHint of
      Nothing ->
        (initialDS vals Nothing) >>= return . InMemoryDS
      Just (InMemoryDS ls) ->
        (initialDS vals (Just ls)) >>= return . InMemoryDS
      Just (ExternalDS ext) ->
        (initialDS vals (Just ext)) >>= return . ExternalDS
  copyDS ds        = case ds of
    InMemoryDS lst -> copyDS lst >>= return . InMemoryDS
    ExternalDS f   -> copyDS f >>= return . ExternalDS
  peekDS ds        = case ds of
    InMemoryDS lst -> peekDS lst
    ExternalDS f   -> peekDS f
  insertDS ds val  = case ds of
    InMemoryDS lst -> insertDS lst val >>= return . InMemoryDS
    ExternalDS f   -> insertDS f   val >>= return . ExternalDS
  deleteDS val ds  = case ds of
    InMemoryDS lst -> deleteDS val lst >>= return . InMemoryDS
    ExternalDS f   -> deleteDS val f   >>= return . ExternalDS
  updateDS v v' ds  = case ds of
    InMemoryDS lst -> updateDS v v' lst >>= return . InMemoryDS
    ExternalDS f   -> updateDS v v' f   >>= return . ExternalDS
  foldDS acc acc_init ds = case ds of
    InMemoryDS lst -> foldDS acc acc_init lst
    ExternalDS f   -> foldDS acc acc_init f
  mapDS func ds     = case ds of
    InMemoryDS lst -> mapDS func lst >>= return . InMemoryDS
    ExternalDS f   -> mapDS func f   >>= return . ExternalDS
  mapDS_ func ds    = case ds of
    InMemoryDS lst -> mapDS_ func lst
    ExternalDS f   -> mapDS_ func f
  filterDS func ds     = case ds of
    InMemoryDS lst -> filterDS func lst >>= return . InMemoryDS
    ExternalDS f   -> filterDS func f   >>= return . ExternalDS
  combineDS l r     = case (l,r) of
    (InMemoryDS l_lst, InMemoryDS r_lst) ->
      combineDS l_lst r_lst >>= return . InMemoryDS
    (ExternalDS l_f, ExternalDS r_f) ->
      combineDS l_f r_f >>= return . ExternalDS
    otherwise -> throwE $ RunTimeInterpretationError "Mismatched collection types in combine"
        -- TODO Could combine an in memory store and an external store into an external store
  splitDS ds       = case ds of
    InMemoryDS lst -> splitDS lst >>= \(l, r) -> return (InMemoryDS l, InMemoryDS r)
    ExternalDS f   -> splitDS f   >>= \(l, r) -> return (ExternalDS l, ExternalDS r)

{- moves to Runtime/Dataspace.hs -}
matchPair :: Value -> Interpretation (Value, Value)
matchPair v =
  case v of
    VTuple (h:t) -> 
      case t of
        (p:[])    -> return (h, p)
        otherwise ->throwE $ RunTimeTypeError "Wrong number of elements in tuple; expected a pair"
    otherwise    -> throwE $ RunTimeTypeError "Non-tuple"

-- TODO kill dependence on Interpretation for error handling
instance EmbeddedKV Interpretation Value Value where
  extractKey value = do
    (key, _) <- matchPair value
    return key
  embedKey key value = return $ VTuple [key, value]
  
instance (Dataspace Interpretation dst Value) => AssociativeDataspace Interpretation dst Value Value where
  lookupKV ds key = 
    foldDS (\result cur_val ->  do
      (cur_key, cur_val) <- matchPair cur_val
      return $ case result of
        Nothing -> if cur_key == key then Just cur_val else Nothing
        Just val -> Just val
     ) Nothing ds
  removeKV ds key _ =
    filterDS (inner) ds
     where
        inner :: Value -> Interpretation Bool
        inner val = do
          cur_val <- extractKey val
          return $ if cur_val == key then False else True
  insertKV ds key value = embedKey key value >>= insertDS ds
  replaceKV ds k v = do
    _ <- removeKV ds k vunit
    insertKV ds k v

emptyStaticEnv :: SEnvironment Value
emptyStaticEnv = ([], AEnvironment [] [])

emptyBindStack :: BindPathStack
emptyBindStack = []

emptyState :: IState
emptyState = IState [] [] (AEnvironment [] []) emptyStaticEnv emptyBindStack

annotationState :: AEnvironment Value -> IState
annotationState aEnv = IState [] [] aEnv emptyStaticEnv emptyBindStack

--TODO is it okay to have empty trigger list here? QueueConfig
simpleEngine :: IO (Engine Value)
simpleEngine = simulationEngine [] False defaultSystem (syntaxValueWD emptyStaticEnv)

{- Identifiers -}
collectionAnnotationId :: Identifier
collectionAnnotationId = "Collection"

externalAnnotationId :: Identifier
externalAnnotationId = "External"

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

-- | These methods create a valid namespace by performing a lookup based on the annotation combo id.
-- TODO any dataspace
initialAnnotatedCollectionBody :: Identifier -> [Value] -> Interpretation (MVar (Collection Value))
initialAnnotatedCollectionBody comboId vals = do
    binders <- lookupACombo comboId
    let initF = initialCtor binders
    cmv        <- initF vals
    -- TODO does this line even do anything?
    void $ liftIO (modifyMVar_ cmv (\(Collection ns ds cid) -> return $ Collection ns ds cid))
    return cmv

initialAnnotatedCollection :: Identifier -> [Value] -> Interpretation Value
initialAnnotatedCollection comboId vals =
  initialAnnotatedCollectionBody comboId vals >>= return . VCollection

emptyAnnotatedCollection :: Identifier -> Interpretation Value
emptyAnnotatedCollection comboId = initialAnnotatedCollection comboId []

-- This gets used as the default collection, so it's an InMemory store
emptyCollection :: Interpretation Value
emptyCollection = initialCollection []

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
expression e_ = 
  case e_ @~ isBindAliasAnnotation of
    Just (EAnalysis (BindAlias i))          -> expr e_ >>= \r -> appendAlias (Named i) >> return r
    Just (EAnalysis (BindFreshAlias i))     -> expr e_ >>= \r -> appendAlias (Temporary i) >> return r
    Just (EAnalysis (BindAliasExtension i)) -> expr e_ >>= \r -> appendAliasExtension i >> return r
    Nothing  -> expr e_
    Just _   -> throwE $ RunTimeInterpretationError "Invalid bind alias annotation matching"

  where
    isBindAliasAnnotation (EAnalysis (BindAlias _))          = True
    isBindAliasAnnotation (EAnalysis (BindFreshAlias _))     = True
    isBindAliasAnnotation (EAnalysis (BindAliasExtension _)) = True
    isBindAliasAnnotation _                                  = False

    expr :: K3 Expression -> Interpretation Value
    -- | Interpretation of constant expressions.
    expr (tag &&& annotations -> (EConstant c, as)) = constant c as

    -- | Interpretation of variable lookups.
    expr (tag -> EVariable i) = lookupE i

    -- | Interpretation of option type construction expressions.
    expr (tag &&& children -> (ESome, [x])) = expression x >>= return . VOption . Just
    expr (tag -> ESome) = throwE $ RunTimeTypeError "Invalid Construction of Option"

    -- | Interpretation of indirection type construction expressions.
    expr (tag &&& children -> (EIndirect, [x])) = do
      expression_result <- expression x
      new_val <- copyValue expression_result
      new_ref <- liftIO $ newIORef new_val
      return $ VIndirection new_ref
    expr (tag -> EIndirect) = throwE $ RunTimeTypeError "Invalid Construction of Indirection"

    -- | Interpretation of tuple construction expressions.
    expr (tag &&& children -> (ETuple, cs)) = mapM expression cs >>= return . VTuple

    -- | Interpretation of record construction expressions.
    expr (tag &&& children -> (ERecord is, cs)) = mapM expression cs >>= return . VRecord . zip is

    -- | Interpretation of function construction.
    expr (tag &&& children -> (ELambda i, [b])) =
      mkFunction $ \v -> modifyE ((i,v):) >> expression b >>= removeE (i,v)
      where
        mkFunction f = closure >>= \cl -> return $ VFunction . (, cl) $ f

        -- TODO: currently, this definition of a closure captures 
        -- annotation member variables during annotation member initialization.
        -- This invalidates the use of annotation member function contextualization
        -- since the context is overridden by the closure whenever applying the
        -- member function.
        closure :: Interpretation (Closure Value)
        closure = do
          globals <- get >>= return . getGlobals
          vars    <- return $ filter (\n -> n /= i && n `notElem` globals) $ freeVariables b
          vals    <- mapM lookupE vars
          return $ zip vars vals

    -- | Interpretation of unary/binary operators.
    expr (tag &&& children -> (EOperate otag, cs))
        | otag `elem` [ONeg, ONot], [a] <- cs = unary otag a
        | otherwise, [a, b] <- cs = binary otag a b
        | otherwise = undefined

    -- | Interpretation of Record Projection.
    expr (tag &&& children -> (EProject i, [r])) = expression r >>= \case
        VRecord vr -> maybe (unknownField i) return $ lookup i vr

        VCollection cmv -> do
          Collection ns _ extId <- liftIO $ readMVar cmv
          if null extId then unannotatedCollection else case lookup i $ collectionNS ns of
            Nothing -> unknownCollectionMember i (map fst $ collectionNS ns)
            Just v' -> return v'

      where unknownField i' = throwE $ RunTimeTypeError $ "Unknown record field " ++ i'
            unannotatedCollection = throwE $ RunTimeTypeError $ "Invalid projection on an unannotated collection"
            unknownCollectionMember i' n = throwE $ RunTimeTypeError $ "Unknown collection member " ++ i' ++ "(valid " ++ show n ++ ")"

    expr (tag -> EProject _) = throwE $ RunTimeTypeError "Invalid Record Projection"

    -- | Interpretation of Let-In Constructions.
    expr (tag &&& children -> (ELetIn i, [e, b])) =
        expression e >>= (\v -> modifyE ((i,v):) >> expression b >>= removeE (i,v))
    expr (tag -> ELetIn _) = throwE $ RunTimeTypeError "Invalid LetIn Construction"

    -- | Interpretation of Assignment.
    expr (tag &&& children -> (EAssign i, [e])) = do
      _    <- lookupE i
      newV <- expression e
      void $ modifyE (((i, newV):) . (deleteBy ((==) `on` fst) (i, newV)))
      return vunit

    expr (tag -> EAssign _) = throwE $ RunTimeTypeError "Invalid Assignment"

    -- | Interpretation of Case-Matches.
    expr (tag &&& children -> (ECaseOf i, [e, s, n])) = expression e >>= \case
        VOption (Just v) -> modifyE ((i, v):) >> expression s >>= removeE (i,v)
        VOption (Nothing) -> expression n
        _ -> throwE $ RunTimeTypeError "Invalid Argument to Case-Match"
    expr (tag -> ECaseOf _) = throwE $ RunTimeTypeError "Invalid Case-Match"

    -- | Interpretation of Binding.
    expr (tag &&& children -> (EBindAs b, [e, f])) = withBindFrame $ do
      bv <- expression e
      bp <- getBindPath >>= \case
        Just ((Named n):t)     -> return $ (Named n):t
        Just ((Temporary n):t) -> return $ (Temporary n):t
        _ -> throwE $ RunTimeTypeError "Invalid bind path in bind-as expression"

      case (b, bv) of
        (BIndirection i, VIndirection r) -> 
          liftIO (readIORef r) >>= (\rV -> 
            modifyE ((i,rV):) >> 
            expression f >>= (\fV -> refreshBindings b bp bv >> removeE (i,rV) fV))

        (BTuple ts, VTuple vs) -> do
          let bindings = zip ts vs
          modifyE ((++) bindings) >>
            expression f >>= (\fV -> refreshBindings b bp bv >> removeAllE bindings fV)

        (BRecord ids, VRecord ivs) -> do
          let (idls, ivls) = (map fst ids, map fst ivs)

          -- Testing the intersection with the bindings ensures every bound name
          -- has a value, while also allowing us to bind a subset of the values.
          if idls `intersect` ivls == idls
            then do
              let envBindings = catMaybes $ joinByKeys (,) idls ids ivs
              modifyE ((++) envBindings) >>
                expression f >>= (\fV -> refreshBindings b bp bv >> removeAllE envBindings fV)
            else throwE $ RunTimeTypeError "Invalid Bind-Pattern"

        _ -> throwE $ RunTimeTypeError "Bind Mis-Match"

      where 
        withBindFrame bindEval = pushBindFrame >> bindEval >>= \r -> popBindFrame >> return r
        removeAllE = flip (foldM (flip removeE))

        joinByKeys joinF keys l r = map (\k -> lookup k l >>= (\matchL -> lookup k r >>= return . joinF matchL)) keys

        -- | Performs a write-back for a bind expression.
        --   This retrieves the current binding values from the environment
        --   and reconstructs a path value to replace the bind target.
        refreshBindings :: Binder -> BindPath -> Value -> Interpretation ()
        refreshBindings (BIndirection i) bindPath bindV = 
          lookupE i >>= replaceBindPath bindPath bindV (\oldV newPathV ->
            case oldV of 
              VIndirection r -> liftIO (writeIORef r newPathV) >> return oldV
              _ -> throwE $ RunTimeTypeError "Invalid bind indirection target")

        refreshBindings (BTuple ts) bindPath bindV =
          mapM lookupE ts
            >>= replaceBindPath bindPath bindV (\_ newPathV -> return newPathV) . VTuple

        refreshBindings (BRecord ids) bindPath bindV =
          mapM lookupE (map snd ids) 
            >>= return . VRecord . zip (map fst ids)
            >>= replaceBindPath bindPath bindV (\oldV newPathV -> mergeRecords oldV newPathV)

        replaceBindPath :: BindPath -> Value -> (Value -> Value -> Interpretation Value) -> Value -> Interpretation ()
        replaceBindPath bindPath bindV refreshF newV =
          case bindPath of 
            (Named n):t     -> lookupE n >>= (\oldV -> reconstructPathValue t newV oldV >>= refreshF oldV >>= replaceE n)
            (Temporary _):t -> reconstructPathValue t newV bindV >>= refreshF bindV >> return ()
            _               -> throwE $ RunTimeInterpretationError "Invalid path in bind writeback"


        reconstructPathValue :: BindPath -> Value -> Value -> Interpretation Value
        reconstructPathValue [] newR@(VRecord _) oldR@(VRecord _) = mergeRecords oldR newR
        reconstructPathValue [] v _ = return v
        
        reconstructPathValue (Indirection:t) v (VIndirection iv) =
          liftIO (readIORef iv)
            >>= reconstructPathValue t v
            >>= liftIO . writeIORef iv >> return (VIndirection iv)

        reconstructPathValue ((TupleField i):t) v (VTuple vs) =
          let (x,y) = splitAt i vs in
          reconstructPathValue t v (last x) >>= \nv -> return $ VTuple ((init x) ++ [nv] ++ y)

        reconstructPathValue ((RecordField n):t) v (VRecord ivs) = do
          fields <- flip mapM ivs (\(fn,fv) -> 
                      if fn == n then (reconstructPathValue t v fv >>= return . (fn,))
                                 else return (fn,fv))
          return $ VRecord fields

        reconstructPathValue _ _ _ =
          throwE $ RunTimeInterpretationError "Invalid path in bind writeback reconstruction"


        mergeRecords :: Value -> Value -> Interpretation Value
        mergeRecords (VRecord r1) (VRecord r2) = 
          return . VRecord $ map (\(n,v) -> maybe (n,v) (n,) $ lookup n r2) r1

        mergeRecords _ _ =
          throwE $ RunTimeTypeError "Invalid bind record target"

    expr (tag -> EBindAs _) = throwE $ RunTimeTypeError "Invalid Bind Construction"

    -- | Interpretation of If-Then-Else constructs.
    expr (tag &&& children -> (EIfThenElse, [p, t, e])) = expression p >>= \case
        VBool True -> expression t
        VBool False -> expression e
        _ -> throwE $ RunTimeTypeError "Invalid Conditional Predicate"

    expr (tag &&& children -> (EAddress, [h, p])) = do
      hv <- expression h
      pv <- expression p
      case (hv, pv) of
        (VString host, VInt port) -> return $ VAddress $ Address (host, port)
        _ -> throwE $ RunTimeTypeError "Invalid address"

    expr (tag -> ESelf) = lookupE annotationSelfId

    expr _ = throwE $ RunTimeInterpretationError "Invalid Expression"

copyValue :: Value -> Interpretation Value
copyValue (VCollection cmv) = do
  old_col <- liftIO $ readMVar cmv
  copyCollection old_col
copyValue val = return val

{- Literal interpretation -}

literal :: K3 Literal -> Interpretation Value
literal (tag -> LBool b)   = return $ VBool b
literal (tag -> LByte b)   = return $ VByte b
literal (tag -> LInt i)    = return $ VInt i
literal (tag -> LReal r)   = return $ VReal r
literal (tag -> LString s) = return $ VString s
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
  where initEmptyCollection comboId = lookupACombo comboId >>= ($ ()) . emptyCtor >>= return . VCollection

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
replaceTrigger n _                  = throwE $ RunTimeTypeError ("Invalid body for trigger " ++ n)

global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> Interpretation ()
global n (tag -> TSink) (Just e)      = expression e >>= replaceTrigger n
global _ (tag -> TSink) Nothing       = throwE $ RunTimeInterpretationError "Invalid sink trigger"
global _ (tag -> TSource) _           = return ()
global _ (tag -> TFunction) _         = return () -- Functions have already been initialized.

-- n is the name of the variable
global n t@(tag -> TCollection) eOpt = elemE n >>= \case
  True  -> void . getComposedAnnotationT $ annotations t
  False -> (getComposedAnnotationT $ annotations t) >>= initializeCollection . maybe "" id
  where
    initializeCollection comboId = case eOpt of
      Nothing | not (null comboId) -> lookupACombo comboId >>= ($ ()) . emptyCtor >>= modifyE . (:) . (n,) . VCollection
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

emptyDataspaceLookup :: [(Identifier, IEnvironment Value)] -> Interpretation (CollectionDataspace Value)
emptyDataspaceLookup namedAnnDefs = do
  case find (\(val, _) -> val == externalAnnotationId) namedAnnDefs of
    Nothing -> emptyDS Nothing >>= return . InMemoryDS
    Just _ -> emptyDS Nothing >>= return . ExternalDS

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

    mkCBinder :: Identifier -> [(Identifier, IEnvironment Value)] -> CollectionBinders Value
    mkCBinder comboId namedAnnDefs =
      CollectionBinders (mkEmptyConstructor comboId namedAnnDefs) (mkInitialConstructor comboId namedAnnDefs) (mkCopyConstructor namedAnnDefs) (mkEmplaceConstructor comboId namedAnnDefs)

    mkEmptyConstructor :: Identifier -> [(Identifier, IEnvironment Value)] -> CEmptyConstructor Value
    mkEmptyConstructor comboId namedAnnDefs = const $ do
      collection <- emptyCollectionBody namedAnnDefs comboId -- extra parameter for the name
      newCMV <- liftIO $ newMVar collection
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      void $ mapM_ (bindAnnotationDef newCMV) namedAnnDefs
      return $ newCMV
    
    mkInitialConstructor :: Identifier -> [(Identifier, IEnvironment Value)] -> CInitialConstructor Value
    mkInitialConstructor comboID namedAnnDefs = const $ do
      collection <- emptyCollectionBody namedAnnDefs comboID -- extra parameter for the name
      -- insert values into collection
      newCMV <- liftIO $ newMVar collection
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      void $ mapM_ (bindAnnotationDef newCMV) namedAnnDefs
      return $ newCMV
    
    mkCopyConstructor :: [(Identifier, IEnvironment Value)] -> CCopyConstructor Value
    mkCopyConstructor namedAnnDefs = \coll -> do
      newDataSpace <- copyDS (dataspace coll)
      let newcol = Collection (namespace coll) (newDataSpace) (extensionId coll)
      newCMV <- liftIO (newMVar newcol)
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      return newCMV
   
    mkEmplaceConstructor :: Identifier -> [(Identifier, IEnvironment Value)] -> CEmplaceConstructor Value
    mkEmplaceConstructor comboID namedAnnDefs = \dataspace -> do
      let newcol = Collection emptyCollectionNamespace dataspace comboID
      newCMV <- liftIO (newMVar newcol)
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      void $ mapM_ (bindAnnotationDef newCMV) namedAnnDefs
      return newCMV


    emptyCollectionBody :: [(Identifier, IEnvironment Value)] -> Identifier -> Interpretation (Collection Value)
    emptyCollectionBody namedAnnDefs n = do
      ds <- emptyDataspaceLookup namedAnnDefs
      return $ Collection emptyCollectionNamespace ds n

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

-- | Creates a contextualized collection member function. That is, the member function
--   will add all collection members to the interpretation environment prior to its
--   evaluation. This way, the member function's body can directly refer to other members
--   by name, rather than using the 'self' keyword.
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
        do   
          sEnv <- get >>= return . getStaticEnv
          let wd = wireDesc sEnv format
          void $ liftEngine (openBuiltin cid builtinId wd)
          return vunit

-- openFile :: ChannelId -> String -> String -> String -> ()
genBuiltin "openFile" t =
  ivfun $ \(VString cid) ->
    ivfun $ \(VString path) ->
      ivfun $ \(VString format) ->
        ivfun $ \(VString mode) ->
          do
            sEnv <- get >>= return . getStaticEnv
            let wd = wireDesc sEnv format
            void $ liftEngine (openFile cid path wd (Just t) mode)
            return vunit

-- openSocket :: ChannelId -> Address -> String -> String -> ()
genBuiltin "openSocket" t =
  ivfun $ \(VString cid) ->
    ivfun $ \(VAddress addr) ->
      ivfun $ \(VString format) ->
        ivfun $ \(VString mode) ->
          do
            sEnv <- get >>= return . getStaticEnv
            let wd = wireDesc sEnv format
            void $ liftEngine (openSocket cid addr wd (Just t) mode)
            return vunit


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

copyCollection :: Collection Value -> Interpretation (Value)
copyCollection newC = do
  binders <- lookupACombo $ extensionId newC
  let copyCstr = copyCtor binders
  c'            <- copyCstr newC
  return $ VCollection c'

{-
 - Collection API : head, map, fold, append/concat, delete
 - these can handle in memory vs external
 - other functions here use this api
 -}
builtinLiftedAttribute :: Identifier -> Identifier -> K3 Type -> UID
                          -> Interpretation (Maybe (Identifier, Value))
builtinLiftedAttribute annId n _ _ 
  | annId == collectionAnnotationId && n == "peek"    = return $ Just (n, peekFn)
  | annId == collectionAnnotationId && n == "insert"  = return $ Just (n, insertFn)
  | annId == collectionAnnotationId && n == "delete"  = return $ Just (n, deleteFn)
  | annId == collectionAnnotationId && n == "update"  = return $ Just (n, updateFn)
  | annId == collectionAnnotationId && n == "combine" = return $ Just (n, combineFn)
  | annId == collectionAnnotationId && n == "split"   = return $ Just (n, splitFn)
  | annId == collectionAnnotationId && n == "iterate" = return $ Just (n, iterateFn)
  | annId == collectionAnnotationId && n == "map"     = return $ Just (n, mapFn)
  | annId == collectionAnnotationId && n == "filter"  = return $ Just (n, filterFn)
  | annId == collectionAnnotationId && n == "fold"    = return $ Just (n, foldFn)
  | annId == collectionAnnotationId && n == "groupBy" = return $ Just (n, groupByFn)
  | annId == collectionAnnotationId && n == "ext"     = return $ Just (n, extFn)

  | annId == externalAnnotationId && n == "peek"    = return $ Just (n, peekFn)
  | annId == externalAnnotationId && n == "insert"  = return $ Just (n, insertFn)
  | annId == externalAnnotationId && n == "delete"  = return $ Just (n, deleteFn)
  | annId == externalAnnotationId && n == "update"  = return $ Just (n, updateFn)
  | annId == externalAnnotationId && n == "combine" = return $ Just (n, combineFn)
  | annId == externalAnnotationId && n == "split"   = return $ Just (n, splitFn)
  | annId == externalAnnotationId && n == "iterate" = return $ Just (n, iterateFn)
  | annId == externalAnnotationId && n == "map"     = return $ Just (n, mapFn)
  | annId == externalAnnotationId && n == "filter"  = return $ Just (n, filterFn)
  | annId == externalAnnotationId && n == "fold"    = return $ Just (n, foldFn)
  | annId == externalAnnotationId && n == "groupBy" = return $ Just (n, groupByFn)
  | annId == externalAnnotationId && n == "ext"     = return $ Just (n, extFn)
  | otherwise = providesError "lifted attribute" n

  where
        copy = copyCollection

        -- | Collection accessor implementation
        peekFn = valWithCollection $ \_ (Collection _ ds _) -> do 
          inner_val <- peekDS ds
          return $ VOption inner_val

        -- | Collection modifier implementation
        insertFn = valWithCollectionMV $ \el cmv -> modifyCollection cmv (insertCollection el)
        deleteFn = valWithCollectionMV $ \el cmv -> modifyCollection cmv (deleteCollection el)
        updateFn = valWithCollectionMV $ \old cmv -> ivfun $ \new -> modifyCollection cmv (updateCollection old new)

        -- BREAKING EXCEPTION SAFETY
        modifyCollection :: MVar (Collection Value) -> (Collection Value -> Interpretation (Collection Value)) -> Interpretation Value
        --TODO modifyMVar_ function has to be over IO
        modifyCollection cmv f = do
            old_col <- liftIO $ readMVar cmv
            result <- f old_col
            --liftIO $ putMVar cmv result
            liftIO $ modifyMVar_ cmv (const $ return result)
            return vunit

        -- TODO move wrapping / unwrapping DataSpace into modifyCollection?
        insertCollection :: Value -> Collection (Value) -> Interpretation (Collection Value)
        insertCollection v  (Collection ns ds extId) = do
            new_ds <- insertDS ds v
            return $ Collection ns new_ds extId

        deleteCollection v    (Collection ns ds extId) = do
            new_ds <- deleteDS v ds
            return $ Collection ns new_ds extId

        updateCollection v v' (Collection ns ds extId) = do
            new_ds <- updateDS v v' ds
            return $ Collection ns new_ds extId

        -- | Collection effector implementation
        iterateFn = valWithCollection $ \f (Collection _ ds _) -> 
          flip (matchFunction iterateFnError) f $
          \f' -> mapDS_ (withClosure f') ds >> return vunit

        -- | Collection transformer implementation
        combineFn = valWithCollection $ \other (Collection ns ds extId) ->
          flip (matchCollection collectionError) other $ 
            \(Collection _ ds' extId') ->
              if extId /= extId' then combineError
              else do 
                new_ds <- combineDS ds ds'
                copy $ Collection ns new_ds extId

        splitFn = valWithCollection $ \_ (Collection ns ds extId) -> do
            (l, r) <- splitDS ds
            lc <- copy (Collection ns l extId)
            rc <- copy (Collection ns r extId)
            return $ VTuple [lc, rc]
        -- Pass in the namespace
        mapFn = valWithCollection $ \f (Collection ns ds ext) ->
          flip (matchFunction mapFnError) f $ 
            \f'  -> mapDS (withClosure f') ds >>= 
            \ds' -> copy (Collection ns ds' ext)

        filterFn = valWithCollection $ \f (Collection ns ds extId) ->
          flip (matchFunction filterFnError) f $
            \f'  -> filterDS (\v -> withClosure f' v >>= matchBool filterValError) ds >>=
            \ds' -> copy (Collection ns ds' extId)

        foldFn = valWithCollection $ \f (Collection _ ds _) ->
          flip (matchFunction foldFnError) f $
            \f' -> ivfun $ \accInit -> foldDS (curryFoldFn f') accInit ds

        curryFoldFn :: (Value -> Interpretation Value, Closure Value) -> Value -> Value -> Interpretation Value
        curryFoldFn f' acc v = do
          result <- withClosure f' acc
          (matchFunction curryFnError) (flip withClosure v) result

        -- TODO: replace assoc lists with a hashmap.
        groupByFn = valWithCollection heres_the_answer
          where
            heres_the_answer :: Value -> Collection Value -> Interpretation Value
            heres_the_answer gb (Collection ns ds ext) = -- Am I passing the right namespace & stuff to the later collections?
              flip (matchFunction partitionFnError) gb $ \gb' -> ivfun $ \f -> 
              flip (matchFunction foldFnError) f $ \f' -> ivfun $ \accInit ->
                do
                  new_space <- emptyDS (Just ds)
                  kvRecords <- foldDS (groupByElement gb' f' accInit) new_space ds
                  -- TODO typecheck that collection
                  copy (Collection ns kvRecords ext)

        groupByElement :: (AssociativeDataspace (Interpretation) ads Value Value) =>
          (Value -> Interpretation Value, Closure Value) -> (Value -> Interpretation Value, Closure Value) -> Value -> ads -> Value -> Interpretation ads
        groupByElement gb' f' accInit acc v = do
          k <- withClosure gb' v
          look_result <- lookupKV acc k
          case look_result of
            Nothing         -> do
              val <- curryFoldFn f' accInit v 
              --tmp_ds <- emptyDS ()
              tmp_ds <- emptyDS (Just acc)
              tmp_ds <- insertKV tmp_ds k val
              combineDS acc tmp_ds
            Just partialAcc -> do 
              new_val <- curryFoldFn f' partialAcc v
              replaceKV acc k new_val

        extFn = valWithCollection $ \f (Collection _ ds _) -> 
          flip (matchFunction extError) f $ 
          \f' -> do
                val_ds <- mapDS (withClosure f') ds
                first_subcol <- peekDS val_ds
                case first_subcol of
                  Nothing -> extError -- Maybe not the right error here
                                      -- really, I should create an empty VCollection
                                      -- with the right type (that of f(elem)), but I
                                      -- don't have a value to copy the type out of
                  Just sub_val -> do
                    val_ds <- deleteDS sub_val val_ds
                    result <- foldDS (\acc val -> combine' acc (Just val)) (Just sub_val) val_ds
                    maybe combineError return result

        combine' :: Maybe Value -> Maybe Value -> Interpretation (Maybe Value)
        combine' Nothing _ = return Nothing
        combine' _ Nothing = return Nothing

        -- TODO: make more efficient by avoiding intermediate MVar construction.
        combine' (Just acc) (Just cv) =
          flip (matchCollection $ return Nothing) acc $ \(Collection ns1 ds1 extId1) -> 
          flip (matchCollection $ return Nothing) cv  $ \(Collection _ ds2 extId2) -> 
          if extId1 /= extId2 then return Nothing
          else do
            new_ds <- combineDS ds1 ds2
            copy (Collection ns1 new_ds extId1) >>= return . Just


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

-- | Constructs a static environment for all globals and annotation
--   combinations by interpreting the program declarations.
--   By ensuring that global and annotation member functions use
--   static initializers (i.e. initializers that do not depend on runtime values),
--   we can simply populate the static environment from the interpreter
--   environment resulting immediately after declaration initialization.
staticEnvironment :: K3 Declaration -> EngineM Value (SEnvironment Value)
staticEnvironment prog = do
  initSt  <- initState prog
  staticR <- runInterpretation' initSt (declaration prog)
  logState "STATIC " Nothing $ getResultState staticR
  let st = getResultState staticR
  annotEnv <- staticAnnotations st
  return (staticFunctions $ getEnv st, annotEnv)

  where
    staticFunctions = filter (functionValue . snd)
    functionValue (VFunction _) = True
    functionValue _             = False

    staticAnnotations st = do
      let annEnvI = foldM addRealization (getAnnotEnv st) $ nub $ declCombos prog
      resultAEnv <- runInterpretation' st annEnvI >>= liftError "(constructing static environment)"
      case getResultVal resultAEnv of
        Left err                 -> throwEngineError . EngineError $ show err
        Right (AEnvironment d r) -> return $ AEnvironment d $ nubBy ((==) `on` fst) r
    
    addRealization aEnv@(AEnvironment d r) annNames = do
      comboIdOpt <- getComposedAnnotation $ (Just $ annotationComboId annNames, annNames)
      case comboIdOpt of
        Nothing  -> return aEnv
        Just cId -> lookupACombo cId >>= return . AEnvironment d . (:r) . (cId,)

    declCombos :: K3 Declaration -> [[Identifier]]
    declCombos = foldTree extractDeclCombos []

    typeCombos :: K3 Type -> [[Identifier]]
    typeCombos = foldTree extractTypeCombos []

    exprCombos :: K3 Expression -> [[Identifier]]
    exprCombos = foldTree extractExprCombos []    
    
    extractDeclCombos :: [[Identifier]] -> K3 Declaration -> [[Identifier]]
    extractDeclCombos st (tag -> DGlobal _ t eOpt)     = st ++ typeCombos t ++ (maybe [] exprCombos eOpt)
    extractDeclCombos st (tag -> DTrigger _ t e)       = st ++ typeCombos t ++ exprCombos e
    extractDeclCombos st (tag -> DAnnotation _ _ mems) = st ++ concatMap memCombos mems
    extractDeclCombos st _ = st
    
    extractTypeCombos :: [[Identifier]] -> K3 Type -> [[Identifier]]
    extractTypeCombos c (tag &&& annotations -> (TCollection, tAnns)) = 
      case namedTAnnotations tAnns of
        []        -> c
        namedAnns -> namedAnns:c
    
    extractTypeCombos c _ = c

    extractExprCombos :: [[Identifier]] -> K3 Expression -> [[Identifier]]
    extractExprCombos c (tag &&& annotations -> (EConstant (CEmpty et), eAnns)) = 
      case namedEAnnotations eAnns of
        []        -> c ++ typeCombos et
        namedAnns -> (namedAnns:c) ++ typeCombos et
    extractExprCombos c _ = c

    memCombos :: AnnMemDecl -> [[Identifier]]
    memCombos (Lifted _ _ t eOpt _)    = typeCombos t ++ (maybe [] exprCombos eOpt)
    memCombos (Attribute _ _ t eOpt _) = typeCombos t ++ (maybe [] exprCombos eOpt)
    memCombos _ = []


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
    registerGlobal n istate = modifyStateGlobals ((:) n ) istate

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

initBootstrap :: PeerBootstrap -> AEnvironment Value -> EngineM Value (IEnvironment Value)
initBootstrap bootstrap aEnv = flip mapM bootstrap (\(n,l) ->
    runInterpretation' (annotationState aEnv) (literal l) 
      >>= liftError "(initializing bootstrap)"
      >>= either invalidErr (return . (n,)) . getResultVal)
  where invalidErr = const $ throwEngineError $ EngineError "Invalid result"

injectBootstrap :: PeerBootstrap -> IResult a -> EngineM Value (IResult a)
injectBootstrap bootstrap r = case r of
  ((Left _, _), _) -> return r
  ((Right val, istate), rLog) -> do
      bootEnv <- initBootstrap bootstrap (getAnnotEnv istate)
      let vEnv = getEnv istate
      let nvEnv = map (\(n,v) -> maybe (n,v) (n,) $ lookup n bootEnv) vEnv
      return ((Right val, modifyStateEnv (const nvEnv) istate), rLog)


initProgram :: PeerBootstrap -> K3 Declaration -> EngineM Value (IResult Value)
initProgram bootstrap prog = do
    initSt   <- initState prog
    staticSt <- buildStaticEnv initSt
    declR    <- runInterpretation' staticSt (declaration prog)
    bootR    <- injectBootstrap bootstrap declR
    initMessages bootR
  where 
    buildStaticEnv istate =
      staticEnvironment prog >>= return . (\v -> modifyStateSEnv (const v) istate)

finalProgram :: IState -> EngineM Value (IResult Value)
finalProgram st = runInterpretation' st $ maybe unknownTrigger runFinal $ lookup "atExit" $ getEnv st
  where runFinal (VFunction (f,[])) = f vunit
        runFinal _                  = throwE $ RunTimeTypeError "Invalid atExit trigger"
        unknownTrigger              = throwE $ RunTimeTypeError "Could not find atExit trigger"


{- Standalone (i.e., single peer) evaluation -}

standaloneInterpreter :: (IEngine -> IO a) -> IO a
standaloneInterpreter f = simpleEngine >>= f

runExpression :: K3 Expression -> IO (Maybe Value)
runExpression e = standaloneInterpreter (withEngine')
  where
    withEngine' engine = flip runEngineM engine (valueOfInterpretation emptyState (expression e))
        >>= return . either (const Nothing) id

runExpression_ :: K3 Expression -> IO ()
runExpression_ e = runExpression e >>= putStrLn . show


{- Distributed program execution -}

-- | Single-machine system simulation.
runProgram :: Bool -> SystemEnvironment -> K3 Declaration -> IO (Either EngineError ())
runProgram isPar systemEnv prog = buildStaticEnv >>= \case
    Left err   -> return $ Left err
    Right sEnv -> do
      trigs  <- return $ getTriggerIds tProg
      engine <- simulationEngine trigs isPar systemEnv $ syntaxValueWD sEnv
      flip runEngineM engine $ runEngine virtualizedProcessor tProg

  where buildStaticEnv = do
          trigs <- return $ getTriggerIds tProg
          preEngine <- simulationEngine trigs isPar systemEnv $ syntaxValueWD emptyStaticEnv
          flip runEngineM preEngine $ staticEnvironment tProg

        tProg = labelBindAliases prog

-- | Single-machine network deployment.
--   Takes a system deployment and forks a network engine for each peer.
runNetwork :: Bool -> SystemEnvironment -> K3 Declaration
           -> IO [Either EngineError (Address, Engine Value, ThreadId)]
runNetwork isPar systemEnv prog =
  let nodeBootstraps = map (:[]) systemEnv in 
    buildStaticEnv (getTriggerIds tProg) >>= \case
      Left err   -> return $ [Left err]
      Right sEnv -> do
        trigs         <- return $ getTriggerIds tProg
        engines       <- mapM (flip (networkEngine trigs isPar) $ syntaxValueWD sEnv) nodeBootstraps
        namedEngines  <- return . map pairWithAddress $ zip engines nodeBootstraps
        engineThreads <- mapM fork namedEngines
        return engineThreads

  where
    buildStaticEnv trigs = do
      preEngine <- simulationEngine trigs isPar systemEnv $ syntaxValueWD emptyStaticEnv
      flip runEngineM preEngine $ staticEnvironment tProg

    pairWithAddress (engine, bootstrap) = (fst . head $ bootstrap, engine)
    fork (addr, engine) = do
      threadId <- flip runEngineM engine $ forkEngine virtualizedProcessor tProg
      return $ either Left (Right . (addr, engine,)) threadId

    tProg = labelBindAliases prog


{- Message processing -}

runTrigger :: IResult Value -> Identifier -> Value -> Value -> EngineM Value (IResult Value)
runTrigger r n a = \case
    (VTrigger (_, Just f)) -> do
        result <- runInterpretation' (getResultState r) (f a)
        logTrigger defaultAddress n a result
        return result
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
        >>= \result -> do logTrigger defaultAddress n args result; return result

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
        logResult "INIT " (Just node) iProgram
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

syntaxValueWD :: SEnvironment Value -> WireDesc Value
syntaxValueWD sEnv =
  WireDesc (packValueSyntax True)
           (\s -> unpackValueSyntax sEnv s >>= return . Just) $ Delimiter "\n"

wireDesc :: SEnvironment Value -> String -> WireDesc Value
wireDesc sEnv "k3" = syntaxValueWD sEnv
wireDesc _ fmt  = error $ "Invalid format " ++ fmt


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

showState :: String -> IState -> EngineM Value [String]
showState str st = return $ [str ++ " { "] ++ (indent 2 $ prettyLines st) ++ ["}"]

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

logState :: String -> Maybe Address -> IState -> EngineM Value ()
logState tag' addr st = do
    msg <- showState (tag' ++ (maybe "" show $ addr)) st
    void $ _notice_Dispatch $ boxToString msg

logResult :: String -> Maybe Address -> IResult Value -> EngineM Value ()
logResult tag' addr r = do
    msg <- showResult (tag' ++ (maybe "" show $ addr)) r 
    void $ _notice_Dispatch $ boxToString msg

logTrigger :: Address -> Identifier -> Value -> IResult Value -> EngineM Value ()
logTrigger addr n args r = do
    msg <- showDispatch addr n args r
    void $ _notice_Dispatch $ boxToString msg

logIState :: Interpretation ()
logIState = get >>= liftIO . putStrLn . pretty

logBindPath :: Interpretation ()
logBindPath = getBindPath >>= void . _notice_BindPath . ("BIND PATH: "++) . show

{- Instances -}
instance Pretty IState where
  prettyLines istate =
         ["Environment:"] ++ (indent 2 $ map prettyEnvEntry $ sortBy (on compare fst) (getEnv istate))
      ++ ["Annotations:"] ++ (indent 2 $ lines $ show $ getAnnotEnv istate)
      ++ ["Static:"]      ++ (indent 2 $ lines $ show $ getStaticEnv istate)
      ++ ["Aliases:"]     ++ (indent 2 $ lines $ show $ getBindStack istate)
    where
      prettyEnvEntry (n,v) = n ++ replicate (maxNameLength - length n) ' ' ++ " => " ++ show v
      maxNameLength        = maximum $ map (length . fst) (getEnv istate)

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
        <$> rt (showString "CNS=") <*> packNamedValues d (namespaceNonFunctions cns) 
        <*> rt (showChar ',')
        <*> rt (showString "ANS=") 
        <*> braces (packDoublyNamedValues d) (map (\(x,y) -> (x, namespaceNonFunctions y)) ans)
    
    -- TODO Need to pattern match on kind of dataspace
    packCollectionDataspace d (InMemoryDS v) =
      (\a b -> a . b) <$> (rt $ showString "InMemoryDS=") <*> brackets (packValue d) v
    packCollectionDataspace d (ExternalDS filename) =
      (\a b -> a . b) <$> (rt $ showString "ExternalDS=") <*> (rt $ showString $ getFile filename)
    -- for now, external ds are shared file path
    -- Need to preserve copy semantics for external dataspaces
    
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

    namespaceNonFunctions = filter (nonFunction . snd)
    nonFunction (VFunction _) = False
    nonFunction _             = True

    True  ? x = const x  
    False ? _ = id


unpackValueSyntax :: SEnvironment Value -> String -> IO Value
unpackValueSyntax sEnv = readSingleParse unpackValue
  where
    rt :: a -> IO a
    rt = return

    unpackValue :: ReadPrec (IO Value)
    unpackValue = parens $ reset $
           (readPrec  >>= return . rt . VInt)
      <++ ((readPrec >>= return . rt . VBool)
      +++  (readPrec  >>= return . rt . VReal)
      +++  (readPrec  >>= return . rt . VString)
      +++  (readPrec  >>= return . rt . VAddress)

      +++ (do
            Ident "B" <- lexP
            v <- readPrec
            return . rt $ VByte v)

      +++ (do
            Ident "Just" <- lexP
            v <- unpackValue
            return (v >>= rt . VOption . Just))

      +++ (do
            Ident "Nothing" <- lexP
            return . rt $ VOption Nothing)

      +++ (prec appPrec1 $ do
            v <- readParens unpackValue
            return (sequence v >>= rt . VTuple))

      +++ (do
            nv <- readNamedValues unpackValue
            return (nv >>= rt . VRecord))

      +++ (do
            Ident "I" <- lexP
            v <- unpackValue
            return (v >>= newIORef >>= rt . VIndirection))
      
      +++ readCollectionPrec)

    readCollectionPrec = parens $
      (prec appPrec1 $ do
        Punc "{" <- lexP
        ns       <- readCollectionNamespace
        Punc "," <- lexP
        v        <- readCollectionDataspace
        Punc "," <- lexP
        Ident n  <- lexP
        Punc "}" <- lexP
        return $ (do
          x   <- ns
          y   <- v
          cmv <- rebuildCollection n $ Collection x y n
          rt $ VCollection cmv))

    readCollectionNamespace :: ReadPrec (IO (CollectionNamespace Value))
    readCollectionNamespace = parens $
      (prec appPrec1 $ do
        void      $ readExpectedName "CNS"
        cns      <- readNamedValues unpackValue
        Punc "," <- lexP
        void      $ readExpectedName "ANS"
        ans      <- readDoublyNamedValues
        return $ (do
          cns' <- cns
          ans' <- ans
          rt $ CollectionNamespace cns' ans'))

    readCollectionDataspace :: ReadPrec (IO (CollectionDataspace Value))
    readCollectionDataspace =
      (parens $ do
        void $ readExpectedName "InMemoryDS"
        v <- readBrackets unpackValue
        return $ InMemoryDS <$> sequence v
      )
      +++
      (parens $ do
        void $ readExpectedName "ExternalDS"
        String filename <- lexP
        return $ rt $ ExternalDS $ FileDataspace filename
      )
      

    readDoublyNamedValues :: ReadPrec (IO [(String, [(String, Value)])])
    readDoublyNamedValues = parens $ do
        v <- readBraces $ readNamedPrec $ readNamedValues unpackValue
        return $ sequence v

    readNamedValues :: ReadPrec (IO a) -> ReadPrec (IO [(String, a)])
    readNamedValues readF = parens $ do
        v <- readBraces $ readNamedPrec readF
        return $ sequence v

    readNamedPrec :: ReadPrec (IO a) -> ReadPrec (IO (String, a))
    readNamedPrec readF = parens $ do
        n <- readName 
        v <- step readF
        return $ v >>= rt . (n,)

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
        []  -> error ("Interpreter.unpackValueSyntax: no parse for " ++ s)
        l   -> error $ "Interpreter.unpackValueSyntax: ambiguous parse (" ++ (show $ length l) ++ " variants)"
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

    rebuildCollection comboId c = 
      case lookup comboId $ realizations $ snd sEnv of
        Nothing -> newMVar c
        Just rec -> do
          let copyCstr = copyCtor rec
          cCopyResult <- simpleEngine >>= \e -> runInterpretation e emptyState (copyCstr c)
          either (const $ newMVar c) (either (const $ newMVar c) return . getResultVal) cCopyResult
    
    appPrec1 = 11  


instance (Show v) => Show (AEnvironment v) where
  show (AEnvironment defs _) = show defs
