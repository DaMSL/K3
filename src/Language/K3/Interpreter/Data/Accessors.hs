{-# LANGUAGE TupleSections #-}

module Language.K3.Interpreter.Data.Accessors where

import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer
import Control.Applicative

import Data.Function
import Data.List
import Data.Maybe

import System.Mem.StableName

import Language.K3.Interpreter.Data.Types

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Runtime.Engine

{- Constants and simple value constructors. -}
vunit :: Value
vunit = VTuple []

vfun :: IFunction -> Interpretation Value
vfun f = liftIO (makeStableName f) >>= return . VFunction . (f,[],)


{- State and result accessors -}

emptyStaticEnv :: SEnvironment Value
emptyStaticEnv = ([], AEnvironment [] [])

emptyBindStack :: BindPathStack
emptyBindStack = []

emptyState :: IState
emptyState = IState [] [] (AEnvironment [] []) emptyStaticEnv emptyBindStack

annotationState :: AEnvironment Value -> IState
annotationState aEnv = IState [] [] aEnv emptyStaticEnv emptyBindStack

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
throwSE :: Maybe (Span, UID) -> (Maybe (Span, UID) -> InterpretationError) -> Interpretation a
throwSE su f = Control.Monad.Trans.Either.left $ f su

throwE :: (Maybe (Span, UID) -> InterpretationError) -> Interpretation a
throwE f = throwSE Nothing f

-- | Raise an error with a UID and span
throwAE :: (HasSpan b, HasUID b) => [b] -> (Maybe (Span, UID) -> InterpretationError) -> Interpretation a
throwAE annos f = throwSE (spanUid annos) f

-- | Get the UID and span out of annotations
spanUid :: (HasUID a, HasSpan a) => [a] -> Maybe (Span, UID)
spanUid anns = convert span uid
  where uid     = fromMaybe Nothing $ find isJust $ map getUID anns
        span    = fromMaybe Nothing $ find isJust $ map getSpan anns
        convert (Just x) (Just y) = Just (x,y)
        convert _        _        = Nothing


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
lookupE :: Maybe (Span, UID) -> Identifier -> Interpretation Value
lookupE su n = get >>= maybe err return . lookup n . getEnv
  where err = throwSE su $ RunTimeTypeError $ "Unknown Variable: '" ++ n ++ "'"

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

lookupACombo :: Identifier -> Interpretation (CollectionConstructors Value)
lookupACombo n = tryLookupACombo n >>= maybe err return
  where err = throwE $ RunTimeTypeError $ "Unknown annotation combination: '" ++ n ++ "'"

tryLookupACombo :: Identifier -> Interpretation (Maybe (CollectionConstructors Value))
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

{- Dataspace accessors -}
listOfMemDS :: PrimitiveMDS v -> Maybe [v]
listOfMemDS (MemDS    (ListMDS v))         = Just v
listOfMemDS (SeqDS    (ListMDS v))         = Just v
listOfMemDS (SetDS    (SetAsOrdListMDS v)) = Just v
listOfMemDS (SortedDS (BagAsOrdListMDS v)) = Just v

typeTagOfMemDS :: PrimitiveMDS v -> String
typeTagOfMemDS (MemDS    _) = "MemDS"
typeTagOfMemDS (SeqDS    _) = "SeqDS"
typeTagOfMemDS (SetDS    _) = "SetDS"
typeTagOfMemDS (SortedDS _) = "SortedDS"
