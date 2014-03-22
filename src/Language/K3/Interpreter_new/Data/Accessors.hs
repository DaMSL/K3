{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter_new.Data.Accessors where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.List
import Data.Maybe
import qualified Data.HashTable.IO as HT 
import qualified Data.Map          as Map

import System.Mem.StableName

import Language.K3.Interpreter_new.Data.Types
import Language.K3.Interpreter_new.Data.Instances ()

import Language.K3.Core.Common
import Language.K3.Runtime.Engine

{- Constants and simple value constructors. -}
vunit :: Value
vunit = VTuple []

vfun :: IFunction -> Interpretation Value
vfun f = (\env tg -> VFunction (f, env, tg)) <$> emptyEnv <*> memEntTag f

{- Entity tag accessors -}
memEntTag :: a -> Interpretation EntityTag
memEntTag a = liftIO (makeStableName a >>= return . MemEntTag)


{- Named bindings and members operations -}
emptyBindings :: NamedBindings a
emptyBindings = Map.empty

lookupBinding :: Identifier -> NamedBindings a -> Maybe a
lookupBinding = Map.lookup

insertBinding :: Identifier -> a -> NamedBindings a -> NamedBindings a
insertBinding = Map.insert

foldBindings :: (Monad m) => (a -> Identifier -> b -> m a) -> a -> NamedBindings b -> m a
foldBindings f z bindings = Map.foldlWithKey chainAcc (return z) bindings
  where chainAcc macc n v = macc >>= \acc -> f acc n v

mapBindings :: (Monad m) => (Identifier -> a -> m b) -> NamedBindings a -> m (NamedBindings b)
mapBindings f bindings = foldBindings appendAcc emptyBindings bindings
  where appendAcc acc k v = f k v >>= \nv -> return $ insertBinding k nv acc 

mapBindings_ :: (Monad m) => (Identifier -> a -> m b) -> NamedBindings a -> m ()
mapBindings_ f bindings = foldBindings chainF () bindings
  where chainF _ k v = f k v >>= const (return ())

emptyMembers :: NamedMembers Value
emptyMembers = Map.empty

lookupMember :: Identifier -> NamedMembers Value -> Maybe (Value, MemberQualifier)
lookupMember = lookupBinding

insertMember :: Identifier -> (Value, MemberQualifier) -> NamedMembers Value -> NamedMembers Value
insertMember = insertBinding

foldMembers :: (Monad m) => (a -> Identifier -> (Value, MemberQualifier) -> m a) -> a -> NamedMembers Value -> m a
foldMembers f z mems = foldBindings f z mems

mapMembers :: (Monad m) => (Identifier -> (Value, MemberQualifier) -> m a) -> NamedMembers Value -> m (NamedBindings a)
mapMembers f mems = mapBindings f mems

mapMembers_ :: (Monad m) => (Identifier -> (Value, MemberQualifier) -> m a) -> NamedMembers Value -> m ()
mapMembers_ f mems = mapBindings_ f mems

bindMembers :: NamedMembers Value -> Interpretation (NamedBindings (IEnvEntry Value))
bindMembers mems = mapMembers pushMember mems
  where pushMember k vq = do
          entry <- envEntry vq
          void $ modifyE $ insertEnv k entry
          return entry
        envEntry (v, MemImmut) = return $ IVal v
        envEntry (v, MemMut)   = liftIO (newMVar v) >>= return . MVal

unbindMembers :: NamedBindings (IEnvEntry Value) -> Interpretation (NamedMembers Value)
unbindMembers bindings = mapBindings popMember bindings
  where popMember k entry = do
          void $ modifyE $ removeEnv k entry
          case entry of 
            IVal v  -> return (v, MemImmut)
            MVal mv -> liftIO (readMVar mv) >>= return . (, MemMut)


{- Environment accessors -}
emptyEnvIO :: IO (IEnvironment Value)
emptyEnvIO = HT.new

emptyEnv :: Interpretation (IEnvironment Value)
emptyEnv = liftIO emptyEnvIO

chainEnv :: (IEnvironment Value -> Interpretation ()) -> IEnvironment Value
         -> Interpretation (IEnvironment Value)
chainEnv f env = f env >>= const (return env)

insertEnv :: Identifier -> IEnvEntry Value -> IEnvironment Value
          -> Interpretation (IEnvironment Value)
insertEnv n v = chainEnv (\env -> liftIO (HT.lookup env n) >>= liftIO . HT.insert env n . (maybe [v] (v:)))

removeEnv :: Identifier -> IEnvEntry Value -> IEnvironment Value
          -> Interpretation (IEnvironment Value)
removeEnv n v = chainEnv (\env -> liftIO (HT.lookup env n) >>= liftIO . maybe (return ()) (remove env))
  where remove env (delete v -> nl) = if null nl then HT.delete env n else HT.insert env n nl

replaceEnv :: Identifier -> IEnvEntry Value -> IEnvironment Value
           -> Interpretation (IEnvironment Value)
replaceEnv n v = chainEnv (\env -> liftIO (HT.lookup env n) >>= liftIO . maybe (return ()) (replace env))
  where replace env [] = HT.insert env n [v]
        replace env l  = HT.insert env n (v:tail l)


{- State and result accessors -}

emptyProxyStack :: ProxyPathStack
emptyProxyStack = []

emptyStaticEnv :: Interpretation (SEnvironment Value)
emptyStaticEnv = emptyEnv >>= return . (, AEnvironment [] [])

emptyStaticEnvIO :: IO (SEnvironment Value)
emptyStaticEnvIO = emptyEnvIO >>= return . (, AEnvironment [] [])

emptyState :: Interpretation IState
emptyState = (\env sEnv -> IState [] env (AEnvironment [] []) sEnv emptyProxyStack)
                <$> emptyEnv <*> emptyStaticEnv 

emptyStateIO :: IO IState
emptyStateIO = (\env sEnv -> IState [] env (AEnvironment [] []) sEnv emptyProxyStack)
                  <$> emptyEnvIO <*> emptyStaticEnvIO

annotationState :: AEnvironment Value -> Interpretation IState
annotationState aEnv = (\env sEnv -> IState [] env aEnv sEnv emptyProxyStack)
                          <$> emptyEnv <*> emptyStaticEnv

modifyStateGlobals :: (Globals -> Globals) -> IState -> IState
modifyStateGlobals f (IState g e a s b) = IState (f g) e a s b

modifyStateAEnv :: (AEnvironment Value -> AEnvironment Value) -> IState -> IState
modifyStateAEnv f (IState g e a s b) = IState g e (f a) s b

modifyStateProxies :: (ProxyPathStack -> ProxyPathStack) -> IState -> IState
modifyStateProxies f (IState g e a s b) = (IState g e a s (f b))

modifyStateEnv :: (IEnvironment Value -> Interpretation (IEnvironment Value)) -> IState
               -> Interpretation IState
modifyStateEnv f (IState g e a s b) = f e >>= \ne -> return $ IState g ne a s b

modifyStateSEnv :: (SEnvironment Value -> Interpretation (SEnvironment Value)) -> IState
                -> Interpretation IState
modifyStateSEnv f (IState g e a s b) = f s >>= \ns -> return $ IState g e a ns b

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
--   to date, and the current state.
throwSE :: Maybe (Span, UID) -> (Maybe (Span, UID) -> InterpretationError) -> Interpretation a
throwSE su f = Control.Monad.Trans.Either.left $ f su

-- | Raise an exception in the interpretation monad.
throwE :: (Maybe (Span, UID) -> InterpretationError) -> Interpretation a
throwE f = throwSE Nothing f

-- | Raise an error with a UID and span
throwAE :: (HasSpan b, HasUID b) => [b] -> (Maybe (Span, UID) -> InterpretationError) -> Interpretation a
throwAE annos f = throwSE (spanUid annos) f

-- | Get the UID and span out of annotations
spanUid :: (HasUID a, HasSpan a) => [a] -> Maybe (Span, UID)
spanUid anns = convert span_ uid
  where uid     = fromMaybe Nothing $ find isJust $ map getUID anns
        span_   = fromMaybe Nothing $ find isJust $ map getSpan anns
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
elemE n = get >>= \s -> liftIO (HT.lookup (getEnv s) n) >>= return . maybe False null

-- | Environment lookup, with a thrown error if unsuccessful.
lookupE :: Maybe (Span, UID) -> Identifier -> Interpretation (IEnvEntry Value)
lookupE su n = get >>= \s -> liftIO (HT.lookup (getEnv s) n)
                   >>= maybe err (\l -> if l == [] then err else return $ head l)
  where err = throwSE su $ RunTimeTypeError $ "Unknown Variable: '" ++ n ++ "'"

-- | Environment modification
modifyE :: (IEnvironment Value -> Interpretation (IEnvironment Value)) -> Interpretation ()
modifyE f = get >>= modifyStateEnv f >>= put

-- | Environment binding insertion
insertE :: Identifier -> IEnvEntry Value -> Interpretation ()
insertE n v = modifyE $ insertEnv n v

-- | Environment binding removal
removeE :: Identifier -> IEnvEntry Value -> a -> Interpretation a
removeE n v r = modifyE (removeEnv n v) >> return r

-- | Environment binding replacement.
replaceE :: Identifier -> IEnvEntry Value -> Interpretation ()
replaceE n v = modifyE $ replaceEnv n v

-- | Annotation environment helpers.
lookupADef :: Identifier -> Interpretation (NamedMembers Value)
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

-- | Proxy stack helpers
getProxyPath :: Interpretation (Maybe ProxyPath)
getProxyPath = get >>= return . (\x -> if x == [] then Nothing else last x) . getProxyStack

modifyProxyStack :: (ProxyPathStack -> ProxyPathStack) -> Interpretation ()
modifyProxyStack f = modify $ modifyStateProxies f

modifyProxyStack_ :: (ProxyPathStack -> Interpretation ProxyPathStack) -> Interpretation ()
modifyProxyStack_ f = do
  st <- get
  newPStack <- f (getProxyStack st)
  put $ modifyStateProxies (\_ -> newPStack) st

pushProxyFrame :: Interpretation ()
pushProxyFrame = modifyProxyStack (\ps -> ps++[Nothing])

popProxyFrame :: Interpretation ()
popProxyFrame = modifyProxyStack (\ps -> init ps)

appendAlias :: ProxyStep -> Interpretation ()
appendAlias n = modifyProxyStack_ pushProxyAlias
  where pushProxyAlias [] = return []
        pushProxyAlias ps = case last ps of
          Nothing -> return $ init ps ++ [Just [n]]
          Just _  -> throwE $ RunTimeInterpretationError "Duplicate bind alias"

appendAliasExtension :: Identifier -> Interpretation ()
appendAliasExtension n = modifyProxyStack_ pushProxyAliasExtension
  where pushProxyAliasExtension [] = return []
        pushProxyAliasExtension ps = case last ps of
          Just proxypath -> return $ init ps ++ [Just $ proxypath++[RecordField n]]
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
