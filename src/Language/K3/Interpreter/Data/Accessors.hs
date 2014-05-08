{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter.Data.Accessors where

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

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Instances ()

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Runtime.Engine

import Language.K3.Utils.Pretty

{- Constants and simple value constructors. -}
vunit :: Value
vunit = VTuple []

vfun :: IFunction -> Interpretation Value
vfun f = (\env tg -> VFunction (f, env, tg)) <$> emptyEnv <*> memEntTag f

-- | Refresh the collection's cached value from its shared (self) value.
--   This treats the shared value as the authoritative contents.
syncCollection :: Value -> Interpretation Value
syncCollection (VCollection (s,_)) =
  liftIO (readMVar s) >>= \case
      VCollection (s2,c2) | s == s2 -> return $ VCollection (s, c2) 
      _ -> throwE $ RunTimeInterpretationError "Invalid cyclic self value"
syncCollection v = return v

syncCollectionIO :: Value -> IO Value
syncCollectionIO v@(VCollection (s,_)) = readMVar s >>= \case
    VCollection (s2,c2) | s == s2 -> return (VCollection (s,c2))
    _ -> return v 
syncCollectionIO v = return v

-- | Refreshes a collection's cached value while also modifying a binding.
syncCollectionE :: Identifier -> IEnvEntry Value -> Value -> Interpretation Value
syncCollectionE i (IVal _)  v@(VCollection _) = syncCollection v >>= \nv -> replaceE i (IVal nv) >> return nv
syncCollectionE _ (MVal mv) v@(VCollection _) = syncCollection v >>= \nv -> liftIO (modifyMVar_ mv $ const $ return nv) >> return nv
syncCollectionE _ _ v = return v

valueQOfEntry :: IEnvEntry Value -> Interpretation (Value, VQualifier)
valueQOfEntry = liftIO . valueQOfEntryIO

valueQOfEntryIO :: IEnvEntry Value -> IO (Value, VQualifier)
valueQOfEntryIO (IVal v)  = return (v, MemImmut)
valueQOfEntryIO (MVal mv) = readMVar mv >>= return . (, MemMut)

valueOfEntry :: IEnvEntry Value -> Interpretation Value
valueOfEntry = liftIO . valueOfEntryIO 

valueOfEntryIO :: IEnvEntry Value -> IO Value
valueOfEntryIO entry = valueQOfEntryIO entry >>= return . fst

valueOfEntryOpt :: Maybe (IEnvEntry Value) -> Interpretation (Maybe Value)
valueOfEntryOpt = liftIO . valueOfEntryOptIO

valueOfEntryOptIO :: Maybe (IEnvEntry Value) -> IO (Maybe Value)
valueOfEntryOptIO (Just e) = valueOfEntryIO e >>= return . Just
valueOfEntryOptIO Nothing  = return Nothing

mkMVal :: Value -> IO (IEnvEntry Value)
mkMVal v = newMVar v >>= return . MVal

mkIVal :: Value -> IO (IEnvEntry Value)
mkIVal v = return $ IVal v

entryOfValueE :: Maybe (Annotation Expression) -> Value -> Interpretation (IEnvEntry Value)
entryOfValueE (Just EMutable)   = liftIO . mkMVal
entryOfValueE (Just EImmutable) = liftIO . mkIVal
entryOfValueE Nothing           = liftIO . mkIVal
entryOfValueE _                 = const $ throwE $ RunTimeTypeError "Invalid qualifier annotation"

entryOfValueT :: Maybe (Annotation Type) -> Value -> Interpretation (IEnvEntry Value)
entryOfValueT (Just TMutable)   = liftIO . mkMVal
entryOfValueT (Just TImmutable) = liftIO . mkIVal
entryOfValueT Nothing           = liftIO . mkIVal
entryOfValueT _                 = const $ throwE $ RunTimeTypeError "Invalid qualifier annotation"

entryOfValueQ :: Value -> VQualifier -> Interpretation (IEnvEntry Value)
entryOfValueQ v q = liftIO $ entryOfValueQIO v q

entryOfValueQIO :: Value -> VQualifier -> IO (IEnvEntry Value)
entryOfValueQIO v MemImmut = mkIVal v
entryOfValueQIO v MemMut   = mkMVal v

{- Entity tag accessors -}
memEntTag :: a -> Interpretation EntityTag
memEntTag a = liftIO (makeStableName a >>= return . MemEntTag)


{- Named bindings and members operations -}
boundNames :: NamedBindings a -> [Identifier] 
boundNames = Map.keys

emptyBindings :: NamedBindings a
emptyBindings = Map.empty

bindingsFromList :: [(Identifier, a)] -> NamedBindings a
bindingsFromList = Map.fromList

bindingsToList :: NamedBindings a -> [(Identifier, a)]
bindingsToList = Map.toList

lookupBinding :: Identifier -> NamedBindings a -> Maybe a
lookupBinding = Map.lookup

insertBinding :: Identifier -> a -> NamedBindings a -> NamedBindings a
insertBinding = Map.insert

mergeBindings :: NamedMembers Value -> NamedMembers Value -> NamedMembers Value
mergeBindings = Map.union

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

membersFromList :: [(Identifier, (Value, VQualifier))] -> NamedMembers Value
membersFromList = bindingsFromList

membersToList :: NamedMembers Value -> [(Identifier, (Value, VQualifier))]
membersToList = bindingsToList

lookupMember :: Identifier -> NamedMembers Value -> Maybe (Value, VQualifier)
lookupMember = lookupBinding

insertMember :: Identifier -> (Value, VQualifier) -> NamedMembers Value -> NamedMembers Value
insertMember = insertBinding

mergeMembers :: NamedMembers Value -> NamedMembers Value -> NamedMembers Value
mergeMembers = mergeBindings

foldMembers :: (Monad m) => (a -> Identifier -> (Value, VQualifier) -> m a) -> a -> NamedMembers Value -> m a
foldMembers f z mems = foldBindings f z mems

mapMembers :: (Monad m) => (Identifier -> (Value, VQualifier) -> m a) -> NamedMembers Value -> m (NamedBindings a)
mapMembers f mems = mapBindings f mems

mapMembers_ :: (Monad m) => (Identifier -> (Value, VQualifier) -> m a) -> NamedMembers Value -> m ()
mapMembers_ f mems = mapBindings_ f mems

bindMembers :: NamedMembers Value -> Interpretation (NamedBindings (IEnvEntry Value))
bindMembers mems = mapMembers pushMember mems
  where pushMember k vq = do
          entry <- (uncurry entryOfValueQ) vq
          void $ modifyE $ insertEnv k entry
          return entry

refreshEnvMembers :: NamedMembers Value -> Interpretation ()
refreshEnvMembers mems = mapMembers_ refreshMember mems
  where refreshMember k vq = do
          entry <- (uncurry entryOfValueQ) vq
          modifyE $ replaceEnv k entry

unbindMembers :: NamedBindings (IEnvEntry Value) -> Interpretation (NamedMembers Value)
unbindMembers bindings = mapBindings popMember bindings
  where popMember k entry = do
          void $ modifyE $ removeEnv k
          valueQOfEntry entry


{- Environment accessors -}
emptyEnvIO :: IO (IEnvironment Value)
emptyEnvIO = HT.new

emptyEnv :: Interpretation (IEnvironment Value)
emptyEnv = liftIO emptyEnvIO

envFromList :: [(Identifier, IEnvEntry Value)] -> Interpretation (IEnvironment Value)
envFromList l = liftIO (envFromListIO l)

envFromListIO :: [(Identifier, IEnvEntry Value)] -> IO (IEnvironment Value)
envFromListIO l = emptyEnvIO >>= \env -> foldM (\acc (n,e) -> insertEnvIO n e acc) env l

lookupEnv :: Identifier -> IEnvironment Value -> Interpretation (Maybe (IEnvEntry Value))
lookupEnv n env = liftIO $ lookupEnvIO n env

lookupEnvIO :: Identifier -> IEnvironment Value -> IO (Maybe (IEnvEntry Value))
lookupEnvIO n env =
  HT.lookup env n >>= return . maybe Nothing (\l -> if null l then Nothing else Just $ head l)

chainEnv :: (Monad m) => (IEnvironment Value -> m ()) -> IEnvironment Value -> m (IEnvironment Value)
chainEnv f env = f env >>= const (return env)

insertEnv :: Identifier -> IEnvEntry Value -> IEnvironment Value
          -> Interpretation (IEnvironment Value)
insertEnv n v env = liftIO (insertEnvIO n v env)

insertEnvIO :: Identifier -> IEnvEntry Value -> IEnvironment Value -> IO (IEnvironment Value)
insertEnvIO n v = chainEnv (\env -> HT.lookup env n >>= HT.insert env n . (maybe [v] (v:)))

removeEnv :: Identifier -> IEnvironment Value -> Interpretation (IEnvironment Value)
removeEnv n env = liftIO (removeEnvIO n env)

removeEnvIO :: Identifier -> IEnvironment Value -> IO (IEnvironment Value)
removeEnvIO n = chainEnv (\env -> HT.lookup env n >>= maybe (return ()) (remove env))
  where remove env []           = HT.delete env n 
        remove env (tail -> nl) = if null nl then HT.delete env n else HT.insert env n nl

replaceEnv :: Identifier -> IEnvEntry Value -> IEnvironment Value
           -> Interpretation (IEnvironment Value)
replaceEnv n v = chainEnv (\env -> liftIO (HT.lookup env n) >>= liftIO . maybe (return ()) (replace env))
  where replace env [] = HT.insert env n [v]
        replace env l  = HT.insert env n (v:tail l)

-- | Adds all bindings from the source environment to the destination environment.
--   All bindings in the source are accessible prior to those in the destination during lookup
mergeEnv :: IEnvironment Value -> IEnvironment Value -> Interpretation (IEnvironment Value)
mergeEnv src dest = liftIO $ HT.foldM (\a (k,vl) -> foldM (\a2 v -> insertEnvIO k v a2) a vl) dest src

-- | Removes all bindings from the source environment if they are present in the destination.
--   This is the inverse of the merge operation above.
-- TODO: think through name-only removal in this instance, rather than named value removal.
pruneEnv :: IEnvironment Value -> IEnvironment Value -> Interpretation (IEnvironment Value)
pruneEnv src dest = liftIO $ HT.foldM (\a (k,vl) -> foldM (\a2 _ -> removeEnvIO k a2) a vl) dest src

foldEnv :: (a -> Identifier -> IEnvEntry Value -> IO a) -> a -> IEnvironment Value -> Interpretation a
foldEnv f z env = liftIO $ foldEnvIO f z env

foldEnvIO :: (a -> Identifier -> IEnvEntry Value -> IO a) -> a -> IEnvironment Value -> IO a
foldEnvIO f z env = HT.foldM (\a (k,vl) -> foldM (\a2 v -> f a2 k v) a vl) z env

-- | Synchronizes all collection bindings in an environment.
syncEnv :: IEnvironment Value -> Interpretation (IEnvironment Value)
syncEnv env = liftIO $ syncEnvIO env

syncEnvIO :: IEnvironment Value -> IO (IEnvironment Value)
syncEnvIO env = emptyEnvIO >>= \nenv ->
  HT.foldM (\acc (k,vl) -> mapM syncEntry vl >>= HT.insert acc k >> return acc) nenv env
  where syncEntry e = valueQOfEntryIO e >>= \(v,q) -> syncCollectionIO v >>= flip entryOfValueQIO q

{- State and result accessors -}

emptyProxyStack :: ProxyPathStack
emptyProxyStack = []

emptyTracer :: ITracer
emptyTracer = ITracer [] [] []

emptyAnnotationEnv :: AEnvironment Value
emptyAnnotationEnv = AEnvironment [] []

emptyStaticEnv :: Interpretation (SEnvironment Value)
emptyStaticEnv = emptyEnv >>= return . (, emptyAnnotationEnv)

emptyStaticEnvIO :: IO (SEnvironment Value)
emptyStaticEnvIO = emptyEnvIO >>= return . (, emptyAnnotationEnv)

emptyStateLambda :: IEnvironment Value -> AEnvironment Value -> SEnvironment Value -> PrintConfig -> IState
emptyStateLambda env aEnv sEnv pc = IState [] env aEnv sEnv emptyProxyStack emptyTracer pc

emptyState :: Interpretation IState
emptyState = emptyStateLambda <$> emptyEnv <*> pure emptyAnnotationEnv <*> emptyStaticEnv <*> pure defaultPrintConfig

emptyStateIO :: IO IState
emptyStateIO = emptyStateLambda <$> emptyEnvIO <*> pure emptyAnnotationEnv <*> emptyStaticEnvIO <*> pure defaultPrintConfig

annotationState :: AEnvironment Value -> Interpretation IState
annotationState aEnv = liftIO $ annotationStateIO aEnv

annotationStateIO :: AEnvironment Value -> IO IState
annotationStateIO aEnv = emptyStateLambda <$> emptyEnvIO <*> pure aEnv <*> emptyStaticEnvIO <*> pure defaultPrintConfig

annotationStatePCIO :: PrintConfig -> AEnvironment Value -> IO IState
annotationStatePCIO pc aEnv = emptyStateLambda <$> emptyEnvIO <*> pure aEnv <*> emptyStaticEnvIO <*> pure pc

staticState :: SEnvironment Value -> Interpretation IState
staticState sEnv = liftIO $ staticStateIO sEnv

staticStateIO :: SEnvironment Value -> IO IState
staticStateIO sEnv = emptyStateLambda <$> emptyEnvIO <*> pure emptyAnnotationEnv <*> pure sEnv <*> pure defaultPrintConfig

modifyStateGlobals :: (Globals -> Globals) -> IState -> IState
-- modifyStateGlobals f (IState g e a s b t) = IState (f g) e a s b t
modifyStateGlobals f st = st {getGlobals = f $ getGlobals st}

modifyStateAEnv :: (AEnvironment Value -> AEnvironment Value) -> IState -> IState
modifyStateAEnv f st = st {getAnnotEnv = f $ getAnnotEnv st}

modifyStateProxies :: (ProxyPathStack -> ProxyPathStack) -> IState -> IState
modifyStateProxies f st = st {getProxyStack = f $ getProxyStack st}

modifyTracer :: (ITracer -> ITracer) -> IState -> IState
modifyTracer f st = st {getTracer = f $ getTracer st}

modifyStateEnv :: (IEnvironment Value -> Interpretation (IEnvironment Value)) -> IState
               -> Interpretation IState
modifyStateEnv f st = f (getEnv st) >>= \ne -> return $ st {getEnv=ne}

modifyStateEnvIO :: (IEnvironment Value -> IO (IEnvironment Value)) -> IState
                 -> IO IState
modifyStateEnvIO f st = f (getEnv st) >>= \ne -> return $ st {getEnv=ne}

modifyStateSEnv :: (SEnvironment Value -> Interpretation (SEnvironment Value)) -> IState
                -> Interpretation IState
modifyStateSEnv f st = f (getStaticEnv st) >>= \ns -> return $ st {getStaticEnv=ns}

modifyStateSEnvIO :: (SEnvironment Value -> IO (SEnvironment Value)) -> IState
                  -> IO IState
modifyStateSEnvIO f st = f (getStaticEnv st) >>= \ns -> return $ st {getStaticEnv=ns}

getResultState :: IResult a -> IState
getResultState ((_, x), _) = x

getResultVal :: IResult a -> Either InterpretationError a
getResultVal ((x, _), _) = x

syncIState :: IState -> IO IState
syncIState st = modifyStateEnvIO syncEnvIO st 

{- Interpretation Helpers -}

-- | Run an interpretation to get a value or error, resulting environment and event log.
runInterpretation :: Engine Value -> IState -> Interpretation a -> IO (Either EngineError (IResult a))
runInterpretation e s = flip runEngineM e . runWriterT . flip runStateT s . runEitherT

runInterpretation' :: IState -> Interpretation a -> EngineM Value (IResult a)
runInterpretation' s = runWriterT . flip runStateT s . runEitherT

-- | Run an interpretation and extract its value.
valueOfInterpretation :: IState -> Interpretation a -> EngineM Value (Maybe a)
valueOfInterpretation s i = runInterpretation' s i >>= return . either (const $ Nothing) Just . fst . fst

-- | Raise an exception in the interpretation monad.
throwE :: (Maybe (Span, UID) -> InterpretationError) -> Interpretation a
throwE f = getTraceUID >>= Control.Monad.Trans.Either.left . f

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
lookupE :: Identifier -> Interpretation (IEnvEntry Value)
lookupE n = get >>= \s -> lookupEnv n (getEnv s) >>= maybe err return
  where err = throwE $ RunTimeTypeError $ "Unknown Variable: '" ++ n ++ "'"

-- | Environment modification
modifyE :: (IEnvironment Value -> Interpretation (IEnvironment Value)) -> Interpretation ()
modifyE f = get >>= modifyStateEnv f >>= put

-- | Environment binding insertion
insertE :: Identifier -> IEnvEntry Value -> Interpretation ()
insertE n v = modifyE $ insertEnv n v

-- | Environment binding removal
removeE :: Identifier -> a -> Interpretation a
removeE n r = modifyE (removeEnv n) >> return r

-- | Environment binding replacement.
replaceE :: Identifier -> IEnvEntry Value -> Interpretation ()
replaceE n v = modifyE $ replaceEnv n v

-- | Bulk environment binding insertion.
mergeE :: IEnvironment Value -> Interpretation ()
mergeE env = modifyE $ mergeEnv env 

-- | Bulk environment binding removal.
pruneE :: IEnvironment Value -> Interpretation ()
pruneE env = modifyE $ pruneEnv env

-- | Synchronize cached collection values
syncE :: Interpretation ()
syncE = modifyE $ syncEnv

-- | Annotation environment helpers.
lookupADef :: Identifier -> Interpretation (NamedMembers Value)
lookupADef n = tryLookupADef n >>= maybe err return
  where err = throwE $ RunTimeTypeError $ "Unknown annotation definition: '" ++ n ++ "'"

tryLookupADef :: Identifier -> Interpretation (Maybe (NamedMembers Value))
tryLookupADef n = get >>= return . lookup n . definitions . getAnnotEnv

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
          Nothing   -> return $ init ps ++ [Just [n]]
          Just path -> throwE $ RunTimeInterpretationError
                              $ "Duplicate bind alias: " ++ pretty n ++ ", " ++ pretty path

appendAliasExtension :: Identifier -> Interpretation ()
appendAliasExtension n = modifyProxyStack_ pushProxyAliasExtension
  where pushProxyAliasExtension [] = return []
        pushProxyAliasExtension ps = case last ps of
          Just proxypath -> return $ init ps ++ [Just $ proxypath++[RecordField n]]
          Nothing       -> throwE $ RunTimeInterpretationError
                                  $ "No bind alias found for extension: " ++ n

-- | Tracing helpers
pushTraceUID :: (Span, UID) -> Interpretation ()
pushTraceUID su = modify $ modifyTracer $ \(ITracer s we wv) -> ITracer (su:s) we wv

getTraceUID :: Interpretation (Maybe (Span, UID))
getTraceUID = get >>= (\l -> if null l then return Nothing else return . Just $ head l) . stackTrace . getTracer

popTraceUID :: Interpretation ()
popTraceUID = modify $ modifyTracer $ \(ITracer s we wv) -> ITracer (tail s) we wv

watchExpression :: UID -> Interpretation ()
watchExpression uid = modify $ modifyTracer $ \(ITracer s we wv) -> ITracer s (uid:we) wv

isWatchedExpression :: UID -> Interpretation Bool
isWatchedExpression uid = get >>= return . elem uid . watchedExprs . getTracer

ignoreWatchExpression :: UID -> Interpretation ()
ignoreWatchExpression uid = modify $ modifyTracer $ \(ITracer s we wv) -> ITracer s (delete uid we) wv

watchVariable :: UID -> Identifier -> Interpretation ()
watchVariable uid i = modify $ modifyTracer $
  \(ITracer s we wv) -> ITracer s we $ snd $ modifyAssoc wv uid $ maybe ((), Just [i]) $ \l -> ((), Just $ i:l)

getWatchedVariables :: UID -> Interpretation [Identifier]
getWatchedVariables uid = get >>= return . maybe [] id . lookup uid . watchedVars . getTracer

ignoreWatchVariable :: UID -> Identifier -> Interpretation ()
ignoreWatchVariable uid i = modify $ modifyTracer $
  \(ITracer s we wv) -> ITracer s we $ snd $ modifyAssoc wv uid $ maybe ((), Nothing) $ \l -> ((), Just $ delete i l) 


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
