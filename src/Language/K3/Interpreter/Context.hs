{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter.Context (
  runInterpretation,

  runExpression,
  runExpression_,

  runNetwork,
  runProgram,

  emptyState,
  getResultVal,

  emptyStaticEnv

-- #ifdef TEST
-- TODO When the ifdef is uncommented, the dataspace tests don't compile
  , throwE
-- #endif
) where

import Control.Arrow hiding ( (+++) )
import Control.Concurrent (ThreadId)
import Control.Concurrent.MVar
import Control.Monad.Reader

import Data.Function
import Data.List

import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Values
import Language.K3.Interpreter.Collection
import Language.K3.Interpreter.Utils
import Language.K3.Interpreter.Builtins
import Language.K3.Interpreter.Evaluation

import Language.K3.Runtime.Common ( PeerBootstrap, SystemEnvironment )
import Language.K3.Runtime.Dispatch
import Language.K3.Runtime.Engine

import Language.K3.Transform.Interpreter.BindAlias ( labelBindAliases )

import Language.K3.Utils.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["RegisterGlobal"])


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
  logIStateM "STATIC " Nothing $ getResultState staticR
  let st = getResultState staticR
  funEnv   <- staticFunctions st   
  annotEnv <- staticAnnotations st
  return (funEnv, annotEnv)

  where
    staticFunctions :: IState -> EngineM Value (IEnvironment Value)
    staticFunctions st = do
      resultIEnv <- runInterpretation' st 
                          (emptyEnv >>= \nenv -> foldEnv insertIfFunction nenv $ getEnv st)
                      >>= liftError "(constructing static function environment)"
      case getResultVal resultIEnv of
        Left err   -> throwEngineError . EngineError $ show err
        Right ienv -> return ienv

    insertIfFunction acc n e@(IVal (VFunction _)) = insertEnvIO n e acc
    insertIfFunction acc n e@(MVal mv) = readMVar mv >>= \case
      VFunction _  -> insertEnvIO n e acc 
      _            -> return acc
    insertIfFunction acc _ _ = return acc

    staticAnnotations :: IState -> EngineM Value (AEnvironment Value)
    staticAnnotations st = do
      let annEnvI = foldM addRealization (getAnnotEnv st) $ nub $ declCombos prog
      resultAEnv <- runInterpretation' st annEnvI >>= liftError "(constructing static annotation environment)"
      case getResultVal resultAEnv of
        Left err                 -> throwEngineError . EngineError $ show err
        Right (AEnvironment d r) -> return $ AEnvironment d $ nubBy ((==) `on` fst) r
    
    addRealization aEnv@(AEnvironment d r) annNames = do
      comboIdOpt <- getComposedAnnotation annNames
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
    initGlobal st' n (tag -> TSink) _            = initTrigger st' n
    initGlobal st' n t@(isFunction -> True) eOpt = initFunction st' n t eOpt
    initGlobal st' _ _ _                         = return st'

    initTrigger st' n = initializeBinding st'
      (memEntTag Nothing >>= \tg -> insertE n $ IVal $ VTrigger (n, Nothing, tg))

    -- Functions create lambda expressions, thus they are safe to initialize during
    -- environment construction. This way, all global functions can be mutually recursive.
    -- Note that since we only initialize functions, no declaration can force function
    -- evaluation during initialization (e.g., as would be the case with variable initializers).
    initFunction st' n t (Just e) = initializeExpr st' n t e
    initFunction st' n t Nothing  = initializeBinding st' $ builtin n t

    initializeExpr st' n t e = initializeBinding st' (expression e >>= entryOfValueT (t @~ isTQualified) >>= insertE n)
    initializeBinding st' interp = runInterpretation' st' interp
                                     >>= liftError "(initializing environment)"
                                     >>= return . getResultState

    -- | Global identifier registration
    registerGlobal :: Identifier -> IState -> IState
    registerGlobal n istate = modifyStateGlobals ((:) n) istate

    registerDecl :: IState -> K3 Declaration -> IState
    registerDecl st' (tag -> DGlobal n _ _)  = _debugI_RegisterGlobal ("Registering global "++n) registerGlobal n st'
    registerDecl st' (tag -> DTrigger n _ _) = _debugI_RegisterGlobal ("Registering global "++n) registerGlobal n st'
    registerDecl st' _                       = _debugI_RegisterGlobal ("Skipping global registration") st'


initState :: K3 Declaration -> EngineM Value IState
initState prog = liftIO emptyStateIO >>= initEnvironment prog

{- Program initialization and finalization via the atInit and atExit triggers -}
initMessages :: IResult () -> EngineM Value (IResult Value)
initMessages ((Left err, s), ilog) = return ((Left err, s), ilog)
initMessages ((Right _, st), ilog) = do
    vOpt <- liftIO (lookupEnvIO "atInit" (getEnv st) >>= valueOfEntryOptIO)
    case vOpt of
      Just (VFunction (f, _, _)) -> runInterpretation' st (f vunit)
      _                          -> return ((unknownTrigger, st), ilog)
  where 
    unknownTrigger = Left $ RunTimeTypeError "Could not find atInit trigger" Nothing

finalMessages :: IState -> EngineM Value (IResult Value)
finalMessages st = do
    atExitVOpt <- liftIO (lookupEnvIO "atExit" (getEnv st) >>= valueOfEntryOptIO)
    runInterpretation' st $ maybe unknownTrigger runFinal atExitVOpt
  
  where runFinal (VFunction (f,_,_)) = f vunit >>= \r -> syncE >> return r
        runFinal _                   = throwE $ RunTimeTypeError "Invalid atExit trigger"
        unknownTrigger               = throwE $ RunTimeTypeError "Could not find atExit trigger"

-- TODO: determine qualifier when creating environment entry for literals
initBootstrap :: PeerBootstrap -> AEnvironment Value -> EngineM Value (IEnvironment Value)
initBootstrap bootstrap aEnv = do
    st <- liftIO $ annotationStateIO aEnv
    namedLiterals <- mapM (interpretLiteral st) bootstrap
    liftIO $ envFromListIO namedLiterals
  where 
    interpretLiteral st (n,l) = runInterpretation' st (literal l)
                                  >>= liftError "(initializing bootstrap)"
                                  >>= either invalidErr (return . (n,) . IVal) . getResultVal
    invalidErr = const $ throwEngineError $ EngineError "Invalid result"

injectBootstrap :: PeerBootstrap -> IResult a -> EngineM Value (IResult a)
injectBootstrap _         r@((Left _, _), _) = return r
injectBootstrap bootstrap ((Right val, st), rLog) = do
    bootEnv <- initBootstrap bootstrap (getAnnotEnv st)
    nEnv    <- liftIO emptyEnvIO
    nvEnv   <- liftIO $ foldEnvIO (addFromBoostrap bootEnv) nEnv $ getEnv st
    nSt     <- liftIO $ modifyStateEnvIO (const $ return nvEnv) st
    return ((Right val, nSt), rLog)

  where addFromBoostrap bootEnv acc n e = lookupEnvIO n bootEnv >>= flip (insertEnvIO n) acc . maybe e id

initProgram :: PeerBootstrap -> SEnvironment Value -> K3 Declaration -> EngineM Value (IResult Value)
initProgram bootstrap staticEnv prog = do
    initSt   <- initState prog
    staticSt <- liftIO $ modifyStateSEnvIO (const $ return staticEnv) initSt
    declR    <- runInterpretation' staticSt (declaration prog)
    bootR    <- injectBootstrap bootstrap declR
    initMessages bootR


{- Standalone (i.e., single peer) evaluation -}

standaloneInterpreter :: (IEngine -> IO a) -> IO a
standaloneInterpreter f = simpleEngine >>= f

runExpression :: K3 Expression -> IO (Maybe Value)
runExpression e = standaloneInterpreter $ \engine -> do
    st <- liftIO $ emptyStateIO
    rv <- runEngineM (valueOfInterpretation st $ expression e) engine
    return $ either (const Nothing) id rv

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
      flip runEngineM engine $ runEngine (virtualizedProcessor sEnv) tProg

  where buildStaticEnv = do
          trigs <- return $ getTriggerIds tProg
          sEnv  <- emptyStaticEnvIO
          preEngine <- simulationEngine trigs isPar systemEnv $ syntaxValueWD sEnv
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
        engineThreads <- mapM (fork sEnv) namedEngines
        return engineThreads

  where
    buildStaticEnv trigs = do
      sEnv <- emptyStaticEnvIO
      preEngine <- simulationEngine trigs isPar systemEnv $ syntaxValueWD sEnv
      flip runEngineM preEngine $ staticEnvironment tProg

    pairWithAddress (engine, bootstrap) = (fst . head $ bootstrap, engine)
    fork staticEnv (addr, engine) = do
      threadId <- flip runEngineM engine $ forkEngine (virtualizedProcessor staticEnv) tProg
      return $ either Left (Right . (addr, engine,)) threadId

    tProg = labelBindAliases prog


{- Message processing -}

runTrigger :: IResult Value -> Identifier -> Value -> Value -> EngineM Value (IResult Value)
runTrigger r n a = \case
    (VTrigger (_, Just f, _)) -> do
        result <- runInterpretation' (getResultState r) (f a)
        logTriggerM defaultAddress n a result
        return result
    (VTrigger _)           -> return $ iError ("Uninitialized trigger " ++ n) Nothing
    _                      -> return $ tError ("Invalid trigger or sink value for " ++ n) Nothing

  where iError s m = mkError r $ RunTimeInterpretationError s m
        tError s m = mkError r $ RunTimeTypeError s m
        mkError ((_,st), ilog) v = ((Left v, st), ilog)


-- | Message processing for multiple (virtualized) peers.
type VirtualizedMessageProcessor = 
  MessageProcessor (K3 Declaration) Value [(Address, IResult Value)] [(Address, IResult Value)]

virtualizedProcessor :: SEnvironment Value -> VirtualizedMessageProcessor
virtualizedProcessor staticEnv = MessageProcessor {
    initialize = initializeVP,
    process    = processVP,
    status     = statusVP,
    finalize   = finalizeVP,
    report     = reportVP
} where
    initializeVP program = do
      engine <- ask
      sequence [initNode node program (deployment engine) | node <- nodes engine]

    initNode node program systemEnv = do
      initEnv     <- return $ maybe [] id $ lookup node systemEnv
      initIResult <- initProgram initEnv staticEnv program
      logIResultM "INIT " (Just node) initIResult
      return (node, initIResult)

    processVP (addr, name, args) ps = fmap snd $ flip runDispatchT ps $ do
      dispatch addr (\s -> runTrigger' s addr name args)

    runTrigger' s addr n a = do
      void $ logTriggerM addr n a s 
      entryOpt <- liftIO $ lookupEnvIO n $ getEnv $ getResultState s
      vOpt     <- liftIO $ valueOfEntryOptIO entryOpt
      case vOpt of
        Nothing -> return (Just (), unknownTrigger s n)
        Just ft -> fmap (Just (),) $ runTrigger s n a ft

    unknownTrigger ((_,st), ilog) n = ((Left $ RunTimeTypeError ("Unknown trigger " ++ n) Nothing, st), ilog)

    -- TODO: Fix status computation to use rest of list.
    statusVP [] = Left []
    statusVP is@(p:_) = case sStatus p of
        Left _ -> Left is
        Right _ -> Right is

    sStatus (node, res) = either (const $ Left (node, res)) (const $ Right (node, res)) $ getResultVal res

    finalizeVP = mapM sFinalize

    sFinalize (node, res) = do
      res' <- either (const $ return res) (const $ finalMessages $ getResultState res) $ getResultVal res
      return (node, res')

    reportVP (Left err)  = mapM_ reportNodeIResult err
    reportVP (Right res) = mapM_ reportNodeIResult res

    reportNodeIResult (addr, r) = do
      void $ liftIO (putStrLn ("[" ++ show addr ++ "]"))
      void $ prettyIResultM r >>= liftIO . putStr . boxToString . indent 2 


-- | Message processing for a single peer.
type SingletonMessageProcessor =
  MessageProcessor (K3 Declaration) Value (IResult Value) (IResult Value)

uniProcessor :: SEnvironment Value -> SingletonMessageProcessor
uniProcessor staticEnv = MessageProcessor {
    initialize = initUP,
    process    = processUP,
    status     = statusUP,
    finalize   = finalizeUP,
    report     = reportUP
} where
    initUP prog = ask >>= \x -> initProgram (uniBootstrap $ deployment x) staticEnv prog
    uniBootstrap [] = []
    uniBootstrap ((_,is):_) = is

    statusUP res   = either (\_ -> Left res) (\_ -> Right res) $ getResultVal res
    finalizeUP res = either (\_ -> return res) (\_ -> finalMessages $ getResultState res) $ getResultVal res

    reportUP (Left err)  = showIResultM err >>= liftIO . putStr
    reportUP (Right res) = showIResultM res >>= liftIO . putStr

    processUP (_, n, args) r = do
      entryOpt <- liftIO $ lookupEnvIO n $ getEnv $ getResultState r
      vOpt     <- liftIO $ valueOfEntryOptIO entryOpt
      maybe (return $ unknownTrigger r n) (run r n args) vOpt

    run r n args trig = do
      void $ logTriggerM defaultAddress n args r
      result <- runTrigger r n args trig
      void $ logTriggerM defaultAddress n args result
      return result

    unknownTrigger ((_,st), ilog) n = ((Left $ RunTimeTypeError ("Unknown trigger " ++ n) Nothing, st), ilog)
