{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter.Context (
  NetworkPeer,
  VirtualizedMessageProcessor,
  SingletonMessageProcessor,

  runInterpretation,

  runExpression,
  runExpression_,

  prepareProgram,
  runProgram,
  
  prepareNetwork,
  runNetwork,

  emptyState,
  getResultVal,

  emptyStaticEnv

-- #ifdef TEST
-- TODO When the ifdef is uncommented, the dataspace tests don't compile
  , throwE
-- #endif
) where

import Control.Arrow hiding ( (+++) )
import Control.Concurrent
import Control.Monad.Identity
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

import Language.K3.Analysis.Interpreter.BindAlias ( labelBindAliases )
import Language.K3.Analysis.AnnotationGraph

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
staticEnvironment :: PrintConfig -> K3 Declaration -> EngineM Value (SEnvironment Value)
staticEnvironment pc prog = do
  initSt  <- initState pc emptyAnnotationEnv prog
  staticR <- runInterpretation' initSt (declaration prog)
  logIStateM "PRE STATIC " Nothing $ getResultState staticR
  let st = getResultState staticR
  funEnv   <- staticFunctions st   
  annotEnv <- staticAnnotations st
  liftIO (staticStateIO (funEnv, annotEnv)) >>= logIStateM "STATIC " Nothing 
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
      let annProvs  = annotationProvides prog
      let flatADefs = flattenADefinitions annProvs (definitions $ getAnnotEnv st)
      let newAEnv   = AEnvironment flatADefs []
      let annEnvI   = foldM addRealization newAEnv $ nub $ declCombos prog
      resultAEnv    <- liftIO (annotationStateIO newAEnv) 
                        >>= \est -> runInterpretation' est annEnvI
                        >>= liftError "(constructing static annotation environment)"
      case getResultVal resultAEnv of
        Left err                 -> throwEngineError . EngineError $ show err
        Right (AEnvironment d r) -> return $ AEnvironment d $ nubBy ((==) `on` fst) r
    
    flattenADefinitions :: [(Identifier, Identifier)] -> AnnotationDefinitions Value
                        -> AnnotationDefinitions Value
    flattenADefinitions provides aDefs = foldl addProvidesMembers aDefs provides

    addProvidesMembers aDefs (s,t) = case (lookup s aDefs, lookup t aDefs) of
      (Just sMems, Just tMems) -> replaceAssoc aDefs s $ mergeMembers sMems tMems 
      _ -> aDefs

    addRealization :: AEnvironment Value -> [Identifier] -> Interpretation (AEnvironment Value)
    addRealization aEnv@(AEnvironment d r) annNames = do
      comboIdOpt <- getComposedAnnotation annNames
      case comboIdOpt of
        Nothing  -> return aEnv
        Just cId -> lookupACombo cId >>= return . AEnvironment d . (:r) . (cId,)

    declCombos :: K3 Declaration -> [[Identifier]]
    declCombos = runIdentity . foldTree extractDeclCombos []

    typeCombos :: K3 Type -> [[Identifier]]
    typeCombos = runIdentity . foldTree extractTypeCombos []

    exprCombos :: K3 Expression -> [[Identifier]]
    exprCombos = runIdentity . foldTree extractExprCombos []    
    
    extractDeclCombos :: [[Identifier]] -> K3 Declaration -> Identity [[Identifier]]
    extractDeclCombos st (tag -> DGlobal _ t eOpt)     = return $ st ++ typeCombos t ++ (maybe [] exprCombos eOpt)
    extractDeclCombos st (tag -> DTrigger _ t e)       = return $ st ++ typeCombos t ++ exprCombos e
    extractDeclCombos st (tag -> DAnnotation _ _ mems) = return $ st ++ concatMap memCombos mems
    extractDeclCombos st _ = return st
    
    extractTypeCombos :: [[Identifier]] -> K3 Type -> Identity [[Identifier]]
    extractTypeCombos c (tag &&& annotations -> (TCollection, tAnns)) = 
      case namedTAnnotations tAnns of
        []        -> return c
        namedAnns -> return $ namedAnns:c
    
    extractTypeCombos c _ = return c

    extractExprCombos :: [[Identifier]] -> K3 Expression -> Identity [[Identifier]]
    extractExprCombos c (tag &&& annotations -> (EConstant (CEmpty et), eAnns)) = 
      case namedEAnnotations eAnns of
        []        -> return $ c ++ typeCombos et
        namedAnns -> return $ (namedAnns:c) ++ typeCombos et
    extractExprCombos c _ = return c

    memCombos :: AnnMemDecl -> [[Identifier]]
    memCombos (Lifted _ _ t eOpt _)    = typeCombos t ++ (maybe [] exprCombos eOpt)
    memCombos (Attribute _ _ t eOpt _) = typeCombos t ++ (maybe [] exprCombos eOpt)
    memCombos _ = []


initEnvironment :: K3 Declaration -> IState -> EngineM Value IState
initEnvironment decl st =
  let declGState  = runIdentity $ foldTree registerDecl st decl
  in initDecl declGState decl
  where 
    initDecl st' (tag &&& children -> (DGlobal n t eOpt, ch)) = initGlobal st' n t eOpt >>= flip (foldM initDecl) ch
    initDecl st' (tag &&& children -> (DTrigger n _ _, ch))   = initTrigger st' n >>= flip (foldM initDecl) ch
    initDecl st' (tag &&& children -> (DRole _, ch))          = foldM initDecl st' ch
    initDecl st' _                                            = return st'

    -- | Global initialization for cyclic dependencies.
    --   This partially initializes sinks and functions (to their defining lambda expression).
    initGlobal :: IState -> Identifier -> K3 Type -> Maybe (K3 Expression) -> EngineM Value IState
    initGlobal st' n (tag -> TSink) _             = initTrigger st' n
    initGlobal st' n t@(isTFunction -> True) eOpt = initFunction st' n t eOpt
    initGlobal st' _ _ _                          = return st'

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

    registerDecl :: IState -> K3 Declaration -> Identity IState
    registerDecl st' (tag -> DGlobal n _ _)  = debugRegDecl ("Registering global "++n)       $ registerGlobal n st'
    registerDecl st' (tag -> DTrigger n _ _) = debugRegDecl ("Registering global "++n)       $ registerGlobal n st'
    registerDecl st' _                       = debugRegDecl ("Skipping global registration") $ st'

    debugRegDecl s a = return $ _debugI_RegisterGlobal s a


initState :: PrintConfig -> AEnvironment Value -> K3 Declaration -> EngineM Value IState
initState pc aEnv prog = liftIO (annotationStatePCIO pc aEnv) >>= initEnvironment prog

{- Program initialization and finalization via the atInit and atExit triggers -}
atInitTrigger :: IResult () -> EngineM Value (IResult Value)
atInitTrigger ((Left err, s), ilog) = return ((Left err, s), ilog)
atInitTrigger ((Right _, st), ilog) = do
    vOpt <- liftIO (lookupEnvIO "atInit" (getEnv st) >>= valueOfEntryOptIO)
    case vOpt of
      Just (VFunction (f, _, _)) -> runInterpretation' st (f vunit)
      _                          -> return ((unknownTrigger, st), ilog)
  where 
    unknownTrigger = Left $ RunTimeTypeError "Could not find atInit trigger" Nothing

atExitTrigger :: IState -> EngineM Value (IResult Value)
atExitTrigger st = do
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

initProgram :: PrintConfig -> PeerBootstrap -> SEnvironment Value -> K3 Declaration -> EngineM Value (IResult Value)
initProgram pc bootstrap staticEnv prog = do
    initSt   <- initState pc (snd staticEnv) prog
    staticSt <- liftIO $ modifyStateSEnvIO (const $ return staticEnv) initSt
    declR    <- runInterpretation' staticSt (declaration prog)
    bootR    <- injectBootstrap bootstrap declR
    atInitTrigger bootR


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

-- | Programs prepared for execution as a simulation.
type PreparedProgram = (K3 Declaration, Engine Value, VirtualizedMessageProcessor)

-- | Programs prepared for execution as virtualized network peers.
type PreparedNetwork = (K3 Declaration, [(Address, Engine Value, VirtualizedMessageProcessor)])

-- | Network peer information for a K3 engine running in network mode.
type NetworkPeer = (Address, Engine Value, ThreadId, VirtualizedMessageProcessor)

-- | Single-machine system simulation.
prepareProgram :: PrintConfig -> Bool -> SystemEnvironment -> K3 Declaration
               -> IO (Either EngineError PreparedProgram)
prepareProgram pc isPar systemEnv prog =
    buildStaticEnv >>= \case
      Left err   -> return $ Left err
      Right sEnv -> do
        trigs   <- return $ getTriggerIds tProg
        engine  <- simulationEngine trigs isPar systemEnv $ syntaxValueWD sEnv
        msgProc <- virtualizedProcessor pc sEnv
        return $ Right (tProg, engine, msgProc)

  where buildStaticEnv = do
          trigs     <- return $ getTriggerIds tProg
          sEnv      <- emptyStaticEnvIO
          preEngine <- simulationEngine trigs isPar systemEnv $ syntaxValueWD sEnv
          flip runEngineM preEngine $ staticEnvironment pc tProg

        tProg = labelBindAliases prog


runProgram :: PrintConfig -> PreparedProgram -> IO (Either EngineError [NetworkPeer])
runProgram pc (prog, engine, msgProc) = do
    eStatus <- runEngineM (runEngine pc msgProc prog) engine
    either (return . Left) (const $ peerStatuses) eStatus

  where peerStatuses = do
          tid     <- myThreadId
          lastRes <- readMVar (snapshot msgProc)
          return . Right $ map (\(addr, _) -> (addr, engine, tid, msgProc)) lastRes


-- | Single-machine network deployment.
--   Takes a system deployment and forks a network engine for each peer.

prepareNetwork :: PrintConfig -> Bool -> SystemEnvironment -> K3 Declaration
               -> IO (Either EngineError PreparedNetwork)
prepareNetwork pc isPar systemEnv prog =
  let nodeBootstraps = map (:[]) systemEnv in 
    buildStaticEnv (getTriggerIds tProg) >>= \case
      Left err   -> return $ Left err
      Right sEnv -> do
        trigs        <- return $ getTriggerIds tProg
        engines      <- mapM (flip (networkEngine trigs isPar) $ syntaxValueWD sEnv) nodeBootstraps
        namedEngines <- return . map pairWithAddress $ zip engines nodeBootstraps
        peers        <- mapM (preparePeer sEnv) namedEngines
        return $ Right (tProg, peers)

  where
    buildStaticEnv trigs = do
      sEnv <- emptyStaticEnvIO
      preEngine <- simulationEngine trigs isPar systemEnv $ syntaxValueWD sEnv
      flip runEngineM preEngine $ staticEnvironment pc tProg

    pairWithAddress (engine, bootstrap) = (fst . head $ bootstrap, engine)

    preparePeer staticEnv (addr, engine) =
      virtualizedProcessor pc staticEnv >>= return . (addr, engine,)

    tProg = labelBindAliases prog


runNetwork :: PrintConfig -> PreparedNetwork -> IO [Either EngineError NetworkPeer]
runNetwork pc (prog, peers) = mapM fork peers
  where fork (addr, engine, msgProc) = do
          threadId <- flip runEngineM engine $ forkEngine pc msgProc prog
          return $ either Left (\tid -> Right (addr, engine, tid, msgProc)) threadId


{- Message processing -}

runTrigger :: Address -> IResult Value -> Identifier -> Value -> Value
           -> EngineM Value (IResult Value)
runTrigger addr result nm arg = \case
    (VTrigger (_, Just f, _)) -> do
        -- Interpret the trigger function and refresh any cached collections in the environment.
        ((v,st),lg) <- runInterpretation' (getResultState result) (f arg)
        st'         <- liftIO $ syncIState st
        logTriggerM addr nm arg st' (Just v) "AFTER"
        return ((v,st'),lg)
    
    (VTrigger _)           -> return $ iError ("Uninitialized trigger " ++ nm) Nothing
    _                      -> return $ tError ("Invalid trigger or sink value for " ++ nm) Nothing

  where iError s m = mkError result $ RunTimeInterpretationError s m
        tError s m = mkError result $ RunTimeTypeError s m
        mkError ((_,st), ilog) v = ((Left v, st), ilog)


-- | Message processing for multiple (virtualized) peers.
type VirtualizedMessageProcessor = 
  MessageProcessor (K3 Declaration) Value [(Address, IResult Value)] [(Address, IResult Value)]

virtualizedProcessor :: PrintConfig -> SEnvironment Value -> IO VirtualizedMessageProcessor
virtualizedProcessor pc staticEnv = do
    snapshotMV <- newEmptyMVar
    return $ MessageProcessor {
      initialize = initializeVP snapshotMV,
      process    = processVP snapshotMV,
      finalize   = finalizeVP snapshotMV,
      status     = statusVP,
      report     = reportVP,
      snapshot   = snapshotMV
    }
  where
    initializeVP snapshotMV program = do
      engine <- ask
      res    <- sequence [initNode node program (deployment engine) | node <- nodes engine]
      void $ liftIO $ putMVar snapshotMV res
      return res

    initNode node program systemEnv = do
      initEnv     <- return $ maybe [] id $ lookup node systemEnv
      initIResult <- initProgram pc initEnv staticEnv program
      logIResultM "INIT " (Just node) initIResult
      return (node, initIResult)

    processVP snapshotMV (addr, name, args) ps = do
      res <- fmap snd $ runDispatchT (dispatch addr (\s -> runPeerTrigger s addr name args)) ps
      void $ liftIO $ modifyMVar_ snapshotMV $ const $ return res
      return res

    runPeerTrigger s addr nm arg = do
      -- void $ logTriggerM addr nm arg s None "BEFORE"
      entryOpt <- liftIO $ lookupEnvIO nm $ getEnv $ getResultState s
      vOpt     <- liftIO $ valueOfEntryOptIO entryOpt
      case vOpt of
        Nothing -> return (Just (), unknownTrigger s nm)
        Just ft -> fmap (Just (),) $ runTrigger addr s nm arg ft

    unknownTrigger ((_,st), ilog) n =
      ((Left $ RunTimeTypeError ("Unknown trigger " ++ n) Nothing, st), ilog)

    -- TODO: Fix status computation to use rest of list.
    statusVP [] = Left []
    statusVP is@(p:_) = case sStatus p of
        Left _ -> Left is
        Right _ -> Right is

    sStatus (node, res) =
      either (const $ Left (node, res)) (const $ Right (node, res)) $ getResultVal res

    finalizeVP snapshotMV ps = do
      res <- mapM sFinalize ps
      void $ liftIO $ modifyMVar_ snapshotMV $ const $ return res
      return res

    sFinalize (node, res) = do
      let (st, val) = (getResultState res, getResultVal res)
      res' <- either (const $ return res) (const $ atExitTrigger st) val
      return (node, res')

    reportVP (Left err)  = mapM_ reportNodeIResult err
    reportVP (Right res) = mapM_ reportNodeIResult res

    reportNodeIResult (addr, r) = do
      void $ liftIO (putStrLn ("[" ++ show addr ++ "]"))
      void $ prettyIResultM r >>= liftIO . putStr . boxToString . indent 2 


-- | Message processing for a single peer.
type SingletonMessageProcessor =
  MessageProcessor (K3 Declaration) Value (IResult Value) (IResult Value)

uniProcessor :: PrintConfig -> SEnvironment Value -> IO SingletonMessageProcessor
uniProcessor pc staticEnv = do
    snapshotMV <- newEmptyMVar
    return $ MessageProcessor {
      initialize = initUP snapshotMV,
      process    = processUP snapshotMV,
      finalize   = finalizeUP snapshotMV,
      status     = statusUP,
      report     = reportUP,
      snapshot   = snapshotMV
    }
  where
    initUP snapshotMV prog = do
      egn <- ask
      res <- initProgram pc (uniBootstrap $ deployment egn) staticEnv prog
      void $ liftIO $ putMVar snapshotMV res
      return res

    uniBootstrap [] = []
    uniBootstrap ((_,is):_) = is

    finalizeUP snapshotMV res = do
      void $ liftIO $ modifyMVar_ snapshotMV $ const $ return res
      either (\_ -> return res) (\_ -> atExitTrigger $ getResultState res) $ getResultVal res

    statusUP res         = either (const $ Left res) (const $ Right res) $ getResultVal res
    reportUP (Left err)  = showIResultM err >>= liftIO . putStr
    reportUP (Right res) = showIResultM res >>= liftIO . putStr

    processUP snapshotMV (_, n, args) r = do
      entryOpt <- liftIO $ lookupEnvIO n $ getEnv $ getResultState r
      vOpt     <- liftIO $ valueOfEntryOptIO entryOpt
      res      <- maybe (return $ unknownTrigger r n) (run r n args) vOpt
      void $ liftIO $ modifyMVar_ snapshotMV $ const $ return res
      return res

    run r n args trig = do
      void   $  logTriggerM defaultAddress n args (getResultState r) Nothing "BEFORE"
      result <- runTrigger defaultAddress r n args trig
      void   $  logTriggerM defaultAddress n args (getResultState result) (Just $ getResultVal result) "AFTER"
      return result

    unknownTrigger ((_,st), ilog) n = ((Left $ RunTimeTypeError ("Unknown trigger " ++ n) Nothing, st), ilog)
