{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
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

  runExpression,
  runExpression_,

  runProgram
) where

import Control.Arrow
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Function
import Data.Functor
import Data.IORef
import Data.List
import Data.Tree
import Data.Word (Word8)
import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Runtime.Dispatch
import Language.K3.Runtime.Engine

import Language.K3.Pretty

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
    | VCollection  [Value]
    | VIndirection (IORef Value)
    | VFunction    (Value -> Interpretation Value)
    | VAddress     Address
    | VTrigger     (Identifier, Maybe (Value -> Interpretation Value))

-- We can't deriving Show because IORefs aren't showable.
instance Show Value where
  show (VBool b)               = "VBool " ++ show b
  show (VByte b)               = "VByte " ++ show b
  show (VInt i)                = "VInt " ++ show i
  show (VReal r)               = "VReal " ++ show r
  show (VString s)             = "VString " ++ show s
  show (VOption m)             = "VOption " ++ show m
  show (VTuple t)              = "VTuple " ++ show t
  show (VRecord r)             = "VRecord " ++ show r
  show (VCollection c)         = "VCollection " ++ show c
  show (VIndirection _)        = "VIndirection <opaque>"
  show (VFunction _)           = "VFunction <function>"
  show (VAddress addr)         = "VAddress " ++ show addr
  show (VTrigger (n, Nothing)) = "VTrigger " ++ n ++ " <uninitialized>"
  show (VTrigger (n, Just _))  = "VTrigger " ++ n ++ " <function>"

instance Read Value

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
type IState = (IEnvironment Value, IEngine)

-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IState (WriterT ILog IO))

-- | An evaluated value type, produced from running an interpretation.
type IResult a = ((Either InterpretationError a, IState), ILog)

-- | Pairing of errors and environments for debugging output.
type EnvOnError = (InterpretationError, IEnvironment Value)

-- | A type capturing the environment resulting from an interpretation
type REnvironment = Either EnvOnError (IEnvironment Value)

{- State and result accessors -}

getEnv :: IState -> IEnvironment Value
getEnv (x,_) = x

getEngine :: IState -> IEngine
getEngine (_,e) = e

getResultState :: IResult a -> IState
getResultState ((_, x), _) = x

getResultVal :: IResult a -> Either InterpretationError a
getResultVal ((x, _), _) = x

{- Interpretation Helpers -}

-- | Run an interpretation to get a value or error, resulting environment and event log.
runInterpretation :: IState -> Interpretation a -> IO (IResult a)
runInterpretation s = runWriterT . flip runStateT s . runEitherT

-- | Run an interpretation and extract the resulting environment
envOfInterpretation :: IState -> Interpretation a -> IO REnvironment
envOfInterpretation s i = runInterpretation s i >>= \case
                                  ((Right _, (env,_)), _) -> return $ Right env
                                  ((Left err, (env,_)), _) -> return $ Left (err, env)

-- | Run an interpretation and extract its value.
valueOfInterpretation :: IState -> Interpretation a -> IO (Maybe a)
valueOfInterpretation s i =
  runInterpretation s i >>= return . either (\_ -> Nothing) Just . fst . fst

-- | Raise an error inside an interpretation. The error will be captured alongside the event log
-- till date, and the current state.
throwE :: InterpretationError -> Interpretation a
throwE = Control.Monad.Trans.Either.left

-- | Environment lookup, with a thrown error if unsuccessful.
lookupE :: Identifier -> Interpretation Value
lookupE n = get >>= maybe (err n) return . lookup n . getEnv
  where err n = throwE $ RunTimeTypeError $ "Unknown Variable: '" ++ n ++ "'"

-- | Environment modification
modifyE :: (IEnvironment Value -> IEnvironment Value) -> Interpretation ()
modifyE f = modify (\(env, eng) -> (f env, eng))

-- | Accessor methods to compute with engine contents
withEngine :: (IEngine -> IO a) -> Interpretation a
withEngine f = get >>= liftIO . f . getEngine

-- | Monadic message passing primitive for the interpreter.
sendE :: Address -> Identifier -> Value -> Interpretation ()
sendE addr n val = get >>= liftIO . (\eg -> send addr n val eg) . getEngine

{- Constants -}
myAddrId :: Identifier
myAddrId = "me"

vunit :: Value
vunit = VTuple []

-- | Default values for specific types
defaultValue :: K3 Type -> Interpretation Value
defaultValue (tag -> TBool)       = return $ VBool False
defaultValue (tag -> TByte)       = return $ VByte 0
defaultValue (tag -> TInt)        = return $ VInt 0
defaultValue (tag -> TReal)       = return $ VReal 0.0
defaultValue (tag -> TString)     = return $ VString ""
defaultValue (tag -> TOption)     = return $ VOption Nothing
defaultValue (tag -> TCollection) = return $ VCollection []
defaultValue (tag -> TAddress)    = return $ VAddress defaultAddress

defaultValue (tag &&& children -> (TIndirection, [x])) = defaultValue x >>= liftIO . newIORef >>= return . VIndirection
defaultValue (tag &&& children -> (TTuple, ch))        = mapM defaultValue ch >>= return . VTuple
defaultValue (tag &&& children -> (TRecord ids, ch))   = mapM defaultValue ch >>= return . VRecord . zip ids
defaultValue _ = undefined

-- | Interpretation of Constants.
constant :: Constant -> Interpretation Value
constant (CBool b)   = return $ VBool b
constant (CInt i)    = return $ VInt i
constant (CByte w)   = return $ VByte w
constant (CReal r)   = return $ VReal r
constant (CString s) = return $ VString s
constant (CNone _)   = return $ VOption Nothing
constant (CEmpty _)  = return $ VCollection []

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
binary OSub = numeric subtract
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
      VFunction b -> b x'
      _ -> throwE $ RunTimeTypeError "Invalid Function Application"

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
expression (tag &&& children -> (ELambda i, [b])) =
  return $ VFunction $ \v ->
    modifyE ((i,v):) >> expression b
      >>= (\rv -> modifyE (deleteBy (\(i,_) (j,_) -> i == j) (i,v)) >> return rv)

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
expression (tag &&& children -> (ELetIn i, [e, b])) = expression e >>= modifyE . (:) . (i,) >> expression b
expression (tag -> ELetIn _) = throwE $ RunTimeTypeError "Invalid LetIn Construction"

-- | Interpretation of Assignment.
expression (tag &&& children -> (EAssign i, [e])) =
  lookupE i >>= (\_ -> expression e)
            >>= modifyE . (\new -> map (\(n,v) -> if i == n then (i,new) else (n,v)))
            >> return vunit
expression (tag -> EAssign _) = throwE $ RunTimeTypeError "Invalid Assignment"

-- | Interpretation of Case-Matches.
expression (tag &&& children -> (ECaseOf i, [e, s, n])) = expression e >>= \case
    VOption (Just v) -> modifyE ((i, v):) >> expression s
    VOption (Nothing) -> expression n
    _ -> throwE $ RunTimeTypeError "Invalid Argument to Case-Match"
expression (tag -> ECaseOf _) = throwE $ RunTimeTypeError "Invalid Case-Match"

-- | Interpretation of Binding.
expression (tag &&& children -> (EBindAs b, [e, f])) = expression e >>= \b' -> case (b, b') of
    (BIndirection i, VIndirection r) -> (modifyE . (:) $ (i, VIndirection r)) >> expression f
    (BTuple ts, VTuple vs) -> (modifyE . (++) $ zip ts vs) >> expression f
    (BRecord ids, VRecord ivs) -> do
        let (idls, idbs) = unzip $ sortBy (compare `on` fst) ids
        let (ivls, ivvs) = unzip $ sortBy (compare `on` fst) ivs
        if idls == ivls
            then modifyE ((++) (zip idbs ivvs)) >> expression f
            else throwE $ RunTimeTypeError "Invalid Bind-Pattern"
    _ -> throwE $ RunTimeTypeError "Bind Mis-Match"
expression (tag -> EBindAs _) = throwE $ RunTimeTypeError "Invalid Bind Construction"

-- | Interpretation of If-Then-Else constructs.
expression (tag &&& children -> (EIfThenElse, [p, t, e])) = expression p >>= \case
    VBool True -> expression t
    VBool False -> expression e
    _ -> throwE $ RunTimeTypeError "Invalid Conditional Predicate"

expression _ = throwE $ RunTimeInterpretationError "Invalid Expression"

{- Declaration interpretation -}

replaceTrigger :: Identifier -> Value -> Interpretation()
replaceTrigger n (VFunction f) = modifyE (\env -> replaceAssoc env n (VTrigger (n, Just f)))
replaceTrigger n _             = throwE $ RunTimeTypeError "Invalid trigger body"

-- FIXME: trigger declarations are no longer globals
global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> Interpretation ()
global n (tag -> TTrigger) (Just e)   = expression e >>= replaceTrigger n
global n (tag -> TSink) (Just e)        = expression e >>= replaceTrigger n
global n (tag -> TSink) Nothing         = throwE $ RunTimeInterpretationError "Invalid sink trigger"
global n (tag -> TSource) _             = return ()
global n t (Just e)                     = expression e >>= modifyE . (:) . (n,)
global n t Nothing | TFunction <- tag t = builtin n t
global n t Nothing                      = defaultValue t >>= modifyE . (:) . (n,)

-- TODO: qualify names?
role :: Identifier -> [K3 Declaration] -> Interpretation ()
role n subDecls = mapM_ declaration subDecls

-- TODO
annotation :: Identifier -> [AnnMemDecl] -> Interpretation ()
annotation n members = undefined

-- TODO: accommodate DTrigger
declaration :: K3 Declaration -> Interpretation ()
declaration (tag &&& children -> (DGlobal n t eO, ch)) =
  debugDecl n t $ global n t eO >> mapM_ declaration ch
  where debugDecl n t = trace (concat ["Adding ", show n, " : ", pretty t])

declaration (tag &&& children -> (DRole r, ch)) = role r ch
declaration (tag -> DAnnotation n members)      = annotation n members

{- Built-in functions -}

ignoreFn e = VFunction $ \_ -> return e
vfun x = return $ VFunction $ x

builtin :: Identifier -> K3 Type -> Interpretation ()
builtin n t = genBuiltin n t >>= modifyE . (:) . (n,)

genBuiltin :: Identifier -> K3 Type -> Interpretation Value

-- parseArgs :: () -> ([String], [(String, String)])
genBuiltin "parseArgs" t =
  return $ ignoreFn $ VTuple [VCollection [], VCollection []]


-- TODO: error handling on all open/close/read/write methods.
-- TODO: argument for initial endpoint bindings for open method as a list of triggers
-- TODO: correct element type (rather than function type sig) for openFile / openSocket

-- type ChannelId = String

-- openFile :: ChannelId -> String -> String -> String -> ()
genBuiltin "openFile" t =
  vfun $ \(VString cid) ->
    vfun $ \(VString path) ->
      vfun $ \(VString format) ->
        vfun $ \(VString mode) ->
          withEngine (openFile cid path (wireDesc format) (Just t) mode) >> return vunit

-- openSocket :: ChannelId -> Address -> String -> String -> ()
genBuiltin "openSocket" t =
  vfun $ \(VString cid) ->
    vfun $ \(VAddress addr) ->
      vfun $ \(VString format) ->
        vfun $ \(VString mode) ->
          withEngine (openSocket cid addr (wireDesc format) (Just t) mode) >> return vunit

-- close :: ChannelId -> ()
genBuiltin "close" t = vfun $ \(VString cid) -> withEngine (close cid) >> return vunit

-- TODO: deregister methods
-- register*Trigger :: ChannelId -> TTrigger () -> ()
genBuiltin "registerFileDataTrigger"     t = registerNotifier "data"
genBuiltin "registerFileCloseTrigger"    t = registerNotifier "close"

genBuiltin "registerSocketAcceptTrigger" t = registerNotifier "accept"
genBuiltin "registerSocketDataTrigger"   t = registerNotifier "data"
genBuiltin "registerSocketCloseTrigger"  t = registerNotifier "close"

-- <source>HasRead :: () -> Bool
genBuiltin (channelMethod -> ("HasRead", Just n)) t = vfun $ \_ -> checkChannel
  where checkChannel = withEngine (hasRead n) >>= maybe invalid (return . VBool)
        invalid = throwE $ RunTimeInterpretationError $ "Invalid source \"" ++ n ++ "\""

-- <source>Read :: () -> t
genBuiltin (channelMethod -> ("Read", Just n)) t = vfun $ \_ -> withEngine (doRead n) >>= throwOnError
  where throwOnError (Just v) = return v
        throwOnError Nothing =
          throwE $ RunTimeInterpretationError $ "Invalid next value from source \"" ++ n ++ "\""

-- <sink>HasWrite :: () -> Bool
genBuiltin (channelMethod -> ("HasWrite", Just n)) t = vfun $ \_ -> checkChannel
  where checkChannel = withEngine (hasWrite n) >>= maybe invalid (return . VBool)
        invalid = throwE $ RunTimeInterpretationError $ "Invalid sink \"" ++ n ++ "\""

-- <sink>Write :: t -> ()
genBuiltin (channelMethod -> ("Write", Just n)) t =
  vfun $ \arg -> withEngine (doWrite n arg) >> return vunit

genBuiltin n _ = throwE $ RunTimeTypeError $ "Invalid builtin \"" ++ n ++ "\""

channelMethod :: String -> (String, Maybe String)
channelMethod x =
  case find (flip isSuffixOf x) ["HasRead", "Read", "HasWrite", "Write"] of
    Just y -> (y, stripSuffix y x)
    Nothing -> (x, Nothing)
  where stripSuffix sfx lst = maybe Nothing (Just . reverse) $ stripPrefix (reverse sfx) (reverse lst)

registerNotifier :: Identifier -> Interpretation Value
registerNotifier n =
  vfun $ \cid -> return $ VFunction $ \target ->
    attach cid n target >> return vunit
  
  where attach (VString cid) n (targetOfValue -> (addr, tid, v)) = withEngine $ attachNotifier_ cid n (addr, tid, v)
        attach _ _ _ = undefined

        targetOfValue (VTuple [VTrigger (n, _), VAddress addr]) = (addr, n, vunit)
        targetOfValue _ = error "Invalid notifier target"

{- Program initialization methods -}

initEnvironment :: K3 Declaration -> IEnvironment Value
initEnvironment = initDecl []
  where initDecl env (tag &&& children -> (DGlobal n t eO, ch)) = foldl initDecl (initGlobal env n t eO) ch
        initDecl env (tag &&& children -> (DRole r, ch))        = foldl initDecl env ch
        initDecl env _                                          = env

        -- FIXME: triggers are a different form of declaration than globals now
        initGlobal env n (tag -> TTrigger) _ = env ++ [(n, VTrigger (n, Nothing))]
        initGlobal env n (tag -> TSink) _      = env ++ [(n, VTrigger (n, Nothing))]
        initGlobal env n (tag -> TFunction) _  = env -- TODO: mutually recursive functions
        initGlobal env _ _ _                   = env

initState :: K3 Declaration -> IEngine -> IState
initState prog engine = (initEnvironment prog, engine)

initMessages :: IResult () -> IO (IResult Value)
initMessages = \case
    ((Right _, state), ilog)
      | Just (VFunction f) <- lookup "atInit" $ getEnv state -> runInterpretation state (f vunit)
      | otherwise                                            -> return ((unknownTrigger, state), ilog)
    ((Left err, state), ilog)                                -> return ((Left err, state), ilog)
  where unknownTrigger = Left $ RunTimeTypeError "Could not find atInit trigger"

initProgram :: K3 Declaration -> IEngine -> IO (IResult Value)
initProgram prog engine = (runInterpretation (initState prog engine) $ declaration prog) >>= initMessages

finalProgram :: IState -> IO (IResult Value)
finalProgram st = runInterpretation st $ maybe unknownTrigger runFinal $ lookup "atExit" $ getEnv st
  where runFinal (VFunction f) = f vunit
        runFinal _             = throwE $ RunTimeTypeError "Invalid atExit trigger"
        unknownTrigger         = throwE $ RunTimeTypeError "Could not find atExit trigger"

{- Standalone (i.e., single peer) evaluation -}

standaloneInterpreter :: (IEngine -> IO a) -> IO a
standaloneInterpreter f = simulationEngine [defaultAddress] valueWD >>= f

runExpression :: K3 Expression -> IO (Maybe Value)
runExpression e = standaloneInterpreter withEngine
  where withEngine engine = valueOfInterpretation ([], engine) (expression e)

runExpression_ :: K3 Expression -> IO ()
runExpression_ e = runExpression e >>= putStrLn . show

runProgramInitializer :: K3 Declaration -> IO ()
runProgramInitializer p = standaloneInterpreter (initProgram p) >>= putIResult

runProgram :: [Address] -> K3 Declaration -> IO ()
runProgram peers prog = simulationEngine peers valueWD >>= (\e -> runEngine valueProcessor e prog)

{- Message processing -}

valueProcessor :: MessageProcessor (K3 Declaration) Value (IResult Value) (IResult Value)
valueProcessor = MessageProcessor { initialize = initProgram, process = process, status = status, finalize = finalize }
  where
        status res   = either (\_ -> Left res) (\_ -> Right res) $ getResultVal res
        finalize res = either (\_ -> return res) (\_ -> finalProgram $ getResultState res) $ getResultVal res

        process (addr, n, args) r =
          maybe (return $ unknownTrigger r n) (runTrigger r n args) $ lookup n $ getEnv $ getResultState r

        runTrigger r n a (VTrigger (_, Just f)) = runInterpretation (getResultState r) $ f a
        runTrigger r n a (VTrigger _)           = return . iError r $ "Uninitialized trigger " ++ n
        runTrigger r n a _                      = return . tError r $ "Invalid trigger or sink value for " ++ n

        unknownTrigger r n = tError r $ "Unknown trigger " ++ n

        iError r = mkError r . RunTimeInterpretationError
        tError r = mkError r . RunTimeTypeError
        mkError ((_,st), ilog) v = ((Left v, st), ilog)

dispatchValueProcessor :: MessageProcessor (K3 Declaration) Value [(Address, IResult Value)] [(Address, IResult Value)]
dispatchValueProcessor = MessageProcessor {
    initialize = initialize,
    process = process,
    status = status,
    finalize = finalize
} where
    initialize program engine = sequence [initNode node program engine | node <- nodes engine]

    initNode node program engine = do
        iProgram <- initProgram program engine
        return (node, iProgram)

    process (addr, name, args) ps = fmap snd $ flip runDispatchT ps $ do
        dispatch addr (\s -> runTrigger' s name args)

    runTrigger' s n a = case lookup n $ getEnv $ getResultState s of
        Nothing -> return (Just (), unknownTrigger s n)
        Just ft -> runTrigger s n a ft

    runTrigger :: IResult Value -> Identifier -> Value -> Value -> IO (Maybe (), IResult Value)
    runTrigger r n a (VTrigger (_, Just f)) = fmap (Just (),) $ runInterpretation (getResultState r) $ f a
    runTrigger r n a (VTrigger _)           = fmap (Just (),) $ return . iError r $ "Uninitialized trigger " ++ n
    runTrigger r n a _                      = fmap (Just (),) $ return . tError r $ "Invalid trigger or sink value for " ++ n

    unknownTrigger r n = tError r $ "Unknown trigger " ++ n

    iError r = mkError r . RunTimeInterpretationError
    tError r = mkError r . RunTimeTypeError
    mkError ((_,st), ilog) v = ((Left v, st), ilog)

    status [] = Left []
    status is@(p:ps) = case sStatus p of
        Left _ -> Left is
        Right _ -> Right is

    sStatus (node, res) = either (const $ Left (node, res)) (const $ Right (node, res)) $ getResultVal res

    finalize = mapM sFinalize

    sFinalize (node, res) = do
        res' <- either (const $ return res) (const $ finalProgram $ getResultState res) $ getResultVal res
        return (node, res')

{- Wire descriptions -}

valueWD :: WireDesc Value
valueWD = WireDesc show read (const True) $ Delimiter "\n"

wireDesc :: String -> WireDesc Value
wireDesc "k3" = valueWD
wireDesc fmt  = error $ "Invalid format " ++ fmt

{- Misc helpers -}

-- | Subtree extraction
children :: Tree a -> Forest a
children = subForest

-- | Associative lists
addAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
addAssoc l a b = (a,b):l

removeAssoc :: Eq a => [(a,b)] -> a -> [(a,b)]
removeAssoc l a = filter ((a /=) . fst) l

replaceAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
replaceAssoc l a b = addAssoc (removeAssoc l a) a b

{- Pretty printing -}

prettyErrorEnv :: EnvOnError -> String
prettyErrorEnv (err, env) = intercalate "\n" ["Error", show err, prettyEnv env]

prettyEnv :: Show v => IEnvironment v -> String
prettyEnv env = intercalate "\n" $ ["Environment:"] ++ map show (reverse env)

putIResult :: Show a => IResult a -> IO ()
putIResult ((Left err, (env, eng)), ilog)  = putStrLn (prettyErrorEnv (err,env)) >> putEngine eng
putIResult ((Right val, (env, eng)), ilog) = putStr (concatMap (++"\n\n") [prettyEnv env, prettyVal]) >> putEngine eng
  where prettyVal = "Value:\n"++show val

