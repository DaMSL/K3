{-# LANGUAGE DoAndIfThenElse #-}
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

  runNetwork,
  runProgram,
  runProgramInitializer,

  showValueSyntax,
  readValueSyntax,

  valueWD,
  syntaxValueWD

) where

import Control.Arrow hiding ( (+++) )
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Function
import Data.IORef
import Data.List
import Data.Word (Word8)
import Debug.Trace

import qualified System.IO          as SIO (stdout, hFlush)
import Text.Read hiding (get, lift)
import qualified Text.Read          as TR (lift)
import Text.ParserCombinators.ReadP as P (skipSpaces)

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
    | VCollection  ([Value], Identifier)
    | VIndirection (IORef Value)
    | VFunction    (Value -> Interpretation Value)
    | VAddress     Address
    | VTrigger     (Identifier, Maybe (Value -> Interpretation Value))

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
type IState = (IEnvironment Value, AEnvironment Value)

-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IState (WriterT ILog (EngineM Value)))

-- | An evaluated value type, produced from running an interpretation.
type IResult a = ((Either InterpretationError a, IState), ILog)

-- | Pairing of errors and environments for debugging output.
type EnvOnError = (InterpretationError, IEnvironment Value)


{- Annotations -}

-- | Annotation environment, for lifted attributes. This contains two mappings:
--  i. annotation ids => lifted attribute ids, lifted attribute value
--  ii. combined annotation ids => combination namespace
-- 
--  The second mapping is used to store concrete annotation combinations used at
--  collection instances (once for all instances), and defines namespaces containing
--  bindings that are introduced to the interpretation environment when invoking members.
data AEnvironment v = AEnvironment {
        definitions  :: AnnotationDefinitions v,
        realizations :: AnnotationCombinations v
    }
    deriving (Read, Show)

type AnnotationDefinitions v  = [(Identifier, IEnvironment v)]
type AnnotationCombinations v = [(Identifier, CollectionNamespace v)]

-- | Two-level namespacing of collection constituents. Collections have two levels of named values:
--   i. global names, comprised of unambiguous annotation member names.
--   ii. annotation-specific names, comprised of overlapping named annotation members.
--   
-- TODO: for now, we assume names are unambiguous and keep everything as a global name.
-- Check with Zach on the typechecker status for annotation-specific names.
data CollectionNamespace v = CollectionNamespace {
        collectionNS :: IEnvironment v,
        annotationNS :: [(Identifier, IEnvironment v)]
    }
  deriving (Read, Show)

{- Instances -}
instance Pretty IState where
  prettyLines (vEnv, aEnv) =    ["Environment:"] ++ map show vEnv ++ ["Annotations:"] ++ (lines $ show aEnv)

-- TODO: error prettification
instance (Pretty a) => Pretty (IResult a) where
    prettyLines ((r, st), _) = ["Status: "] ++ either ((:[]) . show) prettyLines r ++ prettyLines st

instance (Pretty a) => Pretty [(Address, IResult a)] where
    prettyLines l = concatMap (\(x,y) -> prettyLines x ++ prettyLines y) l

{- State and result accessors -}

getEnv :: IState -> IEnvironment Value
getEnv (x,_) = x

getAnnotEnv :: IState -> AEnvironment Value
getAnnotEnv (_,x) = x

modifyStateEnv :: (IEnvironment Value -> IEnvironment Value) -> IState -> IState
modifyStateEnv f (x,y) = (f x, y)

modifyStateAEnv :: (AEnvironment Value -> AEnvironment Value) -> IState -> IState
modifyStateAEnv f (x,y) = (x, f y)

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

-- | Test if a variable is defined in the current interpretation environment.
elemE :: Identifier -> Interpretation Bool
elemE n = get >>= return . maybe False (const True) . find ((n == ) . fst) . getEnv

-- | Environment lookup, with a thrown error if unsuccessful.
lookupE :: Identifier -> Interpretation Value
lookupE n = get >>= maybe err return . lookup n . getEnv
  where err = throwE $ RunTimeTypeError $ "Unknown Variable: '" ++ n ++ "'"

lookupADef :: Identifier -> Interpretation (IEnvironment Value)
lookupADef n = get >>= maybe err return . lookup n . definitions . getAnnotEnv
  where err = throwE $ RunTimeTypeError $ "Unknown annotation definition: '" ++ n ++ "'"

lookupACombo :: Identifier -> Interpretation (CollectionNamespace Value)
lookupACombo n = tryLookupACombo n >>= maybe err return
  where err = throwE $ RunTimeTypeError $ "Unknown annotation combination: '" ++ n ++ "'"

tryLookupACombo :: Identifier -> Interpretation (Maybe (CollectionNamespace Value))
tryLookupACombo n = get >>= return . lookup n . realizations . getAnnotEnv

-- | Environment modification
modifyE :: (IEnvironment Value -> IEnvironment Value) -> Interpretation ()
modifyE f = modify $ modifyStateEnv f

-- | Annotation environment modification
modifyA :: (AEnvironment Value -> AEnvironment Value) -> Interpretation ()
modifyA f = modify $ modifyStateAEnv f

modifyADefs :: (AnnotationDefinitions Value -> AnnotationDefinitions Value) -> Interpretation ()
modifyADefs f = modifyA (\aEnv -> AEnvironment (f $ definitions aEnv) (realizations aEnv))

modifyACombos :: (AnnotationCombinations Value -> AnnotationCombinations Value) -> Interpretation ()
modifyACombos f = modifyA (\aEnv -> AEnvironment (definitions aEnv) (f $ realizations aEnv))

-- | Environment binding removal
removeE :: (Identifier, Value) -> a -> Interpretation a
removeE (n,v) r = modifyE (deleteBy ((==) `on` fst) (n,v)) >> return r

-- | Monadic message passing primitive for the interpreter.
sendE :: Address -> Identifier -> Value -> Interpretation ()
sendE addr n val = liftEngine $ send addr n val

{- Constants -}
vunit :: Value
vunit = VTuple []

emptyCollection :: Value
emptyCollection = VCollection ([], "")

emptyAnnotatedCollection :: Identifier -> Value
emptyAnnotatedCollection comboId = VCollection ([], comboId)

{- Identifiers -}
annotationSelfId :: Identifier
annotationSelfId = "self"
  -- This is a keyword in the language, thus no binding to it can exist beforehand.

annotationDataId :: Identifier
annotationDataId = "data"
  -- TODO: make this a keyword in the syntax.

annotationComboId :: [Identifier] -> Identifier
annotationComboId annIds = intercalate ";" annIds

annotationNamesT :: [Annotation Type] -> [Identifier]
annotationNamesT anns = map extractId $ filter isTAnnotation anns
  where extractId (TAnnotation n) = n
        extractId _ = error "Invalid named annotation"

annotationNamesE :: [Annotation Expression] -> [Identifier]
annotationNamesE anns = map extractId $ filter isEAnnotation anns
  where extractId (EAnnotation n) = n
        extractId _ = error "Invalid named annotation"

annotationComboIdT :: [Annotation Type] -> Maybe Identifier
annotationComboIdT (annotationNamesT -> [])  = Nothing
annotationComboIdT (annotationNamesT -> ids) = Just $ annotationComboId ids

annotationComboIdE :: [Annotation Expression] -> Maybe Identifier
annotationComboIdE (annotationNamesE -> [])  = Nothing
annotationComboIdE (annotationNamesE -> ids) = Just $ annotationComboId ids


{- Interpretation -}

-- | Default values for specific types
defaultValue :: K3 Type -> Interpretation Value
defaultValue (tag -> TBool)       = return $ VBool False
defaultValue (tag -> TByte)       = return $ VByte 0
defaultValue (tag -> TInt)        = return $ VInt 0
defaultValue (tag -> TReal)       = return $ VReal 0.0
defaultValue (tag -> TString)     = return $ VString ""
defaultValue (tag -> TOption)     = return $ VOption Nothing
defaultValue (tag -> TCollection) = return $ emptyCollection
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
constant (CEmpty t)  = (getComposedAnnotation $ annotations t) 
                        >>= return . maybe emptyCollection emptyAnnotatedCollection 

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
      VFunction b -> b x'
      _ -> throwE $ RunTimeTypeError $ "Invalid Function Application\n" ++ pretty f

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
  return $ VFunction $ \v -> modifyE ((i,v):) >> expression b >>= removeE (i,v)

-- | Interpretation of unary/binary operators.
expression (tag &&& children -> (EOperate otag, cs))
    | otag `elem` [ONeg, ONot], [a] <- cs = unary otag a
    | otherwise, [a, b] <- cs = binary otag a b
    | otherwise = undefined

-- | Interpretation of Record Projection.
expression (tag &&& children -> (EProject i, [r])) = expression r >>= \case
    VRecord vr -> maybe (unknownField i) return $ lookup i vr

    VCollection (v,cid) ->
      if null cid then unannotatedCollection else do
        cns <- lookupACombo cid 
        case lookup i $ collectionNS cns of
          Nothing            -> unknownCollectionMember i
          Just (VFunction f) -> wrapDataBinding f (VCollection (v,cid))
          Just v             -> return v

    _ -> throwE $ RunTimeTypeError "Invalid Record Projection"
  
  where 
        wrapDataBinding f v = 
          let dv = (annotationDataId, v)
          in return $ VFunction $ \x ->  modifyE (dv:) >> f x >>= removeE dv
          -- TODO: this copies the whole collection. Change a collection's
          -- implementation to be an IORef [Value], which also better
          -- represents the implementation of its data segment as a
          -- pointer to a chunk/page.

        unknownField i            = throwE $ RunTimeTypeError $ "Unknown record field " ++ i
        unannotatedCollection     = throwE $ RunTimeTypeError $ "Invalid projection on an unannotated collection"
        unknownCollectionMember i = throwE $ RunTimeTypeError $ "Unknown collection member " ++ i

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
      (modifyE . (:) $ (i, VIndirection r)) >> expression f >>= removeE (i, VIndirection r)
    
    (BTuple ts, VTuple vs) ->
      (modifyE . (++) $ zip ts vs) >> expression f >>= flip (foldM (flip removeE)) (zip ts vs)
    
    (BRecord ids, VRecord ivs) -> do
        let (idls, idbs) = unzip $ sortBy (compare `on` fst) ids
        let (ivls, ivvs) = unzip $ sortBy (compare `on` fst) ivs
        if idls == ivls
            then modifyE ((++) (zip idbs ivvs)) >> expression f >>= flip (foldM (flip removeE)) (zip idbs ivvs)
            else throwE $ RunTimeTypeError "Invalid Bind-Pattern"
    _ -> throwE $ RunTimeTypeError "Bind Mis-Match"
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

{- Declaration interpretation -}

replaceTrigger :: Identifier -> Value -> Interpretation()
replaceTrigger n (VFunction f) = modifyE (\env -> replaceAssoc env n (VTrigger (n, Just f)))
replaceTrigger _ _             = throwE $ RunTimeTypeError "Invalid trigger body"

global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> Interpretation ()
global n (tag -> TSink) (Just e)      = expression e >>= replaceTrigger n
global _ (tag -> TSink) Nothing       = throwE $ RunTimeInterpretationError "Invalid sink trigger"
global _ (tag -> TSource) _           = return ()
global n (tag -> TFunction) _         = return () -- Functions have already been initialized.

global n t@(tag -> TCollection) eOpt = elemE n >>= \case
  True  -> void . getComposedAnnotation $ annotations t
  False -> (getComposedAnnotation $ annotations t) >>= initializeCollection eOpt . maybe "" id
  where
    initializeCollection eOpt comboId = case eOpt of
      Nothing -> modifyE ((n, emptyAnnotatedCollection comboId):)
      Just e  -> expression e >>= verifyInitialCollection comboId

    verifyInitialCollection comboId = \case
      VCollection (v, initId) -> if comboId /= initId then collInitError
                                 else modifyE ((n, VCollection (v, initId)):)
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
declaration (tag -> DAnnotation n members)      = annotation n members

declaration _ = undefined

{- Annotations -}

getComposedAnnotation :: [Annotation Type] -> Interpretation (Maybe Identifier)
getComposedAnnotation anns = case annotationComboIdT anns of
  Nothing      -> return Nothing
  Just comboId -> tryLookupACombo comboId 
                    >>= (\cOpt -> initializeComposition comboId anns cOpt >> return (Just comboId))
  where
    initializeComposition comboId anns = \case
      Nothing -> mapM (\x -> lookupADef x >>= return . (x,)) (annotationNamesT anns)
                  >>= (trace ("Adding annotation combo " ++ comboId) $ 
                        modifyACombos . (:) . (comboId,) . mkNamespace comboId)
      Just _  -> return ()

    mkNamespace comboId namedAnnDefs =
      let defsWithBindings = map (fmap $ map $ extendBindings comboId) namedAnnDefs
          global           = concatMap snd defsWithBindings
      in CollectionNamespace global defsWithBindings

    extendBindings comboId (n, VFunction f) = (n,) $ VFunction $ \x -> do
        ns <- lookupACombo comboId
        bindings <- return $ collectionNS ns ++ [(annotationSelfId, VRecord $ collectionNS ns)]
        void $ modifyE (bindings ++) 
        result <- f x
        foldM (flip removeE) result bindings

    extendBindings _ nv = nv


annotationMember :: Identifier -> Bool -> (K3 Expression -> Bool) -> AnnMemDecl 
                    -> Interpretation (Maybe (Identifier, Value))
annotationMember annId matchLifted matchF annMem = case (matchLifted, annMem) of
  (True,  Lifted Provides n t (Just e) uid)    -> interpretExpr n e
  (False, Attribute Provides n t (Just e) uid) -> interpretExpr n e
  (True,  Lifted Provides n t Nothing  uid)    -> builtinLiftedAttribute annId n t uid
  (False, Attribute Provides n t Nothing  uid) -> builtinAttribute annId n t uid 
  _ -> return Nothing
  where interpretExpr n e = if matchF e then expression e >>= return . Just . (n,) else return Nothing

annotation :: Identifier -> [AnnMemDecl] -> Interpretation ()
annotation n memberDecls = do
  bindings <- foldM initializeMembers [] [liftedAttrFuns, liftedAttrs, attrFuns, attrs]
  void $ foldM (flip removeE) () bindings
  trace ("Adding annotation definition " ++ n) $ void $ modifyADefs $ (:) (n,bindings)

  where initializeMembers initEnv spec = foldM (memberWithBindings spec) initEnv memberDecls

        memberWithBindings (isLifted, matchF) accEnv mem =
          annotationMember n isLifted matchF mem 
            >>= maybe (return accEnv) (\nv -> modifyE (nv:) >> return (nv:accEnv))

        (liftedAttrFuns, liftedAttrs) = ((True, matchFunction), (True, not . matchFunction))
        (attrFuns, attrs)             = ((False, matchFunction), (False, not . matchFunction))

        matchFunction (tag -> ELambda _) = True
        matchFunction _ = False


{- Built-in functions -}

ignoreFn :: Value -> Value
ignoreFn = VFunction . const . return

vfun :: (Value -> Interpretation Value) -> Interpretation Value
vfun = return . VFunction

builtin :: Identifier -> K3 Type -> Interpretation ()
builtin n t = genBuiltin n t >>= modifyE . (:) . (n,)

genBuiltin :: Identifier -> K3 Type -> Interpretation Value

-- parseArgs :: () -> ([String], [(String, String)])
genBuiltin "parseArgs" _ =
  return $ ignoreFn $ VTuple [emptyCollection, emptyCollection]


-- TODO: error handling on all open/close/read/write methods.
-- TODO: argument for initial endpoint bindings for open method as a list of triggers
-- TODO: correct element type (rather than function type sig) for openFile / openSocket

-- type ChannelId = String

-- openBuilting :: ChannelId -> String -> ()
genBuiltin "openBuiltin" _ =
  vfun $ \(VString cid) -> 
    vfun $ \(VString builtinId) ->
      vfun $ \(VString format) ->
        liftEngine (openBuiltin cid builtinId (wireDesc format)) >> return vunit

-- openFile :: ChannelId -> String -> String -> String -> ()
genBuiltin "openFile" t =
  vfun $ \(VString cid) ->
    vfun $ \(VString path) ->
      vfun $ \(VString format) ->
        vfun $ \(VString mode) ->
          liftEngine (openFile cid path (wireDesc format) (Just t) mode) >> return vunit

-- openSocket :: ChannelId -> Address -> String -> String -> ()
genBuiltin "openSocket" t =
  vfun $ \(VString cid) ->
    vfun $ \(VAddress addr) ->
      vfun $ \(VString format) ->
        vfun $ \(VString mode) ->
          liftEngine (openSocket cid addr (wireDesc format) (Just t) mode) >> return vunit

-- close :: ChannelId -> ()
genBuiltin "close" _ = vfun $ \(VString cid) -> liftEngine (close cid) >> return vunit

-- TODO: deregister methods
-- register*Trigger :: ChannelId -> TTrigger () -> ()
genBuiltin "registerFileDataTrigger"     _ = registerNotifier "data"
genBuiltin "registerFileCloseTrigger"    _ = registerNotifier "close"

genBuiltin "registerSocketAcceptTrigger" _ = registerNotifier "accept"
genBuiltin "registerSocketDataTrigger"   _ = registerNotifier "data"
genBuiltin "registerSocketCloseTrigger"  _ = registerNotifier "close"

-- <source>HasRead :: () -> Bool
genBuiltin (channelMethod -> ("HasRead", Just n)) _ = vfun $ \_ -> checkChannel
  where checkChannel = liftEngine (hasRead n) >>= maybe invalid (return . VBool)
        invalid = throwE $ RunTimeInterpretationError $ "Invalid source \"" ++ n ++ "\""

-- <source>Read :: () -> t
genBuiltin (channelMethod -> ("Read", Just n)) _ = vfun $ \_ -> liftEngine (doRead n) >>= throwOnError
  where throwOnError (Just v) = return v
        throwOnError Nothing =
          throwE $ RunTimeInterpretationError $ "Invalid next value from source \"" ++ n ++ "\""

-- <sink>HasWrite :: () -> Bool
genBuiltin (channelMethod -> ("HasWrite", Just n)) _ = vfun $ \_ -> checkChannel
  where checkChannel = liftEngine (hasWrite n) >>= maybe invalid (return . VBool)
        invalid = throwE $ RunTimeInterpretationError $ "Invalid sink \"" ++ n ++ "\""

-- <sink>Write :: t -> ()
genBuiltin (channelMethod -> ("Write", Just n)) _ =
  vfun $ \arg -> liftEngine (doWrite n arg) >> return vunit

genBuiltin n _ = throwE $ RunTimeTypeError $ "Invalid builtin \"" ++ n ++ "\""

channelMethod :: String -> (String, Maybe String)
channelMethod x =
  case find (flip isSuffixOf x) ["HasRead", "Read", "HasWrite", "Write"] of
    Just y -> (y, stripSuffix y x)
    Nothing -> (x, Nothing)
  where stripSuffix sfx lst = maybe Nothing (Just . reverse) $ stripPrefix (reverse sfx) (reverse lst)

registerNotifier :: Identifier -> Interpretation Value
registerNotifier n =
  vfun $ \cid -> vfun $ \target -> attach cid n target >> return vunit

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
builtinLiftedAttribute annId n t uid 
  | annId == "Collection" && n == "add"     = return $ Just (n, addFn)
  | annId == "Collection" && n == "combine" = return $ Just (n, combineFn)
  | annId == "Collection" && n == "ext"     = return $ Just (n, extFn)
  | annId == "Collection" && n == "fold"    = return $ Just (n, foldFn)
  | annId == "Collection" && n == "split"   = return $ Just (n, splitFn)
  | otherwise = providesError "lifted attribute" n

  where addFn = valWithCollection $ \el v compId -> return $ VCollection (v++[el], compId)

        combineFn = valWithCollection $ \other v compId -> 
          flip (matchCollection collectionError) other (\v' compId' -> 
            if compId == compId' then return $ VCollection(v++v', compId) else combineError)

        splitFn = valWithCollection (\_ (splitImpl -> (l,r)) compId ->
                    return $ VTuple [VCollection(l, compId), VCollection(r, compId)])
        
        foldFn = VFunction $ \f -> flip (matchFunction foldFnError) f $
          \f' -> return $ valWithCollection (\accInit v _ -> foldM (curryFoldFn f') accInit v)

        curryFoldFn :: (Value -> Interpretation Value) -> Value -> Value -> Interpretation Value
        curryFoldFn f' acc v = do
          f'' <- f' acc
          (matchFunction curryFnError) ($ v) f''

        extFn = valWithCollection $ \f v compId  -> 
          case f of 
            VFunction f' -> do
              vals <- mapM f' v
              case vals of 
                [] -> return $ VCollection ([], compId)
                _  -> maybe combineError return $ flip foldl1 (map Just vals) combine'
            _ -> extError

        combine' Nothing _ = Nothing
        combine' _ Nothing = Nothing
        
        combine' (Just acc) (Just cv) =
          flip (matchCollection Nothing) acc $ \v1 cid1 -> 
          flip (matchCollection Nothing) cv  $ \v2 cid2 -> 
          if cid1 == cid2 then Just $ VCollection(v1++v2, cid1) else Nothing

        valWithCollection :: (Value -> [Value] -> Identifier -> Interpretation Value) -> Value
        valWithCollection f =
          VFunction $ \arg -> lookupE annotationDataId >>= matchCollection collectionError (f arg)

        matchCollection :: a -> ([Value] -> Identifier -> a) -> Value -> a
        matchCollection err f =
          \case
            VCollection (v, compId) -> f v compId
            _ -> err

        matchFunction :: a -> ((Value -> Interpretation Value) -> a) -> Value -> a
        matchFunction err f = 
          \case 
            VFunction f' -> f f'
            _ -> err

        splitImpl l = if length l <= threshold then (l, []) else splitAt (length l `div` 2) l
        threshold = 10

        extError        = throwE $ RunTimeTypeError "Invalid function argument for ext"
        foldFnError     = throwE $ RunTimeTypeError "Invalid fold function"
        curryFnError    = throwE $ RunTimeTypeError "Invalid curried function"
        combineError    = throwE $ RunTimeTypeError "Mismatched collection types for combine"
        collectionError = throwE $ RunTimeTypeError "Invalid collection"


builtinAttribute :: Identifier -> Identifier -> K3 Type -> UID
                    -> Interpretation (Maybe (Identifier, Value))
builtinAttribute annId n t uid = providesError "attribute" n

{- Program initialization methods -}

initEnvironment :: PeerBootstrap -> K3 Declaration -> IState -> EngineM Value IState
initEnvironment bootstrap decl st =
  foldM (\st -> uncurry $ initializeExpr st) st bootstrap >>= flip initDecl decl
  where 
    initDecl st (tag &&& children -> (DGlobal n t eOpt, ch)) = initGlobal st n t eOpt >>= flip (foldM initDecl) ch
    initDecl st (tag &&& children -> (DTrigger n _ _, ch))   = initTrigger st n >>= flip (foldM initDecl) ch
    initDecl st (tag &&& children -> (DRole _, ch))          = foldM initDecl st ch
    initDecl st _                                            = return st

    initGlobal st n (tag -> TSink) _          = initTrigger st n
    initGlobal st n t@(tag -> TFunction) eOpt = initFunction st n t eOpt
    initGlobal st _ _ _                       = return st

    initTrigger st n = return $ modifyStateEnv ((:) $ (n, VTrigger (n, Nothing))) st
        
    -- Functions create lambda expressions, thus they are safe to initialize during
    -- environment construction. This way, all global functions can be mutually recursive.
    -- Note that since we only initialize functions, no declaration can force function
    -- evaluation during initialization (e.g., as would be the case with variable initializers).
    initFunction st n t (Just e) = initializeExpr st n e
    initFunction st n t Nothing  = initializeBinding st (builtin n t)

    initializeExpr st n e = initializeBinding st (expression e >>= modifyE . (:) . (n,))
    initializeBinding st interp = runInterpretation' st interp >>= return . getResultState

initState :: PeerBootstrap -> K3 Declaration -> EngineM Value IState
initState bootstrap prog = initEnvironment bootstrap prog ([], AEnvironment [] [])

initMessages :: IResult () -> EngineM Value (IResult Value)
initMessages = \case
    ((Right _, s), ilog)
      | Just (VFunction f) <- lookup "atInit" $ getEnv s -> runInterpretation' s (f vunit)
      | otherwise                                        -> return ((unknownTrigger, s), ilog)
    ((Left err, s), ilog)                                -> return ((Left err, s), ilog)
  where unknownTrigger = Left $ RunTimeTypeError "Could not find atInit trigger"

initProgram :: PeerBootstrap -> K3 Declaration -> EngineM Value (IResult Value)
initProgram bootstrap prog =
  initState bootstrap prog >>= flip runInterpretation' (declaration prog) >>= initMessages

finalProgram :: IState -> EngineM Value (IResult Value)
finalProgram st = runInterpretation' st $ maybe unknownTrigger runFinal $ lookup "atExit" $ getEnv st
  where runFinal (VFunction f) = f vunit
        runFinal _             = throwE $ RunTimeTypeError "Invalid atExit trigger"
        unknownTrigger         = throwE $ RunTimeTypeError "Could not find atExit trigger"

{- Standalone (i.e., single peer) evaluation -}

standaloneInterpreter :: (IEngine -> IO a) -> IO a
standaloneInterpreter f = simulationEngine defaultSystem syntaxValueWD >>= f

runExpression :: K3 Expression -> IO (Maybe Value)
runExpression e = standaloneInterpreter (withEngine')
  where
    withEngine' engine = flip runEngineM engine (valueOfInterpretation ([], AEnvironment [] []) (expression e))
        >>= return . either (const Nothing) id

runExpression_ :: K3 Expression -> IO ()
runExpression_ e = runExpression e >>= putStrLn . show

runProgramInitializer :: PeerBootstrap -> K3 Declaration -> IO ()
runProgramInitializer bootstrap p = standaloneInterpreter (initProgram bootstrap p) >>= putIResult


{- Distributed program execution -}

-- | Single-machine system simulation.
runProgram :: SystemEnvironment -> K3 Declaration -> IO ()
runProgram systemEnv prog =
  simulationEngine systemEnv syntaxValueWD >>= (\e -> runEngine virtualizedProcessor systemEnv e prog)

-- | Single-machine network deployment.
--   Takes a system deployment and forks a network engine for each peer.
runNetwork :: SystemEnvironment -> K3 Declaration -> IO [(Address, IEngine, ThreadId)]
runNetwork systemEnv prog =
  let nodeBootstraps = map (:[]) systemEnv in do
    engines       <- mapM (flip networkEngine syntaxValueWD) nodeBootstraps
    namedEngines  <- return . map pairWithAddress $ zip engines nodeBootstraps
    engineThreads <- mapM fork namedEngines
    return engineThreads
  where
    pairWithAddress (engine, bootstrap) = (fst . head $ bootstrap, engine, bootstrap)
    fork (addr, engine, bootstrap) = forkEngine virtualizedProcessor bootstrap engine prog >>= return . (addr, engine,)


{- Message processing -}

runTrigger :: IResult Value -> Identifier -> Value -> Value -> EngineM Value (IResult Value)
runTrigger r n a = \case
    (VTrigger (_, Just f)) -> do
        liftIO $ putStrLn ("Running trigger " ++ n)
        liftIO $ SIO.hFlush SIO.stdout
        r <- runInterpretation' (getResultState r) (f a)
        liftIO $ putStrLn ("... done")
        return r

    (VTrigger _) -> return . iError r $ "Uninitialized trigger " ++ n
    _ -> return . tError r $ "Invalid trigger or sink value for " ++ n

  where iError r = mkError r . RunTimeInterpretationError
        tError r = mkError r . RunTimeTypeError
        mkError ((_,st), ilog) v = ((Left v, st), ilog)

uniProcessor :: MessageProcessor SystemEnvironment (K3 Declaration) Value (IResult Value) (IResult Value)
uniProcessor = MessageProcessor {
    initialize = initUP,
    process = processUP,
    status = statusUP,
    finalize = finalizeUP
} where
    initUP [] prog = initProgram [] prog
    initUP ((_,is):_) prog = initProgram is prog

    statusUP res   = either (\_ -> Left res) (\_ -> Right res) $ getResultVal res
    finalizeUP res = either (\_ -> return res) (\_ -> finalProgram $ getResultState res) $ getResultVal res

    processUP (_, n, args) r = maybe (return $ unknownTrigger r n) (run r n args) $
        lookup n $ getEnv $ getResultState r

    run r n args trig = debugDispatch defaultAddress n args r >> runTrigger r n args trig

    unknownTrigger ((_,st), ilog) n = ((Left . RunTimeTypeError $ "Unknown trigger " ++ n, st), ilog)

virtualizedProcessor :: MessageProcessor SystemEnvironment (K3 Declaration) Value [(Address, IResult Value)] [(Address, IResult Value)]
virtualizedProcessor = MessageProcessor {
    initialize = initializeVP,
    process = processVP,
    status = statusVP,
    finalize = finalizeVP
} where
    initializeVP systemEnv program engine =
      sequence [initNode node systemEnv program engine | node <- nodes engine]

    initNode node systemEnv program engine = do
        initEnv <- return $ maybe [] id $ lookup node systemEnv
        iProgram <- initProgram initEnv program engine
        --
        debugResult node iProgram
        --
        return (node, iProgram)

    processVP (addr, name, args) ps = fmap snd $ flip runDispatchT ps $ do
        dispatch addr (\s -> debugDispatch addr name args s >> runTrigger' s name args)

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

{- Wire descriptions -}

valueWD :: WireDesc Value
valueWD = WireDesc show (Just . read) $ Delimiter "\n"

syntaxValueWD :: WireDesc Value
syntaxValueWD = WireDesc showValueSyntax (Just . readValueSyntax) $ Delimiter "\n"

wireDesc :: String -> WireDesc Value
wireDesc "k3" = syntaxValueWD
wireDesc fmt  = error $ "Invalid format " ++ fmt

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
putIResult ((Left err, st), _)  =
  putStrLn (prettyErrorEnv (err, getEnv st)) >> putEngine (getEngine st)

putIResult ((Right val, st), _) = 
     putStr (concatMap (++"\n\n") [prettyEnv $ getEnv st, prettyVal])
  >> putEngine (getEngine st)
  where prettyVal = "Value:\n"++show val


{- Debugging helpers -}
debugDecl :: (Show a, Pretty b) => a -> b -> c -> c
debugDecl n t = trace (concat ["Adding ", show n, " : ", pretty t])

debugResult :: Address -> IResult Value -> IO ()
debugResult addr r = do
  void $ putStrLn $ "=========== " ++ show addr
  void $ print r
  void $ putStrLn "============"

debugDispatch :: Address -> Identifier -> Value -> IResult Value -> IO ()
debugDispatch addr name args r = do
  void $ putStrLn ("Processing " ++ show addr ++ " " ++ name)
  void $ putStrLn "Args:"
  void $ print args
  void $ debugResult addr r
  void $ putStrLn "=========="

{- Show/print methods -}
showsPrecTag :: Show a => String -> Int -> a -> ShowS
showsPrecTag s d v =
  showParen (d > app_prec) $ showString (s++" ") . showsPrec (app_prec+1) v
  where app_prec = 10

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
  showsPrec d (VCollection vn) = showsPrecTag "VCollection" d vn
  showsPrec d (VAddress v)     = showsPrecTag "VAddress" d v
  
  showsPrec d (VIndirection _) =
    showParen (d > app_prec) $ showString "VIndirection " . showString "<opaque>"
    where app_prec = 10

  showsPrec d (VFunction _) =
    showParen (d > app_prec) $ showString "VFunction " . showString "<function>"
    where app_prec = 10

  showsPrec d (VTrigger (n, Nothing)) =
    showParen (d > app_prec) $ showString "VTrigger " . showString n . showString "<uninitialized>"
    where app_prec = 10

  showsPrec d (VTrigger (n, Just _)) =
    showParen (d > app_prec) $ showString "VTrigger " . showString n . showString "<function>"
    where app_prec = 10

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
          vn <- step readPrec
          return (VCollection vn))

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
showValueSyntax :: Value -> String
showValueSyntax v = showsValuePrec 0 v ""
  where
    showsValuePrec d = \case
      VBool v        -> showsPrec d v
      VByte v        -> showChar 'B' . showParen True (showsPrec appPrec1 v)
      VInt v         -> showsPrec d v
      VReal v        -> showsPrec d v
      VString v      -> showsPrec d v
      VOption vOpt   -> maybe (\s -> "Nothing"++s) 
                              (\v -> showParen (d > appPrec) $ ("Just " ++) . showsValuePrec appPrec1 v) vOpt
      VTuple v       -> parens (showsValuePrec $ d+1) v
      VRecord v      -> showsNamedValues (d+1) v
      
      VCollection vn -> showsCollectionPrec (d+1) vn
      --DEPRECATED: VCollection v  -> showsCollectionPrec d v
      
      VIndirection _ -> error "Cannot show indirection"
      VFunction _    -> error "Cannot show function"
      VAddress v     -> showsPrec d v
      VTrigger _     -> error "Cannot show trigger"

    parens   = showCustomList "(" ")" ","
    braces   = showCustomList "{" "}" ","
    brackets = showCustomList "[" "]" ","

    showsCollectionPrec d (v,n) =
      brackets (showsValuePrec $ d+1) v . showChar ',' . showString n

    {- DEPRECATED
    showsCollectionPrec d (CollectionBody (CollectionNamespace cns ans) v) =
      wrap "{" "}" $ showsCollectionNamespace (d+1) (cns, ans) . showsCollectionDataspace (d+1) v

    showsCollectionNamespace d (cns, ans) =
      showString "CNS=" . showsNamedValues d cns . showString "ANS=" . braces (showsDoublyNamedValues d) ans
    
    showsCollectionDataspace d v     = showString "DS=" . brackets (showsValuePrec d) v
    showsDoublyNamedValues d (n, nv) = showString (n ++ "=") . showsNamedValues d nv
    -}

    showsNamedValues d nv       = braces (showsNamedValuePrec d) nv
    showsNamedValuePrec d (n,v) = showString n . showChar '=' . showsValuePrec d v

    showCustomList :: String -> String -> String -> (a -> ShowS) -> [a] -> ShowS
    showCustomList lWrap rWrap _ _     []     s   = lWrap ++ rWrap ++ s
    showCustomList lWrap rWrap sep showF (x:xs) s = lWrap ++ showF x (showl xs)
      where showl []     = rWrap ++ s
            showl (y:ys) = sep ++ showF y (showl ys)

    wrap lWrap rWrap showx = showString lWrap . showx . showString rWrap

    appPrec  = 10
    appPrec1 = 11  

readValueSyntax :: String -> Value
readValueSyntax = readSingleParse readValuePrec
  where
    readValuePrec = parens $ 
          (readPrec >>= return . VInt)
      <++ ((readPrec >>= return . VBool)
      +++ (readPrec >>= return . VReal)
      +++ (readPrec >>= return . VString)

      +++ (do
            Ident "B" <- lexP
            v <- prec appPrec1 readPrec
            return $ VByte v)

      +++ (do
            Ident "Just" <- lexP
            v <- prec appPrec1 readValuePrec
            return . VOption $ Just v)

      +++ (do
            Ident "Nothing" <- lexP
            return $ VOption Nothing)

      +++ (prec appPrec $ do
            v <- step $ readParens readValuePrec
            return $ VTuple v)

      +++ (do
            nv <- readNamedValues readValuePrec
            return $ VRecord nv)

      +++ readCollectionPrec
      
      +++ (readPrec >>= return . VAddress))

    readCollectionPrec = parens $ 
      (prec appPrec $ do
        v        <- step $ readBrackets readValuePrec
        Char ',' <- lexP
        Ident n  <- lexP
        return $ VCollection (v,n))

    {- DEPRECATED
    readCollectionPrec = parens $
      (prec appPrec $ do
        Punc "{" <- lexP
        ns       <- step $ readCollectionNamespace
        ds       <- step $ readCollectionDataspace
        Punc "}" <- lexP
        return $ VCollection $ CollectionBody ns ds)

    readCollectionNamespace = parens $
      (prec appPrec $ do
        void $ readExpectedName "CNS"
        cns <- readNamedValues readValuePrec
        void $ readExpectedName "ANS"
        ans <- readDoublyNamedValues
        return $ CollectionNamespace cns ans)

    readCollectionDataspace = parens $
      (prec appPrec $ do
        void $ readExpectedName "DS"
        readBrackets readValuePrec)

    readDoublyNamedValues = parens $
      (prec appPrec $ readBraces $ readNamedPrec $ readNamedValues readValuePrec)
    -}

    readNamedValues :: ReadPrec a -> ReadPrec [(String, a)]
    readNamedValues readF = parens $
      (prec appPrec $ readBraces $ readNamedPrec readF)

    readNamedPrec :: ReadPrec a -> ReadPrec (String, a)
    readNamedPrec readF = parens $ 
      (prec appPrec $ do
        n <- readName 
        v <- step readF
        return (n,v))

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
    
    readSingleParse :: Show a => ReadPrec a -> String -> a
    readSingleParse readP s =
      case [ x | (x,"") <- readPrec_to_S read' minPrec s ] of
        [x] -> x
        []  -> error "Prelude.read: no parse"
        l   -> error $ "Prelude.read: ambiguous parse " ++ (concatMap ((++" ") . show) l)
      where read' = do
              x <- readP
              TR.lift P.skipSpaces
              return x

    readCustomList :: String -> String -> String -> ReadPrec a -> ReadPrec [a]
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

