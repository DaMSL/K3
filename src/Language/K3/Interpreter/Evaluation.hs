{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter.Evaluation where

import Control.Applicative
import Control.Arrow hiding ( (+++) )
import Control.Concurrent.MVar
import Control.Monad.Reader
import Control.Monad.State

import Data.Fixed
import Data.List
import Data.Maybe
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Values
import Language.K3.Interpreter.Collection
import Language.K3.Interpreter.Utils
import Language.K3.Interpreter.Builtins

import Language.K3.Runtime.Engine

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)


-- | Monadic message passing primitive for the interpreter.
sendE :: Address -> Identifier -> Value -> Interpretation ()
sendE addr n val = liftEngine $ send addr n val


{- Interpretation -}

-- | Default values for specific types
defaultValue :: K3 Type -> Interpretation Value
defaultValue (tag -> TBool)       = return $ VBool False
defaultValue (tag -> TByte)       = return $ VByte 0
defaultValue (tag -> TInt)        = return $ VInt 0
defaultValue (tag -> TReal)       = return $ VReal 0.0
defaultValue (tag -> TString)     = return $ VString ""
defaultValue t@(tag -> TOption)   = return $ VOption (Nothing, vQualOfType t)
defaultValue (tag -> TAddress)    = return $ VAddress defaultAddress

defaultValue (tag &&& children -> (TIndirection, [x])) =
  defaultValue x >>= (\y -> (\i tg -> (i, onQualifiedType x MemImmut MemMut, tg))
    <$> liftIO (newMVar y) <*> memEntTag y) >>= return . VIndirection

defaultValue (tag &&& children -> (TTuple, ch)) =
  mapM (\ct -> defaultValue ct >>= return . (, vQualOfType ct)) ch >>= return . VTuple

defaultValue (tag &&& children -> (TRecord ids, ch)) =
  mapM (\ct -> defaultValue ct >>= return . (, vQualOfType ct)) ch >>= return . VRecord . membersFromList . zip ids

defaultValue (tag &&& annotations -> (TCollection, anns)) =
  (getComposedAnnotationT anns) >>= maybe (emptyCollection annIds) emptyAnnotatedCollection
  where annIds = namedTAnnotations anns

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
constant (CBool b)   _   = return $ VBool b
constant (CByte w)   _   = return $ VByte w
constant (CInt i)    _   = return $ VInt i
constant (CReal r)   _   = return $ VReal r
constant (CString s) _   = return $ VString s
constant (CNone _)  anns = return $ VOption (Nothing, vQualOfAnnsE anns)
constant (CEmpty _) anns =
  (getComposedAnnotationE anns) >>= maybe (emptyCollection annIds) emptyAnnotatedCollection
  where annIds = namedEAnnotations anns

numericOp :: (Word8 -> Word8 -> Word8)
          -> (Int -> Int -> Int)
          -> (Double -> Double -> Double)
          -> Interpretation Value -> Value -> Value -> Interpretation Value
numericOp byteOpF intOpF realOpF err a b =
  case (a, b) of
    (VByte x, VByte y) -> return . VByte $ byteOpF x y
    (VByte x, VInt y)  -> return . VInt  $ intOpF (fromIntegral x) y
    (VByte x, VReal y) -> return . VReal $ realOpF (fromIntegral x) y
    (VInt x,  VByte y) -> return . VInt  $ intOpF x (fromIntegral y)
    (VInt x,  VInt y)  -> return . VInt  $ intOpF x y
    (VInt x,  VReal y) -> return . VReal $ realOpF (fromIntegral x) y
    (VReal x, VByte y) -> return . VReal $ realOpF x (fromIntegral y)
    (VReal x, VInt y)  -> return . VReal $ realOpF x (fromIntegral y)
    (VReal x, VReal y) -> return . VReal $ realOpF x y
    _                  -> err


-- | Common Numeric-Operation handling, with casing for int/real promotion.
numeric :: (forall a. Num a => a -> a -> a)
        -> K3 Expression -> K3 Expression -> Interpretation Value
numeric op a b = do
    a' <- expression a
    b' <- expression b
    numericOp op op op err a' b'

  where err = throwE $ RunTimeTypeError "Arithmetic Type Mis-Match"

-- | Similar to numeric above, except disallow a zero value for the second argument.
numericExceptZero :: (Word8 -> Word8 -> Word8)
                  -> (Int -> Int -> Int)
                  -> (Double -> Double -> Double)
                  -> K3 Expression -> K3 Expression -> Interpretation Value
numericExceptZero byteOpF intOpF realOpF a b = do
    a' <- expression a
    b' <- expression b

    void $ case b' of
        VByte 0 -> throwE $ RunTimeInterpretationError "Zero denominator"
        VInt  0 -> throwE $ RunTimeInterpretationError "Zero denominator"
        VReal 0 -> throwE $ RunTimeInterpretationError "Zero denominator"
        _       -> return ()

    numericOp byteOpF intOpF realOpF err a' b'

  where err = throwE $ RunTimeTypeError "Arithmetic Type Mis-Match"


-- | Common boolean operation handling.
logic :: (Bool -> Bool -> Bool) -> K3 Expression -> K3 Expression -> Interpretation Value
logic op a b = do
  a' <- expression a
  b' <- expression b

  case (a', b') of
      (VBool x, VBool y) -> return $ VBool $ op x y
      _ -> throwE $ RunTimeTypeError "Invalid Boolean Operation"

-- | Common comparison operation handling.
comparison :: (Value -> Value -> Interpretation Value)
           -> K3 Expression -> K3 Expression -> Interpretation Value
comparison op a b = do
  a' <- expression a
  b' <- expression b
  op a' b'

-- | Common string operation handling.
textual :: (String -> String -> String)
        -> K3 Expression -> K3 Expression -> Interpretation Value
textual op a b = do
  a' <- expression a
  b' <- expression b
  case (a', b') of
    (VString s1, VString s2) -> return . VString $ op s1 s2
    _ -> throwE $ RunTimeTypeError "Invalid String Operation"

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

-- | Division and modulo handled similarly, but accounting zero-division errors.
binary ODiv = numericExceptZero div div (/)
binary OMod = numericExceptZero mod mod mod'

-- | Logical operators
binary OAnd = logic (&&)
binary OOr  = logic (||)

-- | Comparison operators
binary OEqu = comparison valueEq
binary ONeq = comparison valueNeq
binary OLth = comparison valueLt
binary OLeq = comparison valueLte
binary OGth = comparison valueGt
binary OGeq = comparison valueGte

-- | String operators
binary OConcat = textual (++)

-- | Function Application
binary OApp = \f x -> do
  f' <- expression f
  x' <- expression x >>= freshenValue

  case f' of
      VFunction (b, cl, _) -> withClosure cl $ b x'
      _ -> throwE $ RunTimeTypeError $ "Invalid Function Application on:\n" ++ pretty f

  where withClosure cl doApp = mergeE cl >> doApp >>= \r -> pruneE cl >> freshenValue r

-- | Message Passing
binary OSnd = \target x -> do
  target'  <- expression target
  x'       <- expression x

  case target' of
    VTuple [(VTrigger (n, _, _), _), (VAddress addr, _)] -> sendE addr n x' >> return vunit
    _ -> throwE $ RunTimeTypeError "Invalid Trigger Target"

-- | Sequential expressions
binary OSeq = \e1 e2 -> expression e1 >> expression e2

binary _ = \_ _ -> throwE $ RunTimeInterpretationError "Invalid binary operation"

-- | Interpretation of Expressions
expression :: K3 Expression -> Interpretation Value
expression e_ = traceExpression $ do
    result <- expr e_
    void $ buildProxyPath e_
    return result

  where
    traceExpression :: Interpretation a -> Interpretation a
    traceExpression m = do
      let suOpt = spanUid (annotations e_)
      pushed <- maybe (return False) (\su -> pushTraceUID su >> return True) suOpt
      result <- m
      void $ if pushed then popTraceUID else return ()
      case suOpt of
        Nothing -> return ()
        Just (_, uid) -> do
            (watched, wvars) <- (,) <$> isWatchedExpression uid <*> getWatchedVariables uid
            void $ if watched then logIStateMI else return ()
            void $ mapM_ (prettyWatchedVar $ maximum $ map length wvars) wvars
      return result

    prettyWatchedVar :: Int -> Identifier -> Interpretation ()
    prettyWatchedVar w i =
      lookupE i >>= liftEngine . prettyIEnvEntry defaultPrintConfig
                >>= liftIO . putStrLn . ((i ++ replicate (max (w - length i) 0) ' '  ++ " => ") ++)

    -- TODO: dataspace bind aliases
    buildProxyPath :: K3 Expression -> Interpretation ()
    buildProxyPath e =
      case e @~ isBindAliasAnnotation of
        Just (EAnalysis (BindAlias i))          -> appendAlias (Named i)
        Just (EAnalysis (BindFreshAlias i))     -> appendAlias (Temporary i)
        Just (EAnalysis (BindAliasExtension i)) -> appendAliasExtension i
        Nothing  -> return ()
        Just _   -> throwE $ RunTimeInterpretationError "Invalid bind alias annotation matching"

    isBindAliasAnnotation :: Annotation Expression -> Bool
    isBindAliasAnnotation (EAnalysis (BindAlias _))          = True
    isBindAliasAnnotation (EAnalysis (BindFreshAlias _))     = True
    isBindAliasAnnotation (EAnalysis (BindAliasExtension _)) = True
    isBindAliasAnnotation _                                  = False

    refreshEntry :: Identifier -> IEnvEntry Value -> Value -> Interpretation ()
    refreshEntry n (IVal _) v  = replaceE n (IVal v)
    refreshEntry _ (MVal mv) v = liftIO (modifyMVar_ mv $ const $ return v)

    lookupVQ :: Identifier -> Interpretation (Value, VQualifier)
    lookupVQ i = lookupE i >>= valueQOfEntry

    -- | Performs a write-back for a bind expression.
    --   This retrieves the current binding values from the environment
    --   and reconstructs a path value to replace the bind target.
    refreshBindings :: Binder -> ProxyPath -> Value -> Interpretation ()
    refreshBindings (BIndirection i) proxyPath bindV =
      lookupVQ i >>= \case
        (iV, MemMut) ->
          replaceProxyPath proxyPath bindV iV (\oldV newPathV ->
            case oldV of
              VIndirection (mv, MemMut, _) ->
                liftIO (modifyMVar_ mv $ const $ return newPathV) >> return oldV
              _ -> throwE $ RunTimeTypeError "Invalid bind indirection target")

        (_, _) -> return () -- Skip writeback to an immutable value.

    refreshBindings (BTuple ts) proxyPath bindV =
      mapM lookupVQ ts >>= \vqs ->
        if any (/= MemImmut) $ map snd vqs
          then replaceProxyPath proxyPath bindV (VTuple vqs) (\_ newPathV -> return newPathV)
          else return () -- Skip writeback if all fields are immutable.

    refreshBindings (BRecord ids) proxyPath bindV =
      mapM lookupVQ (map snd ids) >>= \vqs ->
        if any (/= MemImmut) $ map snd vqs
          then replaceProxyPath proxyPath bindV
                  (VRecord $ membersFromList $ zip (map fst ids) vqs)
                  (\oldV newPathV -> mergeRecords oldV newPathV)
          else return () -- Skip writebsack if all fields are immutable.

    replaceProxyPath :: ProxyPath -> Value -> Value
                     -> (Value -> Value -> Interpretation Value)
                     -> Interpretation ()
    replaceProxyPath proxyPath origV newComponentV refreshF =
      case proxyPath of
        (Named n):t     -> do
                            entry <- lookupE n
                            oldV  <- valueOfEntry entry
                            pathV <- reconstructPathValue t newComponentV oldV
                            refreshF oldV pathV >>= refreshEntry n entry

        (Temporary _):t -> reconstructPathValue t newComponentV origV >>= refreshF origV >> return ()
        _               -> throwE $ RunTimeInterpretationError "Invalid path in bind writeback"


    reconstructPathValue :: ProxyPath -> Value -> Value -> Interpretation Value
    reconstructPathValue [] newR@(VRecord _) oldR@(VRecord _) = mergeRecords oldR newR
    reconstructPathValue [] v _ = return v

    reconstructPathValue (Dereference:t) v (VIndirection (iv, q, tg)) =
      liftIO (readMVar iv)
        >>= reconstructPathValue t v
        >>= \nv -> liftIO (modifyMVar_ iv $ const $ return nv) >> return (VIndirection (iv, q, tg))

    reconstructPathValue (MatchOption:t) v (VOption (Just ov, q)) =
      reconstructPathValue t v ov >>= \nv -> return $ VOption (Just nv, q)

    reconstructPathValue ((TupleField i):t) v (VTuple vs) =
      let (x,y) = splitAt i vs in
      reconstructPathValue t v (fst $ last x) >>= \nv -> return $ VTuple ((init x) ++ [(nv, snd $ last x)] ++ y)

    reconstructPathValue ((RecordField n):t) v (VRecord ivs) = do
      fields <- flip mapMembers ivs (\fn (fv, fq) ->
                  if fn == n then reconstructPathValue t v fv >>= return . (, fq)
                             else return (fv, fq))
      return $ VRecord fields

    reconstructPathValue _ _ _ =
      throwE $ RunTimeInterpretationError "Invalid path in bind writeback reconstruction"

    -- | Merge two records, restricting to the domain of the first record, and
    --   preferring values from the second argument for duplicates.
    mergeRecords :: Value -> Value -> Interpretation Value
    mergeRecords (VRecord r1) (VRecord r2) =
      mapBindings (\n v -> maybe (return v) return $ lookupBinding n r2) r1 >>= return . VRecord

    mergeRecords _ _ =
      throwE $ RunTimeTypeError "Invalid bind record target"

    expr :: K3 Expression -> Interpretation Value
    -- | Interpretation of constant expressions.
    expr (details -> (EConstant c, _, as)) = constant c as

    -- | Interpretation of variable lookups.
    expr (details -> (EVariable i, _, _)) =
      lookupE i >>= \e -> valueOfEntry e >>= syncCollectionE i e

    -- | Interpretation of option type construction expressions.
    expr (tag &&& children -> (ESome, [x])) =
      expression x >>= freshenValue >>= return . VOption . (, vQualOfExpr x) . Just

    expr (details -> (ESome, _, _)) =
      throwE $ RunTimeTypeError "Invalid Construction of Option"

    -- | Interpretation of indirection type construction expressions.
    expr (tag &&& children -> (EIndirect, [x])) = do
      new_val <- expression x >>= freshenValue
      (\a b -> VIndirection (a, vQualOfExpr x, b)) <$> liftIO (newMVar new_val) <*> memEntTag new_val

    expr (details -> (EIndirect, _, _)) =
      throwE $ RunTimeTypeError "Invalid Construction of Indirection"

    -- | Interpretation of tuple construction expressions.
    expr (tag &&& children -> (ETuple, cs)) =
      mapM (\e -> expression e >>= freshenValue >>= return . (, vQualOfExpr e)) cs >>= return . VTuple

    -- | Interpretation of record construction expressions.
    expr (tag &&& children -> (ERecord is, cs)) =
      mapM (\e -> expression e >>= freshenValue >>= return . (, vQualOfExpr e)) cs >>= return . VRecord . membersFromList . zip is

    -- | Interpretation of function construction.
    expr (details -> (ELambda i, [b], _)) =
      mkFunction $ \v -> insertE i (IVal v) >> expression b >>= removeE i
      where
        mkFunction f = (\cl tg -> VFunction (f, cl, tg)) <$> closure <*> memEntTag f

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
          envFromList $ zip vars vals

    -- | Interpretation of unary/binary operators.
    expr (details -> (EOperate otag, cs, _))
        | otag `elem` [ONeg, ONot], [a] <- cs = unary otag a
        | otherwise, [a, b] <- cs = binary otag a b
        | otherwise = undefined

    -- | Interpretation of Record Projection.
    expr (details -> (EProject i, [r], _)) = expression r >>= syncCollection >>= \case
        VRecord vm -> maybe (unknownField i) (return . fst) $ lookupMember i vm

        VCollection (_, c) -> do
          if null (realizationId c) then unannotatedCollection
          else maybe (unknownCollectionMember i c) (return . fst)
                  $ lookupMember i $ collectionNS $ namespace c

        v -> throwE . RunTimeTypeError $ "Invalid projection on value: " ++ show v

      where unknownField i'              = throwE . RunTimeTypeError $ "Unknown record field " ++ i'
            unannotatedCollection        = throwE . RunTimeTypeError $ "Invalid projection on an unannotated collection"
            unknownCollectionMember i' c = throwE . RunTimeTypeError $ "Unknown collection member " ++ i' ++ " in collection " ++ show c

    expr (details -> (EProject _, _, _)) = throwE $ RunTimeTypeError "Invalid Record Projection"

    -- | Interpretation of Let-In Constructions.
    expr (tag &&& children -> (ELetIn i, [e, b])) = do
      entry <- expression e >>= freshenValue >>= entryOfValueE (e @~ isEQualified)
      insertE i entry >> expression b >>= removeE i

    expr (details -> (ELetIn _, _, _)) = throwE $ RunTimeTypeError "Invalid LetIn Construction"

    -- | Interpretation of Assignment.
    expr (details -> (EAssign i, [e], _)) = do
      entry <- lookupE i
      case entry of
        MVal mv -> expression e >>= freshenValue >>= \v -> liftIO (modifyMVar_ mv $ const $ return v) >> return v
        IVal _  -> throwE $ RunTimeInterpretationError
                          $ "Invalid assignment to an immutable variable: " ++ i

    expr (details -> (EAssign _, _, _)) = throwE $ RunTimeTypeError "Invalid Assignment"

    -- | Interpretation of If-Then-Else constructs.
    expr (details -> (EIfThenElse, [p, t, e], _)) = expression p >>= \case
        VBool True  -> expression t
        VBool False -> expression e
        _ -> throwE $ RunTimeTypeError "Invalid Conditional Predicate"

    expr (details -> (EAddress, [h, p], _)) = do
      hv <- expression h
      pv <- expression p
      case (hv, pv) of
        (VString host, VInt port) -> return $ VAddress $ Address (host, port)
        _ -> throwE $ RunTimeTypeError "Invalid address"

    expr (details -> (ESelf, _, _)) = lookupE annotationSelfId >>= valueOfEntry

    -- | Interpretation of Case-Matches.
    -- Case expressions behave like bind-as, i.e., w/ isolated bindings and writeback
    expr (details -> (ECaseOf i, [e, s, n], _)) = do
        void $ pushProxyFrame
        targetV <- expression e
        case targetV of
          VOption (Just v, q) -> do
            pp <- getProxyPath >>= \case
              Just ((Named pn):t)     -> return $ (Named pn):t
              Just ((Temporary pn):t) -> return $ (Temporary pn):t
              _ -> throwE $ RunTimeTypeError "Invalid proxy path in case-of expression"

            void $ popProxyFrame
            entry <- entryOfValueQ v q
            insertE i entry
            sV <- expression s
            void $ lookupVQ i >>= \case
              (iV, MemMut) -> replaceProxyPath pp targetV (VOption (Just iV, MemMut)) (\_ newPathV -> return newPathV)
              _            -> return () -- Skip writeback for immutable values.
            removeE i sV

          VOption (Nothing, _) -> popProxyFrame >> expression n
          _ -> throwE $ RunTimeTypeError "Invalid Argument to Case-Match"

    expr (details -> (ECaseOf _, _, _)) = throwE $ RunTimeTypeError "Invalid Case-Match"

    -- | Interpretation of Binding.
    -- TODO: For now, all bindings are added in mutable fashion. This should be extracted from
    -- the type inferred for the bind target expression.
    expr (details -> (EBindAs b, [e, f], _)) = do
      void $ pushProxyFrame
      pc <- getPrintConfig <$> get
      bv <- expression e
      bp <- getProxyPath >>= \case
        Just ((Named n):t)     -> return $ (Named n):t
        Just ((Temporary n):t) -> return $ (Temporary n):t
        _ -> throwE $ RunTimeTypeError "Invalid bind path in bind-as expression"

      void $ popProxyFrame
      case (b, bv) of
        (BIndirection i, VIndirection (r,q,_)) -> do
          entry <- liftIO (readMVar r) >>= flip entryOfValueQ q
          void  $  insertE i entry
          fV    <- expression f
          void  $  refreshBindings b bp bv
          removeE i fV

        (BTuple ts, VTuple vs) -> do
          let tupMems = membersFromList $ zip ts vs
          bindAndRefresh bp bv tupMems

        (BRecord ids, VRecord ivs) -> do
          let (idls, ivls) = (map fst ids, boundNames ivs)

          -- Testing the intersection with the bindings ensures every bound name
          -- has a value, while also allowing us to bind a subset of the values.
          if idls `intersect` ivls == idls
            then do
              let recordMems = membersFromList $ joinByKeys (,) idls ids ivs
              bindAndRefresh bp bv recordMems

            else throwE $ RunTimeTypeError "Invalid Bind-Pattern"

        (binder, binderV) ->
          throwE $ RunTimeTypeError $
            "Bind Mis-Match: value is " ++ showPC (pc {convertToTuples=False}) binderV
                                        ++ " but bind is " ++ show binder

      where
        bindAndRefresh bp bv mems = do
          bindings <- bindMembers mems
          fV       <- expression f
          void     $  refreshBindings b bp bv
          unbindMembers bindings >> return fV

        joinByKeys joinF keys l r =
          catMaybes $ map (\k -> lookup k l >>= (\matchL -> lookupMember k r >>= return . joinF matchL)) keys

    expr (details -> (EBindAs _,_,_)) = throwE $ RunTimeTypeError "Invalid Bind Construction"

    expr _ = throwE $ RunTimeInterpretationError "Invalid Expression"


{- Literal interpretation -}

literal :: K3 Literal -> Interpretation Value
literal (tag -> LBool b)   = return $ VBool b
literal (tag -> LByte b)   = return $ VByte b
literal (tag -> LInt i)    = return $ VInt i
literal (tag -> LReal r)   = return $ VReal r
literal (tag -> LString s) = return $ VString s
literal l@(tag -> LNone _) = return $ VOption (Nothing, vQualOfLit l)

literal (tag &&& children -> (LSome, [x])) = literal x >>= return . VOption . (, vQualOfLit x) . Just
literal (details -> (LSome, _, _)) = throwE $ RunTimeTypeError "Invalid option literal"

literal (tag &&& children -> (LIndirect, [x])) = literal x >>= (\y -> (\i tg -> (i, vQualOfLit x, tg)) <$> liftIO (newMVar y) <*> memEntTag y) >>= return . VIndirection
literal (details -> (LIndirect, _, _)) = throwE $ RunTimeTypeError "Invalid indirection literal"

literal (tag &&& children -> (LTuple, ch)) = mapM (\l -> literal l >>= return . (, vQualOfLit l)) ch >>= return . VTuple
literal (details -> (LTuple, _, _)) = throwE $ RunTimeTypeError "Invalid tuple literal"

literal (tag &&& children -> (LRecord ids, ch)) = mapM (\l -> literal l >>= return . (, vQualOfLit l)) ch >>= return . VRecord . membersFromList . zip ids
literal (details -> (LRecord _, _, _)) = throwE $ RunTimeTypeError "Invalid record literal"

literal (details -> (LEmpty _, [], anns)) =
  getComposedAnnotationL anns >>= maybe (emptyCollection annIds) emptyAnnotatedCollection
  where annIds = namedLAnnotations anns

literal (details -> (LEmpty _, _, _)) = throwE $ RunTimeTypeError "Invalid empty literal"

literal (details -> (LCollection _, elems, anns)) = do
  cElems <- mapM literal elems
  realizationOpt <- getComposedAnnotationL anns
  case realizationOpt of
    Nothing       -> initialCollection (namedLAnnotations anns) cElems
    Just comboId  -> initialAnnotatedCollection comboId cElems

literal (details -> (LAddress, [h,p], _)) = mapM literal [h,p] >>= \case
  [VString a, VInt b] -> return . VAddress $ Address (a,b)
  _     -> throwE $ RunTimeTypeError "Invalid address literal"

literal (details -> (LAddress, _, _)) = throwE $ RunTimeTypeError "Invalid address literal"

literal _ = throwE $ RunTimeTypeError "Invalid literal"

{- Declaration interpretation -}

replaceTrigger :: (HasSpan a, HasUID a) => Identifier -> [a] -> Value -> Interpretation ()
replaceTrigger n _    (VFunction (f,_,tg)) = replaceE n (IVal $ VTrigger (n, Just f, tg))
replaceTrigger n _ _                       = throwE $ RunTimeTypeError ("Invalid body for trigger " ++ n)

global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> Interpretation ()
global n (details -> (TSink, _, anns)) (Just e) = expression e >>= replaceTrigger n anns
global _ (details -> (TSink, _, _)) Nothing     = throwE $ RunTimeInterpretationError "Invalid sink trigger"
-- ^ Interpret and add sink triggers to the program environment.

-- | Sources have already been translated into K3 code
global _ (tag -> TSource) _ = return ()

-- | Functions have already been initialized as part of the program environment.
global _ (isTFunction -> True) _ = return ()

-- | Add collection declaration, generating the collection type given by the annotation
--   combination on demand.
--   n is the name of the variable
global n t@(details -> (TCollection, _, _)) eOpt = elemE n >>= \case
  True  -> void . getComposedAnnotationT $ annotations t
  False -> (getComposedAnnotationT $ annotations t) >>= initializeCollection . maybe "" id
  where
    initializeCollection comboId = case eOpt of
      Nothing | not (null comboId) -> emptyAnnotatedCollection comboId >>= entryOfValueT (t @~ isTQualified) >>= insertE n
      Just e  | not (null comboId) -> expression e >>= verifyInitialCollection comboId

      -- TODO: error on these cases. All collections must have at least the builtin Collection annotation.
      Nothing -> emptyCollection (namedTAnnotations $ annotations t) >>= entryOfValueT (t @~ isTQualified) >>= insertE n
      Just e  -> expression e >>= entryOfValueT (t @~ isTQualified) >>= insertE n

    verifyInitialCollection comboId = \case
      v@(VCollection (_, Collection _ _ cId)) ->
          if comboId == cId then entryOfValueT (t @~ isTQualified) v >>= insertE n
                            else collInitError comboId cId
      _ -> collValError

    collInitError c c' = throwE . RunTimeTypeError $ "Invalid annotations on collection initializer for " ++ n ++ ": " ++ c ++ " and " ++ c'
    collValError  = throwE . RunTimeTypeError $ "Invalid collection value " ++ n

-- | Instantiate all other globals in the interpretation environment.
global n t eOpt = elemE n >>= \case
    True  -> return ()
    False -> maybe (defaultValue t) expression eOpt >>= entryOfValueT (t @~ isTQualified) >>= insertE n


-- TODO: qualify names?
role :: Identifier -> [K3 Declaration] -> Interpretation ()
role _ subDecls = mapM_ declaration subDecls


declaration :: K3 Declaration -> Interpretation ()
declaration (tag &&& children -> (DGlobal n t eO, ch)) =
    debugDecl n t $ global n t eO >> mapM_ declaration ch

declaration (details -> (DTrigger n t e, cs, anns)) =
    debugDecl n t $ (expression e >>= replaceTrigger n anns) >> mapM_ declaration cs

declaration (tag &&& children -> (DRole r, ch))   = role r ch
declaration (tag -> DAnnotation n vdecls members) = annotation n vdecls members

declaration _ = undefined


{- Annotations -}

annotation :: Identifier -> [TypeVarDecl] -> [AnnMemDecl] -> Interpretation ()
annotation n _ memberDecls = tryLookupADef n >>= \case
  Nothing -> addAnnotationDef
  Just _  -> return ()

  where
    addAnnotationDef = do
      (annMems, bindings) <- foldM initializeMembers
                                      (emptyMembers, emptyBindings)
                                      [liftedAttrFuns, liftedAttrs, attrFuns, attrs]
      _ <- unbindMembers bindings
      void $ modifyADefs $ (:) (n, annMems)

    -- | Initialize members, while adding each member declaration to the environment to
    --   support linear immediate initializer access.
    initializeMembers mbAcc spec = foldM (memberWithBindings spec) mbAcc memberDecls

    memberWithBindings (isLifted, matchF) mbAcc mem = do
      ivOpt <- annotationMember n isLifted matchF mem
      maybe (return mbAcc) (bindAndAppendMem mbAcc) ivOpt

    bindAndAppendMem (memAcc, bindAcc) (memN,(v,q)) = do
      entry <- case q of
                  MemImmut -> return $ IVal v
                  MemMut   -> liftIO (newMVar v) >>= return . MVal
      void $ insertE memN entry
      return (insertMember memN (v,q) memAcc, insertBinding memN entry bindAcc)

    (liftedAttrFuns, liftedAttrs) = ((True, isTFunction), (True, not . isTFunction))
    (attrFuns, attrs)             = ((False, isTFunction), (False, not . isTFunction))


annotationMember :: Identifier -> Bool -> (K3 Type -> Bool) -> AnnMemDecl
                 -> Interpretation (Maybe (Identifier, (Value, VQualifier)))
annotationMember annId matchLifted matchF annMem = case (matchLifted, annMem) of
  (True,  Lifted    Provides n t (Just e) _) | matchF t -> initializeMember n t e
  (False, Attribute Provides n t (Just e) _) | matchF t -> initializeMember n t e
  (True,  Lifted    Provides n t Nothing  _) | matchF t -> builtinLiftedAttribute annId n t >>= return . builtinQual t
  (False, Attribute Provides n t Nothing  _) | matchF t -> builtinAttribute annId n t >>= return . builtinQual t
  _ -> return Nothing

  where initializeMember n t e = expression e >>= \v -> return . Just $ (n, (v, memberQual t))
        builtinQual t (Just (n,v)) = Just (n, (v, memberQual t))
        builtinQual _ Nothing      = Nothing
        memberQual t = case t @~ isTQualified of
          Just TMutable -> MemMut
          _             -> MemImmut
