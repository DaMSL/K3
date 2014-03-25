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

import Debug.Trace

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

$(loggingFunctions)

-- | Helper functions.
onQualifiedType :: K3 Type -> a -> a -> a
onQualifiedType t immutR mutR = case t @~ isTQualified of  
  Just TMutable -> mutR
  _ -> immutR

onQualifiedExpression :: K3 Expression -> a -> a -> a
onQualifiedExpression e immutR mutR = case e @~ isEQualified of  
  Just EMutable -> mutR
  _ -> immutR

onQualifiedAnnotationsE :: [Annotation Expression] -> a -> a -> a
onQualifiedAnnotationsE anns immutR mutR = case anns @~ isEQualified of 
  Just EMutable -> mutR
  _ -> immutR

onQualifiedLiteral :: K3 Literal -> a -> a -> a
onQualifiedLiteral l immutR mutR = case l @~ isLQualified of
  Just LMutable -> mutR
  _ -> immutR

vQualOfType :: K3 Type -> VQualifier
vQualOfType t = onQualifiedType t MemImmut MemMut

vQualOfExpr :: K3 Expression -> VQualifier
vQualOfExpr e = onQualifiedExpression e MemImmut MemMut

vQualOfLit :: K3 Literal -> VQualifier
vQualOfLit l = onQualifiedLiteral l MemImmut MemMut

vQualOfAnnsE :: [Annotation Expression] -> VQualifier
vQualOfAnnsE anns = onQualifiedAnnotationsE anns MemImmut MemMut

-- | Monadic message passing primitive for the interpreter.
sendE :: Address -> Identifier -> Value -> Interpretation ()
sendE addr n val = liftEngine $ send addr n val


{- Interpretation -}

-- | Refresh the collection's cached value from its shared (self) value.
--   This treats the shared value as the authoritative contents.
syncCollection :: Value -> Interpretation Value
syncCollection (VCollection (s,_)) =
  liftIO (readMVar s) >>= \case
      VCollection (s2,c2) | s == s2 -> return $ VCollection (s, c2) 
      _ -> throwE $ RunTimeInterpretationError "Invalid cyclic self value"
syncCollection v = return v

-- | Refreshes a collection's cached value while also modifying a binding.
syncCollectionE :: Identifier -> IEnvEntry Value -> Value -> Interpretation Value
syncCollectionE i (IVal _)  v@(VCollection _) = syncCollection v >>= \nv -> replaceE i (IVal nv) >> return nv
syncCollectionE _ (MVal mv) v@(VCollection _) = syncCollection v >>= \nv -> liftIO (modifyMVar_ mv $ const $ return nv) >> return nv
syncCollectionE _ _ v = return v

-- | Return a fresh, unshared collection value after synchronization.
freshenValue :: Value -> Interpretation Value
freshenValue v@(VCollection _) = syncCollection v >>= \case
  VCollection (_,c2) -> freshCollection c2
  _                  -> throwE $ RunTimeInterpretationError "Invalid collection value"
freshenValue v = return v

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
numeric :: Maybe (Span, UID) -> (forall a. Num a => a -> a -> a) -> 
        K3 Expression -> K3 Expression -> Interpretation Value
numeric su op a b = do
    a' <- expression a
    b' <- expression b
    numericOp op op op err a' b'

  where err = throwSE su $ RunTimeTypeError "Arithmetic Type Mis-Match"

-- | Similar to numeric above, except disallow a zero value for the second argument.
numericExceptZero :: Maybe (Span, UID)
                  -> (Word8 -> Word8 -> Word8)
                  -> (Int -> Int -> Int)
                  -> (Double -> Double -> Double) 
                  -> K3 Expression -> K3 Expression -> Interpretation Value
numericExceptZero su byteOpF intOpF realOpF a b = do
    a' <- expression a
    b' <- expression b

    void $ case b' of
        VByte 0 -> throwSE su $ RunTimeInterpretationError "Zero denominator"
        VInt  0 -> throwSE su $ RunTimeInterpretationError "Zero denominator"
        VReal 0 -> throwSE su $ RunTimeInterpretationError "Zero denominator"
        _       -> return ()

    numericOp byteOpF intOpF realOpF err a' b'

  where err = throwSE su $ RunTimeTypeError "Arithmetic Type Mis-Match"


-- | Common boolean operation handling.
logic :: Maybe (Span, UID) -> (Bool -> Bool -> Bool) -> K3 Expression -> K3 Expression -> Interpretation Value
logic su op a b = do
  a' <- expression a
  b' <- expression b

  case (a', b') of
      (VBool x, VBool y) -> return $ VBool $ op x y
      _ -> throwSE su $ RunTimeTypeError "Invalid Boolean Operation"

-- | Common comparison operation handling.
comparison :: (Value -> Value -> Interpretation Value)
           -> K3 Expression -> K3 Expression -> Interpretation Value
comparison op a b = do
  a' <- expression a
  b' <- expression b
  op a' b'

-- | Interpretation of unary operators.
unary :: Maybe (Span, UID) -> Operator -> K3 Expression -> Interpretation Value

-- | Interpretation of unary negation of numbers.
unary su ONeg a = expression a >>= \case
  VInt i   -> return $ VInt  (negate i)
  VReal r  -> return $ VReal (negate r)
  _ -> throwSE su $ RunTimeTypeError "Invalid Negation"

-- | Interpretation of unary negation of booleans.
unary su ONot a = expression a >>= \case
  VBool b -> return $ VBool (not b)
  _ -> throwSE su $ RunTimeTypeError "Invalid Complement"

unary su _ _ = throwSE su $ RunTimeTypeError "Invalid Unary Operator"

-- | Interpretation of binary operators.
binary :: Maybe (Span, UID) -> Operator -> K3 Expression -> K3 Expression -> Interpretation Value

-- | Standard numeric operators.
binary su OAdd = numeric su (+)
binary su OSub = numeric su (-)
binary su OMul = numeric su (*)

-- | Division and modulo handled similarly, but accounting zero-division errors.
binary su ODiv = numericExceptZero su div div (/)
binary su OMod = numericExceptZero su mod mod mod'

-- | Logical Operators
binary su OAnd = logic su (&&)
binary su OOr  = logic su (||)

-- | Comparison Operators
binary _ OEqu = comparison valueEq
binary _ ONeq = comparison valueNeq
binary _ OLth = comparison valueLt
binary _ OLeq = comparison valueLte
binary _ OGth = comparison valueGt
binary _ OGeq = comparison valueGte

-- | Function Application
binary su OApp = \f x -> do
  f' <- expression f
  x' <- expression x >>= freshenValue

  case f' of
      VFunction (b, cl, _) -> withClosure cl $ b x'
      _ -> throwSE su $ RunTimeTypeError $ "Invalid Function Application on " ++ show f

  where withClosure cl doApp = mergeE cl >> doApp >>= \r -> pruneE cl >> freshenValue r

-- | Message Passing
binary su OSnd = \target x -> do
  target'  <- expression target
  x'       <- expression x

  case target' of
    VTuple [(VTrigger (n, _, _), _), (VAddress addr, _)] -> sendE addr n x' >> return vunit
    _ -> throwSE su $ RunTimeTypeError "Invalid Trigger Target"

-- | Sequential expressions
binary _ OSeq = \e1 e2 -> expression e1 >> expression e2

binary su _ = const . const $ throwSE su $ RunTimeInterpretationError "Unreachable"

-- | Interpretation of Expressions
expression :: K3 Expression -> Interpretation Value
expression e_ = 
  -- TODO: dataspace bind aliases
  case e_ @~ isBindAliasAnnotation of
    Just (EAnalysis (BindAlias i))          -> expr e_ >>= \r -> appendAlias (Named i) >> return r
    Just (EAnalysis (BindFreshAlias i))     -> expr e_ >>= \r -> appendAlias (Temporary i) >> return r
    Just (EAnalysis (BindAliasExtension i)) -> expr e_ >>= \r -> appendAliasExtension i >> return r
    Nothing  -> expr e_
    Just _   -> throwAE (annotations e_) $ RunTimeInterpretationError "Invalid bind alias annotation matching"

  where
    isBindAliasAnnotation (EAnalysis (BindAlias _))          = True
    isBindAliasAnnotation (EAnalysis (BindFreshAlias _))     = True
    isBindAliasAnnotation (EAnalysis (BindAliasExtension _)) = True
    isBindAliasAnnotation _                                  = False

    expr :: K3 Expression -> Interpretation Value
    -- | Interpretation of constant expressions.
    expr (details -> (EConstant c, _, as)) = constant c as

    -- | Interpretation of variable lookups.
    expr (details -> (EVariable i, _, anns)) =
      lookupE (spanUid anns) i >>= \e -> valueOfEntry e >>= syncCollectionE i e 

    -- | Interpretation of option type construction expressions.
    expr (tag &&& children -> (ESome, [x])) =
      expression x >>= freshenValue >>= return . VOption . (, vQualOfExpr x) . Just
    
    expr (details -> (ESome, _, anns)) =
      throwAE anns $ RunTimeTypeError "Invalid Construction of Option"

    -- | Interpretation of indirection type construction expressions.
    expr (tag &&& children -> (EIndirect, [x])) = do
      new_val <- expression x >>= freshenValue
      (\a b -> VIndirection (a, vQualOfExpr x, b)) <$> liftIO (newMVar new_val) <*> memEntTag new_val

    expr (details -> (EIndirect, _, anns)) =
      throwAE anns $ RunTimeTypeError "Invalid Construction of Indirection"

    -- | Interpretation of tuple construction expressions.
    expr (tag &&& children -> (ETuple, cs)) =
      mapM (\e -> expression e >>= freshenValue >>= return . (, vQualOfExpr e)) cs >>= return . VTuple

    -- | Interpretation of record construction expressions.
    expr (tag &&& children -> (ERecord is, cs)) =
      mapM (\e -> expression e >>= freshenValue >>= return . (, vQualOfExpr e)) cs >>= return . VRecord . membersFromList . zip is

    -- | Interpretation of function construction.
    expr (details -> (ELambda i, [b], anns)) =
      mkFunction $ \v -> insertE i (IVal v) >> expression b >>= removeE i (IVal v)
      where
        mkFunction f = (\cl tg -> VFunction (f, cl, tg)) <$> closure (spanUid anns) <*> memEntTag f

        -- TODO: currently, this definition of a closure captures 
        -- annotation member variables during annotation member initialization.
        -- This invalidates the use of annotation member function contextualization
        -- since the context is overridden by the closure whenever applying the
        -- member function.
        closure :: Maybe (Span, UID) -> Interpretation (Closure Value)
        closure su = do
          globals <- get >>= return . getGlobals
          vars    <- return $ filter (\n -> n /= i && n `notElem` globals) $ freeVariables b
          vals    <- mapM (lookupE su) vars
          envFromList $ zip vars vals

    -- | Interpretation of unary/binary operators.
    expr (details -> (EOperate otag, cs, anns))
        | otag `elem` [ONeg, ONot], [a] <- cs = unary (spanUid anns) otag a
        | otherwise, [a, b] <- cs = binary (spanUid anns) otag a b
        | otherwise = undefined

    -- | Interpretation of Record Projection.
    expr (details -> (EProject i, [r], anns)) = expression r >>= syncCollection >>= \case
        VRecord vm -> maybe (unknownField i) (return . fst) $ lookupMember i vm

        VCollection (_, c) -> do
          if null (realizationId c) then unannotatedCollection
          else maybe (unknownCollectionMember i c) (return . fst)
                  $ lookupMember i $ collectionNS $ namespace c

        v -> throwAE anns . RunTimeTypeError $ "Invalid projection on value: " ++ show v

      where unknownField i' = throwAE anns $ RunTimeTypeError $ "Unknown record field " ++ i'
            unannotatedCollection = throwAE anns $ RunTimeTypeError $ "Invalid projection on an unannotated collection"
            unknownCollectionMember i' c = throwAE anns $ RunTimeTypeError $ "Unknown collection member " ++ i' ++ " in collection " ++ show c

    expr (details -> (EProject _, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid Record Projection"

    -- | Interpretation of Let-In Constructions.
    expr (tag &&& children -> (ELetIn i, [e, b])) = do
      entry <- expression e >>= freshenValue >>= entryOfValueE (e @~ isEQualified)
      insertE i entry >> expression b >>= removeE i entry

    expr (details -> (ELetIn _, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid LetIn Construction"

    -- | Interpretation of Assignment.
    expr (details -> (EAssign i, [e], anns)) = do
      entry <- lookupE (spanUid anns) i
      case entry of 
        MVal mv -> expression e >>= freshenValue >>= \v -> liftIO (modifyMVar_ mv $ const return v) >> return v
        IVal _  -> throwAE anns $ RunTimeInterpretationError "Invalid assignment to an immutable value"

    expr (details -> (EAssign _, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid Assignment"

    -- | Interpretation of If-Then-Else constructs.
    expr (details -> (EIfThenElse, [p, t, e], anns)) = expression p >>= \case
        VBool True  -> expression t
        VBool False -> expression e
        _ -> throwAE anns $ RunTimeTypeError "Invalid Conditional Predicate"

    expr (details -> (EAddress, [h, p], anns)) = do
      hv <- expression h
      pv <- expression p
      case (hv, pv) of
        (VString host, VInt port) -> return $ VAddress $ Address (host, port)
        _ -> throwAE anns $ RunTimeTypeError "Invalid address"

    expr (details -> (ESelf, _, anns)) = lookupE (spanUid anns) annotationSelfId >>= valueOfEntry

    -- | Interpretation of Case-Matches.
    -- TODO: case expressions should behave like bind, i.e., w/ bind aliases and transactional write-backs
    expr (details -> (ECaseOf i, [e, s, n], anns)) = expression e >>= \case
        VOption (Just v, q)  -> entryOfValueQ v q >>= \entry -> insertE i entry >> expression s >>= removeE i entry
        VOption (Nothing, _) -> expression n
        _ -> throwAE anns $ RunTimeTypeError "Invalid Argument to Case-Match"
    expr (details -> (ECaseOf _, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid Case-Match"

    -- | Interpretation of Binding.
    -- TODO: For now, all bindings are added in mutable fashion. This should be extracted from
    -- the type inferred for the bind target expression.
    expr (details -> (EBindAs b, [e, f], anns)) = withBindFrame $ do
      bv <- expression e
      bp <- getProxyPath >>= \case
        Just ((Named n):t)     -> return $ (Named n):t
        Just ((Temporary n):t) -> return $ (Temporary n):t
        _ -> throwAE anns $ RunTimeTypeError "Invalid bind path in bind-as expression"

      case (b, bv) of
        (BIndirection i, VIndirection (r,q,_)) -> do
          entry <- liftIO (readMVar r) >>= flip entryOfValueQ q
          void $ insertE i entry
          fV <- expression f
          void $ refreshBindings b bp bv
          removeE i entry fV

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
            
            else throwAE anns $ RunTimeTypeError "Invalid Bind-Pattern"

        _ -> throwAE anns $ RunTimeTypeError "Bind Mis-Match"

      where 
        su = spanUid anns

        withBindFrame bindEval = pushProxyFrame >> bindEval >>= \r -> popProxyFrame >> return r

        bindAndRefresh bp bv mems = do
          bindings <- bindMembers mems
          fV <- expression f
          void $ refreshBindings b bp bv
          unbindMembers bindings >> return fV

        joinByKeys joinF keys l r =
          catMaybes $ map (\k -> lookup k l >>= (\matchL -> lookupMember k r >>= return . joinF matchL)) keys

        refreshEntry :: Identifier -> IEnvEntry Value -> Value -> Interpretation ()
        refreshEntry n (IVal _) v  = replaceE n (IVal v)
        refreshEntry _ (MVal mv) v = liftIO (modifyMVar_ mv $ const $ return v)

        lookupForRefresh :: Identifier -> Interpretation (Value, VQualifier)
        lookupForRefresh i = lookupE su i >>= valueQOfEntry

        -- | Performs a write-back for a bind expression.
        --   This retrieves the current binding values from the environment
        --   and reconstructs a path value to replace the bind target.
        refreshBindings :: Binder -> ProxyPath -> Value -> Interpretation ()
        refreshBindings (BIndirection i) bindPath bindV = 
          lookupForRefresh i >>= \case
            (iV, MemMut) ->
              flip (replaceBindPath bindPath bindV) iV (\oldV newPathV ->
                case oldV of 
                  VIndirection (mv, MemMut, _) -> liftIO (modifyMVar_ mv $ const $ return newPathV) >> return oldV
                  _ -> throwAE anns $ RunTimeTypeError "Invalid bind indirection target")

            _ -> return () -- Skip writeback to an immutable value.

        refreshBindings (BTuple ts) bindPath bindV =
          mapM lookupForRefresh ts >>= \vqs ->
            if any (/= MemImmut) $ map snd vqs
              then replaceBindPath bindPath bindV (\_ newPathV -> return newPathV) $ VTuple vqs
              else return () -- Skip writeback if all fields are immutable.

        refreshBindings (BRecord ids) bindPath bindV =
          mapM lookupForRefresh (map snd ids) >>= \vqs ->
            if any (/= MemImmut) $ map snd vqs
              then replaceBindPath bindPath bindV (\oldV newPathV -> mergeRecords oldV newPathV)
                      $ VRecord $ membersFromList $ zip (map fst ids) vqs
              else return () -- Skip writebsack if all fields are immutable.

        replaceBindPath :: ProxyPath -> Value -> (Value -> Value -> Interpretation Value) -> Value
                        -> Interpretation ()
        replaceBindPath bindPath bindV refreshF newV =
          case bindPath of 
            (Named n):t     -> do
                                entry <- lookupE su n
                                oldV  <- valueOfEntry entry
                                pathV <- reconstructPathValue t newV oldV
                                refreshF oldV pathV >>= refreshEntry n entry

            (Temporary _):t -> reconstructPathValue t newV bindV >>= refreshF bindV >> return ()
            _               -> throwAE anns $ RunTimeInterpretationError "Invalid path in bind writeback"


        reconstructPathValue :: ProxyPath -> Value -> Value -> Interpretation Value
        reconstructPathValue [] newR@(VRecord _) oldR@(VRecord _) = mergeRecords oldR newR
        reconstructPathValue [] v _ = return v
        
        reconstructPathValue (Dereference:t) v (VIndirection (iv, q, tg)) =
          liftIO (readMVar iv)
            >>= reconstructPathValue t v
            >>= \nv -> liftIO (modifyMVar_ iv $ const $ return nv) >> return (VIndirection (iv, q, tg))

        reconstructPathValue ((TupleField i):t) v (VTuple vs) =
          let (x,y) = splitAt i vs in
          reconstructPathValue t v (fst $ last x) >>= \nv -> return $ VTuple ((init x) ++ [(nv, snd $ last x)] ++ y)

        reconstructPathValue ((RecordField n):t) v (VRecord ivs) = do
          fields <- flip mapMembers ivs (\fn (fv, fq) -> 
                      if fn == n then reconstructPathValue t v fv >>= return . (, fq)
                                 else return (fv, fq))
          return $ VRecord fields

        reconstructPathValue _ _ _ =
          throwAE anns $ RunTimeInterpretationError "Invalid path in bind writeback reconstruction"

        -- | Merge two records, restricting to the domain of the first record, and
        --   preferring values from the second argument for duplicates.
        mergeRecords :: Value -> Value -> Interpretation Value
        mergeRecords (VRecord r1) (VRecord r2) =
          mapBindings (\n v -> maybe (return v) return $ lookupBinding n r2) r1 >>= return . VRecord

        mergeRecords _ _ =
          throwAE anns $ RunTimeTypeError "Invalid bind record target"

    expr (details -> (EBindAs _,_,anns)) = throwAE anns $ RunTimeTypeError "Invalid Bind Construction"

    expr (annotations -> anns) = throwAE anns $ RunTimeInterpretationError "Invalid Expression"


{- Literal interpretation -}

literal :: K3 Literal -> Interpretation Value
literal (tag -> LBool b)   = return $ VBool b
literal (tag -> LByte b)   = return $ VByte b
literal (tag -> LInt i)    = return $ VInt i
literal (tag -> LReal r)   = return $ VReal r
literal (tag -> LString s) = return $ VString s
literal l@(tag -> LNone _) = return $ VOption (Nothing, vQualOfLit l)

literal (tag &&& children -> (LSome, [x])) = literal x >>= return . VOption . (, vQualOfLit x) . Just
literal (details -> (LSome, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid option literal"

literal (tag &&& children -> (LIndirect, [x])) = literal x >>= (\y -> (\i tg -> (i, vQualOfLit x, tg)) <$> liftIO (newMVar y) <*> memEntTag y) >>= return . VIndirection
literal (details -> (LIndirect, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid indirection literal"

literal (tag &&& children -> (LTuple, ch)) = mapM (\l -> literal l >>= return . (, vQualOfLit l)) ch >>= return . VTuple
literal (details -> (LTuple, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid tuple literal"

literal (tag &&& children -> (LRecord ids, ch)) = mapM (\l -> literal l >>= return . (, vQualOfLit l)) ch >>= return . VRecord . membersFromList . zip ids
literal (details -> (LRecord _, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid record literal"

literal (details -> (LEmpty _, [], anns)) =
  getComposedAnnotationL anns >>= maybe (emptyCollection annIds) emptyAnnotatedCollection
  where annIds = namedLAnnotations anns

literal (details -> (LEmpty _, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid empty literal"

literal (details -> (LCollection _, elems, anns)) = do
  cElems <- mapM literal elems
  realizationOpt <- getComposedAnnotationL anns
  case realizationOpt of
    Nothing       -> initialCollection (namedLAnnotations anns) cElems
    Just comboId  -> initialAnnotatedCollection comboId cElems

literal (details -> (LAddress, [h,p], anns)) = mapM literal [h,p] >>= \case 
  [VString a, VInt b] -> return . VAddress $ Address (a,b)
  _     -> throwAE anns $ RunTimeTypeError "Invalid address literal"

literal (details -> (LAddress, _, anns)) = throwAE anns $ RunTimeTypeError "Invalid address literal" 

literal (annotations -> anns) = throwAE anns $ RunTimeTypeError "Invalid literal"

{- Declaration interpretation -}

replaceTrigger :: (HasSpan a, HasUID a) => Identifier -> [a] -> Value -> Interpretation ()
replaceTrigger n _    (VFunction (f,_,tg))  = replaceE n (IVal $ VTrigger (n, Just f, tg))
replaceTrigger n anns _                     = throwAE anns $ RunTimeTypeError ("Invalid body for trigger " ++ n)

global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> Interpretation ()
global n (details -> (TSink, _, anns)) (Just e) = expression e >>= replaceTrigger n anns
global _ (details -> (TSink, _, anns)) Nothing  = throwAE anns $ RunTimeInterpretationError "Invalid sink trigger"
-- ^ Interpret and add sink triggers to the program environment.

-- | Sources have already been translated into K3 code
global _ (tag -> TSource) _ = return ()

-- | Functions have already been initialized as part of the program environment.
global _ (isFunction -> True) _ = return ()

-- | Add collection declaration, generating the collection type given by the annotation
--   combination on demand.
--   n is the name of the variable
global n t@(details -> (TCollection, _, anns)) eOpt = elemE n >>= \case
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
                            else collInitError
      _ -> collValError

    collInitError = throwAE anns . RunTimeTypeError $ "Invalid annotations on collection initializer for " ++ n
    collValError  = throwAE anns . RunTimeTypeError $ "Invalid collection value " ++ n

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
annotation n _ memberDecls = do
  (annMems, bindings) <- foldM initializeMembers
                                  (emptyMembers, emptyBindings) 
                                  [liftedAttrFuns, liftedAttrs, attrFuns, attrs]
  _ <- unbindMembers bindings
  void $ modifyADefs $ (:) (n, annMems)

  where
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

    (liftedAttrFuns, liftedAttrs) = ((True, isFunction), (True, not . isFunction))
    (attrFuns, attrs)             = ((False, isFunction), (False, not . isFunction))


annotationMember :: Identifier -> Bool -> (K3 Type -> Bool) -> AnnMemDecl 
                 -> Interpretation (Maybe (Identifier, (Value, VQualifier)))
annotationMember annId matchLifted matchF annMem = case (matchLifted, annMem) of
  (True,  Lifted    Provides n t (Just e) _)   | matchF t -> initializeMember n t e
  (False, Attribute Provides n t (Just e) _)   | matchF t -> initializeMember n t e
  (True,  Lifted    Provides n t Nothing  uid) | matchF t -> builtinLiftedAttribute annId n t uid >>= return . builtinQual t
  (False, Attribute Provides n t Nothing  uid) | matchF t -> builtinAttribute annId n t uid >>= return . builtinQual t
  _ -> return Nothing
  
  where initializeMember n t e = expression e >>= \v -> return . Just $ (n, (v, memberQual t))
        builtinQual t (Just (n,v)) = Just (n, (v, memberQual t))
        builtinQual _ Nothing      = Nothing
        memberQual t = case t @~ isTQualified of
          Just TMutable -> MemMut
          _             -> MemImmut
