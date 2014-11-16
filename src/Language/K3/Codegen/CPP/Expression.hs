{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Expression where

import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Functor
import Data.List (nub, sortBy, (\\))
import Data.Maybe
import Data.Ord (comparing)
import Data.Tree

import Safe

import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Preprocessing
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import qualified Language.K3.Codegen.CPP.Representation as R

import qualified Language.K3.Analysis.CArgs as CArgs

import Language.K3.Transform.Hints

-- | The reification context passed to an expression determines how the result of that expression
-- will be stored in the generated code.
--
-- TODO: Add RAssign/RDeclare distinction.
data RContext

    -- | Indicates that the calling context will ignore the callee's result.
    = RForget

    -- | Indicates that the calling context is a C++ function, in which case the result may be
    -- 'returned' from the callee. A true payload indicates that the return value needs to be
    -- manually move wrapped
    | RReturn { moveReturn :: Bool }

    -- | Indicates that the calling context requires the callee's result to be stored in a variable
    -- of a pre-specified name.
    | RName Identifier

    -- | A free-form reification context, for special cases.
    | RSplice ([CPPGenR] -> CPPGenR)

instance Show RContext where
    show RForget = "RForget"
    show (RReturn b) = "RReturn " ++ show b
    show (RName i) = "RName \"" ++ i ++ "\""
    show (RSplice _) = "RSplice <opaque>"

attachTemplateVars :: Identifier -> K3 Expression -> [(Identifier, K3 Type)] -> CPPGenM R.Name
attachTemplateVars v e g
    | isJust (lookup v g) && isJust (functionType e)
        = do
            signatureType <- case fromJust (lookup v g) of
                                  t@(tag -> TFunction) -> return t
                                  (tag &&& children -> (TForall _, [t'])) -> return t'
                                  _ -> throwE $ CPPGenE "Unreachable Error."
            let ts = snd . unzip . dedup $ matchTrees signatureType $
                       fromMaybe (error "attachTemplateVars: expected just") $ functionType e
            cts <- mapM genCType ts
            return $ if null cts
               then R.Name v
               else R.Specialized cts $ R.Name v
    | otherwise = return $ R.Name v

dedup :: [(Identifier, a)] -> [(Identifier, a)]
dedup = foldl (\ds (t, u) -> if isJust (lookup t ds) then ds else ds ++ [(t, u)]) []

matchTrees :: K3 Type -> K3 Type -> [(Identifier, K3 Type)]
matchTrees (tag -> TDeclaredVar i) u = [(i, u)]
matchTrees (children -> ts) (children -> us) = concat $ zipWith matchTrees ts us

functionType :: K3 Expression -> Maybe (K3 Type)
functionType e = case e @~ \case { EType _ -> True; _ -> False } of
    Just (EType t@(tag -> TFunction)) -> Just t
    _ -> Nothing


-- | Patterns
-- TODO: Check for transformer property.
pattern Fold c <- Node (EProject "fold" :@: _) [c]

-- | Realization of unary operators.
unarySymbol :: Operator -> CPPGenM Identifier
unarySymbol ONot = return "!"
unarySymbol ONeg = return "-"
unarySymbol u = throwE $ CPPGenE $ "Invalid Unary Operator " ++ show u

-- | Realization of binary operators.
binarySymbol :: Operator -> CPPGenM Identifier
binarySymbol OAdd = return "+"
binarySymbol OSub = return "-"
binarySymbol OMul = return "*"
binarySymbol ODiv = return "/"
binarySymbol OMod = return "%" -- TODO: type based selection of % vs fmod
binarySymbol OEqu = return "=="
binarySymbol ONeq = return "!="
binarySymbol OLth = return "<"
binarySymbol OLeq = return "<="
binarySymbol OGth = return ">"
binarySymbol OGeq = return ">="
binarySymbol OAnd = return "&&"
binarySymbol OOr = return "||"
binarySymbol b = throwE $ CPPGenE $ "Invalid Binary Operator " ++ show b

-- | Realization of constants.
constant :: Constant -> CPPGenM R.Literal
constant (CBool True) = return $ R.LBool True
constant (CBool False) = return $ R.LBool False
constant (CInt i) = return $ R.LInt i
constant (CReal d) = return $ R.LDouble d
constant (CString s) = return $ R.LString s
constant (CNone _) = return R.LNullptr
constant c = throwE $ CPPGenE $ "Invalid Constant Form " ++ show c

cDecl :: K3 Type -> Identifier -> CPPGenM [R.Statement]
cDecl (tag &&& children -> (TFunction, [ta, tr])) i = do
    ctr <- genCType tr
    cta <- genCType ta
    return [R.Forward $ R.FunctionDecl (R.Name i) [cta] ctr]
cDecl t i = do
    ct <- genCType t
    return [R.Forward $ R.ScalarDecl (R.Name i) ct Nothing]

inline :: K3 Expression -> CPPGenM ([R.Statement], R.Expression)
inline e@(tag &&& annotations -> (EConstant (CEmpty t), as)) = case annotationComboIdE as of
    Nothing -> throwE $ CPPGenE $ "No Viable Annotation Combination for Empty " ++ show e
    Just ac -> do
        ct <- genCType t
        return ([], R.Initialization (R.Collection ac ct) [])

inline (tag -> EConstant c) = constant c >>= \c' -> return ([], R.Literal c')

-- If a variable was declared as mutable it's been reified as a shared_ptr, and must be
-- dereferenced.
-- Add this binding to global functions. TODO: handle shadowing by locals
inline e@(tag -> EVariable v) = do
  env <- get
  resetApplyLevel
  let cargs = CArgs.eCArgs e
  -- Check if we have a function, and not a builtin
  case lookup v (globals env) of
    Just (tag -> TFunction, False) | applyLevel env < cargs -> return ([], addBind v cargs)
    Just (tag -> TForall _, False) | applyLevel env < cargs -> return ([], addBind v cargs)
    _                              -> return ([], defVar)
  where
    defVar = R.Variable $ R.Name v
    addBind x n = R.Call stdBind [addContext x, stdRefThis, placeHolder n]

    stdBind = R.Variable $ R.Qualified (R.Name "std") (R.Name "bind")
    addContext x = R.Variable $ R.Qualified (R.Name "&__global_context") $ R.Name x
    stdRefThis = R.Call (R.Variable (R.Qualified (R.Name "std") $ R.Name "ref")) [R.Dereference $ R.Variable $ R.Name "this"]
    placeHolder i = R.Variable $ R.Qualified (R.Qualified (R.Name "std") $ R.Name "placeholders") $ R.Name $ "_"++show i

inline (tag &&& children -> (t', [c])) | t' == ESome || t' == EIndirect = do
    (e, v) <- inline c
    ct <- getKType c
    t <- genCType ct
    return (e, R.Call (R.Variable $ R.Specialized [t] (R.Name "make_shared")) [v])

inline (tag &&& children -> (ETuple, [])) = return ([], R.Initialization R.Unit [])

inline (tag &&& children -> (ETuple, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    return (concat es, R.Call (R.Variable $ R.Name "make_tuple") vs)

inline e@(tag &&& children -> (ERecord is, cs)) = do
    (es, vs) <- unzip <$> mapM inline cs
    let vs' = snd . unzip . sortBy (comparing fst) $ zip is vs
    t <- getKType e
    case t of
        (tag &&& children -> (TRecord _, _)) -> do
            sig <- genCType t
            return (concat es, R.Initialization sig vs')
        _ -> throwE $ CPPGenE $ "Invalid Record Type " ++ show t

inline (tag &&& children -> (EOperate uop, [c])) = do
    (ce, cv) <- inline c
    usym <- unarySymbol uop
    return (ce, R.Unary usym cv)

inline (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    (be, bv) <- inline b
    return (ae ++ be, bv)

inline e@(tag &&& children -> (ELambda arg, [body])) = do
    resetApplyLevel
    let (EOpt (FuncHint readOnly)) = fromMaybe (EOpt (FuncHint False))
                                     (e @~ \case { EOpt (FuncHint _) -> True; _ -> False})

    (cRef, cMove, cCopy) <- case (e @~ \case { EOpt (CaptHint _) -> True; _ -> False }) of
                              Just (EOpt (CaptHint ch)) -> return ch
                              -- If we have no capture hints, we need to do the heavy work ourselves
                              _ -> do
                                globVals <- fst . unzip . globals <$> get
                                let fvs = nub $ filter (/= arg) $ freeVariables body
                                return $ (S.empty, S.empty, S.fromList $ fvs \\ globVals)

    (ta, _) <- getKType e >>= \case
        (tag &&& children -> (TFunction, [ta, tr])) -> do
            ta' <- genCInferredType ta
            tr' <- genCInferredType tr

            return (ta', tr')
        _ -> throwE $ CPPGenE "Invalid Function Form"
    let thisCapture = R.ValueCapture (Just ("this", Nothing))
    let refCapture = S.map (\s -> R.RefCapture $ Just (s, Nothing)) cRef
    let moveCapture = S.map (\s -> R.ValueCapture $
                                   Just (s, Just $ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "move"))
                                              [R.Variable $ R.Name s])) cMove
    let copyCapture = S.map (\s -> R.ValueCapture $ Just (s, Nothing)) cCopy
    let capture = thisCapture : (S.toList $ S.unions [refCapture, moveCapture, copyCapture])

    let nrvoHint = case (e @~ \case { EOpt (ReturnMoveHint _) -> True; _ -> False }) of
                     Just (EOpt (ReturnMoveHint b)) -> b
                     _ -> False

    body' <- reify (RReturn nrvoHint) body
    -- TODO: Handle `mutable' arguments.
    let hintedArgType = if readOnly then R.Const (R.Reference ta) else ta
    return ( []
           , R.Lambda capture [(arg, hintedArgType)] True Nothing body'
           )

inline e@(flattenApplicationE -> (tag &&& children -> (EOperate OApp, [Fold c, f, z]))) = do
  (ce, cv) <- inline c
  (fe, fv) <- inline f
  (ze, zv) <- inline z

  let vectorizePragma = case e @~ (\case { EProperty "Vectorize" _ -> True; _ -> False }) of
                          Nothing -> []
                          Just (EProperty _ Nothing) -> [R.Pragma "clang vectorize(enable)"]
                          Just (EProperty _ (Just (tag -> LInt i))) ->
                              [ R.Pragma "clang loop vectorize(enable)"
                              , R.Pragma $ "clang loop vectorize_width(" ++ show i ++ ")"
                              ]

  let interleavePragma = case e @~ (\case { EProperty "Interleave" _ -> True; _ -> False }) of
                           Nothing -> []
                           Just (EProperty _ Nothing) -> [R.Pragma "clang interleave(enable)"]
                           Just (EProperty _ (Just (tag -> LInt i))) ->
                               [ R.Pragma "clang loop interleave(enable)"
                               , R.Pragma $ "clang loop interleave_count(" ++ show i ++ ")"
                               ]

  let loopPragmas = concat [vectorizePragma, interleavePragma]

  g <- genSym
  acc <- genSym

  zt <- getKType z
  accType <- genCType zt

  let loopInit = [R.Forward $ R.ScalarDecl (R.Name acc) accType (Just zv)]
  let loopBody =
          [ R.Assignment (R.Variable $ R.Name acc) $
              R.Call
                (R.Call fv
                  [ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "move"))
                    [R.Variable $ R.Name acc]
                  ]) [R.Variable $ R.Name g]
          ]
  let loop = R.ForEach g R.Inferred cv (R.Block loopBody)
  return (ce ++ fe ++ ze ++ loopInit ++ loopPragmas ++ [loop], (R.Variable $ R.Name acc))

inline e@(flattenApplicationE -> (tag &&& children -> (EOperate OApp, (f:as)))) = do
    -- Inline both function and argument for call.
    incApplyLevel
    (fe, fv) <- inline f
    (aes, avs) <- unzip <$> mapM inline as
    c <- call fv (movedArgs avs)
    return (fe ++ concat aes, c)
  where
    movedArgs args = [ if noMove
                           then arg
                           else R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "move")) [arg]
                     | arg <- args
                     | EOpt (PassHint noMove) <- passHints
                     ]
    call fn@(R.Variable n) args =
      if isJust $ f @~ CArgs.isErrorFn
        then do
          kType <- getKType e
          returnType <- genCType kType
          return $ R.Call (R.Variable $ R.Specialized [returnType] n) args
        else return $ R.Call fn args
    call fn args = return $ R.Call fn args

    getPassHint c = fromMaybe (EOpt (PassHint True)) (c @~ \case { EOpt (PassHint _) -> True; _ -> False})
    passHints = map getPassHint as


inline (tag &&& children -> (EOperate OSnd, [tag &&& children -> (ETuple, [trig@(tag -> EVariable tName), addr]), val])) = do
    d <- genSym
    (te, _)  <- inline trig
    (ae, av)  <- inline addr
    (ve, vv)  <- inline val
    trigList  <- triggers <$> get
    trigTypes <- getKType val >>= genCType
    let className = R.Specialized [trigTypes] (R.Qualified (R.Name "K3" )$ R.Name "ValDispatcher")
        classInst = R.Forward $ R.ScalarDecl (R.Name d) R.Inferred
                      (Just $ R.Call (R.Variable $ R.Specialized [R.Named className]
                                           (R.Qualified (R.Name "std" )$ R.Name "make_shared")) [vv])
    return (concat [te, ae, ve]
                 ++ [ classInst
                    , R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "__engine") (R.Name "send")) [
                                    av, R.Variable (R.Name $ tName), R.Variable (R.Name d)
                                   ]
                    ]
             , R.Initialization R.Unit [])

inline (tag &&& children -> (EOperate bop, [a, b])) = do
    (ae, av) <- inline a
    (be, bv) <- inline b
    bsym <- binarySymbol bop
    return (ae ++ be, R.Binary bsym av bv)

inline e@(tag &&& children -> (EProject v, [k])) = do
    (ke, kv) <- inline k
    return (ke, R.Project kv (R.Name v))

inline (tag &&& children -> (EAssign x, [e])) = reify (RName x) e >>= \a -> return (a, R.Initialization R.Unit [])

inline (tag &&& children -> (EAddress, [h, p])) = do
    (he, hv) <- inline h
    (pe, pv) <- inline p
    return (he ++ pe, R.Call (R.Variable $ R.Name "make_address") [hv, pv])

inline e = do
    k <- genSym
    ct <- getKType e
    decl <- cDecl ct k
    effects <- reify (RName k) e
    return (decl ++ effects, R.Variable $ R.Name k)

-- | The generic function to generate code for an expression whose result is to be reified. The
-- method of reification is indicated by the @RContext@ argument.
reify :: RContext -> K3 Expression -> CPPGenM [R.Statement]

-- TODO: Is this the fix we need for the unnecessary reification issues?
reify RForget e@(tag -> EOperate OApp) = do
    (ee, ev) <- inline e
    return $ ee ++ [R.Ignore ev]

reify r (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    be <- reify r b
    return $ ae ++ be

reify r (tag &&& children -> (ELetIn x, [e, b])) = do
    -- TODO: Push declaration into reification.
    ct <- getKType e
    d <- cDecl ct x
    ee <- reify (RName x) e
    be <- reify r b
    return [R.Block $ d ++ ee ++ be]

-- case `e' of { some `x' -> `s' } { none -> `n' }
reify r k@(tag &&& children -> (ECaseOf x, [e, s, n])) = do
    let (refBinds, copyBinds, writeBinds)
            = case (k @~ \case { EOpt (BindHint _) -> True; _ -> False}) of
                Just (EOpt (BindHint ch)) -> ch
                _ -> (S.empty, S.empty, S.singleton x)

    let isCopyBound i = i `S.member` copyBinds || i `S.member` writeBinds
    let isWriteBound i = i `S.member` writeBinds

    -- Create types for the element and the pointer to said element
    ept <- getKType e
    epc <- genCType ept
    let et = headNote ("Missing type in reify for " ++ show e) $ children ept
    ec  <- genCType et

    (g, gd, ee) <- case tag e of
           -- Reuse an existing variable
           EVariable k -> return (k, [], [])
           _ -> do
             g  <- genSym
             ee <- reify (RName g) e
             return (g, [R.Forward $ R.ScalarDecl (R.Name g) epc Nothing], ee)

    se <- reify r s
    ne <- reify r n

    let d = [R.Forward $ R.ScalarDecl (R.Name x) (if isWriteBound x then R.Inferred else R.Reference R.Inferred)
               (Just $ R.Dereference (R.Variable $ R.Name g))]

    let reconstruct = [R.Assignment (R.Variable $ R.Name g)
                            (R.Call (R.Variable $ R.Qualified (R.Name "std") $ R.Specialized [ec]
                                          (R.Name "make_shared")) [R.Variable $ R.Name x]) | isWriteBound x]

    return $ gd ++ ee ++ [R.IfThenElse (R.Variable $ R.Name g) (d ++ se ++ reconstruct) ne]

reify r k@(tag &&& children -> (EBindAs b, [a, e])) = do
    let (refBinds, copyBinds, writeBinds)
            = case (k @~ \case { EOpt (BindHint _) -> True; _ -> False}) of
                Just (EOpt (BindHint (r, c, w))) -> (r, c, w)
                _ -> (S.empty, S.empty, S.fromList $ bindingVariables b)

    let isCopyBound i = i `S.member` copyBinds || i `S.member` writeBinds
    let isWriteBound i = i `S.member` writeBinds

    (ae, g) <- case a of
        (tag -> EVariable _) -> inline a
        _ -> do
            g' <- genSym
            ta <- getKType a
            da <- cDecl ta g'
            ae' <- reify (RName g') a
            return (da ++ ae', R.Variable $ R.Name g')

    ta <- getKType a

    bindInit <- case b of
            BIndirection i -> do
                let (tag &&& children -> (TIndirection, [ti])) = ta
                let bt = if isCopyBound i then R.Inferred else R.Reference R.Inferred
                return [R.Forward $ R.ScalarDecl (R.Name i) bt (Just $ R.Dereference g)]
            BTuple is ->
                return [ R.Forward $ R.ScalarDecl (R.Name i)
                           (if isCopyBound i then R.Inferred else R.Reference R.Inferred)
                           (Just $ R.Call (R.Variable $ R.Qualified (R.Name "std")
                           (R.Specialized [R.TypeLit $ R.LInt n] $ R.Name "get")) [g])
                       | i <- is
                       | n <- [0..]
                       ]
            BRecord iis -> return [ R.Forward $ R.ScalarDecl (R.Name v)
                                      (if isCopyBound v then R.Inferred else R.Reference R.Inferred)
                                      (Just $ R.Project g (R.Name i))
                                  | (i, v) <- iis]

    let bindWriteback = case b of
            BIndirection i -> [ R.Assignment (R.Dereference g) (R.Variable $ R.Name i) | not (isWriteBound i)]
            BTuple is -> [genTupleAssign g n i | i <- is, isWriteBound i | n <- [0..]]
            BRecord iis -> [genRecordAssign g k v | (k, v) <- iis, isWriteBound v]

    (bindBody, k) <- case r of
        RReturn _ -> do
            g' <- genSym
            te <- getKType e
            de <- cDecl te g'
            re <- reify (RName g') e
            return (de ++ re, Just g')
        _ -> (,Nothing) <$> reify r e

    let bindCleanUp = maybe [] (\k' -> [R.Return (R.Variable $ R.Name k')]) k

    return $ ae ++ [R.Block $ bindInit ++ bindBody ++ bindWriteback ++ bindCleanUp]
  where
    genTupleAssign :: R.Expression -> Int -> Identifier -> R.Statement
    genTupleAssign g n i =
        R.Assignment (R.Call (R.Variable $ R.Specialized [R.Named $ R.Name (show n)] (R.Name "get")) [g])
                     (R.Variable $ R.Name i)
    genRecordAssign g k v = R.Assignment (R.Project g (R.Name k)) (R.Variable $ R.Name v)

reify r (tag &&& children -> (EIfThenElse, [p, t, e])) = do
    (pe, pv) <- inline p
    te <- reify r t
    ee <- reify r e
    return $ pe ++ [R.IfThenElse pv te ee]

reify r e = do
    (effects, value) <- inline e
    reification <- case r of
        RForget -> return []
        RName k -> return [R.Assignment (R.Variable $ R.Name k) value]
        RReturn True -> return [R.Return $ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "move")) [value]]
        RReturn False -> return [R.Return value]
        RSplice _ -> throwE $ CPPGenE "Unsupported reification by splice."
    return $ effects ++ reification
