{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Expression where

import Prelude hiding (any, concat)
import Control.Arrow ((&&&))
import Control.Monad.State

import Data.Foldable
import Data.List (nub, sortBy, (\\))
import Data.Maybe
import Data.Ord (comparing)
import Data.Tree

import qualified Data.Map as M
import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Codegen.Common
import Language.K3.Codegen.CPP.Materialization.Hints
import Language.K3.Codegen.CPP.Preprocessing
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import qualified Language.K3.Codegen.CPP.Representation as R

import Language.K3.Analysis.Core
import qualified Language.K3.Analysis.CArgs as CArgs

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
    | RReturn { byMove :: Bool }

    -- | Indicates that the calling context requires the callee's result to be stored in a variable
    -- of a pre-specified name.
    | RName { target :: Identifier, maybeByMove :: Maybe Bool}
    | RDecl { target :: Identifier, maybeByMove :: Maybe Bool}

    -- | A free-form reification context, for special cases.
    | RSplice ([CPPGenR] -> CPPGenR)

instance Show RContext where
    show RForget = "RForget"
    show (RReturn b) = "RReturn " ++ show b
    show (RName i b) = "RName \"" ++ i ++ "\"" ++ " " ++ show b
    show (RDecl i b) = "RDecl \"" ++ i ++ "\"" ++ " " ++ show b
    show (RSplice _) = "RSplice <opaque>"

-- Helper to default to forwarding when declaration pushdown is impossible.
precludeRDecl i b x = do
  xt <- getKType x
  fd <- cDecl xt i
  x' <- reify (RName i b) x
  return (fd ++ x')

-- | Patterns
-- TODO: Check for transformer property.
pattern Fold c <- Node (EProject "fold" :@: _) [c]

hasMoveProperty :: Annotation Expression -> Bool
hasMoveProperty ae = case ae of
                       (EProperty s) -> ePropertyName s == "Move"
                       _ -> False

forceMoveP :: K3 Expression -> Bool
forceMoveP e = isJust (e @~ hasMoveProperty)

-- Get the materializaitons of a given expression
getMDecisions :: K3 Expression -> M.Map Identifier Decision
getMDecisions e = case e @~ isEMaterialization of
                   Just (EMaterialization ms) -> ms
                   Nothing -> M.empty

-- Move heuristic to avoid code clutter.
move :: K3 Type -> K3 Expression -> Bool
move t e = moveByTypeForm t && moveByExprForm e
 where
  moveByTypeForm :: K3 Type -> Bool
  moveByTypeForm t =
    case t of
      (tag -> TString) -> True
      (tag &&& children -> (TTuple, cs)) -> any moveByTypeForm cs
      (tag &&& children -> (TRecord _, cs)) -> any moveByTypeForm cs
      (tag -> TCollection) -> True
      _ -> False

  moveByExprForm :: K3 Expression -> Bool
  moveByExprForm e =
    case e of
      (tag -> EVariable _) -> True
      (tag -> EProject _) -> True
      (tag -> ELetIn _) -> True
      (tag -> EBindAs _) -> True
      (tag -> ECaseOf _) -> True
      (tag -> EIfThenElse) -> True
      _ -> False

gMoveByE :: K3 Expression -> R.Expression -> R.Expression
gMoveByE e x = fromMaybe x (getKTypeP e >>= \t -> return $ if move t e then R.Move x else x)

gMoveByDE :: (Decision -> Method) -> Decision -> K3 Expression -> R.Expression -> R.Expression
gMoveByDE m d e x = if m d == Moved then gMoveByE e x else x

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
binarySymbol OConcat = return "+"
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
cDecl t i = genCType t >>= \ct -> return [R.Forward $ R.ScalarDecl (R.Name i) ct Nothing]

inline :: K3 Expression -> CPPGenM ([R.Statement], R.Expression)
inline e@(tag &&& annotations -> (EConstant (CEmpty t), as)) = case annotationComboIdE as of
    Nothing -> throwE $ CPPGenE $ "No Viable Annotation Combination for Empty " ++ show e
    Just ac -> genCType t >>= \ct -> return ([], R.Initialization (R.Collection ac ct) [])

inline (tag -> EConstant c) = constant c >>= \c' -> return ([], R.Literal c')

-- If a variable was declared as mutable it's been reified as a shared_ptr, and must be
-- dereferenced.
--
-- Add this binding to global functions.
inline e@(tag -> EVariable v) = do
  env <- get
  resetApplyLevel
  let cargs = CArgs.eCArgs e
  -- Check if we have a function, and not a builtin
  case lookup v (globals env) of
    Just (tag -> TFunction, False) | applyLevel env < cargs -> return ([], addBind v cargs)
    Just (tag -> TForall _, False) | applyLevel env < cargs -> return ([], addBind v cargs)
    _                              -> return ([], R.Variable defVar)
  where
    defVar = R.Name v
    addBind x n = R.Bind (R.TakeReference $ R.Variable $ R.Qualified (R.Name "__global_context") defVar)
                  [R.WRef $ R.Dereference $ R.Variable $ R.Name "this"] n

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
    mtrlzns <- case e @~ isEMaterialization of
                 Just (EMaterialization ms) -> return ms
                 Nothing -> return $ M.fromList [(i, defaultDecision) | i <- is]
    let vs' = [maybe v (\m -> gMoveByDE inD m c v) (M.lookup i mtrlzns) | c <- cs | v <- vs | i <- is]
    let vs'' = snd . unzip . sortBy (comparing fst) $ zip is vs'
    t <- getKType e
    case t of
        (tag &&& children -> (TRecord _, _)) -> do
            sig <- genCType t
            return (concat es, R.Initialization sig vs'')
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

    mtrlzns <- case e @~ isEMaterialization of
                 Just (EMaterialization ms) -> return ms
                 Nothing -> do
                   globVals <- fst . unzip . globals <$> get
                   let fvs = (nub $ filter (/= arg) $ freeVariables body) \\ globVals
                   return $ M.fromList [(i, defaultDecision) | i <- (arg:fvs)]

    ta <- getKType e >>= \case
          (tag &&& children -> (TFunction, [ta, tr])) -> genCInferredType ta
          _ -> throwE $ CPPGenE "Invalid Function Form"

    let thisCapture = R.ValueCapture (Just ("this", Nothing))

    let captureByMtrlzn i d = case inD d of
                                ConstReferenced -> R.ValueCapture (Just (i, Just $ R.CRef $ R.Variable $ R.Name i))
                                Referenced -> R.RefCapture (Just (i, Nothing))
                                Moved -> R.ValueCapture (Just (i, Just $ R.Move $ R.Variable $ R.Name i))
                                Copied -> R.ValueCapture (Just (i, Nothing))

    let captures = [R.ValueCapture (Just ("this", Nothing))] ++
                   (M.elems $ M.mapWithKey captureByMtrlzn $ M.filterWithKey (\k _ -> k /= arg) mtrlzns)

    let argMtrlznType = case inD (mtrlzns M.! arg) of
                          ConstReferenced -> R.Const (R.Reference ta)
                          Referenced -> R.Reference ta
                          _ -> ta

    let nrvo = outD (mtrlzns M.! arg) == Moved

    body' <- reify (RReturn nrvo) body

    return ([], R.Lambda captures [(arg, argMtrlznType)] True Nothing body')

inline e@(tag &&& children -> (EOperate OApp, [(tag &&& children -> (EOperate OApp, [Fold c, f])), z])) = do
  (ce, cv) <- inline c
  (fe, fv) <- inline f
  (ze, zv) <- inline z

  outerMtrlzn' <- case e @~ isEMaterialization of
                   Just (EMaterialization ms) -> return ms
                   Nothing -> return (M.fromList [("", defaultDecision)])

  let outerMtrlzn = M.adjust (\d -> if forceMoveP e then d { inD = Moved } else d) "" outerMtrlzn'

  pass <- case inD (fromJust (M.lookup "" outerMtrlzn)) of
            Copied -> return zv
            Moved -> return (gMoveByE z zv)

  let isVectorizeProp  = \case { EProperty (ePropertyName -> "Vectorize")  -> True; _ -> False }
  let isInterleaveProp = \case { EProperty (ePropertyName -> "Interleave") -> True; _ -> False }

  let vectorizePragma = case e @~ isVectorizeProp of
                          Nothing -> []
                          Just (EProperty (ePropertyValue -> Nothing)) -> [R.Pragma "clang vectorize(enable)"]
                          Just (EProperty (ePropertyValue -> Just (tag -> LInt i))) ->
                              [ R.Pragma "clang loop vectorize(enable)"
                              , R.Pragma $ "clang loop vectorize_width(" ++ show i ++ ")"
                              ]

  let interleavePragma = case e @~ isInterleaveProp of
                           Nothing -> []
                           Just (EProperty (ePropertyValue -> Nothing)) -> [R.Pragma "clang interleave(enable)"]
                           Just (EProperty (ePropertyValue -> Just (tag -> LInt i))) ->
                               [ R.Pragma "clang loop interleave(enable)"
                               , R.Pragma $ "clang loop interleave_count(" ++ show i ++ ")"
                               ]

  let loopPragmas = concat [vectorizePragma, interleavePragma]

  g <- genSym
  acc <- genSym

  let loopInit = [R.Forward $ R.ScalarDecl (R.Name acc) R.Inferred (Just pass)]
  let loopBody =
          [ R.Assignment (R.Variable $ R.Name acc) $
              R.Call (R.Call fv [ R.Move (R.Variable $ R.Name acc)]) [R.Variable $ R.Name g]
          ]
  let loop = R.ForEach g (R.Const $ R.Reference $ R.Inferred) cv (R.Block loopBody)
  return (ce ++ fe ++ ze ++ loopInit ++ loopPragmas ++ [loop], R.Move $ R.Variable $ R.Name acc)

inline e@(tag &&& children -> (EOperate OApp, [f, a])) = do
    -- Inline both function and argument for call.
    incApplyLevel
    let cargs = CArgs.eCArgs f
    (fe, fv) <- inline f
    (ae, av) <- inline a

    mtrlzns' <- case e @~ isEMaterialization of
                  Just (EMaterialization ms) -> return ms
                  Nothing -> return $ M.fromList [("", defaultDecision)]

    let mtrlzns = M.adjust (\d -> if forceMoveP e then d { inD = Moved } else d) "" mtrlzns'

    pass <- case inD (fromJust (M.lookup "" mtrlzns)) of
              Copied -> return av
              Moved -> return (gMoveByE a av)

    c <- call fv pass cargs
    return (fe ++ ae, c)
  where
    call fn@(R.Variable i) arg n =
      if isJust $ f @~ CArgs.isErrorFn
        then do
          kType <- getKType e
          returnType <- genCType kType
          return $ R.Call (R.Variable $ R.Specialized [returnType] i) [arg]
        else return $ R.Bind fn [arg] (n - 1)
    call fn arg n = return $ R.Bind fn [arg] (n - 1)

inline e@(tag &&& children -> (EOperate OSnd, [tag &&& children -> (ETuple, [trig@(tag -> EVariable tName), addr]), val])) = do
    d <- genSym
    let mtrlzns = getMDecisions e
    tIdName <- case trig @~ isEProperty of
                 Just (EProperty (ePropertyValue -> Just (tag -> LString nm))) -> return nm
                 _ -> throwE $ CPPGenE $ "No trigger id property attached to " ++ tName
    (te, _)  <- inline trig
    (ae, av)  <- inline addr
    (ve, vv)  <- inline val

    let messageValue = let m = M.findWithDefault defaultDecision "" mtrlzns in gMoveByDE inD m val vv
    trigList  <- triggers <$> get
    trigTypes <- getKType val >>= genCType
    let className = R.Specialized [trigTypes] (R.Qualified (R.Name "K3" ) $ R.Name "ValDispatcher")
        classInst = R.Forward $ R.ScalarDecl (R.Name d) R.Inferred
                      (Just $ R.Call (R.Variable $ R.Specialized [R.Named className]
                                           (R.Qualified (R.Name "std" ) $ R.Name "make_shared")) [messageValue])
    return (concat [te, ae, ve]
                 ++ [ classInst
                    , R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "__engine") (R.Name "send")) [
                                    av, R.Variable (R.Name $ tIdName), R.Variable $ R.Name d, R.Variable (R.Name "me")
                                   ]
                    ]
             , R.Initialization R.Unit [])
    where
      isETriggerId (EProperty (ePropertyName -> "TriggerId")) = True
      isETriggerId _ = False

inline (tag &&& children -> (EOperate bop, [a, b])) = do
    (ae, av) <- inline a
    (be, bv) <- inline b
    bsym <- binarySymbol bop
    return (ae ++ be, R.Binary bsym av bv)

inline e@(tag &&& children -> (EProject v, [k])) = do
    (ke, kv) <- inline k
    return (ke, R.Project kv (R.Name v))

inline (tag &&& children -> (EAssign x, [e])) = reify (RName x Nothing) e >>= \a -> return (a, R.Initialization R.Unit [])

inline (tag &&& children -> (EAddress, [h, p])) = do
    (he, hv) <- inline h
    (pe, pv) <- inline p
    return (he ++ pe, R.Call (R.Variable $ R.Name "make_address") [hv, pv])

inline e = do
    k <- genSym
    effects <- reify (RDecl k Nothing) e
    return (effects, R.Variable $ R.Name k)

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

reify (RDecl i b) x@(tag -> ELetIn _) = precludeRDecl i b x

reify r lt@(tag &&& children -> (ELetIn x, [e, b])) = do
  let mtrlzns = getMDecisions lt
  ct <- getKType e
  let initD = M.findWithDefault defaultDecision x mtrlzns
  ee <- reify (RDecl x (Just $ inD initD == Moved)) e
  be <- reify r b
  return [R.Block $ ee ++ be]

reify (RDecl i b) x@(tag -> ECaseOf _) = precludeRDecl i b x

-- case `e' of { some `x' -> `s' } { none -> `n' }
reify r k@(tag &&& children -> (ECaseOf x, [e, s, n])) = do
    mtrlzn <- case k @~ isEMaterialization of
                Just (EMaterialization ms) -> return $ ms M.! x
                Nothing -> return defaultDecision

    initKType <- getKType e
    initCType <- genCType initKType

    let innerCType = case initCType of
                       R.Pointer t -> t
                       R.SharedPointer t -> t
                       _ -> error "Invalid pointer type for case/of"

    -- Code to reify the case/of initializer, and the name of the temporary variable used.
    (initName, initReify) <-
      case tag e of
        EVariable k -> return (k, [])
        _ -> do
          g <- genSym
          ee <- reify (RName g Nothing) e
          return (g, [R.Forward $ R.ScalarDecl (R.Name g) initCType Nothing] ++ ee)

    let initExpr = R.Dereference $ R.Variable $ R.Name initName

    let cx = R.Name x

    let initSome =
          case inD mtrlzn of
            Copied -> [R.Forward $ R.ScalarDecl cx innerCType (Just initExpr)]
            Moved -> [R.Forward $ R.ScalarDecl cx innerCType (Just $ R.Move initExpr)]
            Referenced -> [R.Forward $ R.ScalarDecl cx (R.Reference innerCType) (Just initExpr)]
            ConstReferenced -> [R.Forward $ R.ScalarDecl cx (R.Const $ R.Reference innerCType) (Just initExpr)]

    let writeBackSome = case outD mtrlzn of
                          Copied -> [R.Assignment (R.Variable $ R.Name initName) (R.Variable $ R.Name x)]
                          Moved -> [R.Assignment (R.Variable $ R.Name initName) (R.Move $ R.Variable $ R.Name x)]
                          _ -> []

    let writeBackNone = case outD mtrlzn of
                          Copied -> [R.Assignment (R.Variable $ R.Name initName) (R.Literal R.LNullptr)]
                          Moved -> [R.Assignment (R.Variable $ R.Name initName) (R.Literal R.LNullptr)]
                          _ -> []

    -- If this case/of is the last expression in the current function and therefore returns,
    -- writeback must happen before the return takes place.
    (someE, noneE, returnDecl, returnStmt) <-
      case r of
        RReturn m | outD mtrlzn == Copied || outD mtrlzn == Moved -> do
          returnName <- genSym
          returnType <- getKType k >>= genCType
          let returnDecl = [R.Forward $ R.ScalarDecl (R.Name returnName) returnType Nothing]
          let returnStmt = if m then R.Move (R.Variable $ R.Name returnName) else (R.Variable $ R.Name returnName)
          someE <- reify (RName returnName Nothing) s
          noneE <- reify (RName returnName Nothing) n

          return (someE, noneE, returnDecl, [R.Return returnStmt])
        _ -> do
          someE <- reify r s
          noneE <- reify r n
          return (someE, noneE, [], [])

    return $ initReify ++ [R.IfThenElse (R.Variable $ R.Name initName)
                              (returnDecl ++ initSome ++ someE ++ writeBackSome ++ returnStmt)
                              (returnDecl ++ noneE ++ writeBackNone ++ returnStmt)]

reify (RDecl i b) x@(tag -> EBindAs _) = precludeRDecl i b x

reify r k@(tag &&& children -> (EBindAs b, [a, e])) = do
  let newNames =
        case b of
          BIndirection i -> [i]
          BTuple is -> is
          BRecord iis -> snd (unzip iis)

  mtrlzns <- case k @~ isEMaterialization of
                 Just (EMaterialization ms) -> return ms
                 Nothing -> return $ M.fromList (zip newNames $ repeat defaultDecision)

  initKType <- getKType a
  initCType <- genCType initKType

  (initName, initReify) <-
    case tag a of
      EVariable v -> return (v, [])
      _ -> do
        g <- genSym
        ee <- reify (RName g Nothing) a
        return (g, [R.Forward $ R.ScalarDecl (R.Name g) initCType Nothing] ++ ee)

  let initExpr = R.Variable (R.Name initName)

  let initSkeleton t m i e = [R.Forward $ R.ScalarDecl (R.Name i) (t R.Inferred) (Just $ m e)]

  let initByDecision d =
        case d of
          Referenced -> initSkeleton R.Reference id
          ConstReferenced -> initSkeleton (R.Const . R.Reference) id
          Moved -> initSkeleton id R.Move
          Copied -> initSkeleton id id

  let bindInit =
        case b of
          BIndirection i -> initByDecision (inD $ mtrlzns M.! i) i (R.Dereference initExpr)
          BTuple is ->
            concat [initByDecision (inD $ mtrlzns M.! i) i (R.TGet initExpr n) | i <- is | n <- [0..]]
          BRecord iis ->
            concat [initByDecision (inD $ mtrlzns M.! i) i (R.Project initExpr (R.Name f)) | (f, i) <- iis]

  let wbByDecision d old new =
        case d of
          Referenced -> []
          ConstReferenced -> []
          Moved -> [R.Assignment old (R.Move new)]
          Copied -> [R.Assignment old new]

  let bindWriteBack =
        case b of
          BIndirection i -> wbByDecision (outD $ mtrlzns M.! i) initExpr (R.Variable $ R.Name i)
          BTuple is ->
            concat [wbByDecision (outD $ mtrlzns M.! i) (R.TGet initExpr n) (R.Variable $ R.Name i) | i <- is | n <- [0..]]
          BRecord iis ->
            concat [wbByDecision (outD $ mtrlzns M.! i) (R.Project initExpr (R.Name f)) (R.Variable $ R.Name i) | (f, i) <- iis]

  (bindBody, returnDecl, returnStmt) <-
    case r of
      RReturn m | any (\d -> outD d == Moved || outD d == Copied) mtrlzns -> do
        returnName <- genSym
        returnType <- getKType k >>= genCType
        let returnDecl = [R.Forward $ R.ScalarDecl (R.Name returnName) returnType Nothing]
        let returnExpr = if m then R.Move (R.Variable $ R.Name returnName) else (R.Variable $ R.Name returnName)
        bindBody <- reify (RName returnName (Just m)) e
        return (bindBody, returnDecl, [R.Return returnExpr])
      _ -> do
        bindBody <- reify r e
        return (bindBody, [], [])

  return $ initReify ++ [R.Block $ bindInit ++ returnDecl ++ bindBody ++ bindWriteBack ++ returnStmt]

reify (RDecl i mb) x@(tag -> EIfThenElse) = precludeRDecl i mb x

reify r (tag &&& children -> (EIfThenElse, [p, t, e])) = do
    (pe, pv) <- inline p
    te <- reify r t
    ee <- reify r e
    return $ pe ++ [R.IfThenElse pv te ee]

-- | Catch-all case
reify r e = do
    (effects, value) <- inline e
    reification <- case r of
        RForget -> return []
        RName k b -> return [R.Assignment (R.Variable $ R.Name k)
                             (if fromMaybe False b then gMoveByE e value else value)]
        RDecl i b -> return [R.Forward $ R.ScalarDecl (R.Name i) (R.Inferred)
                             (Just $ if fromMaybe False b then gMoveByE e value else value)]
        RReturn b -> return $ [R.Return $ (if b then R.Move else id) value]
        RSplice _ -> throwE $ CPPGenE "Unsupported reification by splice."
    return $ effects ++ reification
