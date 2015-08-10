{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Expression where

import Prelude hiding (any, concat)
import Control.Arrow ((&&&), (***), (>>>))
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
import Language.K3.Codegen.CPP.Materialization.Common
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
getMDecisions :: K3 Expression -> M.Map (Identifier, Direction) Method
getMDecisions e = case e @~ isEMaterialization of
                   Just (EMaterialization ms) -> ms
                   Nothing -> M.empty

getInDecisions :: K3 Expression -> M.Map Identifier Method
getInDecisions e = M.fromList [(i, m) | ((i, r), m) <- M.toList (getMDecisions e), r == In]

getExDecisions :: K3 Expression -> M.Map Identifier Method
getExDecisions e = M.fromList [(i, m) | ((i, r), m) <- M.toList (getMDecisions e), r == Ex]

getMethodFor :: Identifier -> Direction -> K3 Expression -> Method
getMethodFor i d e = M.findWithDefault defaultMethod (i, d) (getMDecisions e)

getInMethodFor :: Identifier -> K3 Expression -> Method
getInMethodFor i e = getMethodFor i In e

getExMethodFor :: Identifier -> K3 Expression -> Method
getExMethodFor i e = getMethodFor i Ex e

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

gMoveByDE :: Method -> K3 Expression -> R.Expression -> R.Expression
gMoveByDE m e x = if m == Moved then gMoveByE e x else x

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
  gEnv <- gets globals
  case lookup v gEnv of
    Just (tag -> TFunction, _) -> return ([], R.Project (R.Dereference $ R.Variable $ R.Name "this") (R.Name v))
    Just (tag -> TForall _, _) -> return ([], R.Project (R.Dereference $ R.Variable $ R.Name "this") (R.Name v))
    _ -> return ([], R.Variable (R.Name v))

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
    let reifyConstructorField (i, c, v) = do
          if getInMethodFor i e == Moved
            then do
              let needsMove = fromMaybe True (flip move c <$> getKTypeP c)
              let reifyModifier = if needsMove then R.RValueReference else id
              g <- genSym
              return ( [R.Forward $ R.ScalarDecl (R.Name g) (reifyModifier R.Inferred) (Just $ if needsMove then R.Move v else v)]
                     , R.Move $ R.Variable $ R.Name g)
            else
              return ([], v)
    (concat -> rs, vs') <- unzip <$> mapM reifyConstructorField (zip3 is cs vs)
    let vs'' = snd . unzip . sortBy (comparing fst) $ zip is vs'
    t <- getKType e
    case t of
        (tag &&& children -> (TRecord _, _)) -> do
            sig <- genCType t
            return (concat es ++ rs, R.Initialization sig vs'')
        _ -> throwE $ CPPGenE $ "Invalid Record Type " ++ show t

inline (tag &&& children -> (EOperate uop, [c])) = do
    (ce, cv) <- inline c
    usym <- unarySymbol uop
    return (ce, R.Unary usym cv)

inline (tag &&& children -> (EOperate OSeq, [a, b])) = do
    ae <- reify RForget a
    (be, bv) <- inline b
    return (ae ++ be, bv)

inline e@(tag -> ELambda _) = do
    resetApplyLevel

    let (unzip -> (argNames, fExprs), b) = rollLambdaChain e

    let formalArgNames = ["_A" ++ show i | i <- [0..] | _ <- argNames]

    -- TODO: Need return types, or allow inferral? Need it iff multiple branches return different
    -- but coercible types.
    -- formalArgTypes <- forM_ fExprs $ \f -> getKType f >>= \case
    --   (tag &&& children -> (TFunction, [ta, _])) -> return ta
    --   _ -> throwE $ CPPGenE "Invalid Function Form"

    returnType <- sequence $ fmap (genCType . last . children) (getKTypeP $ last fExprs)

    let getArg (tag -> ELambda i) = i
    let (outerFExpr, innerFExpr) = (head &&& last) fExprs
    let (outerArg, innerArg) = (getArg outerFExpr, getArg innerFExpr)

    let nrvo = getExMethodFor anon innerFExpr == Moved

    body <- reify (RReturn nrvo) b

    let captureByMtrlzn i m = case m of
          ConstReferenced -> R.ValueCapture (Just (i, Just $ R.CRef $ R.Variable $ R.Name i))
          Referenced -> R.RefCapture (Just (i, Nothing))
          Moved -> R.ValueCapture (Just (i, Just $ R.Move $ R.Variable $ R.Name i))
          Copied -> R.ValueCapture (Just (i, Nothing))

    let captures = [R.ValueCapture (Just ("this", Nothing))] ++
                   (M.elems $ M.mapWithKey captureByMtrlzn $ M.filterWithKey (\k _ -> k /= outerArg)
                            $ getInDecisions outerFExpr)

    let reifyArg a g = if a == "_" then [] else
          let reifyType = case (getInMethodFor a innerFExpr) of
                ConstReferenced -> R.Reference $ R.Const R.Inferred
                Referenced -> R.Reference R.Inferred
                _ -> R.Inferred
          in [R.Forward $ R.ScalarDecl (R.Name a) reifyType (Just $ R.Variable $ R.Name g)]

    let argReifications = concat [reifyArg a g | a <- argNames | g <- formalArgNames]

    let fullBody = argReifications ++ body

    let argList = [ (if fi == "_" then Nothing else Just ai, R.RValueReference R.Inferred)
                  | ai <- formalArgNames
                  | fi <- argNames
                  ]

    return ([], R.Lambda captures argList True returnType fullBody)

inline e@(tag &&& children -> (EOperate OApp, [(tag &&& children -> (EOperate OApp, [Fold c, f])), z])) = do
  (ce, cv) <- inline c
  (fe, fv) <- inline f
  (ze, zv) <- inline z

  pass <- case if forceMoveP e then Moved else getInMethodFor "!" e of
            Copied -> return zv
            Moved -> return (gMoveByE z zv)
            _ -> return zv

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

  fg <- genSym

  let loopInit = [R.Forward $ R.ScalarDecl (R.Name fg) (R.RValueReference R.Inferred) (Just fv), R.Forward $ R.ScalarDecl (R.Name acc) R.Inferred (Just pass)]
  let loopBody =
          [ R.Assignment (R.Variable $ R.Name acc) $
              R.Call (R.Variable $ R.Name fg) [ R.Move (R.Variable $ R.Name acc), R.Variable $ R.Name g]
          ]
  let loop = R.ForEach g (R.Reference $ R.Const $ R.Inferred) cv (R.Block loopBody)
  return (ce ++ fe ++ ze ++ loopInit ++ loopPragmas ++ [loop], R.Move $ R.Variable $ R.Name acc)

inline e@(tag -> EOperate OApp) = do
  -- Inline both function and argument for call.
  incApplyLevel

  let (f, as) = rollAppChain e
  (fe, fv) <- inline f
  let xs = map (last . children) as
  (unzip -> (xes, xvs)) <- mapM inline xs

  gs <- mapM (const genSym) xs

  let argDecl g x xv m = do
        gs <- gets globals
        return $ case x of
          (tag -> EVariable i) | i `elem` map fst gs -> ([], xv)
          _ -> ([R.Forward $ R.ScalarDecl (R.Name g) (R.RValueReference R.Inferred) (Just $ gMoveByDE (getInMethodFor "!" m) x xv)]
               , R.Variable $ R.Name g
               )

  (argDecls, argPasses) <- unzip <$> sequence [argDecl g x xv m | g <- gs | xv <- xvs | x <- xs | m <- as]

  let eName i = maybe (return i) (const $ getKType e >>= genCType >>= \rt -> return $ R.Specialized [rt] i)
                  (f @~ CArgs.isErrorFn)
  fv' <- case fv of
           R.Project s i -> R.Project s <$> eName i
           R.Variable i -> R.Variable <$> eName i
           _ -> return fv

  return (fe ++ concat xes ++ concat argDecls , R.Call fv' argPasses)

inline e@(tag &&& children -> (EOperate OSnd, [tag &&& children -> (ETuple, [trig@(tag -> EVariable tName), addr]), val])) = do
    d <- genSym
    d2 <- genSym
    tIdName <- case trig @~ isEProperty of
                 Just (EProperty (ePropertyValue -> Just (tag -> LString nm))) -> return nm
                 _ -> throwE $ CPPGenE $ "No trigger id property attached to " ++ tName
    (te, _)  <- inline trig
    (ae, av)  <- inline addr
    (ve, vv)  <- inline val
    let messageValue = gMoveByDE (getInMethodFor "!" e) val vv
    trigTypes <- getKType val >>= genCType
    let codec = R.Forward $ R.ScalarDecl (R.Name d2) (R.Static R.Inferred) $ Just $
               R.Call (R.Variable $ R.Qualified (R.Name "Codec") (R.Specialized [trigTypes] (R.Name "getCodec")))
               [R.Variable $ R.Name "__internal_format_"]
    let className = R.Specialized [trigTypes] (R.Name "TNativeValue")
        classInst = R.Forward $ R.ScalarDecl (R.Name d) R.Inferred
                      (Just $ R.Call (R.Variable $ R.Specialized [R.Named className]
                                           (R.Qualified (R.Name "std" ) $ R.Name "make_unique")) [messageValue])
        me = R.Variable $ R.Name "me"
    return (concat [te, ae, ve]
                 ++ [ classInst
                    , codec
                    , R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "__engine_") (R.Name "send"))
                                    [me, av, R.Variable $ R.Name tIdName,  R.Move $ R.Variable (R.Name d), (R.Variable $ R.Name d2) ]
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

reify RForget e@(tag &&& children -> (EOperate OApp, [(tag &&& children -> (EOperate OApp, [Fold _, _])), _])) = do
  (ee, _) <- inline e
  return ee

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
  ct <- getKType e
  let initD = getInMethodFor x lt
  ee <- reify (RDecl x (Just $ initD == Moved)) e
  be <- reify r b
  return [R.Block $ ee ++ be]

reify (RDecl i b) x@(tag -> ECaseOf _) = precludeRDecl i b x

-- case `e' of { some `x' -> `s' } { none -> `n' }
reify r k@(tag &&& children -> (ECaseOf x, [e, s, n])) = do
    let m = getInMethodFor x k

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
          case m of
            Copied -> [R.Forward $ R.ScalarDecl cx innerCType (Just initExpr)]
            Moved -> [R.Forward $ R.ScalarDecl cx innerCType (Just $ R.Move initExpr)]
            Referenced -> [R.Forward $ R.ScalarDecl cx (R.Reference innerCType) (Just initExpr)]
            ConstReferenced -> [R.Forward $ R.ScalarDecl cx (R.Reference $ R.Const innerCType) (Just initExpr)]

    let writeBackSome = case m of
                          Copied -> [R.Assignment (R.Variable $ R.Name initName) (R.Variable $ R.Name x)]
                          Moved -> [R.Assignment (R.Variable $ R.Name initName) (R.Move $ R.Variable $ R.Name x)]
                          _ -> []

    let writeBackNone = case m of
                          Copied -> [R.Assignment (R.Variable $ R.Name initName) (R.Literal R.LNullptr)]
                          Moved -> [R.Assignment (R.Variable $ R.Name initName) (R.Literal R.LNullptr)]
                          _ -> []

    -- If this case/of is the last expression in the current function and therefore returns,
    -- writeback must happen before the return takes place.
    (someE, noneE, returnDecl, returnStmt) <-
      case r of
        RReturn j | m == Copied || m == Moved -> do
          returnName <- genSym
          returnType <- getKType k >>= genCType
          let returnDecl = [R.Forward $ R.ScalarDecl (R.Name returnName) returnType Nothing]
          let returnStmt = if j then R.Move (R.Variable $ R.Name returnName) else (R.Variable $ R.Name returnName)
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
          BIndirection i -> initByDecision (getInMethodFor i k) i (R.Dereference initExpr)
          BTuple is ->
            concat [initByDecision (getInMethodFor i k) i (R.TGet initExpr n) | i <- is | n <- [0..]]
          BRecord iis ->
            concat [initByDecision (getInMethodFor i k) i (R.Project initExpr (R.Name f)) | (f, i) <- iis]

  let wbByDecision d old new =
        case d of
          Referenced -> []
          ConstReferenced -> []
          Moved -> [R.Assignment old (R.Move new)]
          Copied -> [R.Assignment old new]

  let bindWriteBack =
        case b of
          BIndirection i -> wbByDecision (getExMethodFor i k) initExpr (R.Variable $ R.Name i)
          BTuple is ->
            concat [wbByDecision (getExMethodFor i k) (R.TGet initExpr n) (R.Variable $ R.Name i) | i <- is | n <- [0..]]
          BRecord iis ->
            concat [wbByDecision (getExMethodFor i k) (R.Project initExpr (R.Name f)) (R.Variable $ R.Name i) | (f, i) <- iis]

  (bindBody, returnDecl, returnStmt) <-
    case r of
      RReturn m | any (\d -> d == Moved || d == Copied) (getExDecisions k) -> do
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
