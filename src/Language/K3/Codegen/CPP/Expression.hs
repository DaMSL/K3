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
import Data.Traversable

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
    | RName { targetN :: R.Expression, maybeByMove :: Maybe Bool}
    | RDecl { targetD :: Identifier, maybeByMove :: Maybe Bool}

    -- | A free-form reification context, for special cases.
    | RSplice ([CPPGenR] -> CPPGenR)

instance Show RContext where
    show RForget = "RForget"
    show (RReturn b) = "RReturn " ++ show b
    show (RName i b) = "RName \"" ++ (show i) ++ "\"" ++ " " ++ show b
    show (RDecl i b) = "RDecl \"" ++ (show i) ++ "\"" ++ " " ++ show b
    show (RSplice _) = "RSplice <opaque>"

-- Helper to default to forwarding when declaration pushdown is impossible.
precludeRDecl i b x = do
  xt <- getKType x
  fd <- cDecl xt i
  x' <- reify (RName (R.Variable $ R.Name i) b) x
  return (fd ++ x')

-- | Template Patterns
-- TODO: Check for transformer property.
pattern Fold c <- Node (EProject "fold" :@: _) [c]
pattern InsertWith c <- Node (EProject "insert_with" :@: _) [c]
pattern UpsertWith c <- Node (EProject "upsert_with" :@: _) [c]
pattern Lookup c <- Node (EProject "lookup" :@: _) [c]
pattern Peek c <- Node (EProject "peek" :@: _) [c]
pattern SafeAt c <- Node (EProject "safe_at" :@: _) [c]

dataspaceIn :: K3 Expression -> [Identifier] -> Bool
dataspaceIn e as = isJust $ getKTypeP e >>= \t -> t @~ \case { TAnnotation i -> i `elem` as; _ -> False }

stlLinearDSs :: [Identifier]
stlLinearDSs = ["Collection", "Set", "Vector", "Seq"]

stlAssocDSs :: [Identifier]
stlAssocDSs = ["Map"]

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

-- Heuristics
needsMoveCast :: K3 Type -> R.Expression -> Bool
needsMoveCast t e = isNonScalarType t && (not $ R.isMoveInferred e)

-- Move heuristic to avoid code clutter.
move :: K3 Type -> K3 Expression -> Bool
move t e = isNonScalarType t && moveByExprForm e
 where
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
gMoveByDE m e x
  | m == Moved = gMoveByE e x
  | otherwise = x

forwardBy :: R.Expression -> R.Expression
forwardBy x = case x of
  R.Variable _ -> R.FMacro x
  R.Project x' f -> R.Project (forwardBy x') f
  _ -> x

passBy :: Method -> K3 Expression -> R.Expression -> R.Expression
passBy m e x = case m of
  Moved -> gMoveByE e x
  Forwarded -> forwardBy x
  _ -> x

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
          let orderAgnosticP = R.isOrderAgnostic v
          if getInMethodFor i e == Moved
            then do
              castMoveP <- needsMoveCast <$> (getKType c) <*> (pure v)
              let moveCastModifier = if castMoveP then R.Move else id

              if orderAgnosticP
                then return ([], moveCastModifier v)
                else do
                  g <- genSym
                  return ( [R.Forward $ R.ScalarDecl (R.Name g) (R.RValueReference R.Inferred) (Just $ moveCastModifier v)]
                         , R.FMacro $ R.Variable $ R.Name g
                         )
            else do
              if orderAgnosticP
                then return ([], v)
                else do
                  g <- genSym
                  return ( [R.Forward $ R.ScalarDecl (R.Name g) (R.RValueReference R.Inferred) (Just v)]
                         , R.FMacro $ R.Variable $ R.Name g
                         )

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
    let isAccumulating = isJust $ e @~ (\case { EProperty (ePropertyName -> "AccumulatingTransformer") -> True; _ -> False })

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
    let (outerArg, _) = (getArg outerFExpr, getArg innerFExpr)

    -- let nrvo = getExMethodFor anon innerFExpr == Moved

    body <- reify (if isAccumulating then RForget else RReturn False) b

    let captureByMtrlzn i m = case m of
          ConstReferenced -> R.RefCapture (Just (i, Nothing))
          Referenced -> R.RefCapture (Just (i, Nothing))
          Moved -> R.ValueCapture (Just (i, Just $ R.Move $ R.Variable $ R.Name i))
          Copied -> R.ValueCapture (Just (i, Nothing))

    let captures = [R.ValueCapture (Just ("this", Nothing))] ++
                   (M.elems $ M.mapWithKey captureByMtrlzn $ M.filterWithKey (\k _ -> k /= outerArg)
                            $ getInDecisions outerFExpr)

    let reifyArg a g = if a == "_" then [] else
          let reifyType = case if a == head (argNames) && isAccumulating then Referenced else (getInMethodFor a innerFExpr) of
                  ConstReferenced -> R.Reference $ R.Const R.Inferred
                  Referenced -> R.Reference R.Inferred
                  Forwarded -> R.RValueReference R.Inferred
                  _ -> R.Inferred
          in [R.Forward $ R.ScalarDecl (R.Name a) reifyType
               (Just $ R.SForward (R.ConstExpr $ R.Call (R.Variable $ R.Name "decltype") [R.Variable $ R.Name g])
                         (R.Variable $ R.Name g))]

    let argReifications = concat [reifyArg a g | a <- argNames | g <- formalArgNames]

    let fullBody = argReifications ++ body

    let argList = [ (if fi == "_" then Nothing else Just ai, R.RValueReference R.Inferred)
                  | ai <- formalArgNames
                  | fi <- argNames
                  ]

    return ([], R.Lambda captures argList True (if isAccumulating then Just R.Void else returnType) fullBody)

inline e@(tag &&& children -> (EOperate OApp, [(tag &&& children -> (EOperate OApp, [p@(Fold c), f])), z])) = do
  (ce, cv) <- inline c
  (ze, zv) <- inline z

  let isAccumulating = isJust $ p @~ (\case { EProperty (ePropertyName -> "AccumulatingTransformer") -> True; _ -> False })

  eleMove <- getKType c >>= \(tag &&& children -> (TCollection, [t])) -> return $ getInMethodFor anon p == Moved && isNonScalarType t

  let accMove = gMoveByDE (if forceMoveP e then Moved else getInMethodFor anon e) z zv

  accVar <- genSym
  eleVar <- genSym

  let accDecl = R.Forward $ R.ScalarDecl (R.Name accVar) R.Inferred (Just accMove)

  (fe, fb) <- inlineApply (if isAccumulating then RForget else RName (R.Variable $ R.Name accVar) (Just True)) f
                [R.Variable $ R.Name accVar, (if eleMove then R.Move else id) $ R.Variable $ R.Name eleVar]

  let loopBody = fb

  let loop = R.ForEach eleVar (R.Reference R.Inferred) cv (R.Block loopBody)

  let bb = if null fe then loop else R.Block (fe ++ [loop])

  return (ze ++ [accDecl] ++ ce ++ [bb], R.Variable $ R.Name accVar)

  -- Leaving this here for later.
  -- let isVectorizeProp  = \case { EProperty (ePropertyName -> "Vectorize")  -> True; _ -> False }
  -- let isInterleaveProp = \case { EProperty (ePropertyName -> "Interleave") -> True; _ -> False }

  -- let vectorizePragma = case e @~ isVectorizeProp of
  --                         Nothing -> []
  --                         Just (EProperty (ePropertyValue -> Nothing)) -> [R.Pragma "clang vectorize(enable)"]
  --                         Just (EProperty (ePropertyValue -> Just (tag -> LInt i))) ->
  --                             [ R.Pragma "clang loop vectorize(enable)"
  --                             , R.Pragma $ "clang loop vectorize_width(" ++ show i ++ ")"
  --                             ]

  -- let interleavePragma = case e @~ isInterleaveProp of
  --                          Nothing -> []
  --                          Just (EProperty (ePropertyValue -> Nothing)) -> [R.Pragma "clang interleave(enable)"]
  --                          Just (EProperty (ePropertyValue -> Just (tag -> LInt i))) ->
  --                              [ R.Pragma "clang loop interleave(enable)"
  --                              , R.Pragma $ "clang loop interleave_count(" ++ show i ++ ")"
  --                              ]

inline e@(tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
           p@(InsertWith c),
           k])),
           w])) | c `dataspaceIn` stlAssocDSs = do
  (ce, cv) <- inline c
  kg <- genSym
  ke <- reify (RDecl kg Nothing) k
  let kv = R.Project (R.Variable $ R.Name kg) (R.Name "key")

  let uc = R.Call (R.Project cv (R.Name "getContainer")) []

  existing <- genSym
  let existingFind = R.Call (R.Project uc (R.Name "find")) [kv]
  let existingDecl = R.Forward $ R.ScalarDecl (R.Name existing) R.Inferred (Just $ existingFind)
  let existingPred = R.Binary "=="
                       (R.Variable $ R.Name existing)
                       (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uc])

  let nfe = [R.Assignment (R.Subscript uc kv) (R.Variable $ R.Name kg)]
  (wfe, wfb) <- inlineApply (RName (R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")) (Just True)) w
                  [R.Move $ R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second"), (R.Variable $ R.Name kg)]

  return (ce ++ ke ++ [existingDecl] ++ [R.IfThenElse existingPred nfe (wfe ++ wfb)]
         , R.Initialization R.Unit [])


inline e@(tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
           p@(UpsertWith c),
           k])),
           n])),
           w])) | c `dataspaceIn` stlAssocDSs = do
  (ce, cv) <- inline c

  kg <- genSym
  ke <- reify (RDecl kg Nothing) k
  let kv = R.Project (R.Variable $ R.Name kg) (R.Name "key")
  -- (ke, kv) <- case k of
  --   (tag &&& children -> (ERecord fs, cs)) -> case lookup "key" (zip fs cs) of
  --     Nothing -> throwE $ CPPGenE "Missing key field on record."
  --     Just kc -> inline kc
  --   _ -> inline k >>= \(ke', kv') -> return (ke', R.Project kv' (R.Name "key"))
  let uc = R.Call (R.Project cv (R.Name "getContainer")) []

  existing <- genSym
  let existingFind = R.Call (R.Project uc (R.Name "find")) [kv]
  let existingDecl = R.Forward $ R.ScalarDecl (R.Name existing) R.Inferred (Just $ existingFind)
  let existingPred = R.Binary "=="
                       (R.Variable $ R.Name existing)
                       (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uc])

  (nfe, nfb) <- inlineApply (RName (R.Subscript uc kv) (Just True)) n [R.Initialization R.Unit []]
  (wfe, wfb) <- inlineApply (RName (R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")) (Just True)) w
                  [R.Move $ R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")]

  return (ce ++ ke ++ [existingDecl] ++ [R.IfThenElse existingPred (nfe ++ nfb) (wfe ++ wfb)]
         , R.Initialization R.Unit [])

inline e@(tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
           p@(Lookup c),
           k])),
           n])),
           w])) | c `dataspaceIn` stlAssocDSs = do
  (ce, cv) <- inline c
  kg <- genSym
  ke <- reify (RDecl kg Nothing) k
  let kv = R.Project (R.Variable $ R.Name kg) (R.Name "key")
  -- (ke, kv) <- case k of
  --   (tag &&& children -> (ERecord fs, cs)) -> case lookup "key" (zip fs cs) of
  --     Nothing -> throwE $ CPPGenE "Missing key field on record."
  --     Just kc -> inline kc
  --   _ -> inline k >>= \(ke', kv') -> return (ke', R.Project kv' (R.Name "key"))
  let uc = R.Call (R.Project cv (R.Name "getContainer")) []

  existing <- genSym
  let existingFind = R.Call (R.Project uc (R.Name "find")) [kv]
  let existingDecl = R.Forward $ R.ScalarDecl (R.Name existing) R.Inferred (Just $ existingFind)
  let existingPred = R.Binary "=="
                       (R.Variable $ R.Name existing)
                       (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uc])

  result <- genSym
  resultType <- getKType e >>= genCType
  let resultDecl = R.Forward $ R.ScalarDecl (R.Name result) resultType Nothing

  (nfe, nfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) n [R.Initialization R.Unit []]
  (wfe, wfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) w
                  [R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")]

  return (ce ++ ke ++ [resultDecl, existingDecl] ++ [R.IfThenElse existingPred (nfe ++ nfb) (wfe ++ wfb)]
         , R.Variable $ R.Name result)

inline e@(tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
           p@(Peek c),
           n])),
           w])) | c `dataspaceIn` stlLinearDSs = do
  (ce, cv) <- inline c
  let uc = R.Call (R.Project cv (R.Name "getContainer")) []

  first <- genSym
  let firstCall = R.Call (R.Project uc (R.Name "begin")) []
  let firstDecl = R.Forward $ R.ScalarDecl (R.Name first) R.Inferred (Just $ firstCall)
  let firstPred = R.Binary "=="
                    (R.Variable $ R.Name first)
                    (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uc])

  result <- genSym
  resultType <- getKType e >>= genCType
  let resultDecl = R.Forward $ R.ScalarDecl (R.Name result) resultType Nothing
  (nfe, nfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) n [R.Initialization R.Unit []]
  (wfe, wfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) w [R.Dereference $ R.Variable $ R.Name first]

  return (ce ++ [resultDecl, firstDecl] ++ [R.IfThenElse firstPred (nfe ++ nfb) (wfe ++ wfb)]
         , R.Variable $ R.Name result)

inline e@(tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
         (tag &&& children -> (EOperate OApp, [
           p@(SafeAt c),
           i])),
           n])),
           w])) | c `dataspaceIn` stlLinearDSs = do
  (ce, cv) <- inline c
  ig <- genSym
  ie <- reify (RDecl ig Nothing) i
  let iv = R.Variable $ R.Name ig
  let uc = R.Call (R.Project cv (R.Name "getContainer")) []

  let sizeCheck = R.Binary "<" iv (R.Call (R.Project uc (R.Name "size")) [])

  iterator <- genSym
  let advance = [ R.Forward $ R.ScalarDecl (R.Name iterator) R.Inferred (Just $ (R.Call (R.Project uc (R.Name "begin")) []))
                , R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "advance")) [R.Variable (R.Name iterator), iv]
                ]

  result <- genSym
  resultType <- getKType e >>= genCType
  let resultDecl = R.Forward $ R.ScalarDecl (R.Name result) resultType Nothing

  (nfe, nfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) n [R.Initialization R.Unit []]
  (wfe, wfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) w [R.Dereference (R.Variable $ R.Name iterator)]

  return (ce ++ ie ++ [resultDecl] ++ [R.IfThenElse sizeCheck (advance ++ wfe ++ wfb) (nfe ++ nfb)]
         , R.Variable $ R.Name result)

inline e@(tag -> EOperate OApp) = do
  -- Inline both function and argument for call.
  incApplyLevel

  let (f, as) = rollAppChain e
  (fe, fv) <- inline f
  let xs = map (last . children) as
  (unzip -> (xes, xvs)) <- mapM inline xs

  let reifyArg (x, xv, m) = do
        let orderAgnosticP = R.isOrderAgnostic xv
        if getInMethodFor anon m `elem` [Moved, Forwarded]
          then do
            let castMoveP = maybe True (\m -> needsMoveCast m xv) (getKTypeP x)

            let castModifier = case getInMethodFor anon m of
                  Moved | castMoveP -> R.Move
                  Forwarded -> R.FMacro
                  _ -> id

            if orderAgnosticP
              then return ([], castModifier xv)
              else do
                g <- genSym
                return ( [R.Forward $ R.ScalarDecl (R.Name g) (R.RValueReference R.Inferred) (Just $ castModifier xv)]
                       , R.FMacro $ R.Variable $ R.Name g
                       )
          else do
            if orderAgnosticP
              then return ([], xv)
              else do
                g <- genSym
                return ( [R.Forward $ R.ScalarDecl (R.Name g) (R.RValueReference R.Inferred) (Just xv)]
                       , R.FMacro $ R.Variable $ R.Name g
                       )

  (unzip -> (argDecls, argPasses)) <- mapM reifyArg $ zip3 xs xvs as

  let eName i = maybe (return i) (const $ getKType e >>= genCType >>= \rt -> return $ R.Specialized [rt] i)
                  (f @~ CArgs.isErrorFn)
  fv' <- case fv of
           R.Project s i -> R.Project s <$> eName i
           R.Variable i -> R.Variable <$> eName i
           _ -> return fv

  return (fe ++ concat xes ++ concat argDecls , R.Call fv' argPasses)

inline e@(tag &&& children -> (EOperate OSnd, [tag &&& children -> (ETuple, [trig@(tag -> EVariable tName), addr]), val])) = do
    d <- genSym
    tIdName <- case trig @~ isEProperty of
                 Just (EProperty (ePropertyValue -> Just (tag -> LString nm))) -> return nm
                 _ -> throwE $ CPPGenE $ "No trigger id property attached to " ++ tName
    (te, _)  <- inline trig
    (ae, av)  <- inline addr
    (ve, vv)  <- inline val
    let messageValue = passBy (getInMethodFor "!" e) val vv
    trigTypes <- getKType val >>= genCType
    let me = R.Variable $ R.Name "me"
    return (concat [te, ae, ve]
                 ++ [ R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "__engine_") (R.Specialized [trigTypes] (R.Name "send")))
                                    [me, av, R.Variable $ R.Name tIdName, messageValue]
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

inline (tag &&& children -> (EAssign x, [e])) = reify (RName (R.Variable $ R.Name x) Nothing) e >>= \a ->
                                                  return (a, R.Initialization R.Unit [])

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
          ee <- reify (RName (R.Variable $ R.Name g) Nothing) e
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
          someE <- reify (RName (R.Variable $ R.Name returnName) Nothing) s
          noneE <- reify (RName (R.Variable $ R.Name returnName) Nothing) n

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
        ee <- reify (RName (R.Variable $ R.Name g) Nothing) a
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
          BIndirection i -> wbByDecision (getExMethodFor i k) (R.Dereference initExpr) (R.Variable $ R.Name i)
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
        bindBody <- reify (RName (R.Variable $ R.Name returnName) (Just m)) e
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
        RName k b -> return [R.Assignment k (if fromMaybe False b then gMoveByE e value else value)]
        RDecl i b -> return [R.Forward $ R.ScalarDecl (R.Name i) (R.Inferred)
                             (Just $ if fromMaybe False b then gMoveByE e value else value)]
        RReturn b -> return $ [R.Return $ (if b then R.Move else id) value]
        RSplice _ -> throwE $ CPPGenE "Unsupported reification by splice."
    return $ effects ++ reification

-- ** Template Helpers
inlineApply :: RContext -> K3 Expression -> [R.Expression] -> CPPGenM ([R.Statement], [R.Statement])
inlineApply r f@(tag -> ELambda _) xs = do
  let (unzip -> (argNames, fExprs), body) = rollLambdaChain f
  body' <- reify r body

  let getArg (tag -> ELambda i) = i
  let (outerFExpr, innerFExpr) = (head &&& last) fExprs
  let (outerArg, _) = (getArg outerFExpr, getArg innerFExpr)

  let argReifications = map reifyArgument $ filter (\(a, _, _) -> a /= "_") $ zip3 argNames xs fExprs
  let trueCaptures = flip M.filterWithKey (getInDecisions outerFExpr) $ \k m -> k /= outerArg && (m == Copied || m == Moved)

  -- TODO: Find a way around this ugly hack to trick shadowing.
  captureReifications <- for (M.toList trueCaptures) $ \(k, m) -> do
        g <- genSym
        return $ [ R.Forward $ R.ScalarDecl (R.Name g) (R.Reference R.Inferred) (Just $ R.Variable $ R.Name k)
                 , R.Forward $ R.ScalarDecl (R.Name k) R.Inferred (Just $ R.Variable $ R.Name g)
                 ]

  return (concat captureReifications, argReifications ++ body')
 where
  reifyArgument :: (Identifier, R.Expression, K3 Expression) -> R.Statement
  reifyArgument (id &&& R.Name -> (i,  inside),  outside, getInMethodFor i -> m) = case m of
    Referenced -> R.Forward $ R.ScalarDecl inside (R.Reference R.Inferred) (Just $ R.FMacro outside)
    ConstReferenced -> R.Forward $ R.ScalarDecl inside (R.Const $ R.Reference $ R.Inferred) (Just $ R.FMacro outside)
    Copied -> R.Forward $ R.ScalarDecl inside R.Inferred (Just $ R.FMacro outside)
    Moved -> R.Forward $ R.ScalarDecl inside R.Inferred (Just $ R.Move outside)
    Forwarded -> R.Forward $ R.ScalarDecl inside (R.RValueReference R.Inferred) (Just $ R.FMacro outside)

inlineApply r f xs = do
  (fe, fv) <- inline f
  let rValue = R.Call fv xs
  reification <- case r of
    RForget -> return [R.Ignore rValue]
    RName k b -> return [R.Assignment k (if fromMaybe False b then R.Move rValue else rValue)]
    RDecl i b -> return [R.Forward $ R.ScalarDecl (R.Name i) R.Inferred
                           (Just $ if fromMaybe False b then R.Move rValue else rValue)]
    RReturn b -> return [R.Return $ if b then R.Move rValue else rValue]
    RSplice _ -> throwE $ CPPGenE "Unsupported reification by splice"
  return (fe, reification)
