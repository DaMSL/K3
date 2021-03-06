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
pattern UnsafeAt c <- Node (EProject "unsafe_at" :@: _) [c]

pattern (:$:) f x <- Node (EOperate OApp :@: _) [f, x]

dataspaceIn :: K3 Expression -> [Identifier] -> Bool
dataspaceIn e as = isJust $ getKTypeP e >>= \t -> t @~ \case { TAnnotation i -> i `elem` as; _ -> False }

precludeInline :: K3 Expression -> Bool
precludeInline e = isJust $ e @~ \case { (EProperty s) -> ePropertyName s == "NoInline"; _ -> False}

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

deepHasFusionSource :: K3 Expression -> Bool
deepHasFusionSource f = f @:? "FusionSource" || case f of { f' :$: _ -> deepHasFusionSource f'; _ -> False }

hasBoxableProperty :: Annotation Expression -> Bool
hasBoxableProperty ae = case ae of
                       (EProperty s) -> ePropertyName s == "Boxable"
                       _ -> False

ifIsolateApplicationProp :: K3 Expression -> Bool
ifIsolateApplicationProp e = isJust $ e @~ \case { EProperty s -> ePropertyName s == "IsolateApplication"; _ -> False}


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
inline e = do
  isolateApplicationP <- gets (isolateApplicationCG . flags)
  isolateQueryP <- gets (isolateQueryCG . flags)
  boxRecordsP <- gets (boxRecords . flags)
  let doInlineP = True
  case e of
    (tag &&& annotations -> (EConstant (CEmpty t), as)) ->
      case annotationComboIdE as of
        Nothing -> throwE $ CPPGenE $ "No Viable Annotation Combination for Empty " ++ show e
        Just ac -> getKType e >>= genCType >> genCType t >>= \ct -> return ([], R.Initialization (R.Collection ac ct) [])
    (tag -> EConstant c) -> constant c >>= \c' -> return ([], R.Literal c')

    (tag -> EVariable v) -> do
      gEnv <- gets globals
      case lookup v gEnv of
        Just (tag -> TFunction, _) -> return ([], R.Project (R.Dereference $ R.Variable $ R.Name "this") (R.Name v))
        Just (tag -> TForall _, _) -> return ([], R.Project (R.Dereference $ R.Variable $ R.Name "this") (R.Name v))
        _ -> return ([], R.Variable (R.Name v))

    (tag &&& children -> (t', [c])) | t' == ESome || t' == EIndirect -> do
      (e, v) <- inline c
      ct <- getKType c
      t <- genCType ct
      return (e, R.Call (R.Variable $ R.Specialized [t] (R.Name "make_shared")) [v])

    (tag &&& children -> (ETuple, [])) -> return ([], R.Initialization R.Unit [])

    (tag &&& children -> (ETuple, cs)) -> do
      (es, vs) <- unzip <$> mapM inline cs
      return (concat es, R.Call (R.Variable $ R.Name "make_tuple") vs)

    (tag &&& children -> (ERecord is, cs)) -> do
      (es, vs) <- unzip <$> mapM inline cs
      let reifyConstructorField (i, c, v) = do
            let orderAgnosticP = R.isOrderAgnostic v
	    let moveArg = if c @:? "Move" then Moved else getInMethodFor i e
            if moveArg == Moved
              then do
                castMoveP <- needsMoveCast <$> (getKType c) <*> (pure v)
                let moveCastModifier = if castMoveP then R.Move else id

                if orderAgnosticP
                  then return ([], moveCastModifier v)
                  else do
                    g <- genSym
		    let h = if c @:? "Move" then R.Move else R.FMacro
                    return ( [R.Forward $ R.ScalarDecl (R.Name g) (R.RValueReference R.Inferred) (Just $ moveCastModifier v)]
                          , h $ R.Variable $ R.Name g
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

    (tag &&& children -> (EOperate uop, [c])) -> do
      (ce, cv) <- inline c
      usym <- unarySymbol uop
      return (ce, R.Unary usym cv)

    (tag &&& children -> (EOperate OSeq, [a, b])) -> do
      ae <- reify RForget a
      (be, bv) <- inline b
      return (ae ++ be, bv)

    e@(tag -> ELambda _) -> do
      resetApplyLevel
      -- let isAccumulating = (not isolateQueryP) && e @:? "ReturnsArgument"
      let isAccumulating = False

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

      let captures = [R.ThisCapture] ++
                    (M.elems $ M.mapWithKey captureByMtrlzn $ M.filterWithKey (\k _ -> k /= outerArg)
                              $ getInDecisions outerFExpr)

      let reifyArg a g = if a == "_" then [] else
            let discriminator = if a == head (argNames) && isAccumulating then Referenced else (getInMethodFor a innerFExpr)
                reifyType = case discriminator of
                    ConstReferenced -> R.Reference $ R.Const R.Inferred
                    Referenced -> R.Reference R.Inferred
                    Forwarded -> R.RValueReference R.Inferred
                    _ -> R.Inferred
                reifyFrom = case discriminator of
                    -- Copied -> id
                    _ -> R.SForward (R.ConstExpr $ R.Call (R.Variable $ R.Name "decltype") [R.Variable $ R.Name g])
            in [R.Forward $ R.ScalarDecl (R.Name a) reifyType
                (Just $ reifyFrom (R.Variable $ R.Name g))]

      let argReifications = concat [reifyArg a g | a <- argNames | g <- formalArgNames]

      let fullBody = argReifications ++ body

      let argList = [ (if fi == "_" then Nothing else Just ai, R.RValueReference R.Inferred)
                    | ai <- formalArgNames
                    | fi <- argNames
                    ]

      return ([], R.Lambda captures argList True (if isAccumulating then Just R.Void else returnType) fullBody)

    p@(Fold c) :$: f :$: z  | doInlineP -> do
      (ce, cv) <- inline c
      (ze, zv) <- inline z

      -- Accumulating Function
      fs <- genSym
      fe <- reify (RDecl fs Nothing) f

      -- Initial Accumulator
      accVar <- genSym
      let movedAcc = gMoveByDE (if forceMoveP e then Moved else getInMethodFor anonS e) z zv
      let accDecl = R.Forward $ R.ScalarDecl (R.Name accVar) R.Inferred (Just movedAcc)

      -- Index Variable
      eleVar <- genSym
      movedEle <- do
        (tag &&& children -> (TCollection, [t])) <- getKType c
        return $ if getInMethodFor anonS p == Moved && isNonScalarType t
                   then R.Move $ R.Variable $ R.Name eleVar
                   else R.Variable $ R.Name eleVar

      -- TODO: Inline Apply

      -- Core Loop
      -- TODO: Double Reify on Embedded Folds
      let loopBody = R.Assignment
                      (R.Variable $ R.Name accVar)
                      (R.Call
                       (R.Variable $ R.Name fs)
                       [ R.Move $ R.Variable $ R.Name accVar
                       , movedEle
                       ])
      let loop = R.ForEach eleVar (R.Reference R.Inferred) cv (R.Block [loopBody])

      -- Extra Reification for Query Isolation.
      (xe, xv) <- if isolateQueryP
                    then do
                     q <- genSym
                     return ([R.Forward $ R.ScalarDecl (R.Name q) R.Inferred (Just $ R.Variable $ R.Name accVar)], q)
                    else return ([], accVar)
      return (ze ++ [accDecl] ++ ce ++ fe ++ [loop] ++ xe, R.Move $ R.Variable $ R.Name xv)

    p@(InsertWith c) :$: k :$: w | c `dataspaceIn` stlAssocDSs && doInlineP -> do
      (ce, cv) <- inline c
      kn <- genSym
      ke <- reify (RDecl kn Nothing) k
      --(ke, kv) <- case k of
      --  (tag &&& children -> (ERecord fs, cs)) | not br -> do
      --    (unzip -> (fes, catMaybes -> [fv])) <- for (zip fs cs) $ \(f, j) ->
      --      if f == "key"
      --        then do
      --          (fe, fv) <- inline j
      --          return (fe, Just fv)
      --        else do
      --          fe <- reify RForget j
      --          return (fe, Nothing)
      --    return (concat fes, fv)
      --  _ -> inline k >>= \(ke', kv') -> return (ke', R.Project (if br then R.Dereference kv' else kv') (R.Name "key"))
      -- kg <- genSym
      -- ke <- reify (RDecl kg Nothing) k

      br <- gets (boxRecords . flags)

      kt <- getKType k >>= genCType
      rt <- getKType c >>= \(tag &&& children -> (TCollection, [rt])) -> genCType rt
      kp <- genSym
      let kpdecl = R.Forward $ R.ScalarDecl (R.Name kp) (R.Reference R.Inferred) (Just $ R.Project ((if br then R.Dereference else id) $ R.Variable $ R.Name kn) (R.Name "key"))

      ug <- genSym
      let ue = R.Forward $ R.ScalarDecl (R.Name ug) (R.Reference R.Inferred) (Just $  R.Call (R.Project cv (R.Name "getContainer")) [])
      let uv = R.Variable $ R.Name ug

      existing <- genSym
      let existingFind = R.Call (R.Project uv (R.Name "find")) [R.Variable $ R.Name kp]
      let existingDecl = R.Forward $ R.ScalarDecl (R.Name existing) R.Inferred (Just $ existingFind)
      let existingPred = R.Binary "=="
                          (R.Variable $ R.Name existing)
                          (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uv])

      let nfe = [R.Assignment (R.Subscript uv (R.Variable $ R.Name kp)) (R.Move $ R.Variable $ R.Name kn)]
      (wfe, wfb) <- inlineApply (RName (R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")) (Just True)) w
                      [R.Move $ R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second"), R.Variable $ R.Name kn]
                      [rt, kt]

      return (ce ++ [ue] ++ ke ++ [kpdecl] ++ [existingDecl] ++ [R.IfThenElse existingPred nfe (wfe ++ wfb)]
            , R.Initialization R.Unit [])

    p@(UpsertWith c) :$: k :$: n :$: w | c `dataspaceIn` stlAssocDSs && doInlineP -> do
      (ce, cv) <- inline c

      rt <- getKType c >>= \(tag &&& children -> (TCollection, [rt])) -> genCType rt

      kg <- genSym
      br <- gets (boxRecords . flags)
      (ke, kv) <- case k of
        (tag &&& children -> (ERecord fs, cs)) | not br -> do
          (unzip -> (fes, catMaybes -> [fv])) <- for (zip fs cs) $ \(f, j) ->
            if f == "key"
              then do
                (fe, fv) <- inline j
                return (fe, Just fv)
              else do
                fe <- reify RForget j
                return (fe, Nothing)
          return (concat fes, fv)
        _ -> inline k >>= \(ke', kv') -> return (ke', R.Project (if br then R.Dereference kv' else kv') (R.Name "key"))

      ug <- genSym
      let ue = R.Forward $ R.ScalarDecl (R.Name ug) (R.Reference R.Inferred) (Just $  R.Call (R.Project cv (R.Name "getContainer")) [])
      let uv = R.Variable $ R.Name ug

      existing <- genSym
      let existingFind = R.Call (R.Project uv (R.Name "find")) [kv]
      let existingDecl = R.Forward $ R.ScalarDecl (R.Name existing) R.Inferred (Just $ existingFind)
      let existingPred = R.Binary "=="
                          (R.Variable $ R.Name existing)
                          (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uv])

      (nfe, nfb) <- inlineApply (RName (R.Subscript uv kv) (Just True)) n [R.Initialization R.Unit []] [R.Unit]

      --let rArg = isJust $ w @~ (\case { EProperty (ePropertyName -> "ReturnsArgument") -> True; _ -> False })
      let rArg = False
      let rContext = if rArg
                      then RForget
                      else RName (R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")) (Just True)

      (wfe, wfb) <- inlineApply rContext w
                      [(if rArg then id else R.Move) $ R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")]
                      [rt]

      return (ce ++ [ue] ++ ke ++ [existingDecl] ++ [R.IfThenElse existingPred (nfe ++ nfb) (wfe ++ wfb)]
            , R.Initialization R.Unit [])

    p@(UpsertWith c) :$: k :$: n :$: w | c `dataspaceIn` ["IntMap"] && doInlineP -> do
      (ce, cv) <- inline c
      (ke, kv) <- inline k

      recordType <- getKType k >>= genCType

      let keyField = if boxRecordsP then R.Project (R.Dereference kv) (R.Name "key") else R.Project kv (R.Name "key")

      mapi <- genSym
      let mapiDecl = R.Forward $ R.ScalarDecl (R.Name mapi) (R.Pointer R.Inferred)
                       (Just $ R.Call (R.Project cv (R.Name "get_mapi")) [])
      let sizeCheck = R.Binary "==" (R.Project (R.Dereference $ R.Variable $ R.Name mapi) (R.Name "size")) (R.Literal $ R.LInt 0)

      let insertionRecord = R.CCast (R.Pointer R.Void) (R.SCast (R.Const $ R.Pointer $ R.Void) (R.TakeReference kv))
      let insertCall = R.SCast (R.Pointer recordType)
                         (R.Call (R.Variable $ R.Name "mapi_insert") [R.Variable $ R.Name mapi, insertionRecord])

      placement <- genSym
      let placementDecl = R.Forward $ R.ScalarDecl (R.Name placement) (R.Pointer R.Inferred) (Just insertCall)

      (nfe, nfs) <- inlineApply (RName (R.Dereference $ R.Variable $ R.Name placement) (Just True)) n [R.Initialization R.Unit []]
                      [R.Unit]

      let missingBranch = [placementDecl] ++ nfe ++ nfe ++ nfs

      existing <- genSym
      let existingDecl = R.Forward $ R.ScalarDecl (R.Name existing) (R.Pointer R.Inferred)
                           (Just $ R.SCast (R.Pointer recordType)
                             (R.Call (R.Variable $ R.Name "mapi_find") [R.Variable $ R.Name mapi, keyField]))

      let existingCheck = R.Binary "==" (R.Variable $ R.Name existing) (R.Literal R.LNullptr)

      (wfe, wfs) <- inlineApply (RName (R.Dereference $ R.Variable $ R.Name existing) (Just True)) w
                                [R.Move (R.Dereference $ R.Variable $ R.Name existing)]
                                [recordType]

      let nonEmptyCase = R.IfThenElse existingCheck missingBranch (wfe ++ wfs)
      let nonEmptyBranch = [existingDecl, nonEmptyCase]
      let fullCase = R.IfThenElse sizeCheck missingBranch nonEmptyBranch

      return (ce ++ ke ++ [mapiDecl, fullCase], R.Initialization R.Unit [])

    p@(Lookup c) :$: k :$: n :$: w | c `dataspaceIn` stlAssocDSs && doInlineP -> do
      (ce, cv) <- inline c
      rt <- getKType c >>= \(tag &&& children -> (TCollection, [rt])) -> genCType rt
      kg <- genSym
      ke <- reify (RDecl kg Nothing) k
      br <- gets (boxRecords . flags)
      let kv = R.Project ((if br then R.Dereference else id) $ R.Variable $ R.Name kg) (R.Name "key")
      (ke, kv) <- case k of
        (tag &&& children -> (ERecord fs, cs)) | br -> do
          (unzip -> (fes, catMaybes -> [fv])) <- for (zip fs cs) $ \(f, j) ->
            if f == "key"
              then do
                (fe, fv) <- inline j
                return (fe, Just fv)
              else do
                fe <- reify RForget j
                return (fe, Nothing)
          return (concat fes, fv)
        _ -> inline k >>= \(ke', kv') -> return (ke', R.Project (if br then R.Dereference kv' else kv') (R.Name "key"))

      ug <- genSym
      let ue = R.Forward $ R.ScalarDecl (R.Name ug) (R.Reference R.Inferred) (Just $  R.Call (R.Project cv (R.Name "getContainer")) [])
      let uv = R.Variable $ R.Name ug

      existing <- genSym
      let existingFind = R.Call (R.Project uv (R.Name "find")) [kv]
      let existingDecl = R.Forward $ R.ScalarDecl (R.Name existing) R.Inferred (Just $ existingFind)
      let existingPred = R.Binary "=="
                          (R.Variable $ R.Name existing)
                          (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uv])

      result <- genSym
      resultType <- getKType e >>= genCType
      let resultDecl = R.Forward $ R.ScalarDecl (R.Name result) resultType Nothing

      (nfe, nfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) n [R.Initialization R.Unit []] [R.Unit]
      (wfe, wfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) w
                      [R.Project (R.Dereference (R.Variable $ R.Name existing)) (R.Name "second")] [rt]


      return (ce ++ [ue] ++ ke ++ [resultDecl, existingDecl] ++ [R.IfThenElse existingPred (nfe ++ nfb) (wfe ++ wfb)]
            , R.Variable $ R.Name result)

    p@(Peek c) :$: n :$: w | c `dataspaceIn` stlLinearDSs && doInlineP -> do
      (ce, cv) <- inline c
      rt <- getKType c >>= \(tag &&& children -> (TCollection, [rt])) -> genCType rt

      ug <- genSym
      let ue = R.Forward $ R.ScalarDecl (R.Name ug) (R.Reference R.Inferred) (Just $  R.Call (R.Project cv (R.Name "getConstContainer")) [])
      let uv = R.Variable $ R.Name ug

      first <- genSym
      let firstCall = R.Call (R.Project uv (R.Name "begin")) []
      let firstDecl = R.Forward $ R.ScalarDecl (R.Name first) R.Inferred (Just $ firstCall)
      let firstPred = R.Binary "=="
                        (R.Variable $ R.Name first)
                        (R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "end")) [uv])

      result <- genSym
      resultType <- getKType e >>= genCType
      let resultDecl = R.Forward $ R.ScalarDecl (R.Name result) resultType Nothing
      (nfe, nfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) n [R.Initialization R.Unit []] [R.Unit]
      (wfe, wfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) w [R.Dereference $ R.Variable $ R.Name first] [rt]

      return (ce ++ [ue] ++ [resultDecl, firstDecl] ++ [R.IfThenElse firstPred (nfe ++ nfb) (wfe ++ wfb)]
            , R.Variable $ R.Name result)

    p@(SafeAt c) :$: i :$: n :$: w | c `dataspaceIn` stlLinearDSs && doInlineP -> do
      (ce, cv) <- inline c
      rt <- getKType c >>= \(tag &&& children -> (TCollection, [rt])) -> genCType rt
      ig <- genSym
      ie <- reify (RDecl ig Nothing) i
      let iv = R.Variable $ R.Name ig

      ug <- genSym
      let ue = R.Forward $ R.ScalarDecl (R.Name ug) (R.Reference R.Inferred) (Just $  R.Call (R.Project cv (R.Name "getConstContainer")) [])
      let uv = R.Variable $ R.Name ug

      let sizeCheck = R.Binary "<" iv (R.Call (R.Project uv (R.Name "size")) [])

      iterator <- genSym
      let advance = [ R.Forward $ R.ScalarDecl (R.Name iterator) R.Inferred (Just $ (R.Call (R.Project uv (R.Name "begin")) []))
                    , R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "advance")) [R.Variable (R.Name iterator), iv]
                    ]

      result <- genSym
      resultType <- getKType e >>= genCType
      let resultDecl = R.Forward $ R.ScalarDecl (R.Name result) resultType Nothing

      (nfe, nfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) n [R.Initialization R.Unit []] [R.Unit]
      (wfe, wfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) w [R.Dereference (R.Variable $ R.Name iterator)] [rt]

      return (ce ++ [ue] ++ ie ++ [resultDecl] ++ [R.IfThenElse sizeCheck (advance ++ wfe ++ wfb) (nfe ++ nfb)]
            , R.Variable $ R.Name result)

    p@(UnsafeAt c) :$: i :$: w | c `dataspaceIn` stlLinearDSs && doInlineP -> do
      (ce, cv) <- inline c
      rt <- getKType c >>= \(tag &&& children -> (TCollection, [rt])) -> genCType rt
      ig <- genSym
      ie <- reify (RDecl ig Nothing) i
      let iv = R.Variable $ R.Name ig

      ug <- genSym
      let ue = R.Forward $ R.ScalarDecl (R.Name ug) (R.Reference R.Inferred) (Just $  R.Call (R.Project cv (R.Name "getConstContainer")) [])
      let uv = R.Variable $ R.Name ug

      iterator <- genSym
      let advance = [ R.Forward $ R.ScalarDecl (R.Name iterator) R.Inferred (Just $ (R.Call (R.Project uv (R.Name "begin")) []))
                    , R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "advance")) [R.Variable (R.Name iterator), iv]
                    ]

      result <- genSym
      resultType <- getKType e >>= genCType
      let resultDecl = R.Forward $ R.ScalarDecl (R.Name result) resultType Nothing

      (wfe, wfb) <- inlineApply (RName (R.Variable $ R.Name result) Nothing) w [R.Dereference (R.Variable $ R.Name iterator)] [rt]

      return (ce ++ [ue] ++ ie ++ [resultDecl] ++ [R.Block (advance ++ wfe ++ wfb)]
            , R.Variable $ R.Name result)

    f :$: x | isolateApplicationP && deepHasFusionSource f && not (e @:? "ApplicationIsolated") -> do
      g <- genSym
      es <- guardDReify serializeRoundTrip (RDecl g Nothing) (e @:+ "ApplicationIsolated")
      return (es, R.Variable $ R.Name g)

    _ :$: _ -> do
      -- Inline both function and argument for call.
      incApplyLevel

      let (f, as) = rollAppChain e
      (fe, fv) <- inline f
      let xs = map (last . children) as
      let inline' e' =
            if deepHasFusionSource f && isolateApplicationP then serializeRoundTripInline e'
            else inline e'

      (unzip -> (xes, xvs)) <- mapM inline' xs

      let reifyArg (x, xv, m) = do
            let orderAgnosticP = R.isOrderAgnostic xv
            let moveArg = if x @:? "Move" then Moved else getInMethodFor anonS m
            if moveArg `elem` [Moved, Forwarded]
              then do
                let castMoveP = maybe True (\m -> needsMoveCast m xv) (getKTypeP x)

                let castModifier = case moveArg of
                      -- _ | not isolateQueryP && f @:? "ReturnsArgument" -> id
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

      br <- gets (boxRecords . flags)

      let nameMod = if br && isJust (f @~ hasBoxableProperty) then (R.nameConcat "boxed_") else id

      let eName i = maybe (return $ nameMod i) (const $ getKType e >>= genCType >>= \rt -> return $ R.Specialized [rt] (nameMod i))
                      (f @~ CArgs.isErrorFn)
      fv' <- case fv of
              R.Project s i -> R.Project s <$> eName i
              R.Variable i -> R.Variable <$> eName i
              _ -> return fv

      return (fe ++ concat xes ++ concat argDecls , R.Call fv' argPasses)

    (tag &&& children -> (EOperate OSnd, [tag &&& children -> (ETuple, [trig@(tag -> EVariable tName), addr]), val])) -> do
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
      let commonSArgs = [me, av, R.Variable $ R.Name tIdName, messageValue]

      (sName, sArgs) <- case (e @~ isEDelay, e @~ isEDelayOverride, e @~ isEDelayOverrideEdge) of
                        (Just (EProperty (ePropertyValue -> Just (tag -> LInt delay))), Nothing, Nothing) ->
                          return ("delayedSend",
                            commonSArgs ++ [R.Variable $ R.Name "TimerType::Delay", R.Literal $ R.LInt delay])

                        (Nothing, Just (EProperty (ePropertyValue -> Just (tag -> LInt delay))), Nothing) ->
                          return ("delayedSend",
                            commonSArgs ++ [R.Variable $ R.Name "TimerType::DelayOverride", R.Literal $ R.LInt delay])

                        (Nothing, Nothing, Just (EProperty (ePropertyValue -> Just (tag -> LInt delay)))) ->
                          return ("delayedSend",
                            commonSArgs ++ [R.Variable $ R.Name "TimerType::DelayOverrideEdge", R.Literal $ R.LInt delay])

                        (Nothing, Nothing, Nothing) -> return ("send", commonSArgs)

                        _ -> throwE $ CPPGenE $ "Invalid delay send properties"

      return (concat [te, ae, ve]
                  ++ [ R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "__engine_") (R.Specialized [trigTypes] (R.Name sName))) sArgs ]
              , R.Initialization R.Unit [])

    (tag &&& children -> (EOperate bop, [a, b])) -> do
      (ae, av) <- inline a
      (be, bv) <- inline b
      bsym <- binarySymbol bop
      return (ae ++ be, R.Binary bsym av bv)

    (tag &&& children -> (EProject v, [k])) -> do
      (ke, kv) <- inline k

      br <- gets (boxRecords . flags)
      kt <- getKType k
      let bw = case tag kt of
                TRecord _ | br -> R.Dereference kv
                _ -> kv
      -- let bw = if br && tag kt /= TCollection then R.Dereference kv else kv
      return (ke, R.Project bw (R.Name v))

    (tag &&& children -> (EAssign x, [e])) -> reify (RName (R.Variable $ R.Name x) Nothing) e >>= \a ->
                                                    return (a, R.Initialization R.Unit [])

    (tag &&& children -> (EAddress, [h, p])) -> do
      (he, hv) <- inline h
      (pe, pv) <- inline p
      return (he ++ pe, R.Call (R.Variable $ R.Name "make_address") [hv, pv])

    _ -> do
      k <- genSym
      effects <- reify (RDecl k Nothing) e
      return (effects, R.Variable $ R.Name k)
 where
  isETriggerId (EProperty (ePropertyName -> "TriggerId")) = True
  isETriggerId _ = False

  isEDelay (EProperty (ePropertyName -> "Delay")) = True
  isEDelay _ = False

  isEDelayOverride (EProperty (ePropertyName -> "DelayOverride")) = True
  isEDelayOverride _ = False

  isEDelayOverrideEdge (EProperty (ePropertyName -> "DelayOverrideEdge")) = True
  isEDelayOverrideEdge _ = False


-- | The generic function to generate code for an expression whose result is to be reified. The
-- method of reification is indicated by the @RContext@ argument.
reify :: RContext -> K3 Expression -> CPPGenM [R.Statement]
reify r e = do
  isolateApplicationP <- gets (isolateApplicationCG . flags)
  isolateQueryP <- gets (isolateQueryCG . flags)
  let doInlineP = True
  case (r, e) of
    (RForget, Fold _ :$: _ :$: _) | doInlineP -> do
      (ee, _) <- inline e
      return ee

    -- TODO: Is this the fix we need for the unnecessary reification issues?
    (RForget, _ :$: _) -> do
      (ee, ev) <- inline e
      return $ ee ++ [R.Ignore ev]

    (_,(tag &&& children -> (EOperate OSeq, [a, b]))) -> do
      ae <- reify RForget a
      be <- reify r b
      return $ ae ++ be

    (RDecl i b, (tag -> ELetIn _)) -> precludeRDecl i b e

    (_, (tag &&& children -> (ELetIn x, [i, b]))) -> do
      let initD = getInMethodFor x e
      ie <- reify (RDecl x (Just $ initD == Moved)) i
      be <- reify r b
      return [R.Block $ ie ++ be]

    (RDecl i b, (tag -> ECaseOf _)) -> precludeRDecl i b e

    (_, k@(tag &&& children -> (ECaseOf x, [e, s, n]))) -> do
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

    (RDecl i b, x@(tag -> EBindAs _)) -> precludeRDecl i b x

    (_, k@(tag &&& children -> (EBindAs b, [a, e]))) -> do
      let newNames =
            case b of
              BIndirection i -> [i]
              BTuple is -> is
              BRecord iis -> snd (unzip iis)
              BSplice _ -> []

      initKType <- getKType a
      initCType <- genCType initKType

      (initName, initReify) <- do
        g <- genSym
        (e, i) <- inline a
        return (g, e ++ [R.Forward $ R.ScalarDecl (R.Name g) (R.RValueReference R.Inferred) (Just i)])

        {- case tag a of
          EVariable v -> return (v, [])
          _ -> do
            g <- genSym
            ee <- reify (RName (R.Variable $ R.Name g) Nothing) a
            return (g, [R.Forward $ R.ScalarDecl (R.Name g) initCType Nothing] ++ ee) -}

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
              BSplice _ -> []

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
              BSplice _ -> []

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

    (RDecl i mb, x@(tag -> EIfThenElse)) -> precludeRDecl i mb x

    (_, (tag &&& children -> (EIfThenElse, [p, t, e]))) -> do
        (pe, pv) <- inline p
        te <- reify r t
        ee <- reify r e
        return $ pe ++ [R.IfThenElse pv te ee]

    -- | Catch-all case
    _ -> do
      (effects, value) <- inline e
      reification <- doReify r e value
      return $ effects ++ reification

doReify :: RContext -> K3 Expression -> R.Expression -> CPPGenM [R.Statement]
doReify r e v = case r of
  RForget -> return []
  RName k b -> return [R.Assignment k (if fromMaybe False b then gMoveByE e v else v)]
  RDecl i b -> return [R.Forward $ R.ScalarDecl (R.Name i) (R.Inferred)
                      (Just $ if fromMaybe False b then gMoveByE e v else v)]
  RReturn b -> return $ [R.Return $ (if b then R.Move else id) v]
  RSplice _ -> throwE $ CPPGenE "Unsupported reification by splice."

doubleReify :: RContext -> K3 Expression -> CPPGenM [R.Statement]
doubleReify r e = do
  g <- genSym
  es' <- reify (RDecl g Nothing) e
  let rg = R.Variable $ R.Name g
  es <- doReify r e rg
  return $ es' ++ es

serializeRoundTripCPP :: R.Type -> R.Expression -> CPPGenM ([R.Statement], R.Expression)
serializeRoundTripCPP t v = do
  g <- genSym
  let gDecl = R.Forward $ R.ScalarDecl (R.Name g) R.Inferred
                (Just $ R.Call (R.Variable $ R.Name "pack")
                          [v, R.Variable $ R.Qualified (R.Name "CodecFormat") (R.Name "YASBinary")])
  u <- genSym
  let uDecl = R.Forward $ R.ScalarDecl (R.Name u) R.Inferred
                (Just $ R.Call (R.Variable $ R.Specialized [t] $ R.Name "unpack") [R.Move $ R.Variable $ R.Name g])
  return ([gDecl, uDecl], R.Dereference $ R.Variable $ R.Name u)

serializeRoundTripInline :: K3 Expression -> CPPGenM ([R.Statement], R.Expression)
serializeRoundTripInline e = do
  (es, v) <- inline e
  t <- getKType e >>= genCType
  (es', v') <- serializeRoundTripCPP t v
  return (es ++ es', v')

serializeRoundTrip :: RContext -> K3 Expression -> CPPGenM [R.Statement]
serializeRoundTrip r e = do
  (es, v) <- serializeRoundTripInline e
  es' <- doReify r e v
  return $ es ++ es'


-- serializeRoundTrip


guardDReify :: (RContext -> K3 Expression -> CPPGenM [R.Statement]) -> RContext -> K3 Expression -> CPPGenM [R.Statement]
guardDReify f r e = gets (isolateApplicationCG . flags) >>= \a -> if a then f r e else reify r e


-- ** Template Helpers
inlineApply :: RContext -> K3 Expression -> [R.Expression] -> [R.Type] -> CPPGenM ([R.Statement], [R.Statement])
inlineApply r f xs xts = do
  isolateApplicationP <- gets (isolateApplicationCG . flags)
  case f of
    -- Can only inline lambdas, we do not grab definitions from elsewhere.
    (tag -> ELambda outerArg) -> do
      -- Roll the lambda chain up, we need the inner and outer lambdas, as well as the body. The
      -- outer lambda contains the names of the true captures (closure variables which are not
      -- nested lambda arguments), while the inner lambda contains the relevant materialization
      -- decisions.
      let (unzip -> (argNames, fExprs), body) = rollLambdaChain f

      -- Reify the body of the lambda under the desired context.
      rawBody <- if f @:? "FusionSource" && not (f @:? "BuiltinTransformer")
                   then guardDReify serializeRoundTrip r body
                   else reify r body

      let innerFExpr = last fExprs
      let isTrueCapture k m = k /= outerArg && (m == Copied || m == Moved)

      let trueCaptures = M.filterWithKey isTrueCapture (getInDecisions f)

      let reifyArgument formalName reifiedArg formalType = do
            if formalName == "_" then return [] else do
            let formalType = case getInMethodFor formalName innerFExpr of
                  Referenced -> R.Reference R.Inferred
                  ConstReferenced -> R.Reference $ R.Const $ R.Inferred
                  Copied -> R.Inferred
                  Moved -> R.Inferred
                  Forwarded -> R.RValueReference R.Inferred
            if isolateApplicationP && f @:? "FusionSource" && not (f @:? "BuiltinTransformer")
              then do
                (es, v) <- serializeRoundTripCPP formalType reifiedArg
                let fDecl = R.Forward $ R.ScalarDecl (R.Name formalName) (R.Reference R.Inferred) (Just v)
                return $ es ++ [fDecl]
                -- g <- genSym
                -- let firstAssign = R.Forward $ R.ScalarDecl (R.Name g) R.Inferred (Just reifiedArg)
                -- let secondAssign = R.Forward $ R.ScalarDecl (R.Name formalName) formalType (Just $ R.Variable $ R.Name g)
                -- return [firstAssign, secondAssign]
              else return [R.Forward $ R.ScalarDecl (R.Name formalName) formalType (Just reifiedArg)]

      argumentReifications <- concat <$> (sequence $ zipWith3 reifyArgument argNames xs xts)

      -- We only need to reify captures that need to be isolated (copied/moved), since reference
      -- captures will be in scope as desired. If a capture is to be isolated, generate a symbol for
      -- it, and replace the capture's name for it in the reified body.
      let reifyCapture (name, mtrlzn) (decls, oldBody) = do
            g <- genSym
            let cast = if mtrlzn == Moved then R.Move else id
            let decl = [R.Forward $ R.ScalarDecl (R.Name g) R.Inferred (Just $ cast $ R.Variable $ R.Name name)]
            return (decls ++ decl , R.subst g name <$> oldBody)

      (captureStmts, substBody) <- foldrM reifyCapture ([], rawBody) (M.toList trueCaptures)

      return (captureStmts, argumentReifications ++ substBody)
    _ -> do
      (fe, fv) <- inline f

      (concat -> argReifications, actualArgs) <-
        if isolateApplicationP && f @:? "FusionSource" && not (f @:? "BuiltinTransformer")
          then unzip <$> (sequence $ zipWith serializeRoundTripCPP xts xs)
          else return ([], xs)

      let callStmt = R.Call fv actualArgs

      ft <- getKType f
      let (tag &&& children -> (TFunction, [_, rt])) = ft
      crt <- genCType rt

      (resultLocationE, resultLocationV) <-
        if isolateApplicationP && f @:? "FusionSource" && not (f @:? "BuiltinTransformer")
          then serializeRoundTripCPP crt callStmt
            -- return ([R.Forward $ R.ScalarDecl (R.Name g) R.Inferred (Just callStmt)], R.Variable $ R.Name g)
          else return ([], callStmt)

      resultReification <-
        case r of
          RForget -> return [R.Ignore resultLocationV]
          RName k b -> return [R.Assignment k (if fromMaybe False b then R.Move resultLocationV else resultLocationV)]
          RDecl i b -> return [R.Forward $ R.ScalarDecl (R.Name i) R.Inferred
                              (Just $ if fromMaybe False b then R.Move resultLocationV else resultLocationV)]
          RReturn b -> return [R.Return $ if b then R.Move resultLocationV else resultLocationV]
          RSplice _ -> throwE $ CPPGenE "Unsupported reification by splice"

      return (fe ++ argReifications ++ resultLocationE, resultReification)
