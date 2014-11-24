{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.Simplification where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Control.Monad.Identity

import Data.Either
import Data.Fixed
import Data.Function
import Data.List
import Data.Maybe
import Data.Tree
import Data.Word ( Word8 )
import qualified Data.Map as Map
import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Expression as EC
import qualified Language.K3.Core.Constructor.Type       as TC
import qualified Language.K3.Core.Constructor.Literal    as LC

import Language.K3.Analysis.Effects.InsertEffects ( EffectEnv(..), SymbolCategories(..)
                                                  , exprSCategories, expandCategories, expandExpression )

import Language.K3.Transform.Common
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Data.Types

import Language.K3.Utils.Pretty

traceLogging :: Bool
traceLogging = False

localLog :: (Functor m, Monad m) => String -> m ()
localLog = logVoid traceLogging

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
localLogAction = logAction traceLogging


-- | Property testers and constructors
pTransformer :: Annotation Expression
pTransformer = EProperty "Transformer" Nothing

pPureTransformer :: Annotation Expression
pPureTransformer = EProperty "PureTransformer" Nothing

pImpureTransformer :: Annotation Expression
pImpureTransformer = EProperty "ImpureTransformer" Nothing

pFusable :: Annotation Expression
pFusable = EProperty "Fusable" Nothing

pStreamable :: Annotation Expression
pStreamable = EProperty "Streamable" Nothing

pStream :: Annotation Expression
pStream = EProperty "Stream" Nothing

pUnstream :: K3 Type -> Annotation Expression
pUnstream t = EProperty "Unstream" (Just $ LC.string $ show t)

pHasSkip :: Annotation Expression
pHasSkip = EProperty "HasSkip" Nothing

pTAppChain :: Annotation Expression
pTAppChain = EProperty "TAppChain" Nothing

pIElemRec :: Annotation Expression
pIElemRec = EProperty "IElemRec" Nothing

pOElemRec :: Annotation Expression
pOElemRec = EProperty "OElemRec" Nothing

-- | Fold fusion accumulator function classification
data FusionAccFClass = UCondVal -- Direct return of accumulator value (e.g., iterate)
                     | UCond    -- Unconditional accumulation
                     | ICond1   -- Accumulation in one accumulator-independent condition branch
                     | ICondN   -- Accumulation in many accumulator-independent condition branches
                     | DCond2   -- Accumulation in two accumulator-dependent condition branches
                     | Open     -- General accumulation function with unknown structure
                     deriving (Enum, Eq, Ord, Read, Show)

-- | Fold fusion accumulator transform classification
data FusionAccTClass = IdTr     -- Identity transform
                     | IndepTr  -- Accumulator independent transform (although possible element dependent)
                     | DepTr    -- Accumulator dependent transform
                     deriving (Enum, Eq, Ord, Read, Show)

type FusionAccSpec = (FusionAccFClass, FusionAccTClass)

pFusionSpec :: FusionAccSpec -> Annotation Expression
pFusionSpec spec = EProperty "FusionSpec" (Just . LC.string $ show spec)

pFusionLineage :: String -> Annotation Expression
pFusionLineage s = EProperty "FusionLineage" (Just $ LC.string s)

isEPure :: Annotation Expression -> Bool
isEPure (EProperty "Pure" _) = True
isEPure _ = False

isETransformer :: Annotation Expression -> Bool
isETransformer (EProperty "Transformer" _) = True
isETransformer _ = False

isEPureTransformer :: Annotation Expression -> Bool
isEPureTransformer (EProperty "PureTransformer" _) = True
isEPureTransformer _ = False

isEImpureTransformer :: Annotation Expression -> Bool
isEImpureTransformer (EProperty "ImpureTransformer" _) = True
isEImpureTransformer _ = False

isEFusable :: Annotation Expression -> Bool
isEFusable (EProperty "Fusable" _) = True
isEFusable _ = False

isEStreamable :: Annotation Expression -> Bool
isEStreamable (EProperty "Streamable" _) = True
isEStreamable _ = False

isEStream :: Annotation Expression -> Bool
isEStream (EProperty "Stream" _) = True
isEStream _ = False

isEUnstream :: Annotation Expression -> Bool
isEUnstream (EProperty "Unstream" _) = True
isEUnstream _ = False

isEHasSkip :: Annotation Expression -> Bool
isEHasSkip (EProperty "HasSkip" _) = True
isEHasSkip _ = False

isETAppChain :: Annotation Expression -> Bool
isETAppChain (EProperty "TAppChain" _) = True
isETAppChain _ = False

isEIElemRec :: Annotation Expression -> Bool
isEIElemRec (EProperty "IElemRec" _) = True
isEIElemRec _ = False

isEOElemRec :: Annotation Expression -> Bool
isEOElemRec (EProperty "OElemRec" _) = True
isEOElemRec _ = False

isEFusionSpec :: Annotation Expression -> Bool
isEFusionSpec (EProperty "FusionSpec" (Just _)) = True
isEFusionSpec _ = False

getFusionSpec :: Annotation Expression -> Maybe FusionAccSpec
getFusionSpec (EProperty "FusionSpec" (Just (tag -> LString s))) = Just $ read s
getFusionSpec _ = Nothing

getFusionSpecA :: [Annotation Expression] -> Maybe FusionAccSpec
getFusionSpecA anns = case find isEFusionSpec anns of
  Just ann -> getFusionSpec ann
  _ -> Nothing

getFusionSpecE :: K3 Expression -> Maybe FusionAccSpec
getFusionSpecE e = case e @~ isEFusionSpec of
  Just ann -> getFusionSpec ann
  _ -> Nothing

isEFusionLineage :: Annotation Expression -> Bool
isEFusionLineage (EProperty "FusionLineage" (Just _)) = True
isEFusionLineage _ = False

getFusionLineage :: Annotation Expression -> Maybe String
getFusionLineage (EProperty "FusionLineage" (Just (tag -> LString s))) = Just s
getFusionLineage _ = Nothing

getFusionLineageA :: [Annotation Expression] -> Maybe String
getFusionLineageA anns = case find isEFusionLineage anns of
  Just ann -> getFusionLineage ann
  _ -> Nothing

getFusionLineageE :: K3 Expression -> Maybe String
getFusionLineageE e = case e @~ isEFusionLineage of
  Just ann -> getFusionLineage ann
  _ -> Nothing

{- Effect queries -}
readOnly :: Bool -> EffectEnv -> K3 Expression -> Bool
readOnly skipArg env e = let SymbolCategories _ w _ _ io = exprSCategories skipArg e env in null w && not io

noWrites :: Bool -> EffectEnv -> K3 Expression -> Bool
noWrites skipArg env e = let SymbolCategories _ w _ _ _ = exprSCategories skipArg e env in null w

-- | Constant folding
type FoldedExpr = Either String (Either Value (K3 Expression))

type BinOp              a = (a -> a -> a)
type NumericFunction    a = (BinOp Word8) -> (BinOp Int) -> (BinOp Double) -> a
type ComparisonFunction a = (Ordering -> Bool) -> a
type LogicFunction      a = (Bool -> Bool -> Bool) -> a

foldProgramConstants :: K3 Declaration -> Either String (K3 Declaration)
foldProgramConstants prog = mapExpression foldConstants prog

foldConstants :: K3 Expression -> Either String (K3 Expression)
foldConstants expr = simplifyAsFoldedExpr expr >>= either (rebuildValue $ annotations expr) return
  where
    simplifyAsFoldedExpr :: K3 Expression -> FoldedExpr
    simplifyAsFoldedExpr e = foldMapTree simplifyConstants (Left $ VTuple []) e

    simplifyConstants :: [Either Value (K3 Expression)] -> K3 Expression -> FoldedExpr
    simplifyConstants _ n@(tag -> EVariable _) = return $ Right n
    simplifyConstants _ (tag &&& annotations -> (EConstant c, anns)) = constant c anns

    simplifyConstants ch n@(tag -> EOperate OAdd) = applyNum ch n (numericOp (+) (+) (+))
    simplifyConstants ch n@(tag -> EOperate OSub) = applyNum ch n (numericOp (-) (-) (-))
    simplifyConstants ch n@(tag -> EOperate OMul) = applyNum ch n (numericOp (*) (*) (*))
    simplifyConstants ch n@(tag -> EOperate ODiv) = applyNum ch n (numericExceptZero div div (/))
    simplifyConstants ch n@(tag -> EOperate OMod) = applyNum ch n (numericExceptZero mod mod mod')

    simplifyConstants ch n@(tag -> EOperate OEqu) = applyCmp ch n (== EQ)
    simplifyConstants ch n@(tag -> EOperate ONeq) = applyCmp ch n (/= EQ)
    simplifyConstants ch n@(tag -> EOperate OLth) = applyCmp ch n (== LT)
    simplifyConstants ch n@(tag -> EOperate OLeq) = applyCmp ch n (`elem` [LT, EQ])
    simplifyConstants ch n@(tag -> EOperate OGth) = applyCmp ch n (== GT)
    simplifyConstants ch n@(tag -> EOperate OGeq) = applyCmp ch n (`elem` [GT, EQ])

    simplifyConstants ch n@(tag -> EOperate OConcat) = applyOp ch n (asBinary $ stringOp (++))

    simplifyConstants ch n@(tag -> EOperate OAnd) = applyBool ch n (&&)
    simplifyConstants ch n@(tag -> EOperate OOr)  = applyBool ch n (||)

    simplifyConstants ch n@(tag -> EOperate ONeg) = applyOp ch n (asUnary flipOp)
    simplifyConstants ch n@(tag -> EOperate ONot) = applyOp ch n (asUnary flipOp)

    -- | We do not process indirections as a literal constructor here, since
    --   in Haskell this requires a side effect.
    simplifyConstants ch n@(tag -> ESome) =
      applyVCtor ch n (asUnary $ someCtor $ extractQualifier $ head $ children n)

    simplifyConstants ch n@(tag -> ETuple) =
      applyVCtor ch n (tupleCtor $ map extractQualifier $ children n)

    simplifyConstants ch n@(tag -> ERecord ids) =
      applyVCtor ch n (recordCtor ids $ map extractQualifier $ children n)

    -- Binding simplification.
    -- TODO: substitute when we have read-only mutable bnds.
    simplifyConstants ch n@(tag -> ELetIn i) =
      let immutSource = onQualifiedExpression (head $ children n) True False in
      case (head ch, last ch, immutSource) of
        (_, Left v2, _)             -> return $ Left v2
        (Left v, Right bodyE, True) -> substituteBinding i v bodyE >>= simplifyAsFoldedExpr
        (_, _, _)                   -> rebuildNode n ch

    -- TODO: substitute when we have read-only mutable bnds.
    simplifyConstants ch n@(tag -> EBindAs b) =
      case (b, head ch, last ch) of
        (_, _, Left v) -> return $ Left v

        (BTuple ids,  Left (VTuple vqs), Right bodyE) ->
          (foldM substituteQualifiedField bodyE $ zip ids vqs) >>= simplifyAsFoldedExpr

        (BRecord ijs, Left (VRecord nvqs), Right bodyE) -> do
          subVQs <- mapM (\(s,t) -> maybe (invalidRecordBinding s) (return . (t,)) $ Map.lookup s nvqs) ijs
          foldM substituteQualifiedField bodyE subVQs >>= simplifyAsFoldedExpr

        (_, _, _) -> rebuildNode n ch

      where invalidRecordBinding s =
              Left $ "Invalid bind-as mapping source during simplification: " ++ s

    -- Branch simplification.
    simplifyConstants ch n@(tag -> EIfThenElse) =
      case head ch of
        Left (VBool True)  -> return (ch !! 1)
        Left (VBool False) -> return $ last ch
        Right _ -> rebuildNode n ch
        _ -> Left "Invalid if-then-else predicate simplification"

    -- TODO: substitute when we have read-only mutable bnds.
    simplifyConstants ch n@(tag -> ECaseOf i) =
      case head ch of
        Left (VOption (Just v, MemImmut)) ->
          (case ch !! 1 of
            Left v2     -> return $ Left v2
            Right someE -> substituteBinding i v someE >>= simplifyAsFoldedExpr)

        Left (VOption (Nothing,  _)) -> return $ last ch
        Right _ -> rebuildNode n ch
        _ -> Left "Invalid case-of source simplification"

    -- Projection simplification on a constant record.
    -- Since we do not simplify collections, VCollections cannot appear
    -- as the source of the projection expression.
    simplifyConstants ch n@(tag -> EProject i) =
        case head ch of
          Left (VRecord nvqs) -> maybe fieldError (return . Left . fst) $ Map.lookup i nvqs
          Right _ -> rebuildNode n ch
          _ -> Left "Invalid projection source simplification"

      where fieldError = Left $ "Unknown record field in project simplification: " ++ i

    -- The default case is to rebuild the current node as an expression.
    -- This handles: lambda, assignment, addresses, and self expressions
    simplifyConstants ch n = rebuildNode n ch

    rebuildValue :: [Annotation Expression] -> Value -> Either String (K3 Expression)
    rebuildValue anns v = valueAsExpression v >>= return . flip (foldl (@+)) anns

    rebuildNode :: K3 Expression -> [Either Value (K3 Expression)] -> FoldedExpr
    rebuildNode n ch = do
        let chAnns = map annotations $ children n
        nch <- mapM (\(vOrE, anns) -> either (rebuildValue anns) return vOrE) $ zip ch chAnns
        return . Right $ Node (tag n :@: annotations n) nch

    withValueChildren :: K3 Expression -> [Either Value (K3 Expression)]
                      -> ([Value] -> FoldedExpr) -> FoldedExpr
    withValueChildren n ch f = if all isLeft ch then f (lefts ch) else rebuildNode n ch

    substituteQualifiedField :: K3 Expression -> (Identifier, (Value, VQualifier)) -> Either String (K3 Expression)
    substituteQualifiedField targetE (n, (v, MemImmut)) = substituteBinding n v targetE
    substituteQualifiedField targetE (_, (_, MemMut))   = return targetE

    substituteBinding :: Identifier -> Value -> K3 Expression -> Either String (K3 Expression)
    substituteBinding i iV targetE = do
      iE <- valueAsExpression iV
      return $ substituteImmutBinding i iE targetE

    someCtor        q v = return . Left $ VOption (Just v, q)
    tupleCtor      qs l = return . Left $ VTuple  $ zip l qs
    recordCtor ids qs l = return . Left $ VRecord $ Map.fromList $ zip ids $ zip l qs

    applyNum          ch n op   = withValueChildren n ch $ asBinary op
    applyCmp          ch n op   = withValueChildren n ch $ asBinary (comparison op)
    applyBool         ch n op   = withValueChildren n ch $ asBinary (logic op)
    applyOp           ch n op   = withValueChildren n ch $ op
    applyVCtor        ch n ctor = withValueChildren n ch $ ctor

    asUnary f [a] = f a
    asUnary _ _ = Left "Invalid unary operands"

    asBinary f [a,b] = f a b
    asBinary _ _ = Left "Invalid binary operands"

    eQualifier :: VQualifier -> Annotation Expression
    eQualifier MemImmut = EImmutable
    eQualifier MemMut   = EMutable

    noneQualifier :: VQualifier -> NoneMutability
    noneQualifier MemImmut = NoneImmut
    noneQualifier MemMut   = NoneMut

    extractQualifier :: K3 Expression -> VQualifier
    extractQualifier e = onQualifiedExpression e MemImmut MemMut

    valueAsExpression :: Value -> Either String (K3 Expression)
    valueAsExpression (VBool   v)       = Right . EC.constant $ CBool   v
    valueAsExpression (VByte   v)       = Right . EC.constant $ CByte   v
    valueAsExpression (VInt    v)       = Right . EC.constant $ CInt    v
    valueAsExpression (VReal   v)       = Right . EC.constant $ CReal   v
    valueAsExpression (VString v)       = Right . EC.constant $ CString v

    valueAsExpression (VOption (Nothing, vq)) =
      Right . EC.constant $ CNone $ noneQualifier vq

    valueAsExpression (VOption (Just v, vq)) =
      valueAsExpression v >>= Right . (\e -> e @+ eQualifier vq) . EC.some

    valueAsExpression (VTuple  vqs) =
      mapM (valueAsExpression . fst) vqs >>=
        (\l -> Right $ EC.tuple $ map (uncurry (@+)) $ zip l $ map (eQualifier . snd) vqs)

    valueAsExpression (VRecord nvqs) =
      let (ids, vqs) = unzip $ Map.toList nvqs
          (vs, qs)   = unzip vqs
      in mapM valueAsExpression vs >>=
            (\l -> Right $ EC.record $ zip ids $ map (uncurry (@+)) $ zip l $ map eQualifier qs)

    valueAsExpression v = Left $ "Unable to reinject value during simplification: " ++ show v


{- Constant expression evaluation.
   These are similar to the interpreter's evaluation functions, except
   that they are pure, and do not operate in a stateful interpretation monad.
 -}

-- | Evaluate a constant. This is similar to the intepreter's evaluation except
--   that we pass on collections, yielding an expression.
constant :: Constant -> [Annotation Expression] -> FoldedExpr
constant   (CBool b)     _ = return . Left  $ VBool b
constant   (CByte w)     _ = return . Left  $ VByte w
constant   (CInt i)      _ = return . Left  $ VInt i
constant   (CReal r)     _ = return . Left  $ VReal r
constant   (CString s)   _ = return . Left  $ VString s
constant   (CNone _)  anns = return . Left  $ VOption (Nothing, vQualOfAnnsE anns)
constant c@(CEmpty _) anns = return . Right $ Node (EConstant c :@: anns) []

numericOp :: NumericFunction (Value -> Value -> FoldedExpr)
numericOp byteOpF intOpF realOpF a b =
  case (a, b) of
    (VByte x, VByte y) -> return . Left . VByte $ byteOpF x y
    (VByte x, VInt y)  -> return . Left . VInt  $ intOpF (fromIntegral x) y
    (VByte x, VReal y) -> return . Left . VReal $ realOpF (fromIntegral x) y
    (VInt x,  VByte y) -> return . Left . VInt  $ intOpF x (fromIntegral y)
    (VInt x,  VInt y)  -> return . Left . VInt  $ intOpF x y
    (VInt x,  VReal y) -> return . Left . VReal $ realOpF (fromIntegral x) y
    (VReal x, VByte y) -> return . Left . VReal $ realOpF x (fromIntegral y)
    (VReal x, VInt y)  -> return . Left . VReal $ realOpF x (fromIntegral y)
    (VReal x, VReal y) -> return . Left . VReal $ realOpF x y
    _                  -> Left "Invalid numeric binary operands"

-- | Similar to numericOp above, except disallow a zero value for the second argument.
numericExceptZero :: NumericFunction (Value -> Value -> FoldedExpr)
numericExceptZero byteOpF intOpF realOpF a b =
  case b of
    VByte 0 -> Left "Zero denominator in numeric operation"
    VInt  0 -> Left "Zero denominator in numeric operation"
    VReal 0 -> Left "Zero denominator in numeric operation"
    _       -> numericOp byteOpF intOpF realOpF a b

flipOp :: Value -> FoldedExpr
flipOp (VBool b) = return . Left . VBool $ not b
flipOp (VInt  i) = return . Left . VInt  $ negate i
flipOp (VReal r) = return . Left . VReal $ negate r
flipOp _       = Left "Invalid negation/not operation"

comparison :: ComparisonFunction (Value -> Value -> FoldedExpr)
comparison cmp a b = return . Left . VBool . cmp $ compare a b

logic :: LogicFunction (Value -> Value -> FoldedExpr)
logic op a b =
  case (a, b) of
    (VBool x, VBool y) -> return . Left . VBool $ op x y
    _ -> Left $ "Invalid boolean logic operands"

stringOp :: (String -> String -> String) -> Value -> Value -> FoldedExpr
stringOp op a b =
  case (a, b) of
    (VString s, VString t) -> return . Left . VString $ op s t
    _ -> Left $ "Invalid string operands"


-- | Conservative beta reduction.
--   This reduces lambda and let bnds that are used at most once in their bodies.
--
--   TODO:
--   More generally, we can use a cost model to determine this threshold based
--   on the cost of the argument and the cost of the increased lifetime of the
--   object given its encapsulation in a lambda.
--   Furthermore, this only applies to direct lambda invocations, rather than
--   on general function values (i.e., including applications through bnds
--   and substructure). For the latter case, we must inline and defunctionalize first.
betaReductionOnProgram :: EffectEnv -> K3 Declaration -> K3 Declaration
betaReductionOnProgram eenv prog = runIdentity $ mapExpression (betaReduction eenv) prog

betaReduction :: EffectEnv -> K3 Expression -> Identity (K3 Expression)
betaReduction env expr = mapTree reduce expr
  where
    reduce ch n = case replaceCh n ch of
      (tag -> ELetIn i) -> reduceOnOccurrences n ch i (head ch) $ last ch
      (PAppLam i bodyE argE lamAs appAs) -> reduceOnOccurrences n ch i argE bodyE
      _ -> rebuildNode n ch

    reduceOnOccurrences n ch i ie e =
      if readOnly False env ie then
        case tag ie of
          EConstant _ -> betaReduction env $ substituteImmutBinding i ie e
          EVariable _ -> betaReduction env $ substituteImmutBinding i ie e
          _ -> let occurrences = length $ filter (== i) $ freeVariables e in
               if occurrences <= 3
                 then betaReduction env $ substituteImmutBinding i ie e
                 else rebuildNode n ch
      else rebuildNode n ch

    rebuildNode (Node t _) ch = return $ Node t ch


-- | Effect-aware dead code elimination.
--   Currently this only operates on expressions, and does not prune
--   unused declarations from the program.
--   Since constant folding already handles constant-based control flow
--   simplification, the only work left for this transformation is to:
--   i. prune unused let-in bnds and narrow bind-as expressions with record binders.
--   ii. prune unused values (i.e., pure expressions in blocks)
--
-- TODO:
--   iii. remove unread assignments
--   iv. dead data elimination (i.e. eliminate unncessary structure construction,
--       such as unused tuple or record fields)
--   v. covering control elimination, e.g.,
--        if a then (if a then b else c) else d => if a then b else d
--        case x of { Some j -> case x of { Some k -> l } { None -> m }} { None -> n }
--          => case x of { Some j -> l } { None -> n }
eliminateDeadProgramCode :: EffectEnv -> K3 Declaration -> Either String (K3 Declaration)
eliminateDeadProgramCode env prog = mapExpression (eliminateDeadCode env) prog

eliminateDeadCode :: EffectEnv -> K3 Expression -> Either String (K3 Expression)
eliminateDeadCode env expr = mapTree pruneExpr expr
  where
    rcr = eliminateDeadCode env

    pruneExpr ch n = case replaceCh n ch of

      -- Immediate record construction and projection, provided all other record fields are pure.
      (PPrjRec fId ids fieldsE _ _) ->
        flip (maybe $ rebuildNode n ch) (elemIndex fId ids) $ \i ->
          if and $ map (readOnly True env . snd) $ filter ((/= fId) . fst) $ zip ids ch
            then return $ fieldsE !! i
            else rebuildNode n ch

      -- Immediate structure binding, preserving effect ordering of bound substructure expressions.
      (PBindInd i iE bodyE iAs bAs) -> rcr $ (EC.letIn i (PInd iE iAs) bodyE) @<- bAs

      (PBindTup ids fieldsE bodyE _ _) ->
        let vars          = freeVariables bodyE
            unused (i, e) = readOnly True env e && i `notElem` vars
            used          = filter (not . unused) $ zip ids fieldsE
        in rcr $ foldr (\(i,e) accE -> EC.letIn i e accE) bodyE used

      (PBindRec ijs ids fieldsE bodyE _ _) ->
        let vars          = freeVariables bodyE
            unused (i, e) = readOnly True env e && (maybe False (`notElem` vars) $ lookup i ijs)
            used          = filter (not . unused) $ zip ids fieldsE
        in
        rcr $ foldr (\(i,e) accE -> maybe accE (\j -> EC.letIn j e accE) $ lookup i ijs) bodyE used

      -- Unused effect-free binding removal
      (tag -> ELetIn  i) ->
        let vars = freeVariables $ last ch in
        if readOnly True env (head ch) && i `notElem` vars
          then return $ last ch
          else rebuildNode n ch

      (tag -> EBindAs b) ->
        let vars = freeVariables $ last ch in
        case b of
          BRecord ijs ->
            let nBinder = filter (\(_,j) -> j `elem` vars) ijs in
            if readOnly True env (head ch) && null nBinder
              then return $ last ch
              else return $ Node (EBindAs (BRecord $ nBinder) :@: annotations n) ch
          _ -> rebuildNode n ch

      (tag -> EOperate OSeq) ->
        if readOnly True env (head ch)
          then return $ last ch
          else rebuildNode n ch

      -- Immediate option bindings
      e@(PCaseOf (PSome sE optAs) j someE noneE cAs) ->
        return $ if readOnly True env sE then someE else e

      (PCaseOf (PNone _ _) j someE noneE cAs) -> return $ noneE

      -- Branch unnesting for case-of/if-then-else combinations (case-of-case, etc.)
      -- These strip UID and Span annotations due to duplication following the rewrite.
      (PCaseOf (PCaseOf optE i isomeE inoneE icAs) j jsomeE jnoneE jcAs) ->
        rcr $ PCaseOf optE i (PCaseOf isomeE j jsomeE jnoneE $ stripUIDSpanE jcAs)
                             (PCaseOf inoneE j jsomeE jnoneE $ stripUIDSpanE jcAs)
                             icAs

      (PCaseOf (PIfThenElse pE tE eE bAs) i isomeE inoneE cAs) ->
        rcr $ PIfThenElse pE (PCaseOf tE i isomeE inoneE $ stripUIDSpanE cAs)
                             (PCaseOf eE i isomeE inoneE $ stripUIDSpanE cAs)
                             bAs

      (PIfThenElse (PCaseOf optE i someE noneE cAs) otE oeE oAs) ->
        rcr $ PCaseOf optE i (PIfThenElse someE otE oeE $ stripUIDSpanE oAs)
                             (PIfThenElse noneE otE oeE $ stripUIDSpanE oAs)
                             cAs

      (PIfThenElse (PIfThenElse ipE itE ieE iAs) otE oeE oAs) ->
        rcr $ PIfThenElse ipE (PIfThenElse itE otE oeE $ stripUIDSpanE oAs)
                              (PIfThenElse ieE otE oeE $ stripUIDSpanE oAs)
                              iAs

      e -> return e

    rebuildNode n@(tag -> EConstant _) _ = return n
    rebuildNode n@(tag -> EVariable _) _ = return n
    rebuildNode n ch = return $ Node (tag n :@: annotations n) ch

    stripUIDSpanE = filter (not . \a -> isEUID a || isESpan a)


-- | Effect-aware common subexpression elimination.
--
-- Naive algorithm:
--   build tree of candidates: each tree node captures when it is the LCA of a
--   common subexpression, along with the number of times that it occurs.
--     i. propagate all subtrees up, identifying candidates as
--        common subtrees across children (i.e., marking candidates at their LCA).
--        All subtrees are always propagated, including whether they are a candidate
--        locally or not. This can result in multiple nodes marked as the meet point
--        for a candidate (i.e., the meet point for a pair vs a triple vs a quadruple).
--     ii. stop propagation at impure nodes (this is conservative since we assume
--         any effect impedes CSE, rather than checking whether the effect is
--         relevant to the candidate).
--     iii. candidates at each LCA are chosen to be the dominating candidate at that LCA;
--          consider e1 and e2 as a common pair of expressions -- all subexpressions are
--          also common at the same LCA. We elide considering these subexpressions as candidates
--          but continue to propagate them upwards. This ensures that no two covering expressions
--          are considered candidates at the same LCA (with the same frequency; they can be
--          candidates with covered expression occurring more frequently).
--     iv. at each node, the candidates are stored in frequency-order: [(K3 Expression, Int)]
--     v. covered expressions must occur at least as frequently, if not more frequently than
--        their covering expressions nearer the root of the tree.
--
--   build tree of substitutions: greedy selection of what to substitute given candidate tree.
--     i. traverse tree of candidates top-down and mark for substitution, tracking the node UID
--        and the expression to substitute.
--     ii. prune any occurrences of the same or covered candidates at equal or lower frequency
--         from descendants in the candidate tree.
--     iii. for any covered candidate at greater frequency, decrement its counter.
--          note this means that the candidate may occur in the expression being substituted.
--     iv. recur on children.
--
--   flatten substitutions
--     i. extract a list of UIDs and expression to substitute.
--     ii. close over substitutions
--
--   perform substitutions
--     i. traverse tree top-down, and when encountering a UID, test and substitute.
--
-- TODO: add UID and span annotations to the transformed code.
--
type Candidates        = [(K3 Expression, Int)]
type CandidateTree     = Tree (UID, Candidates)
type Substitution      = (UID, K3 Expression, Int)
type NamedSubstitution = (UID, Identifier, K3 Expression, Int)

commonProgramSubexprElim :: EffectEnv -> K3 Declaration -> Either String (K3 Declaration)
commonProgramSubexprElim env prog = mapExpression (commonSubexprElim env) prog

commonSubexprElim :: EffectEnv -> K3 Expression -> Either String (K3 Expression)
commonSubexprElim env expr = do
    cTree <- buildCandidateTree expr
    -- TODO: log candidates for debugging
    pTree <- pruneCandidateTree cTree
    -- TODO: log pruned candidates for debugging
    substituteCandidates pTree

  where
    covers :: K3 Expression -> K3 Expression -> Bool
    covers a b = runIdentity $ (\f -> foldMapTree f False a) $ \chAcc n ->
      if or chAcc then return $ True else return $ n == b

    buildCandidateTree :: K3 Expression -> Either String CandidateTree
    buildCandidateTree e = do
      (cTree, _, _) <- foldMapTree buildCandidates ([], [], []) e
      case cTree of
        [x] -> return x
        _   -> Left "Invalid candidate tree"

    buildCandidates :: [([CandidateTree], [K3 Expression], [K3 Expression])] -> K3 Expression
                    -> Either String ([CandidateTree], [K3 Expression], [K3 Expression])
    buildCandidates _ n@(tag -> EConstant _) = leafTreeAccumulator n
    buildCandidates _ n@(tag -> EVariable _) = leafTreeAccumulator n
    buildCandidates chAccs n@(Node t _) = flip (maybe $ uidError n) (n @~ isEUID) $ \x ->
      case x of
        EUID uid ->
          let (ctCh, sExprCh, subAcc) = unzip3 chAccs
              bnds = case tag t of
                           ELambda i -> [[i]]
                           ELetIn  i -> [[], [i]]
                           ECaseOf j -> [[], [j], []]
                           EBindAs b -> [[], bindingVariables b]
                           _         -> repeat []

              filteredCands   = nub $ concatMap filterOpenCandidates $ zip bnds subAcc
              localCands      = sortBy ((flip compare) `on` snd) $
                                  foldl (addCandidateIfLCA subAcc) [] filteredCands

              candTreeNode    = Node (uid, localCands) $ concat ctCh
              nStrippedExpr   = Node (tag t :@: (filter ((||) <$> isEQualified <*> isAnyETypeOrEffectAnn)
                                                            $ annotations t)) $ concat sExprCh
              propagatedExprs = if readOnly False env n then (concat subAcc)++[nStrippedExpr] else []
          in
          return $ ([candTreeNode], [nStrippedExpr], propagatedExprs)

        _ -> uidError n

      where
        filterOpenCandidates ([], cands) = cands
        filterOpenCandidates (bnds, cands) =
          filter (\e -> null $ freeVariables e `intersect` bnds) cands

    leafTreeAccumulator :: K3 Expression
                        -> Either String ([CandidateTree], [K3 Expression], [K3 Expression])
    leafTreeAccumulator e = do
      ctNode <- leafCandidateNode e
      return $ ([ctNode], [Node (tag e :@: (filter ((||) <$> isEQualified <*> isAnyETypeOrEffectAnn)
                                                            $ annotations e)) []], [])

    leafCandidateNode :: K3 Expression -> Either String CandidateTree
    leafCandidateNode e = case e @~ isEUID of
      Just (EUID uid) -> Right $ Node (uid, []) []
      _               -> uidError e

    addCandidateIfLCA :: [[K3 Expression]] -> Candidates -> K3 Expression -> Candidates
    addCandidateIfLCA descSubs candAcc sub =
      let (branchCnt, totalCnt) = branchCounters sub descSubs in
      if branchCnt <= 1 then candAcc
                        else appendCandidate candAcc (sub, totalCnt)

    appendCandidate :: Candidates -> (K3 Expression, Int) -> Candidates
    appendCandidate acc (e, cnt) =
      let (_, rest)         = partition (\(e2, i) -> (e `covers` e2) && cnt >= i) acc
          (covering, rest2) = partition (\(e2, i) -> (e2 `covers` e) && i >= cnt) rest
      in (if null covering then [(e, cnt)] else covering) ++ rest2

    branchCounters sub descSubs = foldl (countInBranch sub) (0::Int,0::Int) descSubs
    countInBranch sub (a,b) cs =
      let i = length $ filter (== sub) cs in (if i > 0 then a+1 else a, b+i)

    pruneCandidateTree :: CandidateTree -> Either String CandidateTree
    pruneCandidateTree = biFoldMapTree trackCandidates pruneCandidates [] (Node (UID $ -1, []) [])
      where
        trackCandidates :: Candidates -> CandidateTree -> Either String (Candidates, [Candidates])
        trackCandidates candAcc (Node (_, cands) ch) = do
          let nCandAcc = foldl appendCandidate candAcc cands
          return (nCandAcc, replicate (length ch) nCandAcc)

        pruneCandidates :: Candidates -> [CandidateTree] -> CandidateTree
                        -> Either String CandidateTree
        pruneCandidates candAcc ch (Node (uid, cands) _) =
          let used = filter (\p@(e, _) -> elem p candAcc && noWrites False env e) cands
              nUid = if null used then UID $ -1 else uid
          in return $ Node (nUid, used) ch

    substituteCandidates :: CandidateTree -> Either String (K3 Expression)
    substituteCandidates prunedTree = do
        substitutions   <- foldMapTree concatCandidates [] prunedTree
        ncSubstitutions <- foldSubstitutions substitutions
        nExpr           <- foldM substituteAtUID expr ncSubstitutions
        return nExpr

      where
        rebuildAnnNode n ch = Node (tag n :@: annotations n) ch

        concatCandidates candAcc (Node (uid, cands) _) =
          return $ (if null cands then [] else map (\(e,i) -> (uid,e,i)) cands) ++ (concat candAcc)

        foldSubstitutions :: [Substitution] -> Either String [NamedSubstitution]
        foldSubstitutions subs = do
          (_,namedSubs) <- foldM nameSubstitution (0::Int,[]) subs
          foldM (\subAcc sub -> mapM (closeOverSubstitution sub) subAcc) namedSubs namedSubs

        nameSubstitution :: (Int, [NamedSubstitution]) -> Substitution
                         -> Either String (Int, [NamedSubstitution])
        nameSubstitution (cnt, acc) (uid, e, i) =
          return (cnt+1, acc++[(uid, ("__cse"++show cnt), e, i)])

        closeOverSubstitution :: NamedSubstitution -> NamedSubstitution -> Either String NamedSubstitution
        closeOverSubstitution (uid, n, e, _) (uid2, n2, e2, i2)
          | uid == uid2 && n == n2 = return $ (uid, n, e2, i2)
          | e2 `covers` e          = return $ (uid2, n2, substituteExpr e (EC.variable n) e2, i2)
          | otherwise              = return $ (uid2, n2, e2, i2)

        substituteAtUID :: K3 Expression -> NamedSubstitution -> Either String (K3 Expression)
        substituteAtUID targetE (uid, n, e, _) = mapTree (letAtUID uid n e) targetE

        letAtUID :: UID -> Identifier -> K3 Expression -> [K3 Expression] -> K3 Expression
                 -> Either String (K3 Expression)
        letAtUID uid cseId e ch n = case n @~ isEUID of
          Just (EUID uid2) -> return $
            let cseVar = EC.variable cseId in
            if uid == uid2
              then let srcE = case e @~ isEQualified of
                                Nothing -> e @+ EImmutable
                                Just _  -> e
                   in EC.letIn cseId srcE $ substituteExpr e cseVar n
              else rebuildAnnNode n ch
          _ -> return $ rebuildAnnNode n ch

        substituteExpr :: K3 Expression -> K3 Expression -> K3 Expression -> K3 Expression
        substituteExpr compareE newE targetE =
          snd . runIdentity $ foldMapRebuildTree (stripAndSub compareE newE) EC.unit targetE

        stripAndSub compareE newE chAcc ch n = do
          (strippedE, rebuiltE) <- return $ case tag n of
              EConstant _ -> (Node (tag n :@: (filter isEQualified $ annotations n)) [],    Node (tag n :@: annotations n) [])
              EVariable _ -> (Node (tag n :@: (filter isEQualified $ annotations n)) [],    Node (tag n :@: annotations n) [])
              _           -> (Node (tag n :@: (filter isEQualified $ annotations n)) chAcc, Node (tag n :@: annotations n) ch)

          return $ if strippedE == compareE
                     then (newE, foldl (@+) newE $ annotations n)
                     else (strippedE, rebuiltE)


    uidError e = Left $ "No UID found on " ++ show e


-- | Collection transformer fusion.
-- TODO: duplicate eliminating fusion on sets (fine for bags/lists)

streamableTransformerArg :: K3 Expression -> Bool
streamableTransformerArg (PStreamableTransformerArg _ _ _ _) = True
streamableTransformerArg _ = False

encodeTransformers :: EffectEnv -> K3 Declaration -> Either String (K3 Declaration)
encodeTransformers env prog = mapExpression (encodeTransformerExprs env) prog

encodeTransformerExprs :: EffectEnv -> K3 Expression -> Either String (K3 Expression)
encodeTransformerExprs env expr = modifyTree encode expr -- >>= modifyTree markContent
  where
    encode e@(PPrjApp _ fId fAs _ _)
      | unaryTransformer fId && any isETransformer fAs
        = case fId of
            "filter"  -> mkFold1 e
            "map"     -> mkFold1 e
            "iterate" -> mkIter  e
            "ext"     -> return e
            _         -> return e

    encode e@(PPrjApp2 _ fId fAs _ _ _ _)
      | binaryTransformer fId && any isETransformer fAs = mkFold2 e

    encode e@(PPrjApp3 _ fId fAs _ _ _ _ _ _)
      | ternaryTransformer fId && any isETransformer fAs = mkFold3 e

    encode e = return e

    -- Mark whether a transform has an 'elem'-wrapped or 'key-value' input element type.
    markContent e@(PPrjApp2Chain cE "fold" "fold" fArg1 fArg2 gArg1 gArg2
                                 fAs iApp1As iApp2As gAs oApp1As oApp2As)
      | any isETransformer fAs && any isETransformer gAs
      = let ngAs = propagateRecType fAs gAs
        in return $ PPrjApp2Chain cE "fold" "fold" fArg1 fArg2 gArg1 gArg2
                                  fAs iApp1As iApp2As ngAs oApp1As oApp2As

    markContent e = return e

    -- Fold constructors for transformers.
    mkFold1 e@(PPrjApp cE fId fAs fArg appAs) = do
      accE           <- mkAccumE e
      (nfAs', nfArg) <- mkIndepAccF fId fAs fArg
      let nfAs = markPureTransformer False nfAs' fArg
      let (nApp1As, nApp2As) = (markTAppChain appAs, markTAppChain [])
      return $ PPrjApp2 cE "fold" nfAs nfArg accE nApp1As nApp2As

    mkFold1 e = return e

    mkIter e@(PPrjApp cE fId fAs fArg appAs) = do
      let nfAs = fAs ++ [pImpureTransformer, pFusionSpec (UCondVal, IndepTr), pFusionLineage "iterate"]
      let (nApp1As, nApp2As) = (markTAppChain appAs, markTAppChain [])
      return $ PPrjApp2 cE "fold" nfAs (EC.lambda "_" fArg) EC.unit nApp1As nApp2As

    mkIter e = return e

    -- TODO: infer simpler top-level structure of accumulator function than ICondN.
    -- i.e., ICond1? UCond?
    mkFold2 e@(PPrjApp2 cE fId fAs
                        fArg1@(streamableTransformerArg -> streamable) fArg2
                        app1As app2As)
      = let cls   = if streamable then (ICondN,IndepTr) else (Open,DepTr)
            nfAs' = fAs ++ [pFusionSpec cls, pFusionLineage "fold"]
            nfAs  = markPureTransformer True nfAs' fArg1
            r     = PPrjApp2 cE fId nfAs fArg1 fArg2 app1As app2As
        in
        if False then return r else debugInferredFold r nfAs fArg1

        where debugInferredFold e nfAs@(getFusionSpecA -> Just cls) fArg1 = return $
                flip trace e $
                  unlines [ unwords ["Fold function effects"]
                          , pretty $ expandCategories env $ exprSCategories True fArg1 env
                          , unwords ["Inferred fold:", show streamable, show cls]
                          , pretty e ]
              debugInferredFold _ _ _ = Left "Invalid fusion-fold construction"

    mkFold2 e = return e

    mkFold3 e@(PPrjApp3 cE fId fAs fArg1 fArg2 fArg3 app1As app2As app3As)
      = case fId of
          "groupBy" -> do
            (accE, valueT)  <- mkGBAccumE e
            rAccE           <- mkAccumE e
            (nfAs', nfArg1) <- mkGBAccumF valueT fAs fArg1 fArg2 fArg3
            let nfAs                       = nfAs' ++ if readOnly False env fArg1 && readOnly True env fArg2
                                                      then [pPureTransformer] else [pImpureTransformer]
            let (nApp1As, nApp2As)         = (markTAppChain app1As, markTAppChain app2As)
            let buildE                     = PPrjApp2 cE "fold" nfAs nfArg1 accE nApp1As nApp2As
            let copyF                      = mkIdAccF
            let (ncAs, ncApp1As, ncApp2As) = ([ pTransformer, pPureTransformer
                                              , pFusionSpec (UCond, IdTr), pFusionLineage "copy" ]
                                             , markTAppChain [], markTAppChain [])
            return $ PPrjApp2 buildE "fold" ncAs copyF rAccE ncApp1As ncApp2As

          _ -> Left $ "Invalid ternary transformer: " ++ fId

    mkFold3 e = return e

    mkAccF elemF bodyF =
      let (aVar, aVarId) = (EC.variable "acc", "acc")
          (eVar, eVarId) = (EC.variable "e",   "e")
      in
      EC.lambda aVarId $ EC.lambda eVarId $
        bodyF aVar eVar $ PSeq (EC.applyMany (EC.project "insert" aVar) [elemF aVar eVar]) aVar []

    mkCondAccF elemF condF = mkAccF elemF $ \aVar eVar accumE ->
      EC.ifThenElse (EC.applyMany condF [eVar]) accumE aVar

    -- Note the element accumulator must be a function to match with our UCond pattern.
    mkIdAccF = mkAccF (\_ e -> EC.applyMany (EC.lambda "x" $ EC.variable "x") [e]) (\_ _ e -> e)

    mkIndepAccF fId fAs fArg =
      case fId of
        "filter" -> let nfAs = fAs ++ [pFusionSpec (ICond1, IdTr), pFusionLineage "filter"]
                    in return (nfAs, mkCondAccF (\_ e -> e) fArg)

        "map" -> let nfAs = fAs ++ [pOElemRec, pFusionSpec (UCond, IndepTr), pFusionLineage "map"]
                 in return (nfAs, mkAccF (\_ e -> EC.applyMany (EC.lambda "x" $ elemE $ EC.variable "x") [EC.applyMany fArg [e]]) (\_ _ e -> e))

        _ -> invalidAccFerr fId

    mkGBAccumF valueT fAs gbE accFE zE =
      let (aVar, aVarId) = (EC.variable "acc", "acc")
          (eVar, eVarId) = (EC.variable "e",   "e")
          (nVar, nVarId) = (EC.variable "x",   "x")
          (sVar, sVarId) = (EC.variable "y",   "y")

          entryE v = EC.record [("key", EC.applyMany gbE [eVar]), ("value", v)]
          lookupE  = EC.applyMany (EC.project "lookup" aVar) [nVar]

          someE = PSeq (EC.applyMany (EC.project "insert" aVar)
                          [EC.record
                            [("key", EC.project "key" sVar)
                            ,("value", EC.applyMany accFE [EC.project "value" sVar, eVar])]])
                       aVar []

          noneE = PSeq (EC.applyMany (EC.project "insert" aVar)
                          [EC.record [("key", EC.project "key" nVar), ("value", zE)]])
                       aVar []
      in do
      defaultV <- defaultExpression valueT
      return $ (fAs++[pFusionSpec (DCond2, IndepTr), pFusionLineage "groupBy"],
        EC.lambda aVarId $ EC.lambda eVarId $
          EC.letIn nVarId (entryE defaultV) $ EC.caseOf lookupE sVarId someE noneE)

    mkGBAccumE e = case collectionElementType e of
      Just (ct,et) -> mkGBAccumMap e et
      _ -> gbAccumEerr e

    mkGBAccumMap e rt = case recordType rt of
      Just [("key", kt), ("value", vt)] -> return $ ((EC.empty rt) @+ EAnnotation "Map", vt)
      _ -> gbAccumEerr e

    mkAccumE e = case collectionElementType e of
      Just (ct,et) -> return $ annotateCAccum ct $ EC.empty et
      _ -> accumEerr e

    collectionElementType e = case e @~ isEType of
      Just (EType t@(tag -> TCollection)) -> Just $ (t, head $ children t)
      _ -> Nothing

    recordType t@(tag -> TRecord ids) = Just $ zip ids $ children t
    recordType _ = Nothing

    annotateCAccum t e = foldl (@+) e $ concatMap extractTAnnotation $ annotations t
    extractTAnnotation (TAnnotation n) = [EAnnotation n]
    extractTAnnotation _ = []

    unaryTransformer   fId = fId `elem` ["map", "filter", "iterate", "ext"]
    binaryTransformer  fId = fId `elem` ["fold"]
    ternaryTransformer fId = fId `elem` ["groupBy"]

    cleanAnns as = filter (\a -> not (foldAffectedAnn a)) as
    foldAffectedAnn a = isETypeOrBound a || isEQType a || isEEffect a || isESymbol a

    markTAppChain as = cleanAnns $ nub $ as ++ [pTAppChain]

    markPureTransformer skipArg as e = cleanAnns $ nub $
      if readOnly skipArg env e then as ++ [pPureTransformer] else as ++ [pImpureTransformer]

    propagateRecType ias as = nub $ as ++ (if any isEOElemRec ias then [pIElemRec] else [])

    elemE e = EC.record [("elem", e)]

    invalidAccFerr i = Left $ "Invalid transformer function for independent accumulation: " ++ i

    gbAccumEerr e = Left . boxToString $ ["Invalid group-by result type on: "] %$ prettyLines e
    accumEerr   e = Left . boxToString $ ["Invalid accumulator construction on: "] %$ prettyLines e


fuseProgramFoldTransformers :: EffectEnv -> K3 Declaration -> Either String (K3 Declaration)
fuseProgramFoldTransformers env prog =
  mapExpression (fuseFoldTransformers env) prog >>= return . repairProgram "fusion"

fuseFoldTransformers :: EffectEnv -> K3 Expression -> Either String (K3 Expression)
fuseFoldTransformers env expr = do
    (_, eOpt) <- foldMapTree fuseUntilFirst (False, Nothing) expr
    maybe (Left "Invalid fusion result") return eOpt

  where
    --repeatE e = fuse $ (id &&& children) e
    --repeatM m = m >>= fuse . (id &&& children)

    showFusion lAs rAs =
      let (lSpec, rSpec) = (getFusionSpecA lAs, getFusionSpecA rAs)
          (lLin, rLin)   = (getFusionLineageA lAs, getFusionLineageA rAs)
      in unwords [show lLin, show rLin, show lSpec, show rSpec]

    fuseUntilFirst (unzip -> (chFused, catMaybes -> ch)) n =
      if or chFused then return $ (True, Just $ replaceCh n ch)
      else fuse (n,ch) >>= return . second Just

    fuse nch@(PPrjApp2ChainCh cE "fold" "fold" fArg1 fArg2 gArg1 gArg2
                              fAs iApp1As iApp2As gAs oApp1As oApp2As)
      | fusableChain fAs gAs
        = case fuseAccF fArg1 gArg1 fAs gAs of
            Right (Just (ngAs, ngArg1)) -> do
              let r = PPrjApp2 cE "fold" (cleanElemAnns ngAs) ngArg1 gArg2 oApp1As oApp2As
              return (True, debugFusionStep fAs gAs ngArg1 r)

            Right Nothing -> return $ (False, uncurry replaceCh nch)
            Left err -> Left err

      where cleanElemAnns as = filter (\a -> not $ isEIElemRec a) as

    fuse nch@(PApp _ _ (any isETAppChain -> True), _)   = return $ (False, uncurry replaceCh nch)
    fuse nch@(PPrj _ _ (any isETransformer -> True), _) = return $ (False, uncurry replaceCh nch)
    fuse nch = return $ (False, uncurry replaceCh nch)

    debugFusionStep fAs gAs ngArg1 e = flip trace e $
      unlines [ "Fused:" ++ showFusion fAs gAs, pretty $ stripAllExprAnnotations ngArg1 ]

    debugFusionMatching lAccF rAccF lAs rAs r =
      if True then r
      else let pp e   = pretty $ stripAllExprAnnotations e
               onFail = unlines ["Fail", pp lAccF, pp rAccF]
           in trace (unwords ["Fusing:", showFusion lAs rAs
                             , either id (maybe onFail (const "Success")) r]) r

    fuseAccF lAccF rAccF
             lAs@(getFusionSpecA -> Just (lfCls, ltCls))
             rAs@(getFusionSpecA -> Just (rfCls, rtCls))
      =
        debugFusionMatching lAccF rAccF lAs rAs $
        case (lfCls, rfCls) of
          -- Fusion for {UCond, ICond1} x {UCond, ICond1} cases
          --
          (UCond, UCond) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PUCond li lj lfE lfArg, PUCond ri rj rfE rfArg) -> do
                composedE <- chainFunctions lj lfE lfArg lAs rj rfE rfArg rAs
                return $ Just $ (updateFusionSpec rAs (UCond, promoteTCls ltCls rtCls),) $
                  mkAccF li lj composedE (\_ _ e -> e)

              -- These two cases preserve the UCond structure, provided beta reduction
              -- is able to inline the 'lE' expression into the 'rE' expression.
              -- TODO: use a property to indicate to beta reduction that it should always
              -- inline the argument (rather than in cost-based fashion).
              (PChainLambda1 li lj lE lAs1 lAs2, PUCond ri rj rfE rfArg) -> do
                let rE = mkAccE (PVar ri []) $ EC.applyMany rfE [rfArg]
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE [] [] rAs
                return $ Just $ (updateFusionSpec rAs (UCond, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PSUCond ri rj rE) -> do
                let rE' = mkAccE (PVar ri []) rE
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE' [] [] rAs
                return $ Just $ (updateFusionSpec rAs (UCond, promoteTCls ltCls rtCls), nf)

              -- General UCond fusion. We have no assurances the UCond structure is preserved.
              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          (UCond, ICond1) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              -- TODO: this duplicates lfE/lfArg, thus is not safe it has effects.
              (PUCond li lj lfE lfArg, PICond1 ri rj rpE rpArg rtE) -> do
                composedP <- chainFunctions lj lfE lfArg lAs rj rpE rpArg rAs
                composedT <- chainFunRight lj lfE lfArg lAs rj rtE rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT composedP

              -- These two cases preserve the ICond1 structure, provided beta reduction
              -- is able to inline the 'lE' expression into the 'rE' expression.
              (PChainLambda1 li lj lE lAs1 lAs2, PICond1 ri rj rpE rpArg rtE) -> do
                let riV = EC.variable ri
                let rE  = EC.ifThenElse (EC.applyMany rpE [rpArg]) (mkAccE riV rtE) riV
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE [] [] rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PSICond1 ri rj rpE rtE) -> do
                let riV = EC.variable ri
                let rE  = EC.ifThenElse rpE (mkAccE riV rtE) riV
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE [] [] rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          (ICond1, UCond) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PICond1 li lj lpE lpArg ltE, PUCond ri rj rfE rfArg) -> do
                idP       <- mkIdF False lpE lpArg
                composedT <- chainFunLeft lj ltE lAs rj rfE rfArg rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT idP

              -- Structure-preserving handling of PSICond1 and PSUCond.
              (PSICond1 li lj lpE ltE, PUCond ri rj rfE rfArg) -> do
                composedT <- chainFunLeft lj ltE lAs rj rfE rfArg rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT lpE

              (PICond1 li lj lpE lpArg ltE, PSUCond ri rj rvE) -> do
                idP       <- mkIdF False lpE lpArg
                composedT <- chainValues lj ltE lAs rj rvE rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT idP

              (PSICond1 li lj lpE ltE, PSUCond ri rj rvE) -> do
                composedT <- chainValues lj ltE lAs rj rvE rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT lpE

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          -- TODO: PSICond1 cases
          (ICond1, ICond1) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PICond1 li lj lpE lpArg ltE, PICond1 ri rj rpE rpArg rtE) -> do
                let innerF     = mkCondAccF ri rj rtE $ EC.applyMany rpE [rpArg]
                let chainInner = EC.applyMany innerF [EC.variable li, ltE]
                let callOuterP = EC.applyMany lpE [lpArg]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj chainInner callOuterP

              -- Structure-preserving PSICond1 cases
              (PSICond1 li lj lpE ltE, PICond1 ri rj rpE rpArg rtE) -> do
                let innerF     = mkCondAccF ri rj rtE $ EC.applyMany rpE [rpArg]
                let chainInner = EC.applyMany innerF [EC.variable li, ltE]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj chainInner lpE

              (PICond1 li lj lpE lpArg ltE, PSICond1 ri rj rpE rtE) -> do
                let innerF     = mkCondAccF ri rj rtE rpE
                let chainInner = EC.applyMany innerF [EC.variable li, ltE]
                let callOuterP = EC.applyMany lpE [lpArg]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj chainInner callOuterP

              (PSICond1 li lj lpE ltE, PSICond1 ri rj rpE rtE) -> do
                let innerF     = mkCondAccF ri rj rtE rpE
                let chainInner = EC.applyMany innerF [EC.variable li, ltE]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj chainInner lpE

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing


          -- Additional fusion cases with ICondN.
          --
          (UCond, ICondN) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PUCond li lj lfE lfArg, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainFunRightOpen li lj lfE lfArg lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          (ICond1, ICondN) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PICond1 li lj lpE lpArg ltE, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondRightOpen li lj lpE lpArg ltE lAs ri ri rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          (ICondN, UCond) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PChainLambda1 li lj lE lAs1 lAs2, PUCond ri rj rfE rfArg) ->
                let liV = EC.variable li
                    accumF promote e = case e of
                      (PPrjAppVarSeq ((== li) -> True) "insert" v) ->
                        let nv = EC.applyMany (EC.lambda rj $ EC.applyMany rfE [rfArg])
                                              [if promote then elemE v else v]
                        in PSeq (EC.applyMany (EC.project "insert" liV) [nv]) liV []
                      _ -> e
                in do
                  nf <- chainCondN li lj lE accumF lAs1 lAs2 lAs rAs
                  return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          (ICondN, ICond1) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PChainLambda1 li lj lE lAs1 lAs2, PICond1 ri rj rpE rpArg rtE) ->
                let liV = EC.variable li
                    accumF promote e = case e of
                      (PPrjAppVarSeq ((== li) -> True) "insert" v) ->
                        let ntE = PSeq (EC.applyMany (EC.project "insert" liV) [rtE]) liV []
                        in EC.applyMany (EC.lambda rj $ EC.ifThenElse (EC.applyMany rpE [rpArg]) ntE liV)
                                        [if promote then elemE v else v]
                      _ -> e
                in do
                  nf <- chainCondN li lj lE accumF lAs1 lAs2 lAs rAs
                  return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          (ICondN, ICondN) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing


          -- Additional fusion cases with DCond2.
          --
          (UCond, DCond2) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PUCond li lj lfE lfArg, PDCond2 ri rj rlke rci rdlV rgbF raccF rzE) ->
                let lE = EC.applyMany lfE [lfArg] in
                chainValDCond2 li lj lE [] [] ri rj rci rzE (Right (rlke, rdlV, rgbF, raccF)) lAs rAs ltCls rtCls

              (PChainLambda1 li lj lE lAs1 lAs2, PDCond2 ri rj rlke rci rdlV rgbF raccF rzE) -> do
                chainValDCond2 li lj lE lAs1 lAs2 ri rj rci rzE (Right (rlke, rdlV, rgbF, raccF)) lAs rAs ltCls rtCls

              (PChainLambda1 li lj lE lAs1 lAs2, PSDCond2 ri rj rci rentryE rsvE rnkE rzE) -> do
                chainValDCond2 li lj lE lAs1 lAs2 ri rj rci rzE (Left (rentryE, rsvE, rnkE)) lAs rAs ltCls rtCls

              (_, _) -> Right Nothing

          -- TODO: PCL1-PDCond2, PICond1-PSDCond2, PCL1-PSDCond2 cases
          (ICond1, DCond2) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PICond1 li lj lpE lpArg ltE, PDCond2 ri rj rlke rci rdlV rgbF raccF rzE) -> do
                let liV = EC.variable li
                nrF     <- mkGBAccumF ri rj rci rzE (Right (rlke, rdlV, rgbF, raccF))
                promote <- promoteRecType lAs rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, DepTr),) $
                  PChainLambda1 li lj
                    (EC.ifThenElse (EC.applyMany lpE [lpArg])
                                   (EC.applyMany nrF [liV, if promote then elemE ltE else ltE])
                                   liV) [] []

              (_, _) -> Right Nothing

          -- TODO: PSDCond2 case
          (ICondN, DCond2) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PChainLambda1 li lj lE lAs1 lAs2, PDCond2 ri rj rlke rci rdlV rgbF raccF rzE) ->
                let liV = EC.variable li
                    accumF nrF promote e = case e of
                      (PPrjAppVarSeq ((== li) -> True) "insert" v) ->
                        EC.applyMany nrF [liV, if promote then elemE v else v]
                      _ -> e
                in do
                  nrF <- mkGBAccumF ri rj rci rzE (Right (rlke, rdlV, rgbF, raccF))
                  nf  <- chainCondN li lj lE (accumF nrF) lAs1 lAs2 lAs rAs
                  return $ Just $ (updateFusionSpec rAs (ICondN, DepTr), nf)

              (_, _) -> Right Nothing

          ---- TODO: special cases for partial operation on DCond2 result.

          -- TODO: fusion can apply if the transform is an injective function on keys.
          (DCond2, UCond) | nonDepTr ltCls && nonDepTr rtCls -> Right Nothing

          -- If condition is on keys alone, lift the condition.
          --(DCond2, ICond1) | nonDepTr ltCls && rtCls == IdTr ->
          --  case (lAccF, rAccF) of
          --    (PDCond2 li lj llke lci ldlV lgbF laccF lzE
          --    , PICond1 ri rj rpE@(PLam rpi rpBodyE _) rpArg@(PVar ((== rj) -> True) _) rtE)
          --      | xxxKeyOnly rpBodyE ->
          --        let (liV, ljV) = (EC.variable li, EC.variable lj)
          --            lE = EC.ifThenElse
          --                    (EC.applyMany (EC.lambda rj $ EC.applyMany rpE [rpArg])
          --                                  [EC.record [("key", EC.applyMany lgbF ljV)]])
          --                    (EC.applyMany (mkGBAccumF li lj lci lzE (Right (llke, ldlV, lgbF, laccF)))
          --                                  [liV, ljV])
          --                    liV
          --            nf = EC.lambda li $ EC.lambda lj lE
          --        in return $ Just $ (updateFusionSpec rAs (Open, DepTr), nf)

          --    (_, _) -> Right Nothing

          -- TODO: normalize all top-level if-conditions, and lift each
          -- condition above the DCond2.
          --(DCond2, ICondN) | nonDepTr ltCls && nonDepTr rtCls ->

          -- TODO: fuse subprojections. A subprojection is where the
          -- RHS gbF is accesses subfields of the LHS gbF result, and the RHS
          -- accF performs a nesting of the LHS accF results, or distributes
          -- over the LHS accF.
          --(DCond2, DCond2) | nonDepTr ltCls && nonDepTr rtCls ->

          -- Fusion into general fold accumulator.
          (UCond,  Open) | nonDepTr ltCls -> chainUCondLambda  lAccF rAccF lAs rAs ltCls rtCls Open Open
          (ICond1, Open) | nonDepTr ltCls -> chainICond1Lambda lAccF rAccF lAs rAs ltCls rtCls Open Open
          (ICondN, Open) | nonDepTr ltCls -> chainICondNLambda lAccF rAccF lAs rAs ltCls rtCls ICondN

          (UCond,  UCondVal) | nonDepTr ltCls && nonDepTr rtCls -> chainUCondLambda  lAccF rAccF lAs rAs ltCls rtCls UCondVal ICondN
          (ICond1, UCondVal) | nonDepTr ltCls && nonDepTr rtCls -> chainICond1Lambda lAccF rAccF lAs rAs ltCls rtCls UCondVal ICondN
          (ICondN, UCondVal) | nonDepTr ltCls && nonDepTr rtCls -> chainICondNLambda lAccF rAccF lAs rAs ltCls rtCls ICondN

          -- Unhandled cases: (UCondVal, *), (Open,*), and (DCond2, Open | UCondVal)
          (_, _) -> Right Nothing

    fuseAccF _ _ _ _ = Right Nothing

    fusableChain lAs rAs = any isETransformer lAs && any isETransformer rAs
                         && (any isEPureTransformer lAs || any isEPureTransformer rAs)

    -- Case utilities
    chainUCondLambda lAccF rAccF lAs rAs ltCls rtCls nAccCls nAccCls2 = case (lAccF, rAccF) of
      (PUCond li lj lfE lfArg, PChainLambda1 ri rj rE rAs1 rAs2) -> do
        nf <- chainFunRightOpen li lj lfE lfArg lAs ri rj rE rAs1 rAs2 rAs
        return $ Just $ (updateFusionSpec rAs (nAccCls, promoteTCls ltCls rtCls), nf)

      (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
        nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
        return $ Just $ (updateFusionSpec rAs (nAccCls2, promoteTCls ltCls rtCls), nf)

      (_, _) -> Right Nothing

    chainICond1Lambda lAccF rAccF lAs rAs ltCls rtCls nAccCls nAccCls2 = case (lAccF, rAccF) of
      (PICond1 li lj lpE lpArg ltE, PChainLambda1 ri rj rE rAs1 rAs2) -> do
        nf <- chainCondRightOpen li lj lpE lpArg ltE lAs ri ri rE rAs1 rAs2 rAs
        return $ Just $ (updateFusionSpec rAs (nAccCls, promoteTCls ltCls rtCls), nf)

      (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
        nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
        return $ Just $ (updateFusionSpec rAs (nAccCls2, promoteTCls ltCls rtCls), nf)

      (_, _) -> Right Nothing

    chainICondNLambda lAccF rAccF lAs rAs ltCls rtCls nAccCls = case (lAccF, rAccF) of
      (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
        nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
        return $ Just $ (updateFusionSpec rAs (nAccCls, promoteTCls ltCls rtCls), nf)

      (_, _) -> Right Nothing

    chainValDCond2 li lj lE lAs1 lAs2 ri rj rci rzE rbgParams lAs rAs ltCls rtCls = do
      let liV = EC.variable li
      PChainLambda1 ri' rj' rE' rAs1 rAs2 <- mkGBAccumF ri rj rci rzE rbgParams
      nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri' rj' rE' rAs1 rAs2 rAs
      return $ Just $ (updateFusionSpec rAs (DCond2, promoteTCls ltCls rtCls), nf)


    -- Expression construction utilities
    chainFunctions lx lf larg lAs rx rf rarg rAs = do
      promote <- promoteRecType lAs rAs
      case rarg of
        PVar x _ | x == rx -> mkComposedF promote lf larg rf
        _ -> mkChainRightF promote lf larg rx (EC.applyMany rf [rarg])

    chainFunLeft lx le lAs rx rf rarg rAs = do
      promote <- promoteRecType lAs rAs
      case le of
        PApp lf larg _ -> chainFunctions lx lf larg lAs rx rf rarg rAs
        _ -> mkChainLeftF promote le rx rf rarg

    chainFunRight lx lf larg lAs rx re rAs = do
      promote <- promoteRecType lAs rAs
      case re of
        PVar x _ | x == rx -> mkIdF promote lf larg
        PApp rf rarg _ -> chainFunctions lx lf larg lAs rx rf rarg rAs
        _ -> mkChainRightF promote lf larg rx re

    chainFunRightOpen li lj lfE lfArg lAs ri rj rE rAs1 rAs2 rAs = do
      let (liV, le) = (EC.variable li, EC.applyMany lfE [lfArg])
      promote <- promoteRecType lAs rAs
      return $ PChainLambda1 li lj
        (EC.applyMany (PChainLambda1 ri rj rE rAs1 rAs2)
                      [liV, if promote then elemE le else le]) [] []

    chainCondRightOpen li lj lpE lpArg ltE lAs ri rj rE rAs1 rAs2 rAs = do
      let liV = EC.variable li
      promote <- promoteRecType lAs rAs
      return $ PChainLambda1 li lj
        (EC.ifThenElse (EC.applyMany lpE [lpArg])
                       (EC.applyMany (PChainLambda1 ri rj rE rAs1 rAs2)
                          [liV, if promote then elemE ltE else ltE])
                       liV) [] []

    chainValues lx le lAs rx re rAs =
      case (le, re) of
        (PApp lf larg _, PApp rf rarg _) -> chainFunctions lx lf larg lAs rx rf rarg rAs
        (PApp lf larg _, _) -> chainFunRight lx lf larg lAs rx re rAs
        (_, PApp rf rarg _) -> chainFunLeft lx le lAs rx rf rarg rAs
        (_, _) -> promoteRecType lAs rAs >>= \p -> mkChainVals p le rx re

    chainCondN li lj lE accumF lAs1 lAs2 lAs rAs = do
      promote <- promoteRecType lAs rAs
      let (_,nlE) = mapAccumulation (accumF promote) (\x -> x) li lE
      return $ PChainLambda1 li lj nlE lAs1 lAs2

    chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs =
      let (liV, riV) = (EC.variable li, EC.variable ri)
          accumF promote e = case e of
            (PPrjAppVarSeq ((== li) -> True) "insert" v) ->
              EC.applyMany (PChainLambda1 ri rj rE rAs1 rAs2)
                           [liV, if promote then elemE v else v]
            _ -> e
      in chainCondN li lj lE accumF lAs1 lAs2 lAs rAs

    mkIdF promote lf larg =
      let fE = EC.applyMany lf [larg]
      in return $ if promote then elemE fE else fE

    mkComposedF promote lf larg rf = do
      f <- composeUnaryPair promote rf lf
      return $ EC.applyMany f [larg]

    mkChainLeftF promote le rx rf rarg = return $
      EC.applyMany
        (EC.lambda rx $ EC.applyMany rf [rarg])
        [if promote then elemE le else le]

    mkChainRightF promote lf larg rx re =
      let fE   = EC.applyMany lf [larg]
          argE = if promote then elemE fE else fE
      in return $ EC.applyMany (EC.lambda rx re) [argE]

    mkChainVals promote le rx re = return $
      EC.applyMany (EC.lambda rx re) [if promote then elemE le else le]

    composeUnaryPair asElem g f = return $ EC.lambda "x" $
      EC.applyMany (applyWithElemRec asElem g) [EC.applyMany f [EC.variable "x"]]

    applyWithElemRec True  lamE = EC.lambda "xToWrap" $ EC.applyMany lamE [elemVar "xToWrap"]
    applyWithElemRec False lamE = lamE

    elemVar id = EC.record [("elem", EC.variable id)]
    elemE    e = EC.record [("elem", e)]

    --promoteRecType lAs rAs =
    --  case (any isEOElemRec lAs, any isEIElemRec rAs) of
    --    (i,j) | i /= j -> Left $ "Invalid fusion chained element type annotations"
    --    (True, True)   -> Right True
    --    (_, _)         -> Right False

    promoteRecType lAs rAs = Right False

    mkAccE accE eE = PSeq (PPrjApp accE "insert" [] eE []) accE []

    mkAccF aVarId eVarId insertElemE bodyF =
      let (aVar, eVar) = (EC.variable aVarId, EC.variable eVarId)
      in PChainLambda1 aVarId eVarId (bodyF aVar eVar $ mkAccE aVar insertElemE) [] []

    mkCondAccF aVarId eVarId insertElemE condE =
      mkAccF aVarId eVarId insertElemE
        $ \aVar _ accumE -> EC.ifThenElse condE accumE aVar

    mkGBAccumF i j ci zE gbParams =
      let iV   = EC.variable i
          jV   = EC.variable j
          ciV  = EC.variable ci

          (entryEOpt, lookupE, someE, noneE) = case gbParams of
            Left (enE, svE, nkE) ->
              let lookupE = EC.applyMany (EC.project "lookup" iV) [enE]

                  someE = PSeq (EC.applyMany (EC.project "insert" iV)
                                  [EC.record
                                    [("key", EC.project "key" ciV)
                                    ,("value", svE)]])
                               iV []

                  noneE = PSeq (EC.applyMany (EC.project "insert" iV)
                                  [EC.record [("key", nkE), ("value", zE)]])
                               iV []
              in (Nothing, lookupE, someE, noneE)

            Right (lke, dlV, gbE, accFE) ->
              let lkeV     = EC.variable lke
                  entryE   = EC.record [("key", EC.applyMany gbE [jV]), ("value", dlV)]
                  lookupE  = EC.applyMany (EC.project "lookup" iV) [lkeV]

                  someE = PSeq (EC.applyMany (EC.project "insert" iV)
                                  [EC.record
                                    [("key", EC.project "key" ciV)
                                    ,("value", EC.applyMany accFE [EC.project "value" ciV, jV])]])
                               iV []

                  noneE = PSeq (EC.applyMany (EC.project "insert" iV)
                                  [EC.record [("key", EC.project "key" lkeV), ("value", zE)]])
                               iV []
              in (Just (lke, entryE), lookupE, someE, noneE)

      in
      return $ EC.lambda i $ EC.lambda j $ case entryEOpt of
        Just (lke, entryE) -> EC.letIn lke entryE $ EC.caseOf lookupE ci someE noneE
        Nothing -> EC.caseOf lookupE ci someE noneE


    simplifyLambda e = runIdentity $ betaReduction env e

    -- Fusion spec helpers
    updateFusionSpec as spec = filter (not . isEFusionSpec) as ++ [pFusionSpec spec]

    promoteTCls a b = toEnum $ max (fromEnum a) (fromEnum b)

    nonDepTr DepTr = False
    nonDepTr _ = True


-- | Infer return points in expressions that are collection insertions to the given variable.
--   Every return expression must be one of:
--     i.   accumulating expression
--     ii.  alias (for now we consider only the exact variable, not equivalent bnds)
--     iii. a branching expression composed only of accumulating expression and alias children.
mapAccumulation :: (K3 Expression -> K3 Expression)
                -> (K3 Expression -> K3 Expression)
                -> Identifier -> K3 Expression -> (Bool, K3 Expression)
mapAccumulation onAccumF onRetVarF i expr = runIdentity $ do
  (isAcc, e) <- doInference
  return $ (either id id isAcc, e)

  where
    doInference =
      foldMapReturnExpression trackBindings debugReturnAsAccumulator independentF (False, False) (Left False) expr

    debugReturnAsAccumulator tdAcc chAcc e =
      if True then returnAsAccumulator tdAcc chAcc e
      else do
        (rst, e') <- returnAsAccumulator tdAcc chAcc e
        flip trace (return (rst, e')) $! unlines [unwords ["RAA", i, show tdAcc, show rst], pretty e, pretty e']

    trackBindings sp@(shadowed, _) (PPrjAppVarSeq j "insert" v)
      | i == j && not shadowed && notAccessedIn v
         = return (sp, [(shadowed, True), (shadowed, True)])

    trackBindings sp e = case tag e of
        ELambda j -> return (sp, [onBinding sp j])
        ELetIn  j -> return (sp, [sp, onBinding sp j])
        ECaseOf j -> return (sp, [sp, onBinding sp j, sp])
        EBindAs b -> return (sp, [sp, foldl onBinding sp $ bindingVariables b])
        _ -> return (sp, replicate (length $ children e) sp)

      where onBinding shpr j = if i == j then (True, False) else shpr

    -- TODO: check effects and lineage rather than free variables.
    independentF (shadowed, _) _ e
      | EVariable j <- tag e , i == j && not shadowed = return (Right False, e)
      | EAssign   j <- tag e , i == j && not shadowed = return (Right False, e)

    independentF _ (onIndepR -> isAccum) e = return (isAccum, e)

    -- TODO: using symbols as lineage here will provide better alias tracking.
    -- TODO: test in-place modification property
    returnAsAccumulator (shadowed, _) _ e@(PPrjAppVarSeq j "insert" v)
      | i == j && not shadowed && notAccessedIn v =
          if True then return (Right True, onAccumF e)
          else trace ("onAccumF " ++ pretty e) $ return (Right True, onAccumF e)

    returnAsAccumulator (shadowed, protected) _ e@(tag -> EVariable j)
      | i == j && not shadowed =
        if True then
          if protected then return (Left True, e)
                       else return (Left True, onRetVarF e)
        else
          if protected then trace ("onSkipE " ++ pretty e)   $ return (Left True, e)
                       else trace ("onRetVarF " ++ pretty e) $ return (Left True, onRetVarF e)

    returnAsAccumulator _ (onReturnBranch [0]   -> isAccum) e@(tag -> ELambda _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [0]   -> isAccum) e@(tag -> EOperate OApp) = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1]   -> isAccum) e@(tag -> EOperate OSeq) = return (isAccum, e)

    returnAsAccumulator _ (onReturnBranch [1]   -> isAccum) e@(tag -> ELetIn  _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1]   -> isAccum) e@(tag -> EBindAs _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1,2] -> isAccum) e@(tag -> ECaseOf _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1,2] -> isAccum) e@(tag -> EIfThenElse)   = return (isAccum, e)

    returnAsAccumulator _ _ e = return (Right False, e)

    -- TODO: replace with testing effects/symbol.
    notAccessedIn e =
      let r = i `notElem` (freeVariables e)
      in if True then r
                 else trace (unlines [unwords ["NAI", i, show r], pretty e]) r

    onIndepR l = if any not $ rights l then Right False else Left False

    onReturnBranch branchIds l =
      if any not $ rights l then Right False
      else ensureEither (map (l !!) branchIds)

    ensureEither l =
      if all id (lefts l) && all id (rights l)
        then (if null $ rights l then Left else Right) True
        else Right False

inferAccumulation :: Identifier -> K3 Expression -> Bool
inferAccumulation i expr = fst $ mapAccumulation annotationAccumE id i expr
  where annotationAccumE e = e @+ EProperty "Accumulation" Nothing

rewriteStreamAccumulation :: Identifier -> K3 Expression -> (Bool, K3 Expression)
rewriteStreamAccumulation i expr = mapAccumulation rewriteAccumE rewriteVarE i expr
  where rewriteAccumE (PPrjAppVarSeq _ "insert" v) = EC.some v
        rewriteAccumE e = e
        rewriteVarE   _ = EC.constant $ CNone NoneImmut


-- Helper patterns for fusion
pattern PVar     i           iAs   = Node (EVariable i   :@: iAs)   []
pattern PApp     fE  argE    appAs = Node (EOperate OApp :@: appAs) [fE, argE]
pattern PSeq     lE  rE      seqAs = Node (EOperate OSeq :@: seqAs) [lE, rE]
pattern PLam     i   bodyE   iAs   = Node (ELambda i     :@: iAs)   [bodyE]
pattern PPrj     cE  fId     fAs   = Node (EProject fId  :@: fAs)   [cE]
pattern PInd     iE          iAs   = Node (EIndirect     :@: iAs)   [iE]
pattern PRec     ids fieldsE rAs   = Node (ERecord ids   :@: rAs)   fieldsE
pattern PTup         fieldsE tAs   = Node (ETuple        :@: tAs)   fieldsE

pattern PSome sE optAs = Node (ESome :@: optAs) [sE]
pattern PNone nm optAs = Node (EConstant (CNone nm) :@: optAs) []

pattern PLetIn       srcE i   bodyE lAs          = Node (ELetIn  i     :@: lAs) [srcE, bodyE]
pattern PBindAs      srcE bnd bodyE bAs          = Node (EBindAs bnd   :@: bAs) [srcE, bodyE]
pattern PCaseOf      caseE varId someE noneE cAs = Node (ECaseOf varId :@: cAs) [caseE, someE, noneE]
pattern PIfThenElse  pE tE eE cAs                = Node (EIfThenElse   :@: cAs) [pE, tE, eE]

pattern PAppLam i bodyE argE lamAs appAs = PApp (PLam i bodyE lamAs) argE appAs
pattern PApp2 f arg1 arg2 iAppAs oAppAs  = PApp (PApp f arg1 iAppAs) arg2 oAppAs

pattern PBindInd i iE bodyE iAs bAs            = PBindAs (PInd iE          iAs) (BIndirection i)   bodyE bAs
pattern PBindTup ids fieldsE bodyE tAs bAs     = PBindAs (PTup fieldsE     tAs) (BTuple       ids) bodyE bAs
pattern PBindRec ijs ids fieldsE bodyE rAs bAs = PBindAs (PRec ids fieldsE rAs) (BRecord      ijs) bodyE bAs

pattern PPrjRec fId ids fieldsE fAs rAs = PPrj (PRec ids fieldsE rAs) fId fAs

pattern PPrjApp cE fId fAs fArg iAppAs = PApp (PPrj cE fId fAs) fArg iAppAs

pattern PPrjApp2 cE fId fAs fArg1 fArg2 app1As app2As
  = PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As

pattern PPrjApp3 cE fId fAs fArg1 fArg2 fArg3 app1As app2As app3As
  = PApp (PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As) fArg3 app3As

pattern PPrjAppVarSeq i prjId arg <-
  PSeq (PPrjApp (PVar i _) prjId _ arg _) (PVar ((== i) -> True) _) _

pattern PChainLambda1 i j bodyE iAs jAs = PLam i (PLam j bodyE jAs) iAs

pattern PStreamableTransformerArg i j iAs jAs <-
  PChainLambda1 i j (inferAccumulation i -> True) iAs jAs

pattern PPrjApp2Chain cE fId gId fArg1 fArg2 gArg1 gArg2 fAs iApp1As iApp2As gAs oApp1As oApp2As =
  PPrjApp2 (PPrjApp2 cE fId fAs fArg1 fArg2 iApp1As iApp2As) gId gAs gArg1 gArg2 oApp1As oApp2As

pattern PPrjApp2ChainCh cE fId gId fArg1 fArg2 gArg1 gArg2 fAs iApp1As iApp2As gAs oApp1As oApp2As <-
  (PApp _ _ oApp2As
  , [PPrjApp (PPrjApp2 cE fId fAs fArg1 fArg2 iApp1As iApp2As) gId gAs gArg1 oApp1As, gArg2])

{- Fusion accumulator patterns -}
pattern PUCond i j fE fArg <-
  PChainLambda1 i j
    (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _ (PApp fE fArg _) _)
          (PVar ((== i) -> True) _) _) _ _

pattern PSUCond i j vE <-
  PChainLambda1 i j
    (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _ vE _)
          (PVar ((== i) -> True) _) _) _ _

pattern PICond1 i j pE pArg tE <-
  PChainLambda1 i j
    (PIfThenElse (PApp pE pArg _)
                 (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _ tE _)
                       (PVar ((== i) -> True) _) _)
                 (PVar ((== i) -> True) _) _) _ _

pattern PSICond1 i j pE tE <-
  PChainLambda1 i j
    (PIfThenElse pE
                 (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _ tE _)
                       (PVar ((== i) -> True) _) _)
                 (PVar ((== i) -> True) _) _) _ _

pattern PDCond2 i j lke ci dlV gbF accF zE <-
  PChainLambda1 i j
    (PLetIn
      (PRec ["key", "value"] [PApp gbF (PVar ((== j) -> True) _) _, dlV] _)
      lke
      (PCaseOf (PPrjApp (PVar ((== i) -> True) _) "lookup" _
                        (PVar ((== lke) -> True) _) _)
               ci
               (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _
                              (PRec ["key", "value"]
                                    [(PPrj (PVar ((== ci) -> True) _) "key" _)
                                    ,(PApp2 accF (PPrj (PVar ((== ci) -> True) _) "value" _)
                                                 (PVar ((== j) -> True) _) _ _)]
                              _) _)
                     (PVar ((== i) -> True) _) _)
               (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _
                              (PRec ["key", "value"]
                                    [(PPrj (PVar ((== lke) -> True) _) "key" _), zE] _) _)
                     (PVar ((== i) -> True) _) _)
               _)
      _) _ _

pattern PSDCond2 i j ci entryE svE nkE zE <-
  PChainLambda1 i j
    (PCaseOf (PPrjApp (PVar ((== i) -> True) _) "lookup" _ entryE _)
              ci
              (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _
                             (PRec ["key", "value"]
                                   [(PPrj (PVar ((== ci) -> True) _) "key" _), svE]
                             _) _)
                    (PVar ((== i) -> True) _) _)
              (PSeq (PPrjApp (PVar ((== i) -> True) _) "insert" _
                             (PRec ["key", "value"]
                                   [nkE, zE] _) _)
                    (PVar ((== i) -> True) _) _)
              _) _ _
