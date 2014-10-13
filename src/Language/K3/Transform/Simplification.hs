{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.Simplification where

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
betaReductionOnProgram :: K3 Declaration -> K3 Declaration
betaReductionOnProgram prog = runIdentity $ mapExpression betaReduction prog

betaReduction :: K3 Expression -> Identity (K3 Expression)
betaReduction expr = mapTree reduce expr
  where
    reduce ch n@(tag -> ELetIn i) = reduceOnOccurrences n ch i (head ch) $ last ch

    reduce ch n@(tag -> EOperate OApp) =
      let (fn, arg) = (head ch, last ch) in
      case tag fn of
        ELambda i -> reduceOnOccurrences n ch i arg $ head $ children fn

        _ -> rebuildNode n ch

    reduce ch n = rebuildNode n ch

    reduceOnOccurrences n ch i ie e =
      let occurrences = length $ filter (== i) $ freeVariables e in
      if occurrences <= 1
        then betaReduction $ substituteImmutBinding i ie e
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
eliminateDeadProgramCode :: K3 Declaration -> Either String (K3 Declaration)
eliminateDeadProgramCode = mapExpression eliminateDeadCode

eliminateDeadCode :: K3 Expression -> Either String (K3 Expression)
eliminateDeadCode expr = mapTree pruneExpr expr
  where
    pruneExpr ch n@(tag -> ELetIn  i) =
      let vars = freeVariables $ last ch in
      if maybe False (const $ i `notElem` vars) $ (head ch) @~ isEPure
        then return $ last ch
        else rebuildNode n ch

    pruneExpr ch n@(tag -> EBindAs b) =
      let vars = freeVariables $ last ch in
      case b of
        BRecord ijs ->
          let nBinder = BRecord $ filter (\(_,j) -> j `elem` vars) ijs
          in return $ Node (EBindAs nBinder :@: annotations n) ch
        _ -> rebuildNode n ch

    pruneExpr ch n@(tag -> ECaseOf _) = rebuildNode n ch

    pruneExpr ch n@(tag -> EOperate OSeq) =
        case (head ch) @~ isEPure of
          Nothing -> rebuildNode n ch
          Just _  -> return $ last ch

    pruneExpr ch n = rebuildNode n ch

    rebuildNode n@(tag -> EConstant _) _ = return n
    rebuildNode n@(tag -> EVariable _) _ = return n
    rebuildNode n ch = return $ Node (tag n :@: annotations n) ch


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

commonProgramSubexprElim :: K3 Declaration -> Either String (K3 Declaration)
commonProgramSubexprElim prog = mapExpression commonSubexprElim prog

commonSubexprElim :: K3 Expression -> Either String (K3 Expression)
commonSubexprElim expr = do
    cTree <- buildCandidateTree expr
    pTree <- pruneCandidateTree cTree
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
              bnds      = case tag t of
                                ELambda i -> [[i]]
                                ELetIn  i -> [[], [i]]
                                ECaseOf j -> [[], [j], []]
                                EBindAs b -> [[], bindingVariables b]
                                _         -> repeat []
              filteredCands = nub $ concatMap filterOpenCandidates $ zip bnds subAcc
              localCands    = sortBy ((flip compare) `on` snd) $
                                foldl (addCandidateIfLCA subAcc) [] filteredCands
              candTreeNode  = Node (uid, localCands) $ concat ctCh
              nStrippedExpr = Node (tag t :@: (filter isEQualified $ annotations t)) $ concat sExprCh
              propagatedExprs = maybe [] (const $ (concat subAcc)++[nStrippedExpr]) $ n @~ isEPure
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
      return $ ([ctNode], [Node (tag e :@: (filter isEQualified $ annotations e)) []], [])

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
          let used = filter (`elem` candAcc) cands
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

streamableTransformerArg :: K3 Expression -> Bool
streamableTransformerArg (PStreamableTransformerArg _ _ _ _) = True
streamableTransformerArg _ = False

-- | Marks all function applications that are fusable transformers as a
--   preprocessing for fusion optimizations.
inferFusableProgramApplies :: K3 Declaration -> Either String (K3 Declaration)
inferFusableProgramApplies prog = mapExpression inferFusableExprApplies prog

inferFusableExprApplies :: K3 Expression -> Either String (K3 Expression)
inferFusableExprApplies expr = modifyTree fusable expr >>= modifyTree annotateStream
  where
    fusable e@(PPrjApp  cE fId fAs
                        fArg@(streamableTransformerArg -> streamable) appAs)
      | unaryTransformer fId && any isETransformer fAs
        = return $ PPrjApp cE fId nfAs fArg nappAs
        where
          nfAs   = markPureTransformer [fArg] fAs
          nappAs = markOElemRec fId $ markTAppChain $ markStreamableApp streamable appAs

    fusable e@(PPrjApp2 cE fId fAs
                        fArg1@(streamableTransformerArg -> streamable) fArg2
                        app1As app2As)
      | binaryTransformer fId && any isETransformer fAs
        = return $ PPrjApp2 cE fId nfAs fArg1 fArg2 napp1As napp2As
        where
          nfAs    = markPureTransformer [fArg1] fAs
          napp1As = markTAppChain $ markStreamableApp streamable app1As
          napp2As = markTAppChain app2As

    fusable e@(PPrjApp3 cE fId fAs
                        fArg1@(streamableTransformerArg -> streamable) fArg2 fArg3
                        app1As app2As app3As)
      | ternaryTransformer fId && any isETransformer fAs
        = return $ PPrjApp3 cE fId nfAs fArg1 fArg2 fArg3 napp1As napp2As napp3As
        where
          nfAs    = markPureTransformer [fArg1, fArg2] fAs
          napp1As = markTAppChain $ markStreamableApp streamable app1As
          napp2As = markTAppChain app2As
          napp3As = markTAppChain app3As

    fusable e = return e

    -- TODO: ternary chain matching for streamability as needed for groupBys
    annotateStream e@(PChainPrjApp1 cE fId gId fArg gArg fAs iAppAs gAs oAppAs)
      | isAnyStreamed iAppAs && any isETransformer gAs
          = return $ PChainPrjApp1 cE fId gId fArg gArg fAs niAppAs gAs noAppAs

      | otherwise = return $ PChainPrjApp1 cE fId gId fArg gArg fAs iAppAs gAs noAppAs'
      where niAppAs  = markStreamApp iAppAs
            noAppAs  = propagateElemRec gId iAppAs $ propagateStreamApp gId oAppAs
            noAppAs' = propagateElemRec gId iAppAs oAppAs

    annotateStream e@(PChainPrjApp2 cE fId gId fArg gArg1 gArg2 fAs iAppAs gAs oApp1As oApp2As)
      | isAnyStreamed iAppAs && any isETransformer gAs
          = return $ PChainPrjApp2 cE fId gId fArg gArg1 gArg2 fAs niAppAs gAs noApp1As oApp2As

      | otherwise = return $ PChainPrjApp2 cE fId gId fArg gArg1 gArg2 fAs iAppAs gAs noApp1As' oApp2As
      where niAppAs   = markStreamApp iAppAs
            noApp1As  = propagateElemRec gId iAppAs $ propagateStreamApp gId oApp1As
            noApp1As' = propagateElemRec gId iAppAs oApp1As

    annotateStream e@(PChainPrjApp3 cE fId gId
                                    fArg gArg1 gArg2 gArg3
                                    fAs iAppAs gAs oApp1As oApp2As oApp3As)

      | isAnyStreamed iAppAs && any isETransformer gAs
          = return $ PChainPrjApp3 cE fId gId fArg gArg1 gArg2 gArg3
                                   fAs niAppAs gAs noApp1As noApp2As oApp3As

      | otherwise = return $ PChainPrjApp3 cE fId gId fArg gArg1 gArg2 gArg3
                                           fAs iAppAs gAs noApp1As' noApp2As oApp3As
      where niAppAs   = markStreamApp iAppAs
            noApp1As  = propagateElemRec gId iAppAs $ propagateStreamApp gId oApp1As
            noApp2As  = propagateElemRec gId iAppAs oApp2As
            noApp1As' = propagateElemRec gId iAppAs oApp1As

    annotateStream e@(PBinChainPrjApp1 cE fId gId
                                       fArg1 fArg2 gArg
                                       fAs iApp1As iApp2As gAs oAppAs)

      | isAnyStreamed iApp1As && any isETransformer gAs
          = return $ PBinChainPrjApp1 cE fId gId fArg1 fArg2 gArg
                                      fAs niApp1As iApp2As gAs noAppAs

      | otherwise = return e
      where niApp1As = markStreamApp iApp1As
            noAppAs  = propagateStreamApp gId oAppAs

    annotateStream e@(PBinChainPrjApp2 cE fId gId
                                       fArg1 fArg2 gArg gArg2
                                       fAs iApp1As iApp2As gAs oApp1As oApp2As)

      | isAnyStreamed iApp1As && any isETransformer gAs
          = return $ PBinChainPrjApp2 cE fId gId fArg1 fArg2 gArg gArg2
                                      fAs niApp1As iApp2As gAs noApp1As oApp2As

      | otherwise = return e
      where niApp1As = markStreamApp iApp1As
            noApp1As = propagateStreamApp gId oApp1As

    annotateStream e@(PBinChainPrjApp3 cE fId gId
                                       fArg1 fArg2 gArg gArg2 gArg3
                                       fAs iApp1As iApp2As gAs oApp1As oApp2As oApp3As)

      | isAnyStreamed iApp1As && any isETransformer gAs
          = return $ PBinChainPrjApp3 cE fId gId
                                      fArg1 fArg2 gArg gArg2 gArg3
                                      fAs niApp1As iApp2As gAs noApp1As oApp2As oApp3As

      | otherwise = return e
      where niApp1As = markStreamApp iApp1As
            noApp1As = propagateStreamApp gId oApp1As

    annotateStream e@(PApp _ _ (any isETAppChain -> True))   = return e
    annotateStream e@(PPrj _ _ (any isETransformer -> True)) = return e

    annotateStream e = mapM unstreamChild (children e) >>= return . replaceCh e

    unstreamChild :: K3 Expression -> Either String (K3 Expression)
    unstreamChild e@(PAnyStream1 _ _ appAs) = markUnstreamApp e appAs >>= return . (e @<-)

    unstreamChild e@(PAnyStream2 fE arg1E arg2E app1As app2As) =
      markUnstreamApp e app1As >>= \napp1As ->
        return $ PApp (PApp fE arg1E app1As @<- napp1As) arg2E app2As

    unstreamChild e@(PAnyStream3 fE arg1E arg2E arg3E app1As app2As app3As) =
      markUnstreamApp e app1As >>= \napp1As ->
        return $ PApp (PApp (PApp fE arg1E app1As @<- napp1As) arg2E app2As) arg3E app3As

    unstreamChild e = return $ e

    markStreamableApp streamable as =
      nub $ as ++ (if streamable then [pStreamable] else []) ++ [pFusable]

    markPureTransformer el as =
      nub $ maybe (as ++ [pImpureTransformer]) (const $ as ++ [pPureTransformer])
          $ mapM (@~ isEPure) el

    markStreamApp as = nub $ as ++ [pStream]

    markUnstreamApp e as = case e @~ isEType of
        Just (EType t) -> return $ markUnstreamAppT t as
        _ -> Left $ boxToString $ ["No type found on "] %+ prettyLines e

    markUnstreamAppT t as =
      if not $ any isEStream as then as
      else filter (not . isEStream) as ++ [pUnstream t]

    markTAppChain as = nub $ as ++ [pTAppChain]

    markOElemRec "map" as = nub $ as ++ [pOElemRec]
    markOElemRec _ as = as

    propagateStreamApp "iterate" oAs = markUnstreamAppT TC.unit $ markStreamApp oAs
    propagateStreamApp oId oAs       = markStreamApp oAs

    propagateElemRec gId ias as =
      nub $ as ++ if any isEOElemRec ias
                    then ([pIElemRec] ++ if gId == "filter" then [pOElemRec] else [])
                    else []

    isAnyStreamed as = any isEStreamable as || any isEStream as

    unaryTransformer   fId = fId `elem` ["map", "filter", "iterate", "ext"]
    binaryTransformer  fId = fId `elem` ["fold"]
    ternaryTransformer fId = fId `elem` ["groupBy"]

-- TODO: recompute types, and iterate to fusion fixpoint
fuseProgramTransformers :: K3 Declaration -> Either String (K3 Declaration)
fuseProgramTransformers prog = mapExpression fuseTransformers prog >>= return . repairProgram "fusion"

-- TODO: purity: at most one of the transformers must be impure on fusion edges
-- TODO: binary and ternary chain matching
fuseTransformers :: K3 Expression -> Either String (K3 Expression)
fuseTransformers expr = mapTree (flip $ curry fuse) expr
  where
    repeatE e = fuse $ (id &&& children) e
    repeatM m = m >>= fuse . (id &&& children)

    -- TODO: anything to do with arg2E? At least ensure it is an empty collection.
    -- Rewrites fold to: cE.map (\j -> ... Some v ... None)
    -- where Some v and None are the new return values of the accumulation
    fuse nch@(PStreamableTransformerPair cE fId bodyE i j _ iAs jAs fAs appAs)
      | fId == "fold" && any isEStream appAs =

          let (isAccum, nBodyE) = rewriteAccumulation i bodyE
              passThruP a = isEIElemRec a || isETAppChain a

              markPureTransformer e pure =
                let eAnns = [pTransformer] ++ (if pure then [pPureTransformer] else [pImpureTransformer])
                in foldl (@+) e eAnns

              markStreamedApp e = foldl (@+) e $ [pStream, pFusable, pHasSkip, pOElemRec]
                                                 ++ (filter passThruP appAs)

              nBAnns = filter isEPure iAs
              nArgE  = foldl (@+) (EC.lambda j nBodyE) nBAnns
              nPrjE  = markPureTransformer (EC.project "map" cE) $ any isEPure iAs
              nAppE  = markStreamedApp $ EC.applyMany nPrjE [nArgE]
          in
          let isInferAccum = inferAccumulation i bodyE in
          logRewrite "fold-as-map" nch $
            if isAccum
              then repeatE nAppE
              else Left $ boxToString $ (["Invalid accumulator for fold-as-map rewrite "] %+ prettyLines bodyE)
                                          %$ ["Inferred: " ++ show isInferAccum ++ " " ++ show isAccum]

    fuse nch@(PChainPrjApp1Pair _ fId gId _ _ _ iAppAs _ oAppAs)
      | iStream   <- any isEStream   iAppAs
      , oStream   <- any isEStream   oAppAs
      , oUnstream <- any isEUnstream oAppAs
      , iHasSkip  <- any isEHasSkip  iAppAs
      , oHasSkip  <- any isEHasSkip  oAppAs
      , oIElemRec <- any isEIElemRec oAppAs
      , any isEFusable iAppAs && any isEFusable oAppAs
        && validateStreamedPair fId iStream gId oStream oUnstream iHasSkip oHasSkip =
          case (fId, gId) of
            -- Map function fusion
            ("map", "map"    ) -> repeatM $ logRewrite "map-map"     nch $ rewriteUnaryPair nch oStream oUnstream oHasSkip oIElemRec
            ("map", "filter" ) -> repeatM $ logRewrite "map-filter"  nch $ rewriteUnaryPair nch oStream oUnstream oHasSkip oIElemRec
            ("map", "iterate") -> repeatM $ logRewrite "map-iterate" nch $ rewriteUnaryPair nch oStream oUnstream oHasSkip oIElemRec

            -- Filter fusion
            ("filter", "filter")  -> repeatM $ logRewrite "filter-filter"  nch $ rewriteUnaryPair nch oStream oUnstream oHasSkip oIElemRec
            ("filter", "iterate") -> repeatM $ logRewrite "filter-iterate" nch $ rewriteUnaryPair nch oStream oUnstream oHasSkip oIElemRec

            _ -> uncurry rebuildE nch

      | otherwise = uncurry rebuildE nch

    fuse nch@(PChainPrjApp2Pair _ fId gId _ _ _ _ iAppAs _ oApp1As _)
      | iStream   <- any isEStream   iAppAs
      , oStream   <- any isEStream   oApp1As
      , oUnstream <- any isEUnstream oApp1As
      , iHasSkip  <- any isEHasSkip  iAppAs
      , oHasSkip  <- any isEHasSkip  oApp1As
      , oIElemRec <- any isEIElemRec oApp1As
      , any isEFusable iAppAs && any isEFusable oApp1As
        && validateStreamedPair fId iStream gId oStream oUnstream iHasSkip oHasSkip =
          case (fId, gId, oStream) of
            -- Map and filter fusion
            -- rewriteUnaryBinary handles unstreaming in the fold function.
            ("map",    "fold", False) -> repeatM $ logRewrite "map-fold"    nch $ rewriteUnaryBinary nch iStream oIElemRec
            ("filter", "fold", False) -> repeatM $ logRewrite "filter-fold" nch $ rewriteUnaryBinary nch iStream oIElemRec

            _ -> uncurry rebuildE nch

      | otherwise = uncurry rebuildE nch

    fuse nch@(PChainPrjApp3Pair _ fId gId _ _ _ _ _ iAppAs _ oApp1As oApp2As _)
      | iStream   <- any isEStream    iAppAs
      , oStream   <- any isEStream    oApp1As
      , oUnstream <- any isEUnstream  oApp1As
      , iHasSkip  <- any isEHasSkip   iAppAs
      , oHasSkip  <- any isEHasSkip   oApp1As
      , o1IElemRec <- any isEIElemRec oApp1As
      , o2IElemRec <- any isEIElemRec oApp2As
      , any isEFusable iAppAs && any isEFusable oApp1As
        && validateStreamedPair fId iStream gId oStream oUnstream iHasSkip oHasSkip =
          case (fId, gId, iStream, oStream) of
            -- Map function fusion
            ("map", "groupBy", False, False) -> logRewrite "map-groupBy" nch $ rewriteUnaryTernary nch o1IElemRec o2IElemRec

            -- GroupBy cannot support skips without treating the result as an accumulator
            -- That is, we must implement groupBy as a fold with a associative accumulator.
            (_, "groupBy", True, _)          -> Left $ "Unsupported stream operation as input to groupBy"
            _ -> uncurry rebuildE nch

      | otherwise = uncurry rebuildE nch

    fuse nch@(PApp _ _ (any isETAppChain -> True), _)   = uncurry rebuildE nch
    fuse nch@(PPrj _ _ (any isETransformer -> True), _) = uncurry rebuildE nch

    fuse nch = mapM unstreamChild (snd nch) >>= rebuildE (fst nch)

    unstreamChild e@(PUnstream1 (PPrj cE fId fAs) argE oAppAs)
      | oIElemRec <- any isEIElemRec oAppAs
      , oOElemRec <- any isEOElemRec oAppAs
      , fId == "map" || fId == "filter" =
          case find isEUnstream oAppAs of
            Just (EProperty "Unstream" (literalTypeAsTElement -> Just cT)) ->
              let emptyE = EC.constant $ CEmpty cT
                  argYE  = if oIElemRec then EC.record [("elem", EC.variable "y")]
                                        else EC.variable "y"
                  resE e = if oOElemRec then EC.record [("elem", e)] else e

                  appArgE   = EC.applyMany argE [argYE]
                  insertE e = EC.applyMany (EC.project "insert" $ EC.variable "x") [e]
                  accumE  e = EC.binop OSeq (insertE e) $ EC.variable "x"

                  (testE, valE) = if fId == "map"
                                    then (EC.caseOf appArgE "r", EC.variable "r")
                                    else (EC.ifThenElse appArgE, EC.variable "y")

                  foldFE = simplifyLambda $ binaryLambda "x" "y" $
                             uncurry testE ( accumE $ resE $ valE
                                           , EC.variable "x" )
              in
              Right $ EC.applyMany (EC.project "fold" cE) [foldFE, emptyE]

            _ -> Left $ "No valid unstream annotation found on application"

      | fId == "iterate" =
          let iterateFE = simplifyLambda $ EC.lambda "x" $
                EC.caseOf (EC.applyMany argE [EC.variable "x"]) "r" EC.unit EC.unit
          in
          Right $ EC.applyMany (EC.project "iterate" cE) [iterateFE]

      where simplifyLambda e = runIdentity $ betaReduction e

    unstreamChild e = Right e

    validateStreamedPair fId fStream gId gStream gUnstream iHasSkip oHasSkip =
      case (fId, fStream, gId, gStream, gUnstream) of
        (_, False, "map", True,  False) -> (not iHasSkip) && oHasSkip
        (_, False, _,     False, False) -> (not iHasSkip) && (not oHasSkip)
        (_, True,  _, x, y)             -> not (gStream && gUnstream) && iHasSkip
        (_, _,     _, _, _)             -> False

    rewriteUnaryPair (PChainPrjApp1Pair cE fId gId fArg gArg _ iAppAs gAs oAppAs)
                     gStream gUnstream gHasSkip gIElemRec
      | fId == "map" =
        let skipVOpt = if gId == "filter" then Just $ Left $ EC.constant $ CBool False
                                          else Nothing

            (ngArg, noAppAs) = if (gStream || gUnstream) && not gHasSkip
                then (applyWithSkip gIElemRec skipVOpt gArg, oAppAs ++ [pHasSkip])
                else (applyWithElemRec gIElemRec gArg, oAppAs)

            nLambda = simplifyLambda $ composeUnaryPair ngArg fArg
        in
        return $ EC.applyMany (EC.project gId cE @<- gAs) [nLambda]
                   @<- updateElemRec iAppAs noAppAs

      | fId == "filter" && gId == "filter" =
        let ngArg    = applyWithSkip gIElemRec (Just $ Left $ EC.constant $ CBool False) gArg
            ngArgRec = applyWithElemRec gIElemRec gArg

            fLambda id ngArg = simplifyLambda $ EC.lambda id $
              EC.binop OAnd (EC.applyMany fArg  [EC.variable id])
                            (EC.applyMany ngArg [EC.variable id])

            (nLambda, noAppAs) = if (gStream || gUnstream) && not gHasSkip
                then (fLambda "xOpt" ngArg,    oAppAs ++ [pHasSkip])
                else (fLambda "x"    ngArgRec, oAppAs)
        in
        return $ EC.applyMany (EC.project gId cE @<- gAs) [nLambda]
                   @<- updateElemRec iAppAs noAppAs

      | fId == "filter" && gId == "iterate" =
        let ngArg    = applyWithSkip gIElemRec (Just $ Left EC.unit) gArg
            ngArgRec = applyWithElemRec gIElemRec gArg

            iLambda id ngArg = simplifyLambda $
              EC.lambda id $ EC.ifThenElse (EC.applyMany fArg [EC.variable id])
                                 (EC.applyMany ngArg [EC.variable id])
                                 EC.unit

            (nLambda, noAppAs) = if (gStream || gUnstream) && not gHasSkip
                then (iLambda "xOpt" ngArg,    oAppAs ++ [pHasSkip])
                else (iLambda "x"    ngArgRec, oAppAs)
        in
        return $ EC.applyMany (EC.project gId cE @<- gAs) [nLambda]
                   @<- updateElemRec iAppAs noAppAs

      where updateElemRec ias as =
              if any isEIElemRec ias then nub $ as ++ [pIElemRec]
                                     else filter (not . isEIElemRec) as

            simplifyLambda e = runIdentity $ betaReduction e

    rewriteUnaryPair _ _ _ _ _ = Left $ "Invalid unary-unary rewrite"

    rewriteUnaryBinary (PChainPrjApp2Pair cE fId gId@("fold") fArg gArg1 gArg2
                                          _ iAppAs gAs oApp1As oApp2As)
                       fStream gIElemRec
      | fId == "map" =
          let ngArg1 = if fStream then applyAccumWithSkip gIElemRec gArg1
                                  else applyAccumWithElemRec gIElemRec gArg1
              nLambda = simplifyLambda $ composeUnaryBinary ngArg1 fArg
          in return $
               EC.applyMany
                 (EC.applyMany (EC.project gId cE @<- gAs) [nLambda]
                    @<- updateElemRec iAppAs oApp1As)
                 [gArg2] @<- oApp2As

      | fId == "filter" =
          let id2     = if fStream then "ySkip" else "y"
              narg2   = if gIElemRec then EC.record [("elem", EC.variable "y")]
                                     else EC.variable "y"
              onFValE = if fStream
                          then EC.caseOf (EC.variable id2) "y"
                                  (EC.applyMany gArg1 [EC.variable "x", narg2])
                                  (EC.variable "x")
                          else EC.applyMany gArg1 [EC.variable "x", narg2]

              nLambda = simplifyLambda $ EC.lambda "x" $ EC.lambda id2 $
                          EC.ifThenElse (EC.applyMany fArg [EC.variable id2])
                                        onFValE (EC.variable "x")

          in return $ EC.applyMany
                        (EC.applyMany (EC.project gId cE @<- gAs) [nLambda]
                           @<- updateElemRec iAppAs oApp1As)
                        [gArg2] @<- oApp2As

      where updateElemRec ias as =
              if any isEIElemRec ias then nub $ as ++ [pIElemRec]
                                     else filter (not . isEIElemRec) as

            simplifyLambda e = runIdentity $ betaReduction e

    rewriteUnaryBinary _ _ _ = Left $ "Invalid unary-binary rewrite"

    rewriteUnaryTernary (PChainPrjApp3Pair cE _ gId@("groupBy") fArg gArg1 gArg2 gArg3
                                             _ _ gAs oApp1As oApp2As oApp3As)
                        g1IElemRec g2IElemRec =
      let ngArg1 = applyWithElemRec g1IElemRec gArg1
          ngArg2 = applyWithElemRec g2IElemRec gArg2
      in
      return $
        EC.applyMany
          (EC.applyMany
            (EC.applyMany (EC.project gId cE @<- gAs) [composeUnaryPair ngArg1 fArg] @<- oApp1As)
            [composeUnaryBinary ngArg2 fArg] @<- oApp2As)
            [gArg3] @<- oApp3As

    rewriteUnaryTernary _ _ _ = Left $ "Invalid unary-ternary rewrite"

    -- TODO: simplify and inline function bodies where possible.
    composeUnaryPair g f =
      EC.lambda "x" $ EC.applyMany g [EC.applyMany f [EC.variable "x"]]

    composeUnaryBinary gBinary fUnary =
      EC.lambda "x" $ EC.lambda "y" $
        EC.applyMany gBinary [EC.variable "x", EC.applyMany fUnary [EC.variable "y"]]

    applyWithSkip iElemRec skipVOpt lamE =
      let skipVarE = EC.variable "x"
          appLamE  = EC.applyMany lamE [if iElemRec then EC.record [("elem", skipVarE)] else skipVarE]
          caseBranches = case skipVOpt of
                           Nothing              -> (EC.some appLamE, EC.constant $ CNone NoneImmut)
                           Just (Right onSkipE) -> (EC.some appLamE, onSkipE)
                           Just (Left  onSkipE) -> (appLamE, onSkipE)
      in EC.lambda "xSkip" $ uncurry (EC.caseOf (EC.variable "xSkip") "x") caseBranches

    applyAccumWithSkip iElemRec lamE =
      let caseBranches = (EC.applyMany lamE
                            [EC.variable "x",
                             if iElemRec then EC.record [("elem", EC.variable "y")]
                                         else EC.variable "y"]
                         , EC.variable "x")
      in binaryLambda "x" "ySkip" $
           uncurry (EC.caseOf (EC.variable "ySkip") "y") caseBranches

    applyWithElemRec True  lamE = EC.lambda "xToWrap" $ EC.applyMany lamE [elemVar "xToWrap"]
    applyWithElemRec False lamE = lamE

    applyAccumWithElemRec True lamE =
      binaryLambda "x" "yToWrap" $
        EC.applyMany lamE [EC.variable "x", elemVar "yToWrap"]

    applyAccumWithElemRec False lamE = lamE

    elemVar id = EC.record [("elem", EC.variable id)]
    binaryLambda id1 id2 e = EC.lambda id1 $ EC.lambda id2 e

    literalTypeAsTElement (Just (tag -> LString ctStr)) =
      let t = (read ctStr) :: (K3 Type) in
      case tag t of
        TCollection -> Just $ head $ children t
        _ -> Nothing

    literalTypeAsTElement _ = Nothing

    rebuildE (Node n _) ch = return $ Node n ch

    logRewrite msg nch newEEither = flip localLogAction newEEither $ \case
      Nothing -> Nothing
      Just e  -> Just $ boxToString $ ["Rewrite " ++ msg] %+ prettyLines (uncurry replaceCh nch)
                                                 %+ [" "] %+ prettyLines e

-- | Infer return points in expressions that are collection insertions to the given variable.
mapAccumulation :: (K3 Expression -> K3 Expression)
                -> (K3 Expression -> K3 Expression)
                -> Identifier -> K3 Expression -> (Bool, K3 Expression)
mapAccumulation onAccumF onRetVarF i expr = runIdentity $ do
  (isAcc, e) <- doInference
  return $ (either id id isAcc, e)

  where
    doInference =
      foldMapReturnExpression trackBindings returnAsAccumulator independentF ([], []) (Left False) expr

    -- TODO: check effects and lineage rather than free variables.
    independentF (ignores, prts) _ e
      | EVariable j <- tag e , i == j && i `notElem` ignores = return (Right False, e)
      | EAssign   j <- tag e , i == j && i `notElem` ignores = return (Right False, e)

    independentF _ (onIndepR -> isAccum) e = return (isAccum, e)

    -- TODO: no need to track all bnds, this can be a boolean indicating only
    -- if our target variable 'i' is shadowed.
    trackBindings (bnds, prts) e@(InsertAndReturn j "insert" v)
      | i == j && i `notElem` bnds && notAccessedIn v
         = return ((bnds, prts), [(bnds, i:prts), (bnds, i:prts)])

    trackBindings (bnds, prts) e = case tag e of
      ELambda j -> return ((bnds, prts), [(j:bnds, prts)])
      ELetIn  j -> return ((bnds, prts), [(bnds, prts), (j:bnds, prts)])
      ECaseOf j -> return ((bnds, prts), [(bnds, prts), (j:bnds, prts), (bnds, prts)])
      EBindAs b -> return ((bnds, prts), [(bnds, prts), (bnds ++ bindingVariables b, prts)])
      _ -> return ((bnds, prts), replicate (length $ children e) (bnds, prts))

    -- TODO: every return expression must be one of:
    --   i.   accumulating expression
    --   ii.  alias (for now we consider only the exact variable, not equivalent bnds)
    --   iii. a branching expression composed only of accumulating expression and alias children.
    -- TODO: using symbols as lineage here will provide better alias tracking.

    -- TODO: test in-place modification property
    returnAsAccumulator (ignores, _) _ e@(InsertAndReturn j "insert" v)
      | i == j && i `notElem` ignores && notAccessedIn v = return (Right True, onAccumF e)

    returnAsAccumulator (ignores, protected) _ e@(tag -> EVariable j)
      | i == j && i `notElem` ignores =
        if i `elem` protected then return (Left True, e)
                              else return (Left True, onRetVarF e)

    returnAsAccumulator _ (onReturnBranch [0]   -> isAccum) e@(tag -> ELambda _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [0]   -> isAccum) e@(tag -> EOperate OApp) = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1]   -> isAccum) e@(tag -> EOperate OSeq) = return (isAccum, e)

    returnAsAccumulator _ (onReturnBranch [1]   -> isAccum) e@(tag -> ELetIn  _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1]   -> isAccum) e@(tag -> EBindAs _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1,2] -> isAccum) e@(tag -> ECaseOf _)     = return (isAccum, e)
    returnAsAccumulator _ (onReturnBranch [1,2] -> isAccum) e@(tag -> EIfThenElse)   = return (isAccum, e)

    returnAsAccumulator _ _ e = return (Right False, e)

    -- TODO: replace with testing effects/symbol.
    notAccessedIn e = i `notElem` (freeVariables e) && i `notElem` (modifiedVariables e)

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

rewriteAccumulation :: Identifier -> K3 Expression -> (Bool, K3 Expression)
rewriteAccumulation i expr = mapAccumulation rewriteAccumE rewriteVarE i expr
  where rewriteAccumE e@(InsertAndReturn _ "insert" v) = EC.some v
        rewriteVarE   _ = EC.constant $ CNone NoneImmut


-- Helper patterns for fusion
pattern PApp     fE argE  appAs = Node (EOperate OApp :@: appAs) [fE, argE]
pattern PLam     i  bodyE iAs   = Node (ELambda i :@: iAs)       [bodyE]
pattern PPrj     cE fId   fAs   = Node (EProject fId :@: fAs)    [cE]

pattern PPrjApp cE fId fAs fArg iAppAs = PApp (PPrj cE fId fAs) fArg iAppAs

pattern PPrjApp2 cE fId fAs fArg1 fArg2 app1As app2As
  = PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As

pattern PPrjApp3 cE fId fAs fArg1 fArg2 fArg3 app1As app2As app3As
  = PApp (PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As) fArg3 app3As

pattern PChainLambda1  i j bodyE iAs jAs = PLam i (PLam j bodyE jAs) iAs

pattern PChainPrjApp1 cE fId gId fArg gArg fAs iAppAs gAs oAppAs =
  PPrjApp (PPrjApp cE fId fAs fArg iAppAs) gId gAs gArg oAppAs

pattern PChainPrjApp1Pair cE fId gId fArg gArg fAs iAppAs gAs oAppAs <-
  (PApp _ _ oAppAs, [PPrj (PPrjApp cE fId fAs fArg iAppAs) gId gAs, gArg])

pattern PChainPrjApp2 cE fId gId fArg gArg1 gArg2 fAs iAppAs gAs oApp1As oApp2As =
  PApp (PChainPrjApp1 cE fId gId fArg gArg1 fAs iAppAs gAs oApp1As) gArg2 oApp2As

pattern PChainPrjApp2Pair cE fId gId fArg gArg1 gArg2 fAs iAppAs gAs oApp1As oApp2As <-
  (PApp _ _ oApp2As
  , [PChainPrjApp1 cE fId gId fArg gArg1 fAs iAppAs gAs oApp1As, gArg2])

pattern PChainPrjApp3 cE fId gId fArg gArg1 gArg2 gArg3 fAs iAppAs gAs oApp1As oApp2As oApp3As =
  PApp (PChainPrjApp2 cE fId gId fArg gArg1 gArg2 fAs iAppAs gAs oApp1As oApp2As) gArg3 oApp3As

pattern PChainPrjApp3Pair cE fId gId fArg gArg1 gArg2 gArg3 fAs iAppAs gAs oApp1As oApp2As oApp3As <-
  (PApp _ _ oApp3As
  , [PChainPrjApp2 cE fId gId fArg gArg1 gArg2 fAs iAppAs gAs oApp1As oApp2As, gArg3])

pattern PBinChainPrjApp1 cE fId gId fArg1 fArg2 gArg fAs iApp1As iApp2As gAs oAppAs =
  PPrjApp (PPrjApp2 cE fId fAs fArg1 fArg2 iApp1As iApp2As) gId gAs gArg oAppAs

pattern PBinChainPrjApp2 cE fId gId fArg1 fArg2 gArg1 gArg2 fAs iApp1As iApp2As gAs oApp1As oApp2As =
  PApp (PBinChainPrjApp1 cE fId gId fArg1 fArg2 gArg1 fAs iApp1As iApp2As gAs oApp1As) gArg2 oApp2As

pattern PBinChainPrjApp3 cE fId gId fArg1 fArg2 gArg1 gArg2 gArg3 fAs iApp1As iApp2As gAs oApp1As oApp2As oApp3As =
  PApp (PBinChainPrjApp2 cE fId gId fArg1 fArg2 gArg1 gArg2 fAs iApp1As iApp2As gAs oApp1As oApp2As) gArg3 oApp3As

pattern PAnyStream1 fE argE appAs <-
  PApp fE argE (id &&& (\e -> any isEStreamable e || any isEStream e) -> (appAs, True))

pattern PAnyStream2 fE arg1E arg2E app1As app2As <-
  PApp (PAnyStream1 fE arg1E app1As) arg2E app2As

pattern PAnyStream3 fE arg1E arg2E arg3E app1As app2As app3As <-
  PApp (PAnyStream2 fE arg1E arg2E app1As app2As) arg3E app3As

pattern PUnstream1 fE argE appAs <-
  PApp fE argE (id &&& any isEUnstream -> (appAs, True))

pattern PUnstream2 fE arg1E arg2E app1As app2As <-
  PApp (PUnstream1 fE arg1E app1As) arg2E app2As

pattern PUnstream3 fE arg1E arg2E arg3E app1As app2As app3As <-
  PApp (PUnstream2 fE arg1E arg2E app1As app2As) arg3E app3As

pattern PStreamableTransformerArg i j iAs jAs <-
  PChainLambda1 i j (inferAccumulation i -> True) iAs jAs

pattern PStreamableTransformerPair cE fId bodyE i j arg2E iAs jAs fAs appAs <-
  (PApp _ _ (any isETAppChain -> True)
  , [PApp (PPrj cE fId fAs)
          (PChainLambda1 i j bodyE iAs jAs)
          (id &&& any isEStreamable -> (appAs, True))
    , arg2E])

pattern InsertAndReturn i cfId arg <-
    Node (EOperate OSeq :@: _)
      [Node (EOperate OApp :@: _)
        [Node (EProject cfId :@: _) [Node (EVariable i :@: _) []], arg]
      , Node (EVariable ((== i) -> True) :@: _) []]

