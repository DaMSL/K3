{-# LANGUAGE PatternGuards #-}
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

import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import qualified Language.K3.Core.Constructor.Expression as EC

import Language.K3.Analysis.Common
import Language.K3.Analysis.Effect
import Language.K3.Transform.Common
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Data.Types

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
    -- TODO: substitute when we have read-only mutable bindings.
    simplifyConstants ch n@(tag -> ELetIn i) =
      let immutSource = onQualifiedExpression (head $ children n) True False in
      case (head ch, last ch, immutSource) of
        (_, Left v2, _)             -> return $ Left v2
        (Left v, Right bodyE, True) -> substituteBinding i v bodyE >>= simplifyAsFoldedExpr
        (_, _, _)                   -> rebuildNode n ch

    -- TODO: substitute when we have read-only mutable bindings.    
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

    -- TODO: substitute when we have read-only mutable bindings.    
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

    isLeft (Left _) = True
    isLeft _        = False

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
--   This reduces lambda and let bindings that are used at most once in their bodies.
--
--   TODO:
--   More generally, we can use a cost model to determine this threshold based
--   on the cost of the argument and the cost of the increased lifetime of the
--   object given its encapsulation in a lambda.
--   Furthermore, this only applies to direct lambda invocations, rather than
--   on general function values (i.e., including applications through bindings
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
--   i. prune unused let-in bindings and narrow bind-as expressions with record binders.
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
eliminateDeadProgramCode prog = do
  aProg <- analyzeEffects prog 
  mapExpression eliminateDeadCode aProg

eliminateDeadCode :: K3 Expression -> Either String (K3 Expression)
eliminateDeadCode expr = mapTree pruneExpr expr
  where
    pruneExpr ch n@(tag -> ELetIn  i) =
      let vars = freeVariables $ last ch in 
      if maybe False (const $ i `notElem` vars) $ (head ch) @~ ePure
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
        case (head ch) @~ ePure of
          Nothing -> rebuildNode n ch
          Just _  -> return $ last ch

    pruneExpr ch n = rebuildNode n ch

    rebuildNode n@(tag -> EConstant _) _ = return n
    rebuildNode n@(tag -> EVariable _) _ = return n
    rebuildNode n ch = return $ Node (tag n :@: annotations n) ch

    ePure (EProperty "Pure" _) = True
    ePure _ = False


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
              bindings      = case tag t of 
                                ELambda i -> [[i]]
                                ELetIn  i -> [[], [i]]
                                ECaseOf j -> [[], [j], []]
                                EBindAs b -> [[], bindingVariables b]
                                _         -> repeat []
              filteredCands = nub $ concatMap filterOpenCandidates $ zip bindings subAcc
              localCands    = sortBy ((flip compare) `on` snd) $ 
                                foldl (addCandidateIfLCA subAcc) [] filteredCands
              candTreeNode  = Node (uid, localCands) $ concat ctCh
              nStrippedExpr = Node (tag t :@: (filter isEQualified $ annotations t)) $ concat sExprCh
          in
          case n @~ ePure of
            Nothing -> return $ ([candTreeNode], [nStrippedExpr], [])
            Just _  -> return $ ([candTreeNode], [nStrippedExpr], (concat subAcc)++[nStrippedExpr])

        _ -> uidError n

      where 
        filterOpenCandidates ([], cands) = cands
        filterOpenCandidates (bindings, cands) =
          filter (\e -> null $ freeVariables e `intersect` bindings) cands

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

    ePure (EProperty "Pure" _) = True
    ePure _ = False


-- | Collection transformer fusion.

-- | Marks all function applications that are fusable transformers as a
--   preprocessing for fusion optimizations.
inferFusableProgramApplies :: K3 Declaration -> Either String (K3 Declaration)
inferFusableProgramApplies prog = mapExpression inferFusableExprApplies prog

inferFusableExprApplies :: K3 Expression -> Either String (K3 Expression)
inferFusableExprApplies expr = modifyTree fusable expr
  where
    fusable e@(tag -> EOperate OApp) = 
      let pl = filter isPTransformer $ annotations $ head $ children e in
      return $ if null pl then e else e @+ fusableProp
    
    fusable e = return e

    isPTransformer :: Annotation Expression -> Bool
    isPTransformer (EProperty "Transformer" _) = True
    isPTransformer _ = False

    fusableProp :: Annotation Expression
    fusableProp = EProperty "Fusable" Nothing

fuseProgramTransformers :: K3 Declaration -> Either String (K3 Declaration)
fuseProgramTransformers prog = mapExpression fuseTransformers prog

fuseTransformers :: K3 Expression -> Either String (K3 Expression)
fuseTransformers expr = mapTree fuse expr
  where
    fuse ch n | Just (outer, inner) <- fusablePair n ch =
      trace (unwords ["fuse", show outer, "and", show inner]) $ 
      case (outer, inner) of 
        (EProject "map",     EProject "map") -> rewriteMapMap n ch
        (EProject "fold",    EProject "map") -> rebuildE n ch -- TODO
        (EProject "groupby", EProject "map") -> rebuildE n ch -- TODO
        _ -> rebuildE ch n

    fuse ch n = rebuildE ch n
    rebuildE ch (Node n _) = return $ Node n ch

    -- TODO: add generated spans and type annotations for all expressions built.
    rewriteMapMap :: K3 Expression -> [K3 Expression] -> Either String (K3 Expression)
    rewriteMapMap n ch = case extractFusablePairArgs ch of
      Just (c, f, g) -> return $ EC.applyMany (EC.project "map" c) [composeFunctions f g]
      Nothing -> rebuildE ch n

    -- TODO: simplify and inline function bodies where possible.
    -- TODO: add generated spans and type annotations for all expressions built.
    composeFunctions :: K3 Expression -> K3 Expression -> K3 Expression
    composeFunctions f g = EC.lambda "x" $ EC.applyMany f [EC.record [("elem", EC.applyMany g [EC.variable "x"])]]

    fusablePair :: K3 Expression -> [K3 Expression] -> Maybe (Expression, Expression)
    fusablePair n [ch1@(tag &&& children -> (EProject _, [gch])), _]
      | isFusable n && isFusable gch =
        let ggch = children gch in
        if null ggch then Nothing else Just (tag ch1, tag $ head ggch)
      | otherwise = Nothing

    fusablePair _ _ = Nothing

    extractFusablePairArgs :: [K3 Expression] -> Maybe (K3 Expression, K3 Expression, K3 Expression)
    extractFusablePairArgs [(children -> [gchApp]), arg1] = 
      let ggch = children gchApp 
          (target, arg2) = fusableTarget gchApp
      in if null ggch then Nothing else Just (target, arg1, arg2)
    
    extractFusablePairArgs _ = Nothing

    fusableTarget :: K3 Expression -> (K3 Expression, K3 Expression)
    fusableTarget e = let ch = children e in (head $ children $ head ch, last ch)

    isFusable :: K3 Expression -> Bool
    isFusable e@(tag -> EOperate OApp) = isJust $ find isEFusable $ annotations e
    isFusable _ = False

    isEFusable :: Annotation Expression -> Bool
    isEFusable (EProperty "Fusable" _) = True
    isEFusable _ = False
