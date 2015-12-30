{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
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
import Data.IntMap ( IntMap )
import Data.Maybe
import Data.Tree
import Data.Word ( Word8 )
import qualified Data.Map as Map
import qualified Data.IntMap as IntMap

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

import Language.K3.Analysis.Core
import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Inference ( PIEnv )

import Language.K3.Analysis.SEffects.Core
import Language.K3.Analysis.SEffects.Inference                ( readOnly, noWrites, noWritesOn )

import qualified Language.K3.Analysis.Provenance.Constructors as PC
import qualified Language.K3.Analysis.SEffects.Constructors   as FC
import qualified Language.K3.Analysis.SEffects.Inference      as SE

import Language.K3.Transform.Common
import Language.K3.Interpreter.Data.Accessors hiding ( elemE )
import Language.K3.Interpreter.Data.Types

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

import Data.Text ( Text )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

traceLogging :: Bool
traceLogging = False

localLog :: (Functor m, Monad m) => String -> m ()
localLog = logVoid traceLogging

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
localLogAction = logAction traceLogging

-- | Property construction helper
persistentEProp :: Identifier -> Maybe (K3 Literal) -> Annotation Expression
persistentEProp n lopt = EProperty $ Left (n,lopt)

inferredEProp :: Identifier -> Maybe (K3 Literal) -> Annotation Expression
inferredEProp n lopt = EProperty $ Right (n,lopt)

-- | Property testers and constructors
pTransformer :: Annotation Expression
pTransformer = inferredEProp "Transformer" Nothing

pPureTransformer :: Annotation Expression
pPureTransformer = inferredEProp "PureTransformer" Nothing

pImpureTransformer :: Annotation Expression
pImpureTransformer = inferredEProp "ImpureTransformer" Nothing

pFusable :: Annotation Expression
pFusable = inferredEProp "Fusable" Nothing

pStreamable :: Annotation Expression
pStreamable = inferredEProp "Streamable" Nothing

pStream :: Annotation Expression
pStream = inferredEProp "Stream" Nothing

pUnstream :: K3 Type -> Annotation Expression
pUnstream t = inferredEProp "Unstream" $ Just $ LC.string $ show t

pHasSkip :: Annotation Expression
pHasSkip = inferredEProp "HasSkip" Nothing

pTAppChain :: Annotation Expression
pTAppChain = inferredEProp "TAppChain" Nothing

pIElemRec :: Annotation Expression
pIElemRec = inferredEProp "IElemRec" Nothing

pOElemRec :: Annotation Expression
pOElemRec = inferredEProp "OElemRec" Nothing

-- | Fold fusion accumulator function classification
data FusionAccFClass = UCondVal -- Direct return of accumulator value (e.g., iterate)
                     | UCond    -- Unconditional accumulation
                     | ICond1   -- Accumulation in one accumulator-independent condition branch
                     | ICondN   -- Accumulation in many accumulator-independent condition branches
                     | DCond2   -- Accumulation in two accumulator-dependent condition branches
                     | Halt     -- An explicit class to prevent further fusion.
                     | Open     -- General accumulation function with unknown structure
                     deriving (Enum, Eq, Ord, Read, Show)

-- | Fold fusion accumulator transform classification
data FusionAccTClass = IdTr     -- Identity transform
                     | IndepTr  -- Accumulator independent transform (although possible element dependent)
                     | DepTr    -- Accumulator dependent transform
                     deriving (Enum, Eq, Ord, Read, Show)

type FusionAccSpec = (FusionAccFClass, FusionAccTClass)

pFusionSpec :: FusionAccSpec -> Annotation Expression
pFusionSpec spec = persistentEProp "FusionSpec" $ Just . LC.string $ show spec

pFusionLineage :: String -> Annotation Expression
pFusionLineage s = persistentEProp "FusionLineage" $ Just $ LC.string s

pHierarchicalGroupBy :: Annotation Expression
pHierarchicalGroupBy = inferredEProp "HGroupBy" Nothing

pMonoidGroupBy :: Annotation Expression
pMonoidGroupBy = inferredEProp "MGroupBy" Nothing

pAccumulatingTransformer :: Annotation Expression
pAccumulatingTransformer = persistentEProp "AccumulatingTransformer" Nothing

isEPure :: Annotation Expression -> Bool
isEPure (EProperty (ePropertyName -> "Pure")) = True
isEPure _ = False

isETransformer :: Annotation Expression -> Bool
isETransformer (EProperty (ePropertyName -> "Transformer")) = True
isETransformer _ = False

isEPureTransformer :: Annotation Expression -> Bool
isEPureTransformer (EProperty (ePropertyName -> "PureTransformer")) = True
isEPureTransformer _ = False

isEImpureTransformer :: Annotation Expression -> Bool
isEImpureTransformer (EProperty (ePropertyName -> "ImpureTransformer")) = True
isEImpureTransformer _ = False

isEFusable :: Annotation Expression -> Bool
isEFusable (EProperty (ePropertyName -> "Fusable")) = True
isEFusable _ = False

isEStreamable :: Annotation Expression -> Bool
isEStreamable (EProperty (ePropertyName -> "Streamable")) = True
isEStreamable _ = False

isEStream :: Annotation Expression -> Bool
isEStream (EProperty (ePropertyName -> "Stream")) = True
isEStream _ = False

isEUnstream :: Annotation Expression -> Bool
isEUnstream (EProperty (ePropertyName -> "Unstream")) = True
isEUnstream _ = False

isEHasSkip :: Annotation Expression -> Bool
isEHasSkip (EProperty (ePropertyName -> "HasSkip")) = True
isEHasSkip _ = False

isETAppChain :: Annotation Expression -> Bool
isETAppChain (EProperty (ePropertyName -> "TAppChain")) = True
isETAppChain _ = False

isEIElemRec :: Annotation Expression -> Bool
isEIElemRec (EProperty (ePropertyName -> "IElemRec")) = True
isEIElemRec _ = False

isEOElemRec :: Annotation Expression -> Bool
isEOElemRec (EProperty (ePropertyName -> "OElemRec")) = True
isEOElemRec _ = False

isEFusionSpec :: Annotation Expression -> Bool
isEFusionSpec (EProperty (ePropertyV -> ("FusionSpec", Just _))) = True
isEFusionSpec _ = False

getFusionSpec :: Annotation Expression -> Maybe FusionAccSpec
getFusionSpec (EProperty (ePropertyV -> ("FusionSpec", Just (tag -> LString s)))) = Just $ read s
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
isEFusionLineage (EProperty (ePropertyV -> ("FusionLineage", Just _))) = True
isEFusionLineage _ = False

getFusionLineage :: Annotation Expression -> Maybe String
getFusionLineage (EProperty (ePropertyV -> ("FusionLineage", Just (tag -> LString s)))) = Just s
getFusionLineage _ = Nothing

getFusionLineageA :: [Annotation Expression] -> Maybe String
getFusionLineageA anns = case find isEFusionLineage anns of
  Just ann -> getFusionLineage ann
  _ -> Nothing

getFusionLineageE :: K3 Expression -> Maybe String
getFusionLineageE e = case e @~ isEFusionLineage of
  Just ann -> getFusionLineage ann
  _ -> Nothing

isEHierarchicalGroupBy :: Annotation Expression -> Bool
isEHierarchicalGroupBy (EProperty (ePropertyV -> ("HGroupBy", Nothing))) = True
isEHierarchicalGroupBy _ = False

isEMonoidGroupBy :: Annotation Expression -> Bool
isEMonoidGroupBy (EProperty (ePropertyV -> ("MGroupBy", Nothing))) = True
isEMonoidGroupBy _ = False

isENoBetaReduce :: Annotation Expression -> Bool
isENoBetaReduce (EProperty (ePropertyName -> "NoBetaReduce")) = True
isENoBetaReduce _ = False

isEReturnsArgLambda :: Annotation Expression -> Bool
isEReturnsArgLambda (EProperty (ePropertyName -> "ReturnsArgument")) = True
isEReturnsArgLambda _ = False

isEThreadsArgLambda :: Annotation Expression -> Bool
isEThreadsArgLambda (EProperty (ePropertyName -> "ThreadsArgument")) = True
isEThreadsArgLambda _ = False


-- | Constant folding
type FoldedExpr = Either String (Either (Value, Tree UID) (K3 Expression))

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
    simplifyAsFoldedExpr e = foldMapTree simplifyWithRule (Left (VTuple [], leafUID $ UID (-1))) e

    simplifyConstants :: [Either (Value, Tree UID) (K3 Expression)] -> K3 Expression -> FoldedExpr
    simplifyConstants _ n@(tag -> EVariable _) = return $ Right n
    simplifyConstants _ n@(tag &&& annotations -> (EConstant c, anns)) = constant c (uidOf n) anns

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
      applyVCtor ch n (\a b -> asUnary (someCtor (extractQualifier $ head $ children n) a) b)

    simplifyConstants ch n@(tag -> ETuple) =
      applyVCtor ch n (tupleCtor $ map extractQualifier $ children n)

    simplifyConstants ch n@(tag -> ERecord ids) =
      applyVCtor ch n (recordCtor ids $ map extractQualifier $ children n)

    -- Binding simplification.
    -- TODO: substitute when we have read-only mutable binds.
    simplifyConstants ch n@(tag -> ELetIn i) =
      let immutSource = onQualifiedExpression (head $ children n) True False in
      case (head ch, last ch, immutSource) of
        (_, Left v2, _) -> either (const $ return True) (readOnly False) (head ch) >>= \initRO ->
                              if initRO then return (Left v2) else rebuildNode n ch

        (Left v, Right bodyE, True) -> let numOccurs = length $ filter (== i) $ freeVariables bodyE in
                                       if numOccurs == 1 then substituteBinding i v bodyE >>= simplifyAsFoldedExpr
                                                         else rebuildNode n ch
        (_, _, _)                   -> rebuildNode n ch

    -- TODO: substitute when we have read-only mutable binds.
    simplifyConstants ch n@(tag -> EBindAs b) =
      case (b, head ch, last ch) of
        (_, _, Left v) -> do
          initRO <- either (const $ return True) (readOnly False) $ head ch
          if initRO then return (Left v) else rebuildNode n ch

        (BTuple ids,  Left (VTuple vqs, Node _ cu), Right bodyE) ->
          (foldM substituteQualifiedField bodyE $ zip3 ids vqs cu) >>= simplifyAsFoldedExpr

        (BRecord ijs, Left (VRecord nvqs, Node _ cu), Right bodyE) -> do
          subVQs <- mapM (\((s,t),u) -> maybe (invalidRecordBinding s) (return . (t,,u)) $ Map.lookup s nvqs) $ zip ijs cu
          foldM substituteQualifiedField bodyE subVQs >>= simplifyAsFoldedExpr

        (_, _, _) -> rebuildNode n ch

      where invalidRecordBinding s =
              Left $ "Invalid bind-as mapping source during simplification: " ++ s

    -- Branch simplification.
    simplifyConstants ch n@(tag -> EIfThenElse) =
      case head ch of
        Left (VBool True, _)  -> return (ch !! 1)
        Left (VBool False, _) -> return $ last ch
        Right _ -> rebuildNode n ch
        _ -> Left "Invalid if-then-else predicate simplification"

    -- TODO: substitute when we have read-only mutable bnds.
    simplifyConstants ch n@(tag -> ECaseOf i) = do
      initRO <- either (const $ return True) (readOnly False) $ head ch
      case head ch of
        Left (VOption (Just v, MemImmut), Node _ [cu]) | initRO ->
          (case ch !! 1 of
            Left v2     -> return $ Left v2
            Right someE -> substituteBinding i (v,cu) someE >>= simplifyAsFoldedExpr)

        Left (VOption (Nothing,  _), _) | initRO -> return $ last ch
        Right _ -> rebuildNode n ch
        _ -> Left "Invalid case-of source simplification"

    -- Projection simplification on a constant record.
    -- Since we do not simplify collections, VCollections cannot appear
    -- as the source of the projection expression.
    simplifyConstants ch n@(tag -> EProject _) = rebuildNode n ch
    {-
    simplifyConstants ch n@(tag -> EProject i) =
        case head ch of
          Left (VRecord nvqs, _) -> maybe fieldError (return . Left . fst) $ Map.lookup i nvqs
          Right _ -> rebuildNode n ch
          _ -> Left "Invalid projection source simplification"

      where fieldError = Left $ "Unknown record field in project simplification: " ++ i
    -}

    -- The default case is to rebuild the current node as an expression.
    -- This handles: lambda, assignment, addresses, and self expressions
    simplifyConstants ch n = rebuildNode n ch

    rebuildValue :: [Annotation Expression] -> (Value, Tree UID) -> Either String (K3 Expression)
    rebuildValue anns (v, ut) = valueAsExpression ut v >>= (\(e,_,_) -> return $ foldl (@+) e anns)

    rebuildNode :: K3 Expression -> [Either (Value, Tree UID) (K3 Expression)] -> FoldedExpr
    rebuildNode n ch = do
        let chAnns = map annotations $ children n
        nch <- mapM (\(vOrE, anns) -> either (rebuildValue anns) return vOrE) $ zip ch chAnns
        return . Right $ Node (tag n :@: annotations n) nch

    withValueChildren :: K3 Expression -> [Either (Value, Tree UID) (K3 Expression)]
                      -> (UID -> [(Value, Tree UID)] -> FoldedExpr) -> FoldedExpr
    withValueChildren n ch f = if all isLeft ch then f (uidOf n) (lefts ch) else rebuildNode n ch

    substituteQualifiedField :: K3 Expression -> (Identifier, (Value, VQualifier), Tree UID) -> Either String (K3 Expression)
    substituteQualifiedField targetE (n, (v, MemImmut), ut) = substituteBinding n (v, ut) targetE
    substituteQualifiedField targetE (_, (_, MemMut), _)    = return targetE

    substituteBinding :: Identifier -> (Value, Tree UID) -> K3 Expression -> Either String (K3 Expression)
    substituteBinding i (iV, ut) targetE = do
      (iE,_,_) <- valueAsExpression ut iV
      return $ substituteImmutBinding i iE targetE

    someCtor        q u v = return . Left . (, Node u [snd v]) $ VOption (Just $ fst v, q)
    tupleCtor      qs u l = return . Left . (, Node u $ map snd l) $ VTuple  $ zip (map fst l) qs
    recordCtor ids qs u l = return . Left . (, Node u $ map snd l) $ VRecord $ Map.fromList $ zip ids $ zip (map fst l) qs

    applyNum          ch n op   = withValueChildren n ch $ (const $ asBinary op)
    applyCmp          ch n op   = withValueChildren n ch $ (const $ asBinary $ comparison op)
    applyBool         ch n op   = withValueChildren n ch $ (const $ asBinary $ logic op)
    applyOp           ch n op   = withValueChildren n ch $ (const $ op)
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

    valueAsExpression :: Tree UID -> Value -> Either String (K3 Expression, K3 Provenance, K3 Effect)
    valueAsExpression (Node u _) (VBool   v) = leafVE u $ EC.constant $ CBool   v
    valueAsExpression (Node u _) (VByte   v) = leafVE u $ EC.constant $ CByte   v
    valueAsExpression (Node u _) (VInt    v) = leafVE u $ EC.constant $ CInt    v
    valueAsExpression (Node u _) (VReal   v) = leafVE u $ EC.constant $ CReal   v
    valueAsExpression (Node u _) (VString v) = leafVE u $ EC.constant $ CString v

    valueAsExpression (Node u _) (VOption (Nothing, vq)) =
      leafVE u $ EC.constant $ CNone $ noneQualifier vq

    valueAsExpression (Node u [cu]) (VOption (Just v, vq)) = do
      (ce, cp, cf) <- valueAsExpression cu v
      let p = PC.pdata Nothing [cp]
      let f = FC.fdata Nothing [cf]
      let e = foldl (@+) (EC.some ce) [eQualifier vq, EUID u, EProvenance p, ESEffect FC.fnone, EFStructure f]
      Right (e, p, f)

    valueAsExpression (Node u cu) (VTuple vqs) = do
      (cl, cpl, cfl) <- mapM (\(a,(b,_)) -> valueAsExpression a b) (zip cu vqs) >>= return . unzip3
      let p = PC.pdata Nothing cpl
      let f = FC.fdata Nothing cfl
      let e = foldl (@+) (EC.tuple $ map (uncurry (@+)) $ zip cl $ map (eQualifier . snd) vqs)
                         [EUID u, EProvenance p, ESEffect FC.fnone, EFStructure f]
      Right (e, p, f)

    valueAsExpression (Node u cu) (VRecord nvqs) = do
      let (ids, vqs) = unzip $ Map.toList nvqs
      let (vs, qs)   = unzip vqs
      (cl, cpl, cfl) <- mapM (uncurry valueAsExpression) (zip cu vs) >>= return . unzip3
      let p = PC.pdata (Just ids) cpl
      let f = FC.fdata (Just ids) cfl
      let e = foldl (@+) (EC.record $ zip ids $ map (uncurry (@+)) $ zip cl $ map eQualifier qs)
                         [EUID u, EProvenance p, ESEffect FC.fnone, EFStructure f]
      Right (e, p, f)

    valueAsExpression _ v = Left $ "Unable to reinject value during simplification: " ++ show v

    leafVE u e = Right . (, PC.ptemp, FC.fnone) $ foldl (@+) e $ constantAnns u
    constantAnns u = [EUID u, EProvenance PC.ptemp, ESEffect FC.fnone, EFStructure FC.fnone]

    {- Constant expression evaluation.
       These are similar to the interpreter's evaluation functions, except
       that they are pure, and do not operate in a stateful interpretation monad.
     -}

    -- | Evaluate a constant. This is similar to the intepreter's evaluation except
    --   that we pass on collections, yielding an expression.
    constant :: Constant -> UID -> [Annotation Expression] -> FoldedExpr
    constant   (CBool b)   u    _ = leafCE u $ VBool b
    constant   (CByte w)   u    _ = leafCE u $ VByte w
    constant   (CInt i)    u    _ = leafCE u $ VInt i
    constant   (CReal r)   u    _ = leafCE u $ VReal r
    constant   (CString s) u    _ = leafCE u $ VString s
    constant   (CNone _)   u anns = leafCE u $ VOption (Nothing, vQualOfAnnsE anns)
    constant c@(CEmpty _)  _ anns = return . Right $ Node (EConstant c :@: anns) []

    numericOp :: NumericFunction ((Value, Tree UID) -> (Value, Tree UID) -> FoldedExpr)
    numericOp byteOpF intOpF realOpF a b =
      case (fst a, fst b) of
        (VByte x, VByte y) -> return . Left . (, snd a) $ VByte $ byteOpF x y
        (VByte x, VInt  y) -> return . Left . (, snd a) $ VInt  $ intOpF (fromIntegral x) y
        (VByte x, VReal y) -> return . Left . (, snd a) $ VReal $ realOpF (fromIntegral x) y
        (VInt  x, VByte y) -> return . Left . (, snd a) $ VInt  $ intOpF x (fromIntegral y)
        (VInt  x, VInt  y) -> return . Left . (, snd a) $ VInt  $ intOpF x y
        (VInt  x, VReal y) -> return . Left . (, snd a) $ VReal $ realOpF (fromIntegral x) y
        (VReal x, VByte y) -> return . Left . (, snd a) $ VReal $ realOpF x (fromIntegral y)
        (VReal x, VInt  y) -> return . Left . (, snd a) $ VReal $ realOpF x (fromIntegral y)
        (VReal x, VReal y) -> return . Left . (, snd a) $ VReal $ realOpF x y
        _                  -> Left "Invalid numeric binary operands"

    -- | Similar to numericOp above, except disallow a zero value for the second argument.
    numericExceptZero :: NumericFunction ((Value, Tree UID) -> (Value, Tree UID) -> FoldedExpr)
    numericExceptZero byteOpF intOpF realOpF a b =
      case fst b of
        VByte 0 -> Left "Zero denominator in numeric operation"
        VInt  0 -> Left "Zero denominator in numeric operation"
        VReal 0 -> Left "Zero denominator in numeric operation"
        _       -> numericOp byteOpF intOpF realOpF a b

    flipOp :: (Value, Tree UID) -> FoldedExpr
    flipOp (VBool b, u) = return . Left . (, u) . VBool $ not b
    flipOp (VInt  i, u) = return . Left . (, u) . VInt  $ negate i
    flipOp (VReal r, u) = return . Left . (, u) . VReal $ negate r
    flipOp _       = Left "Invalid negation/not operation"

    comparison :: ComparisonFunction ((Value, Tree UID) -> (Value, Tree UID) -> FoldedExpr)
    comparison cmp a b = return . Left . (, snd a) . VBool . cmp $ compare (fst a) (fst b)

    logic :: LogicFunction ((Value, Tree UID) -> (Value, Tree UID) -> FoldedExpr)
    logic op a b =
      case (fst a, fst b) of
        (VBool x, VBool y) -> return . Left . (, snd a) . VBool $ op x y
        _ -> Left $ "Invalid boolean logic operands"

    stringOp :: (String -> String -> String) -> (Value, Tree UID) -> (Value, Tree UID) -> FoldedExpr
    stringOp op a b =
      case (fst a, fst b) of
        (VString s, VString t) -> return . Left . (, snd a) . VString $ op s t
        _ -> Left $ "Invalid string operands"

    leafCE u c = return . Left . (, leafUID u) $ c

    uidOf e   = maybe uidErr (\case {(EUID u) -> u ; _ -> uidErr}) $ e @~ isEUID
    uidErr    = error "Invalid UID in CF"
    leafUID u = Node u []

    -- Constant folding logging.
    simplifyWithRule :: [Either (Value, Tree UID) (K3 Expression)] -> K3 Expression -> FoldedExpr
    simplifyWithRule ch e = do
      veE <- simplifyConstants ch e
      localLog $ showCFRule (show $ tag e) ch veE e
      return veE

    showCFRule rtag ch vOrE e =
      boxToString $ (rpsep %+ premise) %$ separator %$ (rpsep %+ conclusion)
      where rprefix        = unwords [rtag, maybe "<no uid>" (\(EUID euid) -> show euid) $ e @~ isEUID]
            (rplen, rpsep) = (length rprefix, [replicate rplen ' '])
            premise        = if null ch then ["<empty>"] else (intersperseBoxes [" , "] $ map prettyVE ch)
            premLens       = map length premise
            headWidth      = if null premLens then 4 else maximum premLens
            separator      = [rprefix ++ replicate headWidth '-']
            conclusion     = prettyVE vOrE
            prettyVE x     = either (prettyLines . fst) prettyLines x




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
betaReductionOnProgram :: PIEnv -> K3 Declaration -> Either String (K3 Declaration)
betaReductionOnProgram env prog = mapExpression (betaReduction env) prog

-- Effect-aware beta reduction of an expression.
-- We strip UIDs, spans, types and effects present on the substitution.
-- Thus, we cannot recursively invoke the simplification, and must iterate
-- to a fixpoint externally.
betaReduction :: PIEnv -> K3 Expression -> Either String (K3 Expression)
betaReduction env expr = betaReductionDelta env expr >>= return . fst

-- Beta reduction with a boolean indicating if any substitutions were performed.
betaReductionDelta :: PIEnv -> K3 Expression -> Either String (K3 Expression, Bool)
betaReductionDelta env expr = foldMapTree reduce ([], False) expr >>= return . first head
  where
    reduce (onSub -> (ch, True)) n = return $ ([replaceCh n ch], True)
    reduce (onSub -> (ch, False)) n =
      let n' = replaceCh n ch in
      case (skipBetaReduce n', n') of
        (False, tag -> ELetIn i) -> reduceOnOccurrences n' i (head ch) (last ch) (n' @~ isEUID)
        (False, PAppLam i bodyE argE _ _) -> reduceOnOccurrences n' i argE bodyE (n' @~ isEUID)
        (_, _) -> return ([n'], False)

    reduce _ _ = Left "Invalid betaReductionDelta.reduce pattern match"

    -- We perform beta reduction if:
    -- i. the substituted expression is read only. This ensures multiple substitutions do
    --    not cause duplicated write effects.
    -- ii. the target expression into which we are substituting does not perform any writes
    --     on the identifier.
    reduceOnOccurrences n i ie e (Just (EUID u)) = do
      ieRO <- readOnly False ie
      eRO  <- noWritesOn True env i u e
      let numOccurs = length $ filter (== i) $ freeVariables e
      (simple, doReduce) <- reducibleTarget numOccurs i ie e
      if debugCondition ieRO eRO n $ (simple || (ieRO && eRO)) && doReduce
        then return ([substituteImmutBinding i (cleanExpr ie) e], True)
        else return ([n], False)

    reduceOnOccurrences _ _ _ _ _ = Left "Invalid UID for reduceOnOccurrences"

    reducibleTarget numOccurs i ie e =
      case (tag ie, ie @~ isEType, ie @~ isEQualified) of
        (_, Nothing, _)       -> Left "No type found on target during beta reduction"
        (_, _, Just EMutable) -> Right (False, False)
        (EVariable _, _, _)   -> Right (True, fullySubstitutable i ie e)

        -- Collections can be modified in place with insert/update/delete,
        -- thus we can only substitute single uses.
        -- TODO: we can actually substitute provided the collection is not
        -- written in the body. Use effects to determine this.
        (_, Just (EType (tag -> TCollection)), _) -> Right (False, numOccurs <= 1 && fullySubstitutable i ie e)

        {- EConstant case must be handled after collections to address empty collections. -}
        (EConstant _, _, _)   -> Right (True, True)

        _ -> Right (False, numOccurs == 1 && fullySubstitutable i ie e)

    onSub ch = (concat *** any id) $ unzip ch

    skipBetaReduce n = any isENoBetaReduce $ filter isEUserProperty $ annotations n

    cleanExpr e = stripExprAnnotations cleanAnns (const False) e
    cleanAnns a = isEUID a || isESpan a || isAnyETypeOrEffectAnn a

    debugCondition ieRO eRO n' r = if True then r else
      flip trace r $ (boxToString $ [unwords ["BR-COND", show ieRO, show eRO]] %$ prettyLines n')

    {-
    debugROC sube deste r@(re,_) = if True then return r else
      flip trace (return r) (boxToString $ ["BR-ROC"] %$ prettyLines sube
                                                      %$ prettyLines deste
                                                      %$ prettyLines (head re))
    -}

-- Beta reduction of variable applications, i.e., (\x -> ...) var
simpleBetaReduction :: K3 Expression -> K3 Expression
simpleBetaReduction expr = let r = runIdentity $ modifyTree reduce expr
                           in if compareEAST r expr then r else simpleBetaReduction r
  where
    reduce (PAppLam i bodyE argE@(tag -> EVariable _) _ _) = return $ substituteImmutBinding i (cleanExpr argE) bodyE
    reduce n = return n

    cleanExpr e = stripExprAnnotations cleanAnns (const False) e
    cleanAnns a = isEUID a || isESpan a || isAnyETypeOrEffectAnn a


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
eliminateDeadProgramCode prog = mapExpression eliminateDeadCode prog

eliminateDeadCode :: K3 Expression -> Either String (K3 Expression)
eliminateDeadCode expr = eliminateDeadCodeDelta expr >>= return . fst

eliminateDeadCodeDelta :: K3 Expression -> Either String (K3 Expression, Bool)
eliminateDeadCodeDelta expr = foldMapTree pruneExpr ([],False) expr >>= return . first head
  where
    pruneExpr (onSub -> (ch, True)) n = return $ ([replaceCh n ch], True)
    pruneExpr (onSub -> (ch, False)) n =
      let n' = replaceCh n ch in
      case n' of
        -- Immediate record construction and projection, provided all other record fields are pure.
        (PPrjRec fId ids fieldsE _ _) ->
          (\justF -> maybe (rtf n') justF $ elemIndex fId ids) $ \i -> do
            let restCh = filter ((/= fId) . fst) $ zip ids ch
            let readF  = readOnly False . snd
            restRO <- mapM readF restCh >>= return . and
            if restRO then rtt $ fieldsE !! i else rtf n'

        -- Immediate structure binding, preserving effect ordering of bound substructure expressions.
        (PBindInd i iE bodyE _ bAs) -> rtt $ (EC.letIn i iE bodyE) @<- bAs

        (PBindTup ids fieldsE bodyE _ _) -> do
          let vars          = freeVariables bodyE
          let unused (i, e) = readOnly False e >>= return . (&& i `notElem` vars)
          used <- filterM (\i -> unused i >>= return . not) $ zip ids fieldsE
          rtt $ foldr (\(i,e) accE -> EC.letIn i e accE) bodyE used

        (PBindRec ijs ids fieldsE bodyE _ _) -> do
          let vars          = freeVariables bodyE
          let notfvar i     = maybe False (`notElem` vars) $ lookup i ijs
          let unused (i, e) = readOnly False e >>= return . (&& notfvar i)
          used <- filterM (\i -> unused i >>= return . not) $ zip ids fieldsE
          rtt $ foldr (\(i,e) accE -> maybe accE (\j -> EC.letIn j e accE) $ lookup i ijs) bodyE used

        -- Unused effect-free binding removal
        (tag -> ELetIn i) -> do
          let vars = freeVariables $ last ch
          initRO <- readOnly False (head ch) >>= return . (&& i `notElem` vars)
          if initRO then rtt $ last ch else rtf n'

        (tag -> EBindAs b) ->
          let vars = freeVariables $ last ch in
          case b of
            BRecord ijs -> do
              let nBinder = filter (\(_,j) -> j `elem` vars) ijs
              initRO <- readOnly False (head ch) >>= return . (&& null nBinder)
              if initRO
                then rtt $ last ch
                else rtt $ Node (EBindAs (BRecord $ nBinder) :@: annotations n) ch
            _ -> rtf n'

        (tag -> EOperate OSeq) -> do
          lhsRO <- readOnly False (head ch)
          if lhsRO then rtt $ last ch else rtf n'

        -- Immediate option bindings
        PCaseOf (PSome sE _) _ someE _ _ -> do
          someRO <- readOnly False sE
          if someRO then rtt someE else rtf n'

        (PCaseOf (PNone _ _) _ _ noneE _) -> rtt noneE

        -- Branch unnesting for case-of/if-then-else combinations (case-of-case, etc.)
        -- These strip UID and Span annotations due to duplication following the rewrite.
        (PCaseOf (PCaseOf optE i isomeE inoneE icAs) j jsomeE jnoneE jcAs) ->
          rtt $ PCaseOf optE i (PCaseOf isomeE j (cl jsomeE) (cl jnoneE) $ cla jcAs)
                               (PCaseOf inoneE j (cl jsomeE) (cl jnoneE) $ cla jcAs)
                               icAs

        (PCaseOf (PIfThenElse pE tE eE bAs) i isomeE inoneE cAs) ->
          rtt $ PIfThenElse pE (PCaseOf tE i (cl isomeE) (cl inoneE) $ cla cAs)
                               (PCaseOf eE i (cl isomeE) (cl inoneE) $ cla cAs)
                               bAs

        (PIfThenElse (PCaseOf optE i someE noneE cAs) otE oeE oAs) ->
          rtt $ PCaseOf optE i (PIfThenElse someE (cl otE) (cl oeE) $ cla oAs)
                               (PIfThenElse noneE (cl otE) (cl oeE) $ cla oAs)
                               cAs

        (PIfThenElse (PIfThenElse ipE itE ieE iAs) otE oeE oAs) ->
          rtt $ PIfThenElse ipE (PIfThenElse itE (cl otE) (cl oeE) $ cla oAs)
                                (PIfThenElse ieE (cl otE) (cl oeE) $ cla oAs)
                                iAs

        -- Condition aggregation for equivalent sub-branches
        (PIfThenElse opE (PIfThenElse ipE itE (PVar i ivAs) _) (PVar ((== i) -> True) _) oAs) ->
          let npE = EC.binop OAnd opE ipE in
          rtt $ PIfThenElse npE itE (PVar i $ cla ivAs) $ cla oAs

        (PIfThenElse opE (PVar i ivAs) (PIfThenElse ipE (PVar ((== i) -> True) _) ieE _) oAs) ->
          let npE = EC.binop OOr opE ipE in
          rtt $ PIfThenElse npE (PVar i $ cla ivAs) ieE $ cla oAs

        _ -> rtf n'

    pruneExpr _ _ = Left "Invalid eliminateDeadCodeDelta.pruneExpr pattern match"

    onSub ch = (concat *** any id) $ unzip ch
    rtt e = return ([e], True)
    rtf e = return ([e], False)

    cl  = stripExprAnnotations cleanAnns (const False)
    cla = filter (not . cleanAnns)

    cleanAnns a = isEUID a || isESpan a || isAnyETypeOrEffectAnn a


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

instance Pretty CandidateTree where
  prettyLines (Node (uid, cands) ch) =
    [show uid ++ " => "]
      ++ (indent 2 $ concatMap prettyCandidates cands)
      ++ drawSubTrees ch

    where prettyCandidates (e, cnt) = [show cnt ++ " "] %+ (prettyLines $ stripECompare e)

commonProgramSubexprElim :: Maybe ParGenSymS -> K3 Declaration -> Either String (Maybe ParGenSymS, K3 Declaration)
commonProgramSubexprElim cseCntOpt prog = foldExpression commonSubexprElim cseCntOpt prog

commonSubexprElim :: Maybe ParGenSymS -> K3 Expression -> Either String (Maybe ParGenSymS, K3 Expression)
commonSubexprElim cseCntOpt expr = do
    cTree <- buildCandidateTree expr
    -- TODO: log candidates for debugging
    pTree <- debugCTree "CTree" cTree $ pruneCandidateTree cTree
    -- TODO: log pruned candidates for debugging
    substituteCandidates (maybe contigsymS id cseCntOpt) (debugCTree "PTree" pTree pTree) >>= return . first Just

  where
    debugCTree tg ct r = if True then r else trace (boxToString $ [tg] ++ prettyLines ct) r

    covers :: K3 Expression -> K3 Expression -> Bool
    covers a b = runIdentity $ (\f -> foldMapTree f False a) $ \chAcc n ->
      if or chAcc then return $ True else return $ compareEStrictAST n b

    buildCandidateTree :: K3 Expression -> Either String CandidateTree
    buildCandidateTree e = do
      (cTree, _, _) <- foldMapTree buildCandidates ([], [], []) e
      case cTree of
        [x] -> return x
        _   -> Left "Invalid candidate tree"

    buildCandidates :: [([CandidateTree], [K3 Expression], [K3 Expression])] -> K3 Expression
                    -> Either String ([CandidateTree], [K3 Expression], [K3 Expression])
    buildCandidates _ n@(tnc -> (ETuple, [])) = leafTreeAccumulator n
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

              filteredCands = nub $ concatMap pruneOpenCandidates $ zip bnds subAcc
              localCands    = sortBy ((flip compare) `on` snd) $
                                foldl (addCandidateIfLCA subAcc) [] filteredCands

              candTreeNode  = Node (uid, localCands) $ concat ctCh
              nStrippedExpr = Node (tag t :@: cseValidAnnotations t) $ concat sExprCh
              nCands        = case (tag t, n @~ isEType) of
                                (EProject _, _) -> filteredCands ++ []
                                (_, Just (EType (isTFunction -> True))) -> []
                                (_, _) -> filteredCands ++ [nStrippedExpr]
          in do
            nRO <- readOnly False n
            let propagatedExprs = if nRO then nCands else []
            return $ ([candTreeNode], [nStrippedExpr], propagatedExprs)

        _ -> uidError n

      where
        pruneOpenCandidates ([], cands) = cands
        pruneOpenCandidates (bnds, cands) =
          filter (\e -> null $ freeVariables e `intersect` bnds) cands

    leafTreeAccumulator :: K3 Expression
                        -> Either String ([CandidateTree], [K3 Expression], [K3 Expression])
    leafTreeAccumulator e = do
      ctNode <- leafCandidateNode e
      return $ ([ctNode], [Node (tag e :@: cseValidAnnotations e) []], [])

    leafCandidateNode :: K3 Expression -> Either String CandidateTree
    leafCandidateNode e = case e @~ isEUID of
      Just (EUID uid) -> Right $ Node (uid, []) []
      _               -> uidError e

    cseValidAnnotations e = filter cseValidAnn $ annotations e
    cseValidAnn a = isEQualified a || isEUserProperty a || isEUID a || isEAnnotation a || isAnyETypeOrEffectAnn a

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
      let i = length $ filter (compareEStrictAST sub) cs in (if i > 0 then a+1 else a, b+i)

    pruneCandidateTree :: CandidateTree -> Either String CandidateTree
    pruneCandidateTree = biFoldMapTree trackCandidates pruneCandidates [] (Node (UID $ -1, []) [])
      where
        trackCandidates :: Candidates -> CandidateTree -> Either String (Candidates, [Candidates])
        trackCandidates candAcc (Node (_, cands) ch) = do
          let nCandAcc = foldl appendCandidate candAcc cands
          return (nCandAcc, replicate (length ch) nCandAcc)

        pruneCandidates :: Candidates -> [CandidateTree] -> CandidateTree
                        -> Either String CandidateTree
        pruneCandidates candAcc ch (Node (uid, cands) _) = do
          let isCand p@(e, _) = if elem p candAcc then readOnly False e else return False
          used <- filterM isCand cands
          let nUid = if null used then UID $ -1 else uid
          return $ Node (nUid, used) ch

    substituteCandidates :: ParGenSymS -> CandidateTree -> Either String (ParGenSymS, K3 Expression)
    substituteCandidates symS prunedTree = do
        substitutions            <- foldMapTree concatCandidates [] prunedTree
        (nsymS, ncSubstitutions) <- foldSubstitutions symS substitutions
        nExpr                    <- foldM substituteAtUID expr ncSubstitutions
        return (nsymS, nExpr)

      where
        concatCandidates candAcc (Node (uid, cands) _) =
          return $ (map (\(e,i) -> (uid,e,i)) cands) ++ (concat candAcc)

        foldSubstitutions :: ParGenSymS -> [Substitution] -> Either String (ParGenSymS, [NamedSubstitution])
        foldSubstitutions startsymS subs = do
          (nsymS, namedSubs) <- foldM nameSubstitution (startsymS, []) subs
          nnsubs <- foldM (\subAcc sub -> mapM (closeOverSubstitution sub) subAcc) namedSubs namedSubs
          return (nsymS, nnsubs)

        nameSubstitution :: (ParGenSymS, [NamedSubstitution]) -> Substitution -> Either String (ParGenSymS, [NamedSubstitution])
        nameSubstitution (symS', acc) (uid, e, i) = return (nsymS', acc++[(uid, ("__cse"++show c), e, i)])
          where (nsymS', c) = gensym symS'

        closeOverSubstitution :: NamedSubstitution -> NamedSubstitution -> Either String NamedSubstitution
        closeOverSubstitution (uid, n, e, _) (uid2, n2, e2, i2)
          | uid == uid2 && n == n2           = return $ (uid, n, e2, i2)
          | e2 `covers` e && e2 `hasUID` uid = return $ (uid2, n2, substituteExpr e (EC.variable n) e2, i2)
          | otherwise                        = return $ (uid2, n2, e2, i2)

        hasUID :: K3 Expression -> UID -> Bool
        hasUID e u = runIdentity $ foldMapTree checkUID False e
          where checkUID (or -> inCh) n =
                  case n @~ isEUID of
                    Just (EUID u2) -> return $ inCh || u == u2
                    _ -> return $ inCh

        substituteAtUID :: K3 Expression -> NamedSubstitution -> Either String (K3 Expression)
        substituteAtUID targetE (uid, n, e, _) = mapTree (letAtUID uid n e) targetE

        letAtUID :: UID -> Identifier -> K3 Expression -> [K3 Expression] -> K3 Expression
                 -> Either String (K3 Expression)
        letAtUID uid cseId e ch n = case n @~ isEUID of
          Just (EUID uid2) -> return $
            let cseVar = EC.variable cseId in
            let qualE  = case e @~ isEQualified of
                           Nothing -> e @+ EImmutable
                           Just _  -> e
            in if uid == uid2
                 then EC.letIn cseId qualE $ substituteExpr e cseVar n
                 else replaceCh n ch
          _ -> return $ replaceCh n ch

        substituteExpr :: K3 Expression -> K3 Expression -> K3 Expression -> K3 Expression
        substituteExpr compareE newE targetE = runIdentity $ modifyTree (doSub compareE newE) targetE
        doSub compareE newE n = return $ if compareEAST compareE n then qualifySub n newE else n
        qualifySub ((@~ isEQualified) -> Nothing)   n = n
        qualifySub ((@~ isEQualified) -> Just qAnn) n = n @+ qAnn
        qualifySub _ _ = error "Invalid commonSubexprElim.qualifySub pattern match"

    uidError e = Left $ "No UID found on " ++ show e


-- | Collection transformer fusion.
-- TODO: duplicate eliminating fusion on sets (fine for bags/lists)

streamableTransformerArg :: K3 Expression -> (Bool, FusionAccFClass, FusionAccTClass)
streamableTransformerArg (PStreamableTransformerArg i j (PAccumulate ((== i) -> True) ((== j) -> True)) _ _) = (True, UCond, IdTr)
streamableTransformerArg (PStreamableTransformerArg _ _ _ _ _) = (True, ICondN, IndepTr)
streamableTransformerArg _ = (False, Open, DepTr)

encodeProgramTransformers :: K3 Declaration -> Either String (Bool, K3 Declaration)
encodeProgramTransformers prog = foldExpression (encodeTransformers False) False prog

encodeTransformers :: Bool -> Bool -> K3 Expression -> Either String (Bool, K3 Expression)
encodeTransformers noBR restChanged expr = do
    (changed, eOpt) <- foldMapTree encodeUntilFirst (False, Nothing) expr
    maybe err (return . (restChanged || changed,)) eOpt

  where
    err = Left "Invalid fusion encoding"

    encodeUntilFirst (unzip -> (chFused, catMaybes -> ch)) n =
      if or chFused then return $ (True, Just $ replaceCh n ch)
      else encode (replaceCh n ch) >>= return . second Just

    rtt = return . (True,)
    rtf = return . (False,)

    transformable as cE = any isETransformer as && (not $ any isEFusionSpec as) && compatibleCollectionType cE

    compatibleCollectionType cE =
      case cE @~ isEType of
        Just (EType (details -> (TCollection, _, isMapCE -> True))) -> False
        _ -> True

      where isMapCE anns = case find isTAnnotation anns of
                             Nothing -> False
                             Just (TAnnotation i) -> "MapCE" `isInfixOf` i

    encode e@(PPrjApp cE fId fAs _ _)
      | unaryTransformer fId && transformable fAs cE
        = case fId of
            "filter"  -> mkFold1 e
            "map"     -> mkFold1 e
            "iterate" -> mkIter  e
            "ext"     -> rtf e
            _         -> rtf e
      | unaryTransformer fId = rtf $ debugEncode fId fAs e

    encode e@(PPrjApp2 cE fId fAs _ _ _ _)
      | binaryTransformer fId && transformable fAs cE = mkFold2 e
      | binaryTransformer fId = rtf $ debugEncode fId fAs e

    encode e@(PPrjApp3 cE fId fAs _ _ _ _ _ _)
      | ternaryTransformer fId && transformable fAs cE = mkFold3 e
      | ternaryTransformer fId = rtf $ debugEncode fId fAs e

    encode e = rtf e

    debugEncode trid as e =
      let uid = case filter isEUID as of
                  [] -> -1
                  (head -> EUID (UID u)) -> u
                  _ -> error "Invalid encodeTransformers.debugEncode pattern match"

          str = unwords [ "debugEncode", trid, "UID", show uid, ":"
                        , show $ any isETransformer as
                        , show $ any isEFusionSpec as
                        , show $ filter (not . isAnyETypeOrEffectAnn) as]
      in
      if True then e else trace (boxToString $ [str] ++ (prettyLines $ stripExprAnnotations isAnyETypeOrEffectAnn (const False) e)) e

    -- Fold constructors for transformers.
    mkFold1 e@(PPrjApp cE fId fAs fArg appAs) = do
      accE           <- mkAccumE e
      (nfAs', nfArg) <- mkIndepAccF fId fAs fArg
      nfAs           <- markPureTransformer nfAs' fArg
      let (nApp1As, nApp2As) = (markTAppChain appAs, markTAppChain $ filter isEQualified appAs)
      rtt $ PPrjApp2 cE "fold" nfAs nfArg accE nApp1As nApp2As

    mkFold1 e = rtf e

    mkIter (PPrjApp cE _ fAs fArg appAs) = do
      let nfAs = fAs ++ [pImpureTransformer, pFusionSpec (UCondVal, IndepTr), pFusionLineage "iterate"]
      let (nApp1As, nApp2As) = (markTAppChain appAs, markTAppChain $ filter isEQualified appAs)
      rtt $ PPrjApp2 cE "fold" nfAs (EC.lambda "_" fArg) EC.unit nApp1As nApp2As

    mkIter e = rtf e

    -- TODO: infer simpler top-level structure of accumulator function than ICondN.
    -- i.e., ICond1? UCond?
    mkFold2 (PPrjApp2 cE fId fAs
                      fArg1@(streamableTransformerArg -> (streamable, fCls, tCls)) fArg2
                      app1As app2As)
      = do
          let cls   = if streamable then (fCls,tCls) else (Open,DepTr)
          let nfAs' = fAs ++ [pFusionSpec cls, pFusionLineage fId]
          nfAs      <- markPureTransformer nfAs' fArg1
          let r     = PPrjApp2 cE fId nfAs fArg1 fArg2 app1As app2As
          if True then rtt r else debugInferredFold r nfAs fArg1

        where debugInferredFold e (getFusionSpecA -> Just cls) fArg1' = do
                fa1FMap <- either (Left . T.unpack) Right $ SE.inferDefaultExprEffects fArg1'
                rtt $ flip trace e $
                  unlines [ unwords ["Fold function effects"]
                          , T.unpack $ PT.pretty fa1FMap
                          , unwords ["Inferred fold:", show streamable, show cls]
                          , pretty e ]
              debugInferredFold _ _ _ = Left "Invalid fusion-fold construction"

    mkFold2 e = rtf e

    mkFold3 e@(PPrjApp3 cE fId fAs fArg1 fArg2 fArg3 app1As app2As app3As)
      = case fId of
          "group_by" -> do
            (accE, valueT)  <- mkGBAccumE e
            rAccE           <- mkAccumE e
            (nfAs', nfArg1) <- mkGBAccumF valueT fAs fArg1 fArg2 fArg3
            fArg1RO         <- readOnly True fArg1
            fArg2RO         <- readOnly True fArg2
            let nfAs                       = nfAs' ++ if fArg1RO && fArg2RO
                                                      then [pPureTransformer] else [pImpureTransformer]
            let (nApp1As, nApp2As)         = (markTAppChain app1As, markTAppChain app2As)
            let buildE                     = PPrjApp2 cE "fold" nfAs nfArg1 accE nApp1As nApp2As
            let copyF                      = mkIdAccF
            let (ncAs, ncApp1As, ncApp2As) = ([ pTransformer, pPureTransformer
                                              , pFusionSpec (UCond, IdTr), pFusionLineage "copy" ]
                                             , markTAppChain $ filter isEQualified app2As
                                             , markTAppChain $ filter isEQualified app3As)
            rtt $ PPrjApp2 buildE "fold" ncAs copyF rAccE ncApp1As ncApp2As

          _ -> Left $ "Invalid ternary transformer: " ++ fId

    mkFold3 e = rtf e

    mkAccF elemF bodyF =
      let (aVar, aVarId) = (EC.variable "acc", "acc")
          (eVar, eVarId) = (EC.variable "e",   "e")
      in
      EC.lambda aVarId $ EC.lambda eVarId $
        bodyF aVar eVar $ PSeq (EC.applyMany (EC.project "insert" aVar) [elemF aVar eVar]) aVar []

    mkCondAccF elemF condF = mkAccF elemF $ \aVar eVar accumE ->
      EC.ifThenElse (attachBoth $ EC.applyMany (attachFusionSource condF) [eVar]) accumE aVar

    -- Note the element accumulator must be a function to match with our UCond pattern.
    mkIdAccF = mkAccF (\_ e -> EC.applyMany (EC.lambda "x" $ EC.variable "x") [e]) (\_ _ e -> e)

    attachNoBR :: K3 Expression -> K3 Expression
    attachNoBR e = if noBR then e @+ (EProperty (Left ("NoBetaReduce", Nothing))) else e

    attachFusionSource :: K3 Expression -> K3 Expression
    attachFusionSource e = e @+ (EProperty (Left ("FusionSource", Nothing)))

    attachBoth = attachFusionSource . attachNoBR

    mkIndepAccF fId fAs fArg =
      case fId of
        "filter" -> let nfAs = fAs ++ [pFusionSpec (ICond1, IdTr), pFusionLineage "filter"]
                    in return (nfAs, mkCondAccF (\_ e -> e) fArg)

        "map" -> let nfAs = fAs ++ [pOElemRec, pFusionSpec (UCond, IndepTr), pFusionLineage "map"]
                 in return (nfAs, mkAccF (\_ e -> EC.applyMany (EC.lambda "x" $ elemE' $ EC.variable "x")
                                                    [ attachBoth $ EC.applyMany (attachFusionSource fArg) [e] ]) (\_ _ e -> e))

        _ -> invalidAccFerr fId

    mkGBAccumF valueT fAs gbE accFE zE =
      let (aVar, aVarId) = (EC.variable "acc", "acc")
          (eVar, eVarId) = (EC.variable "e",   "e")
          (rVar, rVarId) = (EC.variable "r",   "r")
          (oVar, oVarId) = (EC.variable "o",   "o")

          -- Create a stripped version of accFE for use in the none branch,
          -- to avoid duplicate UIDs.
          missingAccFE = stripEUIDSpan accFE

          entryE v = EC.record [("key", attachBoth $ EC.applyMany (attachFusionSource gbE) [eVar]), ("value", v)]

          missingE = EC.lambda "_" $
                       EC.record [("key", EC.project "key" rVar)
                                 ,("value", EC.applyMany missingAccFE [zE, eVar])]

          presentE = EC.lambda oVarId $
                      EC.record [("key", EC.project "key" oVar)
                                ,("value", attachBoth $ EC.applyMany (attachFusionSource accFE) [EC.project "value" oVar, eVar])]

      in do
      defaultV <- defaultExpression valueT
      return $ (fAs++[pFusionSpec (DCond2, IndepTr), pFusionLineage "group_by"],
        EC.lambda aVarId $ EC.lambda eVarId $
          EC.letIn rVarId (entryE defaultV) $
          PSeq (EC.applyMany (EC.project "upsert_with" $ aVar) [rVar, missingE, presentE]) aVar [])

    mkGBAccumE e = case collectionElementType e of
      Just (_,et) -> mkGBAccumMap e et
      _ -> gbAccumEerr e

    mkGBAccumMap e rt = case recordType rt of
      Just [("key", _), ("value", vt)] -> return $ ((EC.empty rt) @+ EAnnotation "Map", vt)
      _ -> gbAccumEerr e

    mkAccumE e = case collectionElementType e of
      Just (ct,et) -> return $ annotateCAccum ct $ EC.empty et
      _ -> accumEerr e

    collectionElementType e = case e @~ isEType of
      Just (EType t@(tag -> TCollection)) ->
        let ch = children t in
        if null ch then Nothing else Just $ (t, head ch)
      _ -> Nothing

    recordType t@(tag -> TRecord ids) = Just $ zip ids $ children t
    recordType _ = Nothing

    annotateCAccum t e = foldl (@+) e $ concatMap extractTAnnotation $ annotations t
    extractTAnnotation (TAnnotation n) = [EAnnotation n]
    extractTAnnotation _ = []

    unaryTransformer   fId = fId `elem` ["map", "filter", "iterate", "ext"]
    binaryTransformer  fId = fId `elem` ["fold", "sample"]
    ternaryTransformer fId = fId `elem` ["group_by"]

    cleanAnns as = filter (not . isAnyETypeOrEffectAnn) as

    markTAppChain as = cleanAnns $ nub $ as ++ [pTAppChain]

    markPureTransformer as e = do
      eRO <- readOnly True e
      return $ cleanAnns $ nub $
        if eRO then as ++ [pPureTransformer] else as ++ [pImpureTransformer]

    elemE' e = EC.record [("elem", e)]

    invalidAccFerr i = Left $ "Invalid transformer function for independent accumulation: " ++ i

    gbAccumEerr e = Left . boxToString $ ["Invalid group-by result type on: "] %$ prettyLines e
    accumEerr   e = Left . boxToString $ ["Invalid accumulator construction on: "] %$ prettyLines e


fuseProgramFoldTransformers :: K3 Declaration -> Either String (K3 Declaration)
fuseProgramFoldTransformers prog = mapExpression fuseFoldTransformers prog

fuseFoldTransformers :: K3 Expression -> Either String (K3 Expression)
fuseFoldTransformers expr = do
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

    fuse nch@(PPrjApp2ChainCh cE fId1@(leftFusable -> True) "fold"
                              fArg1 ((@~ isEType) -> Just (EType fAccT))
                              gArg1 gArg2@((@~ isEType) -> Just (EType gAccT))
                              fAs _ _ gAs oApp1As oApp2As)
      | fusableChain fAs gAs
        = case fuseAccF fArg1 gArg1 fAs gAs fAccT gAccT of
            Right (Just (ngAs, ngArg1)) -> do
              let r = PPrjApp2 cE fId1 (cleanElemAnns ngAs) ngArg1 gArg2 oApp1As oApp2As
              return (True, debugFusionStep fAs gAs ngArg1 r)

            Right Nothing -> return $ (False, uncurry replaceCh nch)
            Left err -> Left err

      where cleanElemAnns as = filter (\a -> not $ isEIElemRec a) as

    fuse nch@(PApp _ _ (any isETAppChain -> True), _)   = return $ (False, uncurry replaceCh nch)
    fuse nch@(PPrj _ _ (any isETransformer -> True), _) =
      let spec = fst nch @~ isEFusionSpec in
      case spec of
        Just p@(getFusionSpec -> Just spec) ->
            let nann = updateFusionSpec (annotations $ fst nch) spec
            in return $ (False, uncurry replaceCh nch @<- nann)

        _ -> return $ (False, uncurry replaceCh nch)

    fuse nch = return $ (False, uncurry replaceCh nch)

    leftFusable fId = fId `elem` ["sample", "fold"]

    debugFusionStep fAs gAs ngArg1 e =
      if False then e else flip trace e $
        unlines [ "Fused:" ++ (showFusion fAs gAs)]

    debugFusionMatching lAccF rAccF lAs rAs r =
      if True then r
      else let pp e   = pretty $ stripAllExprAnnotations e
               onFail = unlines ["Fail", pp lAccF, pp rAccF]
           in trace (unwords ["Fusing:", showFusion lAs rAs
                             , either id (maybe onFail (const "Success")) r]) r

    debugGroupBy isHG isMG lAccF rAccF r =
      if True then r else flip trace r $
      let match f = case f of
                      PDCond2 _ _ _ _ _ _ _ _  -> "DCond2"
                      PSDCond2 _ _ _ _ _ _ _ _ -> "SDCond2"
                      _ -> "No-match"
      in
      let (lty, rty) = (match lAccF, match rAccF)
      in
      let pfx = if isHG then "H" else if isMG then "M" else "N" in
      boxToString $ [unwords [pfx, "DCond2-DCond2", show isHG, lty, rty, ":"]]
        ++ (prettyLines $ cleanExpr lAccF) ++ (prettyLines $ cleanExpr rAccF)

    fuseAccF lAccF rAccF
             lAs@(getFusionSpecA -> Just (lfCls, ltCls))
             rAs@(getFusionSpecA -> Just (rfCls, rtCls))
             lAccT rAccT
      =
        debugFusionMatching lAccF rAccF lAs rAs $
        case (lfCls, rfCls) of

          -- Copy-elimination when accumulators are of the same type.
          (_, UCond) | rtCls == IdTr && compatibleFusionTypes lAccT rAccT ->
            return $ Just (updateFusionSpec lAs (lfCls, ltCls), lAccF)

          -- Fusion for {UCond, ICond1} x {UCond, ICond1} cases
          --
          (UCond, UCond) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PUCond li lj lfE lfArg, PUCond _ rj rfE rfArg) -> do
                composedE <- chainFunctions lfE lfArg lAs rj rfE rfArg rAs
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
              (PUCond li lj lfE lfArg, PICond1 _ rj rpE rpArg rtE) -> do
                composedP <- chainFunctions lfE lfArg lAs rj rpE rpArg rAs
                composedT <- chainFunRight lj (cleanExpr lfE) (cleanExpr lfArg) lAs rj rtE rAs
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
              (PICond1 li lj lpE lpArg ltE, PUCond _ rj rfE rfArg) -> do
                idP       <- mkIdF False lpE lpArg
                composedT <- chainFunLeft lj ltE lAs rj rfE rfArg rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT idP

              -- Structure-preserving handling of PSICond1 and PSUCond.
              (PSICond1 li lj lpE ltE, PUCond _ rj rfE rfArg) -> do
                composedT <- chainFunLeft lj ltE lAs rj rfE rfArg rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT lpE

              (PICond1 li lj lpE lpArg ltE, PSUCond _ rj rvE) -> do
                idP       <- mkIdF False lpE lpArg
                composedT <- chainValues lj ltE lAs rj rvE rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  mkCondAccF li lj composedT idP

              (PSICond1 li lj lpE ltE, PSUCond _ rj rvE) -> do
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
                let liV        = EC.variable li
                let innerF     = mkCondAccF ri rj rtE $ EC.applyMany rpE [rpArg]
                let chainInner = EC.applyMany innerF [liV, ltE]
                let callOuterP = EC.applyMany lpE [lpArg]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  PChainLambda1 li lj (EC.ifThenElse callOuterP chainInner liV) [] []

              -- Structure-preserving PSICond1 cases
              (PSICond1 li lj lpE ltE, PICond1 ri rj rpE rpArg rtE) -> do
                let liV        = EC.variable li
                let innerF     = mkCondAccF ri rj rtE $ EC.applyMany rpE [rpArg]
                let chainInner = EC.applyMany innerF [liV, ltE]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  PChainLambda1 li lj (EC.ifThenElse lpE chainInner liV) [] []

              (PICond1 li lj lpE lpArg ltE, PSICond1 ri rj rpE rtE) -> do
                let liV        = EC.variable li
                let innerF     = mkCondAccF ri rj rtE rpE
                let chainInner = EC.applyMany innerF [liV, ltE]
                let callOuterP = EC.applyMany lpE [lpArg]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  PChainLambda1 li lj (EC.ifThenElse callOuterP chainInner liV) [] []

              (PSICond1 li lj lpE ltE, PSICond1 ri rj rpE rtE) -> do
                let liV        = EC.variable li
                let innerF     = mkCondAccF ri rj rtE rpE
                let chainInner = EC.applyMany innerF [liV, ltE]
                return $ Just $ (updateFusionSpec rAs (ICond1, promoteTCls ltCls rtCls),) $
                  PChainLambda1 li lj (EC.ifThenElse lpE chainInner liV) [] []

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
                nf <- chainCondRightOpen li lj lpE lpArg ltE lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (PChainLambda1 li lj lE lAs1 lAs2, PChainLambda1 ri rj rE rAs1 rAs2) -> do
                nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs
                return $ Just $ (updateFusionSpec rAs (ICondN, promoteTCls ltCls rtCls), nf)

              (_, _) -> Right Nothing

          (ICondN, UCond) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PChainLambda1 li lj lE lAs1 lAs2, PUCond _ rj rfE rfArg) ->
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
              (PChainLambda1 li lj lE lAs1 lAs2, PICond1 _ rj rpE rpArg rtE) ->
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
              (PUCond li lj lfE lfArg, PDCond2 ri rj rleti roi rdlV rgbF raccF rzE) ->
                let lE = mkAccE (EC.variable li) $ EC.applyMany lfE [lfArg] in
                chainValDCond2 li lj lE [] [] ri rj roi (Right (rleti, rdlV, rgbF, rzE, raccF)) lAs rAs DCond2 $ promoteTCls ltCls rtCls

              (PChainLambda1 li lj lE lAs1 lAs2, PDCond2 ri rj rleti roi rdlV rgbF raccF rzE) -> do
                chainValDCond2 li lj lE lAs1 lAs2 ri rj roi (Right (rleti, rdlV, rgbF, rzE, raccF)) lAs rAs DCond2 $ promoteTCls ltCls rtCls

              (PChainLambda1 li lj lE lAs1 lAs2, PSDCond2 ri rj rleti roi rdlK rdlV rmvE rpvE) -> do
                chainValDCond2 li lj lE lAs1 lAs2 ri rj roi (Left (rleti, rdlK, rdlV, rmvE, rpvE)) lAs rAs DCond2 $ promoteTCls ltCls rtCls

              (_, _) -> Right Nothing

          -- TODO: PCL1-PDCond2, PICond1-PSDCond2, PCL1-PSDCond2 cases
          (ICond1, DCond2) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PICond1 li lj lpE lpArg ltE, PDCond2 ri rj rleti roi rdlV rgbF raccF rzE) -> do
                let liV = EC.variable li
                nrF     <- mkGBAccumF ri rj roi (Right (rleti, rdlV, rgbF, rzE, raccF))
                promote <- promoteRecType lAs rAs
                return $ Just $ (updateFusionSpec rAs (ICond1, DepTr),) $
                  PChainLambda1 li lj
                    (EC.ifThenElse (EC.applyMany lpE [lpArg])
                                   (EC.applyMany nrF [liV, if promote then elemE ltE else ltE])
                                   liV) [] []

              (_, _) -> Right Nothing

          (ICondN, DCond2) | nonDepTr ltCls && nonDepTr rtCls ->
            case (lAccF, rAccF) of
              (PChainLambda1 li lj lE lAs1 lAs2, PDCond2 ri rj rleti roi rdlV rgbF raccF rzE) ->
                chainValDCond2 li lj lE lAs1 lAs2 ri rj roi (Right (rleti, rdlV, rgbF, rzE, raccF)) lAs rAs ICondN DepTr

              (PChainLambda1 li lj lE lAs1 lAs2, PSDCond2 ri rj rleti roi rdlK rdlV rmvE rpvE) -> do
                chainValDCond2 li lj lE lAs1 lAs2 ri rj roi (Left (rleti, rdlK, rdlV, rmvE, rpvE)) lAs rAs DCond2 $ promoteTCls ltCls rtCls

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

          -- Assumes ACC2 is the identity function.
          -- let oentry = {key: mut null_k2, val: z2} in
          -- let ientry = {key: mut null_k1, val: z1} in
          -- \e ->
          --   bind ientry as {key:k} in k = G1(e);
          --   bind oentry as {key:k} in k = G2(ientry.key);
          --   oacc.upsert_with oentry
          --     (\_   -> let niacc = z2 in
          --              niacc.insert {key:ientry.key, val: ACC1 z1 e};
          --              {key:oentry.key, val: niacc})
          --     (\iacc -> iacc.val.upsert_with ientry
          --                 (\_    -> {key:ientry.key, val: ACC1 z1 e})
          --                 (\iold -> {key:iold.key,   val: ACC1 iold.val e});
          --               {key: iacc.key, val: iacc.val});
          --   oacc
          --
          --
          -- Simpler version:
          -- \acc e ->
          --   let ientry = {key: G1(e),      val: z1};
          --   let oentry = {key: G2(ientry), val: z2};
          --   acc.upsert_with oentry
          --     (\_   -> let niacc = z2 in
          --              niacc.insert {key:ientry.key, val: ACC1 ientry.val e};
          --              {key:oentry.key, val: niacc})
          --     (\iacc -> iacc.val.upsert_with ientry
          --                 (\_    -> {key:ientry.key, val: ACC1 ientry.val e})
          --                 (\iold -> {key:iold.key,   val: ACC1 iold.val e});
          --               {key: iacc.key, val: iacc.val});
          --   acc
          --
          (DCond2, DCond2) | (debugGroupBy (any isEHierarchicalGroupBy rAs) False lAccF rAccF $ nonDepTr ltCls)
                              && nonDepTr rtCls && any isEHierarchicalGroupBy rAs
            ->
            let mkKey       e = EC.project "key" e
                mkVal       e = EC.project "value" e
                mkKVRec kE vE = EC.record [("key", kE), ("value", vE)]

                (ieId, oeId)   = ("ientry", "oentry")
                (nVarId, nV)   = ("niacc", EC.variable "niacc")
                (iuVarId, iuV) = ("iold", EC.variable "iold")
                (uVarId, uV)   = ("iacc", EC.variable "iacc")

                iekE         = mkKey $ EC.variable ieId
                ievE         = mkVal $ EC.variable ieId
                oekE         = mkKey $ EC.variable oeId
            in
            case (lAccF, rAccF) of
              (PDCond2 li lj _ _ _ lgbF laccE lzE, PDCond2 _ rj _ _ _ rgbF _ rzE) ->
                let (liV, ljV) = (EC.variable li, EC.variable lj)

                    chainRGBF = EC.lambda rj $ EC.applyMany rgbF [EC.variable rj]

                    ientryE   = stripEUIDSpan $ mkKVRec (EC.applyMany lgbF [ljV]) lzE
                    oentryE   = stripEUIDSpan $ mkKVRec (EC.applyMany chainRGBF [ientryE]) rzE

                    insertF = EC.lambda "_" $ EC.letIn nVarId (stripEUIDSpan rzE) $
                                EC.binop OSeq
                                  (EC.applyMany (EC.project "insert" nV)
                                                [mkKVRec iekE $ EC.applyMany laccE [ievE, ljV]])
                                  (mkKVRec oekE nV)

                    iInsertF = EC.lambda "_" $ mkKVRec iekE
                                 $ EC.applyMany (stripEUIDSpan laccE) [ievE, ljV]

                    iUpdateF = EC.lambda iuVarId $ mkKVRec (mkKey iuV)
                                 $ EC.applyMany (stripEUIDSpan laccE) [mkVal iuV, ljV]

                    updateF = EC.lambda uVarId $
                                EC.binop OSeq
                                  (EC.applyMany (EC.project "upsert_with" $ mkVal uV)
                                                [EC.variable ieId, iInsertF, iUpdateF])
                                  uV

                    nfE = EC.letIn ieId ientryE $
                          EC.letIn oeId oentryE $
                          EC.binop OSeq
                            (EC.applyMany (EC.project "upsert_with" liV)
                                          [EC.variable oeId, insertF, updateF])
                            liV

                    nf  = PChainLambda1 li lj nfE [] []
                in
                return $ Just (updateFusionSpec rAs (Halt, DepTr), nf)
                  -- ^ TODO: for now, we pick a restrictive fusion spec that disallows further
                  --   fusion with downstream operations.

              (PSDCond2 li lj _ loi ldlK ldlV lmvE lpvE, PDCond2 _ _ _ _ _ rgbF _ rzE) ->
                let liV = EC.variable li

                    ientryE   = stripEUIDSpan $ mkKVRec ldlK ldlV
                    oentryE   = stripEUIDSpan $ mkKVRec (EC.applyMany rgbF [ientryE]) rzE

                    insertF = EC.lambda "_" $ EC.letIn nVarId (stripEUIDSpan rzE) $
                                EC.binop OSeq
                                  (EC.applyMany (EC.project "insert" nV) [mkKVRec iekE lmvE])
                                  (mkKVRec oekE nV)

                    iInsertF = EC.lambda "_" $ mkKVRec iekE $ stripEUIDSpan lmvE

                    iUpdateF = EC.lambda iuVarId $ mkKVRec (mkKey iuV)
                                 $ EC.applyMany (EC.lambda loi lpvE) [iuV]

                    updateF = EC.lambda uVarId $
                                EC.binop OSeq
                                  (EC.applyMany (EC.project "upsert_with" $ mkVal uV)
                                                [EC.variable ieId, iInsertF, iUpdateF])
                                  uV

                    nfE = EC.letIn ieId ientryE $
                          EC.letIn oeId oentryE $
                          EC.binop OSeq
                            (EC.applyMany (EC.project "upsert_with" liV)
                                          [EC.variable oeId, insertF, updateF])
                            liV

                    nf  = PChainLambda1 li lj nfE [] []
                in
                return $ Just (updateFusionSpec rAs (Halt, DepTr), nf)
                  -- ^ TODO: for now, we pick a restrictive fusion spec that disallows further
                  --   fusion with downstream operations.

              (PSDCond2 li lj _ loi ldlK ldlV lmvE lpvE, PSDCond2 _ rj _ _ rdlK rdlV rmvE _) ->
                let liV = EC.variable li

                    ientryE   = stripEUIDSpan $ mkKVRec ldlK ldlV
                    oentryE   = stripEUIDSpan $ mkKVRec (EC.applyMany (EC.lambda rj rdlK) [ientryE]) rdlV

                    insertF = EC.lambda "_" $
                                mkKVRec oekE $ EC.applyMany (EC.lambda rj rmvE) [mkKVRec iekE lmvE]

                    iInsertF = EC.lambda "_" $ mkKVRec iekE $ stripEUIDSpan lmvE

                    iUpdateF = EC.lambda iuVarId $ mkKVRec (mkKey iuV)
                                 $ EC.applyMany (EC.lambda loi lpvE) [iuV]

                    updateF = EC.lambda uVarId $
                                EC.binop OSeq
                                  (EC.applyMany (EC.project "upsert_with" $ mkVal uV)
                                                [EC.variable ieId, iInsertF, iUpdateF])
                                  uV

                    nfE = EC.letIn ieId ientryE $
                          EC.letIn oeId oentryE $
                          EC.binop OSeq
                            (EC.applyMany (EC.project "upsert_with" liV)
                                          [EC.variable oeId, insertF, updateF])
                            liV

                    nf  = PChainLambda1 li lj nfE [] []
                in
                return $ Just (updateFusionSpec rAs (Halt, DepTr), nf)
                  -- ^ TODO: for now, we pick a restrictive fusion spec that disallows further
                  --   fusion with downstream operations.

              (_, _) -> Right Nothing

          -- Assumes ACC2 is the same as ACC1, i.e., (+,+) or (*,*).
          -- entry = {key: mut null_k2, val: z2}
          -- for each element in c:
          --   bind entry as {key:k} in k = G2(G1(item));
          --   acc.upsert_with entry
          --     (\_   -> {key:entry.key, val: ACC2 z2 (ACC1 z1 e)})
          --     (\old -> {key:old.key,   val: ACC1 old.val e})
          --
          -- Simpler variant:
          -- \acc e ->
          --   let entry = {key: G2(G1(item)), val: z1};
          --   acc.upsert_with entry
          --     (\_   -> {key:entry.key, val: ACC2 z2 (ACC1 z1 e)})
          --     (\old -> {key:old.key,   val: ACC1 old.val e});
          --   acc
          (DCond2, DCond2) | (debugGroupBy False (any isEMonoidGroupBy rAs) lAccF rAccF $ nonDepTr ltCls)
                              && nonDepTr rtCls && any isEMonoidGroupBy rAs ->
            let mkKey       e = EC.project "key" e
                mkVal       e = EC.project "value" e
                mkKVRec kE vE = EC.record [("key", kE), ("value", vE)]
                (eId, eV)     = ("entry", EC.variable "entry")
                (uVarId, uV)  = ("old", EC.variable "old")
            in
            case (lAccF, rAccF) of
              (PDCond2 li lj _ _ _ lgbF laccE lzE, PDCond2 _ rj _ _ _ rgbF raccE rzE) ->
                let (liV, ljV) = (EC.variable li, EC.variable lj)

                    chainRGBF = EC.lambda rj $ EC.applyMany rgbF [EC.variable rj]

                    entryE    = mkKVRec (EC.applyMany chainRGBF [EC.applyMany lgbF [ljV]]) lzE

                    insertF   = EC.lambda "_" $ mkKVRec (mkKey eV)
                                  $ EC.applyMany raccE [rzE, EC.applyMany laccE [lzE, ljV]]

                    updateF   = EC.lambda uVarId $ mkKVRec (mkKey uV)
                                  $ EC.applyMany laccE [mkVal uV, ljV]

                    nfE = EC.letIn eId entryE $
                          EC.binop OSeq
                            (EC.applyMany (EC.project "upsert_with" liV)
                                          [eV, insertF, updateF])
                            liV

                    nf  = PChainLambda1 li lj nfE [] []
                in
                return $ Just (updateFusionSpec rAs (Halt, DepTr), nf)
                  -- ^ TODO: for now, we pick a restrictive fusion spec that disallows further
                  --   fusion with downstream operations.

              (PSDCond2 li lj _ loi ldlK ldlV lmvE lpvE, PSDCond2 _ rj _ _ rdlK rdlV rmvE _) ->
                let liV = EC.variable li

                    entryE     = mkKVRec (EC.applyMany (EC.lambda rj rdlK) [mkKVRec ldlK ldlV]) rdlV

                    insertF    = EC.lambda "_" $ mkKVRec (mkKey eV)
                                   $ EC.applyMany (EC.lambda rj rmvE) [mkKVRec (stripEUIDSpan ldlK) lmvE]

                    updateF    = EC.lambda uVarId $ mkKVRec (mkKey uV)
                                   $ EC.applyMany (EC.lambda loi lpvE) [uV]

                    nfE = EC.letIn eId entryE $
                          EC.binop OSeq
                            (EC.applyMany (EC.project "upsert_with" liV)
                                          [eV, insertF, updateF])
                            liV

                    nf  = PChainLambda1 li lj nfE [] []
                in
                return $ Just (updateFusionSpec rAs (Halt, DepTr), nf)

              (_, _) -> Right Nothing

          -- Fusion into general fold accumulator.
          (UCond,  Open) | nonDepTr ltCls -> chainUCondLambda  lAccF rAccF lAs rAs ltCls rtCls Open Open
          (ICond1, Open) | nonDepTr ltCls -> chainICond1Lambda lAccF rAccF lAs rAs ltCls rtCls Open Open
          (ICondN, Open) | nonDepTr ltCls -> chainICondNLambda lAccF rAccF lAs rAs ltCls rtCls ICondN

          (UCond,  UCondVal) | nonDepTr ltCls && nonDepTr rtCls -> chainUCondLambda  lAccF rAccF lAs rAs ltCls rtCls UCondVal ICondN
          (ICond1, UCondVal) | nonDepTr ltCls && nonDepTr rtCls -> chainICond1Lambda lAccF rAccF lAs rAs ltCls rtCls UCondVal ICondN
          (ICondN, UCondVal) | nonDepTr ltCls && nonDepTr rtCls -> chainICondNLambda lAccF rAccF lAs rAs ltCls rtCls ICondN

          -- Unhandled cases: (UCondVal, *), (Open,*), and (DCond2, Open | UCondVal)
          (_, _) -> Right Nothing

    fuseAccF _ _ _ _ _ _ = Right Nothing

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
        nf <- chainCondRightOpen li lj lpE lpArg ltE lAs ri rj rE rAs1 rAs2 rAs
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

    chainValDCond2 li lj lE lAs1 lAs2 ri rj roi rgbParams lAs rAs nfCls ntCls = do
      PChainLambda1 ri' rj' rE' rAs1 rAs2 <- mkGBAccumF ri rj roi rgbParams
      nf <- chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri' rj' rE' rAs1 rAs2 rAs
      return $ Just $ (updateFusionSpec rAs (nfCls, ntCls), nf)


    -- Expression construction utilities
    chainFunctions lf larg lAs rx rf rarg rAs = do
      promote <- promoteRecType lAs rAs
      case rarg of
        PVar x _ | x == rx -> mkComposedF promote lf larg rf
        _ -> mkChainRightF promote lf larg rx (EC.applyMany rf [rarg])

    chainFunLeft lx le lAs rx rf rarg rAs = do
      promote <- promoteRecType lAs rAs
      case le of
        PApp lf larg _ -> chainFunctions lf larg lAs rx rf rarg rAs
        _ -> mkChainLeftF promote le rx rf rarg

    chainFunRight lx lf larg lAs rx re rAs = do
      promote <- promoteRecType lAs rAs
      case re of
        PVar x _ | x == rx -> mkIdF promote lf larg
        PApp rf rarg _ -> chainFunctions lf larg lAs rx rf rarg rAs
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
        (PApp lf larg _, PApp rf rarg _) -> chainFunctions lf larg lAs rx rf rarg rAs
        (PApp lf larg _, _) -> chainFunRight lx lf larg lAs rx re rAs
        (_, PApp rf rarg _) -> chainFunLeft lx le lAs rx rf rarg rAs
        (_, _) -> promoteRecType lAs rAs >>= \p -> mkChainVals p le rx re

    chainCondN li lj lE accumF lAs1 lAs2 lAs rAs = do
      promote <- promoteRecType lAs rAs
      let (_,nlE) = mapAccumulation (accumF promote) (\x -> x) li $ simpleBetaReduction lE
      return $ PChainLambda1 li lj nlE lAs1 lAs2

    chainCondNRightOpen li lj lE lAs1 lAs2 lAs ri rj rE rAs1 rAs2 rAs =
      let liV = EC.variable li
          accumF promote e = case e of
            (PPrjAppVarSeq ((== li) -> True) "insert" v) ->
              EC.applyMany (PChainLambda1 ri rj rE rAs1 rAs2)
                           [liV, if promote then elemE v else v]
            -- TODO: Handle upsert_with accumulation for group-bys appearing
            -- on the LHS of a fusion step.
            {-
            (PPrjApp3VarSeq ((== li) -> True) "upsert_with" v ml pl) ->
              EC.applyMany (PChainLambda1 ri rj rE rAs1 rAs2)
                           [liV, if promote then elemE v else v]
            -}
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

    elemVar  i = EC.record [("elem", EC.variable i)]
    elemE    e = EC.record [("elem", e)]

    --promoteRecType lAs rAs =
    --  case (any isEOElemRec lAs, any isEIElemRec rAs) of
    --    (i,j) | i /= j -> Left $ "Invalid fusion chained element type annotations"
    --    (True, True)   -> Right True
    --    (_, _)         -> Right False

    promoteRecType _ _ = Right False

    mkAccE accE eE = PSeq (PPrjApp accE "insert" [] eE []) accE []

    mkAccF aVarId eVarId insertElemE bodyF =
      let (aVar, eVar) = (EC.variable aVarId, EC.variable eVarId)
      in PChainLambda1 aVarId eVarId (bodyF aVar eVar $ mkAccE aVar insertElemE) [] []

    mkCondAccF aVarId eVarId insertElemE condE =
      mkAccF aVarId eVarId insertElemE
        $ \aVar _ accumE -> EC.ifThenElse condE accumE aVar

    mkGBAccumF i j o gbParams =
      let iV = EC.variable i
          jV = EC.variable j
          oV = EC.variable o

          (leti, entryE, missingE, presentE) = case gbParams of
            Left (lleti, dlK, dlV, mvE, pvE) ->
              let lentryE = EC.record [("key", dlK), ("value", dlV)]
                  spresE = EC.lambda o $ EC.record [("key", EC.project "key" oV), ("value", pvE)]
                  smissE = EC.lambda "_" $ EC.record [("key", EC.project "key" $ EC.variable leti), ("value", mvE)]
              in (lleti, lentryE, smissE, spresE)

            Right (rleti, dlV, gbE, zE, accFE) ->
              let rentryE       = EC.record [("key", EC.applyMany gbE [jV]), ("value", dlV)]
                  npresE       = EC.lambda o $ EC.record [("key", EC.project "key" oV), ("value", EC.applyMany accFE [EC.project "value" oV, jV])]
                  missingAccFE = stripEUIDSpan accFE
                  nmissE       = EC.lambda "_" $ EC.record [("key", EC.project "key" $ EC.variable leti), ("value", EC.applyMany missingAccFE [zE, jV])]
              in (rleti, rentryE, nmissE, npresE)

          seqApp entryE' =
            PSeq (EC.applyMany (EC.project "upsert_with" iV)
                               [entryE', missingE, presentE])
              iV []
      in
      return $ EC.lambda i $ EC.lambda j $ EC.letIn leti entryE $ seqApp $ EC.variable leti

    compatibleFusionTypes t1@(tnc -> (TCollection, [c1])) t2@(tnc -> (TCollection, [c2])) =
      case (t1 @~ isTAnnotation, t2 @~ isTAnnotation) of
        (Just (TAnnotation ta1), Just (TAnnotation ta2)) ->
          compatibleAnnotations ta1 ta2 && compareTStrictAST c1 c2
        (_, _) -> compareTStrictAST t1 t2

    compatibleFusionTypes t1 t2 = compareTStrictAST t1 t2

    compatibleAnnotations a1 a2 = a1 == a2 || (a1 `elem` mapCls && a2 `elem` mapCls)
      where mapCls = ["Map", "IntMap", "StrMap"]


    cleanExpr e = stripExprAnnotations cleanAnns (const False) e
    cleanAnns a = isEUID a || isESpan a || isAnyETypeOrEffectAnn a

    -- Fusion spec helpers
    updateFusionSpec as spec = nub $ filter (not . isEFusionSpec) as
                                  ++ [pFusionSpec spec]
                                  ++ (case fst spec of {Open -> []; _ -> [pAccumulatingTransformer]})

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

    -- Inserts into MultiIndexVMaps with an additional argument.
    returnAsAccumulator (shadowed, _) _ e@(PPrjApp2VarSeq j "insert" vid v)
      | i == j && not shadowed && notAccessedIn vid && notAccessedIn v =
          if True then return (Right True, onAccumF e)
          else trace ("onAccumF " ++ pretty e) $ return (Right True, onAccumF e)

    {-
    returnAsAccumulator (shadowed, _) _ e@(PPrjApp3VarSeq j "upsert_with" v ml pl)
      | i == j && not shadowed && notAccessedIn v && notAccessedIn ml && notAccessedIn pl =
          if True then return (Right True, onAccumF e)
          else trace ("onAccumF " ++ pretty e) $ return (Right True, onAccumF e)
    -}

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
  where annotationAccumE e = e @+ (inferredEProp "Accumulation" Nothing)

rewriteStreamAccumulation :: Identifier -> K3 Expression -> (Bool, K3 Expression)
rewriteStreamAccumulation i expr = mapAccumulation rewriteAccumE rewriteVarE i expr
  where rewriteAccumE (PPrjAppVarSeq _ "insert" v) = EC.some v
        rewriteAccumE e = e
        rewriteVarE   _ = EC.constant $ CNone NoneImmut


-- | Mark every lambda that either returns or threads its argument.
markProgramLambdas :: K3 Declaration -> Either String (K3 Declaration)
markProgramLambdas prog = do
  lcenv <- lambdaClosures prog
  mapExpression (markLambdas lcenv) prog

markLambdas :: ClosureEnv -> K3 Expression -> Either String (K3 Expression)
markLambdas lcenv expr = modifyTree doMark expr
  where doMark e@(tnc -> (ELambda i, [body])) = do
          u <- uidOf e
          clvars <- maybe closureErr return $ IntMap.lookup u lcenv
          (ra, nbody) <- case inferVariableReturns i clvars body of
                           (False, _) -> return (False, body)
                           (x,y) -> return (x,y)

          (ta, nbody2) <- case inferThreadReturns i nbody of
                            (False, _) -> return (False, nbody)
                            (x,y) -> return (x,y)

          if ta || ra
            then
              let r  = replaceCh e [nbody2]
                  r2 = if ra then r  @+ inferredEProp "ReturnsArgument" Nothing else r
                  r3 = if ta then r2 @+ inferredEProp "ThreadsArgument" Nothing else r2
              in return r3
            else return e

        -- Mark folds that are threading transformers.
        doMark e@(PPrjApp2 cE "fold" fAs fLam fAcc app1As app2As) =
          case fLam @~ isEThreadsArgLambda of
            Nothing -> return e
            _ -> let prje = (PPrj cE "fold" fAs) @+ inferredEProp "ThreadingTransformer" Nothing
                 in return $ PApp (PApp prje fLam app1As) fAcc app2As

        doMark e = return e

        uidOf e   = maybe uidErr (\case {(EUID (UID u)) -> Right u ; _ -> uidErr}) $ e @~ isEUID

        uidErr     = Left "Invalid UID when marking self-returning lambdas."
        closureErr = Left "Invalid closure vars when marking self-returning lambdas."

-- | Infer return points in lambdas that yield either the argument or a closure-captured variable.
--   We annotate the lambda if every return expression is either:
--     i.  the variable itself syntax-wise (for now we consider only the exact variable, not equivalent bnds)
--     ii. a branching expression whose return points are themselves argument-returns.
mapVariableReturns :: (K3 Expression -> K3 Expression)
                   -> (K3 Expression -> K3 Expression)
                   -> Identifier -> [Identifier] -> K3 Expression -> (Bool, K3 Expression)
mapVariableReturns onRetArgF onRetCVarF i cl expr = runIdentity $ do
  (isAcc, e) <- doInference
  return $ (either id id isAcc, e)

  where
    il = i:cl

    doInference = foldMapReturnExpression trackBindings argumentReturn independentF False (Left False) expr

    trackBindings shadowed e = case tag e of
        ELambda j -> return (shadowed, [onBinding shadowed j])
        ELetIn  j -> return (shadowed, [shadowed, onBinding shadowed j])
        ECaseOf j -> return (shadowed, [shadowed, onBinding shadowed j, shadowed])
        EBindAs b -> return (shadowed, [shadowed, foldl onBinding shadowed $ bindingVariables b])
        _ -> return (shadowed, replicate (length $ children e) shadowed)

      where onBinding sh j = if j `elem` il then True else sh

    -- TODO: check effects and lineage rather than free variables.
    independentF shadowed _ e
      | EVariable j <- tag e , j `elem` il && not shadowed = return (Right False, e)
      | EAssign   j <- tag e , j `elem` il && not shadowed = return (Right False, e)

    independentF _ (onIndepR -> isArgReturn) e = return (isArgReturn, e)

    -- TODO: using symbols as lineage here will provide better alias tracking.
    -- TODO: test in-place modification property
    argumentReturn shadowed _ e@(tag -> EVariable j)
      | i == j && not shadowed      = return (Left True, onRetArgF e)
      | j `elem` cl && not shadowed = return (Left True, onRetCVarF e)

    argumentReturn _ (onDirectReturnBranch [0]   -> isArgRet) e@(tag -> ELambda _)     = return (isArgRet, e)
    argumentReturn _ (onDirectReturnBranch [0]   -> isArgRet) e@(tag -> EOperate OApp) = return (isArgRet, e)
    argumentReturn _ (onDirectReturnBranch [1]   -> isArgRet) e@(tag -> EOperate OSeq) = return (isArgRet, e)

    argumentReturn _ (onDirectReturnBranch [1]   -> isArgRet) e@(tag -> ELetIn  _)     = return (isArgRet, e)
    argumentReturn _ (onDirectReturnBranch [1]   -> isArgRet) e@(tag -> EBindAs _)     = return (isArgRet, e)
    argumentReturn _ (onDirectReturnBranch [1,2] -> isArgRet) e@(tag -> ECaseOf _)     = return (isArgRet, e)
    argumentReturn _ (onDirectReturnBranch [1,2] -> isArgRet) e@(tag -> EIfThenElse)   = return (isArgRet, e)

    argumentReturn _ _ e = return (Right False, e)

    onIndepR l = if any not $ rights l then Right False else Left False

    onDirectReturnBranch branchIds l =
      if any not $ rights (map (l !!) branchIds) then Right False
      else ensureEither (map (l !!) branchIds)

    ensureEither l =
      if all id (lefts l) && all id (rights l)
        then (if null $ rights l then Left else Right) True
        else Right False


inferVariableReturns :: Identifier -> [Identifier] -> K3 Expression -> (Bool, K3 Expression)
inferVariableReturns i cl expr = mapVariableReturns annotateArgE annotateClosureE i cl expr
  where annotateArgE     e = e @+ (inferredEProp "ArgReturn" Nothing)
        annotateClosureE e = e @+ (inferredEProp "ClosureReturn" Nothing)


-- | Apply a function to every return expression that is a fold accepting a variable named as the given identifier.
mapThreadReturns :: (K3 Expression -> K3 Expression) -> Identifier -> K3 Expression -> (Bool, K3 Expression)
mapThreadReturns onFoldRetF i expr = runIdentity $ do
  (isAcc, e) <- doInference
  return $ (either id id isAcc, e)

  where
    doInference = foldMapReturnExpression trackBindings threadReturn independentF False (Left False) expr

    trackBindings shadowed e = case tag e of
        ELambda j -> return (shadowed, [onBinding shadowed j])
        ELetIn  j -> return (shadowed, [shadowed, onBinding shadowed j])
        ECaseOf j -> return (shadowed, [shadowed, onBinding shadowed j, shadowed])
        EBindAs b -> return (shadowed, [shadowed, foldl onBinding shadowed $ bindingVariables b])
        _ -> return (shadowed, replicate (length $ children e) shadowed)

      where onBinding sh j = if i == j then True else sh

    independentF shadowed _ e
      | EVariable j <- tag e , i == j && not shadowed = return (Right False, e)
      | EAssign   j <- tag e , i == j && not shadowed = return (Right False, e)

    independentF _ (onIndepR -> isFoldReturn) e = return (isFoldReturn, e)

    threadReturn shadowed _ e@(PPrjApp2 cE "fold" fAs fLam (PVar j jAs) app1As app2As)
      | i == j && not shadowed =
        case (fLam @~ isEReturnsArgLambda, fLam @~ isEThreadsArgLambda) of
          (Nothing, Nothing) -> return (Right False, e)
          (_, _) -> let re = PApp (PApp (onFoldRetF $ PPrj cE "fold" fAs) fLam app1As) (PVar j jAs) app2As
                    in return (Left True, re)

    threadReturn _ (onDirectReturnBranch [0]   -> isThreadRet) e@(tag -> ELambda _)     = return (isThreadRet, e)
    threadReturn _ (onDirectReturnBranch [0]   -> isThreadRet) e@(tag -> EOperate OApp) = return (isThreadRet, e)
    threadReturn _ (onDirectReturnBranch [1]   -> isThreadRet) e@(tag -> EOperate OSeq) = return (isThreadRet, e)

    threadReturn _ (onDirectReturnBranch [1]   -> isThreadRet) e@(tag -> ELetIn  _)     = return (isThreadRet, e)
    threadReturn _ (onDirectReturnBranch [1]   -> isThreadRet) e@(tag -> EBindAs _)     = return (isThreadRet, e)
    threadReturn _ (onDirectReturnBranch [1,2] -> isThreadRet) e@(tag -> ECaseOf _)     = return (isThreadRet, e)
    threadReturn _ (onDirectReturnBranch [1,2] -> isThreadRet) e@(tag -> EIfThenElse)   = return (isThreadRet, e)

    threadReturn _ _ e = return (Right False, e)

    onIndepR l = if any not $ rights l then Right False else Left False

    onDirectReturnBranch branchIds l =
      if any not $ rights (map (l !!) branchIds) then Right False
      else ensureEither (map (l !!) branchIds)

    ensureEither l =
      if all id (lefts l) && all id (rights l)
        then (if null $ rights l then Left else Right) True
        else Right False


inferThreadReturns :: Identifier -> K3 Expression -> (Bool, K3 Expression)
inferThreadReturns i expr = mapThreadReturns annotateFoldE i expr
  where annotateFoldE e = e @+ (inferredEProp "ThreadFoldReturn" Nothing)


{- Mosaic compilation passes -}
mosaicWarmupMapRewrites :: K3 Declaration -> Either String (K3 Declaration)
mosaicWarmupMapRewrites prog = rewriteIndexConstruction prog
  where
    rewriteIndexConstruction p = do
      (init_exprs, np) <- foldProgram extractIndexCtor idF idF Nothing [] p
      mapNamedDeclExpression "compute_warmup_maps" (injectIndexCtor init_exprs) np

    extractIndexCtor acc d@(tag -> DGlobal n@(isSuffixOf "_init_index" -> True) t eOpt) =
      debugExtract n (acc ++ [EC.applyMany (EC.variable n) [EC.unit]], d)

    extractIndexCtor acc d = return (acc, d)

    injectIndexCtor exprs e@(PLam i bodyE as) = debugInject exprs $ PLam i (foldl (flip $ EC.binop OSeq) bodyE exprs) as
    injectIndexCtor _ e = Left $ boxToString $ ["Invalid Mosaic warmup map initialization function: "]
                                            %$ prettyLines e

    debugExtract n (nacc,d) = if True then return (nacc, d)
                              else flip trace (return (nacc,d))
                                     $ boxToString $ ["Mosaic idx: "]
                                                  %$ concatMap (indent 2 . prettyLines) nacc

    debugInject exprs r = if True then return r
                          else flip trace (return r) $ boxToString $ ["Mosaic inject idx: "]
                                                    %$ (concatMap (indent 2 . prettyLines) $ map stripECompare exprs)
                                                    %$ (indent 2 $ prettyLines $ stripECompare r)

    idF a b = return (a,b)

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

pattern PPrjApp2VarSeq i prjId arg1 arg2 <-
  PSeq (PPrjApp2 (PVar i _) prjId _ arg1 arg2 _ _) (PVar ((== i) -> True) _) _

pattern PPrjApp3VarSeq i prjId arg1 arg2 arg3 <-
  PSeq (PPrjApp3 (PVar i _) prjId _ arg1 arg2 arg3 _ _ _) (PVar ((== i) -> True) _) _

pattern PChainLambda1 i j bodyE iAs jAs = PLam i (PLam j bodyE jAs) iAs

pattern PAccumulate i j <- PPrjAppVarSeq i "insert" (PVar j _)

pattern PStreamableTransformerArg i j bodyE iAs jAs <-
  PChainLambda1 i j (id &&& inferAccumulation i -> (bodyE, True)) iAs jAs

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

pattern PDCond2 i j leti o dlV gbF accF zE <-
  PChainLambda1 i j
    (PLetIn
      (PRec ["key", "value"] [PApp gbF (PVar ((== j) -> True) _) _, dlV] _)
      leti
      (PSeq (PPrjApp3 (PVar ((== i) -> True) _)
              "upsert_with"
              _
              (PVar ((== leti) -> True) _)
              (PLam "_" (PRec ["key", "value"]
                              [ (PPrj (PVar ((== leti) -> True) _) "key" _)
                              , (PApp2 accF zE (PVar ((== j) -> True) _) _ _)] _) _)
              (PLam o   (PRec ["key", "value"]
                              [ (PPrj (PVar ((== o) -> True) _) "key" _)
                              , (PApp2 (compareEAST accF -> True) (PPrj (PVar ((== o) -> True) _) "value" _)
                                                                  (PVar ((== j) -> True) _) _ _)] _) _)
              _ _ _)
            (PVar ((== i) -> True) _) _) _
    ) _ _

pattern PSDCond2 i j leti o dlK dlV mvE pvE <-
  PChainLambda1 i j
    (PLetIn
      (PRec ["key", "value"] [dlK, dlV] _)
      leti
      (PSeq (PPrjApp3 (PVar ((== i) -> True) _)
              "upsert_with"
              _
              (PVar ((== leti) -> True) _)
              (PLam "_" (PRec ["key", "value"]
                              [ (PPrj (PVar ((== leti) -> True) _) "key" _), mvE] _) _)
              (PLam o   (PRec ["key", "value"]
                              [ (PPrj (PVar ((== o) -> True) _) "key" _), pvE] _) _)
              _ _ _)
            (PVar ((== i) -> True) _) _) _
    ) _ _
