{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | K3-to-Haskell code generation.
module Language.K3.Codegen.Haskell where

import Control.Arrow
import Control.Concurrent.MVar
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.List

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Codegen
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.Haskell.Exts.Build  as HB
import qualified Language.Haskell.Exts.SrcLoc as HL
import qualified Language.Haskell.Exts.Syntax as HS

import Language.Haskell.Exts.QQ (hs)

data CodeGenerationError = CodeGenerationError String

type SymbolCounters = [(Identifier, Int)]
type RecordLabels   = [Identifier]

type CGState = (SymbolCounters, RecordLabels)

-- | The code generation monad. This supports CG errors, and stateful operation
--   for symbol generation and type-directed declarations (e.g., record and collection types)
type CodeGeneration = EitherT CodeGenerationError (State CGState)

data HaskellEmbedding
    = HProgram      HS.Module
    | HDeclaration  HS.Decl
    | HExpression   HS.Exp
    | HType         HS.Type
    deriving (Eq, Show)

-- | Runs the code generation monad, yielding a possibly erroneous result, and a final state.
runCodeGeneration :: CGState -> CodeGeneration a -> (Either CodeGenerationError a, CGState)
runCodeGeneration s = flip runState s . runEitherT

{- Code generation state accessors -}
getSymbolCounters :: CGState -> SymbolCounters
getSymbolCounters (x,_) = x

modifySymbolCounters :: (SymbolCounters -> (a, SymbolCounters)) -> CGState -> (a, CGState)
modifySymbolCounters f (x,y) = (r, (nx, y))
  where (r,nx) = f x

getRecordLabels :: CGState -> RecordLabels
getRecordLabels (_,x) = x

modifyRecordLabels :: (RecordLabels -> RecordLabels) -> CGState -> CGState 
modifyRecordLabels f (x,y) = (x, f y)


{- Code generator monad methods -}

throwCG :: CodeGenerationError -> CodeGeneration a
throwCG = Control.Monad.Trans.Either.left

gensymCG :: Identifier -> CodeGeneration Identifier
gensymCG n = state $ modifySymbolCounters $ \c -> modifyAssoc c n incrSym
  where incrSym Nothing  = (n ++ show (0::Int), 1)
        incrSym (Just i) = (n ++ show i, i+1)

addLabel :: Identifier -> CodeGeneration ()
addLabel n = modify $ modifyRecordLabels $ nub . (n:)


{- Code generation annotations -}

isStructure :: Annotation Expression -> Bool
isStructure (EEmbedding (IOStructure _)) = True
isStructure _ = False

isLoad :: Annotation Expression -> Bool
isLoad (EEmbedding IOLoad) = True
isLoad _ = False

isStore :: Annotation Expression -> Bool
isStore (EEmbedding IOStore) = True
isStore _ = False


{- Type embedding -}

cType :: K3 Type -> HaskellEmbedding
cType _ = undefined

-- | Purity annotation extraction
structureQualifier :: PStructure -> PQualifier
structureQualifier = \case
    PLeaf q      -> q
    PSingle q _  -> q
    PComplex q _ -> q

qualifier :: [Annotation Expression] -> Maybe PQualifier
qualifier anns = case filter isStructure anns of
  []  -> Nothing
  [EEmbedding (IOStructure s)] -> Just $ structureQualifier s
  _   -> error "Ambiguous qualifier"

structure :: [Annotation Expression] -> Maybe PStructure
structure anns = case filter isStructure anns of
  [] -> Nothing
  [EEmbedding (IOStructure s)] -> Just s
  _ -> error "Ambiguous structure"

singleStructure :: [Annotation Expression] -> Maybe PStructure
singleStructure anns = case structure anns of
    Nothing -> Nothing
    Just (PSingle _ optS) -> Just optS
    _ -> error "Invalid single structure"

complexStructure :: [Annotation Expression] -> Maybe [PStructure]
complexStructure anns = case structure anns of
    Nothing -> Nothing
    Just (PComplex _ complexS) -> Just complexS
    _ -> error "Invalid complex structure"


load :: [Annotation Expression] -> Bool
load anns = not . null $ filter isLoad anns

store :: [Annotation Expression] -> Bool
store anns = not . null $ filter isStore anns


qualLoadError :: a
qualLoadError = error "Invalid structure qualifier and load combination" 

qualStoreError :: a
qualStoreError = error "Invalid structure qualifier and store combination" 

blockError :: a
blockError = error "Invalid do expression" 

seqDoError :: a
seqDoError = error "Invalid do-expression arguments"


{- Analysis -}

-- | Attaches purity annotations to a K3 expression tree.
purifyExpression :: K3 Expression -> K3 Expression
purifyExpression = undefined


{- Symbols -}

-- TODO
gensym :: Identifier -> Identifier
gensym = id

{- Load-store wrappers -}

doStoreE :: HS.Exp -> HS.Exp
doStoreE e = [hs| newMVar ( $e ) |]

doBindE :: Identifier -> HS.Exp -> HS.Exp
doBindE n e = [hs| do
                      ((nN)) <- return $e
                      $nE |]
  where (nN, nE) = (HB.name n, HB.var $ HB.name n)

doLoadE :: Identifier -> HS.Exp -> HS.Exp
doLoadE n e = [hs| do
                      ((nN)) <- readMVar ( $e )
                      $nE |]
  where (nN, nE) = (HB.name n, HB.var $ HB.name n)

doLoadNameE :: Identifier -> HS.Exp
doLoadNameE n = HB.doE [bindE, HB.qualStmt rE]
  where r = gensym n
        nE = HB.var $ HB.name n
        rE = HB.var $ HB.name r
        bindE = HB.genStmt HL.noLoc (HB.pvar $ HB.name r) [hs| readMVar $nE |]

loadNameE :: [Annotation Expression] -> Identifier -> HS.Exp
loadNameE anns n = case (qualifier anns, load anns) of
  (Nothing, False)         -> varE
  (Just PImmutable, False) -> varE
  (Just PMutable, True)    -> doLoadNameE n
  _ -> qualLoadError
  where varE = HB.var $ HB.name n

storeE :: [Annotation Expression] -> HS.Exp -> HS.Exp
storeE anns e = case (qualifier anns, store anns) of
  (Nothing, False)         -> e
  (Just PImmutable, False) -> e
  (Just PMutable, True)    -> doStoreE e
  _ -> qualStoreError


{- Do-expression manipulation -}

-- | Extracts variable pattern bindings from a do-expression.
extractVarBindingsE :: HS.Exp -> [HS.Name]
extractVarBindingsE (HS.Do stmts) = concatMap extract stmts
  where extract (HS.Generator _ (HS.PVar n) _) = [n]
        extract _ = []

extractVarBindingsE _ = []

-- | Extracts a return value and a context from an expression. For do-expressions this
--   is the qualifier expression and the context is the remainder of the stmts.
--   For any other expression, the context is empty.
extractReturnE :: HS.Exp -> ([HS.Stmt], HS.Exp)
extractReturnE (HS.Do []) = blockError

extractReturnE (HS.Do stmts) = case (init stmts, last stmts) of
  ([], HS.Qualifier e) -> ([], e)
  (c, HS.Qualifier e)  -> (c, e)
  _ -> blockError

extractReturnE e = ([], e)

-- | Prepends a sequence of do-block statements to an expression.
prefixDoE :: [HS.Stmt] -> HS.Exp -> HS.Exp
prefixDoE [] e = e
prefixDoE context (HS.Do stmts) = HB.doE $ context ++ stmts
prefixDoE context e = HB.doE $ context ++ [HB.qualStmt e]

chainReturnE :: HS.Exp -> (HS.Exp -> HS.Exp) -> HS.Exp
chainReturnE e chainF = case extractReturnE e of
  ([], e')   -> chainF e'
  (ctxt, e') -> prefixDoE ctxt $ chainF e'

seqDoE :: HS.Exp -> HS.Exp -> HS.Exp
seqDoE e e2 = 
  let (eStmts, eRetE)   = extractReturnE e
      (e2Stmts, e2RetE) = extractReturnE e2
  in 
  case (eStmts, e2Stmts) of
    ([],[]) -> e2RetE
    _ -> HB.doE $ eStmts ++ e2Stmts ++ [HB.qualStmt e2RetE]

-- | Operator and data constructors applied to evaluated children.
--   TODO: alpha renaming for conflicting binding names during merge
applyE :: ([HS.Exp] -> HS.Exp) -> [HS.Exp] -> HS.Exp
applyE f subE = prefixDoE (concat contexts) $ f args
  where (contexts, args) = unzip $ map extractReturnE subE

unwrapE :: K3 Expression -> (Expression, [Annotation Expression], [K3 Expression])
unwrapE ((tag &&& annotations) &&& children -> ((e, anns), ch)) = (e, anns, ch)


unary :: Operator -> K3 Expression -> CodeGeneration HS.Exp
unary op e = do
  e' <- expression' e
  return $ case op of
    ONeg -> chainReturnE e' HS.NegApp 
    ONot -> chainReturnE e' $ HB.app (HB.function "not")
    _ -> error "Invalid unary operator"

binary :: Operator -> K3 Expression -> K3 Expression -> CodeGeneration HS.Exp
binary op e e' = do
  eE  <- expression' e
  eE' <- expression' e'
  return $ case op of 
    OAdd -> doBinary True "+"  [eE, eE']
    OSub -> doBinary True "-"  [eE, eE']
    OMul -> doBinary True "*"  [eE, eE']
    ODiv -> doBinary True "/"  [eE, eE']
    OAnd -> doBinary True "&&" [eE, eE']
    OOr  -> doBinary True "||" [eE, eE']
    OEqu -> doBinary True "==" [eE, eE']
    ONeq -> doBinary True "/=" [eE, eE']
    OLth -> doBinary True "<"  [eE, eE']
    OLeq -> doBinary True "<=" [eE, eE']
    OGth -> doBinary True ">"  [eE, eE']
    OGeq -> doBinary True ">=" [eE, eE']
    OApp -> applyE applyFn     [eE, eE']
    OSnd -> undefined -- TODO
    OSeq -> doBinary True ">>" [eE, eE']
    _    -> error "Invalid binary operator"

  where doBinary infx opStr args = applyE (doOpF infx opStr) args
        doOpF infx opStr = if infx then infixBinary opStr else appBinary $ HB.function opStr 

        infixBinary op [a,b] = HB.infixApp a (HB.op $ HB.sym op) b
        infixBinary _ _      = error "Invalid binary operator arguments"

        appBinary f [a,b] = HB.appFun f [a,b]
        appBinary _ _     = error "Invalid binary function app arguments"

        applyFn [a,b] = HB.appFun a [b]
        applyFn _ = error "Invalid function application"


-- | Record embedding
buildRecordE :: [Identifier] -> [HS.Exp] -> HS.Exp
buildRecordE names subE =
  HB.tuple [HB.listE namedLblE, foldl concatRecField [hs| emptyRecord |] recFieldE]
  where
    (_, namedLblE, recFieldE) = accumulateLabelAndFieldE names subE

    accumulateLabelAndFieldE names subE = 
      foldl buildRecordFieldE ([hs| firstLabel |], [], []) $ zip names subE

    buildRecordFieldE (keyE, nlblAcc, recAcc) (n,cE) =
      let namedKeyE = applyE HB.tuple [HB.strE n, keyE]
          fieldE    = [hs| $keyE .=. $cE |]
      in ([hs| nextLabel $keyE |], nlblAcc++[namedKeyE], recAcc++[fieldE])

    concatRecField a b = [hs| $b .*. $a |]

recordFieldLookupE :: HS.Exp -> Identifier -> HS.Exp -> HS.Exp
recordFieldLookupE fE n rE = [hs| maybe (error "Record lookup") $fE $ lookup $(HB.strE n) $rE |]


-- | Compiles an expression, yielding a combination of a do-expression of 
--   monadic action bindings, and a residual expression.
expression' :: K3 Expression -> CodeGeneration HS.Exp

-- | Constants
expression' (tag &&& annotations -> (EConstant c, anns)) =
  return $ constantE c anns
  where 
    constantE (CBool b)   _ = if b then [hs| True |] else [hs| False |]
    constantE (CInt i)    _ = HB.intE $ toInteger i
    constantE (CByte w)   _ = HS.Lit $ HS.PrimWord $ toInteger w
    constantE (CReal r)   _ = HS.Lit $ HS.PrimDouble $ toRational r
    constantE (CString s) _ = HB.strE s
    constantE (CNone _)   _ = [hs| Nothing |]
    constantE (CEmpty _) as = HB.eList -- TODO: annotations

-- | Variables
expression' (tag &&& annotations -> (EVariable i, anns)) = return $ loadNameE anns i

-- | Unary option constructor
expression' (tag &&& children -> (ESome, [x])) = do
  x' <- expression' x
  return $ chainReturnE x' $ \e -> [hs| Just $e |]

expression' (tag -> ESome) = throwCG $ CodeGenerationError "Invalid option expression"

-- | Indirection constructor
expression' (tag &&& children -> (EIndirect, [x])) = do
  x' <- expression' x
  return $ chainReturnE x' $ \e -> [hs| newMVar ( $e ) |]

expression' (tag -> EIndirect) = throwCG $ CodeGenerationError "Invalid indirection expression"

-- | Tuple constructor
expression' (tag &&& children -> (ETuple, cs)) = do
  cs' <- mapM expression' cs
  return $ applyE HB.tuple cs'

-- | Record constructor
-- TODO: records need heterogeneous lists. Find another encoding (e.g., Dynamic/HList).
expression' (tag &&& children -> (ERecord is, cs)) = do
  cs' <- mapM expression' cs
  return $ applyE (buildRecordE is) cs'

-- | Functions
expression' (tag &&& children -> (ELambda i,[b])) = do
  b' <- expression' b
  return $ [hs| \((ni)) -> $b' |]
  where ni = HB.name i

expression' (tag -> ELambda _) = throwCG $ CodeGenerationError "Invalid lambda expression"

-- | Operations
expression' (tag &&& children -> (EOperate otag, cs))
    | otag `elem` [ONeg, ONot], [a] <- cs = unary otag a
    | otherwise, [a, b] <- cs             = binary otag a b
    | otherwise                           = throwCG $ CodeGenerationError "Invalid operator expression"

-- | Projections
-- TODO: records need heterogeneous lists. Find another encoding.
-- TODO: chainReturnE?
expression' (tag &&& children -> (EProject i, [r])) = do
  r' <- expression' r
  return $ recordFieldLookupE (HB.function "id") i r'

expression' (tag -> EProject _) = throwCG $ CodeGenerationError "Invalid record projection"

-- | Let-in expressions
expression' (tag &&& children -> (ELetIn i, [e, b])) = do
  b'       <- expression' b
  e'       <- expression' e
  subE     <- return $ storeE (annotations e) e'
  letDecls <- return [HB.nameBind HL.noLoc (HB.name i) subE]
  return $ HB.letE letDecls b'

expression' (tag -> ELetIn _) = throwCG $ CodeGenerationError "Invalid let expression"

-- | Assignments
-- TODO: optimize if expression 'e' does a lookup of variable 'i'
-- TODO: chainReturnE for e?
expression' (tag &&& children -> (EAssign i, [e])) = do
  e' <- expression' e
  return $ [hs| modifyMVar_ $iE (const $e') |]
  where iE = HB.var $ HB.name i

expression' (tag -> EAssign _) = throwCG $ CodeGenerationError "Invalid assignment"

-- | Case-of expressions
-- TODO: chainReturnE for e?
expression' (tag &&& children -> (ECaseOf i, [e, s, n])) = do
  e' <- expression' e
  s' <- expression' s
  n' <- expression' n

  case maybe PImmutable structureQualifier $ singleStructure $ annotations e of
    PImmutable -> return $ immutableCase e' s' n'
    PMutable   -> gensymCG i >>= return . mutableCase e' s' n' . HB.name

  where ni = HB.name i

        immutableCase eE sE nE = [hs| case $eE of
                                        Just ((ni)) -> $sE
                                        Nothing -> $nE |]

        mutableCase eE sE nE nj = [hs| case $eE of
                                         Just ((nj)) -> let __i__ = newMVar $(HB.var nj) in ( $sE )
                                         Nothing -> $nE |]


expression' (tag -> ECaseOf _) = throwCG $ CodeGenerationError "Invalid case expression"

-- | Bind-as expressions
expression' (tag &&& children -> (EBindAs b, [e, f])) = case b of
    BIndirection i -> do
      e' <- expression' e
      f' <- expression' f
      return $ case maybe PImmutable structureQualifier $ singleStructure eAnns of
        PImmutable -> chainReturnE (doLoadE i e') $ const f'
        PMutable   -> chainReturnE (doBindE i e') $ const f'

    BTuple ts   -> bindTupleFields ts
    BRecord ids -> bindRecordFields ids

  where 
    eAnns = annotations e
    defaultStructure l = map (const PImmutable) l
    eStructure l = maybe (defaultStructure l) (map structureQualifier) $ complexStructure eAnns

    bindTupleFields ids = do
      e'         <- expression' e
      f'         <- expression' f
      tNames     <- mapM renameBinding $ zip ids $ eStructure ids
      mutNames   <- return $ filter (uncurry (/=)) tNames
      mutPat     <- return $ HB.pvarTuple $ map (HB.name . fst) mutNames
      mutVars    <- return $ HB.tuple $ map (\(_,x) -> [hs| newMVar ( $(HB.var $ HB.name x) ) |]) mutNames
      tupPat     <- return $ HB.pvarTuple $ map (HB.name . snd) tNames      
      return $ 
        chainReturnE e'  $ \eE -> 
          flip seqDoE f' $
            HB.doE $   [ HB.genStmt HL.noLoc tupPat [hs| return $eE |] ]
                    ++ (if not $ null mutNames then 
                       [HB.genStmt HL.noLoc mutPat [hs| return $mutVars |] ] else [])
                    ++ [HB.qualStmt $(HB.varTuple $ map (HB.name . fst) $ tNames)]

    renameBinding (n, PImmutable) = return (n, n)
    renameBinding (n, PMutable)   = gensymCG n >>= return . (n,)

    bindRecordFields namePairs = do
      e'              <- expression' e
      f'              <- expression' f
      rNamedStructure <- return $ zip namePairs $ eStructure namePairs
      rBindings       <- foldM bindRecordField (doBindE recordId e') rNamedStructure
      return $ seqDoE rBindings f'

    bindRecordField acc ((a,b), q) = return $ seqDoE acc $ doBindE a $ recordField q b

    recordField PImmutable x  = recordFieldLookupE (HB.function "id") x recordVarE
    recordField PMutable x    = recordFieldLookupE (HB.function "newMVar") x recordVarE
    (recordId, recordVarE)    = let x = "__record" in (x, HB.var $ HB.name x)

expression' (tag -> EBindAs _) = throwCG $ CodeGenerationError "Invalid bind expression"

expression' (tag &&& children -> (EIfThenElse, [p, t, e])) = do
  p' <- expression' p
  t' <- expression' t
  e' <- expression' e
  return $ chainReturnE p' $ \predE -> [hs| if ( $predE ) then ( $t' ) else ( $e' ) |]

expression' (tag &&& children -> (EAddress, [h, p])) = do
  h' <- expression' h
  p' <- expression' p
  return $ applyE HB.tuple [h', p']

expression' (tag -> ESelf) = undefined

expression' _ = throwCG $ CodeGenerationError "Invalid expression"


expression :: K3 Expression -> CodeGeneration HaskellEmbedding
expression e = expression' e >>= return . HExpression

global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> CodeGeneration HaskellEmbedding
global _ _ _ = undefined

trigger :: Identifier -> K3 Type -> K3 Expression -> CodeGeneration HaskellEmbedding
trigger _ _ _ = undefined

declaration :: K3 Declaration -> CodeGeneration HaskellEmbedding
declaration = error "NYI"

-- | Top-level code generation function, returning a Haskell module.
generate :: K3 Declaration -> CodeGeneration HaskellEmbedding
generate _ = error "NYI"

compile :: CodeGeneration HaskellEmbedding -> String
compile _ = error "NYI"