{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- TODO:
-- HList record labels
-- Determine if purify expression and annotations are needed
-- Main function and arg processing

-- | K3-to-Haskell code generation.
module Language.K3.Codegen.Haskell where

import Control.Arrow
import Control.Applicative
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Function
import Data.List

import qualified Language.Haskell.Exts.Build  as HB
import qualified Language.Haskell.Exts.SrcLoc as HL
import qualified Language.Haskell.Exts.Syntax as HS
import qualified Language.Haskell.Exts.Pretty as HP

import Language.Haskell.Exts.QQ (hs,dec,ty)

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Codegen
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Codegen.Common


data CodeGenerationError = CodeGenerationError String deriving (Eq, Show)

type SymbolCounters      = [(Identifier, Int)]

-- | Metadata to generate a message dispatcher.
--   The boolean indicates whether trigger returns a monadic action.
type TriggerDispatchSpec = [(Identifier, HS.Type, Bool)]

type RecordSpec     = [(Identifier, HS.Type, Maybe HS.Exp)] -- TODO: separate lifted vs regular attrs.
type RecordSpecs    = [(Identifier, RecordSpec)]

data AnnotationState = AnnotationState { annotationSpecs  :: RecordSpecs
                                       , compositionSpecs :: RecordSpecs }

type CGState = (SymbolCounters, TriggerDispatchSpec, RecordSpecs, AnnotationState)

-- | The code generation monad. This supports CG errors, and stateful operation
--   for symbol generation and type-directed declarations (e.g., record and collection types)
type CodeGeneration = EitherT CodeGenerationError (State CGState)

data HaskellEmbedding
    = HProgram      HS.Module
    | HDeclarations [HS.Decl]
    | HExpression   HS.Exp
    | HType         HS.Type
    | HNoRepr
    deriving (Eq, Show)

{- Intermediate code generation representations -}
data Embedding
    = Pure          CGStructure
    | PartialAction CGStructure
    | Action        CGStructure
  deriving (Eq, Show)

data CGStructure
    = SValue
    | SOption       Embedding
    | SIndirection  Embedding
    | SFunction     Embedding
    | STuple        [Embedding]
    | SRecord       [(Identifier, Embedding)]
  deriving (Eq, Show)

type CGExpr = (HS.Exp, Embedding)


-- | An initial code generation state
emptyCGState :: CGState
emptyCGState = ([], [], [], AnnotationState [] [])

-- | Runs the code generation monad, yielding a possibly erroneous result, and a final state.
runCodeGeneration :: CGState -> CodeGeneration a -> (Either CodeGenerationError a, CGState)
runCodeGeneration s = flip runState s . runEitherT

{- Code generation state accessors -}
getSymbolCounters :: CGState -> SymbolCounters
getSymbolCounters (x,_,_,_) = x

modifySymbolCounters :: (SymbolCounters -> (a, SymbolCounters)) -> CGState -> (a, CGState)
modifySymbolCounters f (w,x,y,z) = (r, (nw, x, y, z))
  where (r, nw) = f w

getTriggerDispatchSpecs :: CGState -> TriggerDispatchSpec
getTriggerDispatchSpecs (_,x,_,_) = x

modifyTriggerDispatchSpecs :: (TriggerDispatchSpec -> TriggerDispatchSpec) -> CGState -> CGState
modifyTriggerDispatchSpecs f (w,x,y,z) = (w, f x, y, z)

getRecordSpecs :: CGState -> RecordSpecs
getRecordSpecs (_,_,x,_) = x

modifyRecordSpecs :: (RecordSpecs -> RecordSpecs) -> CGState -> CGState
modifyRecordSpecs f (w,x,y,z) = (w, x, f y, z)

getAnnotationState :: CGState -> AnnotationState
getAnnotationState (_,_,_,x) = x

modifyAnnotationState :: (AnnotationState -> AnnotationState) -> CGState -> CGState
modifyAnnotationState f (w,x,y,z) = (w, x, y, f z)


{- Code generator monad methods -}

throwCG :: CodeGenerationError -> CodeGeneration a
throwCG = Control.Monad.Trans.Either.left

gensymCG :: Identifier -> CodeGeneration Identifier
gensymCG n = lift $ state $ modifySymbolCounters $ \c -> modifyAssoc c n incrSym
  where incrSym Nothing  = (n ++ show (0::Int), Just 1)
        incrSym (Just i) = (n ++ show i, Just $ i+1)

getTriggerDispatchCG :: CodeGeneration TriggerDispatchSpec
getTriggerDispatchCG = get >>= return . getTriggerDispatchSpecs

modifyTriggerDispatchCG :: (TriggerDispatchSpec -> TriggerDispatchSpec) -> CodeGeneration ()
modifyTriggerDispatchCG f = modify $ modifyTriggerDispatchSpecs f

addRecordSpec :: Identifier -> RecordSpec -> CodeGeneration ()
addRecordSpec n r = modify $ modifyRecordSpecs $ nub . ((n,r):)

lookupRecordSpec :: Identifier -> CodeGeneration (Maybe RecordSpec)
lookupRecordSpec n = get >>= return . lookup n . getRecordSpecs

getAnnotationSpec :: Identifier -> CodeGeneration (Maybe RecordSpec)
getAnnotationSpec n = get >>= return . lookup n . annotationSpecs . getAnnotationState

getCompositionSpec :: Identifier -> CodeGeneration (Maybe RecordSpec)
getCompositionSpec n = get >>= return . lookup n . compositionSpecs . getAnnotationState

modifyAnnotationSpecs :: (RecordSpecs -> RecordSpecs) -> CodeGeneration ()
modifyAnnotationSpecs f =
  modify $ modifyAnnotationState (\s -> AnnotationState (f $ annotationSpecs s) (compositionSpecs s))

modifyCompositionSpecs :: (RecordSpecs -> RecordSpecs) -> CodeGeneration ()
modifyCompositionSpecs f =
  modify $ modifyAnnotationState (\s -> AnnotationState (annotationSpecs s) (f $ compositionSpecs s))


{- Symbols and identifiers -}

-- | Sanitizes a K3 identifier to a backend compatible identifier.
sanitize :: Identifier -> Identifier
sanitize = id

programId :: Identifier
programId = "__progId"

engineModuleAliasId :: Identifier
engineModuleAliasId = "E"

engineValueTypeId :: Identifier
engineValueTypeId = "String"

collectionClassId :: Identifier
collectionClassId = "Collection"

triggerTypeId :: Identifier
triggerTypeId = "Trigger"

triggerConId :: Identifier
triggerConId = "Trigger"

triggerHandleFnId :: Identifier
triggerHandleFnId = "handle"

triggerImplFnId :: Identifier
triggerImplFnId = "impl"


{- Collection identifiers -}

collectionEmptyConPrefixId :: Identifier
collectionEmptyConPrefixId = "empty"

collectionInitConPrefixId :: Identifier
collectionInitConPrefixId = "c"

collectionCopyConPrefixId :: Identifier
collectionCopyConPrefixId = "copy"

collectionCopyDataConPrefixId :: Identifier
collectionCopyDataConPrefixId = "copyWithData"

compositionReprId :: Identifier -> Identifier
compositionReprId n = "C"++n++"_Repr"

compositionTypeId :: Identifier -> Identifier
compositionTypeId n = "C"++n

collectionDataSegId :: Identifier
collectionDataSegId = "__data"

recordReprId :: Identifier -> Identifier
recordReprId n = n



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


{- Error constructors -}
qualLoadError :: CodeGeneration a
qualLoadError = throwCG $ CodeGenerationError "Invalid structure qualifier and load combination"

qualStoreError :: CodeGeneration a
qualStoreError = throwCG $ CodeGenerationError "Invalid structure qualifier and store combination"

blockError :: CodeGeneration a
blockError = throwCG $ CodeGenerationError "Invalid do expression"

seqDoError :: CodeGeneration a
seqDoError = throwCG $ CodeGenerationError "Invalid do-expression arguments"


{- Type embedding -}

argType :: K3 Type -> CodeGeneration (K3 Type)
argType (tag &&& children -> (TFunction, [a,_])) = return a
argType _ = throwCG $ CodeGenerationError "Invalid function type"

returnType :: K3 Type -> CodeGeneration (K3 Type)
returnType (tag &&& children -> (TFunction, [_,r])) = return r
returnType _ = throwCG $ CodeGenerationError "Invalid function type"

namedType :: Identifier -> HS.Type
namedType n = HS.TyCon . HS.UnQual $ HB.name n

qNamedType :: Identifier -> Identifier -> HS.Type
qNamedType m n = HS.TyCon . HS.Qual (HS.ModuleName m) $ HB.name n

tyApp :: Identifier -> HS.Type -> HS.Type
tyApp n t = HS.TyApp (namedType n) t

qTyApp :: Identifier -> Identifier -> HS.Type -> HS.Type
qTyApp m n t = HS.TyApp (qNamedType m n) t

unitType :: HS.Type
unitType = [ty| () |]

boolType :: HS.Type
boolType = [ty| Bool |]

stringType :: HS.Type
stringType = [ty| String |]

maybeType :: HS.Type -> HS.Type
maybeType t = tyApp "Maybe" t

tupleType :: [HS.Type] -> HS.Type
tupleType = HS.TyTuple HS.Boxed

recordType :: Identifier -> HS.Type
recordType = namedType

indirectionType :: HS.Type -> HS.Type
indirectionType t = tyApp "MVar" t

listType :: HS.Type -> HS.Type
listType = HS.TyList

funType :: HS.Type -> HS.Type -> HS.Type
funType a r = HS.TyFun a r

triggerType :: HS.Type -> HS.Type
triggerType a = tyApp triggerTypeId $ funType a $ engineType unitType

collectionType :: Identifier -> HS.Type -> HS.Type
collectionType comboId elType = HS.TyApp (namedType $ compositionTypeId comboId) elType

monadicType :: Identifier -> HS.Type -> HS.Type
monadicType m t = tyApp m t

qMonadicType :: Identifier -> Identifier -> HS.Type -> HS.Type
qMonadicType mdule m t = qTyApp mdule m t

ioType :: HS.Type -> HS.Type
ioType t = monadicType "IO" t

engineType :: HS.Type -> HS.Type
engineType t = HS.TyApp (qMonadicType engineModuleAliasId "EngineM" $ namedType engineValueTypeId) t

typ :: K3 Type -> CodeGeneration HaskellEmbedding
typ t = typ' t >>= return . HType

typ' :: K3 Type -> CodeGeneration HS.Type
typ' (tag -> TBool)       = return [ty| Bool    |]
typ' (tag -> TByte)       = return [ty| Word8   |]
typ' (tag -> TInt)        = return [ty| Int     |]
typ' (tag -> TReal)       = return [ty| Double  |]
typ' (tag -> TString)     = return [ty| String  |]
typ' (tag -> TAddress)    = return [ty| Address |]

typ' (tag &&& children -> (TOption,[x])) = typ' x >>= return . maybeType
typ' (tag -> TOption)                    = throwCG $ CodeGenerationError "Invalid option type"

typ' (tag &&& children -> (TTuple, ch)) = mapM typ' ch >>= return . tupleType

typ' (tag &&& children -> (TIndirection, [x])) = typ' x >>= return . indirectionType
typ' (tag -> TIndirection)                     = throwCG $ CodeGenerationError "Invalid indirection type"

typ' (tag &&& children -> (TFunction, [a,r])) = mapM typ' [a,r] >>= \chT -> return $ funType (head chT) (last chT)
typ' (tag -> TFunction)                       = throwCG $ CodeGenerationError "Invalid function type"

typ' (tag &&& children -> (TSink, [x])) = typ' x >>= return . flip funType unitType
typ' (tag -> TSink)                     = throwCG $ CodeGenerationError "Invalid sink type"

typ' (tag &&& children -> (TTrigger, [x])) = typ' x >>= return . flip funType unitType
typ' (tag -> TTrigger)                     = throwCG $ CodeGenerationError "Invalid trigger type"

typ' t@(tag &&& children -> (TCollection, [x])) =
  case annotationComboIdT $ annotations t of
    Just comboId -> typ' x >>= return . collectionType comboId
    Nothing      -> typ' x >>= return . listType

typ' (tag -> TCollection) = throwCG $ CodeGenerationError "Invalid collection type"

-- TODO
typ' t@(tag &&& children -> (TRecord ids, ch)) = do
  sig     <- signature t
  specOpt <- lookupRecordSpec sig
  return . const (recordType sig) =<< maybe (trackRecordSpec sig) (const $ return ()) specOpt
  where trackRecordSpec sig = mapM typ' ch >>= addRecordSpec sig . map (\(x,y) -> (x,y,Nothing)) . zip ids

-- TODO
typ' (tag -> TBuiltIn TSelf)      = throwCG $ CodeGenerationError "Cannot generate Self type"
typ' (tag -> TBuiltIn TContent)   = throwCG $ CodeGenerationError "Cannot generate Content type"
typ' (tag -> TBuiltIn THorizon)   = throwCG $ CodeGenerationError "Cannot generate Horizon type"
typ' (tag -> TBuiltIn TStructure) = throwCG $ CodeGenerationError "Cannot generate Structure type"

typ' _ = throwCG $ CodeGenerationError "Cannot generate Haskell type"



{- Expressions -}

unifyStructure :: CGStructure -> CGStructure -> CGStructure
unifyStructure a b = if a == b then a else SValue

mkCG :: HS.Exp -> Embedding -> CGExpr
mkCG = (,)

mkPCG :: HS.Exp -> CGStructure -> CGExpr
mkPCG e s = mkCG e $ Pure s

mkPACG :: HS.Exp -> CGStructure -> CGExpr
mkPACG e s = mkCG e $ PartialAction s

mkACG :: HS.Exp -> CGStructure -> CGExpr
mkACG e s = mkCG e $ Action s

mkPValue :: HS.Exp -> CGExpr
mkPValue = flip mkPCG $ SValue

mkAValue :: HS.Exp -> CGExpr
mkAValue = flip mkACG $ SValue

getHSExp :: CGExpr -> HS.Exp
getHSExp = fst

getESub :: Embedding -> CGStructure
getESub = \case
  Pure s          -> s
  PartialAction s -> s
  Action s        -> s

getCGEmbed :: CGExpr -> Embedding
getCGEmbed = snd

getCGSub :: CGExpr -> CGStructure
getCGSub = getESub . getCGEmbed

getHSExpAndSub :: CGExpr -> (HS.Exp, CGStructure)
getHSExpAndSub = getHSExp &&& getCGSub

isPure :: CGExpr -> Bool
isPure (_, Pure _) = True
isPure _           = False

isAction :: CGExpr -> Bool
isAction (_, Action _) = True
isAction _             = False

isPureOrPartial :: CGExpr -> Bool
isPureOrPartial (_, Pure _)          = True
isPureOrPartial (_, PartialAction _) = True
isPureOrPartial _                    = False

getPure :: CGExpr -> CodeGeneration HS.Exp
getPure (expr, Pure _) = return expr
getPure _           = throwCG $ CodeGenerationError "Invalid pure expression"


{- Error messages -}
pureError :: String -> CodeGeneration a
pureError msg = throwCG . CodeGenerationError $ "Invalid pure expression when " ++ msg

actionError :: String -> CodeGeneration a
actionError msg = throwCG . CodeGenerationError $ "Invalid action expression when " ++ msg

argError :: String -> CodeGeneration a
argError n = throwCG . CodeGenerationError $ "Invalid arguments for " ++ n

pureOrPartialArgError :: String -> CodeGeneration a
pureOrPartialArgError n =
  throwCG . CodeGenerationError $ "Invalid pure expression or partial action in " ++ n

actionArgError :: String -> CodeGeneration a
actionArgError n = throwCG . CodeGenerationError $ "Invalid action expression in " ++ n


{- View pattern helpers -}
action :: CGExpr -> Maybe HS.Exp
action (expr, m) = case m of
  Action _ -> Just expr
  _        -> Nothing

pureOrPartial :: CGExpr -> Maybe HS.Exp
pureOrPartial (expr, m) = case m of
  Pure _          -> Just expr
  PartialAction _ -> Just expr
  _               -> Nothing

partialOrAction :: CGExpr -> Maybe HS.Exp
partialOrAction (expr, m) = case m of
  PartialAction _ -> Just expr
  Action _        -> Just expr
  _               -> Nothing


{- CG expression construction -}
type SpliceFunction      = HS.Exp -> CGStructure -> CodeGeneration CGExpr
type MultiSpliceFunction = [(HS.Exp, CGStructure)] -> CodeGeneration CGExpr

pvarCG :: Identifier -> CGExpr
pvarCG n = mkPValue $ HB.var $ HB.name n

avarCG :: Identifier -> CGExpr
avarCG n = mkAValue $ HB.var $ HB.name n

applySpliceF :: SpliceFunction -> CGExpr -> CodeGeneration CGExpr
applySpliceF f = uncurry f . getHSExpAndSub

buildSpliceF :: (HS.Exp -> HS.Exp) -> (CGStructure -> Embedding) -> SpliceFunction
buildSpliceF eF sF = \e s -> return $ mkCG (eF e) (sF s)

buildMultiSpliceF :: ([HS.Exp] -> HS.Exp) -> ([CGStructure] -> Embedding) -> MultiSpliceFunction
buildMultiSpliceF eF sF = \esl -> let (e,s) = unzip esl in return $ mkCG (eF e) (sF s)

buildMultiValueSpliceF :: ([HS.Exp] -> HS.Exp) -> MultiSpliceFunction
buildMultiValueSpliceF eF = buildMultiSpliceF eF $ const $ Pure SValue

-- | Builds a CG expression given a splice function that accepts a pure expression.
spliceE :: Identifier -> CGExpr -> SpliceFunction -> CodeGeneration CGExpr
spliceE _ ce@(pureOrPartial -> Just _) spliceF = spliceValueE ce spliceF
spliceE n ce spliceF = spliceActionE n ce spliceF

-- | Builds a CG expression, given a pair of splice functions for type-directed splicing.
--   The first splice function assumes a pure expression, while the second assumes an action expression.
spliceEWithAction :: SpliceFunction -> SpliceFunction -> CGExpr -> CodeGeneration CGExpr
spliceEWithAction pureF actionF ce = case pureOrPartial ce of
    Just _  -> spliceValueE ce pureF
    _       -> applySpliceF actionF ce

-- | Builds a CG expression by splicing multiple subexpressions. Action subexpressions are
--   bound to names with the given prefix.
spliceManyE :: Identifier -> MultiSpliceFunction -> [CGExpr] -> CodeGeneration CGExpr
spliceManyE n f args = case all isPureOrPartial args of
    True  -> spliceManyValuesE f args
    False -> do {  (stmts, argE) <- foldM (bindName n) ([],[]) args;
                   f argE >>= prefixDoE stmts }

  where bindName _ (stmtAcc, argAcc) ce@(pureOrPartial -> Just _) = extractValueE ce >>= \case
          (ctxt, ce') | isPure ce' -> return (stmtAcc++ctxt, argAcc++[getHSExpAndSub ce'])
                      | otherwise  -> pureError "binding a value for splicing"

        bindName n' (stmtAcc, argAcc) ce = gensymCG n' >>= \n'' ->
          let nN       = HB.name n''
              (nV,nPV) = (HB.var nN, HB.pvar nN)
          in return (stmtAcc++[HB.genStmt HL.noLoc nPV $ getHSExp ce], argAcc++[(nV, getCGSub ce)])


{- Name bindings. These construct partial action expressions. -}

-- | Bind a K3 value to an immutable variable. This constructs a partial action.
bindE :: Identifier -> CGExpr -> CodeGeneration CGExpr
bindE n ce = ensureActionE ce >>= \ce' -> flip applySpliceF ce' $
             \e s -> return $ mkPACG [hs| do { ((nN)) <- $e; $nE } |] s
  where (nN, nE) = (HB.name n, HB.var $ HB.name n)

-- | Store a K3 value as a mutable binding.
doStoreE :: Identifier -> CGExpr -> CodeGeneration CGExpr
doStoreE n = spliceEWithAction pureF actionF
  where pureF   e s = bindE n $ mkACG [hs| liftIO ( newMVar ( $e ) ) |]   s
        actionF e s = bindE n $ mkACG [hs| ( $e ) >>= liftIO . newMVar |] s

-- | Read a mutable expression.
doLoadE :: Identifier -> CGExpr -> CodeGeneration CGExpr
doLoadE n = spliceEWithAction pureF actionF
  where pureF   e s = bindE n $ mkACG [hs| liftIO ( readMVar ( $e ) ) |]   s
        actionF e s = bindE n $ mkACG [hs| ( $e ) >>= liftIO . readMVar |] s

-- | Read from a mutable variable.
doLoadNameE :: Identifier -> CodeGeneration CGExpr
doLoadNameE n = gensymCG n >>= flip doLoadE (pvarCG n)

-- | Read from a K3 variable, with annotations indicating qualifiers.
loadNameE :: [Annotation Expression] -> Identifier -> CodeGeneration CGExpr
loadNameE anns n = case (qualifier anns, load anns) of
  (Nothing, False)         -> return $ avarCG n
  (Just PImmutable, False) -> return $ avarCG n
  (Just PMutable, True)    -> doLoadNameE n
  _ -> qualLoadError

-- | Bind to a K3 variable, with annotations indicating qualifiers.
storeE :: [Annotation Expression] -> Identifier -> CGExpr -> CodeGeneration CGExpr
storeE anns n ce = case (qualifier anns, store anns) of
  (Nothing, False)         -> bindE n ce
  (Just PImmutable, False) -> bindE n ce
  (Just PMutable, True)    -> doStoreE n ce
  _ -> qualStoreError


{- Generic Do-expression constructors. -}

-- | Prepends a sequence of do-block statements to an expression.
prefixDoE :: [HS.Stmt] -> CGExpr -> CodeGeneration CGExpr
prefixDoE [] ce = return ce

prefixDoE context (pureOrPartial &&& getCGSub -> (Just (HS.Do stmts), s)) =
  return $ mkPACG (HB.doE $ context ++ stmts) s

prefixDoE context (pureOrPartial &&& getCGSub -> (Just e, s)) =
  return $ mkPACG (HB.doE $ context ++ [HB.qualStmt e]) s

prefixDoE context (action &&& getCGEmbed -> (Just (HS.Do stmts), m)) =
  return $ mkCG (HB.doE $ context ++ stmts) m

prefixDoE context (action &&& getCGEmbed -> (Just e, m)) =
  return $ mkCG (HB.doE $ context ++ [HB.qualStmt e]) m

prefixDoE _ _ = argError "prefixDoE"


{- Do-expression manipulation on pure expressions and partial actions. -}

-- | Extracts a pure return value and a context from an expression.
--   For do-expressions this is the qualifier expression and the context is
--   the remainder of the stmts. For any other expression, the context is empty.
extractValueE :: CGExpr -> CodeGeneration ([HS.Stmt], CGExpr)
extractValueE (pureOrPartial -> Just (HS.Do [])) = blockError

extractValueE (pureOrPartial &&& getCGSub -> (Just (HS.Do stmts), s)) =
  case (init stmts, last stmts) of
    ([], HS.Qualifier e) -> return ([], mkPCG e s)
    (c,  HS.Qualifier e) -> return (c,  mkPCG e s)
    _ -> blockError

extractValueE (pureOrPartial &&& getCGSub -> (Just e, s)) = return ([], mkPCG e s)
extractValueE _ = pureOrPartialArgError "extractValueE"


-- | Sequences two pure or partial action expressions.
--   Note this discards the first expression if it is pure.
seqDoE :: CGExpr -> CGExpr -> CodeGeneration CGExpr
seqDoE e e2 = do
  (eStmts,  _)      <- extractValueE e
  (e2Stmts, e2RetE) <- extractValueE e2
  case (eStmts, e2Stmts) of
    ([],[]) -> return e2RetE
    _ -> return $ flip mkCG (PartialAction $ getCGSub e2RetE) $
          HB.doE $ eStmts ++ e2Stmts ++ [HB.qualStmt $ getHSExp e2RetE]

spliceValueE :: CGExpr -> SpliceFunction -> CodeGeneration CGExpr
spliceValueE e spliceF = extractValueE e >>= \case
  (ctxt, e') | isPure e' && ctxt == [] -> applySpliceF spliceF e'
             | isPure e'               -> applySpliceF spliceF e' >>= prefixDoE ctxt
             | otherwise               -> pureError "chaining a value"


-- | Operator and data constructors applied to evaluated children.
--   TODO: alpha renaming for conflicting binding names during merge
spliceManyValuesE :: MultiSpliceFunction -> [CGExpr] -> CodeGeneration CGExpr
spliceManyValuesE f subE = do
  (contexts, argsE) <- mapM extractValueE subE >>= return . unzip
  if all isPure argsE then do
    retE <- f $ map getHSExpAndSub argsE
    prefixDoE (concat contexts) retE
  else pureError "applying a function"


{- Monadic action manipulation -}

-- | Construct a complete action from a pure or partial action expression.
ensureActionE :: CGExpr -> CodeGeneration CGExpr
ensureActionE (pureOrPartial -> Just (HS.Do [])) = blockError

ensureActionE (pureOrPartial &&& getCGSub -> (Just (HS.Do stmts), s)) =
  case (init stmts, last stmts) of
    ([], HS.Qualifier e) -> return $ mkACG [hs| return $e |] s
    (c,  HS.Qualifier e) -> return $ mkACG (HS.Do $ c ++ [ HS.Qualifier [hs| return $e |] ]) s
    _ -> blockError

ensureActionE (pureOrPartial &&& getCGSub -> (Just e, s)) =
  return $ mkACG [hs| return ( $e ) |] s

ensureActionE e = return e

-- | Composes two monadic actions with a custom sequencing function.
spliceActionEWithSeq :: Identifier -> CGExpr -> SpliceFunction
                     -> ((HS.Exp, CGStructure) -> (HS.Exp, CGStructure) -> CodeGeneration CGExpr)
                     -> CodeGeneration CGExpr
spliceActionEWithSeq n e spliceF composeF =
  composeActions =<< ((,) <$> (ensureActionE e) <*> (applySpliceF spliceF (pvarCG n) >>= ensureActionE))
  where composeActions (ae, ae')
          | isAction ae && isAction ae' = uncurry composeF $ ((,) `on` getHSExpAndSub) ae ae'
          | otherwise                   = actionError "splicing two actions"

-- | Compose two actions as a do-block, using an action constructor to support dependencies.
spliceActionE :: Identifier -> CGExpr -> SpliceFunction -> CodeGeneration CGExpr
spliceActionE n e spliceF = spliceActionEWithSeq n e spliceF doSeqF
  where doSeqF (ae,_) (ae',s) = return $ mkACG [hs| do { ((nN)) <- $ae; $ae' } |] s
        nN = HB.name n

-- | Compose two actions inline, using an action constructor to support dependencies.
spliceInlineActionE :: Identifier -> CGExpr -> SpliceFunction -> CodeGeneration CGExpr
spliceInlineActionE n e spliceF = spliceActionEWithSeq n e spliceF inlineSeqF
  where inlineSeqF (ae,_) (ae',s) = return $ mkACG [hs| $ae >>= ( \((nN)) -> $ae' ) |] s
        nN = HB.name n


{- Message passing -}
sendFnE :: HS.Exp
sendFnE = HB.qvar (HS.ModuleName engineModuleAliasId) $ HB.name "send"


{- Records embedding -}

-- TODO: change for statically typed records
-- | Ad-hoc record constructor
buildRecordE :: [Identifier] -> [(HS.Exp, CGStructure)] -> CodeGeneration CGExpr
buildRecordE names subE = do
  lSym <- gensymCG "__lbl"
  rSym <- gensymCG "__record"

  (_, namedLblE, recFieldE) <- accumulateLabelAndFieldE names subE
  lblCE <- spliceManyE lSym (buildMultiValueSpliceF HB.listE) namedLblE
  recCE <- foldM concatRecField (mkPValue [hs| emptyRecord |]) recFieldE
  spliceManyE rSym mkRecord [lblCE, recCE]

  where
    mkRecord [(lblE,_), (recE,_)] = return $ mkPValue $ HB.tuple [lblE, recE]
    mkRecord _                    = argError "mkRecord"

    accumulateLabelAndFieldE names' subE' =
      foldM buildRecordFieldE (mkPValue [hs| firstLabel |], [], []) $ zip names' subE'

    buildRecordFieldE (keyCE, nlblAcc, recAcc) (n,(ce,s)) = do
      [kSym, kvSym, nkSym] <- mapM gensymCG ["__key", "__kv", "__nk"]
      nextKeyE  <- spliceE nkSym keyCE $
                      \keyE _ -> return $ mkPCG [hs| nextLabel $keyE |] s

      namedKeyE <- spliceManyE kSym
                      (buildMultiValueSpliceF HB.tuple)
                      [mkPValue (HB.strE n), keyCE]

      fieldE    <- spliceManyE kvSym mkField [keyCE, mkPCG ce s]
      return (nextKeyE, nlblAcc++[namedKeyE], recAcc++[fieldE])

    mkField [(keyE,_), (ce,_)] = return $ mkPValue [hs| $keyE .=. $ce |]
    mkField _                  = argError "mkField"

    concatRecField a b = gensymCG "__crf" >>= \sym -> spliceManyE sym extendRecord [a, b]
    extendRecord [(a,_), (b,_)] = return $ mkPValue [hs| $b .*. $a |]
    extendRecord _      = argError "extendRecord"

-- TODO: change for statically typed records
-- | Record field lookup expression construction
recordFieldLookupE :: SpliceFunction -> Identifier -> CGExpr -> CodeGeneration CGExpr
recordFieldLookupE accessF n rCE = do
  fSym <- gensymCG "__field"
  aSym <- gensymCG "__access"
  fCE  <- spliceE fSym rCE $
            \re s -> return $ mkCG [hs| ( __n__ $re ) |] $
                     case s of
                       SRecord s' -> maybe (Pure SValue) id $ lookup n s'
                       _          -> Pure SValue
  spliceE aSym fCE accessF


{- Default values -}

-- | Default values for specific types
defaultValue :: K3 Type -> CodeGeneration HaskellEmbedding
defaultValue t = defaultValue' t >>= return . HExpression . getHSExp

defaultValue' :: K3 Type -> CodeGeneration CGExpr
defaultValue' (tag -> TBool)       = return $ mkPValue [hs| False |]
defaultValue' (tag -> TByte)       = return $ mkPValue [hs| (0 :: Word8) |]
defaultValue' (tag -> TInt)        = return $ mkPValue [hs| (0 :: Int) |]
defaultValue' (tag -> TReal)       = return $ mkPValue [hs| (0 :: Double) |]
defaultValue' (tag -> TString)     = return $ mkPValue [hs| "" |]
defaultValue' (tag -> TAddress)    = return $ mkPValue [hs| defaultAddress |]

defaultValue' (tag &&& children -> (TOption, [x])) = defaultValue' x >>= spliceEWithAction pureF actionF
  where pureF   e s = return $ mkPCG [hs| Just $e |]                   $ SOption $ Pure s
        actionF e s = return $ mkACG [hs| ( $e  ) >>= return . Just |] $ SOption $ Pure s

defaultValue' (tag -> TOption) = throwCG $ CodeGenerationError "Invalid option type"

defaultValue' (tag &&& children -> (TIndirection, [x])) = defaultValue' x >>= spliceEWithAction pureF actionF
  where pureF   e s = return $ mkACG [hs| liftIO ( newMVar $e ) |]       $ SIndirection $ Pure s
        actionF e s = return $ mkACG [hs| ( $e ) >>= liftIO . newMVar |] $ SIndirection $ Pure s

defaultValue' (tag -> TIndirection) = throwCG $ CodeGenerationError "Invalid indirection type"

defaultValue' (tag &&& children -> (TTuple, ch)) =
  mapM defaultValue' ch >>= spliceManyE "__f" (buildMultiSpliceF HB.tuple $ Pure . STuple . map Pure)

-- TODO
defaultValue' (tag -> TRecord _) = throwCG $ CodeGenerationError "Default records not implemented"

defaultValue' (tag &&& annotations -> (TCollection, anns)) =
  return . mkAValue $ case annotationComboIdT anns of
    Just comboId -> HB.var $ HB.name $ collectionEmptyConPrefixId ++ comboId
    Nothing      -> [hs| return [] |]


defaultValue' (tag -> TFunction) = throwCG $ CodeGenerationError "No default available for a function"
defaultValue' (tag -> TTrigger)  = throwCG $ CodeGenerationError "No default available for a trigger"
defaultValue' (tag -> TSink)     = throwCG $ CodeGenerationError "No default available for a sink"
defaultValue' (tag -> TSource)   = throwCG $ CodeGenerationError "No default available for a source"

defaultValue' _ = throwCG $ CodeGenerationError "Cannot create a type-based default value."

{- Expression code generation -}

unary :: Operator -> K3 Expression -> CodeGeneration CGExpr
unary op e = do
  e' <- expression' e
  case op of
    ONeg -> gensymCG "__neg" >>= \n -> spliceE n e' $ buildSpliceF HS.NegApp Pure
    ONot -> gensymCG "__not" >>= \n -> spliceE n e' $ buildSpliceF (HB.app (HB.function "not")) Pure
    _ -> throwCG $ CodeGenerationError "Invalid unary operator"

binary :: Operator -> K3 Expression -> K3 Expression -> CodeGeneration CGExpr
binary op e e' = do
  eE  <- expression' e
  eE' <- expression' e'
  case op of
    OAdd -> doInfx "__arith" "+"      [eE, eE']
    OSub -> doInfx "__arith" "-"      [eE, eE']
    OMul -> doInfx "__arith" "*"      [eE, eE']
    ODiv -> doInfx "__arith" "/"      [eE, eE'] -- TODO: type based selection of div vs /
    OMod -> doInfx "__arith" "`mod`"  [eE, eE'] -- TODO: type based selection of mod vs mod'
    OAnd -> doInfx "__bool"  "&&"     [eE, eE']
    OOr  -> doInfx "__bool"  "||"     [eE, eE']
    OEqu -> doInfx "__cmp"   "=="     [eE, eE']
    ONeq -> doInfx "__cmp"   "/="     [eE, eE']
    OLth -> doInfx "__cmp"   "<"      [eE, eE']
    OLeq -> doInfx "__cmp"   "<="     [eE, eE']
    OGth -> doInfx "__cmp"   ">"      [eE, eE']
    OGeq -> doInfx "__cmp"   ">="     [eE, eE']
    OSeq -> sequenceActions =<< (,) <$> ensureActionE eE <*> ensureActionE eE'
    OApp -> spliceManyE "__app" applyFn [eE, eE']
    OSnd -> spliceManyE "__snd" doSendE [eE, eE']
    _    -> throwCG $ CodeGenerationError "Invalid binary operator"

  where doInfx n opStr args = gensymCG n >>= \n' -> spliceManyE n' (doOpF True opStr) args
        doOpF infx opStr = if infx then infixBinary opStr else appBinary $ HB.function opStr

        infixBinary op' [(a,_),(b,_)] = return . mkPValue $ HB.infixApp a (HB.op $ HB.sym op') b
        infixBinary _ _              = argError "binary operator"

        appBinary f [(a,_),(b,_)] = return . mkPValue $ HB.appFun f [a,b]
        appBinary _ _     = argError "binary funapp"

        sequenceActions (ae, ae') = return $ mkACG [hs| ( $(getHSExp ae) ) >> ( $(getHSExp ae') ) |] $ getCGSub ae'

        applyFn [(a, fs), (b,_)] = case fs of
          SFunction (Pure s)   -> return $ mkPCG (HB.appFun a [b]) s
          SFunction (Action s) -> return $ mkACG (HB.appFun a [b]) s
          SValue               -> return $ mkPValue $ HB.appFun a [b]
          _                    -> throwCG $ CodeGenerationError "Invalid function structure"

        applyFn _ = argError "funapp"

        doSendE [(targE,_), (msgE,_)] = return $ mkAValue $
          [hs| let (trig, addr) = $targE in
                (liftIO $ ishow $msgE) >>= $sendFnE addr ( __triggerHandleFnId__ trig ) |]

        doSendE _ = argError "message send"


-- | Compiles an expression, yielding a combination of a do-expression of
--   monadic action bindings, and a residual expression.
expression' :: K3 Expression -> CodeGeneration CGExpr

-- | Constants
expression' (tag &&& annotations -> (EConstant c, anns)) =
  constantE c anns
  where
    constantE (CBool b)   _ = return $ mkPValue $ if b then [hs| True |] else [hs| False |]
    constantE (CInt i)    _ = return $ mkPValue $ HB.intE $ toInteger i
    constantE (CByte w)   _ = return $ mkPValue $ HS.Lit $ HS.PrimWord $ toInteger w
    constantE (CReal r)   _ = return $ mkPValue $ HS.Lit $ HS.PrimDouble $ toRational r
    constantE (CString s) _ = return $ mkPValue $ HB.strE s
    constantE (CNone _)   _ = return $ mkPValue $ [hs| Nothing |]

    constantE (CEmpty _) as =
      maybe comboIdErr (collectionCstr . (collectionEmptyConPrefixId++)) $ annotationComboIdE as

    collectionCstr n = return $ avarCG n
    comboIdErr = throwCG $ CodeGenerationError "Invalid combo id for empty collection"

-- | Variables
--   TODO: track structure for global declarations.
expression' (tag &&& annotations -> (EVariable i, anns)) = loadNameE anns i

-- | Unary option constructor
expression' (tag &&& children -> (ESome, [x])) = do
  x' <- expression' x
  n  <- gensymCG "__opt"
  spliceE n x' $ \e s -> return $ mkPCG [hs| Just $e |] $ SOption $ Pure s

expression' (tag -> ESome) = throwCG $ CodeGenerationError "Invalid option expression"

-- | Indirection constructor
expression' (tag &&& children -> (EIndirect, [x])) = do
  x' <- expression' x
  n  <- gensymCG "__ind"
  spliceE n x' $ \e s -> return $ mkACG [hs| liftIO ( newMVar ( $e ) ) |] $ SIndirection $ Pure s

expression' (tag -> EIndirect) = throwCG $ CodeGenerationError "Invalid indirection expression"

-- | Tuple constructor
expression' (tag &&& children -> (ETuple, cs)) = do
  cs' <- mapM expression' cs
  n  <- gensymCG "__tup"
  spliceManyE n (buildMultiSpliceF HB.tuple $ Pure . STuple . map Pure) cs'

-- | Functions
expression' (tag &&& children -> (ELambda i,[b])) = do
  b' <- expression' b
  nb <- if isPure b' then return b' else ensureActionE b'
  return $ mkPCG [hs| \((ni)) -> $(getHSExp nb) |] $ SFunction $ getCGEmbed nb
  where ni = HB.name i

expression' (tag -> ELambda _) = throwCG $ CodeGenerationError "Invalid lambda expression"

-- | Operations
expression' (tag &&& children -> (EOperate otag, cs))
    | otag `elem` [ONeg, ONot], [a] <- cs = unary otag a
    | otherwise, [a, b] <- cs             = binary otag a b
    | otherwise                           = throwCG $ CodeGenerationError "Invalid operator expression"

-- | Let-in expressions
expression' (tag &&& children -> (ELetIn i, [e, b])) = do
  e'       <- expression' e
  b'       <- expression' b
  (n, n')  <- (,) <$> gensymCG "__letE" <*> gensymCG "__letB"
  spliceE n e' $ \ne s -> do
    se <- storeE (annotations e) i $ mkPCG ne s
    spliceE n' se $ \_ _ -> return b'

expression' (tag -> ELetIn _) = throwCG $ CodeGenerationError "Invalid let expression"

-- | Assignments
expression' (tag &&& children -> (EAssign i, [e])) = do
  e' <- expression' e
  n  <- gensymCG "__assign"
  i' <- gensymCG i >>= return . HB.name
  spliceE n e' $ \ae _ -> return $ mkAValue
    [hs| $(HB.var $ HB.name i) >>=
            (\((i')) -> liftIO ( modifyMVar_ $(HB.var i') (const . return $ $ae) ) ) |]

expression' (tag -> EAssign _) = throwCG $ CodeGenerationError "Invalid assignment"

-- | Case-of expressions
expression' (tag &&& children -> (ECaseOf i, [e, s, n])) = do
  e'  <- expression' e
  s'  <- expression' s
  n'  <- expression' n
  sym <- gensymCG "__case"

  case maybe PImmutable structureQualifier $ singleStructure $ annotations e of
    PImmutable -> spliceManyE sym mkICase [e', s', n']
    PMutable   -> mutableCase sym e' s' n'

  where ni = HB.name i

        mutableCase sym eCE sCE nCE = do
          j   <- gensymCG i
          s'' <- ensureActionE sCE
          n'' <- ensureActionE nCE
          spliceManyE sym (mkMCase $ HB.name j) [eCE, s'', n'']

        mkICase [(eE, _), (sE, sS), (nE, nS)] =
          return $ flip mkPCG (if sS == nS then sS else SValue) $
            [hs| case $eE of
                   Just ((ni)) -> $sE
                   Nothing     -> $nE |]

        mkICase _ = argError "mkICase"

        mkMCase x [(eE, _), (sE, sS), (nE, nS)] =
          return $ flip mkACG (if sS == nS then sS else SValue) $
          [hs| case $eE of
                 Just ((x)) -> liftIO ( newMVar ( $(HB.var x) ) ) >>= ( \((ni)) -> $sE )
                 Nothing    -> return $nE |]

        mkMCase _ _ = throwCG $ CodeGenerationError "Invalid mutable case constructor arguments"

expression' (tag -> ECaseOf _) = throwCG $ CodeGenerationError "Invalid case expression"

-- | Bind-as expressions
expression' (tag &&& children -> (EBindAs b, [e, f])) = case b of
    BIndirection i -> do
      e'  <- expression' e
      f'  <- expression' f
      sym <- gensymCG "__bindI"
      case maybe PImmutable structureQualifier $ singleStructure eAnns of
        PImmutable -> doLoadE i e' >>= \eCE -> spliceE sym eCE $ \_ _ -> return f'
        PMutable   -> bindE i e' >>= \eCE -> spliceE sym eCE $ \_ _ -> return f'

    BTuple ts   -> bindTupleFields ts
    BRecord ids -> bindRecordFields ids

  where
    eAnns = annotations e
    defaultStructure l = map (const PImmutable) l
    eStructure l = maybe (defaultStructure l) (map structureQualifier) $ complexStructure eAnns

    -- TODO: simplify with storeE?
    bindTupleFields ids = do
      e'         <- expression' e >>= ensureActionE
      f'         <- expression' f
      sym        <- gensymCG "__bindT"
      tNames     <- mapM renameBinding $ zip ids $ eStructure ids
      mutNames   <- return $ filter (uncurry (/=)) tNames
      mutPat     <- return $ HB.pvarTuple $ map (HB.name . fst) mutNames
      mutVars    <- return $ HB.tuple $ map (\(_,x) -> [hs| liftIO ( newMVar ( $(HB.var $ HB.name x) ) ) |]) mutNames
      tupPat     <- return $ HB.pvarTuple $ map (HB.name . snd) tNames

      bindStmts  <- return $ \eE -> [ HB.genStmt HL.noLoc tupPat eE ]
                                 ++ (if not $ null mutNames then
                                    [HB.genStmt HL.noLoc mutPat [hs| return $mutVars |] ] else [])
                                 ++ [HB.qualStmt $ HB.varTuple $ map (HB.name . fst) $ tNames ]

      spliceE sym e' $ \eE _ -> prefixDoE (bindStmts eE) f'


    renameBinding (n, PImmutable) = return (n, n)
    renameBinding (n, PMutable)   = gensymCG n >>= return . (n,)

    bindRecordFields namePairs = do
      e'              <- expression' e
      f'              <- expression' f
      sym             <- gensymCG "__bindR"
      rNamedStructure <- return $ zip namePairs $ eStructure namePairs
      rBindings       <- bindE recordId e' >>= \rCE -> spliceE sym rCE $
                          \re rs -> foldM (bindRecordField rs) (mkPCG re rs) rNamedStructure
      seqDoE rBindings f'

    bindRecordField rs acc ((x,y), q) = recordField rs q y >>= bindE x >>= seqDoE acc

    recordField rs PImmutable x = recordFieldLookupE (\fe fs -> return $ mkPCG fe fs) x $ recordVarCE rs
    recordField rs PMutable   x = recordFieldLookupE mkMutRecField x $ recordVarCE rs
    (recordId, recordVarCE)     = let x = "__record" in (x, \rs -> mkPCG (HB.var $ HB.name x) rs)

    mkMutRecField fe fs = return $ mkACG [hs| liftIO ( newMVar $fe ) |] fs

expression' (tag -> EBindAs _) = throwCG $ CodeGenerationError "Invalid bind expression"

expression' (tag &&& children -> (EIfThenElse, [p, t, e])) = do
  p' <- expression' p
  t' <- expression' t
  e' <- expression' e
  n  <- gensymCG "__if"
  spliceManyE n mkBranch [p', t', e']
  where mkBranch [(pE,_), (tE, tS), (eE, eS)] =
          return $ flip mkPCG (if tS == eS then tS else SValue) $
            [hs| if ( $pE ) then ( $tE ) else ( $eE ) |]
        mkBranch _ = argError "mkBranch"

expression' (tag &&& children -> (EAddress, [h, p])) = do
  h' <- expression' h
  p' <- expression' p
  n  <- gensymCG "__addr"
  spliceManyE n (buildMultiSpliceF HB.tuple $ Pure . STuple . map Pure) [h', p']

-- | Record constructor
-- TODO: records need heterogeneous lists. Find another encoding (e.g., Dynamic/HList).
-- TODO: record labels used in ad-hoc records
expression' (tag &&& children -> (ERecord is, cs)) = do
  cs' <- mapM expression' cs
  sym <- gensymCG "__record"
  spliceManyE sym (buildRecordE is) cs'

-- | Projections
-- TODO: records need heterogeneous lists. Find another encoding.
expression' (tag &&& children -> (EProject i, [r])) = do
  sym <- gensymCG "__proj"
  r'  <- expression' r
  spliceE sym r' $ \e s -> recordFieldLookupE (\fe fs -> return $ mkPCG fe fs) i $ mkPCG e s

expression' (tag -> EProject _) = throwCG $ CodeGenerationError "Invalid record projection"

-- TODO
expression' (tag -> ESelf) = undefined

expression' _ = throwCG $ CodeGenerationError "Invalid expression"

expression :: K3 Expression -> CodeGeneration HaskellEmbedding
expression e = expression' e >>= return . HExpression . getHSExp


{- Declarations -}

promoteDeclType :: HS.Type -> CGExpr -> CodeGeneration (HS.Type, HS.Exp)
promoteDeclType t e = case getCGEmbed e of
      Pure _          -> return (t, getHSExp e)
      PartialAction _ -> ensureActionE e >>= return . (engineType t,) . getHSExp
      Action _        -> return (engineType t, getHSExp e)

mkNamedDecl :: Identifier -> HS.Exp -> CodeGeneration HaskellEmbedding
mkNamedDecl n initE = return . HDeclarations . (:[]) $ namedVal n initE

mkTypedDecl :: Identifier -> HS.Type -> HS.Exp -> CodeGeneration HaskellEmbedding
mkTypedDecl n nType nInit = return . HDeclarations $ [ typeSig n nType , namedVal n nInit]


mkGlobalDecl :: Identifier -> [Annotation Type] -> HS.Type -> CGExpr -> CodeGeneration HaskellEmbedding
mkGlobalDecl n anns nType nInit = do
  (declType, declInit) <- promoteDeclType nType nInit
  case filter isTQualified anns of
    []           -> mkTypedDecl n declType declInit
    [TImmutable] -> mkTypedDecl n declType declInit
    [TMutable]   -> gensymCG "__decl"
                    >>= \sym -> spliceE sym nInit (\e s -> return $ mkACG [hs| liftIO $ newMVar ( $e ) |] s)
                    >>= mkTypedDecl n (engineType $ indirectionType nType) . getHSExp
    _            -> throwCG $ CodeGenerationError "Ambiguous global declaration qualifier"


globalWithDefault :: Identifier -> [Annotation Type] -> K3 Type -> Maybe (K3 Expression) -> CodeGeneration CGExpr
                  -> CodeGeneration HaskellEmbedding
globalWithDefault n anns t eOpt defaultE = do
    t'    <- typ' t
    e'    <- maybe defaultE expression' eOpt
    n'    <- gensymCG $ "__" ++ n
    litPE <- literal' t
    bsE   <- spliceManyE n' bootstrapE [e', litPE]
    mkGlobalDecl n anns t' bsE

  where
    bootstrapE [(e,s), (le,_)] = return . (flip mkACG s) $
      [hs| getBootstrap $(HB.strE n) >>=
              \x -> case x of { Nothing -> return $e; Just v  -> $le v } |]

    bootstrapE _ = throwCG $ CodeGenerationError "Invalid bootstrap CG arguments"


global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> CodeGeneration HaskellEmbedding
global _ (tag -> TSource) _ = return HNoRepr

-- | Generate builtin functions, e.g., wrappers of the engine API.
global n t@(tag -> TFunction) Nothing  = builtin n t

-- | Functions, like triggers, operate in the EngineM monad.
global n t@(tag -> TFunction) (Just e) = do
  e' <- expression' e
  rtAct <- case getCGSub e' of
            SFunction (Pure _) -> return False
            SFunction _        -> return True
            SValue             -> return False
            _                  -> throwCG $ CodeGenerationError "Invalid function structure"

  at <- argType t >>= typ'
  rt <- returnType t >>= typ'
  t' <- return $ funType at $ if rtAct then engineType rt else rt
  mkGlobalDecl n (annotations t) t' e'


-- TODO: two-level namespaces.
global n t@(tag &&& children -> (TCollection, [_])) eOpt =
  case composedName of
    Nothing      -> initializeCollection eOpt
    Just comboId -> testAndAddComposition comboId >> initializeCollection eOpt

  where anns            = annotations t
        annotationNames = namedTAnnotations anns
        composedName    = annotationComboIdT anns

        testAndAddComposition comboId =
          getCompositionSpec comboId >>= \case
            Nothing -> composeAnnotations comboId >>= \spec -> modifyCompositionSpecs (spec:)
            Just _  -> return ()

        composeAnnotations comboId = mapM lookupAnnotation annotationNames >>= composeSpec comboId
        lookupAnnotation n' = getAnnotationSpec n' >>= maybe (invalidAnnotation n') return

        composeSpec comboId annSpecs =
          let cSpec  = concat annSpecs
              cNames = concat $ map (map (\(x,_,_) -> x)) annSpecs
          in
          if length cSpec == (length $ nub cNames)
          then return (comboId, cSpec)
          else compositionError n

        initializeCollection eOpt' = globalWithDefault n anns t eOpt' (defaultValue' t)

        compositionError n'  = throwCG . CodeGenerationError $ "Overlapping attribute names in collection " ++ n'
        invalidAnnotation n' = throwCG . CodeGenerationError $ "Invalid annotation " ++ n'

global _ (tag -> TCollection) _ = throwCG . CodeGenerationError $ "Invalid global collection"

global n t eOpt = globalWithDefault n (annotations t) t eOpt (defaultValue' t)


-- | Triggers are implemented as functions that operate in the EngineM monad.
trigger :: Identifier -> K3 Type -> K3 Expression -> CodeGeneration HaskellEmbedding
trigger n t e = do
  sym  <- gensymCG "__trig"
  e'   <- expression' e
  t'   <- typ' t

  rtAct <- case getCGSub e' of
            SFunction (Pure _) -> return False
            SFunction _        -> return True
            SValue             -> return False
            _                  -> throwCG $ CodeGenerationError "Invalid function structure"

  impl <- spliceE sym e' (\te ts -> return $ mkPCG (triggerImpl (HB.strE n) te) ts)
  (pt, pImpl) <- promoteDeclType (triggerType t') impl

  void $ modifyTriggerDispatchCG ((n,t',rtAct):)
  return . HDeclarations $ [ typeSig n $ engineType pt, namedVal n pImpl ]

  where
    triggerImpl hndlE implE =
      [hs| return $ $(HB.appFun (HS.Con $ HS.UnQual $ HB.name triggerConId) [hndlE, implE]) |]


annotation :: Identifier -> [TypeVarDecl] -> [AnnMemDecl] -> CodeGeneration ()
annotation n _ {- vdecls -} memberDecls =
  -- TODO: consider: should we do anything with "vdecls", the declared type
  --       variables and their upper bounds?
  foldM (initializeMember n) [] memberDecls >>= modifyAnnotationSpecs . (:) . (n,)
  where initializeMember annId acc m = annotationMember annId m >>= return . maybe acc ((acc++) . (:[]))

-- TODO: handle member mutability qualifier
-- TODO: distinguish lifted and regular attributes.
-- TODO: use default values for attributes specified without initializers.
annotationMember :: Identifier -> AnnMemDecl
                 -> CodeGeneration (Maybe (Identifier, HS.Type, Maybe HS.Exp))
annotationMember annId = \case
  Lifted    Provides n t (Just e) _   -> memberSpec n t e
  Attribute Provides n t (Just e) _   -> memberSpec n t e
  Lifted    Provides n t Nothing  _   -> builtinLiftedAttribute annId n t >>= return . Just
  Attribute Provides n t Nothing  _   -> builtinAttribute annId n t >>= return . Just
  _                                   -> return Nothing
  where memberSpec n t e = do
          e' <- expression' e
          t' <- typ' t
          (t'', e'') <- promoteDeclType t' e'
          return $ Just (n, t'', Just e'')


{- Builtins -}

genNotifier :: Identifier -> String -> CodeGeneration HaskellEmbedding
genNotifier n evt = mkTypedDecl n
  [ty| Identifier -> (Trigger a, Address) -> E.EngineM String () |]
  [hs| \cid (trig, addr) ->
          (liftIO $ ishow ()) >>= E.attachNotifier_ cid $(HB.strE evt) . (addr, handle trig,) |]

builtin :: Identifier -> K3 Type -> CodeGeneration HaskellEmbedding

-- TODO: due to use of identityWD, generate ser/deser with ishow/iread at call sites for these builtins.
builtin "openBuiltin" _ = mkTypedDecl "openBuiltin"
  [ty| Identifier -> Identifier -> String -> E.EngineM String () |]
  [hs| \cid builtinId format -> E.openBuiltin cid builtinId identityWD |]

builtin "openFile" _ = mkTypedDecl "openFile"
  [ty| Identifier -> String -> String -> String -> E.EngineM String () |]
  [hs| \cid path format mode -> E.openFile cid path identityWD Nothing mode |]

builtin "openSocket" _ = mkTypedDecl "openSocket"
  [ty| Identifier -> Address -> String -> String -> E.EngineM String () |]
  [hs| \cid addr format mode -> E.openSocket cid addr identityWD Nothing mode |]

builtin "close" _ = mkTypedDecl "close"
  [ty| Identifier -> E.EngineM String () |]
  [hs| \cid -> E.close cid |]

builtin "registerFileDataTrigger"     _ = genNotifier "registerFileDataTrigger"     "data"
builtin "registerFileCloseTrigger"    _ = genNotifier "registerFileCloseTrigger"    "close"
builtin "registerSocketAcceptTrigger" _ = genNotifier "registerSocketAcceptTrigger" "accept"
builtin "registerSocketDataTrigger"   _ = genNotifier "registerSocketDataTrigger"   "data"
builtin "registerSocketCloseTrigger"  _ = genNotifier "registerSocketCloseTrigger"  "close"

builtin (channelMethod -> ("HasRead", Just n)) _  =
  let fnId = n++"HasRead"
      fnTy = funType unitType $ engineType boolType
  in return . HDeclarations $
    [ typeSig fnId fnTy
    , constFun fnId $
        HB.app (HB.qvar (HS.ModuleName engineModuleAliasId) $ HB.name "hasRead") $ HB.strE n ]

-- TODO: engine error on invalid read.
builtin (channelMethod -> ("Read", Just n)) t = do
  fnId <- return $ n++"Read"
  rt   <- returnType t >>= typ'
  fnTy <- return $ funType unitType $ engineType rt
  return . HDeclarations $
    [ typeSig fnId fnTy
    , constFun fnId $
        [hs| $(HB.qvar (HS.ModuleName engineModuleAliasId) $ HB.name "doRead") $(HB.strE n)
                >>= liftIO . iread >>= return . maybe (error "Invalid read value") id |] ]

builtin (channelMethod -> ("HasWrite", Just n)) _ =
  let fnId = n++"HasWrite"
      fnTy = funType unitType $ engineType boolType
  in return . HDeclarations $
    [ typeSig fnId fnTy
    , constFun fnId $
        HB.app (HB.qvar (HS.ModuleName engineModuleAliasId) $ HB.name "hasWrite") $ HB.strE n ]

builtin (channelMethod -> ("Write", Just n)) t = do
  argId <- return $ "v"
  fnId  <- return $ n++"Write"
  at    <- argType t >>= typ'
  fnTy  <- return $ funType at $ engineType unitType
  return . HDeclarations $
    [ typeSig fnId fnTy
    , simpleFun fnId argId $
        [hs| ishow $(HB.var $ HB.name argId)
              >>= $(HB.qvar (HS.ModuleName engineModuleAliasId) $ HB.name "doWrite") $(HB.strE n) |] ]

-- TODO: extra builtins
builtin "random" _         = return HNoRepr
builtin "randomFraction" _ = return HNoRepr
builtin "hash" _           = return HNoRepr
builtin "range" _          = return HNoRepr

builtin n _ = throwCG . CodeGenerationError $ "Invalid builtin function " ++ n

-- TODO: duplicated from interpreter. Factorize.
channelMethod :: String -> (String, Maybe String)
channelMethod x =
  case find (flip isSuffixOf x) ["HasRead", "Read", "HasWrite", "Write"] of
    Just y -> (y, stripSuffix y x)
    Nothing -> (x, Nothing)
  where stripSuffix sfx lst = maybe Nothing (Just . reverse) $ stripPrefix (reverse sfx) (reverse lst)



builtinLiftedAttribute :: Identifier -> Identifier -> K3 Type -> CodeGeneration (Identifier, HS.Type, Maybe HS.Exp)
builtinLiftedAttribute "Collection" "peek" t = typ' t >>= \t' ->
  return ("peek",   t', Just
    [hs| (\() -> liftIO (readMVar self) >>= return . head . getData) |])

builtinLiftedAttribute "Collection" "insert" t = typ' t >>= \t' ->
  return ("insert", t', Just
    [hs| (\x -> liftIO $ modifyMVar_ self (return . modifyData (++[x]))) |])

builtinLiftedAttribute "Collection" "delete" t = typ' t >>= \t' ->
  return ("delete", t', Just
    [hs| (\x -> liftIO $ modifyMVar_ self (return . modifyData $ delete x)) |])

builtinLiftedAttribute "Collection" "update" t = typ' t >>= \t' ->
  return ("update", t', Just
    [hs| (\x y -> liftIO $ modifyMVar_ self (return . modifyData (\l -> (delete x l)++[y]))) |])

builtinLiftedAttribute "Collection" "combine" t = typ' t >>= \t' ->
  return ("combine", t', Just $
    [hs| (\c' -> liftIO . newMVar =<<
         ((\x y -> copyWithData x (getData x ++ getData y))
           <$> liftIO (readMVar self) <*> liftIO (readMVar c'))) |])

builtinLiftedAttribute "Collection" "split" t = typ' t >>= \t' ->
  return ("split", t', Just $
    [hs| (\() -> liftIO (readMVar self) >>=
         (\r -> let l = getData r
                    threshold = 10
                    (x,y) = if length l <= threshold
                            then (l, []) else splitAt (length l `div` 2) l
                in liftIO ((,) <$> newMVar (copyWithData r x) <*> newMVar (copyWithData r y)) )) |])

-- TODO: these implementations assume the transformer function is in the EngineM monad.
-- Lambda implementations do not guarantee this (blindly using ensureActionE on lambdas
-- is not ideal for non-transformer function application).
builtinLiftedAttribute "Collection" "iterate" t = typ' t >>= \t' ->
  return ("iterate", t', Just [hs| (\f -> liftIO (readMVar self) >>= mapM_ f . getData) |])

builtinLiftedAttribute "Collection" "map" t = typ' t >>= \t' ->
  return ("map", t', Just $
    [hs| (\f -> liftIO (readMVar self) >>=
         (\r -> mapM f (getData r) >>= (liftIO . newMVar $ copyWithData r))) |])

builtinLiftedAttribute "Collection" "filter" t = typ' t >>= \t' ->
  return ("filter", t', Just $
    [hs| (\f -> liftIO (readMVar self) >>=
         (\r -> filterM f (getData r) >>= (liftIO . newMVar $ copyWithData r))) |])

builtinLiftedAttribute "Collection" "fold" t = typ' t >>= \t' ->
  return ("fold", t', Just $
    [hs| (\f accInit -> liftIO (readMVar self) >>= foldM f accInit . getData) |])

-- TODO: key-value record construction for resulting collection
builtinLiftedAttribute "Collection" "groupBy" t = typ' t >>= \t' ->
  return ("groupBy", t', Just $
    [hs| (\gb f accInit ->
            liftIO (readMVar self) >>=
            (\r -> foldM (\m x -> let k = gb x
                                  in M.insert k $ f (M.findWithDefault accInit k m) x)
                         M.empty . getData
                   >>= liftIO . newMVar . copyWithData r . M.toList)) |])

builtinLiftedAttribute "Collection" "ext" t = typ' t >>= \t' ->
  return ("ext", t', Just $
    [hs| (\f -> liftIO (readMVar self) >>=
         (\r -> mapM f . getData >>= liftIO . newMVar . copyWithData r . concat)) |])

builtinLiftedAttribute _ _ _ = throwCG $ CodeGenerationError "Builtin lifted attributes not implemented"


builtinAttribute :: Identifier -> Identifier -> K3 Type -> CodeGeneration (Identifier, HS.Type, Maybe HS.Exp)
builtinAttribute _ _ _ = throwCG $ CodeGenerationError "Builtin attributes not implemented"


{- Top-level functions -}

declaration :: K3 Declaration -> CodeGeneration HaskellEmbedding
declaration decl = case (tag &&& children) decl of
  (DGlobal n t eO, ch) -> do
    d     <- global n t eO >>= extractDeclarations
    decls <- mapM (declaration >=> extractDeclarations) ch
    return . HDeclarations $ d ++ concat decls

  (DTrigger n t e, cs) -> do
    HDeclarations d <- trigger n t e
    decls           <- mapM (declaration >=> extractDeclarations) cs
    return . HDeclarations $ d ++ concat decls

  (DRole _, ch) -> do
    decls <- mapM (declaration >=> extractDeclarations) ch
    return . HDeclarations $ concat decls
      -- TODO: qualify names?

  (DDataAnnotation n [] vdecls members, []) ->
    annotation n vdecls members >> return HNoRepr

  _ -> throwCG $ CodeGenerationError "Invalid declaration"

  where extractDeclarations (HDeclarations decls) = return decls
        extractDeclarations HNoRepr               = return []
        extractDeclarations _ = throwCG $ CodeGenerationError "Invalid declaration"


{- Record and collection generation -}

generateRecords :: CodeGeneration [HS.Decl]
generateRecords =
  get >>= mapM generateRecord . getRecordSpecs
  where
    generateRecord (n, spec) =
      let typeId       = recordReprId n
          typeDeriving = [(HS.UnQual $ HB.name "Eq", [])]
          recFields    = map generateRecFieldTy spec
          recDecl      = HS.RecDecl (HB.name typeId) $ recFields
          typeConDecl  = [HS.QualConDecl HL.noLoc [] [] recDecl]
          typeDecl     = HS.DataDecl HL.noLoc HS.DataType [] (HB.name typeId) [] typeConDecl typeDeriving
      in
      return typeDecl

    generateRecFieldTy (n, t, _) = ([HB.name n], HS.UnBangedTy t)


-- TODO: add support for self-referencing in initializer expressions in non-function attributes.
generateCollectionCompositions :: CodeGeneration [HS.Decl]
generateCollectionCompositions =
  get >>= mapM generateComposition . compositionSpecs . getAnnotationState >>= return . concat
  where
    generateComposition (comboId, spec) =
      let n            = comboId -- TODO: name manipulation as necessary (e.g., shortening)

          contentTyId  = "a"
          contentTyVar = HS.TyVar $ HB.name contentTyId

          reprId       = compositionReprId n
          reprDeriving = [(HS.UnQual $ HB.name "Eq", [])]
          reprTyVars   = [HS.UnkindedVar $ HB.name contentTyId]
          recFields    = map generateRecFieldTy spec ++ [([HB.name collectionDataSegId], HS.UnBangedTy $ listType contentTyVar)]
          reprRecDecl  = HS.RecDecl (HB.name reprId) $ recFields
          reprConDecl  = [HS.QualConDecl HL.noLoc [] [] reprRecDecl]
          reprDecl     = HS.DataDecl HL.noLoc HS.DataType [] (HB.name reprId) reprTyVars reprConDecl reprDeriving

          typeId       = compositionTypeId n
          typeDecl     = HS.TypeDecl HL.noLoc (HB.name typeId) [] $ indirectionType (namedType reprId)
          typeExpr     = tyApp typeId contentTyVar

          dataFieldExpr varE      = [hs| $(HB.var $ HB.name collectionDataSegId) $varE |]
          selfVarE                = HB.var $ HB.name "cmv"
          bindImplicits fieldExpr = return [hs| ( let self = $selfVarE in $fieldExpr ) |]
          fieldExprs fieldF       = mapM (\x -> fieldF x >>= bindImplicits) spec

          reprExpr dataE fieldF = fieldExprs fieldF >>= return . (++[dataE]) >>=
            return . HB.appFun (HS.Con $ HS.UnQual $ HB.name reprId)

          consExpr dataE fieldF = reprExpr dataE fieldF >>= \recE ->
            return [hs| do { cmv <- __emptyConId__ ;
                             void . liftIO $ modifyMVar_ cmv ( const $ $recE );
                             return cmv } |]

          initConId    = collectionInitConPrefixId++n
          initArg      = "l"
          initVar      = HB.var $ HB.name initArg
          initCon      = consExpr initVar generateRecFieldExpr >>= \implE -> return $
                         [ typeSig initConId $ funType (listType contentTyVar) $ engineType typeExpr
                         , simpleFun initConId initArg implE ]

          emptyConId   = collectionEmptyConPrefixId++n
          emptyCon     = consExpr HB.eList generateRecFieldExpr >>= \implE -> return $
                         [ typeSig emptyConId $ engineType typeExpr
                         , namedVal emptyConId implE ]

          copyConId    = collectionCopyConPrefixId++n
          copyArg      = "c"
          copyVar      = HB.var $ HB.name copyArg
          copyCon      = consExpr (dataFieldExpr copyVar) (copyFieldExpr copyVar) >>= \implE -> return $
                         [ typeSig copyConId $ funType typeExpr $ engineType typeExpr
                         , simpleFun copyConId copyArg implE ]

          cDataConId   = collectionCopyDataConPrefixId++n
          cDArgs       = ["c", "elems"]
          cDVars       = map (HB.var . HB.name) cDArgs
          cDataCon     = consExpr (cDVars !! 1) (copyFieldExpr $ cDVars !! 0) >>= \implE -> return $
                         [ typeSig cDataConId $ funType typeExpr $ funType (listType contentTyVar) $ engineType typeExpr
                         , multiFun cDataConId cDArgs implE ]

          cInstDecl =
            let (cReprId, cReprVar) = ("repr", HB.var $ HB.name "repr")
                (elemsId, elemsVar) = ("elems", HB.var $ HB.name "elems")
                modifyFId           = "modifyF"

                modifyE = HB.appFun (HB.function cDataConId) [cReprVar, [hs| (__modifyFId__ $ __collectionDataSegId__ repr) |]]
                copyE   = HB.appFun (HB.function cDataConId) [cReprVar, elemsVar]
            in
            return $
              [HS.InstDecl HL.noLoc [] (HS.UnQual $ HB.name collectionClassId)
                [tyApp reprId contentTyVar, listType contentTyVar]
                [ HS.InsDecl $ simpleFun "getData" cReprId $ [hs| __collectionDataSegId__ repr |]
                , HS.InsDecl $ multiFun  "modifyData" [cReprId, modifyFId] modifyE
                , HS.InsDecl $ multiFun  "copyWithData" [cReprId, elemsId] copyE] ]

          constructorDecls = (\v w x y z -> v ++ w ++ x ++ y ++ z) <$>
            initCon <*> emptyCon <*> copyCon <*> cDataCon <*> cInstDecl

      in ([reprDecl, typeDecl] ++) <$> constructorDecls

    generateRecFieldTy (n, t, _)  = ([HB.name n], HS.UnBangedTy t)
    generateRecFieldExpr (_, _, Just e)  = return e
    generateRecFieldExpr (_, _, Nothing) = throwCG $ CodeGenerationError "No expression found for record field initializer"

    -- | Function fields are not copied between records to ensure correct handling of self-referencing.
    copyFieldExpr _ (_, HS.TyFun _ _, Just e)  = return e
    copyFieldExpr _ (_, HS.TyFun _ _, Nothing) = throwCG $ CodeGenerationError "No expression found for record function field"
    copyFieldExpr varE (n,_,_)                 = return [hs| $(HB.var $ HB.name n) $varE |]


{- Literal parser generation -}

-- | Builds a function for parsing for a K3 literal of the given type.
literal' :: K3 Type -> CodeGeneration CGExpr
literal' lt = immutL lt
  where
    litF e = return $ mkPCG e $ SFunction $ Action SValue

    immutL (tag -> TBool)    = litF [hs| L.bool |]
    immutL (tag -> TByte)    = litF [hs| L.byte |]
    immutL (tag -> TInt)     = litF [hs| L.int |]
    immutL (tag -> TReal)    = litF [hs| L.real |]
    immutL (tag -> TString)  = litF [hs| L.string |]
    immutL (tag -> TAddress) = litF [hs| L.address |]

    immutL (details -> (TOption, [x], _)) =
      ((,) <$> gensymCG "__optL" <*> immutL x) >>=
        (\(sym, sub) -> spliceE sym sub $ \e _ -> litF [hs| L.option $e |])

    immutL (tag -> TOption) = throwCG $ CodeGenerationError "Invalid option type"

    immutL (details -> (TIndirection, [x], _)) =
      ((,) <$> gensymCG "__indL" <*> immutL x) >>=
        (\(sym, sub) -> spliceE sym sub $ \e _ -> litF [hs| L.indirection $e |])

    immutL (tag -> TIndirection) = throwCG $ CodeGenerationError "Invalid indirection type"

    immutL (details -> (TTuple, ch, _)) =
      ((,) <$> gensymCG "__tupL" <*> mapM immutL ch) >>=
        (\(sym, sub) -> flip (spliceManyE sym) sub $
          buildMultiSpliceF (\el -> [hs| L.tuple ( $(tupleL el) ) |])
                            (Pure . SFunction . Action . STuple . map Pure))

    immutL (tag -> TTuple) = throwCG $ CodeGenerationError "Invalid tuple type"

    immutL t@(details -> (TRecord ids, ch, _)) =
      ((,,) <$> gensymCG "__recL" <*> signature t <*> mapM immutL ch) >>=
        (\(sym, recSig, sub) -> flip (spliceManyE sym) sub $
          buildMultiSpliceF (\el -> [hs| L.record ( $(recordL (recordCstr recSig) $ zip ids el) ) |])
                            (Pure . SFunction . Action . SRecord . map (Pure <$>) . zip ids))

      where recordCstr recSig = HS.Con . HS.UnQual . HB.name $ recordReprId recSig

    immutL (tag -> TRecord _) = throwCG $ CodeGenerationError "Invalid record type"

    immutL (details -> (TCollection, [x], anns)) =
      ((,) <$> gensymCG "__collL" <*> immutL x) >>=
        (\(sym, sub) -> spliceE sym sub (\e _ ->
                          collectionL anns e >>= \e' -> litF [hs| L.anyCollection $e' |]))

    immutL (tag -> TCollection) = throwCG $ CodeGenerationError "Invalid collection type"

    immutL _ = throwCG $ CodeGenerationError "Cannot construct type from literal"


{- Template Haskell methods for recursive literal construction -}

-- | Builds a lambda expression to construct a tuple, given a list of
--   individual field constructors.
tupleL :: [HS.Exp] -> HS.Exp
tupleL constructors = HB.lamE HL.noLoc [lPat] $ HB.letE decls $ HB.tuple exps
  where n = length constructors
        (decls, exps) = aux n ([],[])
        aux 0 (dAcc, eAcc) = (dAcc, eAcc)
        aux i (dAcc, eAcc) = aux (i-1) (dAcc++[fieldD $ n-i], eAcc++[fieldE $ n-i])

        fieldE i = HB.var $ fieldName i
        fieldD i = letDecl i $
                     [hs| $(constructors !! i) ( $lArg !! $(HB.intE $ toInteger i) ) |]

        letDecl i e = HB.nameBind HL.noLoc (fieldName i) e

        fieldName i = HB.name $ "__f"++(show i)
        lArg        = HB.var . HB.name $ "__argL"
        lPat        = HB.pvar . HB.name $ "__argL"


-- | Builds a lambda expression to construct a Haskell data type, given
--   a Haskell constructor name, and a list of field names and constructors.
--   The data type constructor is applied to arguments based on their appearance order in the list.
recordL :: HS.Exp -> [(Identifier, HS.Exp)] -> HS.Exp
recordL recordCstr fieldCstrs = HB.lamE HL.noLoc [lPat] $ HB.letE decls $ HB.appFun recordCstr exps
  where
        decls = fst $ foldl (\(dAcc, i) (n,e) -> (dAcc++[fieldD n e i], i+1)) ([], 0::Int) fieldCstrs
        exps  = fst $ foldl (\(eAcc, i) _ -> (eAcc++[fieldE i], i+1)) ([],0::Int) fieldCstrs

        fieldE i     = HB.var $ fieldName i
        fieldD n e i = letDecl i $ [hs| maybe $errorE $e $ lookup $(HB.strE n) $lArg |]

        letDecl i e = HB.nameBind HL.noLoc (fieldName i) e
        errorE      = [hs| E.throwEngineError $ E.EngineError "Invalid field" |]

        fieldName i = HB.name $ "__f"++(show i)
        lArg        = HB.var . HB.name $ "__namedLitL"
        lPat        = HB.pvar . HB.name $ "__namedLitL"


-- | Builds a lambda expression to construct an empty collection as represented
--   by the code generator.
emptyL :: [Annotation Type] -> CodeGeneration HS.Exp
emptyL anns = case annotationComboIdT anns of
  Nothing      -> mkEmptyF [hs| return [] |]
  Just comboId -> mkEmptyF $ HB.var . HB.name $ collectionEmptyConPrefixId++comboId

  where mkEmptyF e = return $ HB.lamE HL.noLoc [HB.wildcard] e


-- | Builds a lambda expression to construct an initialized collection as
--   represented by the code generator.
collectionL :: [Annotation Type] -> HS.Exp -> CodeGeneration HS.Exp
collectionL anns elemL = case annotationComboIdT anns of
  Nothing      -> mkInitF [hs| mapM ( $elemL ) l |]
  Just comboId -> mkInitF [hs| mapM ( $elemL ) l >>= $(HB.var $ HB.name $ collectionInitConPrefixId++comboId) |]

  where mkInitF e = return $ HB.lamE HL.noLoc [HB.pvar $ HB.name "l"] e


{- Program-level code generation -}

-- | Top-level code generation function, returning a Haskell module.
-- TODO: record label, type and default constructor generation
-- TODO: main, argument processing
generate :: String -> K3 Declaration -> CodeGeneration HaskellEmbedding
generate progName p = declaration p >>= mkProgram
  where
        mkProgram (HDeclarations decls) = programDecls decls
          >>= return . HProgram . HS.Module HL.noLoc (HS.ModuleName $ sanitize progName) pragmas warning expts impts

        mkProgram _ = throwCG $ CodeGenerationError "Invalid program"

        pragmas   = [ HS.LanguagePragma HL.noLoc $ [ HB.name "MultiParamTypeClasses"
                                                   , HB.name "TupleSections"
                                                   , HB.name "FlexibleInstances"] ]
        warning   = Nothing
        expts     = Nothing

        programDecls decls =
          generateRecords >>= \recordDecls ->
          generateCollectionCompositions >>= \comboDecls ->
          generateDispatch >>= return . ((preDecls ++ recordDecls ++ comboDecls ++ decls) ++) . postDecls

        impts = [ imprtDecl "Control.Concurrent"             False Nothing
                , imprtDecl "Control.Concurrent.MVar"        False Nothing
                , imprtDecl "Control.Monad"                  False Nothing
                , imprtDecl "Control.Monad.IO.Class"         False Nothing
                , imprtDecl "Options.Applicative"            False Nothing
                , imprtDecl "Language.K3.Core.Common"        False Nothing
                , imprtDecl "Language.K3.Utils.Pretty"       False Nothing
                , imprtDecl "Language.K3.Runtime.Common"     False Nothing
                , imprtDecl "Language.K3.Runtime.Literal"    False Nothing
                , imprtDecl "Language.K3.Runtime.Engine"     True  (Just engineModuleAliasId)
                , imprtDecl "Language.K3.Runtime.Options"    False Nothing
                , imprtDecl "Data.Map.Lazy"                  True  (Just "M") ]

        preDecls =
          [ typeSig programId stringType,
            namedVal programId $ HB.strE $ sanitize progName

          , [dec| class Collection a b where
                    getData      :: a -> b
                    modifyData   :: a -> (b -> b) -> a
                    copyWithData :: a -> b -> a |]

          , [dec| instance Pretty (Either E.EngineError ()) where
                    prettyLines rts = [show rts] |]

          , [dec| data Trigger a = Trigger { __triggerHandleFnId__ :: Identifier
                                           , __triggerImplFnId__   :: a } |]

          , [dec| type RuntimeStatus = Either E.EngineError () |] ]

        postDecls dispatchDeclOpt =
          [
            [dec| getBootstrap :: String -> E.EngineM String (Maybe (K3 Literal)) |]
          , [dec| getBootstrap n = do
                    engine <- ask
                    case deployment engine of
                      [(addr, bootstrap)] -> lookup n bootstrap
                      _ -> E.throwEngineError $ E.EngineError "Invalid system environment" |]

          , [dec| identityWD :: E.WireDesc String |]
          , [dec| identityWD = E.WireDesc return (return . Just) (E.Delimiter "\n") |]

          , [dec| compiledMsgPrcsr :: E.MessageProcessor () String RuntimeStatus E.EngineError |]
          , [dec| compiledMsgPrcsr = E.MessageProcessor {
                                         E.initialize = initializeRT
                                       , E.process    = processRT
                                       , E.status     = statusRT
                                       , E.finalize   = finalizeRT
                                       , E.report     = reportRT
                                     }
                    where
                      initializeRT _ = atInit ()
                      finalizeRT _   = atExit ()

                      statusRT rts = either Left (Right . Right) rts

                      reportRT (Left err) = liftIO $ print err
                      reportRT (Right _)  = return ()

                      processRT (addr, n, msg) rts = dispatch addr n msg
            |] ]
          ++ (case dispatchDeclOpt of
                Nothing -> []
                Just dispatchDecl ->
                  [ [dec| dispatch :: Address -> Identifier -> String -> E.EngineM String RuntimeStatus |]
                  , dispatchDecl ]
              )
          ++
          [ [dec| main = do
                           sysEnv <- liftIO $ execParser options
                           engine <- E.networkEngine sysEnv identityWD
                           void $ E.runEngineM (E.runEngine compiledMsgPrcsr sysEnv ()) engine
                         where options = info (helper <*> sysEnvOptions) $ fullDesc
                                          <> progDesc (__programId__ ++ " K3 binary.")
                                          <> header (__programId__ ++ " K3 binary.")
                |] ]

        (dispatchArgs, dispatchVars) = unzip $ map (\n -> (n, HB.var $ HB.name n)) ["addr", "n", "msg"]
        dispatchNV    = dispatchVars !! 1
        dispatchMsgV  = dispatchVars !! 2

        generateDispatch = do
          alts <- dispatchCaseAlts
          case alts of
            [] -> return Nothing
            _  -> return . Just . multiFun "dispatch" dispatchArgs $ HB.caseE dispatchNV alts

        dispatchCaseAlts = do
          trigIds <- getTriggerDispatchCG
          return $ map (\(n,t,rtAct) -> HB.alt HL.noLoc (HB.strP n) $ HB.paren $ dispatchMsg n t rtAct) trigIds

        dispatchMsg n argT rtAct =
          let trigFn e = if rtAct then [hs| __n__ >>= __triggerImplFnId__ >>= ($ $e) |]
                                  else [hs| __n__ >>= __triggerImplFnId__ >>= (\f -> return $ f $e) |]
          in
          HB.doE
            [ HB.genStmt HL.noLoc (HB.pvar $ HB.name "payload")
                  (HB.paren $ typedExpr [hs| liftIO ( iread $dispatchMsgV ) |] $ engineType $ maybeType argT)

            , HB.qualStmt [hs| case payload of
                                  Nothing -> error "Failed to extract message payload" -- TODO: throw engine error
                                  Just v  -> $(trigFn . HB.var $ HB.name "v") >>= return . Right |]
            ]


compile :: CodeGeneration HaskellEmbedding -> Either String String
compile cg = either (Left . show) (Right . compile') $ fst $ runCodeGeneration emptyCGState cg
  where compile' (HProgram      mdule) = HP.prettyPrint mdule
        compile' (HDeclarations decls) = unlines $ map HP.prettyPrint decls
        compile' (HExpression   expr)  = HP.prettyPrint expr
        compile' (HType         t)     = HP.prettyPrint t
        compile' HNoRepr               = ""



{- Analysis -}

-- | Attaches purity annotations to a K3 expression tree.
purifyExpression :: K3 Expression -> K3 Expression
purifyExpression = undefined



{- Declaration and expression constructors -}

typeSig :: Identifier -> HS.Type -> HS.Decl
typeSig n t = HS.TypeSig HL.noLoc [HB.name n] t

typedExpr :: HS.Exp -> HS.Type -> HS.Exp
typedExpr e t = HS.ExpTypeSig HL.noLoc e t

namedVal :: Identifier -> HS.Exp -> HS.Decl
namedVal n e = HB.nameBind HL.noLoc (HB.name n) e

simpleFun :: Identifier -> Identifier -> HS.Exp -> HS.Decl
simpleFun n a e = HB.simpleFun HL.noLoc (HB.name n) (HB.name a) e

constFun :: Identifier -> HS.Exp -> HS.Decl
constFun n e = HS.FunBind [HS.Match HL.noLoc (HB.name n) [HB.wildcard] Nothing (HS.UnGuardedRhs e) HB.noBinds]

multiFun :: Identifier -> [Identifier] -> HS.Exp -> HS.Decl
multiFun n a e = HB.sfun HL.noLoc (HB.name n) (map HB.name a) (HS.UnGuardedRhs e) HB.noBinds

dataCaseAlt :: Identifier -> [Identifier] -> HS.Exp -> HS.Alt
dataCaseAlt con vars e = HB.alt HL.noLoc (HB.pApp (HB.name con) $ map (HB.pvar . HB.name) vars) e

imprtDecl :: Identifier -> Bool -> Maybe Identifier -> HS.ImportDecl
imprtDecl m q alias = HS.ImportDecl HL.noLoc (HS.ModuleName m) q src pkg (maybe Nothing (Just . HS.ModuleName) alias) specs
  where src   = False
        pkg   = Nothing
        specs = Nothing
