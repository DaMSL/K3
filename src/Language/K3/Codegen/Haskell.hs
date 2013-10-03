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
import qualified Language.Haskell.Exts.Pretty as HP

import Language.Haskell.Exts.QQ (hs,dec,ty)

data CodeGenerationError = CodeGenerationError String deriving (Eq, Show)

type SymbolCounters      = [(Identifier, Int)]
type TriggerDispatchSpec = [(Identifier, HS.Type)]

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
data CGExpr
    = Pure          HS.Exp
    | PartialAction HS.Exp
    | Action        HS.Exp

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
  where (r,nw) = f w

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
gensymCG n = state $ modifySymbolCounters $ \c -> modifyAssoc c n incrSym
  where incrSym Nothing  = (n ++ show (0::Int), 1)
        incrSym (Just i) = (n ++ show i, i+1)

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

-- TODO: duplicate of interpreter. Move into common utils.
annotationNamesT :: [Annotation Type] -> [Identifier]
annotationNamesT anns = map extractId $ filter isTAnnotation anns
  where extractId (TAnnotation n) = n
        extractId _ = error "Invalid named annotation"

annotationNamesE :: [Annotation Expression] -> [Identifier]
annotationNamesE anns = map extractId $ filter isEAnnotation anns
  where extractId (EAnnotation n) = n
        extractId _ = error "Invalid named annotation"

annotationComboId :: [Identifier] -> Identifier
annotationComboId annIds = intercalate "_" annIds

annotationComboIdT :: [Annotation Type] -> Maybe Identifier
annotationComboIdT (annotationNamesT -> [])  = Nothing
annotationComboIdT (annotationNamesT -> ids) = Just $ annotationComboId ids

annotationComboIdE :: [Annotation Expression] -> Maybe Identifier
annotationComboIdE (annotationNamesE -> [])  = Nothing
annotationComboIdE (annotationNamesE -> ids) = Just $ annotationComboId ids


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


signature :: K3 Type -> CodeGeneration Identifier
signature (tag -> TBool)        = return "P"
signature (tag -> TByte)        = return "B"
signature (tag -> TInt)         = return "N"
signature (tag -> TReal)        = return "D"
signature (tag -> TString)      = return "S"
signature (tag -> TAddress)     = return "A"

signature (tag &&& children -> (TOption, [x])) = signature x >>= return . ("O" ++)

signature (tag &&& children -> (TTuple, ch)) =
  mapM signature ch >>= return . (("T" ++ (show $ length ch)) ++) . concat

signature (tag &&& children -> (TRecord ids, ch)) =
  mapM signature ch 
  >>= return . intercalate "_" . map (\(x,y) -> (sanitize x) ++ "_" ++ y) . zip ids
  >>= return . (("R" ++ (show $ length ch)) ++)

signature (tag &&& children -> (TIndirection, [x])) = signature x >>= return . ("I" ++)
signature (tag &&& children -> (TCollection, [x]))  = signature x >>= return . ("C" ++)

signature (tag &&& children -> (TFunction, [a,r]))  = signature a >>= \s -> signature r >>= return . (("F" ++ s) ++)
signature (tag &&& children -> (TSink, [x]))        = signature x >>= return . ("K" ++)
signature (tag &&& children -> (TSource, [x]))      = signature x >>= return . ("U" ++)

signature (tag &&& children -> (TTrigger, [x])) = signature x >>= return . ("G" ++)

-- TODO
signature (tag -> TBuiltIn _)   = throwCG $ CodeGenerationError "Cannot create type signature for builtins"

signature (tag -> _)  = throwCG $ CodeGenerationError "Invalid type found when constructing a signature"


-- TODO
typeOfSignature :: Identifier -> CodeGeneration (K3 Type)
typeOfSignature n = throwCG $ CodeGenerationError "Cannot reconstruct signatures yet."


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


{- Error messages -}
pureError :: String -> CodeGeneration a
pureError msg = throwCG . CodeGenerationError $ "Invalid pure expression when " ++ msg

actionError :: String -> CodeGeneration a
actionError msg = throwCG . CodeGenerationError $ "Invalid action expression when " ++ msg

pureOrPartialArgError :: String -> CodeGeneration a
pureOrPartialArgError n =
  throwCG . CodeGenerationError $ "Invalid pure expression or partial action in " ++ n

actionArgError :: String -> CodeGeneration a
actionArgError n = throwCG . CodeGenerationError $ "Invalid action expression in " ++ n


{- View pattern helpers -}
pureOrPartial :: CGExpr -> Maybe HS.Exp
pureOrPartial = \case
  Pure e          -> Just e
  PartialAction e -> Just e
  _               -> Nothing

partialOrAction :: CGExpr -> Maybe HS.Exp
partialOrAction = \case
  PartialAction e -> Just e
  Action e        -> Just e
  _               -> Nothing

isPure :: CGExpr -> Bool
isPure (Pure _) = True
isPure _        = False

isPureOrPartial :: CGExpr -> Bool
isPureOrPartial (Pure _) = True
isPureOrPartial (PartialAction _) = True
isPureOrPartial _ = False

getPure :: CGExpr -> CodeGeneration HS.Exp
getPure (Pure e) = return e
getPure _ = throwCG $ CodeGenerationError "Invalid pure expression"

getHSExpression :: CGExpr -> HS.Exp
getHSExpression = \case
  Pure e          -> e
  PartialAction e -> e
  Action e        -> e


{- CG expression construction -}

-- | Builds a CG expression given a splice function that accepts a pure expression.
spliceE :: Identifier -> CGExpr -> (HS.Exp -> CodeGeneration CGExpr) -> CodeGeneration CGExpr
spliceE n e@(pureOrPartial -> Just _) spliceF = spliceValueE e spliceF
spliceE n e spliceF = spliceActionE n e spliceF

-- | Builds a CG expression, given a pair of splice functions for type-directed splicing.
--   The first splice function assumes a pure expression, while the second assumes an action expression.
spliceEWithAction :: (HS.Exp -> CodeGeneration CGExpr) -> (HS.Exp -> CodeGeneration CGExpr) -> CGExpr
                  -> CodeGeneration CGExpr
spliceEWithAction pureF actionF ce = case pureOrPartial ce of
    Just _  -> spliceValueE ce pureF
    _       -> actionF $ getHSExpression ce

-- | Builds a CG expression by splicing multiple subexpressions. Action subexpressions are
--   bound to names with the given prefix.
spliceManyE :: Identifier -> ([HS.Exp] -> CodeGeneration CGExpr) -> [CGExpr] -> CodeGeneration CGExpr
spliceManyE n f args = case all isPureOrPartial args of
    True  -> spliceManyValuesE f args
    False -> do {  (stmts, argE) <- foldM (bindName n) ([],[]) args;
                   f argE >>= prefixDoE stmts }

  where bindName n (stmtAcc, argAcc) e@(pureOrPartial -> Just _) = extractValueE e >>= \case
          (ctxt, Pure e') -> return (stmtAcc++ctxt, argAcc++[e'])
          (_,_)           -> pureError "binding a value for splicing"

        bindName n (stmtAcc, argAcc) (Action e) = gensymCG n >>= \n' ->
          let nN       = HB.name n'
              (nV,nPV) = (HB.var nN, HB.pvar nN)
          in return (stmtAcc++[HB.genStmt HL.noLoc nPV e], argAcc++[nV])


{- Name bindings. These construct partial action expressions. -}

-- | Bind a K3 value to an immutable variable. This constructs a partial action.
bindE :: Identifier -> CGExpr -> CodeGeneration CGExpr
bindE n e = ensureActionE e >>= \e' -> return $
              PartialAction [hs| do { ((nN)) <- $(getHSExpression e'); $nE } |]
  where (nN, nE) = (HB.name n, HB.var $ HB.name n)

-- | Store a K3 value as a mutable binding.
doStoreE :: Identifier -> CGExpr -> CodeGeneration CGExpr
doStoreE n = spliceEWithAction pureF actionF
  where pureF   e = bindE n $ Action [hs| liftIO ( newMVar ( $e ) ) |]
        actionF e = bindE n $ Action [hs| ( $e ) >>= liftIO . newMVar |]

-- | Read a mutable expression.
doLoadE :: Identifier -> CGExpr -> CodeGeneration CGExpr
doLoadE n = spliceEWithAction pureF actionF
  where pureF   e = bindE n $ Action [hs| liftIO ( readMVar ( $e ) ) |]
        actionF e = bindE n $ Action [hs| ( $e ) >>= liftIO . readMVar |]

-- | Read from a mutable variable.
doLoadNameE :: Identifier -> CodeGeneration CGExpr
doLoadNameE n = gensymCG n >>= flip doLoadE (Pure . HB.var $ HB.name n)

-- | Read from a K3 variable, with annotations indicating qualifiers.
loadNameE :: [Annotation Expression] -> Identifier -> CodeGeneration CGExpr
loadNameE anns n = case (qualifier anns, load anns) of
  (Nothing, False)         -> return varE
  (Just PImmutable, False) -> return varE
  (Just PMutable, True)    -> doLoadNameE n
  _ -> qualLoadError
  where varE = Pure . HB.var $ HB.name n

-- | Bind to a K3 variable, with annotations indicating qualifiers.
storeE :: [Annotation Expression] -> Identifier -> CGExpr -> CodeGeneration CGExpr
storeE anns n e = case (qualifier anns, store anns) of
  (Nothing, False)         -> bindE n e
  (Just PImmutable, False) -> bindE n e
  (Just PMutable, True)    -> doStoreE n e
  _ -> qualStoreError


{- Generic Do-expression constructors. -}

-- | Prepends a sequence of do-block statements to an expression.
prefixDoE :: [HS.Stmt] -> CGExpr -> CodeGeneration CGExpr
prefixDoE [] e = return e

prefixDoE context (pureOrPartial -> Just (HS.Do stmts)) = return . PartialAction . HB.doE $ context ++ stmts
prefixDoE context (pureOrPartial -> Just e) = return . PartialAction . HB.doE $ context ++ [HB.qualStmt e]

prefixDoE context (Action (HS.Do stmts)) = return . Action . HB.doE $ context ++ stmts
prefixDoE context (Action e) = return . Action . HB.doE $ context ++ [HB.qualStmt e]


{- Do-expression manipulation on pure expressions and partial actions. -}

-- | Extracts a pure return value and a context from an expression.
--   For do-expressions this is the qualifier expression and the context is
--   the remainder of the stmts. For any other expression, the context is empty.
extractValueE :: CGExpr -> CodeGeneration ([HS.Stmt], CGExpr)
extractValueE (pureOrPartial -> Just (HS.Do [])) = blockError

extractValueE (pureOrPartial -> Just (HS.Do stmts)) =
  case (init stmts, last stmts) of
    ([], HS.Qualifier e) -> return ([], Pure e)
    (c, HS.Qualifier e)  -> return (c, Pure e)
    _ -> blockError

extractValueE (pureOrPartial -> Just e) = return ([], Pure e)
extractValueE _ = pureOrPartialArgError "extractValueE"


-- | Sequences two pure or partial action expressions.
--   Note this discards the first expression if it is pure.
seqDoE :: CGExpr -> CGExpr -> CodeGeneration CGExpr
seqDoE e e2 = do
  (eStmts,  eRetE)  <- extractValueE e
  (e2Stmts, e2RetE) <- extractValueE e2
  case (eStmts, e2Stmts) of
    ([],[]) -> return e2RetE
    _ -> return . PartialAction .
          HB.doE $ eStmts ++ e2Stmts ++ [HB.qualStmt $ getHSExpression e2RetE]


spliceValueE :: CGExpr -> (HS.Exp -> CodeGeneration CGExpr) -> CodeGeneration CGExpr
spliceValueE e spliceF = extractValueE e >>= \case
  ([], Pure e')   -> spliceF e'
  (ctxt, Pure e') -> spliceF e' >>= prefixDoE ctxt
  (_,_)           -> pureError "chaining a value"


-- | Operator and data constructors applied to evaluated children.
--   TODO: alpha renaming for conflicting binding names during merge
spliceManyValuesE :: ([HS.Exp] -> CodeGeneration CGExpr) -> [CGExpr] -> CodeGeneration CGExpr
spliceManyValuesE f subE = do
  (contexts, argsE) <- mapM extractValueE subE >>= return . unzip
  if all isPure argsE then do
    retE <- f $ map getHSExpression argsE
    prefixDoE (concat contexts) retE
  else pureError "applying a function"


{- Monadic action manipulation -}

-- | Construct a complete action from a pure or partial action expression.
ensureActionE :: CGExpr -> CodeGeneration CGExpr
ensureActionE (pureOrPartial -> Just (HS.Do [])) = blockError

ensureActionE (pureOrPartial -> Just (HS.Do stmts)) =
  case (init stmts, last stmts) of
    ([], HS.Qualifier e) -> return $ Action [hs| return e |]
    (c, HS.Qualifier e)  -> return . Action . HS.Do $ c ++ [ HS.Qualifier [hs| return $e |] ]
    _ -> blockError

ensureActionE (pureOrPartial -> Just e) = return $ Action [hs| return ( $e ) |]
ensureActionE e = return e

-- | Composes two monadic actions with a custom sequencing function.
spliceActionEWithSeq :: Identifier -> CGExpr -> (HS.Exp -> CodeGeneration CGExpr)
                     -> (HS.Exp -> HS.Exp -> CodeGeneration CGExpr)
                     -> CodeGeneration CGExpr
spliceActionEWithSeq n e spliceF composeF =
  composeActions =<< ((,) <$> (ensureActionE e) <*> (spliceF (HB.var nN) >>= ensureActionE))
  where composeActions (Action ae, Action ae') = composeF ae ae'
        composeActions _ = actionError "splicing two actions"
        nN = HB.name n

-- | Compose two actions as a do-block, using an action constructor to support dependencies.
spliceActionE :: Identifier -> CGExpr -> (HS.Exp -> CodeGeneration CGExpr) -> CodeGeneration CGExpr
spliceActionE n e spliceF = spliceActionEWithSeq n e spliceF doSeqF
  where doSeqF ae ae' = return $ Action [hs| do { ((nN)) <- $ae; $ae' } |]
        nN = HB.name n

-- | Compose two actions inline, using an action constructor to support dependencies.
spliceInlineActionE :: Identifier -> CGExpr -> (HS.Exp -> CodeGeneration CGExpr) -> CodeGeneration CGExpr
spliceInlineActionE n e spliceF = spliceActionEWithSeq n e spliceF inlineSeqF
  where inlineSeqF ae ae' = return $ Action [hs| $ae >>= ( \((nN)) -> $ae' ) |]
        nN = HB.name n


{- Message passing -}
sendFnE :: HS.Exp
sendFnE = HB.qvar (HS.ModuleName engineModuleAliasId) $ HB.name "sendE"


{- Records embedding -}

-- TODO: change for statically typed records
-- | Ad-hoc record constructor
buildRecordE :: [Identifier] -> [HS.Exp] -> CodeGeneration CGExpr
buildRecordE names subE = do
  lSym <- gensymCG "__lbl"
  rSym <- gensymCG "__record"

  (_, namedLblE, recFieldE) <- accumulateLabelAndFieldE names subE
  lblCE <- spliceManyE lSym (return . Pure . HB.listE) namedLblE
  recCE <- foldM concatRecField (Pure [hs| emptyRecord |]) recFieldE
  spliceManyE rSym mkRecord [lblCE, recCE]
  
  where
    mkRecord [lblE, recE] = return . Pure $ HB.tuple [lblE, recE]

    accumulateLabelAndFieldE names subE = 
      foldM buildRecordFieldE (Pure [hs| firstLabel |], [], []) $ zip names subE

    buildRecordFieldE (keyCE, nlblAcc, recAcc) (n,ce) = do
      [kSym, kvSym, nkSym] <- mapM gensymCG ["__key", "__kv", "__nk"]
      nextKeyE  <- spliceE nkSym keyCE $ \keyE -> return $ Pure [hs| nextLabel $keyE |]
      namedKeyE <- spliceManyE kSym (return . Pure . HB.tuple) [Pure $ HB.strE n, keyCE]
      fieldE    <- spliceManyE kvSym mkField [keyCE, Pure ce]
      return (nextKeyE, nlblAcc++[namedKeyE], recAcc++[fieldE])

    mkField [keyE, ce] = return $ Pure [hs| $keyE .=. $ce |]
    mkField _          = throwCG $ CodeGenerationError "Invalid field constructor arguments"
    
    concatRecField a b = gensymCG "__crf" >>= \sym -> spliceManyE sym extendRecord [a, b]
    extendRecord [a, b] = return $ Pure [hs| $b .*. $a |]
    extendRecord _ = throwCG $ CodeGenerationError "Invalid record extension arguments"

-- TODO: change for statically typed records
-- | Record field lookup expression construction
recordFieldLookupE :: (HS.Exp -> CodeGeneration CGExpr) -> Identifier -> CGExpr -> CodeGeneration CGExpr
recordFieldLookupE accessF n rCE = do
  fSym <- gensymCG "__field"
  aSym <- gensymCG "__access"
  fCE  <- spliceE fSym rCE $ \re -> return $ Pure [hs| ( __n__ $re ) |]
  spliceE aSym fCE accessF


{- Default values -}

-- | Default values for specific types
defaultValue :: K3 Type -> CodeGeneration HaskellEmbedding
defaultValue t = defaultValue' t >>= return . HExpression . getHSExpression

defaultValue' :: K3 Type -> CodeGeneration CGExpr
defaultValue' (tag -> TBool)       = return $ Pure [hs| False |]
defaultValue' (tag -> TByte)       = return $ Pure [hs| (0 :: Word8) |]
defaultValue' (tag -> TInt)        = return $ Pure [hs| (0 :: Int) |]
defaultValue' (tag -> TReal)       = return $ Pure [hs| (0 :: Double) |]
defaultValue' (tag -> TString)     = return $ Pure [hs| "" |]
defaultValue' (tag -> TAddress)    = return $ Pure [hs| defaultAddress |]

defaultValue' (tag &&& children -> (TOption, [x])) = defaultValue' x >>= spliceEWithAction pureF actionF
  where pureF   e = return $ Pure   [hs| Just $e |]
        actionF e = return $ Action [hs| ( $e  ) >>= return . Just |]

defaultValue' (tag -> TOption) = throwCG $ CodeGenerationError "Invalid option type"

defaultValue' (tag &&& children -> (TIndirection, [x])) = defaultValue' x >>= spliceEWithAction pureF actionF
  where pureF   e = return $ Action [hs| liftIO ( newMVar $e ) |]
        actionF e = return $ Action [hs| ( $e ) >>= liftIO . newMVar |]

defaultValue' (tag -> TIndirection) = throwCG $ CodeGenerationError "Invalid indirection type"

defaultValue' (tag &&& children -> (TTuple, ch)) =
  mapM defaultValue' ch >>= spliceManyE "__f" (return . Pure . HB.tuple)

-- TODO
defaultValue' (tag -> TRecord ids) = throwCG $ CodeGenerationError "Default records not implemented"

defaultValue' (tag &&& annotations -> (TCollection, anns)) = 
  return . Action $ case annotationComboIdT anns of
    Just comboId -> HB.var $ HB.name $ collectionEmptyConPrefixId ++ comboId
    Nothing      -> [hs| return [] |]


defaultValue' (tag -> TFunction) = throwCG $ CodeGenerationError "No default available for a function"
defaultValue' (tag -> TTrigger)  = throwCG $ CodeGenerationError "No default available for a trigger"
defaultValue' (tag -> TSink)     = throwCG $ CodeGenerationError "No default available for a sink"
defaultValue' (tag -> TSource)   = throwCG $ CodeGenerationError "No default available for a source"


{- Expression code generation -}

unary :: Operator -> K3 Expression -> CodeGeneration CGExpr
unary op e = do
  e' <- expression' e
  case op of
    ONeg -> gensymCG "__neg" >>= \n -> spliceE n e' $ return . Pure . HS.NegApp
    ONot -> gensymCG "__not" >>= \n -> spliceE n e' $ return . Pure . HB.app (HB.function "not")
    _ -> throwCG $ CodeGenerationError "Invalid unary operator"

binary :: Operator -> K3 Expression -> K3 Expression -> CodeGeneration CGExpr
binary op e e' = do
  eE  <- expression' e
  eE' <- expression' e'
  case op of 
    OAdd -> doInfx "__arith" "+"  [eE, eE']
    OSub -> doInfx "__arith" "-"  [eE, eE']
    OMul -> doInfx "__arith" "*"  [eE, eE']
    ODiv -> doInfx "__arith" "/"  [eE, eE']
    OAnd -> doInfx "__bool"  "&&" [eE, eE']
    OOr  -> doInfx "__bool"  "||" [eE, eE']
    OEqu -> doInfx "__cmp"   "==" [eE, eE']
    ONeq -> doInfx "__cmp"   "/=" [eE, eE']
    OLth -> doInfx "__cmp"   "<"  [eE, eE']
    OLeq -> doInfx "__cmp"   "<=" [eE, eE']
    OGth -> doInfx "__cmp"   ">"  [eE, eE']
    OGeq -> doInfx "__cmp"   ">=" [eE, eE']
    OSeq -> doInfx "__seq"   ">>" [eE, eE']
    OApp -> spliceManyE "__app" applyFn [eE, eE']
    OSnd -> spliceManyE "__snd" doSendE [eE, eE']
    _    -> throwCG $ CodeGenerationError "Invalid binary operator"

  where doInfx n opStr args = gensymCG n >>= \n' -> spliceManyE n' (doOpF True opStr) args
        doOpF infx opStr = if infx then infixBinary opStr else appBinary $ HB.function opStr 

        infixBinary op [a,b] = return . Pure $ HB.infixApp a (HB.op $ HB.sym op) b
        infixBinary _ _      = throwCG $ CodeGenerationError "Invalid binary operator arguments"

        appBinary f [a,b] = return . Pure $ HB.appFun f [a,b]
        appBinary _ _     = throwCG $ CodeGenerationError "Invalid binary function app arguments"

        applyFn [a,b] = return . Pure $ HB.appFun a [b]
        applyFn _     = throwCG $ CodeGenerationError "Invalid function application"

        doSendE [targE, msgE] = return . Action $
          [hs| let (addr,trig) = $targE in
                (liftIO $ ishow $msgE) >>= $sendFnE addr ( __triggerHandleFnId__ trig ) |]

        doSendE _ = throwCG $ CodeGenerationError "Invalid message send"


-- | Compiles an expression, yielding a combination of a do-expression of 
--   monadic action bindings, and a residual expression.
expression' :: K3 Expression -> CodeGeneration CGExpr

-- | Constants
expression' (tag &&& annotations -> (EConstant c, anns)) =
  constantE c anns
  where 
    constantE (CBool b)   _ = return . Pure $ if b then [hs| True |] else [hs| False |]
    constantE (CInt i)    _ = return . Pure $ HB.intE $ toInteger i
    constantE (CByte w)   _ = return . Pure $ HS.Lit $ HS.PrimWord $ toInteger w
    constantE (CReal r)   _ = return . Pure $ HS.Lit $ HS.PrimDouble $ toRational r
    constantE (CString s) _ = return . Pure $ HB.strE s
    constantE (CNone _)   _ = return . Pure $ [hs| Nothing |]
    
    constantE (CEmpty _) as =
      maybe comboIdErr (collectionCstr . (collectionEmptyConPrefixId++)) $ annotationComboIdE as
    
    collectionCstr n = return . Action . HB.var . HB.name $ n
    comboIdErr = throwCG $ CodeGenerationError "Invalid combo id for empty collection"

-- | Variables
expression' (tag &&& annotations -> (EVariable i, anns)) = loadNameE anns i

-- | Unary option constructor
expression' (tag &&& children -> (ESome, [x])) = do
  x' <- expression' x
  n  <- gensymCG "__opt"
  spliceE n x' $ \e -> return $ Pure [hs| Just $e |]

expression' (tag -> ESome) = throwCG $ CodeGenerationError "Invalid option expression"

-- | Indirection constructor
expression' (tag &&& children -> (EIndirect, [x])) = do
  x' <- expression' x
  n  <- gensymCG "__ind"
  spliceE n x' $ \e -> return $ Action [hs| liftIO ( newMVar ( $e ) ) |]

expression' (tag -> EIndirect) = throwCG $ CodeGenerationError "Invalid indirection expression"

-- | Tuple constructor
expression' (tag &&& children -> (ETuple, cs)) = do
  cs' <- mapM expression' cs
  n  <- gensymCG "__tup"  
  spliceManyE n (return . Pure . HB.tuple) cs'

-- | Functions
expression' (tag &&& children -> (ELambda i,[b])) = do
  b' <- expression' b
  n  <- gensymCG "__lam"
  case b' of 
    Pure be -> return . Pure $ [hs| \((ni)) -> $be |]
    _       -> ensureActionE b' >>= \bCE -> return . Pure $ [hs| \((ni)) -> $(getHSExpression bCE) |]
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
  spliceE n e' $ \ne -> do
    se <- storeE (annotations e) i (Pure ne)
    spliceE n' se $ return . const b'

expression' (tag -> ELetIn _) = throwCG $ CodeGenerationError "Invalid let expression"

-- | Assignments
expression' (tag &&& children -> (EAssign i, [e])) = do
  e' <- expression' e
  n  <- gensymCG "__assign"
  spliceE n e' $ \ae -> return $ Action [hs| liftIO ( modifyMVar_ $iE (return . const $ $ae) ) |]
  where iE = HB.var $ HB.name i

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

        mkICase [eE, sE, nE] = return . Pure $
          [hs| case $eE of
                 Just ((ni)) -> $sE
                 Nothing     -> $nE |]

        mkICase _ = throwCG $ CodeGenerationError "Invalid immutable case constructor arguments"

        mkMCase x [eE, sE, nE] = return . Action $
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
        PImmutable -> doLoadE i e' >>= \eCE -> spliceE sym eCE $ return . const f'
        PMutable   -> bindE i e' >>= \eCE -> spliceE sym eCE $ return . const f'

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

      spliceE sym e' $ \eE -> prefixDoE (bindStmts eE) f'


    renameBinding (n, PImmutable) = return (n, n)
    renameBinding (n, PMutable)   = gensymCG n >>= return . (n,)

    bindRecordFields namePairs = do
      e'              <- expression' e
      f'              <- expression' f
      sym             <- gensymCG "__bindR"
      rNamedStructure <- return $ zip namePairs $ eStructure namePairs
      rBindings       <- bindE recordId e' >>= \rCE -> spliceE sym rCE $ 
                          \re -> foldM bindRecordField (Pure re) rNamedStructure      
      seqDoE rBindings f'

    bindRecordField acc ((a,b), q) = recordField q b >>= bindE a >>= seqDoE acc

    recordField PImmutable x = recordFieldLookupE (return . Pure) x $ Pure recordVarE
    recordField PMutable   x = recordFieldLookupE mkMutRecField x $ Pure recordVarE
    (recordId, recordVarE)   = let x = "__record" in (x, HB.var $ HB.name x)

    mkMutRecField fe = return $ Action [hs| liftIO ( newMVar $fe ) |]

expression' (tag -> EBindAs _) = throwCG $ CodeGenerationError "Invalid bind expression"

expression' (tag &&& children -> (EIfThenElse, [p, t, e])) = do
  p' <- expression' p
  t' <- expression' t
  e' <- expression' e
  n  <- gensymCG "__if"
  spliceManyE n mkBranch [p', t', e']
  where mkBranch [pE, tE, eE] = return $ Pure [hs| if ( $pE ) then ( $tE ) else ( $eE ) |]
        mkBranch _ = throwCG $ CodeGenerationError "Invalid branch constructor arguments"

expression' (tag &&& children -> (EAddress, [h, p])) = do
  h' <- expression' h
  p' <- expression' p
  n  <- gensymCG "__addr"
  spliceManyE n (return . Pure . HB.tuple) [h', p']

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
  spliceE sym r' $ \e -> recordFieldLookupE (return . Pure) i $ Pure e

expression' (tag -> EProject _) = throwCG $ CodeGenerationError "Invalid record projection"

-- TODO
expression' (tag -> ESelf) = undefined

expression' _ = throwCG $ CodeGenerationError "Invalid expression"

expression :: K3 Expression -> CodeGeneration HaskellEmbedding
expression e = expression' e >>= return . HExpression . getHSExpression


{- Declarations -}

promoteDeclType :: HS.Type -> CGExpr -> CodeGeneration (HS.Type, HS.Exp)
promoteDeclType t e = case e of
      Pure e'          -> return (t, e')
      PartialAction e' -> ensureActionE e >>= return . (engineType t,) . getHSExpression
      Action e'        -> return (engineType t, e')

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
                    >>= \sym -> spliceE sym nInit (\e -> return $ Action [hs| liftIO $ newMVar ( $e ) |])
                    >>= mkTypedDecl n (engineType $ indirectionType nType) . getHSExpression
    _            -> throwCG $ CodeGenerationError "Ambiguous global declaration qualifier"


globalWithDefault :: Identifier -> [Annotation Type] -> HS.Type -> Maybe (K3 Expression) -> CodeGeneration CGExpr
                  -> CodeGeneration HaskellEmbedding
globalWithDefault n anns t eOpt defaultE = do
  e' <- maybe defaultE expression' eOpt
  mkGlobalDecl n anns t e'


global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> CodeGeneration HaskellEmbedding
global _ (tag -> TSource) _ = return HNoRepr

-- | Generate builtin functions, e.g., wrappers of the engine API.
global n t@(tag -> TFunction) Nothing  = builtin n t

-- | Functions, like triggers, operate in the EngineM monad.
global n t@(tag -> TFunction) (Just e) = do
  at <- argType t >>= typ'
  rt <- returnType t >>= typ'
  t' <- return $ funType at $ engineType rt
  e' <- expression' e
  mkGlobalDecl n (annotations t) t' e'

-- TODO: two-level namespaces.
global n t@(tag &&& children -> (TCollection, [x])) eOpt =
  case composedName of
    Nothing      -> initializeCollection eOpt
    Just comboId -> testAndAddComposition comboId >> initializeCollection eOpt

  where anns            = annotations t
        annotationNames = annotationNamesT anns
        composedName    = annotationComboIdT anns

        testAndAddComposition comboId = 
          getCompositionSpec comboId >>= \case 
            Nothing -> composeAnnotations comboId >>= \spec -> modifyCompositionSpecs (spec:)
            Just _  -> return ()

        composeAnnotations comboId = mapM lookupAnnotation annotationNames >>= composeSpec comboId
        lookupAnnotation n = getAnnotationSpec n >>= maybe (invalidAnnotation n) return

        composeSpec comboId annSpecs = 
          let cSpec  = concat annSpecs
              cNames = concat $ map (map (\(x,y,z) -> x)) annSpecs
          in
          if length cSpec == (length $ nub cNames)
          then return (comboId, cSpec)
          else compositionError n

        initializeCollection eOpt = 
          typ' t >>= \t' -> globalWithDefault n anns t' eOpt (defaultValue' t)
        
        compositionError n  = throwCG . CodeGenerationError $ "Overlapping attribute names in collection " ++ n
        invalidAnnotation n = throwCG . CodeGenerationError $ "Invalid annotation " ++ n

global n t@(tag -> TCollection) eOpt = throwCG . CodeGenerationError $ "Invalid global collection"

global n t eOpt = typ' t >>= \t' -> globalWithDefault n (annotations t) t' eOpt (defaultValue' t)


-- | Triggers are implemented as functions that operate in the EngineM monad.
trigger :: Identifier -> K3 Type -> K3 Expression -> CodeGeneration HaskellEmbedding
trigger n t e = do
  e'   <- expression' e
  t'   <- typ' t

  sym  <- gensymCG "__trig"
  impl <- spliceE sym e' (return . Pure . triggerImpl (HB.strE n))

  void $ modifyTriggerDispatchCG ((n,t'):)
  (pt, pImpl) <- promoteDeclType (triggerType t') impl
  return . HDeclarations $ [ typeSig n pt, namedVal n pImpl ]
  
  where 
    triggerImpl hndlE implE = 
      HB.appFun (HS.Con $ HS.UnQual $ HB.name triggerConId) [hndlE, implE]


annotation :: Identifier -> [AnnMemDecl] -> CodeGeneration ()
annotation n memberDecls =
  foldM (initializeMember n) [] memberDecls >>= modifyAnnotationSpecs . (:) . (n,)
  where initializeMember annId acc m = annotationMember annId m >>= return . maybe acc ((acc++) . (:[]))

-- TODO: handle member mutability qualifier
-- TODO: distinguish lifted and regular attributes.
-- TODO: use default values for attributes specified without initializers.
annotationMember :: Identifier -> AnnMemDecl -> CodeGeneration (Maybe (Identifier, HS.Type, Maybe HS.Exp))
annotationMember annId = \case
  Lifted    Provides n t (Just e) uid -> memberSpec n t e
  Attribute Provides n t (Just e) uid -> memberSpec n t e
  Lifted    Provides n t Nothing  uid -> builtinLiftedAttribute annId n t uid >>= return . Just
  Attribute Provides n t Nothing  uid -> builtinAttribute annId n t uid >>= return . Just
  _                                   -> return Nothing
  where memberSpec n t e = do
          e' <- expression' e
          t' <- typ' t 
          (t'', e'') <- promoteDeclType t' e'
          return $ Just (n, t'', Just e'')


{- Builtins -}

genNotifier :: Identifier -> String -> CodeGeneration HaskellEmbedding
genNotifier n evt = mkTypedDecl n
  [ty| Identifier -> Trigger a -> E.EngineM String () |] 
  [hs| \cid (trig, addr) -> 
          (liftIO $ ishow ()) >>= attachNotifier_ cid $(HB.strE evt) . (addr, handle trig,) |]

builtin :: Identifier -> K3 Type -> CodeGeneration HaskellEmbedding
builtin "parseArgs" _ = return HNoRepr -- TODO

builtin "openBuiltin" _ = mkTypedDecl "openBuiltin"
  [ty| Identifier -> Identifier -> String -> E.EngineM String () |]
  [hs| \cid builtinId format -> E.openBuiltin cid builtinId (wireDesc format) |]

builtin "openFile" t = mkTypedDecl "openFile"
  [ty| Identifier -> String -> String -> String -> E.EngineM String () |]
  [hs| \cid path format mode -> E.openFile cid path (wireDesc format) mode |]

builtin "openSocket" t = mkTypedDecl "openSocket"
  [ty| Identifier -> Address -> String -> String -> E.EngineM String () |]
  [hs| \cid addr format mode -> E.openSocket cid addr (wireDesc format) mode |]

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

builtin n t = throwCG . CodeGenerationError $ "Invalid builtin function " ++ n

-- TODO: duplicated from interpreter. Factorize.
channelMethod :: String -> (String, Maybe String)
channelMethod x =
  case find (flip isSuffixOf x) ["HasRead", "Read", "HasWrite", "Write"] of
    Just y -> (y, stripSuffix y x)
    Nothing -> (x, Nothing)
  where stripSuffix sfx lst = maybe Nothing (Just . reverse) $ stripPrefix (reverse sfx) (reverse lst)



builtinLiftedAttribute :: Identifier -> Identifier -> K3 Type -> UID -> CodeGeneration (Identifier, HS.Type, Maybe HS.Exp)
builtinLiftedAttribute "Collection" "peek" t uid = typ' t >>= \t' -> 
  return ("peek",   t', Just
    [hs| (\() -> liftIO (readMVar self) >>= return . head . getData) |])

builtinLiftedAttribute "Collection" "insert" t uid = typ' t >>= \t' -> 
  return ("insert", t', Just
    [hs| (\x -> liftIO $ modifyMVar_ self (return . modifyData (++[x]))) |])

builtinLiftedAttribute "Collection" "delete" t uid = typ' t >>= \t' ->
  return ("delete", t', Just
    [hs| (\x -> liftIO $ modifyMVar_ self (return . modifyData $ delete x)) |])

builtinLiftedAttribute "Collection" "update" t uid = typ' t >>= \t' ->
  return ("update", t', Just
    [hs| (\x y -> liftIO $ modifyMVar_ self (return . modifyData (\l -> (delete x l)++[y]))) |])

builtinLiftedAttribute "Collection" "combine" t uid = typ' t >>= \t' ->
  return ("combine", t', Just $
    [hs| (\c' -> liftIO . newMVar =<< 
         ((\x y -> copyWithData x (getData x ++ getData y))
           <$> liftIO (readMVar self) <*> liftIO (readMVar c'))) |])

builtinLiftedAttribute "Collection" "split" t uid = typ' t >>= \t' ->
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
builtinLiftedAttribute "Collection" "iterate" t uid = typ' t >>= \t' ->
  return ("iterate", t', Just [hs| (\f -> liftIO (readMVar self) >>= mapM_ f . getData) |])

builtinLiftedAttribute "Collection" "map" t uid = typ' t >>= \t' ->
  return ("map", t', Just $
    [hs| (\f -> liftIO (readMVar self) >>=
         (\r -> mapM f (getData r) >>= (liftIO . newMVar $ copyWithData r))) |])

builtinLiftedAttribute "Collection" "filter" t uid = typ' t >>= \t' ->
  return ("filter", t', Just $
    [hs| (\f -> liftIO (readMVar self) >>= 
         (\r -> filterM f (getData r) >>= (liftIO . newMVar $ copyWithData r))) |])

builtinLiftedAttribute "Collection" "fold" t uid = typ' t >>= \t' ->
  return ("fold", t', Just $ 
    [hs| (\f accInit -> liftIO (readMVar self) >>= foldM f accInit . getData) |])

-- TODO: key-value record construction for resulting collection
builtinLiftedAttribute "Collection" "groupBy" t uid = typ' t >>= \t' -> 
  return ("groupBy", t', Just $
    [hs| (\gb f accInit -> 
            liftIO (readMVar self) >>=
            (\r -> foldM (\m x -> let k = gb x 
                                  in M.insert k $ f (M.findWithDefault accInit k m) x)
                         M.empty . getData
                   >>= liftIO . newMVar . copyWithData r . M.toList)) |])

builtinLiftedAttribute "Collection" "ext" t uid = typ' t >>= \t' ->
  return ("ext", t', Just $
    [hs| (\f -> liftIO (readMVar self) >>= 
         (\r -> mapM f . getData >>= liftIO . newMVar . copyWithData r . concat)) |])

builtinLiftedAttribute annId n t uid = throwCG $ CodeGenerationError "Builtin lifted attributes not implemented"


builtinAttribute :: Identifier -> Identifier -> K3 Type -> UID -> CodeGeneration (Identifier, HS.Type, Maybe HS.Exp)
builtinAttribute annId n t uid = throwCG $ CodeGenerationError "Builtin attributes not implemented"


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
  
  (DRole r, ch) -> do
    decls <- mapM (declaration >=> extractDeclarations) ch
    return . HDeclarations $ concat decls
      -- TODO: qualify names?

  (DAnnotation n members, []) -> annotation n members >> return HNoRepr

  _ -> throwCG $ CodeGenerationError "Invalid declaration"

  where extractDeclarations (HDeclarations decls) = return decls
        extractDeclarations HNoRepr               = return []
        extractDeclarations _ = throwCG $ CodeGenerationError "Invalid declaration"


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

    generateRecFieldTy (n, ty, _) = ([HB.name n], HS.UnBangedTy ty)


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
          (selfVarId, selfVarE)   = ("cmv", HB.var $ HB.name "cmv")
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
          initPvar     = HB.pvar $ HB.name initArg
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

    generateRecFieldTy (n, ty, _)  = ([HB.name n], HS.UnBangedTy ty)
    generateRecFieldExpr (_, _, Just e)  = return e
    generateRecFieldExpr (_, _, Nothing) = throwCG $ CodeGenerationError "No expression found for record field initializer"

    -- | Function fields are not copied between records to ensure correct handling of self-referencing.
    copyFieldExpr _ (n, HS.TyFun _ _, Just e)  = return e
    copyFieldExpr _ (n, HS.TyFun _ _, Nothing) = throwCG $ CodeGenerationError "No expression found for record function field"
    copyFieldExpr varE (n,_,_)                 = return [hs| $(HB.var $ HB.name n) $varE |] 


-- | Top-level code generation function, returning a Haskell module.
-- TODO: record label, type and default constructor generation
-- TODO: main, argument processing
generate :: String -> K3 Declaration -> CodeGeneration HaskellEmbedding
generate progName p = declaration p >>= mkProgram
  where 
        mkProgram (HDeclarations decls) = programDecls decls
          >>= return . HProgram . HS.Module HL.noLoc (HS.ModuleName $ sanitize progName) pragmas warning expts impts
        
        mkProgram _ = throwCG $ CodeGenerationError "Invalid program"

        pragmas   = []
        warning   = Nothing
        expts     = Nothing
        
        programDecls decls =
          generateRecords >>= \recordDecls ->
          generateCollectionCompositions >>= \comboDecls -> 
          generateDispatch >>= return . ((preDecls ++ recordDecls ++ comboDecls ++ decls) ++) . postDecls

        impts = [ imprtDecl "Control.Concurrent"          False Nothing
                , imprtDecl "Control.Concurrent.MVar"     False Nothing
                , imprtDecl "Control.Monad"               False Nothing
                , imprtDecl "Options.Applicative"         False Nothing                
                , imprtDecl "Language.K3.Common"          False Nothing
                , imprtDecl "Language.K3.Runtime.Options" False Nothing
                , imprtDecl "Language.K3.Runtime.Engine"  True  (Just engineModuleAliasId)
                , imprtDecl "Data.Map.Lazy"               True  (Just "M") ]

        preDecls = 
          [ typeSig programId stringType,
            namedVal programId $ HB.strE $ sanitize progName,

            [dec| class Collection a b where
                    getData      :: a -> b
                    modifyData   :: a -> (b -> b) -> a
                    copyWithData :: a -> b -> a |]

          , [dec| data Trigger a = Trigger { __triggerHandleFnId__ :: Identifier
                                           , __triggerImplFnId__   :: a } |] 

          , [dec| type RuntimeStatus = Either E.EngineError () |] ]

        postDecls dispatchDeclOpt =
          [ 
            [dec| identityWD :: E.WireDesc String |]
          , [dec| identityWD = E.WireDesc return (return . Just) (Delimiter "\n") |]

          , [dec| compiledMsgPrcsr :: E.MessageProcessor SystemEnvironment () String RuntimeStatus E.EngineError |]
          , [dec| compiledMsgPrcsr = E.MessageProcessor {
                                         initialize = initializeRT
                                       , process    = processRT
                                       , status     = statusRT
                                       , finalize   = finalizeRT
                                       , report     = reportRT
                                     }
                    where
                      initializeRT _ _ _ = atInit ()
                      finalizeRT _ = atExit ()
                      
                      statusRT rts = either id (const rts) rts
                                            
                      reportRT (Left err) = print err
                      reportRT (Right _)  = return ()

                      processRT (addr, n, msg) rts = dispatch addr n msg
            |] ]
          ++ (case dispatchDeclOpt of
                Nothing -> []
                Just dispatchDecl ->
                  [ [dec| dispatch :: Address -> Identifier -> String -> E.EngineM String () |]
                  , dispatchDecl ]
              )
          ++
          [ [dec| main = do
                           sysEnv <- liftIO $ execParser options
                           engine <- E.networkEngine sysEnv identityWD
                           void $ E.runEngine compiledMsgPrcsr sysEnv engine ()
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
          return $ map (\(n,t) -> HB.alt HL.noLoc (HB.strP n) $ HB.paren $ dispatchMsg n t) trigIds

        dispatchMsg n argT = 
          HB.doE
            [ HB.genStmt HL.noLoc (HB.pvar $ HB.name "payload")
                  (HB.paren $ typedExpr [hs| liftIO ( iread $dispatchMsgV ) |] $ engineType $ maybeType argT)
            
            , HB.qualStmt [hs| case payload of 
                                  Nothing -> error "Failed to extract message payload" -- TODO: throw engine error
                                  Just v  -> return $ ( (__triggerImplFnId__ __n__) v ) |]
            ]


compile :: CodeGeneration HaskellEmbedding -> Either String String
compile cg = either (Left . show) (Right . compile') $ fst $ runCodeGeneration emptyCGState cg 
  where compile' (HProgram      mdule) = HP.prettyPrint mdule
        compile' (HDeclarations decls) = unlines $ map HP.prettyPrint decls
        compile' (HExpression   expr)  = HP.prettyPrint expr
        compile' (HType         typ)   = HP.prettyPrint typ
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
