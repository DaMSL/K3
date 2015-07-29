{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

module Language.K3.Parser.SQL where

import Control.Arrow ( (***), (&&&), first, second )
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Except

import Data.Functor.Identity
import Data.Function ( on )
import Data.Monoid
import Data.Either ( isLeft, partitionEithers )
import Data.List ( find, intersect, isInfixOf, nub, nubBy, reverse )
import Data.Tree
import Data.Tuple ( swap )

import Data.Map ( Map )
import Data.Set ( Set )
import qualified Data.Map as Map
import qualified Data.Set as Set

import Debug.Trace

import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Annotation hiding ( Annotation )
import qualified Database.HsSqlPpp.Annotation as HA ( Annotation )
import Database.HsSqlPpp.Parser
import Database.HsSqlPpp.Pretty

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal
import Language.K3.Core.Metaprogram
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Declaration as DC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Literal     as LC
import qualified Language.K3.Core.Constructor.Type        as TC

import Language.K3.Parser.ProgramBuilder ( declareBuiltins )

import Language.K3.Analysis.Core hiding ( ScopeEnv )
import Language.K3.Analysis.HMTypes.Inference ( inferProgramTypes, translateProgramTypes )

import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

data OperatorFn = UnaryOp Operator ScalarExpr
                | BinaryOp Operator ScalarExpr ScalarExpr
                deriving (Eq, Show)

-- | Relation names and types.
type RTypeEnv = Map Identifier (K3 Type)

-- | Attribute dependency graph.
type ADGPtr = Int

data ADGNode = ADGNode { adnn  :: Identifier
                       , adnr  :: Identifier
                       , adne  :: Maybe ScalarExpr
                       , adnch :: [ADGPtr] }
              deriving (Eq, Show)

type ADGraph = Map ADGPtr ADGNode

-- | Relational-K3 type mapping, indicating
--   i. no type prefix
--   ii. common type path prefix for all fields
--   iii. per-field full paths
type TypePath      = [Identifier]
type TypePathMap   = Map Identifier TypePath
type TLabelMapping = Maybe (Either Identifier TypePathMap)

-- | Relational value-type mappings, indicating a relational type as a
--   i. single variable-and-type mapping
--   ii. per-field variable-and-type mapping
type VTMapping = (Identifier, TLabelMapping)
type FieldVTMap = Map Identifier VTMapping
data TValueMapping = TVMType      TLabelMapping
                   | TVMNamed     VTMapping
                   | TVMComposite FieldVTMap
                   deriving (Eq, Show)

-- | Mapped relational types.
type MRType  = (K3 Type, TLabelMapping, [ADGPtr])
type MERType = (K3 Type, TValueMapping)

-- | Qualified attribute env
type AQEnv = Map Identifier MRType

-- | Attribute pointer env
type ADGPtrEnv = ([ADGPtr], [(Identifier, [ADGPtr])])

-- | Attribute binding env
data AEnv = AEnv { aeuq :: MERType, aeq :: AQEnv }
            deriving (Eq, Show)

data ParseResult = ParseResult { pexpr  :: K3 Expression
                               , prt    :: K3 Type
                               , pkt    :: K3 Type
                               , ptmap  :: TLabelMapping
                               , palias :: Maybe Identifier
                               , pptrs  :: [ADGPtr] }
                  deriving (Eq, Show)

-- | Scope environments.
type ScopePtr = Int
type ScopeEnv = Map ScopePtr AEnv

-- | Parsing state environment.
data SQLEnv = SQLEnv { relations :: RTypeEnv
                     , aliassym  :: ParGenSymS
                     , adgraph   :: ADGraph
                     , adpsym    :: ParGenSymS
                     , scopeenv  :: ScopeEnv
                     , spsym     :: ParGenSymS
                     , stgsym    :: ParGenSymS
                     , slblsym   :: ParGenSymS }
            deriving (Eq, Show)

-- | A stateful SQL parsing monad.
type SQLParseM = ExceptT String (State SQLEnv)


{- Naming helpers. -}

materializeId :: Identifier -> Identifier
materializeId n = "output" ++ n

stageId :: Identifier -> Identifier
stageId n = "stage" ++ n


{- Data.Text helpers -}

sqlenv0 :: SQLEnv
sqlenv0 = SQLEnv Map.empty contigsymS Map.empty contigsymS Map.empty contigsymS contigsymS contigsymS

-- | Relation type accessors
stlkup :: RTypeEnv -> Identifier -> Except String (K3 Type)
stlkup env x = maybe err return $ Map.lookup x env
  where err = throwE $ "Unknown relation in sql parser environment: " ++ show x

stext :: RTypeEnv -> Identifier -> K3 Type -> RTypeEnv
stext env x t = Map.insert x t env

stdel :: RTypeEnv -> Identifier -> RTypeEnv
stdel env x = Map.delete x env

-- | Dependency graph accessors.
sglkup :: ADGraph -> ADGPtr -> Except String ADGNode
sglkup g p = maybe err return $ Map.lookup p g
  where err = throwE $ "Unknown attribute node in sql parser environment: " ++ show p

sgext :: ADGraph -> ADGNode -> ParGenSymS -> (ADGPtr, ParGenSymS, ADGraph)
sgext g n sym = (ptr, nsym, Map.insert ptr n g)
  where (nsym, ptr) = gensym sym

-- | Scope environment accessors.
sclkup :: ScopeEnv -> ScopePtr -> Except String AEnv
sclkup e p = maybe err return $ Map.lookup p e
  where err = throwE $ "Unknown scope in sql parser environment: " ++ show p

scext :: ScopeEnv -> AEnv -> ParGenSymS -> (ScopePtr, ParGenSymS, ScopeEnv)
scext se ae sym = (ptr, nsym, Map.insert ptr ae se)
  where (nsym, ptr) = gensym sym


-- | State accessors.
sqelkup :: SQLEnv -> Identifier -> Except String (K3 Type)
sqelkup senv n = stlkup (relations senv) n

sqeext :: SQLEnv -> Identifier -> K3 Type -> SQLEnv
sqeext senv n t = senv { relations = stext (relations senv) n t }

sqedel :: SQLEnv -> Identifier -> SQLEnv
sqedel senv n = senv { relations = stdel (relations senv) n }

sqglkup :: SQLEnv -> ADGPtr -> Except String ADGNode
sqglkup senv p = sglkup (adgraph senv) p

sqgext :: SQLEnv -> ADGNode -> (ADGPtr, SQLEnv)
sqgext senv n = (ptr, senv {adgraph = ng, adpsym = nsym})
  where (ptr, nsym, ng) = sgext (adgraph senv) n (adpsym senv)

sqclkup :: SQLEnv -> ScopePtr -> Except String AEnv
sqclkup senv p = sclkup (scopeenv senv) p

sqcext :: SQLEnv -> AEnv -> (ScopePtr, SQLEnv)
sqcext senv n = (ptr, senv {scopeenv = ne, spsym = nsym})
  where (ptr, nsym, ne) = scext (scopeenv senv) n (spsym senv)

sqsext :: SQLEnv -> (Int, SQLEnv)
sqsext senv = (n, senv {stgsym = nsym})
  where (nsym, n) = gensym (stgsym senv)

sasext :: SQLEnv -> (Int, SQLEnv)
sasext senv = (n, senv {aliassym = nsym})
  where (nsym, n) = gensym (aliassym senv)

slblsext :: SQLEnv -> (Int, SQLEnv)
slblsext senv = (n, senv {slblsym = nsym})
  where (nsym, n) = gensym (slblsym senv)

-- | Monadic accessors.
sqelkupM :: Identifier -> SQLParseM (K3 Type)
sqelkupM n = get >>= liftExceptM . (\env -> sqelkup env n)

sqeextM :: Identifier -> K3 Type -> SQLParseM ()
sqeextM n t = get >>= \env -> return (sqeext env n t) >>= put

sqglkupM :: ADGPtr -> SQLParseM ADGNode
sqglkupM p = get >>= liftExceptM . (\env -> sqglkup env p)

sqgextM :: ADGNode -> SQLParseM ADGPtr
sqgextM n = get >>= \env -> return (sqgext env n) >>= \(r, nenv) -> put nenv >> return r

sqclkupM :: ScopePtr -> SQLParseM AEnv
sqclkupM p = get >>= liftExceptM . (\env -> sqclkup env p)

sqcextM :: AEnv -> SQLParseM ScopePtr
sqcextM n = get >>= \env -> return (sqcext env n) >>= \(r, nenv) -> put nenv >> return r

sqsextM :: SQLParseM Int
sqsextM = get >>= \env -> return (sqsext env) >>= \(i, nenv) -> put nenv >> return i

sasextM :: SQLParseM Int
sasextM = get >>= \env -> return (sasext env) >>= \(i, nenv) -> put nenv >> return i

slblsextM :: SQLParseM Int
slblsextM = get >>= \env -> return (slblsext env) >>= \(i, nenv) -> put nenv >> return i

{- Attribute dependency graph accessors. -}

-- | Inverse indexing for attribute nodes.
sqgidxM :: [ADGPtr] -> SQLParseM (Map Identifier ADGPtr)
sqgidxM ptrs = do
  l <- mapM (\p -> sqglkupM p >>= \node -> return (adnn node, p)) ptrs
  return $ Map.fromList l


-- | Adds base relation attributes to the ADG.
sqgextSchemaM :: Maybe Identifier -> K3 Type -> SQLParseM [ADGPtr]
sqgextSchemaM (Just n) t = do
    rt <- telemM t
    case tag rt of
      TRecord ids -> mapM sqgextM $ map (\i -> ADGNode i n Nothing []) ids
      _ -> throwE $ boxToString $ ["Invalid relational element type"] %$ prettyLines rt

sqgextSchemaM _ _ = throwE "No relation name specified when extending attribute graph"

-- | Adds unchanged derived schema attributes to the ADG
sqgextIdentityM :: Maybe Identifier -> K3 Type -> [ADGPtr] -> SQLParseM [ADGPtr]
sqgextIdentityM (Just n) t ptrs = do
  rt <- telemM t
  case tag rt of
    TRecord ids -> mapM sqgextM $ map (\(i, p) -> ADGNode i n (Just $ Identifier emptyAnnotation $ Nmc i) [p])
                                $ zip ids ptrs
    _ -> throwE $ boxToString $ ["Invalid relational element type"] %$ prettyLines rt

sqgextIdentityM _ _ _ = throwE "Invalid identity arguments when extending attribute graph"

-- | Extend the attribute graph for a relation type derived directly from another relation.
sqgextAliasM :: Maybe Identifier -> K3 Type -> K3 Type -> [ADGPtr] -> SQLParseM [ADGPtr]
sqgextAliasM (Just n) (tag -> TRecord dstids) (tag -> TRecord srcids) inputptrs
  | length dstids == length srcids
  = do { idx <- sqgidxM inputptrs;
         nodes <- mapM (mknode idx) $ zip dstids srcids;
         mapM sqgextM nodes }

  where mknode ptridx (d, s) = do
          p <- maybe (lkuperr s) return $ Map.lookup s ptridx
          return $ ADGNode d n (Just $ Identifier emptyAnnotation $ Nmc s) [p]

        lkuperr v = throwE $ "No attribute graph node found for " ++ v

sqgextAliasM _ _ _ _ = throwE "Invalid alias arguments when extending attribute graph"

-- | Extend the attribute graph for a relation type computed from the given expressions.
sqgextExprM :: String -> Maybe Identifier -> K3 Type -> [ScalarExpr] -> ADGPtrEnv -> SQLParseM [ADGPtr]
sqgextExprM usetag (Just n) t exprs penv = do
  rt <- telemM t
  case tag rt of
    TRecord ids | length ids == length exprs -> do
      uidx <- sqgidxM $ fst penv
      qidx <- Map.fromList <$> mapM (\(i,ps) -> sqgidxM ps >>= return . (i,)) (snd penv)
      nodes <- mapM (mknode uidx qidx) (zip ids exprs)
      mapM sqgextM nodes

    _ -> throwE $ boxToString $ ["Invalid relational element type for " ++ usetag ++ " attributes"]
                              %$ prettyLines rt

  where mknode uidx qidx (i, e) = do
          let (uv, qv) = extractvars e
          ps <- mapM (lkupu uidx) $ uv
          qps <- mapM (lkupq qidx) qv
          return $ ADGNode i n (Just e) $ ps ++ qps

        lkupu uidx v = maybe (lkuperr v) return $ Map.lookup v uidx
        lkupq qidx [x,y] = maybe (lkuperr x) (\idx -> lkupu idx y) $ Map.lookup x qidx
        lkupq _ _ = throwE $ "Invalid pair qualified identifer"

        lkuperr v = throwE $ "No attribute graph node found for " ++ v

sqgextExprM _ _ _ _ _ = throwE "Invalid expr arguments when extending attribute graph"

-- | Returns the set of pointers present in an pointer environment.
adgproots :: ADGPtrEnv -> [ADGPtr]
adgproots penv = nub $ fst penv ++ (concatMap snd $ snd penv)

-- | Chases a given attribute pointer to its set of source nodes.
adgchaseM :: ADGPtr -> SQLParseM [ADGNode]
adgchaseM p = get >>= \env -> aux (adgraph env) [] p
  where aux g path n = case Map.lookup n g of
          Nothing -> lookuperr n
          Just node ->
            if null $ adnch node then return [node]
            else
              let nch   = filter (`notElem` path) $ adnch node
                  npath = path ++ nch
              in mapM (aux g npath) nch >>= return . concat

        lookuperr n = throwE $ "No attribute graph node found for " ++ show n


{- SQLParseM helpers. -}

runSQLParseM :: SQLEnv -> SQLParseM a -> (Either String a, SQLEnv)
runSQLParseM env m = flip runState env $ runExceptT m

runSQLParseEM :: SQLEnv -> SQLParseM a -> Either String (a, SQLEnv)
runSQLParseEM env m = r >>= return . (,e)
  where (r,e) = runSQLParseM env m

evalSQLParseM :: SQLEnv -> SQLParseM a -> Either String a
evalSQLParseM env m = fst $ runSQLParseM env m

reasonM :: (String -> String) -> SQLParseM a -> SQLParseM a
reasonM errf = mapExceptT $ \m -> m >>= \case
  Left  err -> return $ Left $ errf err
  Right r   -> return $ Right r

errorM :: String -> SQLParseM a
errorM msg = reasonM id $ throwE msg

liftExceptM :: Except String a -> SQLParseM a
liftExceptM = mapExceptT (return . runIdentity)

liftEitherM :: Either String a -> SQLParseM a
liftEitherM = either throwE return


{- Attribute environment helpers. -}

mrtype :: ParseResult -> SQLParseM MRType
mrtype r = telemM (prt r) >>= \ret -> return (ret, ptmap r, pptrs r)

mertypeR :: K3 Type -> TLabelMapping -> Maybe Identifier -> SQLParseM MERType
mertypeR t tmap alias = do
  ret <- telemM t
  return (ret, maybe (TVMType tmap) (\i -> TVMNamed (i, tmap)) alias)

mertype :: ParseResult -> SQLParseM MERType
mertype r = mertypeR (prt r) (ptmap r) $ palias r

aqenv0 :: [(Identifier, MRType)] -> AQEnv
aqenv0 l = Map.fromList l

aqenv1 :: ParseResult -> SQLParseM AQEnv
aqenv1 r = do
  mrt <- mrtype r
  return $ maybe Map.empty (\i -> aqenv0 [(i, mrt)]) $ palias r

aqenvl :: [ParseResult] -> SQLParseM AQEnv
aqenvl l = mapM aqenv1 l >>= return . mconcat

adgenv1R :: [ADGPtr] -> Maybe Identifier -> SQLParseM ADGPtrEnv
adgenv1R ptrs al = return (ptrs, maybe [] (\i -> [(i, ptrs)]) al)

adgenv1 :: ParseResult -> SQLParseM ADGPtrEnv
adgenv1 r = return (pptrs r, maybe [] (\i -> [(i, pptrs r)]) $ palias r)

adgenvql :: [ParseResult] -> SQLParseM [(Identifier, [ADGPtr])]
adgenvql rl = return $ concatMap (\r -> maybe [] (\i -> [(i, pptrs r)]) $ palias r) rl

aenv0 :: MERType -> AQEnv -> AEnv
aenv0 u q = AEnv u q

aenv1 :: ParseResult -> SQLParseM (AEnv, ADGPtrEnv)
aenv1 r = (\mert q p -> (aenv0 mert q, p)) <$> mertype r <*> aqenv1 r <*> adgenv1 r

aenvsub :: Identifier -> AEnv -> SQLParseM AEnv
aenvsub i (AEnv _ q) = do
    imrt@(rt, tmap, _) <- mrt
    rct <- tcolM rt
    mert <- mertypeR rct tmap $ Just i
    return $ aenv0 mert $ aqenv0 [(i,imrt)]

  where mrt = maybe mrterr return $ Map.lookup i q
        mrterr = throwE $ "Invalid sub aenv qualifier " ++ show i

aenvl :: [ParseResult] -> SQLParseM (AEnv, ADGPtrEnv)
aenvl l = (\(mert, p) q pq -> (aenv0 mert q, (p, pq))) <$> mr0 <*> aqenvl l <*> adgenvql l
  where mr0 = do
          merl <- mapM mertype l
          let (ids, t, tvm) = concatmer merl
          p <- pp0 ids
          return ((t,tvm),p)

        pp0 uids = do
          idx <- sqgidxM $ concatMap pptrs l
          mapM (\i -> maybe (lkuperr i) return $ Map.lookup i idx) $ Set.toList uids

        concatmer merl =
          let (uniqids, idt, idvt) = foldl accmer (Set.empty, [], []) merl
              uidt  = filter ((`Set.member` uniqids) . fst) idt
              uidvt = concatMap (fieldvt uniqids) idvt
          in (uniqids, recT uidt, TVMComposite $ Map.fromList uidvt)

        accmer (accS, accL, tvmaccL) (tnc -> (TRecord ids, ch), tvm) =
          let new     = Set.fromList ids
              common  = Set.intersection accS new
              newaccS = Set.difference (Set.union accS new) common
          in (newaccS, accL ++ zip ids ch, tvmaccL ++ map (, extractvt tvm) ids)

        accmer acc _ = acc

        fieldvt uniqids (i,vt) = if i `Set.member` uniqids then maybe [] (\x -> [(i,x)]) vt else []

        extractvt (TVMNamed vt) = Just vt
        extractvt _ = Nothing

        lkuperr v = throwE $ "No attribute graph node found for " ++ v


{- SQL AST helpers. -}

projectionexprs :: SelectItem -> ScalarExpr
projectionexprs (SelExp _ e) = e
projectionexprs (SelectItem _ e _) = e

-- TODO: correlated variables in subqueries.
extractvars :: ScalarExpr -> ([Identifier], [[Identifier]])
extractvars e = case e of
  (Identifier _ (sqlnmcomponent -> i)) -> ([i], [])

  (QIdentifier _ nmcl) -> case nmcl of
                            [x,y] -> ([], [["__" ++ sqlnmcomponent x, sqlnmcomponent y]])
                            _ -> zero

  (FunCall _ _ args) -> maprcr args
  (Case _ whense elsee) ->
    many (\(whenel, thene) -> maprcr whenel `rconcat` extractvars thene) whense
      `rconcat` maybe zero extractvars elsee

  (CaseSimple _ ce whense elsee) ->
    extractvars ce
      `rconcat` many (\(whenel, thene) -> maprcr whenel `rconcat` extractvars thene) whense
      `rconcat` maybe zero extractvars elsee

  _ -> ([], [])

  where maprcr l = (concat *** concat) $ unzip $ map extractvars l
        many f l = (concat *** concat) $ unzip $ map f l
        rconcat (a,c) (b,d) = (a++b, c++d)
        zero = ([], [])


{- Type and alias helpers. -}

tblaliasM :: ParseResult -> SQLParseM Identifier
tblaliasM (palias -> (Just a)) = return a
tblaliasM _ = throwE $ "Invalid table alias"

expraliasesGenM :: Int -> [ParseResult] -> SQLParseM (Int, [Identifier])
expraliasesGenM start l = foldM ensure (start,[]) l
  where ensure (i, acc) r = return $ maybe (i+1, acc ++ ["f" ++ show i]) (\a -> (i,acc++[a])) $ palias r

telemM :: K3 Type -> SQLParseM (K3 Type)
telemM (tnc -> (TCollection, [t])) = return t
telemM t = throwE $ boxToString $ ["Invalid relation type"] %$ prettyLines t

tcolM :: K3 Type -> SQLParseM (K3 Type)
tcolM t = return $ (TC.collection t) @<- [TAnnotation "Collection", TImmutable]

taliaselemM :: TableAlias -> K3 Type -> SQLParseM (K3 Type)
taliaselemM alias t@(tnc -> (TRecord ids, ch)) =
  case alias of
    NoAlias _      -> return t
    TableAlias _ _ -> return t
    FullAlias _ _ fnc | length fnc == length ids -> return $ recT $ zip (map sqlnmcomponent fnc) ch
    FullAlias _ _ _ -> throwE $ "Mismatched alias fields length"

taliaselemM _ t = throwE $ boxToString $ ["Invalid relation element type"] %$ prettyLines t

taliascolM :: TableAlias -> K3 Type -> SQLParseM (K3 Type)
taliascolM alias t@(tnc -> (TCollection, [et@(tag -> TRecord _)])) =
  taliaselemM alias et >>= \net -> return $ (TC.collection net) @<- (annotations t)

taliascolM _ t = throwE $ boxToString $ ["Invalid relation type"] %$ prettyLines t

-- | Wraps a K3 record type with a 'elem' label
twrapelemM :: K3 Type -> SQLParseM (K3 Type)
twrapelemM t@(tag -> TRecord _) = return $ recT [("elem", t)]
twrapelemM t = throwE $ boxToString $ ["Invalid element type for wrapping:"] %$ prettyLines t

-- | Wraps a collection's element type with an 'elem' label
twrapcolelemM :: K3 Type -> SQLParseM (K3 Type)
twrapcolelemM t@(tnc -> (TCollection, [et])) = twrapelemM et >>= \net -> return $ (TC.collection net) @<- annotations t
twrapcolelemM t = throwE $ boxToString $ ["Invalid collection type for wrapping:"] %$ prettyLines t

-- | Creates a type map for an 'elem' wrapped record.
telemtmapM :: SQLParseM TLabelMapping
telemtmapM = return $ Just $ Left "elem"

tresolvetmapM :: Identifier -> TLabelMapping -> SQLParseM [Identifier]
tresolvetmapM i tlm = maybe (return []) resolve tlm
  where resolve (Left j) = return [j, i]
        resolve (Right tpm) = maybe resolveerr return $ Map.lookup i tpm
        resolveerr = throwE $ "Unknown type mapping field " ++ i


{- Expression construction helpers -}

-- | Property construction helper
sqlAttrProp :: ADGPtr -> Annotation Expression
sqlAttrProp p = EProperty $ Left ("SQLAttr", Just $ LC.int p)

sqlPred1Prop :: Annotation Expression
sqlPred1Prop = EProperty $ Left ("SQLPred1", Nothing)

sqlPredEquiProp :: Annotation Expression
sqlPredEquiProp = EProperty $ Left ("SQLPredEqui", Nothing)

sqlAEnvProp :: ScopePtr -> Annotation Expression
sqlAEnvProp p = EProperty $ Left ("SQLAEnv", Just $ LC.int p)

sqlPEnvProp :: ADGPtrEnv -> Annotation Expression
sqlPEnvProp penv = EProperty $ Left ("SQLPEnv", Just $ LC.string $ show penv)

sqlStagedProp :: String -> Annotation Expression
sqlStagedProp n = EProperty $ Left ("Staged", Just $ LC.string n)

sqlMaterializedProp :: Annotation Expression
sqlMaterializedProp = EProperty $ Left ("Materialized", Nothing)

sqlQueryResultProp :: String -> Annotation Expression
sqlQueryResultProp n = EProperty $ Left ("QueryResult", Just $ LC.string n)

sqlAggMergeProp :: K3 Expression -> Annotation Expression
sqlAggMergeProp f = EProperty $ Left ("AggMerge", Just $ LC.string $ show f)

isSqlAttrProp :: Annotation Expression -> Bool
isSqlAttrProp (EProperty (Left ("SQLAttr", _))) = True
isSqlAttrProp _ = False

isSqlPred1Prop :: Annotation Expression -> Bool
isSqlPred1Prop (EProperty (Left ("SQLPred1", _))) = True
isSqlPred1Prop _ = False

isSqlPredEquiProp :: Annotation Expression -> Bool
isSqlPredEquiProp (EProperty (Left ("SQLPredEqui", _))) = True
isSqlPredEquiProp _ = False

isSqlAEnvProp :: Annotation Expression -> Bool
isSqlAEnvProp (EProperty (Left ("SQLAEnv", _))) = True
isSqlAEnvProp _ = False

isSqlPEnvProp :: Annotation Expression -> Bool
isSqlPEnvProp (EProperty (Left ("SQLPEnv", _))) = True
isSqlPEnvProp _ = False

isSqlStagedProp :: Annotation Expression -> Bool
isSqlStagedProp (EProperty (Left ("Staged", _))) = True
isSqlStagedProp _ = False

isSqlMaterializedProp :: Annotation Expression -> Bool
isSqlMaterializedProp (EProperty (Left ("Materialized", _))) = True
isSqlMaterializedProp _ = False

isSqlQueryResultProp :: Annotation Expression -> Bool
isSqlQueryResultProp (EProperty (Left ("QueryResult", _))) = True
isSqlQueryResultProp _ = False

isSqlAggMergeProp :: Annotation Expression -> Bool
isSqlAggMergeProp (EProperty (Left ("AggMerge", _))) = True
isSqlAggMergeProp _ = False


immutT :: K3 Type -> K3 Type
immutT t = t @<- ((filter (not . isTQualified) $ annotations t) ++ [TImmutable])

mutT :: K3 Type -> K3 Type
mutT t = t @<- ((filter (not . isTQualified) $ annotations t) ++ [TMutable])

immutE :: K3 Expression -> K3 Expression
immutE e = e @<- ((filter (not . isEQualified) $ annotations e) ++ [EImmutable])

mutE :: K3 Expression -> K3 Expression
mutE e = e @<- ((filter (not . isEQualified) $ annotations e) ++ [EMutable])

tupE :: [K3 Expression] -> K3 Expression
tupE [e] = immutE e
tupE el = EC.tuple $ map immutE el

recE :: [(Identifier, K3 Expression)] -> K3 Expression
recE ide = EC.record $ map (\(i,e) -> (i, immutE e)) ide

recT :: [(Identifier, K3 Type)] -> K3 Type
recT idt = TC.record $ map (\(i,t) -> (i, immutT t)) idt

projectE :: Identifier -> Identifier -> (Identifier, K3 Expression)
projectE i f = (f, EC.project f $ EC.variable i)

projectPathE :: K3 Expression -> [Identifier] -> K3 Expression
projectPathE e p = foldl (\accE i -> EC.project i accE) e p

fieldE :: Identifier -> TLabelMapping -> Identifier -> SQLParseM (K3 Expression)
fieldE v tmap i = do
  path <- tresolvetmapM i tmap
  return $ if null path then EC.project i $ EC.variable v
                        else projectPathE (EC.variable v) path

namedRecordE :: Identifier -> TLabelMapping -> [Identifier] -> SQLParseM (K3 Expression)
namedRecordE v (Just (Left prefix)) _ = return $ EC.project prefix $ EC.variable v
namedRecordE v tmap ids = foldM field [] ids >>= return . recE
  where field acc i = fieldE v tmap i >>= \f -> return $ acc ++ [(i,f)]


compositeRecordE :: FieldVTMap -> [Identifier] -> SQLParseM (K3 Expression)
compositeRecordE fvm ids = foldM fieldOpt [] ids >>= return . recE
  where fieldOpt acc i = maybe (compositeErr i) (field acc i) $ Map.lookup i fvm
        field acc i (v, tmap) = fieldE v tmap i >>= \f -> return $ acc ++ [(i,f)]

        compositeErr i = throwE $ "Cannot find field-specific mapping for " ++ i


bindE :: AEnv -> Identifier -> K3 Expression -> SQLParseM (Maybe Identifier, K3 Expression)
bindE (AEnv (tag -> TRecord ids, tvm) _) backupvar e = do
  case tvm of
    TVMType tmap -> do
      initE <- namedRecordE backupvar tmap ids
      return (Just backupvar, EC.bindAs initE (BRecord $ zip ids ids) e)

    TVMNamed (i, tmap) -> do
      initE <- namedRecordE i tmap ids
      return (Just i, EC.bindAs initE (BRecord $ zip ids ids) e)

    TVMComposite fvm -> do
      initE <- compositeRecordE fvm ids
      return (Nothing, EC.bindAs initE (BRecord $ zip ids ids) e)

bindE (AEnv (t, _) _) _ _ = throwE $ boxToString $ ["Unable to bind type"] %$ prettyLines t


concatE :: AEnv -> SQLParseM (K3 Expression)
concatE (AEnv (tag -> TRecord ids, tvm) _) = do
  case tvm of
    TVMType _ -> throwE $ "Cannot concat type-only attribute env"
    TVMNamed (i, tmap) -> namedRecordE i tmap ids
    TVMComposite fvm -> compositeRecordE fvm ids

concatE (AEnv (t, _) _) = throwE $ boxToString $ ["Unable to concat type"] %$ prettyLines t


zeroE :: [(Identifier, K3 Type)] -> SQLParseM (K3 Expression)
zeroE [] = return EC.unit
zeroE [(_,t)] = either throwE return $ defaultExpression t
zeroE l = either throwE return $ defaultExpression $ recT l


aggE :: Identifier -> (Identifier, K3 Expression) -> K3 Expression
aggE i (f, e) = EC.applyMany e [EC.project f $ EC.variable i]

aggMergeE :: Identifier -> Identifier -> (Identifier, K3 Expression) -> K3 Expression
aggMergeE i j (f, mergeF) = EC.applyMany mergeF [EC.project f $ EC.variable i, EC.project f $ EC.variable j]


-- TODO: gensym
memoE :: K3 Expression -> (K3 Expression -> K3 Expression) -> K3 Expression
memoE srcE bodyF = case tag srcE of
  EConstant _ -> bodyF srcE
  EVariable _ -> bodyF srcE
  _ -> EC.letIn "__memo" (immutE srcE) $ bodyF $ EC.variable "__memo"


matchE :: K3 Expression -> K3 Expression -> K3 Expression
matchE elemexpr colexpr =
  memoE (immutE elemexpr) $ \e -> EC.applyMany (EC.project "filter" $ colexpr) [matchF e]
  where matchF e = EC.lambda "__x" $ EC.binop OEqu (EC.variable "__x") e

memberE :: Bool -> K3 Expression -> K3 Expression -> K3 Expression
memberE asMem elemexpr colexpr  = emptyE (not asMem) $ matchE elemexpr colexpr

emptyE :: Bool -> K3 Expression -> K3 Expression
emptyE asEmpty colexpr = EC.binop (if asEmpty then OEqu else ONeq)
  (EC.applyMany (EC.project "size" colexpr) [EC.unit]) $ EC.constant $ CInt 0


{- Parsing toplevel. -}
sqlstmt :: Statement -> SQLParseM [K3 Declaration]
sqlstmt (CreateTable _ nm attrs _) = do
  t <- sqltabletype attrs
  wt <- twrapcolelemM t
  sqeextM (sqlnm nm) t
  return [DC.global (sqlnm nm) wt Nothing]

sqlstmt (QueryStatement _ query) = sqlquerystmt query
sqlstmt s = throwE $ "Unimplemented SQL stmt: " ++ show s


sqlquerystmt :: QueryExpr -> SQLParseM [K3 Declaration]
sqlquerystmt q = do
  qpr <- sqlquery q
  qexpr <- sqloptimize $ pexpr qpr
  s <- sqsextM >>= return . show
  let outid = materializeId s
  let trigid = stageId s
  return [ DC.global outid (mutT $ pkt qpr) Nothing
         , DC.trigger trigid TC.unit $ EC.lambda "_" $
            (EC.assign outid (qexpr @+ sqlStagedProp trigid)) @+ (sqlQueryResultProp trigid) ]


-- | Expression construction, and inlined type inference.
sqlquery :: QueryExpr -> SQLParseM ParseResult
sqlquery (Select _ distinct selectL tableL whereE gbL havingE orderL limitE offsetE) = do
  tables  <- sqltablelist tableL
  selects <- sqlwhere tables whereE
  (groupby, nselectL) <- sqlgroupby selects selectL havingE gbL
  sorted  <- sqlsort groupby orderL
  limited <- sqltopk sorted limitE offsetE
  sqlproject limited nselectL

sqlquery q = throwE $ "Unhandled query " ++ show q


sqltablelist :: TableRefList -> SQLParseM ParseResult
sqltablelist [] = throwE $ "Empty from clause"    -- TODO: empty collection?
sqltablelist [x] = sqltableexpr x
sqltablelist (h:t) = do
  hpr <- sqltableexpr h
  foldM cartesian_product hpr t

  where cartesian_product lpr r = do
          rpr <- sqltableexpr r
          (aenv@(AEnv (ct, _) _), penv) <- aenvl [lpr, rpr]
          (al, ar) <- (,) <$> tblaliasM lpr <*> tblaliasM rpr
          sptr <- sqcextM aenv
          ce   <- concatE aenv
          rt   <- tcolM ct
          kt   <- twrapcolelemM rt
          tmap <- telemtmapM
          let matchF   = EC.lambda "_" $ EC.lambda "_" $ EC.constant $ CBool True
          let combineF = EC.lambda al $ EC.lambda ar ce
          let jexpr    = EC.applyMany (EC.project "join" $ pexpr lpr) [pexpr rpr, matchF, combineF]
          let rexpr    = (jexpr @+ sqlPEnvProp penv) @+ sqlAEnvProp sptr
          let tid = Just "__CP"
          aptrs <- sqgextAliasM tid ct ct $ fst penv
          return $ ParseResult rexpr rt kt tmap tid aptrs


sqltableexpr :: TableRef -> SQLParseM ParseResult
sqltableexpr (Tref _ nm al) = do
    let tid = sqltablealias ("__" ++ sqlnm nm) al
    t  <- sqelkupM tnm
    rt <- taliascolM al t
    kt <- twrapcolelemM rt
    tmap <- telemtmapM
    aptrs <- sqgextSchemaM tid rt
    return $ ParseResult (EC.variable tnm) rt kt tmap tid aptrs
  where tnm = sqlnm nm

-- TODO: nat, jointy
sqltableexpr (JoinTref _ jlt nat jointy jrt onE jal) = do
  lpr <- sqltableexpr jlt
  rpr <- sqltableexpr jrt
  (aenv@(AEnv (ct, _) _), penv) <- aenvl [lpr, rpr]
  (al, ar) <- (,) <$> tblaliasM lpr <*> tblaliasM rpr
  sptr   <- sqcextM aenv
  joinpr <- maybe (return joinpr0) (sqljoinexpr aenv penv) onE
  ce     <- concatE aenv
  rt     <- tcolM ct >>= taliascolM jal
  kt     <- twrapcolelemM rt
  tmap   <- telemtmapM
  (Nothing, be) <- bindE aenv "x" (pexpr joinpr)
  let matchF   = EC.lambda al $ EC.lambda ar be
  let combineF = EC.lambda al $ EC.lambda ar ce
  let jexpr    = EC.applyMany (EC.project "join" $ pexpr lpr) [pexpr rpr, matchF, combineF]
  let rexpr    = (jexpr @+ sqlPEnvProp penv) @+ sqlAEnvProp sptr
  jrsym <- sasextM
  let tid = sqltablealias ("__JR" ++ show jrsym) jal
  ret   <- telemM rt
  aptrs <- sqgextAliasM tid ret ct $ fst penv
  return $ ParseResult rexpr rt kt tmap tid aptrs

  where joinpr0 = ParseResult (EC.constant $ CBool True) TC.bool TC.bool Nothing Nothing []


sqltableexpr (SubTref _ query al) = do
  qpr  <- sqlquery query
  rt   <- taliascolM al (prt qpr)
  kt   <- twrapcolelemM rt
  tmap <- telemtmapM

  (aenv@(AEnv (tag -> TRecord ids, _) _), penv) <- aenv1 qpr
  rnsym <- sasextM
  let tid = sqltablealias ("__RN" ++ show rnsym) al
  ret <- telemM rt
  sqet <- telemM $ prt qpr
  aptrs <- sqgextAliasM tid ret sqet $ fst penv
  case tag ret of
    TRecord nids | ids == nids -> return qpr { palias = tid, pptrs = aptrs }

    TRecord nids | length ids == length nids -> do
      (iOpt, be) <- bindE aenv "x" $ recE $ map (\(n,o) -> (n, EC.variable o)) $ zip nids ids
      mapF <- maybe (tblaliasM qpr) return iOpt >>= \a -> return $ EC.lambda a be
      let rexpr = EC.applyMany (EC.project "map" $ pexpr qpr) [mapF]
      return $ ParseResult rexpr rt kt tmap tid aptrs

    _ -> throwE $ "Invalid subquery alias"

sqltableexpr t@(FunTref _ funE _)  = throwE $ "Unhandled table ref: " ++ show t


sqlwhere :: ParseResult -> MaybeBoolExpr -> SQLParseM ParseResult
sqlwhere tables whereEOpt = flip (maybe $ return tables) whereEOpt $ \whereE -> do
  (aenv, penv) <- aenv1 tables
  wpr <- sqlscalar aenv penv whereE
  (onesided, equi, remdr) <- cascadePredicate $ pexpr wpr
  foldM mkFilterL tables [ (onesided, Just sqlPred1Prop)
                         , (equi, Just sqlPredEquiProp)
                         , maybe ([], Nothing) (\e -> ([e], Nothing)) remdr ]
  where
    mkFilterL accpr (el, classp) = foldM (mkFilter classp) accpr el
    mkFilter classp inpr predicate = do
      (aenv, penv) <- aenv1 inpr
      (iOpt, be) <- bindE aenv "x" predicate
      filterF <- maybe (tblaliasM inpr) return iOpt >>= \a -> return $ EC.lambda a be
      aptrs <- sqgextIdentityM (palias inpr) (prt inpr) $ fst penv
      let fexpr = EC.applyMany (EC.project "filter" $ pexpr inpr) [filterF]
      let rexpr = maybe fexpr (fexpr @+) classp
      return $ inpr { pexpr = rexpr, pptrs = aptrs }


sqlproject :: ParseResult -> SelectList -> SQLParseM ParseResult
sqlproject limits (SelectList _ projections) = do
  (aenv, penv) <- aenv1 limits
  aggsE <- mapM (sqlaggregate aenv penv) projections

  case partitionEithers aggsE of
    ([], prs) -> aggregate aenv penv aggsE $ map (\(a,_,_) -> a) prs
    (prjl, []) -> project aenv penv prjl
    (_, _) -> throwE $ "Invalid mixed select list of aggregates and non-aggregates"

  where
    aggregate aenv penv aggsE aggprs = do
      let (aggexprs, aggtypes) = unzip $ map (pexpr &&& prt) aggprs
      (_, aAliases) <- expraliasesGenM 0 aggprs
      let aidt = zip aAliases aggtypes
      (aiOpt, abe) <- bindE aenv "x" $ case aggexprs of
                        []  -> EC.variable "acc"
                        [e] -> EC.applyMany e [EC.variable "acc"]
                        _   -> recE $ zip aAliases $ map (aggE "acc") $ zip aAliases aggexprs

      aggF <- maybe (tblaliasM limits) return aiOpt >>= \a -> return $ EC.lambda "acc" $ EC.lambda a abe
      zF   <- zeroE aidt

      let rexpr = EC.applyMany (EC.project "fold" $ pexpr limits) [aggF, zF]
      let ral = Just "__R"
      rt <- tcolM $ recT aidt
      let aggprojections = zip aggsE $ map projectionexprs projections
      let sqlaggs = concatMap (\(ae, e) -> if isLeft ae then [] else [e]) aggprojections
      aptrs <- sqgextExprM "aggregate" ral rt sqlaggs penv
      return $ ParseResult rexpr rt rt Nothing ral aptrs

    project aenv penv prjl = do
      eprs <- mapM (sqlprojection aenv penv) prjl
      (_, eAliases) <- expraliasesGenM 0 eprs
      let (exprs, types) = unzip $ map (pexpr &&& prt) eprs
      (iOpt, be) <- bindE aenv "x" $ recE $ zip eAliases exprs
      mapF <- maybe (tblaliasM limits) return iOpt >>= \a -> return $ EC.lambda a be
      let rexpr  = EC.applyMany (EC.project "map" $ pexpr limits) [mapF]
      let ralias = Just "__R"
      rt   <- tcolM $ recT $ zip eAliases types
      kt   <- twrapcolelemM rt
      tmap <- telemtmapM
      aptrs <- sqgextExprM "projection" ralias rt (map projectionexprs prjl) penv
      return $ ParseResult rexpr rt kt tmap ralias aptrs


sqlgroupby :: ParseResult -> SelectList -> MaybeBoolExpr -> ScalarExprList -> SQLParseM (ParseResult, SelectList)
sqlgroupby selects (SelectList slann projections) having [] =
  return (selects, SelectList slann $ projections ++ maybe [] (\e -> [SelExp emptyAnnotation e]) having)

sqlgroupby selects (SelectList slann projections) having gbL = do
  (aenv, penv) <- aenv1 selects
  gbprs <- mapM (sqlscalar aenv penv) gbL
  aggsE <- mapM (sqlaggregate aenv penv) projections
  let (nprojects, aggsqeprs) = foldl partitionEitherPairs ([], []) $ zip aggsE projections

  let (ugbprs, ugbsqlexprs) = unzip $ nubBy ((==) `on` fst) $ zip gbprs gbL
  let (uaggprs, uaggmerges, uaggsqlexprs) = unzip3 $ nubBy ((==) `on` (\(a,_,_) -> a)) aggsqeprs

  let (gbexprs, gbtypes)   = unzip $ map (pexpr &&& prt) ugbprs
  let (aggexprs, aggtypes) = unzip $ map (pexpr &&& prt) uaggprs

  (gaid, gAliases) <- expraliasesGenM 0 ugbprs
  (_, aAliases)    <- expraliasesGenM (if null ugbprs then 1 else gaid) uaggprs

  (hcnt, nhaving, havingaggs) <- maybe (return (0, Nothing, [])) (havingaggregates (zip aAliases uaggprs) aenv penv) having
  havingAggsE <- mapM (sqlaggregate aenv penv) havingaggs
  let ([], havingaggsqeprs) = foldl partitionEitherPairs ([], []) $ zip havingAggsE havingaggs

  let (uhvprs, uhmerges, uhsqlexprs) = unzip3 $ nubBy ((==) `on` (\(a,_,_) -> a)) havingaggsqeprs
  let (hvexprs, hvtypes) = unzip $ map (pexpr &&& prt) uhvprs
  let hAliases = if null havingaggs then [] else ["h" ++ show i | i <- [0..hcnt-1]]

  (giOpt, gbe) <- bindE aenv "x" $ case gbexprs of
                    [] -> EC.unit
                    [e] -> e
                    _ -> recE $ zip gAliases gbexprs

  groupF <- maybe (tblaliasM selects) return giOpt >>= \a -> return $ EC.lambda a gbe

  let auaggexprs = aggexprs ++ hvexprs
  let auaggtypes = aggtypes ++ hvtypes
  let auAliases  = aAliases ++ hAliases
  let aumerges   = uaggmerges ++ uhmerges

  (aiOpt, abe) <- bindE aenv "x" $ case auaggexprs of
                    []  -> EC.variable "acc"
                    [e] -> EC.applyMany e [EC.variable "acc"]
                    _   -> recE $ zip auAliases $ map (aggE "acc") $ zip auAliases auaggexprs

  aggF <- maybe (tblaliasM selects) return aiOpt >>= \a -> return $ EC.lambda "acc" $ EC.lambda a abe
  zF <- zeroE $ zip auAliases auaggtypes

  mergeF <- case aumerges of
              [] -> return $ EC.lambda "_" $ EC.lambda "_" $ EC.unit
              [e] -> return $ e
              _ -> return $ EC.lambda "acc1" $ EC.lambda "acc2" $ recE $ zip auAliases $
                      map (aggMergeE "acc1" "acc2") $ zip auAliases aumerges

  let tpmap p idtl = map (\(i,_) -> (i, [p, i])) idtl

  let gidt = zip gAliases gbtypes
  let (kidt, keyT, ktpl) = case gbtypes of
                             []  -> ([("f0", TC.unit)], TC.unit, [("f0", ["key"])])
                             [t] -> (gidt, t, [(fst $ head gidt, ["key"])])
                             _   -> (gidt, recT gidt, tpmap "key" gidt)

  let aid0 = if null ugbprs then "f1" else "f" ++ show gaid
  let aidt = zip auAliases auaggtypes
  let (vidt, valT, vtpl) = case auaggtypes of
                             []  -> ([(aid0, TC.unit)], TC.unit, [(aid0, ["value"])])
                             [t] -> (aidt, t, [(fst $ head aidt, ["value"])])
                             _   -> (aidt, recT aidt, tpmap "value" aidt)

  let rexpr = (EC.applyMany (EC.project "groupBy" $ pexpr selects) [groupF, aggF, zF])
                  @+ sqlAggMergeProp mergeF

  let tmap = Just $ Right $ (Map.fromList ktpl) <> (Map.fromList vtpl)
  let ral = Just "__R"

  rt <- tcolM $ recT $ kidt ++ vidt
  kt <- tcolM $ recT [("key", keyT), ("value", valT)]

  hrexpr <- havingexpr rt tmap ral (fst penv) aenv nhaving rexpr

  let exprdeps = ugbsqlexprs ++ (map projectionexprs $ uaggsqlexprs ++ uhsqlexprs)
  aptrs <- sqgextExprM "group-by" ral rt exprdeps penv

  let naggprojects = map (\(i,_) -> SelExp emptyAnnotation $ Identifier emptyAnnotation $ Nmc i) vidt
  return (ParseResult hrexpr rt kt tmap ral aptrs, SelectList slann $ nprojects ++ naggprojects)

  where havingexpr _ _ _ _ _ Nothing e = return e
        havingexpr rt tmap alias ptrs (AEnv _ q) (Just he) e = do
          nmert <- mertypeR rt tmap alias
          let aenv = AEnv nmert q
          penv <- adgenv1R ptrs alias
          hpr <- sqlscalar aenv penv he
          (iOpt, be) <- bindE aenv "x" (pexpr hpr)
          filterF <- maybe (return "__R") return iOpt >>= \a -> return $ EC.lambda a be
          return $ EC.applyMany (EC.project "filter" $ e) [filterF]

        havingaggregates aggaprs aenv penv e = do
          (a,b,c) <- extractaggregates aggaprs aenv penv 0 e
          nc <- mapM (\ae -> return $ SelExp emptyAnnotation ae) c
          return (a, Just b, nc)

        partitionEitherPairs (pacc,aacc) (e, sqe) = case e of
          Left si -> (pacc++[si], aacc)
          Right (pr,me,_) -> (pacc, aacc++[(pr,me,sqe)])


-- TODO
sqlsort :: ParseResult -> ScalarExprDirectionPairList -> SQLParseM ParseResult
sqlsort having orderL = return having

sqltopk :: ParseResult -> MaybeBoolExpr -> MaybeBoolExpr -> SQLParseM ParseResult
sqltopk sorted limitE offsetE = return sorted


sqljoinexpr :: AEnv -> ADGPtrEnv -> JoinExpr -> SQLParseM ParseResult
sqljoinexpr aenv penv (JoinOn _ e) = sqlscalar aenv penv e
sqljoinexpr _ _ je@(JoinUsing _ _) = throwE $ "Unhandled join expression" ++ show je

sqlaggregate :: AEnv -> ADGPtrEnv -> SelectItem
             -> SQLParseM (Either SelectItem (ParseResult, K3 Expression, Maybe (K3 Expression)))
sqlaggregate aenv penv si@(SelExp _ e) = sqlaggexpr aenv penv e >>= return . maybe (Left si) Right
sqlaggregate aenv penv si@(SelectItem _ e nm) = do
  prOpt <- sqlaggexpr aenv penv e
  return $ maybe (Left si) rename prOpt

  where rename (pr,merge,final) = Right (pr { palias = Just (sqlnmcomponent nm) }, merge, final)

sqlprojection :: AEnv -> ADGPtrEnv -> SelectItem -> SQLParseM ParseResult
sqlprojection aenv penv (SelExp _ e) = sqlscalar aenv penv e
sqlprojection aenv penv (SelectItem _ e nm) = do
  pr <- sqlscalar aenv penv e
  return pr { palias = Just (sqlnmcomponent nm) }


pr0 :: K3 Type -> K3 Expression -> SQLParseM ParseResult
pr0 t e = return $ ParseResult e t t Nothing Nothing []

pri0 :: Identifier -> K3 Type -> K3 Expression -> SQLParseM ParseResult
pri0 i t e = return $ ParseResult e t t Nothing (Just i) []

-- TODO: avg, use finalization.
sqlaggexpr :: AEnv -> ADGPtrEnv -> ScalarExpr
           -> SQLParseM (Maybe (ParseResult, K3 Expression, Maybe (K3 Expression)))
sqlaggexpr aenv penv (FunCall _ nm args) = do
  let fn = sqlnm nm
  case (fn, args) of
    ("sum"  , [e]) -> sqlscalar aenv penv e >>= aggop "sum_" sumMergeE Nothing (\a b -> EC.binop OAdd a b)
    ("count", [e]) -> sqlscalar aenv penv e >>= aggop "cnt_" sumMergeE Nothing (\a _ -> EC.binop OAdd a $ EC.constant $ CInt 1)
    ("min"  , [e]) -> sqlscalar aenv penv e >>= aggop "min_" minMergeE Nothing (\a b -> EC.applyMany (EC.variable "min") [a, b])
    ("max"  , [e]) -> sqlscalar aenv penv e >>= aggop "max_" maxMergeE Nothing (\a b -> EC.applyMany (EC.variable "max") [a, b])
    (_, _) -> return Nothing

  where
    aggop i merge final f pr = return $ Just $
      (pr { pexpr  = EC.lambda "aggacc" $ f (EC.variable "aggacc") (pexpr pr)
          , palias = maybe Nothing (\j -> Just $ i ++ j) $ palias pr }
      , merge
      , final)

    sumMergeE = EC.lambda "a" $ EC.lambda "b" $ EC.binop OAdd (EC.variable "a") (EC.variable "b")
    minMergeE = EC.lambda "a" $ EC.lambda "b" $ EC.applyMany (EC.variable "min") [EC.variable "a", EC.variable "b"]
    maxMergeE = EC.lambda "a" $ EC.lambda "b" $ EC.applyMany (EC.variable "max") [EC.variable "a", EC.variable "b"]

sqlaggexpr _ _ _ = return Nothing


extractaggregates :: [(Identifier, ParseResult)] -> AEnv -> ADGPtrEnv -> Int -> ScalarExpr
                  -> SQLParseM (Int, ScalarExpr, [ScalarExpr])
extractaggregates aggaprs aenv penv i e@(FunCall ann nm args) = do
  aggeopt <- sqlaggexpr aenv penv e
  case aggeopt of
    Nothing -> do
      (ni, nxagg, nargs) <- foldM foldOnChild (i,[],[]) args
      return (ni, FunCall ann nm nargs, nxagg)

    Just (pr,_,_) -> let aprOpt = find (\(_,aggpr) -> pr == aggpr) aggaprs
                         (nid, nex) = maybe (aggnm i, [e]) (\(a,_) -> (Nmc a, [])) aprOpt
                     in return (i+1, Identifier emptyAnnotation nid, nex)

  where foldOnChild (j,acc,argacc) ce = do
          (nj, nce, el) <- extractaggregates aggaprs aenv penv j ce
          return (nj, acc++el, argacc++[nce])

        aggnm j = Nmc $ "h" ++ show j

extractaggregates _ _ _ i e = return (i,e,[])


-- TODO: variable access in correlated subqueries, nulls
sqlscalar :: AEnv -> ADGPtrEnv -> ScalarExpr -> SQLParseM ParseResult
sqlscalar _ _ (BooleanLit _ b) = pr0 TC.bool $ EC.constant $ CBool b

sqlscalar _ _ (NumberLit _ i) = if "." `isInfixOf` i
                                  then pr0 TC.real $ EC.constant $ CReal $ read i
                                  else pr0 TC.int  $ EC.constant $ CInt $ read i

sqlscalar _ _ (StringLit _ s) = pr0 TC.string $ EC.constant $ CString s

sqlscalar (AEnv (ret@(tnc -> (TRecord ids, ch)), _) _) penv (Identifier _ (sqlnmcomponent -> i)) = do
    idx <- sqgidxM $ fst penv
    p <- maybe (plkuperr i) return $ Map.lookup i idx
    maybe (varerror i) (\t -> pri0 i t $ EC.variable i @+ (sqlAttrProp p)) $ lookup i (zip ids ch)

  where
    varerror n = throwE $ boxToString $ ["Unknown unqualified variable " ++ n] %$ prettyLines ret
    plkuperr n = throwE $ "No attribute graph node found for " ++ n


sqlscalar (AEnv _ q) penv e@(QIdentifier _ nmcl) =
  case nmcl of
    [x,y] -> qpair x y
    _ -> qiderr

  where qpair (sqlnmcomponent -> nx) (sqlnmcomponent -> ny) = do
          let knx = "__" ++ nx
          qidx <- Map.fromList <$> mapM (\(i,ps) -> sqgidxM ps >>= return . (i,)) (snd penv)
          p <- maybe (lkuperr knx) (\idx -> maybe (lkuperr ny) return $ Map.lookup ny idx) $ Map.lookup knx qidx
          case Map.lookup knx q of
            Just (tnc -> (TRecord ids, ch), _, _) ->
              let idt = zip ids ch in
              maybe qidterr (\t -> pr0 t $ (EC.project ny $ EC.variable knx) @+ sqlAttrProp p) $ lookup ny idt

            Just _  -> qidelemerr
            Nothing -> qidlkuperr

        qiderr     = throwE $ unwords ["Invalid qualified identifier:", show e, show $ Map.keys q]
        qidterr    = throwE $ unwords ["Unknown qualified identifier type:", show e, show $ Map.keys q]
        qidelemerr = throwE $ unwords ["Invalid element for qid:", show e, show $ Map.keys q]
        qidlkuperr = throwE $ unwords ["Unknown qid component:", show e, show $ Map.keys q]
        lkuperr  v = throwE $ "No attribute graph node found for " ++ v

sqlscalar aenv penv (FunCall _ nm args) = do
  let fn = sqlnm nm
  case sqloperator fn args of
    (Just (UnaryOp  o x))   -> do
      xpr <- sqlscalar aenv penv x
      return (xpr {pexpr = EC.unop o $ pexpr xpr})

    (Just (BinaryOp o x y)) -> do
      xpr <- sqlscalar aenv penv x
      ypr <- sqlscalar aenv penv y
      pr0 (prt xpr) $ EC.binop o (pexpr xpr) $ pexpr ypr

    _ -> do
      case (fn, args) of
        ("!between", [_,_,_]) -> do
          [apr,bpr,cpr] <- mapM (sqlscalar aenv penv) args
          pr0 TC.bool $ EC.binop OAnd (EC.binop OLeq (pexpr bpr) $ pexpr apr)
                                     $ EC.binop OLeq (pexpr apr) $ pexpr cpr

        (_, _) -> do
          aprl <- mapM (sqlscalar aenv penv) args
          pr0 TC.unit $ EC.applyMany (EC.variable fn) $ map pexpr aprl -- TODO: return type?

sqlscalar (AEnv (ret@(tag -> TRecord ids), _) _) _ (Star _) =
  pr0 ret $ recE $ map (\i -> (i, EC.variable i)) ids

sqlscalar (AEnv _ q) _ (QStar _ (sqlnmcomponent -> n)) = do
  let kn = "__" ++ n
  case Map.lookup kn q of
    Just (qt@(tag -> TRecord ids), _, _) -> pr0 qt $ recE $ map (\i -> (i, EC.project i $ EC.variable kn)) ids
    Nothing -> throwE $ "Invalid qualified name in qstar"
    Just _ -> throwE $ "Invalid element type in qualified environment"

sqlscalar aenv penv (Case _ whense elsee) = do
  prl <- forM whense $ \(whenel, thene) -> (,) <$> mapM (sqlscalar aenv penv) whenel <*> sqlscalar aenv penv thene
  (eT, eE, wprl) <- case (prl, elsee) of
    ([], Nothing) -> throwE $ "Invalid case expression"
    (l, Nothing) -> let t = prt $ snd $ head l
                    in either throwE (\de -> return (t, de, l)) $ defaultExpression t
    (l, Just e) -> sqlscalar aenv penv e >>= \epr -> return (prt epr, pexpr epr, l)

  pr0 eT $ foldl (\accE (wprs, tpr) -> EC.ifThenElse (andE wprs) (pexpr tpr) accE) eE $ reverse wprl

  where andE [] = EC.constant $ CBool False
        andE (h:t) = foldl (\accE wpr -> EC.binop OAnd accE $ pexpr wpr) (pexpr h) t

sqlscalar aenv penv (CaseSimple _ e whense elsee) = do
  vpr <- sqlscalar aenv penv e
  prl <- forM whense $ \(whenel, thene) -> (,) <$> mapM (sqlscalar aenv penv) whenel <*> sqlscalar aenv penv thene
  (eT, eE, wprl) <- case (prl, elsee) of
    ([], Nothing) -> throwE $ "Invalid case expression"
    (l, Nothing) -> let t = prt $ snd $ head l
                    in either throwE (\de -> return (t, de, l)) $ defaultExpression t
    (l, Just ee) -> sqlscalar aenv penv ee >>= \epr -> return (prt epr, pexpr epr, l)

  pr0 eT $ memoE (pexpr vpr) $ \ve ->
    foldl (\accE (wprs, tpr) -> EC.ifThenElse (andE ve wprs) (pexpr tpr) accE) eE $ reverse wprl

  where andE _ [] = EC.constant $ CBool False
        andE ve (h:t) = foldl (\accE wpr -> EC.binop OAnd accE $ EC.binop OEqu ve $ pexpr wpr) (EC.binop OEqu ve $ pexpr h) t

sqlscalar _ _ (ScalarSubQuery _ qe) = sqlquery qe

sqlscalar _ _ (Exists _ qe) = do
  qpr <- sqlquery qe
  pr0 TC.bool $ emptyE False $ pexpr qpr

sqlscalar aenv penv (InPredicate _ e isIn inl) = do
  epr <- sqlscalar aenv penv e
  case inl of
    InList _ el -> do
      eprl <- mapM (sqlscalar aenv penv) el
      case eprl of
        [] -> pr0 TC.bool $ EC.constant $ CBool $ not isIn
        (h:t) -> pr0 TC.bool $ memoE (immutE $ pexpr epr) $
                      \ve -> foldl (\accE p -> mergeE accE $ testE ve p) (testE ve h) t

    InQueryExpr _ q -> do
      qpr <- sqlquery q
      pr0 TC.bool $ memberE isIn (pexpr epr) $ pexpr qpr

  where testE ve pr = EC.binop (if isIn then OEqu else ONeq) ve $ pexpr pr
        mergeE accE nE = EC.binop (if isIn then OOr else OAnd) accE nE

sqlscalar _ _ e = throwE $ "Unhandled scalar expr: " ++ show e


sqloperator :: String -> ScalarExprList -> Maybe OperatorFn
sqloperator "-"    [x]   = Just (UnaryOp  ONeg x)
sqloperator "!not" [x]   = Just (UnaryOp  ONot x)
sqloperator "+"    [x,y] = Just (BinaryOp OAdd x y)
sqloperator "-"    [x,y] = Just (BinaryOp OSub x y)
sqloperator "*"    [x,y] = Just (BinaryOp OMul x y)
sqloperator "/"    [x,y] = Just (BinaryOp ODiv x y)
sqloperator "%"    [x,y] = Just (BinaryOp OMod x y)
sqloperator "="    [x,y] = Just (BinaryOp OEqu x y)
sqloperator "!="   [x,y] = Just (BinaryOp ONeq x y)
sqloperator "<>"   [x,y] = Just (BinaryOp ONeq x y)
sqloperator "<"    [x,y] = Just (BinaryOp OLth x y)
sqloperator "<="   [x,y] = Just (BinaryOp OGeq x y)
sqloperator ">"    [x,y] = Just (BinaryOp OGth x y)
sqloperator ">="   [x,y] = Just (BinaryOp OGeq x y)
sqloperator "!and" [x,y] = Just (BinaryOp OAnd x y)
sqloperator "!or"  [x,y] = Just (BinaryOp OOr  x y)
sqloperator _ _ = Nothing

-- TODO
sqlsubexpr :: [(Identifier, ScalarExpr)] -> ScalarExpr -> SQLParseM (ScalarExpr)
sqlsubexpr cenv e@(Identifier _ (sqlnmcomponent -> i)) = maybe (return e) return $ lookup i cenv

sqlsubexpr cenv (FunCall anns nm args) = do
  nargs <- mapM (sqlsubexpr cenv) args
  return $ FunCall anns nm nargs

sqlsubexpr _ e = return e


-- | Type construction
sqltabletype :: AttributeDefList -> SQLParseM (K3 Type)
sqltabletype attrs = sqlrectype attrs >>= tcolM

sqlrectype :: AttributeDefList -> SQLParseM (K3 Type)
sqlrectype attrs = mapM sqlattr attrs >>= \ts -> return (recT ts)

sqlattr :: AttributeDef -> SQLParseM (Identifier, K3 Type)
sqlattr (AttributeDef _ nm t _ _) = sqltypename t >>= sqltype >>= return . (sqlnmcomponent nm,)

sqltype :: String -> SQLParseM (K3 Type)
sqltype s = case s of
  "int"              -> return TC.int
  "integer"          -> return TC.int
  "real"             -> return TC.real
  "double precision" -> return TC.real
  "text"             -> return TC.string
  _ -> throwE $ "Invalid K3-SQL type: " ++ s

sqlnm :: Name -> String
sqlnm (Name _ comps) = concatMap sqlnmcomponent comps

sqlnmcomponent :: NameComponent -> String
sqlnmcomponent (Nmc s) = s
sqlnmcomponent (QNmc s) = s

sqltypename :: TypeName -> SQLParseM String
sqltypename (SimpleTypeName _ t) = return t
sqltypename t = throwE $ "Invalid sql typename " ++ show t

sqltablealias :: Identifier -> TableAlias -> Maybe Identifier
sqltablealias def alias = case alias of
    NoAlias _        -> Just def
    TableAlias _ nc  -> Just $ "__" ++ sqlnmcomponent nc
    FullAlias _ nc _ -> Just $ "__" ++ sqlnmcomponent nc


{- K3 optimizations -}

-- | Returns SQL attribute pointers in an expression, without descending into lambdas (e.g., subqueries).
sqlAttrs :: K3 Expression -> SQLParseM [ADGPtr]
sqlAttrs e = foldTree extract [] e
  where extract acc (tag -> ELambda _) = return acc
        extract acc n = return $ nub $ acc ++ (concatMap extractADGPtr $ filter isSqlAttrProp $ annotations n)
        extractADGPtr (EProperty (Left (_, Just (tag -> LInt p)))) = [p]
        extractADGPtr _ = []

baseRelationsP :: [ADGPtr] -> SQLParseM [Identifier]
baseRelationsP ps = foldM baserel [] ps
  where baserel acc p = adgchaseM p >>= \nodes -> return $ nub $ acc ++ map adnr nodes

baseRelationsE :: K3 Expression -> SQLParseM [Identifier]
baseRelationsE e = sqlAttrs e >>= baseRelationsP

allRelations :: SQLParseM [(Identifier, K3 Type)]
allRelations = get >>= \env -> return $ Map.toList $ relations env

-- TODO: group expressions by base relations.
cascadePredicate :: K3 Expression -> SQLParseM ([K3 Expression], [K3 Expression], Maybe (K3 Expression))
cascadePredicate predicate = do foldMapTree extract zero predicate
  where
    extract acc n@(tnc -> (EOperate OAnd, _)) =
      let (n1, nequi, restch) = accflatten acc in
      case restch of
        [Nothing, Nothing] -> return (n1, nequi, Nothing)
        [Just nx, Nothing] -> return (n1, nequi, Just nx)
        [Nothing, Just ny] -> return (n1, nequi, Just ny)
        [Just nx, Just ny] -> return (n1, nequi, Just $ (EC.binop OAnd nx ny) @<- annotations n)
        _ -> throwE $ "Invalid boolean operator children"

    extract _ n = classify n

    classify e@(tnc -> (EOperate OEqu, [x,y])) = do
      xrels <- baseRelationsE x
      yrels <- baseRelationsE y
      case (xrels, yrels) of
        ([a],[b]) | a /= b -> return ([], [e], Nothing)
        (i,j) | length (nub $ i ++ j) == 1 -> return ([e], [], Nothing)
        _ -> return ([], [], Just e)

    classify e = do
      rels <- baseRelationsE e
      case rels of
        []  -> return ([],  [], Just e)
        [_] -> return ([e], [], Nothing)
        _   -> return ([],  [], Just e)

    accflatten l = let (x,y,z) = unzip3 l in (concat x, concat y, z)

    zero = ([], [], Nothing)

sqloptimize :: K3 Expression -> SQLParseM (K3 Expression)
sqloptimize queryexpr = do
    rexpr <- optimizeexpr queryexpr
    if rexpr `compareEAST` queryexpr
      then pickjoinalg rexpr
      else sqloptimize rexpr

  where optimizeexpr e = modifyTree optimize e
        pickjoinalg e = modifyTree joinalg e

        -- Transformer fusion for SQL-K3.
        optimize e@(PFilterJoin lE rE filterE predE outE jAs) = do
          filterBodyE <- case filterE of
                           PLamBind _ _ _ pe _ _ -> return pe
                           PLam _ pe _ -> return pe
                           _ -> throwE $ boxToString $ ["Invalid predicate "] %$ prettyLines filterE

          case predE of
            PLam2Bind i j srcE bnd bodyE _ _ _ -> do
              nfilterE <- rewritePredicate jAs filterE
              let npredE = PLam2Bind i j srcE bnd (EC.binop OAnd bodyE nfilterE) [] [] []
              return $ PCJoin lE rE npredE outE [] [] [] jAs

            PLam2 i j bodyE _ _ -> do
              nfilterE <- rewritePredicate jAs filterBodyE
              let npredE = PLam2 i j (EC.binop OAnd bodyE nfilterE) [] []
              return $ PCJoin lE rE npredE outE [] [] [] jAs

            _ -> return e

        optimize (PMapJoin lE rE mapE predE outE jAs) = do
          cE <- composeE
          return $ PCJoin lE rE predE cE [] [] [] jAs

          where
            er e = recE [("elem", e)]
            composeE = case outE of
              PLam2Bind i j _ _ _ _ _ _ -> return $
                PLam2App i j mapE (er $ PApp2 outE (EC.variable i) (EC.variable j) [] []) [] [] []

              PLam2App i j _ _ _ _ _ -> return $
                PLam2App i j mapE (er $ PApp2 outE (EC.variable i) (EC.variable j) [] []) [] [] []

              PLam2 i j bodyE _ _ -> return $
                PLam2App i j mapE (er bodyE) [] [] []

              _ -> throwE $ boxToString $ ["Invalid join combiner expression"] %$ prettyLines outE

        optimize (PJoin lE rE predE outE jAs) = do
          pbodyE <- case predE of
                      PLam2Bind _ _ _ _ bodyE _ _ _ -> return bodyE
                      PLam2 _ _ bodyE _ _ -> return bodyE
                      _ -> throwE $ boxToString $ ["Invalid predicate "] %$ prettyLines predE

          (lal, ral, _, _, lbr, rbr, _, _, laenv, raenv) <- joinInfo jAs

          (onesided, equi, remdr) <- cascadePredicate pbodyE
          pushdownCands <- mapM (\pe -> baseRelationsE pe >>= return . (,pe)) $ onesided ++ equi

          jclassified <- forM pushdownCands $ \(rels,pe) ->
                           if (rels `intersect` lbr) == rels then return $ (([pe], []), [])
                           else if (rels `intersect` rbr) == rels then return $ (([], [pe]), [])
                           else return $ (([], []), [pe])

          let (pushdowns, jequi) = second concat $ unzip jclassified
          let (lpush, rpush) = (concat *** concat) $ unzip pushdowns
          nlE <- mkPredicates laenv lal sqlPred1Prop lE lpush
          nrE <- mkPredicates raenv ral sqlPred1Prop rE rpush
          let jConjuncts = pruneConjuncts $ jequi ++ maybe [] (:[]) remdr
          npredE <- rebuildPredE predE jConjuncts
          return $ PCJoin nlE nrE npredE outE [] [] [] jAs

        optimize n = return n


        -- Promote joins to equi-joins.
        joinalg (PJoin lE rE predE outE jAs) = do
          pbodyE <- case predE of
                      PLam2Bind _ _ _ _ bodyE _ _ _ -> return bodyE
                      PLam2 _ _ bodyE _ _ -> return bodyE
                      _ -> throwE $ boxToString $ ["Invalid predicate "] %$ prettyLines predE

          (lal, ral, _, _, lbr, rbr, _, _, laenv, raenv) <- joinInfo jAs

          (onesided, equi, remdr) <- cascadePredicate pbodyE
          pushdownCands <- mapM (\pe -> baseRelationsE pe >>= return . (,pe)) $ onesided ++ equi

          jclassified <- forM pushdownCands $ \(rels,pe) ->
                           if (rels `intersect` lbr) == rels then return $ (([pe], []), [])
                           else if (rels `intersect` rbr) == rels then return $ (([], [pe]), [])
                           else return $ (([], []), [pe])

          let (pushdowns, jequi') = second concat $ unzip jclassified
          let (lpush, rpush) = (concat *** concat) $ unzip pushdowns

          case (lpush, rpush, jequi') of
            ([], [], jequi) ->
              let jConjuncts = pruneConjuncts $ jequi ++ maybe [] (:[]) remdr in
              if jConjuncts /= jequi
                then do
                  npredE <- rebuildPredE predE jConjuncts
                  return $ PCJoin lE rE npredE outE [] [] [] jAs

                -- TODO: partial equality join API.
                else do
                  (lk,rk,crem) <- extractJoinKeys lbr rbr jConjuncts
                  case crem of
                    [] -> do
                      lkeyE <- mkJoinKey laenv lal lk
                      rkeyE <- mkJoinKey raenv ral rk
                      return $ PCEJoin lE rE lkeyE rkeyE outE [] [] [] [] jAs

                    _ -> do
                      npredE <- rebuildPredE predE jConjuncts
                      return $ PCJoin lE rE npredE outE [] [] [] jAs

            (_, _, _) -> throwE $ "Incomplete optimization fixpoint"

        joinalg n = return n


        joinInfo anns = case (filter isSqlAEnvProp anns, filter isSqlPEnvProp anns) of
          ([EProperty (Left (_, Just (tag -> LInt sp)))],
           [EProperty (Left (_, Just (tag -> LString (read -> penv))))]) -> do
             aenv <- sqclkupM sp
             let roots = adgproots penv
             baserels <- mapM (\(_, ps) -> baseRelationsP ps) $ snd penv
             let (lal, ral) = (fst $ head $ snd penv, fst $ head $ tail $ snd penv)
             laenv <- aenvsub lal aenv
             raenv <- aenvsub ral aenv
             (lbr, rbr) <- case baserels of
                              [l,r] -> return (l,r)
                              _ -> throwE $ "Invalid binary join base relations."

             return (lal, ral, roots, baserels, lbr, rbr, penv, aenv, laenv, raenv)


          _ -> throwE $ "Invalid join expression for metadata extraction"

        rewriteAttr :: [ADGPtr] -> AEnv -> ADGPtrEnv -> ADGPtr -> SQLParseM (K3 Expression)
        rewriteAttr roots aenv penv ptr = do
          (_, sqlexpr) <- rcrsub ptr
          pr <- sqlscalar aenv penv sqlexpr
          return $ pexpr pr

          where
            sub p node | p `elem` roots = return $ Identifier emptyAnnotation $ Nmc $ adnn node

            sub _ (adne &&& adnch -> (Just def, chp)) = do
              cenv <- mapM rcrsub chp
              sqlsubexpr cenv def

            sub p _ = throwE $ "Rewrite substitution failed for " ++ show p

            rcrsub p = sqglkupM p >>= \n -> sub p n >>= return . (adnn n,)

        rewritePredicate :: [Annotation Expression] -> K3 Expression -> SQLParseM (K3 Expression)
        rewritePredicate joinAnns e = do
            (_, _, roots, _, _, _, penv, aenv, _, _) <- joinInfo joinAnns
            modifyTree (rewrite roots aenv penv) e
          where
            rewrite roots aenv penv (attrptr -> Just p) = rewriteAttr roots aenv penv p
            rewrite _ _ _ re = return re

            attrptr :: K3 Expression -> Maybe Int
            attrptr ((@~ isSqlAttrProp) -> Just (EProperty (Left (_, Just (tag -> LInt p))))) = Just p
            attrptr _ = Nothing

        mkPredicates aenv alias classp e el = foldM (mkPredicate aenv alias classp) e el
        mkPredicate aenv alias classp acce predicate = do
          (iOpt, be) <- bindE aenv "x" predicate
          filterF <- maybe (return alias) return iOpt >>= \a -> return $ EC.lambda a be
          let fexpr = EC.applyMany (EC.project "filter" acce) [filterF]
          let rexpr = fexpr @+ classp
          return rexpr

        rebuildPredE pE conjuncts = case pE of
          PLam2Bind i j srcE bnd _ _ _ _ -> do
            nbodyE <- mkConjunctive conjuncts
            return $ PLam2Bind i j srcE bnd nbodyE [] [] []

          PLam2 i j _ _ _ -> do
            nbodyE <- mkConjunctive conjuncts
            return $ PLam2 i j nbodyE [] []

          _ -> throwE $ boxToString $ ["Invalid predicate "] %$ prettyLines pE

        extractJoinKeys lbr rbr conjuncts = foldM (extractJoinKey lbr rbr) ([], [], []) conjuncts
        extractJoinKey lbr rbr (lacc,racc,uacc) e@(tnc -> (EOperate OEqu, [x,y])) = do
          xrels <- baseRelationsE x
          yrels <- baseRelationsE y
          let (xl, xr) = (xrels `intersect` lbr == xrels, xrels `intersect` rbr == xrels)
          let (yl, yr) = (yrels `intersect` lbr == yrels, yrels `intersect` rbr == yrels)
          case (xl, xr, yl, yr) of
            (True, False, False, True) -> return (lacc ++ [x], racc ++ [y], uacc)
            (False, True, True, False) -> return (lacc ++ [y], racc ++ [x], uacc)
            _ -> return (lacc, racc, uacc ++ [e])

        extractJoinKey _ _ (lacc,racc,uacc) e = return (lacc, racc, uacc ++ [e])

        mkJoinKey aenv alias l = do
          (iOpt, be) <- bindE aenv "x" $ tupE l
          i <- maybe (return alias) return iOpt
          return $ EC.lambda i be

        pruneConjuncts  l = filter (\e -> case tag e of {EConstant (CBool True) -> False; _ -> True}) l
        mkConjunctive  [] = return $ EC.constant $ CBool True
        mkConjunctive   l = return $ foldl1 (\a b -> EC.binop OAnd a b) l


sqlstages :: Bool -> K3 Declaration -> SQLParseM (K3 Declaration)
sqlstages distributed prog = do
  tprog <- either throwE return $ do
      (qtprog,_) <- inferProgramTypes $ stripTypeAndEffectAnns prog
      translateProgramTypes qtprog

  allrels <- allRelations
  ((newDecls, inits, _), nprog) <- stageExpressions (map fst allrels) tprog

  case tnc nprog of
    (DRole n, declch) -> do
      strigD <- startTrig inits
      return $ declareBuiltins$ DC.role n $
        declch ++ newDecls ++ [strigD] ++ if distributed then loadersAndPeerLocal allrels ++ [master] else []

    (tg,_) -> throwE $ "Invalid program toplevel: " ++ show tg

  where
    stageExpressions allrels qprog = foldExpression stageF ([], [], []) qprog
      where stageF = if distributed then stageDistributed allrels else stageSingleThreaded allrels

    {- Distributed staging. -}
    stageDistributed rels acc e = biFoldMapRebuildTree stageSTSym (stageD rels) (Nothing, Nothing) acc e

    stageD _ _ acc ch n@((@~ isSqlStagedProp) -> Just _) = return (aconcat acc, replaceCh n ch)

    stageD _ _ acc ch n@((@~ isSqlQueryResultProp) -> Just (EProperty (Left (_, Just (tag -> LString stgid))))) =
      case n of
        PAssign _ (PEJoin _ _ _ _ _ _) _ -> annotateDEJoin stgid (replaceCh n ch) >>= return . (aconcat acc,)
        PAssign _ (PGroupBy _ _ _ _ gbAs) _ -> annotateDGroupBy gbAs stgid (replaceCh n ch) >>= return . (aconcat acc,)
        _ -> return (aconcat acc, replaceCh n ch)

    stageD _ symOpt acc ch n@(PJoin _ _ _ _ _)       = mkStage 2 (Right annotateJoin) symOpt acc ch n
    stageD _ symOpt acc ch n@(PEJoin _ _ _ _ _ _)    = mkStage 2 (Right $ (\a _ c -> annotateDEJoin a c)) symOpt acc ch n
    stageD _ symOpt acc ch n@(PGroupBy _ _ _ _ gbAs) = mkStage 1 (Right $ (\a _ c -> annotateDGroupBy gbAs a c)) symOpt acc ch n

    stageD rels (_,sym) (aconcat -> (decls, inits, [])) ch n@(tag -> EVariable i) =
      let nacc = if i `elem` rels then (decls, inits, [(i, sym)]) else (decls, inits, [])
      in return (nacc, replaceCh n ch)

    stageD _ _ acc ch n = return (aconcat acc, replaceCh n ch)

    annotateDEJoin stgid bodye =
      return $ bodye @+ (EApplyGen True "DistributedHashJoin" $ Map.fromList [("lbl", SLabel stgid)])

    annotateDGroupBy gbAs stgid bodye =
      case filter isSqlAggMergeProp gbAs of
        [EProperty (Left (_, Just (tag -> LString (read -> mergeF))))] ->
          return $ bodye @+ (EApplyGen True "DistributedGroupBy" $ groupByArgs stgid mergeF)
        _ -> throwE "No merge function found for distributed group-by"

    groupByArgs stgid mergeF = Map.fromList [ ("lbl", SLabel stgid)
                                            , ("merge", SExpr mergeF) ]

    {- Single-machine staging. -}
    stageSingleThreaded rels acc e = biFoldMapRebuildTree stageSTSym (stageST rels) (Nothing, Nothing) acc e

    stageSTSym (_,psym) n@((@~ isSqlStagedProp) -> Just (EProperty (Left (_, Just (tag -> LString i))))) =
      let nsympair = (psym, Just i) in
      return (nsympair, replicate (length $ children n) nsympair)

    stageSTSym (_, psym) n@(PJoin _ _ _ _ _)    = mkSym psym n
    stageSTSym (_, psym) n@(PEJoin _ _ _ _ _ _) = mkSym psym n
    stageSTSym (_, psym) n@(PGroupBy _ _ _ _ _) = mkSym psym n
    stageSTSym td n = return (td, replicate (length $ children n) td)

    stageST _ _ acc ch n@((@~ isSqlStagedProp) -> Just (EProperty _)) = return (aconcat acc, replaceCh n ch)
    stageST _ _ acc ch n@((@~ isSqlQueryResultProp) -> Just _) = finalBarrier acc $ replaceCh n ch
    stageST _ symOpt acc ch n@(PJoin _ _ _ _ _)    = mkStage 2 (Right annotateJoin)   symOpt acc ch n
    stageST _ symOpt acc ch n@(PEJoin _ _ _ _ _ _) = mkStage 2 (Right annotateJoin)   symOpt acc ch n
    stageST _ symOpt acc ch n@(PGroupBy _ _ _ _ _) = mkStage 1 (Right $ \_ _ e -> return e) symOpt acc ch n

    stageST rels (_,sym) (aconcat -> (decls, inits, [])) ch n@(tag -> EVariable i) =
      let nacc = if i `elem` rels then (decls, inits, [(i, sym)]) else (decls, inits, [])
      in return (nacc, replaceCh n ch)

    stageST _ _ acc ch n = return (aconcat acc, replaceCh n ch)

    aconcat acc = (concat decls, concat inits, concat accrels)
      where (decls, inits, accrels) = unzip3 acc

    mkSym psym n = do
      s <- sqsextM
      let nsympair = (psym, Just $ show s)
      return (nsympair, replicate (length $ children n) $ nsympair)

    mkStage relsForInit annotF (psymOpt, symOpt) (aconcat -> (decls, inits, rels)) ch n =
      case find isEType $ annotations n of
        Just (EType t) -> do
          let nexpr = replaceCh n ch

          stgid <- maybe (sqsextM >>= return . show) return symOpt
          let dsid     = materializeId stgid
          let tid      = stageId stgid
          let stgds    = DC.global dsid (mutT t) Nothing

          let ninits = if length rels == relsForInit then inits ++ [(tid, distributed)] else inits

          annotasgnexpr <- case annotF of
                             Left f -> f tid nexpr >>= \e -> return $ EC.assign dsid e
                             _ -> return $ EC.assign dsid nexpr

          let asgnsendexpr psym = EC.block [annotasgnexpr, EC.send (EC.variable psym) (EC.variable "me") EC.unit]
          let bodyexpr          = maybe annotasgnexpr asgnsendexpr $ psymOpt

          annotbexpr <- case annotF of
                            Right f -> f tid nexpr bodyexpr
                            _ -> return bodyexpr

          let stgtrig  = DC.trigger tid TC.unit $ EC.lambda "_" annotbexpr
          return ((decls ++ [stgds, stgtrig], ninits, rels), (EC.variable dsid) @+ sqlMaterializedProp)

        _ -> throwE $ boxToString $ ["No type found for"] %$ prettyLines n

    materializedE e = foldMapTree matE False e
      where
        matE _ ((@~ isSqlMaterializedProp) -> Just (EProperty _)) = return True
        matE _ (tag -> ELambda _) = return False
        matE l _ = return $ any id l

    annotateJoin _ e re = do
      barrier <- joinBarrier e
      if barrier then mkCountBarrier re (EC.constant $ CInt 2) else return re

    joinBarrier (PJoin lE rE _ _ _) = binaryBarrier lE rE
    joinBarrier (PEJoin lE rE _ _ _ _) = binaryBarrier lE rE
    joinBarrier _ = return False

    binaryBarrier lE rE = (&&) <$> materializedE lE <*> materializedE rE

    finalBarrier acc n@(PAssign _ e _) = assignBarrier acc e n
    finalBarrier acc n = return (aconcat acc, n)

    assignBarrier acc assignE e = do
      barrier <- joinBarrier assignE
      be <- if barrier then mkCountBarrier e (EC.constant $ CInt 2) else return e
      return (aconcat acc, be)

    mkCountBarrier e countE = do
      args <- barrierArgs countE
      return $ e @+ EApplyGen True "OnCounter" args

    barrierArgs countE = do
      lblsym <- slblsextM
      return $ Map.fromList [ ("id", SLabel $ "barrier" ++ show lblsym)
                            , ("eq", SExpr $ countE)
                            , ("reset", SExpr $ EC.constant $ CBool False)
                            , ("profile", SExpr $ EC.constant $ CBool False) ]

    startTrig inits = do
      sendsE <- mapM mkSend inits
      return $ DC.trigger "start" TC.unit $ EC.lambda "_" $ EC.block $ [EC.unit @+ EApplyGen True "SQL" Map.empty] ++ sendsE

    mkSend (i, distributedInit) =
      if distributedInit
        then
          let iterateF = EC.lambda "p" $ EC.send (EC.variable i) (EC.project "addr" $ EC.variable "p") EC.unit
          in mkCountBarrier (EC.applyMany (EC.project "iterate" $ EC.variable "peers") [iterateF])
                           $ EC.applyMany (EC.project "size" $ EC.variable "peers") [EC.unit]
        else return $ EC.send (EC.variable i) (EC.variable "me") EC.unit

    loadersAndPeerLocal allrels =
      let (loaderDecls, loaderExprs) = unzip $ map mkLoader allrels in
      concat loaderDecls ++
      [DC.trigger "startPeer" TC.unit $ EC.lambda "_" $
        EC.block $ loaderExprs ++ [EC.send (EC.variable "start") (EC.variable "master") EC.unit]]

    mkLoader (i,t) =
      let pathCT = (TC.collection $ recT [("path", TC.string)]) @+ TAnnotation "Collection"
          rexpr = EC.applyMany (EC.variable $ i ++ "LoaderP") [EC.variable $ i ++ "Files", EC.variable i, EC.variable $ i ++ "_r"]
      in
      ([DC.global (i ++ "LoaderP") (immutT $ TC.function pathCT $ TC.function t TC.unit) Nothing,
        DC.global (i ++ "Files") (immutT pathCT) Nothing],
       -- rexpr)
       EC.unit)

    master = DC.global "master" (immutT TC.address) Nothing


{- Optimization patterns -}

pattern PVar i        iAs   = Node (EVariable i   :@: iAs)   []
pattern PApp fE argE  appAs = Node (EOperate OApp :@: appAs) [fE, argE]
pattern PSeq lE rE    seqAs = Node (EOperate OSeq :@: seqAs) [lE, rE]
pattern PLam i  bodyE iAs   = Node (ELambda i     :@: iAs)   [bodyE]
pattern PPrj cE fId   fAs   = Node (EProject fId  :@: fAs)   [cE]

pattern PBindAs srcE bnd bodyE bAs = Node (EBindAs bnd :@: bAs) [srcE, bodyE]

pattern PAssign i assignE aAs = Node (EAssign i :@: aAs) [assignE]

pattern PLamBind i srcE bnd bodyE lAs bAs = PLam i (PBindAs srcE bnd bodyE bAs) lAs
pattern PLamApp i fE argE appAs iAs = PLam i (PApp fE argE appAs) iAs

pattern PPrjApp cE fId fAs fArg iAppAs = PApp (PPrj cE fId fAs) fArg iAppAs

pattern PLam2 i j bodyE iAs jAs = PLam i (PLam j bodyE jAs) iAs
pattern PApp2 f arg1 arg2 iAppAs oAppAs  = PApp (PApp f arg1 iAppAs) arg2 oAppAs

pattern PLam2Bind i j srcE bnd bodyE iAs jAs bAs = PLam i (PLamBind j srcE bnd bodyE jAs bAs) iAs
pattern PLam2App i j fE argE iAs jAs appAs = PLam i (PLamApp j fE argE appAs jAs) iAs

pattern PPrjApp2 cE fId fAs fArg1 fArg2 app1As app2As
  = PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As

pattern PPrjApp3 cE fId fAs fArg1 fArg2 fArg3 app1As app2As app3As
  = PApp (PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As) fArg3 app3As

pattern PPrjApp4 cE fId fAs fArg1 fArg2 fArg3 fArg4 app1As app2As app3As app4As
  = PApp (PApp (PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As) fArg3 app3As) fArg4 app4As

pattern PFilter cE lamE <- PPrjApp cE "filter" _ lamE _

pattern PMap cE lamE <- PPrjApp cE "map" _ lamE _

pattern PJoin lE rE predE outE jAs <- PPrjApp3 lE "join" _ rE predE outE _ _ jAs

pattern PEJoin lE rE lkeyE rkeyE outE jAs <- PPrjApp4 lE "equijoin" _ rE lkeyE rkeyE outE _ _ _ jAs

pattern PGroupBy cE gbF accF zE gbAs <- PPrjApp3 cE "groupBy" _ gbF accF zE _ _ gbAs

pattern PCFilter cE lamE pAs app1As <- PPrjApp cE "filter" pAs lamE app1As

pattern PCMap cE lamE pAs app1As <- PPrjApp cE "map" pAs lamE app1As

pattern PCJoin lE rE predE outE pAs app1As app2As app3As =
  PPrjApp3 lE "join" pAs rE predE outE app1As app2As app3As

pattern PCEJoin lE rE lkeyE rkeyE outE pAs app1As app2As app3As app4As =
  PPrjApp4 lE "equijoin" pAs rE lkeyE rkeyE outE app1As app2As app3As app4As

pattern PCGroupBy cE gbF accF zE fAs app1As app2As app3As =
  PPrjApp3 cE "groupBy" fAs gbF accF zE app1As app2As app3As

pattern PFilterJoin lE rE filterE predE outE jAs <- PFilter (PJoin lE rE predE outE jAs) filterE

pattern PMapJoin lE rE mapE predE outE jAs <- PMap (PJoin lE rE predE outE jAs) mapE
