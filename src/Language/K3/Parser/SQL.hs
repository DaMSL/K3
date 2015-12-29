{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

-- TODO:
-- 3. chain simplification
-- 5. expression case completion
-- 6. pushdown in subqueries
-- 7. subqueries in gbs, prjs, aggs
-- 8. correlated subqueries, and query decorrelation
-- x. more groupByPushdown, subquery and distributed plan testing
-- y. distinct, order, limit, offset SQL support

module Language.K3.Parser.SQL where

import Control.Arrow ( (***), (&&&), first, second )
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Except

import Data.Function ( on )
import Data.Functor.Identity
import Data.Maybe ( catMaybes )
import Data.Either ( partitionEithers )
import Data.Monoid
import Data.List ( (\\), find, intersect, nub, isInfixOf, isPrefixOf, sortBy, unzip4 )

import Data.Map ( Map )
import Data.Set ( Set )
import Data.Tree
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

import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

data OperatorFn = UnaryOp Operator ScalarExpr
                | BinaryOp Operator ScalarExpr ScalarExpr
                deriving (Eq, Show)

data AggregateFn = AggSum
                 | AggCount
                 | AggMin
                 | AggMax
                 deriving (Eq, Read, Show)

-- | Relation names and types.
type RTypeEnv = Map Identifier (K3 Type)

-- | Attribute dependency graph.
type ADGPtr = Int

data ADGNode = ADGNode { adnn  :: Identifier
                       , adnt  :: K3 Type
                       , adnr  :: Maybe Identifier
                       , adne  :: Maybe ScalarExpr
                       , adnch :: [ADGPtr] }
              deriving (Eq, Show)

type ADGraph = Map ADGPtr ADGNode

-- | Scope frames are mappings of name paths (qualified or unqualified) to attributes.
type AttrPath   = [Identifier]
type Qualifiers = [[Identifier]]
type AttrEnv    = Map AttrPath ADGPtr
type ScopeFrame = (AttrEnv, [AttrPath], Qualifiers)

-- | Scope environments store bindings throughout a query's subexpressions.
type ScopeId  = Int
type ScopeEnv = Map ScopeId ScopeFrame

-- | Internal query plan representation.
data QueryClosure = QueryClosure { qcfree  :: [AttrPath]
                                 , qcplan  :: QueryPlan }
                    deriving (Eq, Show)

type SubqueryBindings = [(ScalarExpr, (Identifier, QueryClosure))]

data PlanCPath = PlanCPath { pcoutsid     :: ScopeId
                           , pcselects    :: ScalarExprList
                           , pcgroupbys   :: ScalarExprList
                           , pcprojects   :: SelectItemList
                           , pcaggregates :: SelectItemList
                           , pchaving     :: MaybeBoolExpr
                           , pcbindings   :: SubqueryBindings }
                deriving (Eq, Show)

data PlanNode = PJoin     { pnjprocsid    :: ScopeId
                          , pnjoutsid     :: ScopeId
                          , pnjtype       :: Maybe (Natural, JoinType)
                          , pnjequalities :: [(ScalarExpr, ScalarExpr)]
                          , pnjpredicates :: ScalarExprList
                          , pnjpredbinds  :: SubqueryBindings
                          , pnjpath       :: [PlanCPath] }

              | PSubquery { pnsqoutsid   :: ScopeId
                          , pnqclosure   :: QueryClosure }

              | PTable    { pntid        :: Identifier
                          , pntoutsid    :: ScopeId
                          , pntref       :: Maybe TableRef
                          , pntbindmap   :: Maybe BindingMap
                          , pntpath      :: [PlanCPath] }
              deriving (Eq, Show)

type PlanTree = Tree PlanNode

data QueryPlan = QueryPlan { qjoinTree   :: Maybe PlanTree
                           , qpath       :: [PlanCPath]
                           , qstageid    :: Maybe Identifier }
                deriving (Eq, Show)

-- | Binding mappings
type TypePrefix  = Identifier
type TypePath    = [Identifier]
type TypeMapping = Maybe (Either TypePrefix TypePath)

data BindingMap = BMNone
                | BMTPrefix      Identifier
                | BMTFieldMap    (Map Identifier TypePath)
                | BMTVPartition  (Map Identifier (Identifier, TypeMapping))
                deriving (Eq, Show)

-- | Parsing state environment.
data SQLEnv = SQLEnv { relations :: RTypeEnv
                     , adgraph   :: ADGraph
                     , scopeenv  :: ScopeEnv
                     , aliassym  :: ParGenSymS
                     , adpsym    :: ParGenSymS
                     , spsym     :: ParGenSymS
                     , sqsym     :: ParGenSymS
                     , stgsym    :: ParGenSymS
                     , slblsym   :: ParGenSymS
                     , aggdsym   :: ParGenSymS }
            deriving (Eq, Show)

-- | A stateful SQL parsing monad.
type SQLParseM = ExceptT String (State SQLEnv)

-- | SQL program statements
data SQLDecl = SQLRel   (Identifier, K3 Type)
             | SQLStage (Identifier, K3 Type)
             | SQLQuery QueryPlan
             deriving (Eq, Show)

type StageGraph = [Either Identifier (Identifier, Identifier)]

{- Naming helpers. -}

materializeId :: Identifier -> Identifier
materializeId n = "output" ++ n

stageId :: Identifier -> Identifier
stageId n = "stage" ++ n


{- Type and alias helpers. -}

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


{- SQLEnv helpers -}

sqlenv0 :: SQLEnv
sqlenv0 = SQLEnv Map.empty Map.empty Map.empty contigsymS contigsymS contigsymS contigsymS contigsymS contigsymS contigsymS

-- | Relation type accessors
srlkup :: RTypeEnv -> Identifier -> Except String (K3 Type)
srlkup env x = maybe err return $ Map.lookup x env
  where err = throwE $ "Unknown relation in sql parser environment: " ++ show x

srext :: RTypeEnv -> Identifier -> K3 Type -> RTypeEnv
srext env x t = Map.insert x t env

srdel :: RTypeEnv -> Identifier -> RTypeEnv
srdel env x = Map.delete x env

-- | Dependency graph accessors.
sglkup :: ADGraph -> ADGPtr -> Except String ADGNode
sglkup g p = maybe err return $ Map.lookup p g
  where err = throwE $ "Unknown attribute node in sql parser environment: " ++ show p

sgext :: ADGraph -> ADGNode -> ParGenSymS -> (ADGPtr, ParGenSymS, ADGraph)
sgext g n sym = (ptr, nsym, Map.insert ptr n g)
  where (nsym, ptr) = gensym sym

-- | Scope environment accessors.
sflkup :: ScopeFrame -> [Identifier] -> Except String ADGPtr
sflkup (fr,_,_) path = maybe err return $ Map.lookup path fr
  where err = throwE $ unwords ["Invalid scope path:", show path, "in", show fr]

sftrylkup :: ScopeFrame -> [Identifier] -> Maybe ADGPtr
sftrylkup (fr,_,_) path = Map.lookup path fr

sfptrs :: ScopeFrame -> Except String [ADGPtr]
sfptrs sf@(fr, ord, _) = mapM (\p -> sflkup sf p) ord

sfpush :: ScopeFrame -> [Identifier] -> ADGPtr -> ScopeFrame
sfpush (fr,ord,q) path ptr = (Map.insert path ptr fr, ord ++ [path], nq)
  where nq = if length path > 1 then (if init path `notElem` q then q ++ [init path] else q) else q

sfpop :: ScopeFrame -> [Identifier] -> (Maybe ADGPtr, ScopeFrame)
sfpop (fr,ord,q) path = (npopt, (nfr, filter (== path) ord, newq))
  where (npopt, nfr) = Map.updateLookupWithKey (\_ _ -> Nothing) path fr
        newquals = filter (not . null) $ map (\p -> if length p > 1 then init p else []) $ Map.keys nfr
        newq = filter (`elem` newquals) q

sclkup :: ScopeEnv -> ScopeId -> Except String ScopeFrame
sclkup env p = maybe err return $ Map.lookup p env
  where err = throwE $ "Unknown scope: " ++ show p

scpush :: ScopeEnv -> ScopeFrame -> ParGenSymS -> (ScopeId, ParGenSymS, ScopeEnv)
scpush env fr sym = (ptr, nsym, Map.insert ptr fr env)
  where (nsym, ptr) = gensym sym

scpop :: ScopeEnv -> (Maybe ScopeFrame, ScopeEnv)
scpop env = if Map.null env then (Nothing, env) else (Just fr, Map.delete sp env)
  where (sp, fr) = Map.findMax env

scflkup :: ScopeEnv -> ScopeId -> [Identifier] -> Except String ADGPtr
scflkup env sp path = sclkup env sp >>= \fr -> sflkup fr path

scftrylkup :: ScopeEnv -> ScopeId -> [Identifier] -> Except String (Maybe ADGPtr)
scftrylkup env sp path = sclkup env sp >>= \fr -> return $ sftrylkup fr path

scfptrs :: ScopeEnv -> ScopeId -> Except String [ADGPtr]
scfptrs env sp = sclkup env sp >>= sfptrs

scfpush :: ScopeEnv -> [Identifier] -> ADGPtr -> ParGenSymS -> (ScopeId, ParGenSymS, ScopeEnv)
scfpush env path np sym = if Map.null env then scpush env (Map.singleton path np, [path], pathqual) sym
                                          else (sp, sym, Map.insert sp (sfpush fr path np) env)
  where (sp,fr) = Map.findMax env
        pathqual = if length path > 1 then [init path] else []

scfpop :: ScopeEnv -> [Identifier] -> (Maybe ADGPtr, ScopeEnv)
scfpop env path = if Map.null env then (Nothing, env) else (npopt, Map.insert sp nfr env)
  where (sp, fr) = Map.findMax env
        (npopt, nfr) = sfpop fr path


-- | Symbol generation.
sasext :: SQLEnv -> (Int, SQLEnv)
sasext senv = (n, senv {aliassym = nsym})
  where (nsym, n) = gensym (aliassym senv)

stgsext :: SQLEnv -> (Int, SQLEnv)
stgsext senv = (n, senv {stgsym = nsym})
  where (nsym, n) = gensym (stgsym senv)

ssqsext :: SQLEnv -> (Int, SQLEnv)
ssqsext senv = (n, senv {sqsym = nsym})
  where (nsym, n) = gensym (sqsym senv)

slblsext :: SQLEnv -> (Int, SQLEnv)
slblsext senv = (n, senv {slblsym = nsym})
  where (nsym, n) = gensym (slblsym senv)

saggsext :: SQLEnv -> (Int, SQLEnv)
saggsext senv = (n, senv {aggdsym = nsym})
  where (nsym, n) = gensym (aggdsym senv)

-- | State accessors.
sqrlkup :: SQLEnv -> Identifier -> Except String (K3 Type)
sqrlkup senv n = srlkup (relations senv) n

sqrext :: SQLEnv -> Identifier -> K3 Type -> SQLEnv
sqrext senv n t = senv { relations = srext (relations senv) n t }

sqrdel :: SQLEnv -> Identifier -> SQLEnv
sqrdel senv n = senv { relations = srdel (relations senv) n }

sqglkup :: SQLEnv -> ADGPtr -> Except String ADGNode
sqglkup senv p = sglkup (adgraph senv) p

sqgext :: SQLEnv -> ADGNode -> (ADGPtr, SQLEnv)
sqgext senv n = (ptr, senv {adgraph = ng, adpsym = nsym})
  where (ptr, nsym, ng) = sgext (adgraph senv) n (adpsym senv)

sqclkup :: SQLEnv -> ScopeId -> Except String ScopeFrame
sqclkup env p = sclkup (scopeenv env) p

sqcpush :: SQLEnv -> ScopeFrame -> (ScopeId, SQLEnv)
sqcpush env fr = (nsp, env {scopeenv = nenv, spsym = nsym})
  where (nsp, nsym, nenv) = scpush (scopeenv env) fr (spsym env)

sqcpop :: SQLEnv -> (Maybe ScopeFrame, SQLEnv)
sqcpop env = (fropt, env { scopeenv = nsenv })
  where (fropt, nsenv) = scpop (scopeenv env)

sqcflkup :: SQLEnv -> ScopeId -> [Identifier] -> Except String ADGPtr
sqcflkup env sp path = scflkup (scopeenv env) sp path

sqcftrylkup :: SQLEnv -> ScopeId -> [Identifier] -> Except String (Maybe ADGPtr)
sqcftrylkup env sp path = scftrylkup (scopeenv env) sp path

sqcfptrs :: SQLEnv -> ScopeId -> Except String [ADGPtr]
sqcfptrs env sp = scfptrs (scopeenv env) sp

sqcfpush :: SQLEnv -> [Identifier] -> ADGPtr -> (ScopeId, SQLEnv)
sqcfpush env path np = (nsp, env {scopeenv = nsenv, spsym = nsym})
  where (nsp, nsym, nsenv) = scfpush (scopeenv env) path np (spsym env)

sqcfpop :: SQLEnv -> [Identifier] -> (Maybe ADGPtr, SQLEnv)
sqcfpop env path = (npopt, env {scopeenv = nsenv})
  where (npopt, nsenv) = scfpop (scopeenv env) path


-- | Monadic accessors.
sqrlkupM :: Identifier -> SQLParseM (K3 Type)
sqrlkupM n = get >>= liftExceptM . (\env -> sqrlkup env n)

sqrextM :: Identifier -> K3 Type -> SQLParseM ()
sqrextM n t = get >>= \env -> return (sqrext env n t) >>= put

sqglkupM :: ADGPtr -> SQLParseM ADGNode
sqglkupM p = get >>= liftExceptM . (\env -> sqglkup env p)

sqgextM :: ADGNode -> SQLParseM ADGPtr
sqgextM n = get >>= \env -> return (sqgext env n) >>= \(r, nenv) -> put nenv >> return r

sqclkupM :: ScopeId -> SQLParseM ScopeFrame
sqclkupM sp = get >>= liftExceptM . (\env -> sqclkup env sp)

sqcpushM :: ScopeFrame -> SQLParseM ScopeId
sqcpushM fr = get >>= \env -> return (sqcpush env fr) >>= \(r, nenv) -> put nenv >> return r

sqcpopM :: SQLParseM (Maybe ScopeFrame)
sqcpopM = get >>= \env -> return (sqcpop env) >>= \(r, nenv) -> put nenv >> return r

sqcflkupM :: ScopeId -> [Identifier] -> SQLParseM ADGPtr
sqcflkupM sp path = get >>= liftExceptM . (\env -> sqcflkup env sp path)

sqcftrylkupM :: ScopeId -> [Identifier] -> SQLParseM (Maybe ADGPtr)
sqcftrylkupM sp path = get >>= liftExceptM . (\env -> sqcftrylkup env sp path)

sqcfptrsM :: ScopeId -> SQLParseM [ADGPtr]
sqcfptrsM sp = get >>= liftExceptM . (\env -> sqcfptrs env sp)

sqcfpushM :: [Identifier] -> ADGPtr -> SQLParseM ScopeId
sqcfpushM path np = get >>= \env -> return (sqcfpush env path np) >>= \(r, nenv) -> put nenv >> return r

sqcfpopM :: [Identifier] -> SQLParseM (Maybe ADGPtr)
sqcfpopM path = get >>= \env -> return (sqcfpop env path) >>= \(r, nenv) -> put nenv >> return r

sasextM :: SQLParseM Int
sasextM = get >>= \env -> return (sasext env) >>= \(i, nenv) -> put nenv >> return i

stgsextM :: SQLParseM Int
stgsextM =  get >>= \env -> return (stgsext env) >>= \(i, nenv) -> put nenv >> return i

ssqsextM :: SQLParseM Int
ssqsextM = get >>= \env -> return (ssqsext env) >>= \(i, nenv) -> put nenv >> return i

slblsextM :: SQLParseM Int
slblsextM = get >>= \env -> return (slblsext env) >>= \(i, nenv) -> put nenv >> return i

saggsextM :: SQLParseM Int
saggsextM = get >>= \env -> return (saggsext env) >>= \(i, nenv) -> put nenv >> return i


{- Scope construction -}
sqgextScopeM :: Identifier -> [Identifier] -> [ADGPtr] -> SQLParseM ScopeId
sqgextScopeM qualifier ids ptrs = do
    let paths = map (:[]) ids
    let qpaths = map (\i -> [qualifier,i]) ids
    sqcpushM (Map.fromList $ zip paths ptrs ++ zip qpaths ptrs, paths, [[qualifier]])

sqgextSchemaM :: Maybe Identifier -> K3 Type -> SQLParseM ScopeId
sqgextSchemaM (Just n) t = do
    rt <- telemM t
    case tnc rt of
      (TRecord ids, ch) -> do
        ptrs <- mapM sqgextM $ map (\(i, ct) -> ADGNode i ct (Just n) Nothing []) $ zip ids ch
        sqgextScopeM n ids ptrs

      _ -> throwE $ boxToString $ ["Invalid relational element type"] %$ prettyLines rt

sqgextSchemaM _ _ = throwE "No relation name specified when extending attribute graph"

sqgextAliasM :: Maybe Identifier -> K3 Type -> [ADGPtr] -> SQLParseM ScopeId
sqgextAliasM (Just n) t srcptrs = do
  rt <- telemM t
  case tag rt of
    TRecord dstids | length dstids == length srcptrs -> do
      destnodes <- mapM mknode $ zip dstids srcptrs
      destptrs <- mapM sqgextM destnodes
      sqgextScopeM n dstids destptrs

    _ -> throwE $ boxToString $ ["Invalid alias type when extending attribute graph"] %$ prettyLines rt

  where mknode (d, ptr) = do
          node <- sqglkupM ptr
          return $ ADGNode d (adnt node) (Just n) (Just $ Identifier emptyAnnotation $ Nmc $ adnn node) [ptr]

sqgextAliasM _ _ _ = throwE "Invalid alias arguments when extending attribute graph"

-- | Extend the attribute graph for a relation type computed from the given expressions.
sqgextExprM :: ScopeId -> Maybe Identifier -> [(Identifier, K3 Type, ScalarExpr)] -> SQLParseM ScopeId
sqgextExprM sid (Just n) exprs = do
    nodes <- mapM mknode exprs
    ptrs <- mapM sqgextM nodes
    sqgextScopeM n (map (\(i,_,_) -> i) exprs) ptrs

  where mknode (i, t, e) = do
          eptrs <- exprAttrs sid e
          return $ ADGNode i t (Just n) (Just e) eptrs

sqgextExprM _ _ _ = throwE "Invalid expr arguments when extending attribute graph"


{- Relation type and name construction. -}

sqltabletype :: AttributeDefList -> SQLParseM (K3 Type)
sqltabletype attrs = sqlrectype attrs >>= tcolM

sqlrectype :: AttributeDefList -> SQLParseM (K3 Type)
sqlrectype attrs = mapM sqlattr attrs >>= \ts -> return (recT ts)

sqlattr :: AttributeDef -> SQLParseM (Identifier, K3 Type)
sqlattr (AttributeDef _ nm t _ _) = sqlnamedtype t >>= return . (sqlnmcomponent nm,)

-- TODO: timestamp, interval, size limits for numbers
sqltype :: String -> Maybe Int -> Maybe Int -> SQLParseM (K3 Type)
sqltype s lpOpt uOpt = case s of
  "int"               -> return TC.int
  "integer"           -> return TC.int
  "real"              -> return TC.real
  "double precision"  -> return TC.real
  "text"              -> return TC.string
  "varchar"           -> return $ maybe TC.string (\i -> TC.string @+ TProperty (Left $ "TPCHVarchar_" ++ show i)) lpOpt
  "date"              -> return $ TC.int @+ TProperty (Left "TPCHDate")
  _ -> throwE $ "Invalid K3-SQL type: " ++ s

sqlnamedtype :: TypeName -> SQLParseM (K3 Type)
sqlnamedtype tn = case tn of
  ArrayTypeName _ ctn   -> sqlnamedtype ctn >>= \t -> return $ (TC.collection t) @+ TAnnotation "Vector"
  Prec2TypeName _ s l u -> sqltype s (Just $ fromInteger l) (Just $ fromInteger u)
  PrecTypeName _ s p    -> sqltype s (Just $ fromInteger p) Nothing
  SetOfTypeName _ ctn   -> sqlnamedtype ctn >>= \t -> return $ (TC.collection t) @+ TAnnotation "Set"
  SimpleTypeName _ s    -> sqltype s Nothing Nothing

sqlnm :: Name -> String
sqlnm (Name _ comps) = concatMap sqlnmcomponent comps

sqlnmcomponent :: NameComponent -> String
sqlnmcomponent (Nmc s) = s
sqlnmcomponent (QNmc s) = s

sqlnmpath :: [NameComponent] -> [String]
sqlnmpath nmcl = map sqlnmcomponent nmcl

sqltablealias :: Identifier -> TableAlias -> Maybe Identifier
sqltablealias def alias = case alias of
    NoAlias _        -> Just def
    TableAlias _ nc  -> Just $ "__" ++ sqlnmcomponent nc
    FullAlias _ nc _ -> Just $ "__" ++ sqlnmcomponent nc

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


{- SQL AST helpers. -}
projectionexprs :: SelectItemList -> ScalarExprList
projectionexprs sl = map projectionexpr sl

projectionexpr :: SelectItem -> ScalarExpr
projectionexpr (SelExp _ e) = e
projectionexpr (SelectItem _ e _) = e

mkprojection :: ScalarExpr -> SelectItem
mkprojection e = SelExp emptyAnnotation e

aggregateexprs :: SelectItemList -> SQLParseM (ScalarExprList, [AggregateFn])
aggregateexprs sl = mapM aggregateexpr sl >>= return . unzip

aggregateexpr :: SelectItem -> SQLParseM (ScalarExpr, AggregateFn)
aggregateexpr (projectionexpr -> e) = case e of
  FunCall _ nm args -> do
      let fn = sqlnm nm
      case (fn, args) of
        ("sum"  , [e']) -> return (e', AggSum)
        ("count", [e']) -> return (e', AggCount)
        ("min"  , [e']) -> return (e', AggMin)
        ("max"  , [e']) -> return (e', AggMax)
        (_, _) -> throwE $ "Invalid aggregate expression: " ++ show e

  _ -> throwE $ "Invalid aggregate expression: " ++ show e

mkaggregate :: AggregateFn -> SelectItem -> ScalarExpr -> SQLParseM SelectItem
mkaggregate fn agg e = case fn of
  AggSum   -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "sum"])   [e]
  AggCount -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "count"]) [e]
  AggMin   -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "min"])   [e]
  AggMax   -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "max"])   [e]

  where rt (SelExp _ _) e' = return (SelExp emptyAnnotation e')
        rt (SelectItem _ _ nmc) e' = return (SelectItem emptyAnnotation e' nmc)

isAggregate :: ScalarExpr -> SQLParseM Bool
isAggregate (FunCall _ nm args) = do
  let fn = sqlnm nm
  case (fn, args) of
    ("sum"  , [_]) -> return True
    ("count", [_]) -> return True
    ("min"  , [_]) -> return True
    ("max"  , [_]) -> return True
    (_, _) -> return False

isAggregate _ = return False

-- TODO: qualified, more expression types
substituteExpr :: [(Identifier, ScalarExpr)] -> ScalarExpr -> SQLParseM ScalarExpr
substituteExpr bindings e =  case e of
  (Identifier _ (sqlnmcomponent -> i)) -> return $ maybe e id $ lookup i bindings
  (QIdentifier _ _) -> throwE "Cannot substitute qualified expressions."
  (FunCall ann nm args) -> mapM (substituteExpr bindings) args >>= \nargs -> return $ FunCall ann nm nargs
  _ -> return e


{- Scope accessors. -}
attrIds :: AttrEnv -> SQLParseM [Identifier]
attrIds env = mapM (\ptr -> adnn <$> sqglkupM ptr) $ Map.elems env

singletonPath :: AttrPath -> SQLParseM Identifier
singletonPath [i] = return i
singletonPath p = throwE $ "Invalid singleton path: " ++ show p

uniqueScopeQualifier :: ScopeId -> SQLParseM Identifier
uniqueScopeQualifier sid = do
    (_, _, quals) <- sqclkupM sid
    case quals of
      [[q]] -> return q
      _ -> throwE $ "Invalid unique scope qualifier: " ++ show quals

unqualifiedAttrs :: ScopeFrame -> SQLParseM [Identifier]
unqualifiedAttrs (_, ord, _) = forM ord $ singletonPath

qualifiedAttrs :: AttrPath -> ScopeFrame -> SQLParseM AttrEnv
qualifiedAttrs prefix (fr, _, q)
  | prefix `elem` q = return $ Map.filterWithKey (\path _ -> prefix `isPrefixOf` path) fr
  | otherwise = throwE $ unwords ["Could not find qualifier:", show prefix, "(", show q, ")"]

partitionAttrEnv :: ScopeFrame -> SQLParseM (AttrEnv, AttrEnv)
partitionAttrEnv (fr,_,_) = return $ Map.partitionWithKey (\path _ -> length path <= 1) fr

partitionCommonQualifedAttrEnv :: ScopeFrame -> SQLParseM (Map AttrPath AttrEnv)
partitionCommonQualifedAttrEnv (fr, ord, _) = return $ Map.foldlWithKey (addQualifiedCommon ord) Map.empty fr
  where
    addQualifiedCommon commons acc path ptr
      | length path <= 1 = acc
      | otherwise =
        let (qual, attr) = (init path, last path) in
        if [attr] `notElem` commons then acc else Map.alter (inject [attr] ptr) qual acc

    inject path ptr Nothing = Just $ Map.singleton path ptr
    inject path ptr (Just aenv) = Just $ Map.insert path ptr aenv

unqualifiedScopeFrame :: ScopeFrame -> SQLParseM ScopeFrame
unqualifiedScopeFrame (fr, ord, _)
  | all (\path -> length path == 1) ord = return (Map.filterWithKey (\path _ -> length path == 1) fr, ord, [])
  | otherwise = throwE "Unable to extract unqualified scope (lossy order specification)"

qualifiedScopeFrame :: ScopeFrame -> SQLParseM ScopeFrame
qualifiedScopeFrame (fr, _, q) = return (Map.filterWithKey (\path _ -> length path > 1) fr, [], q)

renameScopeFrame :: Identifier -> ScopeFrame -> SQLParseM ScopeFrame
renameScopeFrame i sf = do
  (ufr, ord, _) <- unqualifiedScopeFrame sf
  return (ufr <> Map.mapKeys (\path -> [i] ++ path) ufr, ord, [[i]])

concatScopeFrames :: ScopeFrame -> ScopeFrame -> SQLParseM ScopeFrame
concatScopeFrames (lfr,lord,lq) (rfr,rord,rq) = return (lfr <> rfr, lord ++ rord, lq++rq)

mergeScopeFrames :: ScopeFrame -> ScopeFrame -> SQLParseM ScopeFrame
mergeScopeFrames lsf@(_, lord, lquals) rsf@(_, rord, rquals) = do
  let common = lord `intersect` rord
  let nord = (nub $ lord ++ rord) \\ common
  ((lu,lq), (ru,rq)) <- (,) <$> partitionAttrEnv lsf <*> partitionAttrEnv rsf
  let nu = (foldl (flip Map.delete) lu common) <> (foldl (flip Map.delete) ru common)
  return (nu <> lq <> rq, nord, lquals ++ rquals)

mergeScopes :: ScopeId -> ScopeId -> SQLParseM ScopeId
mergeScopes id1 id2 = do
  (lsf, rsf) <- (,) <$> sqclkupM id1 <*> sqclkupM id2
  nsf <- mergeScopeFrames lsf rsf
  sqcpushM nsf

outputTypeAndQualifier :: K3 Type -> Identifier -> Maybe TableAlias -> SQLParseM (K3 Type, Maybe Identifier)
outputTypeAndQualifier t pfx alOpt = do
    sym <- sasextM
    case alOpt of
      Nothing -> return (t, Just $ pfx ++ show sym)
      Just al -> (,) <$> taliascolM al t <*> return (sqltablealias (pfx ++ show sym) al)

typedOutputScope :: K3 Type -> Identifier -> Maybe TableAlias -> SQLParseM ScopeId
typedOutputScope t pfx alOpt = do
    (rt, tid) <- outputTypeAndQualifier t pfx alOpt
    sqgextSchemaM tid rt

outputScope :: ScopeId -> Identifier -> Maybe TableAlias -> SQLParseM ScopeId
outputScope sid pfx alOpt = do
    t <- scopeType sid
    typedOutputScope t pfx alOpt

aliasedOutputScope :: ScopeId -> Identifier -> Maybe TableAlias -> SQLParseM ScopeId
aliasedOutputScope sid pfx alOpt = do
    (t, ptrs) <- (,) <$> scopeType sid <*> sqcfptrsM sid
    (rt, tid) <- outputTypeAndQualifier t pfx alOpt
    sqgextAliasM tid rt ptrs

exprOutputScope :: ScopeId -> Identifier -> [(Identifier, K3 Type, ScalarExpr)] -> SQLParseM ScopeId
exprOutputScope sid pfx exprs = do
    sym <- sasextM
    let qual = Just $ pfx ++ show sym
    sqgextExprM sid qual exprs


{- Binding map helpers. -}
bmelem :: BindingMap
bmelem = BMTPrefix "elem"

{- Query plan accessors. -}
ttag :: PlanTree -> PlanNode
ttag (Node tg _) = tg

replaceData :: Tree a -> a -> Tree a
replaceData (Node _ ch) n = Node n ch

isEquiJoin :: PlanTree -> Bool
isEquiJoin (ttag -> PJoin _ _ _ (_:_) [] _ _) = True
isEquiJoin _ = False

isJoin :: PlanTree -> Bool
isJoin (ttag -> PJoin _ _ _ _ _ _ _) = True
isJoin _ = False

{- Path/chain accessors. -}
trypath :: Maybe ScopeId -> SQLParseM a -> (ADGPtr -> SQLParseM a) -> AttrPath -> SQLParseM a
trypath sidOpt rfail rsuccess path = (\f -> maybe rfail f sidOpt) $ \sid -> do
  pOpt <- sqcftrylkupM sid path
  maybe rfail rsuccess pOpt

isAggregatePath :: PlanCPath -> Bool
isAggregatePath (PlanCPath _ _ [] [] _ _ _) = True
isAggregatePath _ = False

isGroupByAggregatePath :: PlanCPath -> Bool
isGroupByAggregatePath (PlanCPath _ _ (_:_) _ (_:_) _ _) = True
isGroupByAggregatePath _ = False

isNonAggregatePath :: PlanCPath -> Bool
isNonAggregatePath (PlanCPath _ _ [] _ [] Nothing _) = True
isNonAggregatePath _ = False

planNodeChains :: PlanNode -> Maybe [PlanCPath]
planNodeChains (PJoin _ _ _ _ _ _ chains) = Just chains
planNodeChains (PTable _ _ _ _ chains) = Just chains
planNodeChains (PSubquery _ qcl) = queryClosureChains qcl

treeChains :: PlanTree -> Maybe [PlanCPath]
treeChains (Node n _) = planNodeChains n

queryPlanChains :: QueryPlan -> Maybe [PlanCPath]
queryPlanChains (QueryPlan tOpt chains _) = if null chains then maybe Nothing treeChains tOpt else Just chains

queryClosureChains :: QueryClosure -> Maybe [PlanCPath]
queryClosureChains (QueryClosure _ plan) = queryPlanChains plan

pcext :: PlanCPath -> PlanTree -> SQLParseM PlanTree
pcext p (Node n ch) = case n of
  PJoin psid osid jt jeq jp jpb chains -> return $ Node (PJoin psid osid jt jeq jp jpb $ chains ++ [p]) ch
  PTable i tsid trOpt bmOpt chains -> return $ Node (PTable i tsid trOpt bmOpt $ chains ++ [p]) ch
  PSubquery osid qcl -> pcextclosure p qcl >>= \nqcl -> return $ Node (PSubquery osid nqcl) ch

  where pcextclosure p (QueryClosure fvs plan) = pcextplan p plan >>= \nplan -> return $ QueryClosure fvs nplan
        pcextplan p (QueryPlan tOpt chains stgOpt) = return $ QueryPlan tOpt (chains ++ [p]) stgOpt

pcextSelect :: ScopeId -> SubqueryBindings -> ScalarExpr -> [PlanCPath] -> [PlanCPath]
pcextSelect sid qbs p [] = [PlanCPath sid [p] [] [] [] Nothing qbs]
pcextSelect _ qbs p pcl@(last -> c) = init pcl ++ [c {pcselects = pcselects c ++ [p], pcbindings = pcbindings c ++ qbs}]

pcextGroupBy :: SelectItemList -> SelectItemList -> PlanNode -> SQLParseM PlanNode
pcextGroupBy gbs aggs n@(PJoin _ osid _ _ _ _ chains) = do
  aggsid <- aggregateSchema (Just $ chainSchema osid chains) gbs aggs
  return $ n { pnjpath = chains ++ [PlanCPath aggsid [] (projectionexprs gbs) gbs aggs Nothing []] }

pcextGroupBy gbs aggs n@(PTable _ sid _ _ chains) = do
  aggsid <- aggregateSchema (Just $ chainSchema sid chains) gbs aggs
  return $ n { pntpath = chains ++ [PlanCPath aggsid [] (projectionexprs gbs) gbs aggs Nothing []] }

pcextGroupBy gbs aggs n@(PSubquery _ qcl) = extPlan (qcplan qcl) >>= \p -> return $ n { pnqclosure = qcl { qcplan = p } }
  where extPlan p@(QueryPlan tOpt chains stgOpt) = do
          sid <- planSchema p
          aggsid <- aggregateSchema (Just sid) gbs aggs
          return $ QueryPlan tOpt (chains ++ [PlanCPath aggsid [] (projectionexprs gbs) gbs aggs Nothing []]) stgOpt

pcNonAggExprs :: ScopeId -> [PlanCPath] -> [(ScopeId, ScalarExprList)]
pcNonAggExprs _ [] = []
pcNonAggExprs sid (h:t) = snd $ foldl accum (pcoutsid h, [extract sid h]) t
  where accum (sid', expracc) pcp = (pcoutsid pcp, expracc ++ [extract sid' pcp])
        extract sid' (PlanCPath _ selects groupbys projects _ _ _) = (sid', selects ++ groupbys ++ projectionexprs projects)

pcAggExprs :: ScopeId -> [PlanCPath] -> [(ScopeId, ScalarExprList)]
pcAggExprs _ [] = []
pcAggExprs sid (h:t) = snd $ foldl accum (pcoutsid h, [extract sid h]) t
  where accum (sid', expracc) pcp = (pcoutsid pcp, expracc ++ [extract sid' pcp])
        extract sid' (PlanCPath _ _ _ _ aggs _ _) = (sid', projectionexprs aggs)

chainSchema :: ScopeId -> [PlanCPath] -> ScopeId
chainSchema sid chains = if null chains then sid else pcoutsid $ last chains

treeSchema :: PlanTree -> SQLParseM ScopeId
treeSchema (ttag -> PJoin _ sid _ _ _ _ chains) = return $ chainSchema sid chains
treeSchema (ttag -> PSubquery sid _) = return sid
treeSchema (ttag -> PTable _ sid _ _ chains) = return $ chainSchema sid chains
treeSchema _ = throwE "Invalid plan node input for treeSchema"

planSchema :: QueryPlan -> SQLParseM ScopeId
planSchema (QueryPlan Nothing [] _) = throwE "Invalid query plan with no tables or expressions"
planSchema (QueryPlan (Just t) [] _) = treeSchema t
planSchema (QueryPlan _ chains _) = return $ pcoutsid $ last chains

aggregateSchema :: Maybe ScopeId -> SelectItemList -> SelectItemList -> SQLParseM ScopeId
aggregateSchema (Just sid) [] [] = return sid
aggregateSchema sidOpt projects aggregates = do
    let pexprs = projectionexprs projects
    let aexprs = projectionexprs aggregates
    (prji, prjids) <- foldM selectItemIdAcc (0, []) projects
    (_, aggids) <- foldM selectItemIdAcc (prji, []) aggregates
    prjt <- mapM (scalarexprType sidOpt) pexprs
    aggt <- mapM (aggregateType sidOpt) aexprs
    case sidOpt of
      Nothing -> typedOutputScope (recT $ zip prjids prjt ++ zip aggids aggt) "__RN" Nothing
      Just sid -> exprOutputScope sid "__AGG" $ (zip3 prjids prjt pexprs) ++ (zip3 aggids aggt aexprs)

refreshInputSchema :: PlanNode -> [PlanTree] -> SQLParseM PlanNode
refreshInputSchema (PJoin _ _ jt jeq jp jpb chains) [l,r] = do
    (lsid, rsid)  <- (,) <$> treeSchema l <*> treeSchema r
    jpsid         <- mergeScopes lsid rsid
    josid         <- outputScope jpsid "__JR" Nothing
    return $ PJoin jpsid josid jt jeq jp jpb chains

refreshInputSchema n [] = return n
refreshInputSchema _ _ = throwE "Invalid plan tree node for refreshInputSchema"

scopeType :: ScopeId -> SQLParseM (K3 Type)
scopeType sid = do
  sf@(_, ord, _) <- sqclkupM sid >>= unqualifiedScopeFrame
  ptrs <- mapM (\path -> liftExceptM $ sflkup sf path) ord
  nodes <- mapM sqglkupM ptrs
  tcolM $ recT $ map (adnn &&& adnt) nodes

k3ScopeType :: ScopeId -> BindingMap -> SQLParseM (K3 Type)
k3ScopeType sid bm = do
  t <- scopeType sid
  rt <- telemM t
  case (tnc rt, bm) of
    ((TRecord ids, ch), BMNone) -> return t
    ((TRecord ids, ch), BMTPrefix j) -> tcolM $ recT [(j,rt)]
    ((TRecord ids, ch), BMTFieldMap fb) -> namedRecordT fb (zip ids ch) >>= tcolM
    ((TRecord ids, ch), BMTVPartition pb) -> throwE "BMTVPartition mapping unsupported in k3ScopeType"
    _ -> throwE "Invalid k3ScopeType element type"

  where
    namedRecordT fb idt = foldM (field fb) [] idt >>= return . recT
    field fb acc (j,t) = maybe (err j) (extendNestedRecord t acc) $ Map.lookup j fb

    extendNestedRecord _ fieldsAcc [] = throwE "Invalid nested record extension"
    extendNestedRecord t fieldsAcc [i] = return $ fieldsAcc ++ [(i,t)]
    extendNestedRecord t fieldsAcc (h:rest) =
      case lookup h fieldsAcc of
        Nothing -> do
          subfields <- extendNestedRecord t [] rest
          return $ fieldsAcc ++ [(h, recT subfields)]

        Just (tnc -> (TRecord ids, tch)) -> do
          subfields <- extendNestedRecord t (zip ids tch) rest
          return $ map (replaceField h $ recT subfields) fieldsAcc

        Just _ -> throwE $ "Existing non-record field when attempting to extend nested record"

    replaceField dst nt (src,t) | src == dst = (dst, nt)
                                | otherwise = (src, t)

    err j = throwE $ "No field binding found in namedRecordT for " ++ show j

k3PlanType :: BindingMap -> QueryPlan -> SQLParseM (K3 Type)
k3PlanType _ (QueryPlan Nothing [] _) = throwE "Invalid query plan with no tables or expressions"
k3PlanType bm p = do
  sid <- planSchema p
  case queryPlanChains p of
    Just [] -> k3ScopeType sid bm
    Just l | isAggregatePath (last l) -> do
      t <- k3ScopeType sid bm
      rt <- telemM t
      case tnc rt of
        (TRecord ids, ch) -> zeroT $ zip ids ch
        _ -> throwE "Invalid k3 aggregate plan type"

    _ -> k3ScopeType sid bm


-- TODO:
-- i. builtin function types
-- ii. AST: AggregateFn, Extract, Interval, LiftOperator, NullLit, Placeholder, PositionalArg, WindowFn
scalarexprType :: Maybe ScopeId -> ScalarExpr -> SQLParseM (K3 Type)
scalarexprType _ (BooleanLit _ _) = return TC.bool
scalarexprType _ (StringLit _ _) = return TC.string
scalarexprType _ (NumberLit _ i) = return $ if "." `isInfixOf` i then TC.real else TC.int
scalarexprType _ (TypedStringLit _ tn _) = sqlnamedtype tn

scalarexprType _ (Cast _ _ tn) = sqlnamedtype tn

scalarexprType sidOpt (Identifier _ (sqlnmcomponent -> i)) = do
  sf <- maybe (return Nothing) (\i -> sqclkupM i >>= return . Just) sidOpt
  trypath sidOpt (trace (unwords ["bottom", i, show sidOpt, show sf]) $ return TC.bottom) (\ptr -> sqglkupM ptr >>= return . adnt) [i]

scalarexprType sidOpt (QIdentifier _ (sqlnmpath -> path)) = do
  sf <- maybe (return Nothing) (\i -> sqclkupM i >>= return . Just) sidOpt
  trypath sidOpt (trace (unwords ["bottom", show path, show sidOpt, show sf]) $ return TC.bottom) (\ptr -> sqglkupM ptr >>= return . adnt) path

scalarexprType sidOpt (Case _ whens elseexpr) = maybe (caselistType sidOpt whens) (scalarexprType sidOpt) elseexpr
scalarexprType sidOpt (CaseSimple _ expr whens elseexpr) = maybe (caselistType sidOpt whens) (scalarexprType sidOpt) elseexpr

scalarexprType sidOpt (FunCall _ nm args) = do
  let fn = sqlnm nm
  case sqloperator fn args of
    (Just (UnaryOp  _ x))   -> scalarexprType sidOpt x
    (Just (BinaryOp _ x y)) -> do
      xt <- scalarexprType sidOpt x
      yt <- scalarexprType sidOpt y
      if xt == yt then return xt
                  else throwE $ boxToString $ ["Binary operator sql type mismatch"]
                              %$ prettyLines xt %$ prettyLines yt

    _ -> do
      case (fn, args) of
        ("!between", [_,_,_]) -> return TC.bool
        ("!like", [_,_])      -> return TC.bool
        ("!notlike", [_,_])   -> return TC.bool
        (_, _) -> throwE $ "Unsupported function in scalarexprType: " ++ fn

scalarexprType (Just sid) (Star _) = scopeType sid >>= telemM
scalarexprType (Just sid) (QStar _ (sqlnmcomponent -> n)) = do
  (fr,_,_) <- sqclkupM sid
  let ptrs = Map.elems $ Map.filterWithKey (\k _ -> [n] `isPrefixOf` k) fr
  idt <- mapM (\p -> sqglkupM p >>= return . (adnn &&& adnt)) ptrs
  return $ recT idt

scalarexprType _ (ScalarSubQuery _ _) = return TC.bool -- TODO: return subquery type, not bool!
scalarexprType _ (Exists _ _) = return TC.bool
scalarexprType _ (InPredicate _ _ _ _) = return TC.bool

scalarexprType _ e = throwE $ "Type inference unsupported for: " ++ show e

caselistType :: Maybe ScopeId -> [([ScalarExpr], ScalarExpr)] -> SQLParseM (K3 Type)
caselistType _ [] = throwE $ "Invalid empty case-list in caselistType"
caselistType sidOpt ((_,e):_) = scalarexprType sidOpt e

aggregateType :: Maybe ScopeId -> ScalarExpr -> SQLParseM (K3 Type)
aggregateType sidOpt agg@(FunCall _ nm args) = do
      let fn = sqlnm nm
      case (fn, args) of
        ("sum"  , [e]) -> scalarexprType sidOpt e
        ("count", [e]) -> return $ TC.int
        ("min"  , [e]) -> scalarexprType sidOpt e
        ("max"  , [e]) -> scalarexprType sidOpt e
        (_, _) -> throwE $ "Invalid aggregate expression: " ++ show agg

aggregateType _ agg = throwE $ "Invalid aggregate expression: " ++ show agg

selectItemId :: Int -> SelectItem -> SQLParseM (Int, Identifier)
selectItemId i (SelExp _ (Identifier _ (Nmc n))) = return (i, n)
selectItemId i (SelExp _ _) = return (i+1, "f" ++ show i)
selectItemId i (SelectItem _ _ (sqlnmcomponent -> n)) = return (i, n)

selectItemIdAcc :: (Int, [Identifier]) -> SelectItem -> SQLParseM (Int, [Identifier])
selectItemIdAcc (i, acc) (SelExp _ (Identifier _ (Nmc n))) = return (i, acc ++ [n])
selectItemIdAcc (i, acc) (SelExp _ _) = return (i+1, acc ++ ["f" ++ show i])
selectItemIdAcc (i, acc) (SelectItem _ _ (sqlnmcomponent -> n)) = return (i, acc ++ [n])


nodeBindingMap :: PlanNode -> SQLParseM BindingMap
nodeBindingMap (PJoin _ _ _ _ _ _ _) = return bmelem
nodeBindingMap _ = return BMNone

chainBindingMap :: PlanCPath -> SQLParseM BindingMap
chainBindingMap (PlanCPath _ _ gbs prjs aggs _ _)
  | null gbs && null aggs = return bmelem
  | otherwise = do
    (prji, prjids) <- foldM selectItemIdAcc (0, []) prjs
    (_, aggids) <- foldM selectItemIdAcc (prji, []) aggs
    let (nidx, keyPaths) = prefixTypePath (0::Int) "key" prjids
    let (_, valPaths) = prefixTypePath nidx "value" aggids
    return $ BMTFieldMap $ Map.fromList $ keyPaths ++ valPaths

  where prefixTypePath i pfx l = case l of
          []  -> (i+1, [("f" ++ show i, [pfx])])
          [j] -> (i, [(j, [pfx])])
          _   -> (i, map (\j -> (j, [pfx, j])) l)

treeBindingMap :: PlanTree -> SQLParseM BindingMap
treeBindingMap t = case treeChains t of
  Nothing -> nodeBindingMap $ ttag t
  Just [] -> nodeBindingMap $ ttag t
  Just l -> chainBindingMap $ last l

planBindingMap :: QueryPlan -> SQLParseM BindingMap
planBindingMap (QueryPlan Nothing chains _)
  | null chains = throwE "Invalid query plan with empty tree and chains"
  | otherwise = chainBindingMap $ last chains

planBindingMap (QueryPlan (Just t) chains _)
  | null chains = treeBindingMap t
  | otherwise = chainBindingMap $ last chains


keyValuePrefix :: TypePath -> Bool
keyValuePrefix tp = ["key"] `isPrefixOf` tp || ["value"] `isPrefixOf` tp

keyValueMapping :: TypeMapping -> Bool
keyValueMapping tm = maybe False (either (\pfx -> pfx `elem` ["key", "value"]) keyValuePrefix) tm

isKVBindingMap :: BindingMap -> SQLParseM Bool
isKVBindingMap (BMTFieldMap fields) = return $ Map.foldl (\acc tp -> acc && keyValuePrefix tp) True fields
isKVBindingMap (BMTVPartition partitions) = return $ Map.foldl (\acc (_,tm) -> acc && keyValueMapping tm) True partitions
isKVBindingMap _ = return False

{- Rewriting helpers. -}

-- TODO: case, etc.
-- This function does not descend into subqueries.
-- However, it should include free variables present in the subquery, and defined in
-- the given scope.
exprAttrs :: ScopeId -> ScalarExpr -> SQLParseM [ADGPtr]
exprAttrs sid e = case e of
  (Identifier _ (sqlnmcomponent -> i)) -> sqcflkupM sid [i] >>= return . (:[])
  (QIdentifier _ (sqlnmpath -> path)) -> sqcflkupM sid path >>= return . (:[])
  (FunCall _ _ args) -> mapM (exprAttrs sid) args >>= return . concat
  _ -> return []

-- | Returns pointers bound in a scope frame.
attrEnvPtrs :: ScopeFrame -> [ADGPtr]
attrEnvPtrs (fr, _, _) = nub $ Map.elems fr

-- | Chases a given attribute pointer to the provided roots (or its set of source nodes if
--   no roots are given).
adgchaseM :: [ADGPtr] -> ADGPtr -> SQLParseM [(ADGPtr, ADGNode)]
adgchaseM roots ptr = sqglkupM ptr >>= \n -> chase [] ptr n
  where chase path p n
          | p `elem` path || p `elem` roots = return [(p, n)]
          | null (adnch n) = return [(p, n)]
          | otherwise = mapM (\c -> sqglkupM c >>= chase (path ++ [p]) c) (adnch n) >>= return . concat

adgchaseExprM :: [ADGPtr] -> ScopeId -> ScalarExpr -> SQLParseM ScalarExpr
adgchaseExprM roots sid expression = exprAttrs sid expression >>= \ptrs -> chase [] expression ptrs
  where chase path e ptrs = do
          let remptrs = filter (\p -> p `notElem` roots && p `notElem` path) ptrs
          if null remptrs
            then return e
            else do
              nodes <- mapM sqglkupM remptrs
              let env = concatMap (\(i,eOpt) -> maybe [] (\e' -> [(i,e')]) eOpt) $ map (adnn &&& adne) nodes
              ne <- substituteExpr env e
              chase (path ++ remptrs) ne $ concatMap adnch nodes

-- | Returns a set of nodes visited during a chase on a given list of roots and a starting pointer.
adgchaseNodesM :: [ADGPtr] -> ADGPtr -> SQLParseM [(ADGPtr, ADGNode)]
adgchaseNodesM roots ptr = sqglkupM ptr >>= \node -> chase [(ptr,node)] [] ptr node
  where chase acc path p n
          | p `elem` path || p `elem` roots || null (adnch n) = return acc
          | otherwise = foldM (rcr p path) acc (adnch n)

        rcr p path acc cp = sqglkupM cp >>= \cn -> chase (acc ++ [(cp,cn)]) (path ++ [p]) cp cn


baseRelationsP :: ADGPtr -> SQLParseM [Identifier]
baseRelationsP ptr = adgchaseM [] ptr >>= return . nub . catMaybes . map (adnr . snd)

baseRelationsE :: ScopeId -> ScalarExpr -> SQLParseM [Identifier]
baseRelationsE sid expression = do
  ptrs <- exprAttrs sid expression
  mapM (adgchaseM []) ptrs >>= return . nub . catMaybes . map (adnr . snd) . concat

baseRelationsS :: ScopeId -> SQLParseM [Identifier]
baseRelationsS sid = do
  ptrs <- sqcfptrsM sid
  mapM (adgchaseM []) ptrs >>= return . nub . catMaybes . map (adnr . snd) . concat

rebaseAttrsToRoots :: [ADGPtr] -> [ADGPtr] -> SQLParseM [ADGPtr]
rebaseAttrsToRoots roots ptrs = mapM (adgchaseM roots) ptrs >>= return . nub . map fst . concat

rebaseAttrs :: ScopeId -> [ADGPtr] -> SQLParseM [ADGPtr]
rebaseAttrs sid ptrs = sqclkupM sid >>= \fr -> rebaseAttrsToRoots (attrEnvPtrs fr) ptrs

rebaseExprsToRoots :: ScopeId -> [ADGPtr] -> ScalarExprList -> SQLParseM ScalarExprList
rebaseExprsToRoots sid roots exprs = mapM (adgchaseExprM roots sid) exprs

rebaseExprs :: ScopeId -> [ScopeId] -> ScalarExprList -> SQLParseM ScalarExprList
rebaseExprs ssid dsidl exprs = do
  frs <- mapM sqclkupM dsidl
  let ptrs = nub $ concatMap attrEnvPtrs frs
  rebaseExprsToRoots ssid ptrs exprs

rebaseSelectItemsToRoots :: ScopeId -> [ADGPtr] -> SelectItemList -> SQLParseM SelectItemList
rebaseSelectItemsToRoots ssid roots items = mapM rebase items
  where rebase (SelExp ann e) = adgchaseExprM roots ssid e >>= \ne -> return (SelExp ann ne)
        rebase (SelectItem ann e nmc) = adgchaseExprM roots ssid e >>= \ne -> return (SelectItem ann ne nmc)

rebaseSelectItems :: ScopeId -> [ScopeId] -> SelectItemList -> SQLParseM SelectItemList
rebaseSelectItems ssid dsidl items = do
  frs <- mapM sqclkupM dsidl
  let ptrs = nub $ concatMap attrEnvPtrs frs
  rebaseSelectItemsToRoots ssid ptrs items


localizeInputExprs :: ScopeId -> ScopeId -> ScopeId -> ScalarExprList -> SQLParseM (ScalarExprList, ScalarExprList, ScalarExprList)
localizeInputExprs sid lsid rsid exprs = do
    [lfr, rfr] <- mapM sqclkupM [lsid, rsid]
    let (lroots, rroots) = (nub $ attrEnvPtrs lfr, nub $ attrEnvPtrs rfr)
    foldM (localize lroots rroots $ nub $ concat [lroots, rroots]) ([], [], []) exprs

  where
    localize lroots rroots roots (lacc, racc, acc) e = do
      eptrs <- exprAttrs sid e
      reptrs <- rebaseAttrsToRoots roots eptrs
      return $
        if reptrs `intersect` lroots == reptrs then (lacc++[e], racc, acc)
        else if reptrs `intersect` rroots == reptrs then (lacc, racc++[e], acc)
        else (lacc, racc, acc++[e])



{- Optimization -}

-- TODO: query decorrelation
sqloptimize :: [Statement] -> SQLParseM [SQLDecl]
sqloptimize l = mapM stmt l
  where
    stmt (CreateTable _ nm attrs _) = do
      t <- sqltabletype attrs
      sqrextM (sqlnm nm) t
      return $ SQLRel (sqlnm nm, t)

    stmt (QueryStatement _ q) = do
      qcl <- query q
      return $ SQLQuery $ qcplan qcl

    stmt s = throwE $ "Unimplemented SQL stmt: " ++ show s

    -- TODO: distinct, order, limit, offset
    query (Select _ _ selectL tableL whereE gbL havingE _ _ _) = queryPlan selectL tableL whereE gbL havingE
    query q = throwE $ "Unhandled query " ++ show q

    -- TODO: simplify chains with join tree before adding to top-level plan.
    queryPlan selectL tableL whereE gbL havingE = do
      tfvOpt <- joinTree tableL
      case tfvOpt of
        Nothing -> do
          (prjs, aggs, gsid) <- aggregatePath Nothing selectL
          (efvs, subqs) <- varsAndQueries Nothing $ projectionexprs $ prjs ++ aggs
          return $ QueryClosure efvs $ QueryPlan Nothing [PlanCPath gsid [] [] prjs aggs Nothing subqs] Nothing

        Just (t, fvs) -> do
          sid <- treeSchema t
          conjuncts <- maybe (return []) splitConjuncts whereE
          (nt, remconjuncts) <- predicatePushdown (Just sid) conjuncts t
          (prjs, aggs, gsid) <- aggregatePath (Just sid) selectL
          if all null [remconjuncts, gbL, projectionexprs prjs, projectionexprs aggs]
            then return $ QueryClosure fvs $ QueryPlan (Just nt) [] Nothing
            else do
              (gnt, naggs)   <- if null gbL then return (nt, aggs) else groupByPushdown nt sid (map mkprojection gbL) aggs
              (efvs, subqs)  <- debugGBPushdown gnt $ varsAndQueries (Just sid) $ remconjuncts ++ gbL ++ (projectionexprs $ prjs ++ naggs)
              (hfvs, hsubqs) <- maybe (return ([], [])) (\e -> varsAndQueries (Just gsid) [e]) havingE
              let chains = [PlanCPath gsid remconjuncts gbL prjs naggs havingE $ subqs ++ hsubqs]
              return $ QueryClosure (nub $ fvs ++ efvs ++ hfvs) $ QueryPlan (Just gnt) chains Nothing

    debugGBPushdown x y = if False then y else trace (boxToString $ ["GB pushdown result"] %$ prettyLines x) y

    joinTree [] = return Nothing
    joinTree (h:t) = do
      n <- unaryNode h
      (tree, tfvs) <- foldM binaryNode n t
      return $ Just (tree, tfvs)

    binaryNode (lhs, lfvs) n = do
      (rhs, rfvs) <- unaryNode n
      (lsid, rsid) <- (,) <$> treeSchema lhs <*> treeSchema rhs
      jpsid <- mergeScopes lsid rsid
      josid <- aliasedOutputScope jpsid "__CP" Nothing
      return (Node (PJoin jpsid josid Nothing [] [] [] []) [lhs, rhs], nub $ lfvs ++ rfvs)

    unaryNode n@(Tref _ nm al) = do
      let tid = sqltablealias ("__" ++ sqlnm nm) al
      t    <- sqrlkupM $ sqlnm nm
      rt   <- taliascolM al t
      tsid <- sqgextSchemaM tid rt
      return (Node (PTable (sqlnm nm) tsid (Just n) Nothing []) [], [])

    unaryNode (SubTref _ q al) = do
      qcl    <- query q
      nqsid  <- planSchema $ qcplan qcl
      qalsid <- outputScope nqsid "__RN" (Just al)
      return (Node (PSubquery qalsid qcl) [], qcfree qcl)

    unaryNode (JoinTref _ jlt nat jointy jrt onE jal) = do
      (lhs, lfvs)          <- unaryNode jlt
      (rhs, rfvs)          <- unaryNode jrt
      (lsid, rsid)         <- (,) <$> treeSchema lhs <*> treeSchema rhs
      jpsid                <- mergeScopes lsid rsid
      (jeq, jp, pfvs, psq) <- joinPredicate jpsid lsid rsid onE
      josid                <- aliasedOutputScope jpsid "__JR" (Just jal)
      return (Node (PJoin jpsid josid (Just (nat,jointy)) jeq jp psq []) [lhs, rhs], nub $ lfvs ++ rfvs ++ pfvs)

    unaryNode (FunTref _ _ _) = throwE "Table-valued functions are not supported"

    joinPredicate :: ScopeId -> ScopeId -> ScopeId -> OnExpr -> SQLParseM ([(ScalarExpr, ScalarExpr)], ScalarExprList, [AttrPath], SubqueryBindings)
    joinPredicate sid lsid rsid (Just (JoinOn _ joinE)) = do
      conjuncts <- splitConjuncts joinE
      (sepcons, nsepcons) <- classifyConjuncts sid lsid rsid conjuncts >>= return . partitionEithers
      let (lseps, rseps) = unzip sepcons
      (lcvs, lsubqs) <- varsAndQueries (Just sid) lseps
      (rcvs, rsubqs) <- varsAndQueries (Just sid) rseps
      (cvs, subqs) <- varsAndQueries (Just sid) nsepcons
      return (sepcons, nsepcons, nub $ lcvs ++ rcvs ++ cvs, nub $ lsubqs ++ rsubqs ++ subqs)

    joinPredicate sid _ _ (Just (JoinUsing _ nmcs)) = do
      (_, _, [[lqual], [rqual]]) <- sqclkupM sid
      let eqs = map (\i -> (QIdentifier emptyAnnotation [Nmc lqual, i], QIdentifier emptyAnnotation [Nmc rqual, i])) nmcs
      return (eqs, [], [], [])

    joinPredicate _ _ _ _ = return ([], [], [], [])

    splitConjuncts :: ScalarExpr -> SQLParseM ScalarExprList
    splitConjuncts e@(FunCall _ nm args) = do
      let fn = sqlnm nm
      case (fn, args) of
        ("!and", [x,y]) -> (++) <$> splitConjuncts x <*> splitConjuncts y
        _ -> return [e]

    splitConjuncts e = return [e]

    classifyConjuncts :: ScopeId -> ScopeId -> ScopeId -> ScalarExprList -> SQLParseM [Either (ScalarExpr, ScalarExpr) ScalarExpr]
    classifyConjuncts sid lsid rsid es = do
      (lrels, rrels) <- (,) <$> baseRelationsS lsid <*> baseRelationsS rsid
      mapM (classifyConjunct sid lsid rsid lrels rrels) es

    classifyConjunct :: ScopeId -> ScopeId -> ScopeId -> [Identifier] -> [Identifier] -> ScalarExpr
                     -> SQLParseM (Either (ScalarExpr, ScalarExpr) ScalarExpr)
    classifyConjunct sid lsid rsid lrels rrels e@(FunCall _ nm args) = do
      let fn = sqlnm nm
      case sqloperator fn args of
        (Just (BinaryOp OEqu x y)) -> do
          (xrels, yrels) <- (,) <$> baseRelationsE sid x <*> baseRelationsE sid y
          classify x y xrels yrels
        _ -> return $ Right e

      where
        classify x y (nub -> xrels) (nub -> yrels)
          | xrels `intersect` lrels == xrels && yrels `intersect` rrels == yrels = do
              [nx] <- rebaseExprs sid [lsid] [x]
              [ny] <- rebaseExprs sid [rsid] [y]
              return $ Left (x,y)

          | xrels `intersect` rrels == xrels && yrels `intersect` lrels == yrels = do
              [ny] <- rebaseExprs sid [lsid] [y]
              [nx] <- rebaseExprs sid [rsid] [x]
              return $ Left (y,x)

          | otherwise = return $ Right e

    classifyConjunct _ _ _ _ _ e = return $ Right e

    aggregatePath :: Maybe ScopeId -> SelectList -> SQLParseM (SelectItemList, SelectItemList, ScopeId)
    aggregatePath sidOpt (SelectList _ selectL) = do
      (prjs, aggs) <- mapM classifySelectItem selectL >>= return . partitionEithers
      asid <- aggregateSchema sidOpt prjs aggs
      return (prjs, aggs, asid)

    classifySelectItem :: SelectItem -> SQLParseM (Either SelectItem SelectItem)
    classifySelectItem si@(SelExp _ e) = isAggregate e >>= \agg -> return $ if agg then Right si else Left si
    classifySelectItem si@(SelectItem _ e _) = isAggregate e >>= \agg -> return $ if agg then Right si else Left si

    -- TODO: AggregateFn, Interval, LiftOperator, WindowFn
    varsAndQueries :: Maybe ScopeId -> ScalarExprList -> SQLParseM ([AttrPath], SubqueryBindings)
    varsAndQueries sidOpt exprs = processMany exprs
      where process (Identifier _ (sqlnmcomponent -> i)) = trypath sidOpt (return ([[i]], [])) (const $ return ([], [])) [i]
            process (QIdentifier _ (sqlnmpath -> path)) = trypath sidOpt (return ([path], [])) (const $ return ([], [])) path

            process (Case _ whens elseexpr) = caseList (maybe [] (:[]) elseexpr) whens
            process (CaseSimple _ expr whens elseexpr) = caseList ([expr] ++ maybe [] (:[]) elseexpr) whens

            process (FunCall _ _ args) = processMany args

            process e@(Exists _ q) = bindSubquery e q
            process e@(ScalarSubQuery _ q) = bindSubquery e q

            process (InPredicate _ ine _ (InList _ el)) = processMany $ ine : el

            process e@(InPredicate _ ine _ (InQueryExpr _ q)) = do
              (invs, inbs) <- process ine
              (qvs, qbs) <- bindSubquery e q
              return (invs ++ qvs, inbs ++ qbs)

            process (Cast _ e _) = process e
            process (Extract _ _ e) = process e

            process _ = return ([], [])

            concatR (a,b) (c,d) = (nub $ a++c, b++d)
            concatMany l = return $ (nub . concat) *** concat $ unzip l

            processMany el = mapM process el >>= concatMany

            bindSubquery e q = do
              sym <- ssqsextM
              qcl <- query q
              vbl <- mapM (\path -> trypath sidOpt (return ([path], [])) (const $ return ([], [])) path) $ qcfree qcl
              return (concat $ map fst vbl, [(e, ("__subquery" ++ show sym, qcl))])

            caseList extra whens = do
              rl <- (\a b -> a ++ [b]) <$> mapM (\(l,e) -> processMany $ l++[e]) whens <*> processMany extra
              concatMany rl


    predicatePushdown :: Maybe ScopeId -> ScalarExprList -> PlanTree -> SQLParseM (PlanTree, ScalarExprList)
    predicatePushdown Nothing preds jtree = return (jtree, preds)
    predicatePushdown (Just sid) preds jtree = foldM push (jtree, []) preds
      where
        push (t, remdr) p = do
          rels <- baseRelationsE sid p
          (nt, accs) <- onRelLCA t rels $ inject p
          return (nt, if any id accs then remdr ++ [p] else remdr)

        inject p _ n@(ttag -> PTable tid tsid trOpt bmOpt chains) = do
          [np] <- rebaseExprs sid [tsid] [p]
          (_, npqbs) <- varsAndQueries (Just tsid) [np]
          return (replaceData n $ PTable tid tsid trOpt bmOpt $ pcextSelect sid npqbs np chains, False)

        inject p [lrels, rrels] n@(Node (PJoin psid osid jt jeq jp jpb chains) [l,r]) = do
          (lsid, rsid) <- (,) <$> treeSchema l <*> treeSchema r
          [np] <- rebaseExprs sid [psid] [p]
          sepE <- classifyConjunct sid lsid rsid lrels rrels np
          (njeq, njp, nsubqbs) <- case sepE of
                                    Left (lsep,rsep) -> do
                                      (_, lqbs) <- varsAndQueries (Just lsid) [lsep]
                                      (_, rqbs) <- varsAndQueries (Just rsid) [rsep]
                                      return (jeq ++ [(lsep,rsep)], jp, lqbs ++ rqbs)

                                    Right nonsep -> do
                                      (_, rqbs) <- varsAndQueries (Just psid) [nonsep]
                                      return (jeq, jp ++ [nonsep], rqbs)

          return (replaceData n $ PJoin psid osid jt njeq njp (jpb ++ nsubqbs) chains, False)

        inject _ _ n = return (n, True)


    onRelLCA :: PlanTree -> [Identifier] -> ([[Identifier]] -> PlanTree -> SQLParseM (PlanTree, a)) -> SQLParseM (PlanTree, [a])
    onRelLCA t rels f = do
        (_,_,x,y) <- foldMapTree go (False, [], [], []) t
        return (head x, y)

      where
        go (conc -> (True, _, nch, acc)) n = return (True, [], [replaceCh n nch], acc)

        go (conc -> (False, relsByCh@(concat -> chrels), nch, acc)) n@(ttag -> PTable (("__" ++) -> i) _ _ _ _)
          | rels `intersect` (chrels ++ [i]) == rels = f relsByCh (replaceCh n nch) >>= \(n',r) -> return (True, [], [n'], acc++[r])
          | otherwise = return (False, chrels++[i], [replaceCh n nch], acc)

        go (conc -> (False, relsByCh@(concat -> chrels), nch, acc)) n
          | rels `intersect` chrels == rels = f relsByCh (replaceCh n nch) >>= \(n', r) -> return (True, [], [n'], acc++[r])
          | otherwise = return (False, chrels, [replaceCh n nch], acc)

        go _ _ = throwE "onRelLCA pattern mismatch"

        conc cl = (\(a,b,c,d) -> (any id a, b, concat c, concat d)) $ unzip4 cl


    groupByPushdown :: PlanTree -> ScopeId -> SelectItemList -> SelectItemList -> SQLParseM (PlanTree, SelectItemList)
    groupByPushdown jtree s g a = walk s g a jtree
      where
        walk sid gbs aggs e@(Node n ch) = do
          let onRoot = n == ttag jtree
          continue <- trace (boxToString $ ["GB walk"] %$ prettyLines e) $ trypush sid gbs aggs ch n
          case continue of
            Left doExtend -> complete onRoot doExtend aggs sid gbs aggs $ Node n ch
            Right (doExtend, naggs, chsga) -> do
              nch <- mapM (\((cs,cg,ca), c) -> walk cs cg ca c >>= return . fst) $ zip chsga ch
              complete onRoot doExtend naggs sid gbs naggs $ Node n nch

        trypush sid gbs aggs [lt,rt] n@(PJoin psid osid _ jeq jp _ chains)
          | not $ null jeq = do
            jptrs <- joinAttrs psid osid jeq jp chains
            aptrs <- aggAttrs psid sid aggs
            let caggs = chainAggregates osid chains
            let overlaps = jptrs `intersect` aptrs
            case (caggs, overlaps) of
              ([],[]) -> do
                (lsid, rsid) <- (,) <$> treeSchema lt <*> treeSchema rt
                (lgbs, rgbs, remgbs) <- localizeJoin lsid rsid psid sid gbs jptrs
                (laggs, raggs, remaggs, naggs) <- decomposeAggregates lsid rsid psid sid aggs
                if debugAggDecomp psid osid lgbs rgbs remgbs laggs raggs remaggs naggs $ not (null remgbs && null remaggs)
                  then return $ Left True
                  else return $ Right (True, naggs, [(lsid, lgbs, laggs), (rsid, rgbs, raggs)])

              (_,_) -> return $ Left $ null caggs

          | otherwise = return $ Left $ null $ chainAggregates osid chains

        trypush _ _ _ _ (PJoin _ _ _ _ _ _ _) = throwE "Invalid binary join node"
        trypush _ _ _ ch n@(PTable _ tsid _ _ chains) = return $ Left $ null $ chainAggregates tsid chains
        trypush _ _ _ ch n@(PSubquery _ _) = return $ Left True

        debugAggDecomp psid osid lgbs rgbs remgbs laggs raggs naggs remaggs m =
          trace (unwords ["Agg decomp", show psid, show osid
                         , "GBs:", show $ length lgbs, show $ length rgbs, show $ length remgbs
                         , "Aggs:", show $ length laggs, show $ length raggs, show $ length naggs, show $ length remaggs]) m

        complete onRoot doExtend raggs sid gbs aggs (Node n ch) = do
          n' <- refreshInputSchema n ch
          n'' <- if not onRoot && doExtend then pcextGroupBy gbs aggs n' else return n'
          trace (boxToString $ ["Completed"] %$ prettyLines (Node n'' ch)) $
            return (Node n'' ch, if onRoot then raggs else [])

        chainAggregates sid chains = concatMap snd $ pcAggExprs sid chains

        joinAttrs dsid sid1 eqexprs neqexprs chains = do
          eqptrs  <- mapM (\(x,y) -> (++) <$> exprAttrs dsid x <*> exprAttrs dsid y) eqexprs >>= return . concat
          neqptrs <- mapM (exprAttrs dsid) neqexprs >>= return . concat
          let sidAndExprs = pcNonAggExprs sid1 chains
          exprptrs <- concatMapM (\(ssid,el) -> concatMapM (exprAttrs ssid) el) sidAndExprs
          rexprptrs <- rebaseAttrs dsid exprptrs
          return $ nub $ eqptrs ++ neqptrs ++ rexprptrs

        aggAttrs dsid ssid aggs = do
          aggptrs <- concatMapM (exprAttrs ssid) $ projectionexprs aggs
          rebaseAttrs dsid $ nub aggptrs

        localizeJoin lsid rsid psid sid gbs jptrs = do
          gbptrs <- concatMapM (exprAttrs sid) $ projectionexprs gbs
          rgbptrs <- rebaseAttrs psid gbptrs
          localizeAttrs lsid rsid psid $ rgbptrs ++ jptrs

        localizeAttrs lsid rsid psid ptrs = do
          nodes <- mapM sqglkupM $ nub $ ptrs
          (lexprs, rexprs, rest) <- localizeInputExprs psid lsid rsid $ map (\n -> Identifier emptyAnnotation $ Nmc $ adnn n) nodes
          return (map mkprojection lexprs, map mkprojection rexprs, rest)

        decomposeAggregates lsid rsid psid sid aggs = do
          (aggexprs, aggFns) <- aggregateexprs aggs
          raggptrs <- mapM (\e -> exprAttrs sid e >>= rebaseAttrs psid) aggexprs
          ragglocals <- mapM (localizeAttrs lsid rsid psid) raggptrs
          foldM (decomposeAggByFn lsid rsid sid) ([], [], [], []) $ zip3 ragglocals (zip aggs aggexprs) aggFns

        decomposeAggByFn lsid rsid sid (lacc, racc, acc, nacc) ((ldeps, rdeps, deps), (agg,e), fn) =
          if null deps then do
            (eagg, cagg, nagg) <- rewriteAgg sid (if null rdeps then lsid else rsid) fn agg e
            let (nlacc, nracc) = if null rdeps then (lacc ++ [eagg], racc ++ [cagg]) else (lacc ++ [cagg], racc ++ [eagg])
            return (nlacc, nracc, acc, nacc ++ [nagg])
          else return (lacc, racc, acc ++ [e], nacc)

        rewriteAgg ssid dsid aggFn agg e = do
          ne <- rebaseExprs ssid [dsid] [e] >>= return . head
          (ei, ci) <- (\a b -> ("__AGGD" ++ show a, "__AGGD" ++ show b)) <$> saggsextM <*> saggsextM
          (,,) <$> mkaggregate aggFn    (SelectItem emptyAnnotation e $ Nmc ei) ne
               <*> mkaggregate AggCount (SelectItem emptyAnnotation e $ Nmc ci) (Star emptyAnnotation)
               <*> mkaggregate aggFn    agg                                     (FunCall emptyAnnotation (Name emptyAnnotation [Nmc "*"])
                                                                                  [Identifier emptyAnnotation $ Nmc ei
                                                                                  ,Identifier emptyAnnotation $ Nmc ci])

        concatMapM f x = mapM f x >>= return . concat


sqlstage :: [SQLDecl] -> SQLParseM ([SQLDecl], StageGraph)
sqlstage stmts = mapM stage stmts >>= return . (concat *** concat) . unzip
  where
    stage s@(SQLRel _) = return ([s], [])
    stage s@(SQLStage _) = throwE "SQLStage called with existing stage declarations"
    stage (SQLQuery plan) = stagePlan plan >>= \(_,l,g) -> return (l,g)

    stagePlan (QueryPlan tOpt chains stgOpt) = do
      (ntOpt, (tstages, tstgg)) <- maybe (return (Nothing, ([], []))) (\t -> stageTree t >>= return . first Just) tOpt
      (nplan, cstages, nstgg) <- stagePlanChains tstgg ntOpt stgOpt chains
      return (nplan, tstages ++ cstages, nstgg)

    stageTree jtree = (\((a,b),c) -> (c,(a,b))) <$> foldMapRebuildTree stageNode ([],[]) jtree

    stageNode (aconcat -> (acc, [lstgg,rstgg])) ch n@(ttag -> PJoin psid osid jt jeq jp jpb chains) = do
      stgid <- stgsextM >>= return . stageId . show
      let (jchains, schains) = nonAggregatePrefix chains
      let jtOpt = Just $ Node (PJoin psid osid jt jeq jp jpb jchains) ch
      let jstage = SQLQuery $ QueryPlan jtOpt [] (Just stgid)
      let nt = Node (PTable stgid osid Nothing Nothing []) []
      let jstgg = stgEdges [lstgg, rstgg] stgid
      (st, nstages, nstgg) <- stageNodeChains jstgg nt schains
      let nstgg' = lstgg ++ rstgg ++ nstgg
      kt <- k3ScopeType osid bmelem
      return ((acc ++ [SQLStage (stgid, kt), jstage] ++ nstages, nstgg'), st)

    stageNode (aconcat -> (acc, stgg)) ch n@(ttag -> PTable i tsid trOpt bmOpt chains) = do
      let (tchains, schains) = nonAggregatePrefix chains
      let nt = Node (PTable i tsid trOpt bmOpt tchains) ch
      (st, nstages, nstgg) <- stageNodeChains [Left i] nt schains
      return ((acc ++ nstages, concat stgg ++ nstgg), st)

    stageNode (aconcat -> (acc, stgg)) ch n@(ttag -> PSubquery osid (QueryClosure fvs plan)) = do
      (nplan, nstages, nstgg) <- stagePlan plan
      return ((acc ++ nstages, concat stgg ++ nstgg), Node (PSubquery osid $ QueryClosure fvs nplan) ch)

    stageNode _ _ n = throwE $ boxToString $ ["Invalid tree node for staging"] %$ prettyLines n

    stagePlanChains stgg Nothing stgOpt chains = return (QueryPlan Nothing chains stgOpt, [], stgg)
    stagePlanChains stgg (Just t) stgOpt chains = do
      let (pchains, schains) = nonAggregatePrefix chains
      nt <- foldM (flip pcext) t pchains
      (st, nstages, nstgg) <- stageNodeChains stgg nt schains
      return (QueryPlan (Just st) [] stgOpt, nstages, nstgg)

    stageNodeChains stgg t chains = foldM onPath (t,[],stgg) chains
      where onPath (t, acc, stggacc) p = do
              pt <- pcext p t
              if isNonAggregatePath p
                then return (pt, acc, stggacc)
                else do
                  stgid <- stgsextM >>= return . stageId . show
                  let pstage = SQLQuery $ QueryPlan (Just pt) [] (Just stgid)
                  osid <- treeSchema pt
                  bm <- treeBindingMap pt
                  kt <- k3ScopeType osid bm
                  rkt <- if not $ isAggregatePath p
                          then return kt
                          else do
                            et <- telemM kt
                            case tnc et of
                              (TRecord ids, ch) -> zeroT $ zip ids ch
                              _ -> throwE "Invalid k3 aggregate plan type"

                  let nt = Node (PTable stgid osid Nothing (Just bm) []) []
                  let nstgg = stggacc ++ stgEdges [stggacc] stgid
                  return (nt, acc ++ [SQLStage (stgid, rkt), pstage], nstgg)

    stgEdges ll i = map (Right . (,i)) $ catMaybes $ stgCh ll
    stgCh ll = map (\l -> if null l then Nothing else Just $ either id snd $ last l) ll

    nonAggregatePrefix chains = fst $ foldl accum (([],[]), False) chains
      where accum ((nagg,agg), found) p | not found && isNonAggregatePath p = ((nagg ++ [p], agg), False)
                                        | otherwise = ((nagg, agg++[p]), True)

    aconcat = (concat *** id) . unzip


sqlcodegen :: Bool -> ([SQLDecl], StageGraph) -> SQLParseM (K3 Declaration)
sqlcodegen distributed (stmts, stgg) = do
    (decls, inits) <- foldM cgstmt ([], []) stmts
    initDecl <- mkInit decls
    return $ DC.role "__global" $ [master] ++ decls ++ mkPeerInit inits ++ initDecl

  where
    trig i = i ++ "_trigger"

    (leaves, edges) = partitionEithers stgg
    stagechildren = foldl (\acc (s,t) -> Map.insertWith (++) t [s] acc) Map.empty edges
    stageinits = Map.foldlWithKey (\acc p ch -> if ch `intersect` leaves == ch then acc ++ [p] else acc) [] stagechildren

    cgstmt (dacc, iacc) (SQLRel (i, t)) = do
      gt <- twrapcolelemM t
      (ldecls, linit) <- mkLoader (i,t)
      return (dacc ++ [DC.global i gt Nothing] ++ ldecls, iacc ++ [linit])

    cgstmt (dacc, iacc) (SQLStage (i, t)) = return (dacc ++ [DC.global i (mutT t) Nothing], iacc)

    cgstmt (dacc, iacc) (SQLQuery plan) = do
      (e,sid,bm,merge) <- cgplan plan
      t <- k3PlanType bm plan
      (outid, trigid, decls) <- case qstageid plan of
                                  Just i -> return (i, trig i, [])
                                  Nothing -> do
                                    s  <- stgsextM >>= return . show
                                    let i = materializeId s
                                    return (i, stageId s, [DC.global i (mutT t) Nothing])

      let execStageF i e = case lookup i edges of
                             Nothing -> return e
                             Just next ->
                               let nextE = EC.send (EC.variable $ trig next) (EC.variable "me") EC.unit
                                   execE = EC.block [e, nextE]
                               in annotateTriggerBody i plan merge execE

      trigBodyE <- execStageF outid $ EC.assign outid e
      return (dacc ++ decls ++ [ DC.trigger trigid TC.unit $ EC.lambda "_" trigBodyE ], iacc)

    cgclosure (QueryClosure free plan)
      | null free = cgplan plan
      | otherwise = throwE "Code generation not supported for correlated queries"

    cgplan (QueryPlan tOpt chains _) = do
      esbmOpt <- maybe (return Nothing) (\t -> cgtree t >>= return . Just) tOpt
      cgchains esbmOpt chains

    cgtree n@(Node (PJoin psid osid jtOpt jeq jp jsubqbs chains) [l,r]) = do
      sf@(_,_,[[lqual],[rqual]]) <- sqclkupM psid
      cqaenv <- partitionCommonQualifedAttrEnv sf
      (lexpr, lsid, lbm, _) <- cgtree l
      (rexpr, rsid, rbm, _) <- cgtree r
      (li, ri) <- (,) <$> uniqueScopeQualifier lsid <*> uniqueScopeQualifier rsid
      jbm <- bindingMap [(lqual, Map.lookup [lqual] cqaenv, lbm), (rqual, Map.lookup [rqual] cqaenv, rbm)]
      case (jeq, jp) of
        ((_:_), []) -> do
          let (lexprs, rexprs) = unzip jeq
          lkbodyE  <- mapM (cgexpr jsubqbs Nothing lsid) lexprs >>= \es -> bindE lsid lbm (Just li) $ tupE es
          rkbodyE  <- mapM (cgexpr jsubqbs Nothing rsid) rexprs >>= \es -> bindE rsid rbm (Just ri) $ tupE es
          obodyE <- concatE psid jbm Nothing
          let lkeyE  = EC.lambda lqual lkbodyE
          let rkeyE  = EC.lambda rqual rkbodyE
          let outE   = EC.lambda lqual $ EC.lambda rqual obodyE
          joinKV <- isKVBindingMap rbm
          let joinE  = EC.applyMany (EC.project (if joinKV then "equijoin_kv" else "equijoin") lexpr) [rexpr, lkeyE, rkeyE, outE]
          cgchains (Just (joinE, osid, bmelem, Nothing)) chains

        (_, _) -> do
          mbodyE <- case jp of
                      [] -> bindE psid jbm Nothing $ EC.constant $ CBool True
                      (h:t) -> cgexpr jsubqbs Nothing psid h >>= \he -> foldM (cgconjunct jsubqbs psid) he t >>= \e -> bindE psid jbm Nothing e
          obodyE <- concatE psid jbm Nothing
          let matchE = EC.lambda lqual $ EC.lambda rqual mbodyE
          let outE   = EC.lambda lqual $ EC.lambda rqual obodyE
          joinKV <- isKVBindingMap rbm
          let joinE  = EC.applyMany (EC.project (if joinKV then "join_kv" else "join") lexpr) [rexpr, matchE, outE]
          cgchains (Just (joinE, osid, bmelem, Nothing)) chains

    cgtree (Node (PSubquery _ qcl) ch) = cgclosure qcl
    cgtree n@(Node (PTable i tsid _ bmOpt chains) []) = cgchains (Just (EC.variable i, tsid, maybe bmelem id bmOpt, Nothing)) chains
    cgtree _ = throwE "Invalid plan tree"

    cgchains esbmOpt chains = do
      resbmOpt <- foldM cgchain esbmOpt chains
      maybe (throwE "Invalid chain result") return resbmOpt

    cgchain (Just (e,sid,bm,_)) (PlanCPath osid selects gbs prjs aggs having subqbs) = do
      fe <- case selects of
              [] -> return e
              l -> foldM (filterChainE sid bm subqbs) e l
      case (gbs, prjs, aggs, having) of
        ([], [], [], Nothing) -> return $ Just (fe, osid, bm, Nothing)
        ([], _, _, Nothing) -> cgselectlist sid osid bm subqbs fe prjs aggs
        (h:t, _, _, _) -> cggroupby sid osid bm subqbs fe gbs prjs aggs having
        _ -> throwE $ "Invalid group-by and having expression pair"

    cgchain Nothing (PlanCPath osid [] [] prjs [] Nothing []) = cglitselectlist [] osid prjs
    cgchain Nothing _ = throwE "Invalid scalar chain component"

    cggroupby sid osid bm subqbs e gbs prjs aggs having = do
      i <- uniqueScopeQualifier sid
      o <- uniqueScopeQualifier osid

      gbie <- (\f -> foldM f (0,[]) gbs >>= return . snd) $ \(i,acc) gbe -> do
                gbke <- cgexpr subqbs Nothing sid gbe
                case gbe of
                  (Identifier _ (Nmc n)) -> return (i, acc++[(n,gbke)])
                  _ -> return (i+1, acc++[("f" ++ show i, gbke)])

      gbbodyE <- bindE sid bm (Just i) $ case gbie of
                   [] -> EC.unit
                   [(_,e)] -> e
                   _ -> recE gbie
      let groupF = EC.lambda i gbbodyE

      (prjsymidx, prjie) <- cgprojections subqbs 0 sid prjs
      prjt <- mapM (scalarexprType $ Just sid) $ projectionexprs prjs
      unless (all (\((_,a), (_,b)) -> compareEAST a b) $ zip prjie gbie) $ throwE "Mismatched groupbys and projections"

      (_, aggie, mergeie) <- cgaggregates subqbs prjsymidx sid aggs
      aggbodyE <- bindE sid bm (Just i) $ case aggie of
                      [] -> EC.variable "acc"
                      [(_,e)] -> EC.applyMany e [EC.variable "acc"]
                      _ -> recE $ map (aggE "acc") aggie
      let aggF = EC.lambda "acc" $ EC.lambda i $ aggbodyE

      mergeF <- case mergeie of
                  [] -> return $ EC.lambda "_" $ EC.lambda "_" $ EC.unit
                  [(_,e)] -> return $ e
                  _ -> return $ EC.lambda "acc1" $ EC.lambda "acc2"
                              $ recE $ map (aggMergeE "acc1" "acc2") mergeie

      aggt <- mapM (aggregateType $ Just sid) $ projectionexprs aggs
      let aggit = zip (map fst aggie) aggt
      zE <- zeroE aggit

      let rE = EC.applyMany (EC.project "group_by" e) [groupF, aggF, zE]

      let prefixTypePath i pfx l = case l of
                                     []  -> (i+1, [("f" ++ show i, [pfx])])
                                     [j] -> (i, [(j, [pfx])])
                                     _   -> (i, map (\j -> (j, [pfx, j])) l)

      let (nidx, keyPaths) = prefixTypePath (0::Int) "key" $ map fst prjie
      let (_, valPaths) = prefixTypePath nidx "value" $ map fst aggie
      let rbm = BMTFieldMap $ Map.fromList $ keyPaths ++ valPaths

      hve <- maybe (return Nothing) (havingE aggie osid rbm o) having
      let hrE = maybe rE (\h -> EC.applyMany (EC.project "filter" rE) [EC.lambda o h]) hve

      return $ Just (hrE, osid, rbm, Just mergeF)

      where havingE aggie osid rbm o e = do
              let aggei = map (\(a,b) -> (b,a)) aggie
              he <- cgexpr subqbs (Just $ subAgg aggei) osid e
              hbodyE <- bindE osid rbm (Just o) $ he
              return $ Just hbodyE

            subAgg aggei e = do
              case lookup e aggei of
                Nothing -> return e
                Just i -> return $ EC.variable i

    cgselectlist sid osid bm subqbs e prjs aggs = case (prjs, aggs) of
      (p, []) -> do
        i <- uniqueScopeQualifier sid
        mbodyE <- cgprojections subqbs 0 sid prjs >>= \(_, fields) -> bindE sid bm (Just i) $ recE fields
        let prjE = EC.applyMany (EC.project "map" e) [EC.lambda i mbodyE]
        return $ Just (prjE, osid, bmelem, Nothing)

      ([], a) -> do
        i <- uniqueScopeQualifier sid
        (_, aggfields, mergeie) <- cgaggregates subqbs 0 sid aggs
        aggbodyE <- bindE sid bm (Just i) $ case aggfields of
                      [] -> EC.variable "acc"
                      [(_,e)] -> EC.applyMany e [EC.variable "acc"]
                      _ -> recE $ map (aggE "acc") aggfields
        let aggF = EC.lambda "acc" $ EC.lambda i $ aggbodyE

        mergeF <- case mergeie of
                    [] -> return $ EC.lambda "_" $ EC.lambda "_" $ EC.unit
                    [(_,e)] -> return $ e
                    _ -> return $ EC.lambda "acc1" $ EC.lambda "acc2"
                                $ recE $ map (aggMergeE "acc1" "acc2") mergeie

        rElemT <- scopeType osid >>= telemM
        zE <- case tnc rElemT of
                (TRecord ids, ch) -> zeroE $ zip ids ch
                _ -> throwE "Invalid aggregate result type"

        let rexpr = EC.applyMany (EC.project "fold" e) [aggF, zE]
        return $ Just (rexpr, osid, BMNone, Just mergeF)

      _ -> throwE $ "Invalid mutually exclusive projection-aggregate combination"

    -- TODO: we should not pass down sid here, or state assumption that sid is not used.
    cglitselectlist subqbs sid prjs = cgprojections subqbs 0 sid prjs >>= \(_, ide) -> return $ Just (recE ide, sid, BMNone, Nothing)

    cgprojections subqbs i sid l = foldM (cgprojection subqbs sid) (i, []) l
    cgprojection subqbs sid (i, acc) si = do
      (ni, n) <- selectItemId i si
      cgaccprojection subqbs sid acc ni n $ projectionexpr si

    cgaccprojection subqbs sid acc i n e = cgexpr subqbs Nothing sid e >>= \rE -> return (i, acc ++ [(n, rE)])

    cgaggregates subqbs i sid l = foldM (cgaggregate subqbs sid) (i, [], []) l
    cgaggregate subqbs sid (i, eacc, mrgacc) si = do
      (ni, n) <- selectItemId i si
      cgaccaggregate subqbs sid eacc mrgacc ni n si

    cgaccaggregate subqbs sid eacc mrgacc i n si =
      cgaggexpr subqbs sid si >>= \(rE, mergeE) -> return (i, eacc ++ [(n, rE)], mrgacc ++ [(n, mergeE)])

    cgconjunct subqbs sid eacc e = cgexpr subqbs Nothing sid e >>= \e' -> return $ EC.binop OAnd eacc e'

    cgaggexpr subqbs sid si = do
      (e, aggFn) <- aggregateexpr si
      aE <- cgexpr subqbs Nothing sid e
      return $ case aggFn of
        AggSum   -> (binagg OAdd aE, mergeagg OAdd)
        AggCount -> (binagg OAdd $ EC.constant $ CInt 1, mergeagg OAdd)
        AggMin   -> (binapp "min" aE, mergeapp "min")
        AggMax   -> (binapp "max" aE, mergeapp "max")

      where binagg op e = EC.lambda "aggacc" $ EC.binop op (EC.variable "aggacc") e
            binapp f e  = EC.lambda "aggacc" $ EC.applyMany (EC.variable f) [EC.variable "aggacc", e]
            mergeagg op = EC.lambda "a" $ EC.lambda "b" $ EC.binop op (EC.variable "a") $ EC.variable "b"
            mergeapp f  = EC.lambda "a" $ EC.lambda "b" $ EC.applyMany (EC.variable f) [EC.variable "a", EC.variable "b"]

    -- TODO: case, correlated subqueries, more type constructors
    -- TODO: Cast, Interval, LiftOperator, NullLit, Placeholder, PositionalArg, WindowFn
    cgexpr _ _ _ (BooleanLit _ b) = return $ EC.constant $ CBool b
    cgexpr _ _ _ (NumberLit _ i) = return $ if "." `isInfixOf` i
                                     then EC.constant $ CReal $ read i
                                     else EC.constant $ CInt $ read i

    cgexpr _ _ _ (StringLit _ s) = return $ EC.constant $ CString s

    cgexpr _ _ _ (TypedStringLit _ tn s) = do
        t <- sqlnamedtype tn
        case (tag t, find isTProperty $ annotations t) of
          (TInt, Just (TProperty (tPropertyName -> "TPCHDate"))) -> return $ EC.constant $ CInt $ read $ filter (/= '-') s
          (_, _) -> throwE $ boxToString $ ["Unsupported constructor for"] %$ prettyLines t

    cgexpr _ _ _ (Identifier _ (sqlnmcomponent -> i)) = return $ EC.variable i

    cgexpr _ _ sid (QIdentifier _ nmcl) = do
      ptr <- sqcflkupM sid $ sqlnmpath nmcl
      EC.variable . adnn <$> sqglkupM ptr

    cgexpr subqbs f sid (Case _ whens elseexpr) = cgcase subqbs f sid elseexpr whens
    cgexpr subqbs f sid (CaseSimple _ expr whens elseexpr) = cgcasesimple subqbs f sid expr elseexpr whens

    cgexpr subqbs f sid e@(FunCall _ nm args) = do
      isAgg <- isAggregate e
      if isAgg then do
        (agge,_) <- cgaggexpr subqbs sid (SelExp emptyAnnotation e)
        maybe err ($ agge) f

      else do
        let fn = sqlnm nm
        case sqloperator fn args of
          (Just (UnaryOp  o x)) -> EC.unop o <$> cgexpr subqbs f sid x
          (Just (BinaryOp o x y)) -> EC.binop o <$> cgexpr subqbs f sid x <*> cgexpr subqbs f sid y
          _ -> do
            case (fn, args) of
              ("!between", [x,y,z]) ->
                let cg a b c = EC.binop OAnd (EC.binop OLeq b a) $ EC.binop OLeq a c
                in cg <$> cgexpr subqbs f sid x <*> cgexpr subqbs f sid y <*> cgexpr subqbs f sid z

              -- TODO
              ("!like", [_,_])      -> throwE $ "LIKE operator not yet implemented"
              ("!notlike", [_,_])   -> throwE $ "NOTLIKE operator not yet implemented"

              (_, _) -> EC.applyMany (EC.variable fn) <$> mapM (cgexpr subqbs f sid) args

      where err = throwE "Invalid aggregate expression in cgexpr"

    cgexpr _ _ sid (Star _) = do
      (_, ord, _) <- sqclkupM sid >>= unqualifiedScopeFrame
      recE <$> mapM (\p -> singletonPath p >>= \j -> return (j, EC.variable j)) ord

    cgexpr _ _ sid (QStar _ (sqlnmcomponent -> i)) = do
      qattrs <- sqclkupM sid >>= qualifiedAttrs [i] >>= attrIds
      recE <$> mapM (\j -> return (j, EC.variable j)) qattrs

    cgexpr subqbs _ _ e@(Exists _ _) = do
      (subexpr, _, _) <- cgsubquery subqbs e
      emptyE False subexpr

    cgexpr subqbs _ _ e@(ScalarSubQuery _ _) = cgsubquery subqbs e >>= \(r,_,_) -> return r

    cgexpr subqbs f sid (InPredicate _ ine isIn (InList _ el)) = do
      testexpr <- cgexpr subqbs f sid ine
      valexprs <- mapM (cgexpr subqbs f sid) el

      case valexprs of
        [] -> return $ EC.constant $ CBool $ not isIn
        (h:t) -> memoE (immutE $ testexpr) $
                   \vare -> return $ foldl (\accE vale -> mergeE accE $ testE vare vale) (testE vare h) t

      where testE vare vale = EC.binop (if isIn then OEqu else ONeq) vare vale
            mergeE acce nexte = EC.binop (if isIn then OOr else OAnd) acce nexte

    cgexpr subqbs f sid e@(InPredicate _ ine isIn (InQueryExpr _ _)) = do
      testexpr <- cgexpr subqbs f sid ine
      (subexpr, osid, bm) <- cgsubquery subqbs e
      memberE isIn osid bm testexpr subexpr

    cgexpr _ _ _ e = throwE $ "Unhandled expression in codegen: " ++ show e

    -- Case-statement generation.
    cgcase _ _ _ _ [] = throwE $ "Invalid empty case-list in cgcase"
    cgcase subqbs f sid elseexpr whens@((_,e):_) = do
      elseE <- maybe (zeroSQLE (Just sid) e) (cgexpr subqbs f sid) elseexpr
      foldM (cgcasebranch subqbs f sid) elseE whens

    cgcasesimple _ _ _ _ _ [] = throwE $ "Invalid empty case-list in cgcasesimple"
    cgcasesimple subqbs f sid expr elseexpr whens@((_, e):_) = do
      valE  <- cgexpr subqbs f sid expr
      elseE <- maybe (zeroSQLE (Just sid) e) (cgexpr subqbs f sid) elseexpr
      foldM (cgcasebrancheq subqbs f sid valE) elseE whens

    cgcasebranch subqbs f sid elseE (l,e) = do
      predE <- case l of
                  [] -> throwE "Invalid case-branch-list"
                  [x] -> cgexpr subqbs Nothing sid x
                  h:t -> cgexpr subqbs Nothing sid h >>= \hE -> foldM (cgconjunct subqbs sid) hE t

      thenE <- cgexpr subqbs f sid e
      return $ EC.ifThenElse predE thenE elseE

    cgcasebrancheq subqbs f sid valE elseE (l,e) = do
      testValE <- case l of
                    [x] -> cgexpr subqbs Nothing sid x
                    _ -> throwE "Invalid case-branch-eq-list"
      thenE <- cgexpr subqbs f sid e
      return $ EC.ifThenElse (EC.binop OEqu valE testValE) thenE elseE

    -- Subqueries
    cgsubquery subqbs e =
      case lookup e subqbs of
        Nothing -> throwE $ "Found a subquery without a binding: " ++ show e
        Just (_, qcl) -> cgclosure qcl >>= \(r, osid, bm, _) -> return (r, osid, bm)

    bindingMap l = foldM qualifyBindings (BMTVPartition Map.empty) l

    qualifyBindings (BMTVPartition acc) (qual, Just aenv, bm) = do
      f <- case bm of
                BMNone -> return $ commonNoneBinding qual
                BMTPrefix i -> return $ commonPrefixBinding qual i
                BMTFieldMap fb -> return $ commonFieldBinding qual fb
                _ -> throwE "Cannot qualify partitioned bindings"
      return $ BMTVPartition $ Map.foldlWithKey f acc aenv

    qualifyBindings _ _ = throwE "Cannot qualify partitioned bindings"

    commonNoneBinding qual acc path _ = case path of
      [i] -> Map.insert i (qual, Nothing) acc
      _ -> acc

    commonPrefixBinding qual pfx acc path _ = case path of
      [i] -> Map.insert i (qual, Just $ Left pfx) acc
      _ -> acc

    commonFieldBinding qual fb acc path _ = case path of
      [i] -> maybe acc (\typePath -> Map.insert i (qual, Just $ Right typePath) acc) $ Map.lookup i fb
      _ -> acc

    filterChainE :: ScopeId -> BindingMap -> SubqueryBindings -> K3 Expression -> ScalarExpr -> SQLParseM (K3 Expression)
    filterChainE sid bm subqbs eacc e = do
      i <- uniqueScopeQualifier sid
      filterE <- cgexpr subqbs Nothing sid e
      bodyE <- bindE sid bm (Just i) filterE
      return $ EC.applyMany (EC.project "filter" eacc) [EC.lambda i bodyE]

    annotateTriggerBody i (QueryPlan tOpt chains _) mergeOpt e = do
      if distributed
        then case tOpt of
               Just (isEquiJoin -> True) ->
                 return $ e @+ (EApplyGen True "DistributedHashJoin2" $ Map.fromList [("lbl", SLabel i)])

               Just (isJoin -> True) ->
                 return $ e @+ (EApplyGen True "BroadcastJoin2" $ Map.fromList [("lbl", SLabel i)])

               Just (treeChains -> Just chains) | not (null chains) && isGroupByAggregatePath (last chains) ->
                 case mergeOpt of
                   Just mergeF -> return $ e @+ (EApplyGen True "DistributedGroupBy2"
                                         $ Map.fromList [("lbl", SLabel i), ("merge", SExpr mergeF)])

                   Nothing -> throwE "No merge function found for group-by stage"

               _ -> return e

        else maybe (return e) (const $ joinBarrier e) $ Map.lookup i stagechildren

    joinBarrier e = mkCountBarrier e $ EC.constant $ CInt 2

    mkCountBarrier e countE = do
      args <- barrierArgs countE
      return $ e @+ EApplyGen True "OnCounter" args

    barrierArgs countE = do
      lblsym <- slblsextM
      return $ Map.fromList [ ("id", SLabel $ "barrier" ++ show lblsym)
                            , ("eq", SExpr $ countE)
                            , ("reset", SExpr $ EC.constant $ CBool False)
                            , ("profile", SExpr $ EC.constant $ CBool False) ]

    master = DC.global "master" (immutT TC.address) Nothing

    mkLoader (i,t) = do
      dt <- twrapcolelemM t
      let pathCT = (TC.collection $ recT [("path", TC.string)]) @+ TAnnotation "Collection"
      let rexpr = EC.applyMany (EC.variable $ i ++ "LoaderE") [EC.variable $ i ++ "Files", EC.variable i]
      return $
        ([(DC.global (i ++ "LoaderE") (immutT $ TC.function pathCT $ TC.function dt TC.unit) Nothing) @+ cArgsProp 2,
          DC.global (i ++ "Files") (immutT pathCT) Nothing],
         rexpr)

    mkPeerInit exprs =
        [DC.trigger "startPeer" TC.unit $ EC.lambda "_" $
          EC.block $ exprs ++ [EC.send (EC.variable "start") (EC.variable $ if distributed then "master" else "me") EC.unit]]

    mkInit decls = do
      sendsE <- if distributed then
                  let startE = EC.block $ flip map stageinits $ \i ->
                                 EC.applyMany (EC.project "iterate" $ EC.variable "peers")
                                   [EC.lambda "p" $ EC.send (EC.variable $ trig i) (EC.project "addr" $ EC.variable "p") EC.unit]

                  in mkCountBarrier startE $ EC.applyMany (EC.project "size" $ EC.variable "peers") [EC.unit]
                else return $ EC.block $ map (\i -> EC.send (EC.variable i) (EC.variable "me") EC.unit) $ foldl declTriggers [] decls

      return $ [DC.trigger "start" TC.unit $ EC.lambda "_" $
                  EC.block $ [EC.unit @+ EApplyGen True "SQL" Map.empty, sendsE]]

    declTriggers acc (tag -> DTrigger i _ _) = acc ++ [i]
    declTriggers acc _ = acc

sqlstringify :: [SQLDecl] -> SQLParseM [String]
sqlstringify stmts = mapM prettystmt stmts
  where prettystmt (SQLRel   (i, t)) = return $ boxToString $ [unwords ["Table:", i]] %$ prettyLines t
        prettystmt (SQLStage (i, t)) = return $ boxToString $ [unwords ["Stage:", i]] %$ prettyLines t
        prettystmt (SQLQuery plan)   = return $ boxToString $ ["Plan: "] %$ prettyLines plan


sqldepgraph :: [SQLDecl] -> SQLParseM [String]
sqldepgraph stmts = mapM depgraph stmts >>= return . concat
  where depgraph (SQLRel _) = return []
        depgraph (SQLStage _) = return []
        depgraph (SQLQuery (QueryPlan Nothing [] _)) = return []
        depgraph (SQLQuery (QueryPlan Nothing chains _)) = chaseScope $ pcoutsid $ last chains
        depgraph (SQLQuery (QueryPlan (Just t) chains _)) = treeSchema t >>= \sid -> chaseScope $ chainSchema sid chains

        chaseScope sid = do
          sf <- sqclkupM sid
          ptrs <- sqcfptrsM sid
          nodes <- mapM (adgchaseNodesM []) ptrs >>= return . nub . concat
          return $ [unwords ["Scope", show sid, show sf, show ptrs]] ++ (indent 2 $ adgnodes nodes)

        adgnodes nodes = map (\(p,node) -> unwords [show p, show $ adnn node, show $ adnr node, show $ adnch node])
                            $ sortBy (compare `on` fst) nodes


{- Code generation helpers. -}
projectPathE :: K3 Expression -> [Identifier] -> K3 Expression
projectPathE e p = foldl (\accE i -> EC.project i accE) e p

fieldE :: TypeMapping -> Identifier -> K3 Expression -> K3 Expression
fieldE Nothing _ e = e
fieldE (Just (Left pfx)) i e = EC.project i $ EC.project pfx e
fieldE (Just (Right tp)) _ e = projectPathE e tp

namedRecordE :: Identifier -> Map Identifier TypePath -> [Identifier] -> SQLParseM (K3 Expression)
namedRecordE i fb ids = foldM field [] ids >>= return . recE
  where field acc j = maybe (err j) (\tp -> return $ acc ++ [(j, projectPathE (EC.variable i) tp)]) $ Map.lookup j fb
        err j = throwE $ "No field binding found in namedRecordE for " ++ show j

compositeRecordE :: Map Identifier (Identifier, TypeMapping) -> [Identifier] -> SQLParseM (K3 Expression)
compositeRecordE pb ids = foldM field [] ids >>= return . recE
  where field acc i = maybe (err i) (\(v, tm) -> return $ acc ++ [(i, fieldE tm i $ EC.variable v)]) $ Map.lookup i pb
        err i = throwE $ "No field binding found in compositeRecordE for " ++ show i

bindE :: ScopeId -> BindingMap -> Maybe Identifier -> K3 Expression -> SQLParseM (K3 Expression)
bindE sid bm iOpt e = do
  ids <- sqclkupM sid >>= unqualifiedAttrs
  case (iOpt, bm) of
    (Just i, BMNone) -> return $ EC.bindAs (EC.variable i) (BRecord $ zip ids ids) e
    (Just i, BMTPrefix j) -> return $ EC.bindAs (EC.project j $ EC.variable i) (BRecord $ zip ids ids) e
    (Just i, BMTFieldMap fb) -> do
      initE <- namedRecordE i fb ids
      return $ EC.bindAs initE (BRecord $ zip ids ids) e

    (_, BMTVPartition pb) -> do
      initE <- compositeRecordE pb ids
      return $ EC.bindAs initE (BRecord $ zip ids ids) e

    _ -> throwE "Invalid binding variable in bindE"

concatE :: ScopeId -> BindingMap -> Maybe Identifier -> SQLParseM (K3 Expression)
concatE sid bm iOpt = do
  ids <- sqclkupM sid >>= unqualifiedAttrs
  case (iOpt, bm) of
    (Just i, BMNone) -> return $ EC.variable i
    (Just i, BMTPrefix j) -> return $ EC.project j $ EC.variable i
    (Just i, BMTFieldMap fb) -> namedRecordE i fb ids
    (_, BMTVPartition pb) -> compositeRecordE pb ids
    _ -> throwE "Invalid binding variable in concatE"

aggE :: Identifier -> (Identifier, K3 Expression) -> (Identifier, K3 Expression)
aggE i (f, e) = (f, EC.applyMany e [EC.project f $ EC.variable i])

aggMergeE :: Identifier -> Identifier -> (Identifier, K3 Expression) -> (Identifier, K3 Expression)
aggMergeE i j (f, mergeF) = (f, EC.applyMany mergeF [EC.project f $ EC.variable i, EC.project f $ EC.variable j])

zeroE :: [(Identifier, K3 Type)] -> SQLParseM (K3 Expression)
zeroE [] = return EC.unit
zeroE [(_,t)] = either throwE return $ defaultExpression t
zeroE l = either throwE return $ defaultExpression $ recT l

zeroT :: [(Identifier, K3 Type)] -> SQLParseM (K3 Type)
zeroT [] = return TC.unit
zeroT [(_,t)] = return t
zeroT l = return $ recT l

zeroSQLE :: Maybe ScopeId -> ScalarExpr -> SQLParseM (K3 Expression)
zeroSQLE sidOpt e = scalarexprType sidOpt e >>= \t -> either throwE return $ defaultExpression t

-- TODO: gensym
memoE :: K3 Expression -> (K3 Expression -> SQLParseM (K3 Expression)) -> SQLParseM (K3 Expression)
memoE srcE bodyF = case tag srcE of
  EConstant _ -> bodyF srcE
  EVariable _ -> bodyF srcE
  _ -> do { be <- bodyF $ EC.variable "__memo";
            return $ EC.letIn "__memo" (immutE srcE) $ be }

matchE :: ScopeId -> BindingMap -> K3 Expression -> K3 Expression -> SQLParseM (K3 Expression)
matchE sid bm elemexpr colexpr = do
  ids <- sqclkupM sid >>= unqualifiedAttrs
  targetE <- matchTargetE ids
  memoE (immutE elemexpr) $ \e -> do
      bodyE <- bindE sid bm (Just "__x") $ EC.binop OEqu targetE e
      return $ EC.applyMany (EC.project "filter" $ colexpr) [EC.lambda "__x" bodyE]

  where matchTargetE [x] = return $ EC.variable x
        matchTargetE l = throwE $ "Invalid match targets: " ++ show l


memberE :: Bool -> ScopeId -> BindingMap -> K3 Expression -> K3 Expression -> SQLParseM (K3 Expression)
memberE asMem sid bm elemexpr colexpr = matchE sid bm elemexpr colexpr >>= emptyE (not asMem)

emptyE :: Bool -> K3 Expression -> SQLParseM (K3 Expression)
emptyE asEmpty colexpr = return $
  EC.binop (if asEmpty then OEqu else ONeq)
    (EC.applyMany (EC.project "size" colexpr) [EC.unit]) $ EC.constant $ CInt 0


-- | Property construction helper
cArgsProp :: Int -> Annotation Declaration
cArgsProp i = DProperty $ Left ("CArgs", Just $ LC.int i)


{- Query plan pretty printing. -}

prettyList :: (Pretty a) => [a] -> [String]
prettyList [] = []
prettyList [x] = "|" : (shift "`- " "   " $ prettyLines x)
prettyList l = "|" : (concatMap (\x -> shift "+- " "|  " $ prettyLines x) (init l)
                      ++ ["|"] ++ (shift "`- " "   " $ prettyLines $ last l))

instance Pretty (Tree PlanNode) where
    prettyLines (Node (PJoin psid osid jt jeq jp _ chains) ch) =
      [unwords ["Join", show psid, show osid, show jt, "equalities", show $ length jeq, "preds", show $ length jp]]
        ++ prettyList chains ++ drawSubTrees ch

    prettyLines (Node (PTable n sid _ _ chains) _) =
      [unwords ["Table", n, show sid]] ++ prettyList chains

    prettyLines (Node (PSubquery _ qcl) _) = ["Subquery", "|"] ++ (shift "`- " "   " $ prettyLines qcl)

instance Pretty QueryClosure where
    prettyLines (QueryClosure _ plan) = ["QueryClosure", "|"] ++ (shift "`- " "   " $ prettyLines plan)

instance Pretty QueryPlan where
    prettyLines (QueryPlan treeOpt chains stgOpt) =
      ["QueryPlan " ++ maybe "" id stgOpt] ++ (maybe [] treeF treeOpt) ++ prettyList chains
      where treeF t = if null chains then "|" : (shift "`- " "   " $ prettyLines t)
                                     else "|" : (shift "+- " "|  " $ prettyLines t)

instance Pretty PlanCPath where
    prettyLines (PlanCPath sid selects gbs prjs aggs having _) =
      [unwords ["PlanCPath", show sid
                , "sels", show $ length selects
                , "gbys", show $ length gbs
                , "prjs", show $ length prjs
                , "aggs", show $ length aggs
                , maybe "<no having>" (const "having") having]]
