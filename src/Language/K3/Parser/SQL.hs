{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

-- TODO:
-- 4. not exists, scalar subquery on count
-- 5. expression case completion
-- 6. pushdown in subqueries
-- 7. subqueries in gbs, prjs, aggs
-- 8. magic decorrelation
-- 9. join types, including outer joins
-- w. extended chain simplification (e.g., duplicate gb/agg)
-- x. more groupByPushdown, subquery and distributed plan testing
-- y. distinct, order, limit, offset SQL support

module Language.K3.Parser.SQL where

import Control.Arrow ( (***), (&&&), first, second )
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Except

import Data.Char ( toLower )
import Data.Function ( on )
import Data.Functor.Identity
import Data.Maybe ( catMaybes, fromJust, isJust )
import Data.Either ( partitionEithers )
import Data.Monoid
import Data.List

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
type ScopeId    = Int
type ScopeEnv   = Map ScopeId ScopeFrame

-- | A stack of available bindings
type ScopeStack = [ScopeId]

-- | Join Types
type PJoinType = Either (Maybe (Natural, JoinType)) PIJoinType

data PIJoinType = Antijoin
                deriving (Eq, Show)


-- | Internal query plan representation.
data PlanCPath = PlanCPath { pcoutsid     :: ScopeId
                           , pcselects    :: ScalarExprList
                           , pcgroupbys   :: ScalarExprList
                           , pcprojects   :: SelectItemList
                           , pcaggregates :: SelectItemList
                           , pchaving     :: MaybeBoolExpr
                           , pcsubqueries :: PlanCSubqueries }
                deriving (Eq, Show)

data PlanNode = PJoin     { pnjprocsid    :: ScopeId
                          , pnjoutsid     :: ScopeId
                          , pnjtype       :: PJoinType
                          , pnjequalities :: [(ScalarExpr, ScalarExpr)]
                          , pnjpredicates :: ScalarExprList
                          , pnjpredsubqs  :: PlanCSubqueries
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

type SubqueryBindings = [(ScalarExpr, (Identifier, Bool, QueryClosure))]

-- TODO: MagicDecorrelation
data DecorrelatedQuery = RRDecorrelation ScalarExprList QueryClosure
                          -- ^ Range restricted decorrelation: RR-constraints, subquery

                       deriving (Eq, Show)

type DecorrelatedQueries = [(ScalarExpr, (Identifier, Bool, DecorrelatedQuery))]

-- | Independent, and correlated subqueries.
--   For now we only support correlated subqueries in where clauses.
--   TODO: correlated subqueries in having clauses.
data PlanCSubqueries = PlanCSubqueries { psselbinds   :: SubqueryBindings
                                       , pshvgbinds   :: SubqueryBindings
                                       , pscorrelated :: DecorrelatedQueries }
                       deriving (Eq, Show)

-- | Query open constraints (i.e., conjuncts that have at least one free variable).
data QCLConstraint = LConstraint ScalarExpr Operator Name AttrPath ScalarExpr
                   | RConstraint ScalarExpr Operator Name AttrPath ScalarExpr
                   | BConstraint ScalarExpr Operator Name AttrPath AttrPath
                   deriving (Eq, Show)

type QCLConstraints = [QCLConstraint]

data QueryClosure = QueryClosure { qcfree       :: [AttrPath]
                                 , qconstraints :: QCLConstraints
                                 , qcplan       :: QueryPlan }
                    deriving (Eq, Show)

type QueryAnalysis = ([AttrPath], QCLConstraints)


-- | Binding mappings
type TypePrefix  = Identifier
type TypePath    = [Identifier]
type TypeMapping = Maybe (Either TypePrefix TypePath)

data BindingMap = BMNone
                | BMTPrefix      Identifier
                | BMTFieldMap    (Map AttrPath TypePath)
                | BMTVPartition  (Map AttrPath (Identifier, TypeMapping))
                deriving (Eq, Show)

-- | Parsing state environment.
data SQLEnv = SQLEnv { relations  :: RTypeEnv
                     , adgraph    :: ADGraph
                     , scopeenv   :: ScopeEnv
                     , scopestack :: ScopeStack
                     , aliassym   :: ParGenSymS
                     , adpsym     :: ParGenSymS
                     , spsym      :: ParGenSymS
                     , sqsym      :: ParGenSymS
                     , stgsym     :: ParGenSymS
                     , slblsym    :: ParGenSymS
                     , aggdsym    :: ParGenSymS
                     , bindsym    :: ParGenSymS }
            deriving (Eq, Show)

-- | A stateful SQL parsing monad.
type SQLParseM = ExceptT String (State SQLEnv)

-- | SQL program statements
data SQLDecl = SQLRel   (Identifier, K3 Type)
             | SQLStage (Identifier, K3 Type)
             | SQLQuery QueryPlan
             deriving (Eq, Show)

type StageGraph = [Either Identifier (Identifier, Identifier)]

debugPlan :: Bool
debugPlan = True

{- Naming helpers. -}

materializeId :: Identifier -> Identifier
materializeId n = "output" ++ n

stageId :: Identifier -> Identifier
stageId n = "stage" ++ n

bindId :: Identifier -> Identifier
bindId n = "__bnd" ++ n

qPrefix :: Identifier -> Identifier
qPrefix i = if "__" `isPrefixOf` i then i else "__" ++ i

unqualifiedPath :: AttrPath -> AttrPath
unqualifiedPath [] = []
unqualifiedPath [x] = [x]
unqualifiedPath path = [last path]

pathQualifier :: AttrPath -> AttrPath
pathQualifier = init

pathAttribute :: AttrPath -> Identifier
pathAttribute = last

splitPath :: AttrPath -> (AttrPath, Identifier)
splitPath l = (init l, last l)

stripPathPrefix :: AttrPath -> AttrPath -> AttrPath
stripPathPrefix pfx path = maybe path id $ stripPrefix pfx path

stripPathQualifiers :: [AttrPath] -> [AttrPath]
stripPathQualifiers l = map unqualifiedPath l

scopePath :: AttrPath -> AttrPath
scopePath [] = []
scopePath [x] = [x]
scopePath l = (map qPrefix $ init l) ++ [last l]

nmcPath :: AttrPath -> [NameComponent]
nmcPath [] = []
nmcPath (scopePath -> l) = (map QNmc $ init l) ++ [Nmc $ last l]

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
sqlenv0 =
  SQLEnv Map.empty Map.empty Map.empty []
    contigsymS contigsymS contigsymS contigsymS contigsymS contigsymS contigsymS contigsymS

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
sfptrs sf@(_, ord, _) = mapM (\p -> sflkup sf p) ord

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

scstrylkup :: ScopeEnv -> ScopeStack -> [Identifier] -> Except String (Maybe ADGPtr)
scstrylkup env st path = foldM lkup Nothing st
  where lkup r@(Just _) _ = return r
        lkup _ sid = scftrylkup env sid path

scspush :: ScopeId -> ScopeStack -> ScopeStack
scspush sid st = sid : st

scspop :: ScopeStack -> (Maybe ScopeId, ScopeStack)
scspop [] = (Nothing, [])
scspop (h:t) = (Just h, t)


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

sbindsext :: SQLEnv -> (Int, SQLEnv)
sbindsext senv = (n, senv {bindsym = nsym})
  where (nsym, n) = gensym (bindsym senv)


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

sqcstrylkup :: SQLEnv -> [Identifier] -> Except String (Maybe ADGPtr)
sqcstrylkup env path = scstrylkup (scopeenv env) (scopestack env) path

sqcspush :: SQLEnv -> ScopeId -> SQLEnv
sqcspush env sid = env { scopestack = scspush sid $ scopestack env }

sqcspop :: SQLEnv -> (Maybe ScopeId, SQLEnv)
sqcspop env = (sidOpt, env { scopestack = nstack })
  where (sidOpt, nstack) = scspop $ scopestack env


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

sbindsextM :: SQLParseM Int
sbindsextM = get >>= \env -> return (sbindsext env) >>= \(i, nenv) -> put nenv >> return i

sqcstrylkupM :: [Identifier] -> SQLParseM (Maybe ADGPtr)
sqcstrylkupM path = get >>= liftExceptM . (\env -> sqcstrylkup env path)

sqcspushM :: ScopeId -> SQLParseM ()
sqcspushM sid = get >>= \env -> return (sqcspush env sid) >>= \nenv -> put nenv

sqcspopM :: SQLParseM (Maybe ScopeId)
sqcspopM = get >>= \env -> return (sqcspop env) >>= \(sidOpt, nenv) -> put nenv >> return sidOpt


{- Scope construction -}
sqgextScopeM :: Identifier -> [Identifier] -> [ADGPtr] -> SQLParseM ScopeId
sqgextScopeM qualifier ids ptrs = do
    let paths = map (:[]) ids
    let qpaths = map (\i -> [qualifier,i]) ids
    sqcpushM (Map.fromList $ zip paths ptrs ++ zip qpaths ptrs, paths, [[qualifier]])

sqgextNodeM :: Qualifiers -> [(AttrPath, ADGNode)] -> SQLParseM ScopeId
sqgextNodeM quals nodes = do
    ptrs <- mapM sqgextM $ map snd nodes
    sqcpushM (Map.fromList $ zip (map fst nodes) ptrs, map ((:[]) . adnn . snd) nodes, quals)

sqgextNodeByPathM :: [AttrPath] -> Qualifiers -> [(AttrPath, ADGNode)] -> SQLParseM ScopeId
sqgextNodeByPathM outputs quals nodes = do
    ptrs <- mapM sqgextM $ map snd nodes
    sqcpushM (Map.fromList $ zip (map fst nodes) ptrs, outputs, quals)

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
sqltype s lpOpt _ = case s of
  "int"               -> return TC.int
  "integer"           -> return TC.int
  "real"              -> return TC.real
  "double precision"  -> return TC.real
  "decimal"           -> return TC.real
  "numeric"           -> return TC.real
  "text"              -> return TC.string
  "char"              -> return $ maybe TC.string (\i -> TC.string @+ TProperty (Left $ "TPCHChar_" ++ show i)) lpOpt
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
sqlnmcomponent (Nmc s) = map toLower s
sqlnmcomponent (QNmc s) = map toLower s

sqlnmpath :: [NameComponent] -> AttrPath
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

selectItemList :: ScalarExprList -> SelectItemList
selectItemList el = map mkprojection el

namedSelectItemList :: SelectItemList -> ScalarExprList -> SQLParseM SelectItemList
namedSelectItemList sl el
    | length sl == length el = return $ map rebuild $ zip sl el
    | otherwise = throwE $ "List mismatch in namedSelectItemList"
  where rebuild ((SelExp _ _), e) = mkprojection e
        rebuild ((SelectItem _ _ nc), e) = SelectItem emptyAnnotation e nc

nameSelectItemList :: SelectItemList -> SQLParseM SelectItemList
nameSelectItemList sl = foldM mkNamed (0,[]) sl >>= return . snd
  where mkNamed (i,acc) (SelExp a e@(Identifier _ (Nmc n))) = return (i, acc ++ [SelectItem a e $ Nmc n])
        mkNamed (i,acc) (SelExp a e) = return (i+1, acc ++ [SelectItem a e $ Nmc $ "f" ++ show i])
        mkNamed (i,acc) si@(SelectItem _ _ _) = return (i, acc ++ [si])

aliasSelectItemList :: SelectItemList -> SQLParseM SelectItemList
aliasSelectItemList sl = mapM mkAlias sl
  where mkAlias (SelectItem _ _ a@(Nmc n)) = return $ SelectItem emptyAnnotation (Identifier emptyAnnotation a) a
        mkAlias e = throwE $ boxToString $ ["Invalid select item for aliasing"] %$ prettyLines e

aggregateexprs :: SelectItemList -> SQLParseM (ScalarExprList, [AggregateFn])
aggregateexprs sl = mapM aggregateexpr sl >>= return . unzip . concat

-- TODO: more expression types
aggregateexpr :: SelectItem -> SQLParseM [(ScalarExpr, AggregateFn)]
aggregateexpr (projectionexpr -> e') = aux e'
  where aux e = case e of
          FunCall _ nm args -> do
              let fn = sqlnm nm
              case (fn, args) of
                ("sum"  , [e']) -> return [(e', AggSum)]
                ("count", [e']) -> return [(e', AggCount)]
                ("min"  , [e']) -> return [(e', AggMin)]
                ("max"  , [e']) -> return [(e', AggMax)]
                ("avg"  , [e']) -> return [(e', AggSum), (Star emptyAnnotation, AggCount)]
                (_, _) -> mapM aux args >>= return . concat

          _ -> return []

mkaggregateSI :: AggregateFn -> SelectItem -> ScalarExpr -> SQLParseM SelectItem
mkaggregateSI fn agg e = case fn of
  AggSum   -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "sum"])   [e]
  AggCount -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "count"]) [e]
  AggMin   -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "min"])   [e]
  AggMax   -> rt agg $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "max"])   [e]

  where rt (SelExp _ _) e' = return (SelExp emptyAnnotation e')
        rt (SelectItem _ _ nmc) e' = return (SelectItem emptyAnnotation e' nmc)

mkaggregate :: AggregateFn -> ScalarExpr -> SQLParseM SelectItem
mkaggregate fn e = case fn of
  AggSum   -> mk "sum"
  AggCount -> mk "count"
  AggMin   -> mk "min"
  AggMax   -> mk "max"

  where mk i = return $ SelExp emptyAnnotation $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc i]) [e]

isAggregate :: ScalarExpr -> SQLParseM Bool
isAggregate (FunCall _ nm args) = do
  let fn = sqlnm nm
  case (fn, args) of
    ("sum"  , [_]) -> return True
    ("count", [_]) -> return True
    ("min"  , [_]) -> return True
    ("max"  , [_]) -> return True
    ("avg"  , [_]) -> return True
    (_, _) -> mapM isAggregate args >>= return . or

isAggregate _ = return False


{- Scope accessors. -}
attrIds :: AttrEnv -> SQLParseM [Identifier]
attrIds env = mapM (\ptr -> adnn <$> sqglkupM ptr) $ Map.elems env

singletonPath :: AttrPath -> SQLParseM Identifier
singletonPath [i] = return i
singletonPath p = throwE $ "Invalid singleton path: " ++ show p

scopeQualifiers :: ScopeId -> SQLParseM Qualifiers
scopeQualifiers sid = sqclkupM sid >>= \(_, _, quals) -> return quals

uniqueScopeQualifier :: ScopeId -> SQLParseM Identifier
uniqueScopeQualifier sid = do
    (_, _, quals) <- sqclkupM sid
    case quals of
      [[q]] -> return q
      _ -> throwE $ "Invalid unique scope qualifier: " ++ unwords [show sid, show quals]

outputAttrs :: ScopeFrame -> SQLParseM [AttrPath]
outputAttrs (_, ord, _) = return ord

unqualifiedOutputAttrs :: ScopeFrame -> SQLParseM [AttrPath]
unqualifiedOutputAttrs (_, ord, quals) = return $ foldl strip [] ord
  where strip acc path =
          let q = foldl (maxprefix path) [] quals in
          maybe acc (\p -> acc ++ [p]) $ stripPrefix q path

        maxprefix p acc qual = if qual `isPrefixOf` p && length qual > length acc then qual else acc

qualifiedOutputAttrs :: ScopeFrame -> SQLParseM [AttrPath]
qualifiedOutputAttrs (fr, outputs, quals) = return $ foldl tryqualprefix [] outputs
  where tryqualprefix acc path =
          let maxpath = foldl (withqual path) [] quals in
          acc ++ (if null maxpath then [path] else [maxpath])

        withqual path acc q =
          let np = q ++ path in
          if np `Map.member` fr && length np > length acc then np else acc

outputAttrQualifiers :: ScopeFrame -> SQLParseM Qualifiers
outputAttrQualifiers (_, outputs, quals) =
  foldM (\acc q -> return $ if any (q `isPrefixOf`) outputs then acc ++ [q] else acc) [] quals

outputAttrBindings :: ScopeFrame -> SQLParseM ([AttrPath], [Identifier])
outputAttrBindings (_, outputs, quals) = foldM accum ([], []) outputs
  where accum (oacc, bacc) o@[v] = return (oacc ++ [o], bacc ++ [v])
        accum (oacc, bacc) o =
          let q = pathQualifier o in
          return (oacc ++ [o], bacc ++ (if (last q) `notElem` bacc then [last q] else []))

qualifiedAttrs :: AttrPath -> ScopeFrame -> SQLParseM AttrEnv
qualifiedAttrs prefix (fr, _, q)
  | prefix `elem` q = return $ Map.filterWithKey (\path _ -> prefix `isPrefixOf` path) fr
  | otherwise = throwE $ unwords ["Could not find qualifier:", show prefix, "(", show q, ")"]

partitionAttrEnv :: ScopeFrame -> SQLParseM (AttrEnv, AttrEnv)
partitionAttrEnv (fr,_,_) = return $ Map.partitionWithKey (\path _ -> length path <= 1) fr

partitionAttrEnvByQual :: ScopeFrame -> Qualifiers -> SQLParseM (Map AttrPath AttrEnv)
partitionAttrEnvByQual (fr, _, quals) pquals =
  if pquals `intersect` quals == pquals
    then return $ Map.foldlWithKey addP Map.empty fr
    else throwE $ "Invalid qualifiers for partitioning attrEnv"
  where
    addP acc path ptr
      | length path <= 1 = acc
      | otherwise =
        let q = foldl (maxprefix path) [] pquals
            remPathOpt = stripPrefix q path
        in
        if not (null q) && isJust remPathOpt
          then Map.alter (inject (fromJust remPathOpt) ptr) q acc
          else acc

    maxprefix p acc qual = if qual `isPrefixOf` p && length qual > length acc then qual else acc

    inject path ptr Nothing = Just $ Map.singleton path ptr
    inject path ptr (Just aenv) = Just $ Map.insert path ptr aenv

partitionCommonQualifedAttrEnv :: ScopeFrame -> SQLParseM (Map AttrPath AttrEnv)
partitionCommonQualifedAttrEnv (fr, ord, _) = return $ Map.foldlWithKey (addQualifiedCommon ord) Map.empty fr
  where
    addQualifiedCommon common acc path ptr
      | length path <= 1 = acc
      | otherwise =
        let (qual, attr) = splitPath path in
        if null common || [attr] `elem` common then Map.alter (inject [attr] ptr) qual acc else acc

    inject path ptr Nothing = Just $ Map.singleton path ptr
    inject path ptr (Just aenv) = Just $ Map.insert path ptr aenv

outputScopeFrame :: ScopeFrame -> SQLParseM ScopeFrame
outputScopeFrame (fr, ord, _) = return (Map.filterWithKey (\path _ -> path `elem` ord) fr, ord, [])

qualifiedScopeFrame :: ScopeFrame -> SQLParseM ScopeFrame
qualifiedScopeFrame (fr, _, q) = return (Map.filterWithKey (\path _ -> length path > 1) fr, [], q)

concatScopeFrames :: ScopeFrame -> ScopeFrame -> SQLParseM ScopeFrame
concatScopeFrames (lfr,lord,lq) (rfr,rord,rq) = return (lfr <> rfr, lord ++ rord, lq++rq)

mergeScopeFrames :: ScopeFrame -> ScopeFrame -> SQLParseM ScopeFrame
mergeScopeFrames lsf@(_, lord, lquals) rsf@(_, rord, rquals) = do
  let common = (map unqualifiedPath lord) `intersect` (map unqualifiedPath rord)
  let nord = (nub $ lord ++ rord) \\ common
  ((lu,lq), (ru,rq)) <- (,) <$> partitionAttrEnv lsf <*> partitionAttrEnv rsf
  let nu = (foldl (flip Map.delete) lu common) <> (foldl (flip Map.delete) ru common)
  let nenv = nu <> lq <> rq
  let nord' = Map.foldlWithKey (addIfCommon common) nord $ lq <> rq
  debugMerge lq rq $ return (nenv, nord', lquals ++ rquals)

  where
    addIfCommon common oacc path ptr =
      let u = unqualifiedPath path
      in if u `notElem` common || any (path `isSuffixOf`) oacc then oacc else oacc ++ [path]

    debugMerge lq rq r = if not debugPlan then r else flip trace r $
        boxToString $ ["Merge SF"] %$ ["LQ" ++ (show $ Map.toList lq)]
                                   %$ ["RQ" ++ (show $ Map.toList rq)]

mergeScopes :: ScopeId -> ScopeId -> SQLParseM ScopeId
mergeScopes id1 id2 = do
  (lsf, rsf) <- (,) <$> sqclkupM id1 <*> sqclkupM id2
  nsf <- mergeScopeFrames lsf rsf
  sqcpushM nsf

outputQualifier :: Identifier -> Maybe TableAlias -> SQLParseM (Maybe Identifier)
outputQualifier pfx alOpt = do
    sym <- sasextM
    case alOpt of
      Nothing -> return $ Just $ map toLower $ pfx ++ show sym
      Just al -> return $ sqltablealias (pfx ++ show sym) al

outputTypeAndQualifier :: K3 Type -> Identifier -> Maybe TableAlias -> SQLParseM (K3 Type, Maybe Identifier)
outputTypeAndQualifier t pfx alOpt = do
    sym <- sasextM
    case alOpt of
      Nothing -> return (t, Just $ map toLower $ pfx ++ show sym)
      Just al -> (,) <$> taliascolM al t <*> return (sqltablealias (pfx ++ show sym) al)

typedOutputScope :: K3 Type -> Identifier -> Maybe TableAlias -> SQLParseM ScopeId
typedOutputScope t pfx alOpt = do
    (rt, tid) <- outputTypeAndQualifier t pfx alOpt
    sqgextSchemaM tid rt

exprOutputScope :: ScopeId -> Identifier -> [(Identifier, K3 Type, ScalarExpr)] -> SQLParseM ScopeId
exprOutputScope sid pfx exprs = do
    sym <- sasextM
    let qual = Just $ map toLower $ pfx ++ show sym
    sqgextExprM sid qual exprs

maxQualifier :: AttrEnv -> Qualifiers -> SQLParseM AttrPath
maxQualifier fr quals =
    let p = foldl maxpath [] $ Map.keys fr
        q = foldl (maxprefix p) [] quals
    in if not (null p || null q)
         then return q
         else throwE $ "Could not find alias qualifier in " ++ show fr

  where
    maxpath acc path = if length path > length acc then path else acc
    maxprefix p acc qual = if qual `isPrefixOf` p && length qual > length acc then qual else acc

maxScopeQualifier :: ScopeId -> SQLParseM AttrPath
maxScopeQualifier sid = do
  sf@(fr, _, quals) <- sqclkupM sid
  maxQualifier fr quals

aliasNodesByPath :: ScopeFrame -> [(AttrPath, AttrPath)] -> SQLParseM [(AttrPath, ADGNode)]
aliasNodesByPath sf pathPairs = do
  ptrs <- mapM (\(old, new) -> liftExceptM $ sflkup sf old) pathPairs
  ptrsPathsNodes <- mapM sqglkupM ptrs >>= return . zip3 ptrs (map snd pathPairs)
  mapM mkNode ptrsPathsNodes

  where
    mkNode (ptr, path, node) = nexpr >>= \e -> return (path, ADGNode i t relOpt (Just e) [ptr])
      where i = pathAttribute path
            t = adnt node
            relOpt = if length path > 1 then Just $ last $ pathQualifier path else Nothing
            nexpr = case nmcPath path of
                      []    -> throwE "Invalid attribute name in aliasedQualOutputScope"
                      [nmc] -> return $ Identifier emptyAnnotation nmc
                      l     -> return $ QIdentifier emptyAnnotation l


aliasedOutputScope :: ScopeId -> Identifier -> Maybe TableAlias -> SQLParseM ScopeId
aliasedOutputScope sid pfx alOpt = do
    Just tid <- outputQualifier pfx alOpt
    sf@(fr, outputs, quals) <- sqclkupM sid
    alqual <- case quals of
                [q] -> return q
                _ -> maxQualifier fr quals
    let qpaths = Map.foldlWithKey (accumMByPrefix alqual) [] fr
    let nqpaths = map (\path -> [tid] ++ tail path) qpaths
    let noutputs = foldl (disambiguatePath nqpaths) [] nqpaths
    let nupaths = filter (\p -> length p < 2) noutputs
    qnodes <- aliasNodesByPath sf $ zip qpaths nqpaths
    unodes <- aliasNodesByPath sf $ zip nupaths nupaths
    r <- sqgextNodeByPathM noutputs [[tid]] $ qnodes ++ unodes
    rsf <- sqclkupM r
    debugScope sf rsf $ return r

  where
    accumMByPrefix pfx acc path _ = accumByPrefix pfx acc path
    accumByPrefix pfx acc path = if pfx `isPrefixOf` path then acc ++ [path] else acc

    disambiguatePath npaths acc path@(unqualifiedPath -> upath) =
      let ambiguous = not $ null $ filter (\o -> unqualifiedPath o == upath) $ filter (/= path) npaths
      in if ambiguous then acc ++ [path] else acc ++ [upath]

    debugScope sf rsf r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["ALIAS"] %$ [show sf] %$ [show rsf]

joinOutputScope :: ScopeId -> Identifier -> Maybe TableAlias -> SQLParseM ScopeId
joinOutputScope sid pfx alOpt = do
    sf@(fr, outputs, quals) <- sqclkupM sid
    -- For multiway joins, strip out intermediate paths and keep only base table paths
    let envPaths = filter (\path -> length path <= 2) $ Map.keys fr
    let nquals = filter (`elem` (map pathQualifier envPaths)) quals
    nodes <- aliasNodesByPath sf $ zip envPaths envPaths
    sym <- sasextM
    Just tid <- case alOpt of
                  Nothing -> return $ Just $ pfx ++ show sym
                  Just al -> return $ sqltablealias (pfx ++ show sym) al
    let qpaths = filter (\(p,_) -> length p > 1) nodes
    let npaths = map (\(p,_) -> [tid] ++ p) qpaths
    let noutputs = foldl (disambiguatePath npaths) [] npaths
    qnodes <- aliasNodesByPath sf $ zip (map fst qpaths) npaths
    let rnodes = nodes ++ qnodes
    debugScope envPaths rnodes $ sqgextNodeByPathM noutputs (nquals ++ [[tid]]) rnodes

  where
    disambiguatePath npaths acc path@(unqualifiedPath -> upath) =
      let ambiguous = not $ null $ filter (\o -> unqualifiedPath o == upath) $ filter (/= path) npaths
      in if ambiguous then acc ++ [path] else acc ++ [upath]

    debugScope qpaths nodes r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["JOS "] ++ [show qpaths] ++ concatMap (prettyLines . snd) nodes


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
  pOpt <- sqcftrylkupM sid $ scopePath path
  maybe rfail rsuccess pOpt

isAggregatePath :: PlanCPath -> Bool
isAggregatePath (PlanCPath _ _ [] [] (_:_) _ _) = True
isAggregatePath _ = False

isGroupByAggregatePath :: PlanCPath -> Bool
isGroupByAggregatePath (PlanCPath _ _ (_:_) _ (_:_) _ _) = True
isGroupByAggregatePath _ = False

isNonAggregatePath :: PlanCPath -> Bool
isNonAggregatePath (PlanCPath _ _ [] _ [] Nothing _) = True
isNonAggregatePath _ = False

-- | Chain extraction.
planNodeChains :: PlanNode -> Maybe [PlanCPath]
planNodeChains (PJoin _ _ _ _ _ _ chains) = Just chains
planNodeChains (PTable _ _ _ _ chains) = Just chains
planNodeChains (PSubquery _ qcl) = queryClosureChains qcl

treeChains :: PlanTree -> Maybe [PlanCPath]
treeChains (Node n _) = planNodeChains n

queryPlanChains :: QueryPlan -> Maybe [PlanCPath]
queryPlanChains (QueryPlan tOpt chains _) = if null chains then maybe Nothing treeChains tOpt else Just chains

queryClosureChains :: QueryClosure -> Maybe [PlanCPath]
queryClosureChains (QueryClosure _ _ plan) = queryPlanChains plan

-- | Chains with their input schema.
planNodeChainsAndInputSchema :: PlanNode -> SQLParseM (Maybe ([PlanCPath], ScopeId, [ScopeId]))
planNodeChainsAndInputSchema (PJoin sid _ _ _ _ _ chains) = return $ Just (chains, sid, [])
planNodeChainsAndInputSchema (PTable _ sid _ _ chains) = return $ Just (chains, sid, [])
planNodeChainsAndInputSchema (PSubquery sid qcl) = do
  rOpt <- queryClosureChainsAndInputSchema qcl
  return $ maybe Nothing (\(a,b,c) -> Just (a,b,c++[sid])) rOpt

treeChainsAndInputSchema :: PlanTree -> SQLParseM (Maybe ([PlanCPath], ScopeId, [ScopeId]))
treeChainsAndInputSchema (Node n _) = planNodeChainsAndInputSchema n

queryPlanChainsAndInputSchema :: QueryPlan -> SQLParseM (Maybe ([PlanCPath], ScopeId, [ScopeId]))
queryPlanChainsAndInputSchema (QueryPlan tOpt chains _) =
  if null chains then maybe (return Nothing) treeChainsAndInputSchema tOpt
  else maybe (return Nothing) (\t -> treeSchema t >>= \sid -> return $ Just (chains, sid, [])) tOpt

queryClosureChainsAndInputSchema :: QueryClosure -> SQLParseM (Maybe ([PlanCPath], ScopeId, [ScopeId]))
queryClosureChainsAndInputSchema (QueryClosure _ _ plan) = queryPlanChainsAndInputSchema plan

-- | Chain replacement.
replacePlanNodeChains :: [PlanCPath] -> PlanNode -> SQLParseM PlanNode
replacePlanNodeChains nchains n@(PJoin _ _ _ _ _ _ _) = return $ n { pnjpath = nchains }
replacePlanNodeChains nchains n@(PTable _ _ _ _ _) = return $ n { pntpath = nchains }
replacePlanNodeChains nchains n@(PSubquery _ qcl) = do
  nqcl <- replaceQueryClosureChains nchains qcl
  nqsid <- closureSchema nqcl
  return $ n { pnsqoutsid = nqsid, pnqclosure = nqcl }

replaceTreeChains :: [PlanCPath] -> PlanTree -> SQLParseM PlanTree
replaceTreeChains nchains t@(Node n _) = replaceData t <$> replacePlanNodeChains nchains n

replaceQueryPlanChains :: [PlanCPath] -> QueryPlan -> SQLParseM QueryPlan
replaceQueryPlanChains nchains p@(QueryPlan tOpt chains _) =
  if null chains
    then maybe (return p) (\t -> replaceTreeChains nchains t >>= \nt -> return $ p { qjoinTree = Just nt }) tOpt
    else return $ p { qpath = nchains }

replaceQueryClosureChains :: [PlanCPath] -> QueryClosure -> SQLParseM QueryClosure
replaceQueryClosureChains nchains cl@(QueryClosure _ _ plan) = do
  nplan <- replaceQueryPlanChains nchains plan
  return $ cl { qcplan = nplan }

pcext :: PlanCPath -> PlanTree -> SQLParseM PlanTree
pcext p (Node n ch) = case n of
  PJoin psid osid jt jeq jp jpb chains -> return $ Node (PJoin psid osid jt jeq jp jpb $ chains ++ [p]) ch
  PTable i tsid trOpt bmOpt chains -> return $ Node (PTable i tsid trOpt bmOpt $ chains ++ [p]) ch
  PSubquery _ qcl -> do
    nqcl <- pcextclosure p qcl
    nqsid <- closureSchema nqcl
    return $ Node (PSubquery nqsid nqcl) ch

  where pcextclosure p' (QueryClosure fvs cstrs plan) = pcextplan p' plan >>= \nplan -> return $ QueryClosure fvs cstrs nplan
        pcextplan p' (QueryPlan tOpt chains stgOpt) = return $ QueryPlan tOpt (chains ++ [p']) stgOpt

pcextSelect :: ScopeId -> PlanCSubqueries -> ScalarExpr -> [PlanCPath] -> [PlanCPath]
pcextSelect sid subqs p [] = [PlanCPath sid [p] [] [] [] Nothing subqs]
pcextSelect _ subqs p pcl@(last -> c) = init pcl ++ [c {pcselects = pcselects c ++ [p], pcsubqueries = concatSubqueries (pcsubqueries c) subqs}]

pcextGroupBy :: SelectItemList -> SelectItemList -> PlanNode -> SQLParseM PlanNode
pcextGroupBy gbs aggs n@(PJoin _ osid _ _ _ _ chains) = do
  aggsid <- aggregateSchema (Just $ chainSchema osid chains) gbs aggs
  return $ n { pnjpath = chains ++ [PlanCPath aggsid [] (projectionexprs gbs) gbs aggs Nothing $ PlanCSubqueries [] [] []] }

pcextGroupBy gbs aggs n@(PTable _ sid _ _ chains) = do
  aggsid <- aggregateSchema (Just $ chainSchema sid chains) gbs aggs
  return $ n { pntpath = chains ++ [PlanCPath aggsid [] (projectionexprs gbs) gbs aggs Nothing $ PlanCSubqueries [] [] []] }

pcextGroupBy gbs aggs n@(PSubquery _ qcl) = extPlan (qcplan qcl) >>= \p -> return $ n { pnqclosure = qcl { qcplan = p } }
  where extPlan p@(QueryPlan tOpt chains stgOpt) = do
          sid <- planSchema p
          aggsid <- aggregateSchema (Just sid) gbs aggs
          return $ QueryPlan tOpt (chains ++ [PlanCPath aggsid [] (projectionexprs gbs) gbs aggs Nothing $ PlanCSubqueries [] [] []]) stgOpt

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

modifyClosurePlan :: (QueryPlan -> SQLParseM QueryPlan) -> QueryClosure -> SQLParseM QueryClosure
modifyClosurePlan modifyF (QueryClosure fvs cstrs plan) = do
  nplan <- modifyF plan
  return $ QueryClosure fvs cstrs nplan

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

closureSchema :: QueryClosure -> SQLParseM ScopeId
closureSchema (QueryClosure _ _ plan) = planSchema plan

aggregateSchema :: Maybe ScopeId -> SelectItemList -> SelectItemList -> SQLParseM ScopeId
aggregateSchema (Just sid) [] [] = return sid
aggregateSchema sidOpt projects aggregates = do
    let pexprs = projectionexprs projects
    let aexprs = projectionexprs aggregates
    (prji, prjids) <- foldM selectItemIdAcc (0, []) projects
    (_, aggids) <- foldM selectItemIdAcc (prji, []) aggregates
    prjt <- mapM (scalarexprType sidOpt) pexprs
    aggt <- mapM (aggregateType sidOpt) aexprs
    let qual = if null aexprs then "__PRJ" else "__AGG"
    case sidOpt of
      Nothing -> typedOutputScope (recT $ zip prjids prjt ++ zip aggids aggt) "__RN" Nothing
      Just sid -> exprOutputScope sid qual $ (zip3 prjids prjt pexprs) ++ (zip3 aggids aggt aexprs)

refreshInputSchema :: PlanNode -> [PlanTree] -> SQLParseM PlanNode
refreshInputSchema (PJoin _ _ jt jeq jp jpb chains) [l,r] = do
    (lsid, rsid)  <- (,) <$> treeSchema l <*> treeSchema r
    jpsid         <- mergeScopes lsid rsid
    (lsf, rsf, jpsf) <- (,,) <$> sqclkupM lsid <*> sqclkupM rsid <*> sqclkupM jpsid
    josid         <- debugRefresh jpsid jpsf lsf rsf $ joinOutputScope jpsid "__JR" Nothing
    return $ PJoin jpsid josid jt jeq jp jpb chains

  where debugRefresh jpsid jpsf lsf rsf result = if not debugPlan then result else flip trace result $ boxToString $
          ["REF JPSF: " ++ show jpsid] %$ [show jpsf] %$ ["LSF"] %$ [show lsf] %$ ["RSF"] %$ [show rsf]

refreshInputSchema n [] = return n
refreshInputSchema _ _ = throwE "Invalid plan tree node for refreshInputSchema"

nestedRecordField :: K3 Type -> AttrPath -> SQLParseM (K3 Type)
nestedRecordField t [] = return t
nestedRecordField t@(tnc -> (TRecord ids, ch)) (h:rest) = maybe err (flip nestedRecordField rest) $ lookup h $ zip ids ch
  where err = throwE $ boxToString $ ["Could not find nested record component " ++ show h] %$ prettyLines t

nestedRecordField t l = throwE $ boxToString $ ["Could not extract path " ++ show l] %$ prettyLines t

extendNestedRecord :: K3 Type -> [(Identifier, K3 Type)] -> [Identifier] -> SQLParseM [(Identifier, K3 Type)]
extendNestedRecord _ _ [] = throwE "Invalid nested record extension"
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

  where
    replaceField dst nt (src,t) | src == dst = (dst, nt)
                                | otherwise = (src, t)

scopeType :: ScopeId -> SQLParseM (K3 Type)
scopeType sid = do
  sf@(fr, ord, _) <- sqclkupM sid
  case ord of
    [] -> do
      let pathptrs = Map.toList fr
      nodes <- mapM (sqglkupM . snd) pathptrs
      fields <- foldM (\acc ((path, _), n) -> extendNestedRecord (adnt n) acc path) [] $ zip pathptrs nodes
      tcolM $ recT fields

    _ -> do
      ptrs <- mapM (\path -> liftExceptM $ sflkup sf path) ord
      nodes <- mapM sqglkupM ptrs
      tcolM $ recT $ map (\(i,n) -> (i, adnt n)) $ zip (map head ord) nodes

k3ScopeType :: ScopeId -> BindingMap -> SQLParseM (K3 Type)
k3ScopeType sid bm = do
  sf <- sqclkupM sid
  t <- scopeType sid
  rt <- telemM t
  case (tnc rt, bm) of
    ((TRecord _, _), BMNone) -> return t
    ((TRecord _, _), BMTPrefix j) -> tcolM $ recT [(j,rt)]
    (_, BMTFieldMap fb) -> fieldMapT sf rt fb >>= tcolM
    (_, BMTVPartition pb) -> partitionMapT sf rt pb >>= tcolM
    _ -> throwE "Invalid k3ScopeType element type"

  where
    fieldMapT sf t fb = trace (boxToString $ ["FIMAP " ++ show sid] ++ [show sf] ++ [show fb] ++ prettyLines t) $ do
      fields <- Map.foldlWithKey (fieldMapping t) (return []) fb
      return $ recT fields

    fieldMapping t accM path typepath = do
      acc <- accM
      ft <- nestedRecordField t path
      extendNestedRecord ft acc typepath

    partitionMapT sf t pb = trace (boxToString $ ["PMAP " ++ show sid] ++ [show sf] ++ [show pb] ++ prettyLines t) $ do
      fields <- Map.foldlWithKey (partitionMapping t) (return []) pb
      return $ recT fields

    partitionMapping t accM path (v, typemap) = do
      acc <- accM
      let typepath = maybe [v] (either (\pfx -> [v,pfx]) (\path -> v:path)) typemap
      ft <- nestedRecordField t path
      extendNestedRecord ft acc typepath


k3PlanType :: BindingMap -> QueryPlan -> SQLParseM (K3 Type)
k3PlanType _ (QueryPlan Nothing [] _) = throwE "Invalid query plan with no tables or expressions"
k3PlanType bm p = do
  sid <- planSchema p
  t <- k3ScopeType sid bm
  case queryPlanChains p of
    Just [] -> return t
    Just l | isAggregatePath (last l) -> do
      rt <- telemM t
      case tnc rt of
        (TRecord ids, ch) -> zeroT $ zip ids ch
        _ -> throwE "Invalid k3 aggregate plan type"

    _ -> return t


-- TODO:
-- i. builtin function types
-- ii. AST: AggregateFn, Interval, LiftOperator, NullLit, Placeholder, PositionalArg, WindowFn
scalarexprType :: Maybe ScopeId -> ScalarExpr -> SQLParseM (K3 Type)
scalarexprType _ (BooleanLit _ _) = return $ immutT $ TC.bool
scalarexprType _ (StringLit _ _) = return $ immutT $ TC.string
scalarexprType _ (NumberLit _ i) = return $ immutT $ if "." `isInfixOf` i then TC.real else TC.int
scalarexprType _ (TypedStringLit _ tn _) = sqlnamedtype tn

scalarexprType _ (Cast _ _ tn) = sqlnamedtype tn
scalarexprType _ (Extract _ _ _) = return TC.int

scalarexprType sidOpt (Identifier _ (sqlnmcomponent -> i)) = do
  sf <- maybe (return Nothing) (\sid -> sqclkupM sid >>= return . Just) sidOpt
  trypath sidOpt (trace (unwords ["bottom", i, show sidOpt, show sf]) $ return TC.bottom) (\ptr -> sqglkupM ptr >>= return . adnt) [i]

scalarexprType sidOpt (QIdentifier _ (sqlnmpath -> (scopePath -> path))) = do
  sf <- maybe (return Nothing) (\i -> sqclkupM i >>= return . Just) sidOpt
  trypath sidOpt (trace (unwords ["bottom", show path, show sidOpt, show sf]) $ return TC.bottom) (\ptr -> sqglkupM ptr >>= return . adnt) path

scalarexprType sidOpt (Case _ whens elseexpr) = maybe (caselistType sidOpt whens) (scalarexprType sidOpt) elseexpr
scalarexprType sidOpt (CaseSimple _ _ whens elseexpr) = maybe (caselistType sidOpt whens) (scalarexprType sidOpt) elseexpr

scalarexprType sidOpt e@(FunCall _ nm args) = do
  let fn = sqlnm nm
  case sqloperator fn args of
    (Just (UnaryOp  _ x))   -> scalarexprType sidOpt x
    (Just (BinaryOp _ x y)) -> do
      xt <- scalarexprType sidOpt x
      yt <- scalarexprType sidOpt y
      if unify xt yt then return xt
                     else throwE $ boxToString $ ["Binary operator sql type mismatch"]
                                %$ prettyLines xt %$ prettyLines yt
                                %$ ["On:"] %$ prettyLines e

    _ -> do
      case (fn, args) of
        ("!between", [_,_,_]) -> return TC.bool
        ("!like", [_,_])      -> return TC.bool
        ("!notlike", [_,_])   -> return TC.bool
        (_, _) -> throwE $ "Unsupported function in scalarexprType: " ++ fn

  where unify (tag -> TReal) (tag -> TInt)  = True
        unify (tag -> TInt)  (tag -> TReal) = True
        unify l r = compareTStrictAST l r

scalarexprType (Just sid) (Star _) = scopeType sid >>= telemM
scalarexprType (Just sid) (QStar _ (sqlnmcomponent -> n)) = do
  (fr,_,_) <- sqclkupM sid
  let ptrs = Map.elems $ Map.filterWithKey (\k _ -> [n] `isPrefixOf` k) fr
  idt <- mapM (\p -> sqglkupM p >>= return . (adnn &&& adnt)) ptrs
  return $ recT idt

scalarexprType _ (ScalarSubQuery _ _) = return TC.int -- TODO: return subquery type, not int!
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
      ("count", [_]) -> return $ TC.int
      ("min"  , [e]) -> scalarexprType sidOpt e
      ("max"  , [e]) -> scalarexprType sidOpt e
      ("avg"  , [e]) -> scalarexprType sidOpt e
      (_, _) ->
        case sqloperator fn args of
          (Just (UnaryOp  _ x))   -> aggregateType sidOpt x
          (Just (BinaryOp _ x y)) -> do
            xt <- aggregateType sidOpt x
            yt <- aggregateType sidOpt y
            if unify xt yt then return xt
                           else throwE $ boxToString $ ["Binary operator sql type mismatch"]
                                      %$ prettyLines xt %$ prettyLines yt
                                      %$ ["On:"] %$ prettyLines agg

          _ -> do
            case (fn, args) of
              ("!between", [_,_,_]) -> return TC.bool
              ("!like", [_,_])      -> return TC.bool
              ("!notlike", [_,_])   -> return TC.bool
              (_, _) -> throwE $ "Unsupported function in aggregateType: " ++ fn

  where unify (tag -> TReal) (tag -> TInt)  = True
        unify (tag -> TInt)  (tag -> TReal) = True
        unify l r = compareTStrictAST l r

aggregateType sidOpt e = scalarexprType sidOpt e

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
          []  -> (i+1, [(["f" ++ show i], [pfx])])
          [j] -> (i, [([j], [pfx])])
          _   -> (i, map (\j -> ([j], [pfx, j])) l)

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


{- Subqueries accessors -}
nullSubqueries :: PlanCSubqueries -> Bool
nullSubqueries (PlanCSubqueries [] [] []) = True
nullSubqueries _ = False

concatQAnalysis :: QueryAnalysis -> QueryAnalysis -> QueryAnalysis
concatQAnalysis (fvs1, qcl1) (fvs2, qcl2) = (nub $ fvs1 ++ fvs2, nub $ qcl1 ++ qcl2)

concatSubqueries :: PlanCSubqueries -> PlanCSubqueries -> PlanCSubqueries
concatSubqueries (PlanCSubqueries selbs1 hvgbs1 decorr1) (PlanCSubqueries selbs2 hvgbs2 decorr2) =
  PlanCSubqueries (nub $ selbs1 ++ selbs2) (nub $ hvgbs1 ++ hvgbs2) (nub $ decorr1 ++ decorr2)


{- Rewriting helpers. -}

-- TODO: case, etc.
-- This function does not descend into subqueries.
-- However, it should include free variables present in the subquery, and defined in
-- the given scope.
exprPathAttrs :: ScopeId -> ScalarExpr -> SQLParseM [(AttrPath, ADGPtr)]
exprPathAttrs sid e = trace (boxToString $ ["EPA " ++ show sid] %$ prettyLines e) $ case e of
  (Identifier _ (sqlnmcomponent -> i)) -> ensureLookup [i]
  (QIdentifier _ (sqlnmpath -> (scopePath -> path))) -> ensureLookup path
  (FunCall _ _ args) -> mapM (exprPathAttrs sid) args >>= return . concat
  _ -> return []

  where ensureLookup path = do
          sf <- sqclkupM sid
          ptrOpt <- sqcftrylkupM sid path
          case ptrOpt of
            Nothing -> sqcstrylkupM path >>= maybe (lookupErr sf path) (const $ return [])
            Just ptr -> return [(path, ptr)]

        lookupErr sf path = throwE $ boxToString $ [msg ++ unwords [show sid, show path]] %$ [show sf]
        msg = "Could not find path in scope env or stack: "

exprAttrs :: ScopeId -> ScalarExpr -> SQLParseM [ADGPtr]
exprAttrs sid e = exprPathAttrs sid e >>= return . map snd

-- Returns all qualifier path prefixes used in expression variables.
exprQualifiers :: ScalarExpr -> SQLParseM [[Identifier]]
exprQualifiers e = case e of
  (Identifier _ _) -> return []
  (QIdentifier _ (sqlnmpath -> (scopePath -> path))) -> return [pathQualifier path]
  (FunCall _ _ args) -> mapM exprQualifiers args >>= return . concat
  _ -> return []

-- TODO: more expression types
exprPaths :: ScalarExpr -> SQLParseM [AttrPath]
exprPaths e' = aux [] e'
  where aux acc e = case e of
          (Identifier _ (sqlnmcomponent -> i)) -> return $ acc ++ [[i]]
          (QIdentifier _ (sqlnmpath -> (scopePath -> path))) -> return $ acc ++ [path]
          (FunCall _ _ args) -> foldM aux acc args
          _ -> return acc

pathAsExpr :: AttrPath -> SQLParseM ScalarExpr
pathAsExpr []  = throwE "Invalid attribute path in simplifyExprSubqueries"
pathAsExpr [x] = return $ Identifier emptyAnnotation $ Nmc x
pathAsExpr nl  = return $ QIdentifier emptyAnnotation $ nmcPath nl


-- TODO: more expression types
substituteExpr :: [(AttrPath, ScalarExpr)] -> ScalarExpr -> SQLParseM ScalarExpr
substituteExpr bindings e = case e of
  (Identifier _ (sqlnmcomponent -> i)) -> return $ maybe e id $ lookup [i] bindings
  (QIdentifier _ (sqlnmpath -> (scopePath -> path))) -> return $ maybe e id $ lookup path bindings
  (FunCall ann nm args) -> mapM (substituteExpr bindings) args >>= \nargs -> return $ FunCall ann nm nargs
  _ -> return e

-- TODO: more expression types
substituteAggregates :: [((ScalarExpr, AggregateFn), ScalarExpr)] -> SelectItemList -> SQLParseM SelectItemList
substituteAggregates env l = forM l $ \si ->
    case si of
      SelExp a e -> aux e >>= \ne -> return (SelExp a ne)
      SelectItem a e nc -> aux e >>= \ne -> return (SelectItem a ne nc)

  where aux e = case e of
          FunCall a nm args -> do
              let fn = sqlnm nm
              case (fn, args) of
                ("sum"  , [e']) -> maybe (return e) return $ lookup (e', AggSum)   env
                ("count", [e']) -> maybe (return e) return $ lookup (e', AggCount) env
                ("min"  , [e']) -> maybe (return e) return $ lookup (e', AggMin)   env
                ("max"  , [e']) -> maybe (return e) return $ lookup (e', AggMax)   env

                ("avg"  , [e']) -> do
                  lOpt <- return ((,) <$> lookup (e', AggSum) env <*> lookup (Star emptyAnnotation, AggCount) env)
                  maybe (return e) (\(se,ce) -> return $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "/"]) [se,ce]) lOpt

                (_, _) -> mapM aux args >>= \nargs -> return $ FunCall a nm nargs

          _ -> return e


-- TODO: more expression types
stripExprQualifiers :: Qualifiers -> ScalarExpr -> SQLParseM ScalarExpr
stripExprQualifiers quals e = case e of
    (QIdentifier ann (sqlnmpath -> (scopePath -> path))) ->
      let q = foldl (maxprefix path) [] quals
          npath = maybe path id $ stripPrefix q path
      in return $ QIdentifier ann $ nmcPath npath

    (FunCall ann nm args) -> do
      nargs <- mapM (stripExprQualifiers quals) args
      return $ FunCall ann nm nargs

    _ -> return e

  where
    maxprefix p acc qual = if qual `isPrefixOf` p && length qual > length acc then qual else acc


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

adgchaseExprM :: Qualifiers -> [ADGPtr] -> ScopeId -> ScalarExpr -> SQLParseM ScalarExpr
adgchaseExprM excludes roots sid expression = do
    pathPtrs <- exprPathAttrs sid expression
    re <- chase [] expression pathPtrs
    sf <- sqclkupM sid
    debugChaseResult sf pathPtrs re $ return re
  where
    -- Invariant: pathPtrs are present in e.
    chase cyclepath e pathPtrs = do
      let remptrs = flip filter pathPtrs $ \(path,ptr) -> not (any (`isPrefixOf` path) excludes)
                                                            && ptr `notElem` roots
                                                            && ptr `notElem` cyclepath
      if null remptrs
        then return e
        else do
          pathExprAndChptrs <- mapM (\(path, ptr) -> sqglkupM ptr >>= \node -> return (path, adne node, adnch node)) remptrs
          envAndPP <- foldM accumQualified [] pathExprAndChptrs
          let (env, nextPathPtrs) = second concat $ unzip envAndPP
          ne <- substituteExpr env e
          debugChaseRcr e ne env $ chase (cyclepath ++ (map snd remptrs)) ne nextPathPtrs

    accumQualified acc (path, eOpt, chPtrs) = do
      chSids <- mapM findScopeM chPtrs >>= return . nub
      flip (maybe (return acc)) eOpt $ \e' -> do
        (ne', nPathPtrs) <- requalifyExpr chSids chPtrs e'
        return $ acc ++ [((path, ne'), nPathPtrs)]

    -- TODO: searching through the full scope env is extremely expensive; optimize.
    findScopeM p = do
      senv <- get >>= return . scopeenv
      let sidOpt = Map.foldlWithKey (findInFrame p) Nothing senv
      maybe (findErr p) return sidOpt

    findInFrame p r@(Just _) _ _ = r
    findInFrame p Nothing sid (fr, _, _) = if p `elem` Map.elems fr then Just sid else Nothing

    findErr p = throwE $ "Could not find a scope for attrptr " ++ show p

    requalifyExpr :: [ScopeId] -> [ADGPtr] -> ScalarExpr -> SQLParseM (ScalarExpr, [(AttrPath, ADGPtr)])
    requalifyExpr sids expectedPtrs e = do
      paths <- exprPaths e
      pathSPs <- foldM (\acc path -> foldM (accumSidPtr path) acc sids) Map.empty paths
      (subs, npp) <- Map.foldlWithKey (accumEnvPP expectedPtrs) (return ([], [])) pathSPs
      requalified <- substituteExpr subs e
      return (requalified, npp)

    accumSidPtr path acc sid = do
      ptrOpt <- sqcftrylkupM sid path
      return $ maybe acc (\ptr -> Map.insertWith (++) path [(sid,ptr)] acc) ptrOpt

    accumEnvPP expectedPtrs m path spl = do
      (eacc, nppacc) <- m
      case spl of
        [(sid,ptr)] | ptr `elem` expectedPtrs -> do
          (fr, _, _) <- sqclkupM sid
          let qpathOpt = Map.foldlWithKey (findMaxPath ptr) Nothing fr
          accQPath path ptr eacc nppacc qpathOpt

        [(_,ptr)] -> throwE $ "Unexpected ptr " ++ show ptr ++ " (expected: " ++ show expectedPtrs ++ ")"
        [] -> throwE $ "No scope/ptr found for " ++ show path
        _ -> throwE $ "Ambiguous scopes and ptrs found for " ++ show path ++ ": " ++ show spl

    accQPath path ptr eacc nppacc Nothing = return (eacc, nppacc)

    accQPath path ptr eacc nppacc (Just qpath) = do
      pe <- pathAsExpr qpath
      return (eacc ++ [(path, pe)], nppacc ++ [(qpath, ptr)])

    findMaxPath searchptr acc path ptr =
      if ptr == searchptr then
        maybe (Just path) (\r -> Just $ if length path > length r then path else r) acc
      else acc

    debugChaseRcr e ne env r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["CHASE"] %$ prettyLines e %$ prettyLines ne
                       %$ concatMap (\(p,e) -> [show p ++ "=>"] ++ prettyLines e) env

    debugChaseResult sf pathPtrs re r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["ADGCH " ++ show sid]
              %$ [show sf] %$ [show pathPtrs] %$ [show roots] %$ prettyLines re


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
  nodes <- mapM (adgchaseM []) ptrs
  debugBR ptrs nodes
    $ return $ nub $ catMaybes $ map (adnr . snd) $ concat nodes

  where debugBR ptrs nodes r = if not debugPlan then r else flip trace r $ boxToString $
          ["BRPTRS: " ++ show ptrs] ++
          ["BRNODES: " ++ (show $ concat nodes)]

baseRelationsS :: ScopeId -> SQLParseM [Identifier]
baseRelationsS sid = do
  ptrs <- sqcfptrsM sid
  mapM (adgchaseM []) ptrs >>= return . nub . catMaybes . map (adnr . snd) . concat

rebaseAttrsToRoots :: [ADGPtr] -> [ADGPtr] -> SQLParseM [ADGPtr]
rebaseAttrsToRoots roots ptrs = mapM (adgchaseM roots) ptrs >>= return . nub . map fst . concat

rebaseAttrs :: ScopeId -> [ADGPtr] -> SQLParseM [ADGPtr]
rebaseAttrs sid ptrs = sqclkupM sid >>= \fr -> rebaseAttrsToRoots (attrEnvPtrs fr) ptrs

rebaseExprsToRoots :: Qualifiers -> ScopeId -> [ADGPtr] -> ScalarExprList -> SQLParseM ScalarExprList
rebaseExprsToRoots excludes sid roots exprs = mapM (adgchaseExprM excludes roots sid) exprs

rebaseExprs :: ScopeId -> [ScopeId] -> ScalarExprList -> SQLParseM ScalarExprList
rebaseExprs ssid dsidl exprs = do
  frs <- mapM sqclkupM dsidl
  let ptrs = nub $ concatMap attrEnvPtrs frs
  rebaseExprsToRoots [] ssid ptrs exprs

rebaseExprsWithExcludes :: Qualifiers -> ScopeId -> [ScopeId] -> ScalarExprList -> SQLParseM ScalarExprList
rebaseExprsWithExcludes excludes ssid dsidl exprs = do
  frs <- mapM sqclkupM dsidl
  let ptrs = nub $ concatMap attrEnvPtrs frs
  rebaseExprsToRoots excludes ssid ptrs exprs

rebaseSelectItemsToRoots :: ScopeId -> [ADGPtr] -> SelectItemList -> SQLParseM SelectItemList
rebaseSelectItemsToRoots ssid roots items = mapM rebase items
  where rebase (SelExp ann e) = do
          ne <- adgchaseExprM [] roots ssid e
          case e of
            (Identifier _ (Nmc n)) | e /= ne -> return (SelectItem ann ne $ Nmc n)
            _ -> return (SelExp ann ne)

        rebase (SelectItem ann e nmc) = adgchaseExprM [] roots ssid e >>= \ne -> return (SelectItem ann ne nmc)

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

sqloptimize :: [Statement] -> SQLParseM [SQLDecl]
sqloptimize l = mapM stmt l
  where
    stmt (CreateTable _ nm attrs _) = do
      t <- sqltabletype attrs
      sqrextM (sqlnm nm) t
      return $ SQLRel (sqlnm nm, t)

    stmt (QueryStatement _ q) = do
      qcl <- query [] [] False q
      return $ SQLQuery $ qcplan qcl

    stmt s = throwE $ "Unimplemented SQL stmt: " ++ show s

    -- Simplifies toplevel aggregates, extracting complex expressions over aggregates into a subquery.
    -- TODO: lifts disjunctions into a union.
    lowerAggregates q@(Select a d s@(SelectList selectA selectL) f w g h ord l off) = do
      (prjs, aggs) <- mapM classifySelectItem selectL >>= return . partitionEithers
      case aggs of
        [] -> return q
        _ -> do
          (agges, aggfns) <- aggregateexprs aggs
          if length aggfns == length aggs
          then return q
          else do
            let aggfes = nub $ zip aggfns agges
            naggs <- mapM (\(fn,e) -> mkaggregate fn e) aggfes >>= nameSelectItemList
            nprjs <- nameSelectItemList prjs
            oprjs <- aliasSelectItemList nprjs
            env <- mapM aggSubBinding $ zip aggfes naggs
            oaggs <- substituteAggregates env aggs
            let ea = emptyAnnotation
            let nselectL = nprjs ++ naggs
            let oselectL = oprjs ++ oaggs
            let ns = SelectList selectA nselectL
            subq <- return $ Select a d ns f w g h ord l off
            let r = Select ea Dupes (SelectList ea oselectL) [SubTref ea subq $ NoAlias ea] Nothing [] Nothing [] Nothing Nothing
            trace (boxToString $ ["LOWERED"] %$ prettyLines r) $ return r

    lowerAggregates q = liftUnions q

    aggSubBinding ((fn, e), SelectItem _ _ (Nmc n)) = return ((e, fn), Identifier emptyAnnotation (Nmc n))
    aggSubBinding ((fn, e), si) = throwE $ boxToString $ ["Invalid aggregate select item"] %$ prettyLines si

    liftUnions q@(Select a d s f w g h ord l off) =
      case w of
        Just whereE -> do
          wl <- splitDisjuncts whereE
          case wl of
            []  -> return q
            [_] -> return q
            _   -> let ql = map (\nw -> Select a d s f (Just nw) g h ord l off) wl in
                   return $ foldl1 (\q1 q2 -> CombineQueryExpr emptyAnnotation Union q1 q2) ql

        Nothing -> return q

    liftUnions q = return q

    -- TODO: distinct, order, limit, offset
    -- Assumes required attributes are available immediately in the
    -- from-list (rather than in subqueries)
    query requiredPaths ePrune asCount q@(Select _ _ _ _ _ _ _ _ _ _) = do
      (Select _ _ selectL tableL whereE gbL havingE _ _ _) <- lowerAggregates q
      queryPlan requiredPaths ePrune asCount selectL tableL whereE gbL havingE

    query requiredPaths ePrune asCount q@(CombineQueryExpr _ UnionAll q1 q2) = throwE $ "TODO: union queries"

    query requiredPaths ePrune asCount q@(WithQueryExpr _ _ _) = throwE "TODO: aliased with queries"

    query _ _ _ q = throwE $ "Unhandled query " ++ show q

    queryPlan requiredPaths ePrune asCount selectL tableL whereE gbL havingE = do
      let (treeReqPaths, reqAsProjects) = case (whereE, gbL, havingE) of
                                            (Nothing, [], Nothing) -> (requiredPaths, True)
                                            (_, _, _) -> ([], False)

      talOpt <- joinTree treeReqPaths ePrune tableL
      case talOpt of
        Nothing ->
          if asCount then do
            let countSL = selectItemList [NumberLit emptyAnnotation "0"]
            (prjs, aggs, reqprjs, gsid) <- aggregatePath requiredPaths Nothing $ SelectList emptyAnnotation countSL
            let pcp = PlanCPath gsid [] [] (prjs ++ reqprjs) aggs Nothing zsubq
            return $ QueryClosure [] [] $ QueryPlan Nothing [pcp] Nothing

          else do
            (prjs, aggs, reqprjs, gsid) <- aggregatePath requiredPaths Nothing selectL
            (panl, psubqs, nprjs) <- simplifyExprSubqueries False Nothing ePrune $ projectionexprs $ prjs ++ reqprjs
            (aanl, asubqs, naggs) <- simplifyExprSubqueries False Nothing ePrune $ projectionexprs $ aggs
            let (rfvs, rcstrs) = concatQAnalysis panl aanl
            let subqs = concatSubqueries psubqs asubqs
            prjsels <- namedSelectItemList (prjs ++ reqprjs) nprjs
            aggsels <- namedSelectItemList aggs naggs
            let pcp = PlanCPath gsid [] [] prjsels aggsels Nothing subqs
            return $ QueryClosure rfvs rcstrs $ QueryPlan Nothing [pcp] Nothing

        Just (t, (fvs, cstrs)) -> do
          sid <- treeSchema t
          sqcspushM sid

          conjuncts <- maybe (return []) splitConjuncts whereE
          (cal, csubqs, nconjuncts) <- debugConjuncts conjuncts $ simplifyExprSubqueries True (Just sid) ePrune
                                                                $ filter (`notElem` ePrune) conjuncts

          -- Where-clause query decorrelation.
          -- i. We handle this prior to predicate pushdown to ensure expression replacements
          --    are correctly scoped.
          -- ii. We handle this prior to the select to ensure any decorrelated subqueries
          --     can extend the plan without having propagate schema changes.
          (nt, nsid, dsubqs) <- debugDecorrelate csubqs $
                                case csubqs of
                                  PlanCSubqueries ssubs hsubs dl | not (null dl) -> do
                                    (rt, rsidOpt) <- decorrelate ePrune t dl
                                    return (rt, maybe sid id rsidOpt, PlanCSubqueries ssubs hsubs [])

                                  PlanCSubqueries ssubs hsubs [] | not (null $ ssubs ++ hsubs) -> do
                                    ssids <- forM ssubs $ \(_, (qid, _, qcl)) -> replacementScope True qid qcl
                                    rsid <- foldM mergeScopes sid $ concat ssids
                                    return (t, rsid, PlanCSubqueries ssubs hsubs [])

                                  _ -> return (t, sid, csubqs)

          --replaceSids <- pushReplacements csubqs
          (dt, remconjuncts) <- debugPrePredPushdown nt $ predicatePushdown (Just nsid) ePrune nconjuncts nt

          --popReplacements replaceSids
          dsid <- debugPstPredPushdown dt remconjuncts $ treeSchema dt

          void $ sqcspopM
          sqcspushM dsid

          let nselectL = if asCount then SelectList emptyAnnotation $ selectItemList [countFn] else selectL
          (prjs, aggs, reqprjs, gsid) <- aggregatePath requiredPaths (Just dsid) nselectL
          if all null [remconjuncts, gbL, projectionexprs $ prjs ++ reqprjs, projectionexprs aggs]
            then do
              void $ sqcspopM
              return $ QueryClosure fvs cstrs $ QueryPlan (Just dt) [] Nothing

            else debugAsCount asCount nselectL prjs reqprjs aggs $ do
              -- Group-by pushdown
              let gbSI = selectItemList gbL ++ (if reqAsProjects then [] else reqprjs)
              (gnt, naggs) <- if null gbSI then return (dt, aggs)
                              else groupByPushdown dt dsid gbSI aggs

              gnsid <- treeSchema gnt

              -- Subquery extraction
              [gasq, pasq, aasq] <- debugGBPushdown gnt naggs $
                                      forM [gbSI, prjs ++ reqprjs, naggs] $ \si ->
                                        simplifyExprSubqueries True (Just gnsid) ePrune (projectionexprs si)

              (hal, hsubqs, nhve) <- maybe rtzal (\e -> simplifyExprSubqueries False (Just gsid) ePrune [e]) havingE

              let (al,subql,ell) = trace (let ((afvs, _),_,_) = aasq in "AASQ FVS " ++ show afvs) $ unzip3 [gasq, pasq, aasq]
              let (nfvs, ncstrs) = foldl1 concatQAnalysis $ [(fvs, cstrs), cal] ++ al ++ [hal]
              let subqs = foldl1 concatSubqueries $ [dsubqs] ++ subql ++ [hsubqs]
              let [ngbl, nprjs, nnaggs] = ell
              let nsubqs = concatSubqueries subqs hsubqs

              prjsels <- namedSelectItemList (prjs ++ reqprjs) nprjs
              aggsels <- namedSelectItemList naggs nnaggs

              let chains = [PlanCPath gsid remconjuncts ngbl prjsels aggsels
                              (if null nhve then Nothing else Just $ head nhve)
                              nsubqs]

              let plan = QueryPlan (Just gnt) chains Nothing
              nplan <- consolidatePlan plan
              void $ sqcspopM
              debugResult requiredPaths asCount $ QueryClosure nfvs ncstrs $ nplan

    debugConjuncts conjuncts r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["simplify conjuncts"] %$ concatMap prettyLines conjuncts

    debugDecorrelate subqs r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["decorrelate on"] %$ prettyLines subqs

    debugAsCount asCount (SelectList _ selectL) prjs reqprjs aggs r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["select list (count: " ++ show asCount ++ ")"]
                                     %$ concatMap prettyLines selectL
                                     %$ concatMap (\(i,l') -> [i] ++ concatMap prettyLines l')
                                          [("PRJ", prjs ++ reqprjs), ("AGG", aggs)]

    debugPrePredPushdown nt r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["pred pushdown"] %$ prettyLines nt

    debugPstPredPushdown dt remconjuncts r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["pushdown result"] %$ prettyLines dt
                                     %$ ["remconjuncts"] %$ [show remconjuncts]

    debugGBPushdown gbt aggs r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["GB pushdown result"] %$ prettyLines gbt
                                     %$ ["Root aggregates"] %$ concatMap prettyLines aggs

    debugResult requiredPaths asCount r =
      if not debugPlan then (return r) else flip trace (return r) $ boxToString $
        ["result: count " ++ show asCount] %$ ["Required: " ++ show requiredPaths] %$ prettyLines r

    joinTree _ _ [] = return Nothing
    joinTree treeReqPaths ePrune (h:t) = do
      n <- unaryNode (if null t then treeReqPaths else []) ePrune h
      (tree, tanl) <- foldM (binaryNode ePrune) n t
      return $ Just (tree, tanl)

    binaryNode ePrune (lhs, lanl) n = do
      (rhs, ranl) <- unaryNode [] ePrune n
      (lsid, rsid) <- (,) <$> treeSchema lhs <*> treeSchema rhs
      jpsid <- mergeScopes lsid rsid
      josid <- joinOutputScope jpsid "__CP" Nothing
      (lsf, rsf, jpsf, josf) <- (,,,) <$> sqclkupM lsid <*> sqclkupM rsid <*> sqclkupM jpsid <*> sqclkupM josid
      let productJoinType = Left Nothing
      let rt = Node (PJoin jpsid josid productJoinType [] [] (PlanCSubqueries [] [] []) []) [lhs, rhs]
      debugJoinScope lsid lsf rsid rsf jpsid josid jpsf josf $ return (rt, concatQAnalysis lanl ranl)

      where debugJoinScope lsid lsf rsid rsf jpsid josid jpsf josf r =
              if not debugPlan then r
              else flip trace r $ boxToString  $ ["JSF " ++ (unwords $ map show [lsid, rsid, jpsid, josid])]
                                              %$ ["JPSF", show jpsf] %$ ["JOSF", show josf]
                                              %$ ["LSF", show lsf] %$ ["RSF", show rsf]

    unaryNode _ _ n@(Tref _ nm al) = do
      let tid = sqltablealias ("__" ++ sqlnm nm) al
      t    <- sqrlkupM $ sqlnm nm
      rt   <- taliascolM al t
      tsid <- sqgextSchemaM tid rt
      sf <- sqclkupM tsid
      debugTableScope tsid sf $ return (Node (PTable (sqlnm nm) tsid (Just n) Nothing []) [], zanalysis)

      where debugTableScope sid sf r =
              if not debugPlan then r
              else flip trace r $ boxToString $ ["TSF " ++ show sid] ++ [show sf]

    unaryNode treeReqPaths ePrune (SubTref _ q al) = do
      qcl    <- query treeReqPaths ePrune False q
      nqsid  <- planSchema $ qcplan qcl
      qalsid <- aliasedOutputScope nqsid "__RN" (Just al)
      return (Node (PSubquery qalsid qcl) [], (qcfree qcl, qconstraints qcl))

    unaryNode _ ePrune (JoinTref _ jlt nat jointy jrt onE jal) = do
      (lhs, lanl)          <- unaryNode [] ePrune jlt
      (rhs, ranl)          <- unaryNode [] ePrune jrt
      (lsid, rsid)         <- (,) <$> treeSchema lhs <*> treeSchema rhs
      jpsid                <- mergeScopes lsid rsid
      (jeq, jp, panl, psq) <- joinPredicate jpsid lsid rsid ePrune onE
      josid                <- joinOutputScope jpsid "__JR" (Just jal)
      let rt = Node (PJoin jpsid josid (Left $ Just (nat,jointy)) jeq jp psq []) [lhs, rhs]
      return (rt, foldl1 concatQAnalysis [lanl, ranl, panl])

    unaryNode _ _ (FunTref _ _ _) = throwE "Table-valued functions are not supported"

    joinPredicate :: ScopeId -> ScopeId -> ScopeId -> ScalarExprList -> OnExpr
                  -> SQLParseM ([(ScalarExpr, ScalarExpr)], ScalarExprList, QueryAnalysis, PlanCSubqueries)
    joinPredicate sid lsid rsid ePrune (Just (JoinOn _ joinE)) = do
      conjuncts <- splitConjuncts joinE
      conjunctiveJoinPredicate sid lsid rsid ePrune $ filter (`notElem` ePrune) $ conjuncts

    joinPredicate sid _ _ _ (Just (JoinUsing _ nmcs)) = do
      (_, _, [[lqual], [rqual]]) <- sqclkupM sid
      let eqs = map (\i -> (QIdentifier emptyAnnotation [Nmc lqual, i], QIdentifier emptyAnnotation [Nmc rqual, i])) nmcs
      return (eqs, [], ([], []), PlanCSubqueries [] [] [])

    joinPredicate _ _ _ _ _ = return ([], [], ([], []), PlanCSubqueries [] [] [])

    conjunctiveJoinPredicate :: ScopeId -> ScopeId -> ScopeId -> ScalarExprList -> ScalarExprList
                             -> SQLParseM ([(ScalarExpr, ScalarExpr)], ScalarExprList, QueryAnalysis, PlanCSubqueries)
    conjunctiveJoinPredicate sid lsid rsid ePrune conjuncts = do
      (sepcons, nsepcons) <- classifyConjuncts sid lsid rsid conjuncts >>= return . partitionEithers
      let (lseps, rseps) = unzip sepcons
      (lcal, lsubqs, nlseps) <- simplifyExprSubqueries True (Just sid) ePrune lseps
      (rcal, rsubqs, nrseps) <- simplifyExprSubqueries True (Just sid) ePrune rseps
      (cal, subqs, nnsepcons) <- simplifyExprSubqueries True (Just sid) ePrune nsepcons
      return (zip nlseps nrseps, nnsepcons, foldl1 concatQAnalysis [lcal, rcal, cal], foldl1 concatSubqueries [lsubqs, rsubqs, subqs])

    splitConjuncts :: ScalarExpr -> SQLParseM ScalarExprList
    splitConjuncts e@(FunCall _ nm args) = do
      let fn = sqlnm nm
      case (fn, args) of
        ("!and", [x,y]) -> (++) <$> splitConjuncts x <*> splitConjuncts y
        _ -> return [e]

    splitConjuncts e = return [e]

    splitDisjuncts :: ScalarExpr -> SQLParseM ScalarExprList
    splitDisjuncts e@(FunCall _ nm args) = do
      let fn = sqlnm nm
      case (fn, args) of
        ("!or", [x,y]) -> (++) <$> splitDisjuncts x <*> splitDisjuncts y
        _ -> return [e]

    splitDisjuncts e = return [e]

    mergeConjuncts :: ScalarExprList -> SQLParseM ScalarExpr
    mergeConjuncts [] = throwE "Invalid empty conjuncts in mergeConjuncts"
    mergeConjuncts (h:t) = foldM rebuild h t
      where rebuild acc e = return $ FunCall emptyAnnotation (Name emptyAnnotation [Nmc "!and"]) [acc, e]

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
          (xquals, yquals) <- (,) <$> exprQualifiers x <*> exprQualifiers y
          (lsf@(_,_,lquals), rsf@(_,_,rquals)) <- (,) <$> sqclkupM lsid <*> sqclkupM rsid
          (xrels, yrels) <- (,) <$> baseRelationsE sid x <*> baseRelationsE sid y
          debugClassify lquals rquals xquals yquals xrels yrels
            $ classify x y lquals rquals xquals yquals xrels yrels
        _ -> return $ Right e

      where
        classify x y (nub -> lquals) (nub -> rquals) (nub -> xquals) (nub -> yquals) (nub -> xrels) (nub -> yrels)
          | xrels `intersect` lrels == xrels && yrels `intersect` rrels == yrels =
              return $ Left (x,y)

          | xrels `intersect` rrels == xrels && yrels `intersect` lrels == yrels =
              return $ Left (y,x)

          | xquals `intersect` lquals == xquals && yquals `intersect` rquals == yquals =
              return $ Left (x,y)

          | xquals `intersect` rquals == xquals && yquals `intersect` lquals == yquals =
              return $ Left (y,x)

          | otherwise = return $ Right e

        debugClassify lquals rquals xquals yquals xrels yrels r =
          if not debugPlan then r else flip trace r $ boxToString $
            ["CLCONJ"] ++ map show [lquals, rquals, xquals, yquals]
                       ++ map show [lrels, rrels, xrels, yrels]
                       ++ prettyLines e

    classifyConjunct _ _ _ _ _ e = return $ Right e

    aggregatePath :: [AttrPath] -> Maybe ScopeId -> SelectList
                  -> SQLParseM (SelectItemList, SelectItemList, SelectItemList, ScopeId)
    aggregatePath requiredPaths sidOpt (SelectList _ selectL) = do
      (prjs, aggs) <- mapM classifySelectItem selectL >>= return . partitionEithers
      deltaprjs <- ensureRequiredPaths requiredPaths sidOpt prjs
      asid <- aggregateSchema sidOpt (prjs ++ deltaprjs) aggs
      return (prjs, aggs, deltaprjs, asid)

    ensureRequiredPaths :: [AttrPath] -> Maybe ScopeId -> SelectItemList -> SQLParseM SelectItemList
    ensureRequiredPaths _ Nothing _ = throwE "Cannot process required attributes without a schema"
    ensureRequiredPaths requiredPaths (Just sid) prjs = do
        (_, prjids) <- foldM selectItemIdAcc (0, []) prjs
        sf <- sqclkupM sid
        ptrs <- mapM (lookupRequired sf) requiredPaths
        nodes <- mapM sqglkupM ptrs
        let newprjs = foldl (accum prjids) [] nodes
        return $ selectItemList newprjs

      where accum prjids nacc n = if (adnn n) `elem` prjids then nacc
                                  else nacc ++ [Identifier emptyAnnotation $ Nmc $ adnn n]

            lookupRequired sf path = maybe (err path) return $ sftrylkup sf path
            err path = throwE $ "Could not find required path: " ++ show path

    classifySelectItem :: SelectItem -> SQLParseM (Either SelectItem SelectItem)
    classifySelectItem si@(SelExp _ e) = isAggregate e >>= \agg -> return $ if agg then Right si else Left si
    classifySelectItem si@(SelectItem _ e _) = isAggregate e >>= \agg -> return $ if agg then Right si else Left si

    consolidatePlan :: QueryPlan -> SQLParseM QueryPlan
    consolidatePlan q@(QueryPlan Nothing _ _) = return q
    consolidatePlan (QueryPlan (Just t) chains stgidOpt) = do
        nt      <- consolidateTree t
        ntcsOpt <- treeChainsAndInputSchema nt
        nchains <- debugConsolidate nt $ maybe (return chains) (consolidatePlanChains chains) ntcsOpt
        nt'     <- replaceTreeChains nchains nt
        debugReplace nchains nt' $ return $ QueryPlan (Just nt') [] stgidOpt

      where
        consolidatePlanChains pchains (tchains, tsid, subqSids) =
          debugInit pchains tchains tsid subqSids $ do
            (_, _, rchains) <- consolidateChains subqSids tsid tchains pchains
            return rchains

        debugInit pchains tchains tsid subqSids r =
          if not debugPlan then r else flip trace r $
              boxToString $ ["CPC " ++ unwords [show tsid, show subqSids]]
                         %$ concatMap prettyLines pchains
                         %$ ["VS"] %$ concatMap prettyLines tchains

        debugReplace nchains t r = if not debugPlan then r else flip trace r $
            boxToString $ ["REPLACE"] %$ concatMap prettyLines nchains %$ prettyLines t

        debugConsolidate t' r = if not debugPlan then r else flip trace r $ boxToString $ ["consolidate"] %$ prettyLines t'


    consolidateTree :: PlanTree -> SQLParseM PlanTree
    consolidateTree t = foldRebuildTree rebuild () t >>= return . snd
      where rebuild _ _ (Node (PTable tid tsid trOpt bmOpt chains) _) = do
              (_, _, nchains) <- consolidateChain (tsid, tsid) chains
              return ((), Node (PTable tid tsid trOpt bmOpt nchains) [])

            rebuild _ nch (Node (PJoin psid osid jt jeq jp jpb chains) _) = do
              (_, _, nchains) <- consolidateChain (psid, psid) chains
              return ((), Node (PJoin psid osid jt jeq jp jpb nchains) nch)

            rebuild _ _ (Node (PSubquery qsid (QueryClosure fvs cstrs plan)) _) = do
              nplan <- consolidatePlan plan
              return ((), Node (PSubquery qsid (QueryClosure fvs cstrs nplan)) [])

    consolidateChains :: [ScopeId] -> ScopeId -> [PlanCPath] -> [PlanCPath] -> SQLParseM (ScopeId, ScopeId, [PlanCPath])
    consolidateChains rsids sid [] rc = consolidateChain (rsid, rsid) rc
      where rsid = if null rsids then sid else last rsids

    consolidateChains rsids sid lc rc = do
      ((_, _, nlc), (_, _, nrc)) <- (,) <$> consolidateChain (sid, sid) lc <*> consolidateChain (rsid, rsid) rc
      foldM consolidateCPath (sid, rsid, nlc) $ nrc

      where rsid = if null rsids then pcoutsid $ last lc else last rsids

    consolidateChain :: (ScopeId, ScopeId) -> [PlanCPath] -> SQLParseM (ScopeId, ScopeId, [PlanCPath])
    consolidateChain (lisid, risid) c = foldM consolidateCPath (lisid, risid, []) c

    -- Invariant: lisid must be the input scope of the last element of acc, risid must be the input scope
    -- of the current PlanCPath being processed.
    consolidateCPath :: (ScopeId, ScopeId, [PlanCPath]) -> PlanCPath -> SQLParseM (ScopeId, ScopeId, [PlanCPath])
    consolidateCPath (lisid, risid, []) c = return (risid, pcoutsid c, [c])
    consolidateCPath (lisid, risid, acc) c =
      let lc = last acc in
      if isNonAggregatePath lc
        then consolidateSelectProject (lisid, risid, init acc) lc c
        else return $ (risid, pcoutsid c, acc ++ [c])

    consolidateSelectProject :: (ScopeId, ScopeId, [PlanCPath]) -> PlanCPath -> PlanCPath -> SQLParseM (ScopeId, ScopeId, [PlanCPath])
    consolidateSelectProject (i1sid, i2sid, acc) (PlanCPath _ sel1 [] prj1 [] Nothing subq1)
                                                 (PlanCPath osid2 sel2 gb2 prj2 agg2 hv2 subq2)
      | nullSubqueries subq1 || nullSubqueries subq2
      = do
          nsel2 <- rebaseExprs i2sid [i1sid] sel2
          ngb2  <- rebaseExprs i2sid [i1sid] gb2
          nprj2 <- rebaseSelectItems i2sid [i1sid] prj2
          nagg2 <- rebaseSelectItems i2sid [i1sid] agg2
          nhv2  <- maybe (return Nothing) (rebaseSingletonOpt i2sid [i1sid]) hv2
          let r = PlanCPath osid2 (nub $ sel1 ++ nsel2) ngb2 nprj2 nagg2 nhv2 $ if nullSubqueries subq1 then subq2 else subq1
          return (i2sid, osid2, acc ++ [r])

      where rebaseSingletonOpt ssid dsidl e = do
              [ne] <- rebaseExprs ssid dsidl [e]
              return $ Just ne

    consolidateSelectProject (_, c2isid ,acc) c1 c2 = return (c2isid, pcoutsid c2, acc ++ [c1, c2])

    {-
    pushReplacements :: PlanCSubqueries -> SQLParseM [ScopeId]
    pushReplacements (PlanCSubqueries ssubs _ decorr) = do
      ssids <- forM ssubs $ \(_, (qid, replaced, qcl)) -> if replaced then replacementScope True qid qcl else return []
      dsids <- forM decorr $ \(_, (qid, replaced, RRDecorrelation _ qcl)) -> if replaced then replacementScope False qid qcl else return []
      let r = concat $ ssids ++ dsids
      forM_ r $ sqcspushM
      return r

    popReplacements sids = forM_ sids $ const $ void $ sqcspopM
    -}

    replacementScope asSingleton qid qcl = do
      qsid <- closureSchema qcl
      (t, ptrs) <- (,) <$> scopeType qsid <*> sqcfptrsM qsid
      (ids,ch,cptrs) <- telemM t >>= \elemT -> case tnc elemT of
                     (TRecord ids, ch) -> replacementFields qid asSingleton ids ch ptrs
                     (_, _) -> throwE "Invalid scope type"
      aptrs <- mapM sqgextM $ map mkNode $ zip3 ids ch cptrs
      rsid <- sqgextScopeM qid ids aptrs
      return [rsid]

      where mkNode (i, ct, cp) = ADGNode i ct (Just qid) (Just $ Identifier emptyAnnotation $ Nmc i) [cp]

    replacementFields qid True (_:_) (th:_) (ph:_) = return ([qid], [th], [ph])
    replacementFields _ _ ids ch ptrs
      | length ids == length ch && length ids == length ptrs = return (ids, ch, ptrs)
      | otherwise = throwE "Invalid type/ptr for replacementFields"


    predicatePushdown :: Maybe ScopeId -> ScalarExprList -> ScalarExprList -> PlanTree
                      -> SQLParseM (PlanTree, ScalarExprList)
    predicatePushdown Nothing _ preds jtree = return (jtree, preds)
    predicatePushdown (Just sid) ePrune preds jtree = foldM push (jtree, []) preds
      where
        push (t, remdr) p = do
          rels <- baseRelationsE sid p
          qualifiers <- exprQualifiers p
          (hasLCA, nt, accs) <- debugPush rels qualifiers p
                                  $ onRelLCA t rels qualifiers $ \x y z -> inject p x y z
          return (nt, if (not hasLCA) || any id accs then remdr ++ [p] else remdr)

        inject p qexcludes _ n@(ttag -> PTable tid tsid trOpt bmOpt chains) = do
          [np] <- rebaseExprsWithExcludes qexcludes sid [tsid] [p]
          (_, nsubqbs, [nnp]) <- simplifyExprSubqueries True (Just tsid) ePrune [np]
          return (replaceData n $ PTable tid tsid trOpt bmOpt $ pcextSelect tsid nsubqbs nnp chains, False)

        inject p qexcludes [lrels, rrels] n@(Node (PJoin psid osid jt jeq jp jpb chains) [le,re]) = do
          (lsid, rsid) <- (,) <$> treeSchema le <*> treeSchema re
          [np] <- rebaseExprsWithExcludes qexcludes sid [psid] [p]
          sepE <- debugClassify np $ classifyConjunct sid lsid rsid lrels rrels np
          (njeq, njp, nsubqs) <- case sepE of
                                    Left (lsep,rsep) -> debugSeparation "JSEPINJ" [lsep, rsep] $ do
                                      (_, lsubqs, [nlsep]) <- simplifyExprSubqueries True (Just lsid) ePrune [lsep]
                                      (_, rsubqs, [nrsep]) <- simplifyExprSubqueries True (Just rsid) ePrune [rsep]
                                      return (jeq ++ [(nlsep,nrsep)], jp, concatSubqueries lsubqs rsubqs)

                                    Right nonsep -> debugSeparation "JINJ" [nonsep] $ do
                                      (_, rsubqs, [nnonsep]) <- simplifyExprSubqueries True (Just psid) ePrune [nonsep]
                                      return (jeq, jp ++ [nnonsep], rsubqs)

          return (replaceData n $ PJoin psid osid jt njeq njp (concatSubqueries jpb nsubqs) chains, False)

        inject _ _ _ n = return (n, True)

        debugClassify np r =
          if not debugPlan then r else flip trace r $ boxToString $ ["JCLASS"] ++ prettyLines np

        debugSeparation msg seps r = if not debugPlan then r else flip trace r $
          boxToString $ [msg] ++ concatMap prettyLines seps

        debugPush rels qualifiers p r = if not debugPlan then r else flip trace r $ boxToString
                         $ ["Rels: " ++ show rels]
                        %$ ["Qualifiers: " ++ show qualifiers]
                        %$ [show p]

    onRelLCA :: PlanTree -> [Identifier] -> [[Identifier]]
             -> (Qualifiers -> [[Identifier]] -> PlanTree -> SQLParseM (PlanTree, Bool))
             -> SQLParseM (Bool, PlanTree, [Bool])
    onRelLCA t rels qualifiers f = do
        (hasLCA,_,x,y) <- foldMapTree go (False, [], [], []) t
        return (hasLCA, head x, y)

      where
        go (conc -> (True, _, nch, acc)) n = return (True, [], [replaceCh n nch], acc)

        go (conc -> (False, relsByCh@(concat -> chrels), nch, acc)) n@(ttag -> PTable _ tsid _ _ _) = do
          i <- uniqueScopeQualifier tsid
          (rsect, qsect, qdiff) <- lca (chrels ++ [i]) n
          if rsect || qsect then do
              (n', r) <- f qdiff relsByCh (replaceCh n nch)
              let (x,y,z) = if r then ([], [n'], acc++[r])
                            else (chrels++[i], [n'], acc)
              return (not r, x, y, z)
          else return (False, chrels++[i], [replaceCh n nch], acc)

        go (conc -> (False, relsByCh@(concat -> chrels), nch, acc)) n = do
          (rsect, qsect, qdiff) <- lca chrels n
          if rsect || qsect then do
              (n', r) <- f qdiff relsByCh (replaceCh n nch)
              let (x,y,z) = if r then ([], [n'], acc++[r])
                            else (chrels, [n'], acc)
              return (not r, x, y, z)
          else return (False, chrels, [replaceCh n nch], acc)

        go _ _ = throwE "onRelLCA pattern mismatch"

        lca nodeRels n = do
          sid <- lcaSchema n
          sf@(_,_,quals) <- sqclkupM sid
          let rsect = rels `intersect` nodeRels == rels
          let qsect = (not $ null qualifiers) && qualifiers `intersect` quals == qualifiers
          let qdiff = qualifiers \\ quals
          debugLCA n rels nodeRels qualifiers quals (rsect, qsect, qdiff)

        conc cl = (\(a,b,c,d) -> (any id a, b, concat c, concat d)) $ unzip4 cl

        lcaSchema (ttag -> PJoin sid _ _ _ _ _ _) = return sid
        lcaSchema (ttag -> PSubquery _ qcl) = closureSchema qcl
        lcaSchema (ttag -> PTable _ sid _ _ _) = return sid
        lcaSchema _ = throwE "Invalid plan node input for lcaSchema"

        debugLCA n a b c d (x,y,z) = if not debugPlan then return (x,y,z) else flip trace (return (x,y,z)) $ boxToString $
          ["LCA: " ++ unwords [show x, show y]] ++ [show a] ++ [show b] ++ [show c] ++ [show d] ++ prettyLines n


    simplifyExprSubqueries :: Bool -> Maybe ScopeId -> ScalarExprList -> ScalarExprList
                           -> SQLParseM (QueryAnalysis, PlanCSubqueries, ScalarExprList)
    simplifyExprSubqueries asSelectSubqueries sidOpt ePrune exprs = processMany exprs
      where
        process e@(Identifier _ (sqlnmcomponent -> i)) = onFreeVar i (idrt e) (const $ zerort e)
        process e@(QIdentifier _ (sqlnmpath -> (scopePath -> path))) = onFreePath path (idrt e) (const $ zerort e)

        process (FunCall ann nm args) =
            case sqloperator (sqlnm nm) args of
              (Just (UnaryOp ONot (Exists _ _))) -> throwE "Unsupported NOT EXISTS subquery"

              (Just (BinaryOp op x y)) | compareOp op -> do
                ((lfree, lpath), (rfree, rpath)) <- (,) <$> isFreeVarE x <*> isFreeVarE y
                if lfree || rfree
                  then processFunCstr op nm lfree lpath rfree rpath ann nm args
                  else processFunction ann nm args

              _ -> processFunction ann nm args

        process (Case _ whens elseexpr) = caseList (maybe [] (:[]) elseexpr) whens
        process (CaseSimple _ e whens elseexpr) = caseListSimple e (maybe [] (:[]) elseexpr) whens

        process (Cast ann e tn) = process e >>= \(al,sq,[ce]) -> return (al, sq, [Cast ann ce tn])
        process (Extract ann ef e) = process e >>= \(al,sq,[ce]) -> return (al, sq, [Extract ann ef ce])

        process e@(Exists _ q) = bindSubquery True existsCtor e q
        process e@(ScalarSubQuery _ q) = bindSubquery False scalarSubqueryCtor e q

        process (InPredicate ann ine isIn (InList lann el)) = do
          (eal, esubq, [nine]) <- process ine
          (elal, elsubq, nel) <- processMany el
          let (ral, rsubq) = concatAS (eal, esubq) (elal, elsubq)
          return (ral, rsubq, [InPredicate ann nine isIn $ InList lann nel])

        process (InPredicate ann ine isIn (InQueryExpr qann q)) = do
          (ial, isubqs, [nine]) <- process ine
          let ne = InPredicate ann nine isIn (InQueryExpr qann q)
          (qal, qsubqs, _) <- bindSubquery False inPredicateCtor ne q
          let (nal, nsubqs) = concatAS (ial, isubqs) (qal, qsubqs)
          return (nal, nsubqs, [ne])

        process e = zerort e

        processMany el = mapM process el >>= return . concatMany

        processFunCstr op opnm lfree lpath rfree rpath ann nm args = do
          ((fvs, cstrs), subqs, [ne]) <- processFunction ann nm args
          ncstr <- case (lfree, rfree, binaryOp ne) of
                     (True, True, Just _)            -> return $ BConstraint ne op opnm lpath rpath
                     (True,    _, Just (op', _, re)) -> return $ LConstraint ne op' opnm lpath re
                     (_,    True, Just (op', le, _)) -> return $ RConstraint ne op' opnm rpath le
                     (_, _, _)                       -> throwE "Invalid function-as-constraint expression"
          return ((fvs, cstrs++[ncstr]), subqs, [ne])

        processFunction ann nm args = do
          (al, subqs, nargs) <- processMany args
          if length args == length nargs
            then return (al, subqs, [FunCall ann nm nargs])
            else throwE $ boxToString $ ["Invalid FunCall subquery simplification, argument arity mismatch:"]
                                      %$ concatMap prettyLines args %$ ["VS"] %$ concatMap prettyLines nargs

        bindSubquery asCount eCtor e q = do
          sym <- ssqsextM
          let qid = "__subquery" ++ show sym
          qcl <- query [] ePrune asCount q
          fbvl <- debugBind qcl $ mapM partitionFree $ qcfree qcl
          let fvl = concatMap fst fbvl -- Free variables at this scope (sidOpt).
          let bvl = concatMap snd fbvl -- Variables bound here.
          processSubquery asCount eCtor e qid qcl fvl bvl q

        partitionFree path = onFreePath path (\p -> return ([p], [])) (\p -> return ([], [p]))

        debugBind qcl r = if not debugPlan then r else flip trace r $ boxToString $ ["BSQ:"] %$ prettyLines qcl

        existsCtor decorrelated _ qid qcl nqclOpt =
          let scalar = Identifier emptyAnnotation $ Nmc qid
              args le = [le, NumberLit emptyAnnotation "0"]
              eqfn le = FunCall emptyAnnotation (Name emptyAnnotation [Nmc ">"]) $ args le
          in
          case (decorrelated, nqclOpt) of
            (False, _) -> return (True, eqfn scalar)
            (True, Just nqcl) ->
              -- TODO: handle when the count aggregate is decomposed in the subquery
              countStarAttr qid nqcl >>= \el -> return (True, eqfn $ head el)

            (_, _) -> throwE $ "No rewritten query provided for exists subquery"

        scalarSubqueryCtor decorrelated _ qid qcl _ =
          if decorrelated
            then singletonAttr qid qcl >>= \el -> return (True, head el)
            else return (True, Identifier emptyAnnotation $ Nmc qid)

        inPredicateCtor _ e _ _ _ = return (False, e)

        processSubquery asCount eCtor e qid qcl fvl bvl q =
          case (fvl, bvl, qconstraints qcl) of
            ([], [], []) -> debugSubquery "Independent:" False e $ do -- Independent query.
              (replaced, ne) <- eCtor False e qid qcl Nothing
              let (ssq, hsq) = if asSelectSubqueries
                                 then ([(e, (qid, replaced, qcl))], [])
                                 else ([], [(e, (qid, replaced, qcl))])

              return (zanalysis, PlanCSubqueries ssq hsq [], [ne])

            ([], _, _) -> debugSubquery "Correlated: " False e $ do -- Immediately correlated query.
              (fcstrs, bcstrs, bpaths, prunecstrs) <- partitionConstraints qid bvl $ qconstraints qcl
              nqcl <- trace ("CSUBQ" ++ show bpaths) $ query bpaths (ePrune ++ prunecstrs) asCount q
              let dq = RRDecorrelation bcstrs nqcl
              (replaced, ne) <- eCtor True e qid qcl $ Just nqcl
              return ((fvl, fcstrs), PlanCSubqueries [] [] [(e, (qid, replaced, dq))], [ne])

            (_, _, _) -> -- Complex correlated query with variables bound here and/or above.
                         throwE $ "Complex query correlation unsupported: "
                                    ++ unwords [show $ length fvl, show $ length bvl]

        partitionConstraints qid bvl cl = foldM (accumConstraint qid bvl) ([], [], [], []) cl

        debugSubquery msg withE e r = if not debugPlan then r else flip trace r $
            boxToString $ [msg] ++ if withE then prettyLines e else []

        accumConstraint _ bvl (facc, bacc, pthacc, pracc) c@(BConstraint ce op opnm lp rp) =
          case (lp `elem` bvl, rp `elem` bvl) of
            (True, True) -> constraintAsExpr [] c >>= \e -> return (facc, bacc ++ [e], pthacc, pracc ++ [ce])
            (True, _)    -> pathAsExpr lp >>= \e -> return (facc ++ [RConstraint ce op opnm rp e], bacc, pthacc, pracc)
            (_, True)    -> pathAsExpr rp >>= \e -> return (facc ++ [LConstraint ce op opnm lp e], bacc, pthacc, pracc)
            (_, _)       -> return (facc ++ [c], bacc, pthacc, pracc)

        accumConstraint qid bvl (facc, bacc, pthacc, pracc) c@(LConstraint ce _ _ lp re) | lp `elem` bvl = do
          rps <- exprPaths re
          subs <- requalifyBindings qid rps
          e <- constraintAsExpr subs c
          return (facc, bacc ++ [e], pthacc ++ rps, pracc ++ [ce])

        accumConstraint qid bvl (facc, bacc, pthacc, pracc) c@(RConstraint ce _ _ rp le) | rp `elem` bvl = do
          lps <- exprPaths le
          subs <- requalifyBindings qid lps
          e <- constraintAsExpr subs c
          return (facc, bacc ++ [e], pthacc ++ lps, pracc ++ [ce])

        accumConstraint _ _ (facc, bacc, pthacc, pracc) c = return (facc ++ [c], bacc, pthacc, pracc)

        constraintAsExpr _ (BConstraint _ _ opnm lp rp) =
          (\nl nr -> FunCall emptyAnnotation opnm [nl,nr]) <$> pathAsExpr lp <*> pathAsExpr rp

        constraintAsExpr subs (LConstraint _ _ opnm lp re) =
          (\nl nr -> FunCall emptyAnnotation opnm [nl,nr]) <$> pathAsExpr lp <*> substituteExpr subs re

        constraintAsExpr subs (RConstraint _ _ opnm rp le) =
          (\nl nr -> FunCall emptyAnnotation opnm [nl,nr]) <$> substituteExpr subs le <*> pathAsExpr rp

        requalifyBindings qualifier paths = forM paths $ \path -> do
          e <- pathAsExpr [qualifier, pathAttribute path]
          return (path, e)

        singletonAttr qid qcl = do
          qsid <- closureSchema qcl
          outputs <- sqclkupM qsid >>= unqualifiedOutputAttrs
          case outputs of
            [upath] -> return [QIdentifier emptyAnnotation $ nmcPath (qid:upath)]
            _ -> throwE $ "Invalid singleton attribute for building result expression: " ++ show outputs

        countStarAttr qid qcl = do
            qsid <- closureSchema qcl
            nodes <- sqcfptrsM qsid >>= mapM sqglkupM
            let aggs = map adnn $ filter (isCountStar . adne) nodes
            case aggs of
              [] -> throwE $ "Could not find aggregate attribute for building result expression: " ++ show nodes
              _ -> return [QIdentifier emptyAnnotation $ nmcPath [qid, last aggs]]

          where isCountStar (Just (FunCall _ (Name _ [Nmc "count"]) [Star _])) = True
                isCountStar _ = False

        caseWhens whens = do
          rl <- mapM (\(el,e) -> concatWhens <$> processMany el <*> process e) whens
          return $ concatManyWhens rl

        concatWhens (a,b,c) (d,e,f) =
          let (na, nsq) = concatAS (a,b) (d,e) in ((na, nsq), (c, last f))

        concatManyWhens (unzip -> (aql, eles)) =
          let (na, nqs) = foldl1 concatAS aql in (na, nqs, eles)

        caseList extra whens = do
          (x,y) <- (,) <$> caseWhens whens <*> processMany extra
          mkCase x y
          where mkCase (a, b, c) (d, e, f) =
                  let (na, nsq) = concatAS (a,b) (d,e) in
                  case f of
                    [] -> return (na, nsq, [Case emptyAnnotation c Nothing])
                    [x] -> return (na, nsq, [Case emptyAnnotation c $ Just x])
                    _ -> throwE $ "Invalid caseList"


        caseListSimple extra1 extra2 whens = do
            (x,y,z) <- (,,) <$> process extra1 <*> caseWhens whens <*> processMany extra2
            mkCaseSimple x y z
          where mkCaseSimple (a,b,c) (d,e,f) (g,h,i) =
                  let (na, nsq) = concatAS (concatAS (a,b) (d,e)) (g,h) in
                  case i of
                    [] -> return (na, nsq, [CaseSimple emptyAnnotation (last c) f Nothing])
                    [x] -> return (na, nsq, [CaseSimple emptyAnnotation (last c) f $ Just x])
                    _ -> throwE $ "Invalid caseListSimple"


        binaryOp (FunCall _ nm args) = case sqloperator (sqlnm nm) args of
                                         (Just (BinaryOp op x y)) -> Just (op, x, y)
                                         _ -> Nothing

        binaryOp _ = Nothing

        isFreeVarE (Identifier _ (sqlnmcomponent -> i)) = isFreeVar i
        isFreeVarE (QIdentifier _ (sqlnmpath -> (scopePath -> path)))  = isFreePath path
        isFreeVarE _ = return (False, [])

        isFreeVar i = isFreePath [i]
        isFreePath path = trypath sidOpt (return (True, path)) (const $ return (False, path)) path

        onFreeVar i onFree onBound = onFreePath [i] onFree onBound

        onFreePath :: AttrPath -> (AttrPath -> SQLParseM a) -> (AttrPath -> SQLParseM a) -> SQLParseM a
        onFreePath path onFree onBound = do
          sfOpt <- maybe (return Nothing) (\sid -> sqclkupM sid >>= return . Just) sidOpt
          (if path == ["__aggd0"] then trace (boxToString $ ["FREEPATH"] ++ [maybe "<null>" show sfOpt]) else id)
            $ trypath sidOpt (onFree path) (const $ onBound path) path

        concatAS (a,b) (d,e) = (concatQAnalysis a d, concatSubqueries b e)
        concatResult (a,b,c) (d,e,f) = (concatQAnalysis a d, concatSubqueries b e, c++f)
        concatMany rl = if null rl then zal else foldl1 concatResult rl

        idrt :: ScalarExpr -> AttrPath -> SQLParseM (QueryAnalysis, PlanCSubqueries, ScalarExprList)
        idrt e p = return (aFreeVar p, zsubq, [e])

        zerort :: ScalarExpr -> SQLParseM (QueryAnalysis, PlanCSubqueries, ScalarExprList)
        zerort e = return (zanalysis, zsubq, [e])

        aFreeVar path = ([path], zcstr)

    countFn = FunCall emptyAnnotation (Name emptyAnnotation [Nmc "count"]) [Star emptyAnnotation]

    zanalysis = ([], zcstr)
    zcstr = []
    zsubq = PlanCSubqueries [] [] []
    zal = (zanalysis, zsubq, [])
    rtzal = return zal

    groupByPushdown :: PlanTree -> ScopeId -> SelectItemList -> SelectItemList -> SQLParseM (PlanTree, SelectItemList)
    groupByPushdown jtree s g a = walk s g a jtree
      where
        walk sid gbs aggs e@(Node n ch) = do
          let onRoot = n == ttag jtree
          continue <- debugWalk e $ trypush sid gbs aggs ch n
          case continue of
            Left doExtend -> complete onRoot doExtend gbs aggs $ Node n ch
            Right (doExtend, naggs, chsga) -> do
              nch <- mapM (\((cs,cg,ca), c) -> walk cs cg ca c >>= return . fst) $ zip chsga ch
              complete onRoot doExtend gbs naggs $ Node n nch

        trypush sid gbs aggs [lt,rt] (PJoin psid osid _ jeq jp _ chains)
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
                if debugAggDecomp psid osid jptrs aggs lgbs rgbs remgbs laggs raggs remaggs naggs $ not (null remgbs && null remaggs)
                  then return $ Left True
                  else return $ Right (True, naggs, [(lsid, lgbs, laggs), (rsid, rgbs, raggs)])

              (_,_) -> return $ Left $ null caggs

          | otherwise = return $ Left $ null $ chainAggregates osid chains

        trypush _ _ _ _ (PJoin _ _ _ _ _ _ _) = throwE "Invalid binary join node"
        trypush _ _ _ _ (PTable _ tsid _ _ chains) = return $ Left $ null $ chainAggregates tsid chains
        trypush _ _ _ _ (PSubquery _ _) = return $ Left True

        complete onRoot doExtend gbs aggs (Node n ch) = do
          n' <- refreshInputSchema n ch
          n'' <- if not onRoot && doExtend then pcextGroupBy gbs aggs n' else return n'
          debugComplete onRoot (Node n'' ch, if onRoot then aggs else [])

        debugWalk e r =
          if not debugPlan then r
          else flip trace r $ boxToString $ ["GB walk"] %$ prettyLines e

        debugAggDecomp psid osid jptrs aggs lgbs rgbs remgbs laggs raggs naggs remaggs m =
          if not debugPlan then m else flip trace m $
            unwords ["Agg decomp", show psid, show osid
                    , "GBs:", show $ length jptrs, show $ length lgbs, show $ length rgbs, show $ length remgbs
                    , "Aggs:", show $ length aggs, show $ length laggs, show $ length raggs, show $ length naggs, show $ length remaggs]

        debugComplete onRoot (n, r) = if not debugPlan then return (n,r) else flip trace (return (n,r)) $
            boxToString $ ["Completed: (onRoot " ++ show onRoot ++ ")"] %$ prettyLines n

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
          (ei, ci) <- (\x y -> ("__AGGD" ++ show x, "__AGGD" ++ show y)) <$> saggsextM <*> saggsextM
          (,,) <$> mkaggregateSI aggFn    (SelectItem emptyAnnotation e $ Nmc ei) ne
               <*> mkaggregateSI AggCount (SelectItem emptyAnnotation e $ Nmc ci) (Star ea)
               <*> combineSubAgg aggFn agg ei ci

        combineSubAgg AggSum agg ei ci =
          mkaggregateSI AggSum agg (FunCall ea (Name ea [Nmc "*"]) [Identifier ea $ Nmc ei, Identifier ea $ Nmc ci])

        combineSubAgg AggCount agg ei ci =
          mkaggregateSI AggCount agg (FunCall ea (Name ea [Nmc "*"]) [Identifier ea $ Nmc ei, Identifier ea $ Nmc ci])

        combineSubAgg AggMin agg ei ci = mkaggregateSI AggMin agg $ Identifier ea $ Nmc ei
        combineSubAgg AggMax agg ei ci = mkaggregateSI AggMax agg $ Identifier ea $ Nmc ei

        concatMapM f x = mapM f x >>= return . concat
        ea = emptyAnnotation

    decorrelate :: ScalarExprList -> PlanTree -> DecorrelatedQueries -> SQLParseM (PlanTree, Maybe ScopeId)
    decorrelate ePrune t decorr = foldM decorrgb (t, Nothing) decorr
      where
        decorrgb (accT, _) (_, (qid, _, RRDecorrelation constraints qcl)) = do
          (lsid, qsid) <- (,) <$> treeSchema accT <*> closureSchema qcl
          [rsid] <- replacementScope False qid qcl
          let dq = Node (PSubquery rsid qcl) []
          jpsid <- mergeScopes lsid rsid
          (jeq, jp, _, psq) <- conjunctiveJoinPredicate jpsid lsid rsid ePrune constraints
          josid             <- joinOutputScope jpsid "__JR" Nothing
          (rsf, qsf, jpsf, josf) <- (,,,) <$> sqclkupM rsid <*> sqclkupM qsid <*> sqclkupM jpsid <*> sqclkupM josid
          debugDecorrelation jpsf josf jeq (qsid, qsf) (rsid, rsf) $
            return (Node (PJoin jpsid josid (Left Nothing) jeq jp psq []) [accT, dq], Just jpsid)

        debugDecorrelation jpsf josf jeq (qsid, qsf) (rsid, rsf) r = if not debugPlan then r else flip trace r
          (boxToString $ ["RSID: " ++ show rsid] %$ [show rsf]
                      %$ ["QSID: " ++ show qsid] %$ [show qsf]
                      %$ ["JPSID: "] %$ [show jpsf]
                      %$ ["JOSID: "] %$ [show josf]
                      %$ ["JEQ: "] %$ [show jeq])


sqlstage :: [SQLDecl] -> SQLParseM ([SQLDecl], StageGraph)
sqlstage stmts = mapM stageStmt stmts >>= return . (concat *** concat) . unzip
  where
    stageStmt s@(SQLRel _) = return ([s], [])
    stageStmt (SQLStage _) = throwE "SQLStage called with existing stage declarations"
    stageStmt (SQLQuery plan) = do
      (nplan,l,g) <- stagePlan plan
      (nl,ng) <- stageSingleton nplan l g
      debugGraph nplan nl ng $ return (nl,ng)

    stageSingleton plan decls stgg = case (plan, decls, partitionEithers stgg) of
      (QueryPlan (Just t) [] Nothing, [], (_, [])) | isJust (treeChains t) -> do
        stgid <- stgsextM >>= return . stageId . show
        osid <- treeSchema t
        bm <- treeBindingMap t
        kt <- trace (boxToString $ ["STAGESING " ++ show osid] %$ [show bm]) $ k3ScopeType osid bm
        let squery = SQLQuery $ QueryPlan (Just t) [] (Just stgid)
        return ([SQLStage (stgid, kt), squery], stgg ++ stgEdges [stgg] stgid)

      (_, _, _) -> return (decls, stgg)

    stagePlan (QueryPlan tOpt chains stgOpt) = do
      (ntOpt, (tstages, tstgg)) <- maybe ztree (\t -> stageTree t >>= return . first Just) tOpt
      (nplan, cstages, nstgg) <- stagePlanChains tstgg ntOpt stgOpt chains
      return (nplan, tstages ++ cstages, nstgg)

      where ztree = return (Nothing, ([], []))

    stageTree jtree = (\((a,b),c) -> (c,(a,b))) <$> foldMapRebuildTree stageNode ([],[]) jtree

    stageNode (aconcat -> (acc, [lstgg,rstgg])) ch (ttag -> PJoin psid osid jt jeq jp jpb chains) = do
      stgid <- stgsextM >>= return . stageId . show
      let (jchains, schains) = nonAggregatePrefix chains
      let jtOpt = Just $ Node (PJoin psid osid jt jeq jp jpb jchains) ch
      let jplan = QueryPlan jtOpt [] (Just stgid)
      stgsid <- planSchema jplan

      let nt = Node (PTable stgid stgsid Nothing Nothing []) []
      let jstgg = stgEdges [lstgg, rstgg] stgid
      (st, nstages, nstgg) <- stageNodeChains jstgg nt schains
      let nstgg' = lstgg ++ rstgg ++ nstgg

      kt <- trace ("STAGENODE") $ k3PlanType bmelem jplan
      return ((acc ++ [SQLStage (stgid, kt), SQLQuery jplan] ++ nstages, nstgg'), st)

    stageNode (aconcat -> (acc, stgg)) ch (ttag -> PTable i tsid trOpt bmOpt chains) = do
      let (tchains, schains) = nonAggregatePrefix chains
      let nt = Node (PTable i tsid trOpt bmOpt tchains) ch
      (st, nstages, nstgg) <- stageNodeChains [Left i] nt schains
      return ((acc ++ nstages, concat stgg ++ nstgg), st)

    stageNode (aconcat -> (acc, stgg)) ch (ttag -> PSubquery osid (QueryClosure fvs cstrs plan)) = do
      (nplan, nstages, nstgg) <- stagePlan plan
      let nt = Node (PSubquery osid $ QueryClosure fvs cstrs nplan) ch
      trace (boxToString $ ["Stage Subquery"]) $ debugGraph nplan nstages nstgg $
        return ((acc ++ nstages, concat stgg ++ nstgg), nt)

    stageNode _ _ n = throwE $ boxToString $ ["Invalid tree node for staging"] %$ prettyLines n

    stagePlanChains stgg Nothing stgOpt chains = return (QueryPlan Nothing chains stgOpt, [], stgg)
    stagePlanChains stgg (Just t) stgOpt chains = do
      let (pchains, schains) = nonAggregatePrefix chains
      nt <- foldM (flip pcext) t pchains
      (st, nstages, nstgg) <- stageNodeChains stgg nt schains
      return (QueryPlan (Just st) [] stgOpt, nstages, nstgg)

    stageNodeChains stgg t' chains = foldM onPath (t',[],stgg) chains
      where onPath (t, acc, stggacc) p = do
              pt <- pcext p t
              if isNonAggregatePath p
                then return (pt, acc, stggacc)
                else do
                  stgid <- stgsextM >>= return . stageId . show
                  let pstage = SQLQuery $ QueryPlan (Just pt) [] (Just stgid)
                  osid <- treeSchema pt
                  bm <- treeBindingMap pt
                  kt <- trace (boxToString $ ["STAGENODECH " ++ show osid] %$ [show bm]) $ k3ScopeType osid bm
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

    debugGraph plan stages edges r = do
      stgstr <- sqlstringify stages
      flip trace r $ boxToString $ ["Stages"]
                                %$ prettyLines plan
                                %$ stgstr %$ [show edges]


sqlcodegen :: Bool -> ([SQLDecl], StageGraph) -> SQLParseM (K3 Declaration)
sqlcodegen distributed (stmts, stgg) = do
    (decls, inits) <- foldM debugStmt ([], []) stmts
    initDecl <- mkInit decls
    return $ DC.role "__global" $ [master] ++ decls ++ mkPeerInit inits ++ initDecl

  where
    debugStmt acc st =
      let r = cgstmt acc st
      in if not debugPlan then r
         else trace (boxToString $ ["CGStmt"] %$ prettyLines st) r

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
      (e,_,bm,merge) <- cgplan plan
      t <- trace (boxToString $ ["CGSTMTQ"] %$ [show bm]) $ k3PlanType bm plan
      (outid, trigid, decls) <- case qstageid plan of
                                  Just i -> return (i, trig i, [])
                                  Nothing -> do
                                    s  <- stgsextM >>= return . show
                                    let i = materializeId s
                                    return (i, stageId s, [DC.global i (mutT t) Nothing])

      let execStageF i e' = case lookup i edges of
                              Nothing -> return e'
                              Just next ->
                                let nextE = EC.send (EC.variable $ trig next) (EC.variable "me") EC.unit
                                    execE = EC.block [e', nextE]
                                in annotateTriggerBody i plan merge execE

      trigBodyE <- execStageF outid $ EC.assign outid e
      return (dacc ++ decls ++ [ DC.trigger trigid TC.unit $ EC.lambda "_" trigBodyE ], iacc)

    cgclosure (QueryClosure free cstrs plan)
      | null free && null cstrs = cgplan plan
      | otherwise = throwE "Code generation not supported for correlated queries"

    cgplan (QueryPlan tOpt chains _) = do
      esbmOpt <- maybe (return Nothing) (\t -> cgtree t >>= return . Just) tOpt
      cgchains esbmOpt chains

    cgtree (Node (PJoin psid osid jtE jeq jp jsubqbs chains) [l,r]) = do
      (lexpr, lsid, lbm, _) <- cgtree l
      (rexpr, rsid, rbm, _) <- cgtree r
      sf <- sqclkupM psid
      (lsf@(lfr, _, lquals), rsf@(rfr, _, rquals)) <- (,) <$> sqclkupM lsid <*> sqclkupM rsid
      (lqual, rqual) <- (\a b -> (head a, head b)) <$> maxQualifier lfr lquals <*> maxQualifier rfr rquals
      (li, ri) <- (,) <$> scopeBindIdentifier True lsid <*> scopeBindIdentifier True rsid
      cqaenv <- partitionAttrEnvByQual sf [[lqual], [rqual]] -- partitionCommonQualifedAttrEnv sf
      jbm <- bindingMap psid sf cqaenv [(lqual, Map.lookup [lqual] cqaenv, lbm), (rqual, Map.lookup [rqual] cqaenv, rbm)]
      case (jtE, jeq, jp) of
        (Right Antijoin, _, []) -> accumulatingJoin antiEquiAcc lqual lexpr lsid lbm li rqual rexpr rsid rbm ri jbm
        (Right Antijoin, _, _)  -> accumulatingJoin antiEqThetaAcc lqual lexpr lsid lbm li rqual rexpr rsid rbm ri jbm
        (Left (Just (_, LeftOuter)), (_:_), _)  -> accumulatingJoin (equiOrOuterAcc True) lqual lexpr lsid lbm li rqual rexpr rsid rbm ri jbm
        (Left (Just (_, LeftOuter)), [], _)     -> throwE "Left outer joins without equality predicates not supported"
        (Left (Just (_, RightOuter)), _, _)     -> throwE "Right outer joins not supported"
        (Left (Just (_, FullOuter)), _, _)      -> throwE "Full outer joins not supported"

        (_, (_:_), []) -> do
          (lkeyE, rkeyE) <- joinKeyFns lqual lsid lbm li rqual rsid rbm ri jeq jsubqbs
          obodyE <- concatE psid jbm Nothing
          let outE = EC.lambda lqual $ EC.lambda rqual obodyE
          joinKV <- isKVBindingMap rbm
          let joinE  = EC.applyMany (EC.project (if joinKV then "equijoin_kv" else "equijoin") lexpr) [rexpr, lkeyE, rkeyE, outE]
          cgchains (Just (joinE, osid, bmelem, Nothing)) chains

        (_, [], _) -> do
          mbodyE <- case jp of
                      [] -> bindE psid jbm Nothing $ EC.constant $ CBool True
                      (h:t) -> cgexpr jsubqbs Nothing psid h >>= \he -> foldM (cgconjunct jsubqbs psid) he t >>= \e -> bindE psid jbm Nothing e
          obodyE <- concatE psid jbm Nothing
          let mtchE  = EC.lambda lqual $ EC.lambda rqual mbodyE
          let outE   = EC.lambda lqual $ EC.lambda rqual obodyE
          joinKV <- isKVBindingMap rbm
          let joinE  = EC.applyMany (EC.project (if joinKV then "join_kv" else "join") lexpr) [rexpr, mtchE, outE]
          cgchains (Just (joinE, osid, bmelem, Nothing)) chains

        (_, _, _) -> accumulatingJoin (equiOrOuterAcc False) lqual lexpr lsid lbm li rqual rexpr rsid rbm ri jbm

      where
        equiOrOuterAcc asLeftOuter lqual _ _ _ _ rqual _ _ rbm _ jbm exprl = do
          let (h,t) = (head jp, tail jp)
          predE <- cgexpr jsubqbs Nothing psid h >>= \he -> foldM (cgconjunct jsubqbs psid) he t >>= \e -> bindE psid jbm Nothing e
          obodyE <- concatE psid jbm Nothing
          let accE = EC.lambda "acc" $ EC.lambda lqual $ EC.lambda rqual $ EC.ifThenElse predE
                        (EC.binop OSeq
                          (EC.applyMany (EC.project "insert" $ EC.variable "acc") [obodyE])
                          (EC.variable "acc"))
                        (EC.variable "acc")

          elemT <- trace (boxToString $ ["EOZET " ++ show osid] %$ [show jbm]) $ k3ScopeType osid jbm >>= telemM
          let zE = EC.empty elemT @+ EAnnotation "Collection"

          joinKV <- isKVBindingMap rbm
          let joinTransform = if asLeftOuter
                                then if joinKV then "outerequijoin_fold_kv" else "outerequijoin_fold"
                                else if joinKV then "equijoin_fold_kv" else "equijoin_fold"

          return (joinTransform, exprl ++ [accE, zE])

        antiEquiAcc lqual _ lsid lbm _ _ _ _ rbm _ jbm exprl = do
          obodyE <- concatE lsid lbm Nothing -- TODO: other joins use psid and jbm, test this.
          let accE = EC.lambda "acc" $ EC.lambda lqual $
                        (EC.binop OSeq
                          (EC.applyMany (EC.project "insert" $ EC.variable "acc") [obodyE])
                          (EC.variable "acc"))

          elemT <- trace (boxToString $ ["AEZET " ++ show osid] %$ [show jbm]) $ k3ScopeType osid jbm >>= telemM
          let zE = EC.empty elemT @+ EAnnotation "Collection"
          joinKV <- isKVBindingMap rbm
          let joinTransform = if joinKV then "antiequijoin_fold_kv" else "antiequijoin_fold"
          return (joinTransform, exprl ++ [accE, zE])

        antiEqThetaAcc lqual _ lsid lbm _ rqual _ _ rbm _ jbm exprl = do
          let (h,t) = (head jp, tail jp)
          mbodyE <- cgexpr jsubqbs Nothing psid h >>= \he -> foldM (cgconjunct jsubqbs psid) he t >>= \e -> bindE psid jbm Nothing e
          let mtchE  = EC.lambda lqual $ EC.lambda rqual mbodyE

          obodyE <- concatE lsid lbm Nothing -- TODO: other joins use psid and jbm, test this.
          let accE = EC.lambda "acc" $ EC.lambda lqual $
                        (EC.binop OSeq
                          (EC.applyMany (EC.project "insert" $ EC.variable "acc") [obodyE])
                          (EC.variable "acc"))

          elemT <- trace (boxToString $ ["AETZET " ++ show osid] %$ [show jbm]) $ k3ScopeType osid jbm >>= telemM
          let zE = EC.empty elemT @+ EAnnotation "Collection"
          joinKV <- isKVBindingMap rbm
          let joinTransform = if joinKV then "antieqthetajoin_fold_kv" else "antieqthetajoin_fold"
          return (joinTransform, exprl ++ [mtchE, accE, zE])

        accumulatingJoin accF lqual lexpr lsid lbm li rqual rexpr rsid rbm ri jbm = do
          (lkeyE, rkeyE) <- joinKeyFns lqual lsid lbm li rqual rsid rbm ri jeq jsubqbs
          (joinTransform, argsE) <- accF lqual lexpr lsid lbm li rqual rexpr rsid rbm ri jbm [rexpr, lkeyE, rkeyE]
          let joinE  = EC.applyMany (EC.project joinTransform lexpr) argsE
          cgchains (Just (joinE, osid, bmelem, Nothing)) chains

        joinKeyFns lqual lsid lbm li rqual rsid rbm ri jeq jsubqbs  = do
          let (lexprs, rexprs) = unzip jeq
          slexprs <- mapM (stripExprQualifiers [[lqual]]) lexprs
          srexprs <- mapM (stripExprQualifiers [[rqual]]) rexprs
          lkbodyE <- debugKeyFn "L" lqual lexprs slexprs $ mapM (cgexpr jsubqbs Nothing lsid) slexprs >>= \es -> bindE lsid lbm (Just li) $ tupE es
          rkbodyE <- debugKeyFn "R" rqual rexprs srexprs $ mapM (cgexpr jsubqbs Nothing rsid) srexprs >>= \es -> bindE rsid rbm (Just ri) $ tupE es
          return (EC.lambda lqual lkbodyE, EC.lambda rqual rkbodyE)

        debugKeyFn tg qual exprs sexprs r = if not debugPlan then r else flip trace r $
          boxToString $ ["JK" ++ tg ++ "F " ++ show qual] ++ concatMap (concatMap prettyLines) [exprs, sexprs]


    cgtree (Node (PSubquery osid qcl) _) = do
      (e,_,bm,mOpt) <- cgclosure qcl
      trace (boxToString $ ["SUBQBM"] %$ [show bm]) $ return (e, osid, bm, mOpt)

    cgtree (Node (PTable i tsid _ bmOpt chains) []) = cgchains (Just (EC.variable i, tsid, maybe bmelem id bmOpt, Nothing)) chains
    cgtree _ = throwE "Invalid plan tree"

    cgchains esbmOpt chains = do
      (resbmOpt, _) <- foldM cgchain (esbmOpt, False) chains
      trace (boxToString $ ["CHAINSBM"] %$ [maybe "<null>" (\(_,sid,bm,_) -> unwords ["RES", show sid, show bm]) resbmOpt]) $ maybe (throwE "Invalid chain result") return resbmOpt

    -- TODO: having subqueries, subquery asSelect
    cgchain (Just (e,sid,bm,_), True) (PlanCPath osid [] [] prjs [] Nothing subqbs) =
      cgselectlist True sid osid bm subqbs e prjs []

    cgchain (Just (e,sid,bm,_), True) (PlanCPath _ _ _ _ _ _ _) = throwE "Invalid chain component on scalar value"

    cgchain (Just (e,sid,bm,_), False) (PlanCPath osid selects gbs prjs aggs having subqbs) = do
      let selsubqs = filter (\(_, (_, replaced, _)) -> replaced) $ psselbinds subqbs

      fe <- case selects of
              [] -> return e
              l -> foldM (filterChainE sid bm subqbs) e l

      (esbmOpt, asScalar) <- case (gbs, prjs, aggs, having) of
                               ([], [], [], Nothing) -> return (Just (fe, osid, bm, Nothing), False)
                               ([], _, _, Nothing) -> cgselectlist False sid osid bm subqbs fe prjs aggs
                               (_:_, _, _, _) -> cggroupby sid osid bm subqbs fe gbs prjs aggs having >>= return . (, False)
                               _ -> throwE $ "Invalid group-by and having expression pair"

      case esbmOpt of
        Nothing -> return (esbmOpt, asScalar)
        Just (re, rsid, rbm, rmergeOpt) -> do
          nre <- foldM (letSubqueryChainE sid bm) re $ reverse selsubqs
          return (Just (nre, rsid, rbm, rmergeOpt), asScalar)

    cgchain (Nothing, _) (PlanCPath osid [] [] prjs [] Nothing subqs) = cglitselectlist subqs osid prjs
    cgchain (Nothing, _) _ = throwE "Invalid scalar chain component"

    cggroupby sid osid bm subqbs e gbs prjs aggs having = do
      i <- scopeBindIdentifier True sid
      o <- scopeBindIdentifier True osid

      gbie <- (\f -> foldM f (0::Int,[]) gbs >>= return . snd) $ \(j,acc) gbe -> do
                gbke <- cgexpr subqbs Nothing sid gbe
                case gbe of
                  (Identifier _ (Nmc n)) -> return (j, acc++[(n,gbke)])
                  _ -> return (j+1, acc++[("f" ++ show j, gbke)])

      gbbodyE <- bindE sid bm (Just i) $ case gbie of
                   [] -> EC.unit
                   [(_,e')] -> e'
                   _ -> recE gbie
      let groupF = EC.lambda i gbbodyE

      (prjsymidx, prjie) <- cgprojections subqbs 0 sid prjs
      prjt <- mapM (scalarexprType $ Just sid) $ projectionexprs prjs
      unless (all (\((_,a), (_,b)) -> compareEAST a b) $ zip prjie gbie)
        $ throwE $ boxToString $ ["Mismatched groupbys and projections"]
                              %$ ["PRJ"] %$ (concatMap prettyLines $ map snd prjie)
                              %$ ["GBS"] %$ (concatMap prettyLines $ map snd gbie)

      (_, aggie, mergeie) <- cgaggregates subqbs prjsymidx sid aggs
      aggbodyE <- bindE sid bm (Just i) $ case aggie of
                      [] -> EC.variable "acc"
                      [(_,e')] -> EC.applyMany e' [EC.variable "acc"]
                      _ -> recE $ map (aggE "acc") aggie
      let aggF = EC.lambda "acc" $ EC.lambda i $ aggbodyE

      mergeF <- case mergeie of
                  [] -> return $ EC.lambda "_" $ EC.lambda "_" $ EC.unit
                  [(_,e')] -> return $ e'
                  _ -> return $ EC.lambda "acc1" $ EC.lambda "acc2"
                              $ recE $ map (aggMergeE "acc1" "acc2") mergeie

      aggt <- mapM (aggregateType $ Just sid) $ projectionexprs aggs
      let aggit = zip (map fst aggie) aggt
      zE <- zeroE aggit

      let rE = EC.applyMany (EC.project "group_by" e) [groupF, aggF, zE]

      let prefixTypePath i' pfx l = case l of
                                      []  -> (i'+1, [(["f" ++ show i'], [pfx])])
                                      [j] -> (i', [([j], [pfx])])
                                      _   -> (i', map (\j -> ([j], [pfx, j])) l)

      let (nidx, keyPaths) = trace (boxToString $ ["PRJIE " ++ show (map fst prjie)]) $ prefixTypePath (0::Int) "key" $ map fst prjie
      let (_, valPaths) = trace (boxToString $ ["AGGIE " ++ show (map fst aggie)]) $ prefixTypePath nidx "value" $ map fst aggie
      let rbm = BMTFieldMap $ Map.fromList $ keyPaths ++ valPaths

      hve <- maybe (return Nothing) (havingE aggie osid rbm o) having
      let hrE = maybe rE (\h -> EC.applyMany (EC.project "filter" rE) [EC.lambda o h]) hve

      return $ Just (hrE, osid, rbm, Just mergeF)

      where havingE aggie osid' rbm o e' = do
              let aggei = map (\(a,b) -> (b,a)) aggie
              he <- cgexpr subqbs (Just $ subAgg aggei) osid' e'
              hbodyE <- bindE osid' rbm (Just o) $ he
              return $ Just hbodyE

            subAgg aggei e' = do
              case lookup e' aggei of
                Nothing -> return e'
                Just j -> return $ EC.variable j

    cgselectlist asScalar sid osid bm subqbs e prjs aggs = case (prjs, aggs) of
      (_, []) -> do
        i <- scopeBindIdentifier True sid
        mbodyE <- cgprojections subqbs 0 sid prjs >>= \(_, fields) -> bindE sid bm (Just i) $ recE fields
        if not asScalar then do
          let prjE = EC.applyMany (EC.project "map" e) [EC.lambda i mbodyE]
          return (Just (prjE, osid, bmelem, Nothing), False)
        else do
          let prjE = EC.applyMany (EC.lambda i mbodyE) [e]
          return (Just (prjE, osid, BMNone, Nothing), True)

      ([], _) -> do
        i <- scopeBindIdentifier True sid
        (_, aggfields, mergeie) <- cgaggregates subqbs 0 sid aggs
        aggbodyE <- bindE sid bm (Just i) $ case aggfields of
                      [] -> EC.variable "acc"
                      [(_,e')] -> EC.applyMany e' [EC.variable "acc"]
                      _ -> recE $ map (aggE "acc") aggfields
        let aggF = EC.lambda "acc" $ EC.lambda i $ aggbodyE

        mergeF <- case mergeie of
                    [] -> return $ EC.lambda "_" $ EC.lambda "_" $ EC.unit
                    [(_,e')] -> return e'
                    _ -> return $ EC.lambda "acc1" $ EC.lambda "acc2"
                                $ recE $ map (aggMergeE "acc1" "acc2") mergeie

        rElemT <- scopeType osid >>= telemM
        zE <- case tnc rElemT of
                (TRecord ids, ch) -> zeroE $ zip ids ch
                _ -> throwE "Invalid aggregate result type"

        let rexpr = EC.applyMany (EC.project "fold" e) [aggF, zE]
        return (Just (rexpr, osid, BMNone, Just mergeF), True)

      _ -> throwE $ "Invalid mutually exclusive projection-aggregate combination"

    -- TODO: we should not pass down sid here, or state assumption that sid is not used.
    cglitselectlist subqbs sid prjs = do
      (_, ide) <- cgprojections subqbs 0 sid prjs
      return (Just (recE ide, sid, BMNone, Nothing), True)

    cgprojections subqbs i sid l = foldM (cgprojection subqbs sid) (i, []) l
    cgprojection subqbs sid (i, acc) si = do
      (ni, n) <- selectItemId i si
      trace (boxToString $ ["CGPROJ " ++ n] %$ prettyLines si) $ cgaccprojection subqbs sid acc ni n $ projectionexpr si

    cgaccprojection subqbs sid acc i n e = cgexpr subqbs Nothing sid e >>= \rE -> return (i, acc ++ [(n, rE)])

    cgaggregates subqbs i sid l = foldM (cgaggregate subqbs sid) (i, [], []) l
    cgaggregate subqbs sid (i, eacc, mrgacc) si = do
      (ni, n) <- selectItemId i si
      cgaccaggregate subqbs sid eacc mrgacc ni n si

    cgaccaggregate subqbs sid eacc mrgacc i n si =
      cgaggexpr subqbs sid si >>= \(rE, mergeE) -> return (i, eacc ++ [(n, rE)], mrgacc ++ [(n, mergeE)])

    cgconjunct subqbs sid eacc e = cgexpr subqbs Nothing sid e >>= \e' -> return $ EC.binop OAnd eacc e'

    cgaggexpr subqbs sid si = do
      [(e, aggFn)] <- aggregateexpr si
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

    -- TODO: more type constructors
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
    cgexpr _ _ sid (QIdentifier _ (sqlnmpath -> (scopePath -> path))) = do
      npath <- boundAttrE sid path
      projectPathVarE npath

    cgexpr subqbs f sid (Case _ whens elseexpr) = cgcase subqbs f sid elseexpr whens
    cgexpr subqbs f sid (CaseSimple _ cexpr whens elseexpr) = cgcasesimple subqbs f sid cexpr elseexpr whens

    -- Date field extraction, assuming an integer of format: yyyymmdd
    -- TODO: more fields.
    cgexpr subqbs f sid (Extract _ xf x) = cgexpr subqbs f sid x >>= mkField xf
      where mkField y xe = case y of
                             ExtractCentury       -> return $ EC.binop ODiv xe $ EC.constant $ CInt 1000000
                             ExtractDecade        -> return $ EC.binop ODiv xe $ EC.constant $ CInt 100000
                             ExtractYear          -> return $ EC.binop ODiv xe $ EC.constant $ CInt 10000
                             ExtractQuarter       -> return $ EC.binop OMod (EC.binop ODiv xe $ EC.constant $ CInt 300)
                                                                            (EC.constant $ CInt 100)
                             ExtractMonth         -> return $ EC.binop OMod (EC.binop ODiv xe $ EC.constant $ CInt 100)
                                                                            (EC.constant $ CInt 100)
                             _ -> throwE $ "Unsupported field in extract(): " ++ show y

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

              ("!like", [x,y]) ->
                let cg a b = EC.binop OGth (EC.applyMany (EC.variable "regex_match_int") [a, b])
                                           (EC.constant $ CInt 0)
                in cg <$> cgexpr subqbs f sid x <*> cgexpr subqbs f sid y

              ("!notlike", [x,y]) ->
                let cg a b = EC.binop OLeq (EC.applyMany (EC.variable "regex_match_int") [a, b])
                                           (EC.constant $ CInt 0)
                in cg <$> cgexpr subqbs f sid x <*> cgexpr subqbs f sid y

              (_, _) -> EC.applyMany (EC.variable fn) <$> mapM (cgexpr subqbs f sid) args

      where err = throwE $ boxToString $ ["Invalid aggregate expression in cgexpr"] %$ prettyLines e

    cgexpr _ _ sid (Star _) = do
      outputs <- sqclkupM sid >>= unqualifiedOutputAttrs
      recE <$> mapM (\p -> projectPathVarE p >>= return . (pathAttribute p,)) outputs

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
    cgcasesimple subqbs f sid cexpr elseexpr whens@((_, e):_) = do
      valE  <- cgexpr subqbs f sid cexpr
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
    -- TODO: asSelectSubquery to distinguish when to pick from ssubs vs hsubs
    cgsubquery (PlanCSubqueries ssubs hsubs _) e =
      case (lookup e ssubs, lookup e hsubs) of
        (Just (_, _, qcl), Nothing) -> cgclosure qcl >>= \(r, osid, bm, _) -> return (r, osid, bm)
        (Nothing, Just (_, _, qcl)) -> cgclosure qcl >>= \(r, osid, bm, _) -> return (r, osid, bm)
        (Nothing, Nothing) -> throwE $ "Found a subquery without a binding: " ++ show e
        (Just _, Just _) -> throwE $ "Found a subquery with duplicate bindings: " ++ show e

    scopeBindIdentifier True sid = maxScopeQualifier sid >>= return . head
    scopeBindIdentifier False sid = do
      quals <- scopeQualifiers sid
      case quals of
        [[q]] -> return q
        _ -> sbindsextM >>= \j -> return $ bindId $ show j

    bindingMap sid sf cqaenv l =
      debugBindingMap sid sf cqaenv $ foldM (qualifyBindings sid sf cqaenv) (BMTVPartition Map.empty) l

    debugBindingMap sid sf cqaenv r = r >>= \m ->
      if not debugPlan then return m
      else flip trace (return m) $ boxToString $ ["FQUB " ++ show sid] %$ [show cqaenv] %$ [show sf] %$ [show m]

    qualifyBindings _ (_, outputs, _) _ (BMTVPartition acc) (qual, Just aenv, bm) = do
      f <- case bm of
             BMNone -> return $ commonNoneBinding outputs qual
             BMTPrefix i -> return $ commonPrefixBinding outputs qual i
             BMTFieldMap fb -> return $ commonFieldBinding outputs qual fb
             _ -> throwE $ boxToString $ ["Cannot qualify partitioned bindings"] ++ [show bm]
      return $ BMTVPartition $ Map.foldlWithKey f acc aenv

    qualifyBindings sid sf cqaenv _ (qual, _, _) = throwE $ boxToString $
      ["Could not find binding map for " ++ unwords [qual, "in", show sid]] %$ [show cqaenv] %$ [show sf]

    qualifyPathForBinding outputs qual path@(unqualifiedPath -> upath) = debugQPFB outputs qual path $
      let (match, rest) = partition (\o -> o `isSuffixOf` path) outputs
          ambiguous = not $ null $ filter (\o -> unqualifiedPath o == upath) rest
      in if ambiguous || null match then (qual:path) else head match

    debugQPFB outputs qual path r =
      if not debugPlan then r
      else flip trace r $ boxToString $ ["QPFB " ++ qual] %$ [show outputs] %$ [show path] %$ [show r]

    commonNoneBinding outputs qual acc path _ = Map.insert (qualifyPathForBinding outputs qual path) (qual, Nothing) acc
    commonPrefixBinding outputs qual pfx acc path _ = Map.insert (qualifyPathForBinding outputs qual path) (qual, Just $ Left pfx) acc
    commonFieldBinding outputs qual fb acc path _ = maybe acc (\typePath -> Map.insert (qualifyPathForBinding outputs qual path) (qual, Just $ Right typePath) acc) $ Map.lookup path fb

    filterChainE :: ScopeId -> BindingMap -> PlanCSubqueries -> K3 Expression -> ScalarExpr -> SQLParseM (K3 Expression)
    filterChainE sid bm subqbs eacc e = do
      i <- scopeBindIdentifier True sid
      filterE <- cgexpr subqbs Nothing sid e
      bodyE <- bindE sid bm (Just i) filterE
      return $ EC.applyMany (EC.project "filter" eacc) [EC.lambda i bodyE]

    letSubqueryChainE :: ScopeId -> BindingMap -> K3 Expression -> (ScalarExpr, (Identifier, Bool, QueryClosure)) -> SQLParseM (K3 Expression)
    letSubqueryChainE _ _ accE (e, (qid, _, qcl)) = do
      (re, _, _, _) <- cgclosure qcl
      nre <- case e of
               Exists _ _ -> testZeroE False re
               _ -> return re
      return $ (EC.letIn qid nre accE) @+ EProperty (Left ("NoBetaReduce", Nothing))

    annotateTriggerBody i (QueryPlan tOpt _ _) mergeOpt e = do
      if distributed
        then case tOpt of
               Just (isEquiJoin -> True) ->
                 return $ e @+ (EApplyGen True "DistributedHashJoin2" $ Map.fromList [("lbl", SLabel i)])

               Just (isJoin -> True) ->
                 return $ e @+ (EApplyGen True "BroadcastJoin2" $ Map.fromList [("lbl", SLabel i)])

               Just (treeChains -> Just tchains) | not (null tchains) && isGroupByAggregatePath (last tchains) ->
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

projectPathVarE :: [Identifier] -> SQLParseM (K3 Expression)
projectPathVarE [] = throwE $ "Invalid empty path for projectPathVarE"
projectPathVarE (h:t) = return $ projectPathE (EC.variable h) t

projectPathE :: K3 Expression -> [Identifier] -> K3 Expression
projectPathE e p = foldl (\accE i -> EC.project i accE) e p

fieldE :: TypeMapping -> Identifier -> K3 Expression -> K3 Expression
fieldE Nothing _ e = e
fieldE (Just (Left pfx)) i e = EC.project i $ EC.project pfx e
fieldE (Just (Right tp)) _ e = projectPathE e tp

fieldPE :: TypeMapping -> [Identifier] -> K3 Expression -> K3 Expression
fieldPE Nothing _ e = e
fieldPE (Just (Left pfx)) p e = projectPathE (EC.project pfx e) p
fieldPE (Just (Right tp)) _ e = projectPathE e tp

extendNestedRecordE :: K3 Expression -> [(Identifier, K3 Expression)] -> TypePath -> SQLParseM [(Identifier, K3 Expression)]
extendNestedRecordE _ _ [] = throwE "Invalid nested record extension"
extendNestedRecordE e fieldsAcc [i] = return $ fieldsAcc ++ [(i, e)]
extendNestedRecordE e fieldsAcc (h:rest) =
      case lookup h fieldsAcc of
        Nothing -> do
          subfields <- extendNestedRecordE e [] rest
          return $ fieldsAcc ++ [(h, recE subfields)]

        Just (tnc -> (ERecord ids, ch)) -> do
          subfields <- extendNestedRecordE e (zip ids ch) rest
          return $ map (replaceField h $ recE subfields) fieldsAcc

        Just _ -> throwE $ "Existing non-record field when attempting to extend nested record"

  where
    replaceField dst ne (src,e') | src == dst = (dst, ne)
                                 | otherwise = (src, e')

namedRecordE :: Identifier -> Map AttrPath TypePath -> [AttrPath] -> SQLParseM (K3 Expression)
namedRecordE i fb paths = foldM field [] paths >>= return . recE
  where field acc path = maybe (err path) (onTypePath acc path) $ Map.lookup path fb
        onTypePath acc path tp =
          let target = projectPathE (EC.variable i) tp
          in debugField target $ extendNestedRecordE target acc path

        debugField target m =
          if not debugPlan then m
          else m >>= \iel -> flip trace (return iel) $
                  boxToString $ ["NRE: " ++ (show $ fst $ last iel)]
                             %$ prettyLines target
                             %$ prettyLines (snd $ last iel)

        err path = throwE $ boxToString $ ["No field binding found in namedRecordE for " ++ show path] ++ [show fb]

compositeRecordE :: Map AttrPath (Identifier, TypeMapping) -> [AttrPath] -> SQLParseM (K3 Expression)
compositeRecordE pb paths = foldM field [] paths >>= return . recE
  where field acc path = maybe (err path) (onTypeMapping acc path) $ Map.lookup path pb
        onTypeMapping acc path (v,tm) = do
          let target = fieldPE tm (stripPathPrefix [v] path) $ EC.variable v
          fpath <- flattenPath path
          debugField path target $ extendNestedRecordE target acc fpath

        flattenPath [] = throwE $ "Cannot flatten empty path"
        flattenPath [x] = return [x]
        flattenPath path = return $ [last $ pathQualifier path] ++ unqualifiedPath path

        debugField path target m =
          if not debugPlan then m
          else m >>= \iel -> flip trace (return iel) $
                 boxToString $ ["CRE: " ++ show path ++ " " ++ (show $ fst $ last iel)]
                            %$ prettyLines target
                            %$ prettyLines (snd $ last iel)

        err i = throwE $ boxToString $ ["No field binding found in compositeRecordE for " ++ show i] ++ [show pb]

bindE :: ScopeId -> BindingMap -> Maybe Identifier -> K3 Expression -> SQLParseM (K3 Expression)
bindE sid bm iOpt e = do
  sf <- sqclkupM sid
  (outputs, bindings) <- outputAttrBindings sf
  let binder = BRecord $ zip bindings bindings
  case (iOpt, bm) of
    (Just i, BMNone) -> return $ EC.bindAs (EC.variable i) binder e
    (Just i, BMTPrefix j) -> return $ EC.bindAs (EC.project j $ EC.variable i) binder e
    (Just i, BMTFieldMap fb) -> do
      initE <- namedRecordE i fb outputs
      return $ EC.bindAs initE binder e

    (_, BMTVPartition pb) -> do
      initE <- compositeRecordE pb outputs
      return $ EC.bindAs initE binder e

    _ -> throwE "Invalid binding variable in bindE"

concatE :: ScopeId -> BindingMap -> Maybe Identifier -> SQLParseM (K3 Expression)
concatE sid bm iOpt = do
  outputs <- sqclkupM sid >>= outputAttrs
  case (iOpt, bm) of
    (Just i, BMNone) -> return $ EC.variable i
    (Just i, BMTPrefix j) -> return $ EC.project j $ EC.variable i
    (Just i, BMTFieldMap fb) -> namedRecordE i fb outputs
    (_, BMTVPartition pb) -> debugCRE outputs $ compositeRecordE pb outputs
    _ -> throwE "Invalid binding variable in concatE"

  where debugCRE outputs r = if not debugPlan then r else flip trace r $ boxToString $ ["CONCRE"] %$ [show outputs]

-- | Returns an output attribute for any overqualified path
--   (underqualified paths may be ambiguous and are not present in the outputs).
boundAttrE :: ScopeId -> AttrPath -> SQLParseM AttrPath
boundAttrE sid path = do
  (_, outputs, _) <- sqclkupM sid
  let r = foldl longest [] $ filter (`elem` outputs) $ tails path
  return $ if null r then path else r

  where longest acc p = if length p > length acc then p else acc


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
  outputs <- sqclkupM sid >>= unqualifiedOutputAttrs
  targetE <- matchTargetE outputs
  memoE (immutE elemexpr) $ \e -> do
      bodyE <- bindE sid bm (Just "__x") $ EC.binop OEqu targetE e
      return $ EC.applyMany (EC.project "filter" $ colexpr) [EC.lambda "__x" bodyE]

  where matchTargetE [path] = projectPathVarE path
        matchTargetE l = throwE $ "Invalid match targets: " ++ show l


memberE :: Bool -> ScopeId -> BindingMap -> K3 Expression -> K3 Expression -> SQLParseM (K3 Expression)
memberE asMem sid bm elemexpr colexpr = matchE sid bm elemexpr colexpr >>= emptyE (not asMem)

emptyE :: Bool -> K3 Expression -> SQLParseM (K3 Expression)
emptyE asEmpty colexpr = return $
  EC.binop (if asEmpty then OEqu else ONeq)
    (EC.applyMany (EC.project "size" colexpr) [EC.unit]) $ EC.constant $ CInt 0

testZeroE :: Bool -> K3 Expression -> SQLParseM (K3 Expression)
testZeroE asEq e = return $ EC.binop (if asEq then OEqu else ONeq) e $ EC.constant $ CInt 0

-- | Property construction helper
cArgsProp :: Int -> Annotation Declaration
cArgsProp i = DProperty $ Left ("CArgs", Just $ LC.int i)


{- Query plan pretty printing. -}
nshift :: [String] -> [String]
nshift = shift "+- " "|  "

tshift :: [String] -> [String]
tshift = shift "`- " "   "

prettyList :: (Pretty a) => Bool -> [a] -> [String]
prettyList _ []  = []

prettyList terminal [x] = let f = if terminal then terminalShift else nonTerminalShift
                          in "|" : (f x)

prettyList terminal l   = let f = if terminal then terminalShift else nonTerminalShift
                          in concatMap (\i -> "|" : nonTerminalShift i) (init l) ++ ["|"] ++ (f $ last l)

{- SQL expression pretty printing -}
instance Pretty ScalarExpr where
  prettyLines scalarexpr = case scalarexpr of
    AggregateFn _ distinct e edl -> ["AggregateFn " ++ show distinct]
                                        ++ nonTerminalShift e
                                        ++ concatMap (\(e',d) -> [show d] ++ nonTerminalShift e') edl

    AntiScalarExpr s -> ["AntiScalarExpr " ++ show s]
    BooleanLit _ b -> ["Boolean " ++ show b]

    Case _ whens elseexpr -> ["Case"] ++ concatMap (caseexpr False) (init whens)
                                      ++ caseexpr (isJust elseexpr) (last whens)
                                      ++ maybe [] terminalShift elseexpr

    CaseSimple _ e whens elseexpr -> ["CaseSimple"] ++ nonTerminalShift e
                                        ++ concatMap (caseexpr False) (init whens)
                                        ++ caseexpr (isJust elseexpr) (last whens)
                                        ++ maybe [] terminalShift elseexpr

    Cast _ e tn    -> ["Cast"] ++ nonTerminalShift e ++ terminalShift tn
    Exists _ q     -> ["Exists"] ++ terminalShift q
    Extract _ ef e -> ["Extract " ++ show ef] ++ terminalShift e

    FunCall _ (sqlnm -> nm) sl -> ["FunCall " ++ show nm] ++ prettyList True sl

    Identifier _ (sqlnmcomponent -> s) -> ["Id " ++ show s]

    InPredicate _ e isIn inList -> ["InPredicate " ++ show isIn]
                                     ++ nonTerminalShift e ++ terminalShift inList

    Interval _ s iv iOpt -> ["Interval " ++ unwords [s, show iv, maybe "" show iOpt]]

    LiftOperator _ opnm lft sl -> ["LiftOperator " ++ unwords [opnm, show lft]] ++ prettyList True sl

    NullLit _                                        -> ["Null"]
    NumberLit _ s                                    -> ["Number " ++ show s]
    Placeholder _                                    -> ["Placeholder"]
    PositionalArg _ i                                -> ["PositionalArg " ++ show i]
    QIdentifier _ (sqlnmpath -> (scopePath -> path)) -> ["QId " ++ show path]
    QStar _ (sqlnmcomponent -> s)                    -> ["QStar " ++ show s]
    ScalarSubQuery _ q                               -> ["ScalarSubQuery"] ++ terminalShift q
    Star _                                           -> ["Star"]
    StringLit _ s                                    -> ["String " ++ show s]
    TypedStringLit _ tn s                            -> ["TypedStringLit " ++ show s] ++ terminalShift tn

    WindowFn _ e partL orderL frame -> ["WindowFn"]
                                          ++ nonTerminalShift e
                                          ++ concatMap nonTerminalShift partL
                                          ++ concatMap (\(e',d) -> [show d] ++ nonTerminalShift e') orderL
                                          ++ [show frame]

    where caseexpr asTerminal (sl, s) =
               ["When"] ++ concatMap nonTerminalShift sl
            ++ ["Then"] ++ ((if asTerminal then terminalShift else nonTerminalShift) s)


instance Pretty TypeName where
  prettyLines tn = case tn of
    ArrayTypeName _ ctn   -> ["Array"] ++ terminalShift ctn
    Prec2TypeName _ s i j -> ["Prec2 " ++ unwords [s, show i, show j]]
    PrecTypeName _ s i    -> ["Prec" ++ unwords [s, show i]]
    SetOfTypeName _ ctn   -> ["Set"] ++ terminalShift ctn
    SimpleTypeName _ s    -> ["SimpleType " ++ show s]

instance Pretty SelectItem where
  prettyLines (SelExp _ e) = ["SelExp"] ++ terminalShift e
  prettyLines (SelectItem _ e (sqlnmcomponent -> n)) = ["SelectItem " ++ n] ++ terminalShift e

instance Pretty JoinExpr where
  prettyLines je = case je of
    JoinOn _ e -> ["JoinOn"] ++ terminalShift e
    JoinUsing _ (map sqlnmcomponent -> nl) -> ["JoinUsing " ++ intercalate "," nl]

instance Pretty TableAlias where
  prettyLines ta = case ta of
    FullAlias _ (sqlnmcomponent -> n) (map sqlnmcomponent -> nl) ->
      ["FullAlias " ++ unwords [n, intercalate "," nl]]

    NoAlias _ -> ["NoAlias"]
    TableAlias _ (sqlnmcomponent -> n) -> ["TableAlias" ++ show n]

instance Pretty TableRef where
  prettyLines tr = case tr of
    FunTref _ e tal -> ["FunTref " ++ pretty tal] ++ terminalShift e

    JoinTref _ lt nat jtyp rt jeOpt tal -> ["JoinTref " ++ unwords [show nat, show jtyp, pretty tal]]
                                              ++ maybe [] nonTerminalShift jeOpt
                                              ++ nonTerminalShift lt
                                              ++ terminalShift rt

    SubTref _ q tal -> ["SubTref " ++ pretty tal] ++ terminalShift q
    Tref _ (sqlnm -> n) tal -> ["Tref " ++ n ++ " " ++ pretty tal]

instance Pretty InList where
  prettyLines il = case il of
    InList _ sl -> ["InList"] ++ prettyList True sl
    InQueryExpr _ q -> ["InQueryExpr"] ++ terminalShift q

instance Pretty QueryExpr where
  prettyLines queryexpr = case queryexpr of
    CombineQueryExpr _ combine q1 q2 -> ["Combine " ++ show combine]
                                          ++ nonTerminalShift q1
                                          ++ terminalShift q2

    Select _ distinct (SelectList _ selectL) fromL whereL gbL havingE orderL limitOpt offsetOpt ->
      ["Select " ++ show distinct]
        ++ concatMap nonTerminalShift selectL
        ++ concatMap nonTerminalShift fromL
        ++ maybe ["<no-where>"] nonTerminalShift whereL
        ++ concatMap nonTerminalShift gbL
        ++ maybe ["<no-having>"] nonTerminalShift havingE
        ++ concatMap (\(e,d) -> [show d] ++ terminalShift e) orderL
        ++ maybe ["<no-limit>"] nonTerminalShift limitOpt
        ++ maybe ["<no-offset>"] nonTerminalShift offsetOpt

    Values _ tupleL -> ["Values"] ++ concatMap (concatMap nonTerminalShift) tupleL

    WithQueryExpr _ wql q -> concatMap nonTerminalShift wql ++ terminalShift q

instance Pretty WithQuery where
  prettyLines (WithQuery _ (sqlnmcomponent -> n) nmLOpt q) =
    ["WithQuery " ++ show n ++ maybe "" (intercalate "," . map sqlnmcomponent) nmLOpt]
      ++ terminalShift q

{- SQL AST pretty printing -}
instance Pretty ADGNode where
  prettyLines (ADGNode n t rOpt eOpt ptrs) =
    ["ADGNode " ++ unwords [n, maybe "<no-rel>" id rOpt, show ptrs]]
      ++ (maybe terminalShift (const nonTerminalShift) eOpt) t
      ++ maybe [] terminalShift eOpt

instance Pretty PlanCPath where
    prettyLines (PlanCPath sid selects gbs prjs aggs having subqs) =
      [unwords ["PlanCPath", show sid
                , "sels", show $ length selects
                , "gbys", show $ length gbs
                , "prjs", show $ length prjs
                , "aggs", show $ length aggs
                , maybe "<no having>" (const "having") having]]
      ++ (if null selects then [] else "|" : (nshift $ ["Sels"] ++ prettyList True selects))
      ++ (if null prjs    then [] else "|" : (nshift $ ["Prjs"] ++ prettyList True prjs))
      ++ (if null aggs    then [] else "|" : (nshift $ ["Aggs"] ++ prettyList True aggs))
      ++ (if null gbs     then [] else "|" : (nshift $ ["Gbys"] ++ prettyList True gbs))
      ++ maybe [] nonTerminalShift having
      ++ "|" : (terminalShift subqs)

instance Pretty PlanCSubqueries where
    prettyLines (PlanCSubqueries ssubs hsubs decorr) =
      [unwords ["PlanCSubqueries", show $ length ssubs, "ssubs"
                                 , show $ length hsubs, "hsubs"
                                 , show $ length decorr, "decorr" ]]

instance Pretty (Tree PlanNode) where
    prettyLines (Node (PJoin psid osid jt jeq jp _ chains) ch) =
      [unwords ["Join", show psid, show osid, show jt, "equalities", show $ length jeq, "preds", show $ length jp]]
        ++ "|" : prettyEqualities jeq
        ++ "|" : prettyPredicates jp
        ++ "|" : prettyList (null ch) chains ++ drawSubTrees ch

      where
        prettyEqualities epl = nshift $ ["Equalities"] ++ concatMap prettyEquality epl
        prettyEquality (x,y) = "|" : nonTerminalShift x ++ terminalShift y
        prettyPredicates l = nshift $ ["Predicates"] ++ concatMap nonTerminalShift l

    prettyLines (Node (PTable n sid _ _ chains) _) =
      [unwords ["Table", n, show sid]] ++ prettyList True chains

    prettyLines (Node (PSubquery sid qcl) _) = ["Subquery " ++ show sid, "|"] ++ (terminalShift qcl)

instance Pretty QueryClosure where
    prettyLines (QueryClosure free cstrs plan) =
        ["QueryClosure " ++ qcspec]
          ++ nshift ["Free: " ++ show free]
          ++ "|" : (terminalShift plan)
      where qcspec = unwords [show $ length free, "freevars", show $ length cstrs, "constraints"]

instance Pretty QueryPlan where
    prettyLines (QueryPlan treeOpt chains stgOpt) =
      ["QueryPlan " ++ maybe "" id stgOpt ++ "(" ++ show (length chains) ++ " chains)"]
        ++ (maybe [] treeF treeOpt) ++ prettyList True chains
      where treeF t = if null chains then "|" : (terminalShift t)
                                     else "|" : (nonTerminalShift t)

instance Pretty SQLDecl where
  prettyLines d = case d of
    SQLRel   (i, t) -> ["SQLRel " ++ show i] ++ terminalShift t
    SQLStage (i, t) -> ["SQLStage " ++ show i] ++ terminalShift t
    SQLQuery q -> ["SQLQuery"] ++ terminalShift q

