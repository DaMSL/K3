{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

module Language.K3.Parser.SQL where

import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Except
import Data.Functor.Identity

import Data.Map ( Map )
import qualified Data.Map as Map

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
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Declaration as DC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Type        as TC

import Language.K3.Utils.Pretty

data OperatorFn = UnaryOp Operator ScalarExpr
                | BinaryOp Operator ScalarExpr ScalarExpr
                deriving (Eq, Show)

-- | Relation names and types.
type RTypeEnv = Map Identifier (K3 Type)

data SQLEnv = SQLEnv { relations :: RTypeEnv }
            deriving (Eq, Show)

-- | A stateful SQL parsing monad.
type SQLParseM = ExceptT String (State SQLEnv)

-- | Type aliases
type K3ExpType = (K3 Expression, K3 Type)

{- Data.Text helpers -}

sqlenv0 :: SQLEnv
sqlenv0 = SQLEnv Map.empty

stlkup :: RTypeEnv -> Identifier -> Except String (K3 Type)
stlkup env x = maybe err return $ Map.lookup x env
  where err = throwE msg
        msg = "Unknown relation in sql parser environment: " ++ show x

stext :: RTypeEnv -> Identifier -> K3 Type -> RTypeEnv
stext env x t = Map.insert x t env

stdel :: RTypeEnv -> Identifier -> RTypeEnv
stdel env x = Map.delete x env

sqelkup :: SQLEnv -> Identifier -> Except String (K3 Type)
sqelkup senv n = stlkup (relations senv) n

sqeext :: SQLEnv -> Identifier -> K3 Type -> SQLEnv
sqeext senv n t = senv { relations = stext (relations senv) n t }

sqedel :: SQLEnv -> Identifier -> SQLEnv
sqedel senv n = senv { relations = stdel (relations senv) n }

sqelkupM :: Identifier -> SQLParseM (K3 Type)
sqelkupM n = get >>= liftExceptM . (\env -> sqelkup env n)

sqeextM :: Identifier -> K3 Type -> SQLParseM ()
sqeextM n t = get >>= \env -> return (sqeext env n t) >>= put

{- SQLParseM helpers. -}

runSQLParseM :: SQLEnv -> SQLParseM a -> (Either String a, SQLEnv)
runSQLParseM env m = flip runState env $ runExceptT m

evalSQLParseM :: SQLEnv -> SQLParseM a -> Either String a
evalSQLParseM env m = fst $ runSQLParseM env m

reasonM :: (String -> String) -> SQLParseM a -> SQLParseM a
reasonM errf = mapExceptT $ \m -> m >>= \case
  Left  err -> get >>= \env -> (return . Left $ errf "")
  Right r   -> return $ Right r

errorM :: String -> SQLParseM a
errorM msg = reasonM id $ throwE msg

liftExceptM :: Except String a -> SQLParseM a
liftExceptM = mapExceptT (return . runIdentity)

liftEitherM :: Either String a -> SQLParseM a
liftEitherM = either throwE return

k3ofsql :: FilePath -> IO ()
k3ofsql path = do
	stmtE <- parseStatementsFromFile path
	either (putStrLn . show) k3program stmtE

  where
    k3program stmts = do
      let declsM = mapM sqlstmt stmts
      either putStrLn (putStrLn . pretty . DC.role "__global") $ evalSQLParseM sqlenv0 declsM

sqlstmt :: Statement -> SQLParseM (K3 Declaration)
sqlstmt (CreateTable _ nm attrs cstrs) = do
  t <- sqltabletype attrs
  sqeextM (sqlnm nm) t
  return $ DC.global (sqlnm nm) t Nothing

sqlstmt (QueryStatement _ query) = sqlquerystmt query
sqlstmt s = throwE $ "Unimplemented SQL stmt: " ++ show s

-- TODO: declare global for results, and assign to this global.
sqlquerystmt q = do
  (expr, _) <- sqlquery q
  return $ DC.trigger "query" TC.unit $ EC.applyMany (EC.variable "ignore") [expr]


-- | Expression construction, and inlined type inference.
sqlquery :: QueryExpr -> SQLParseM K3ExpType
sqlquery q@(Select _ distinct selectL tableL whereE gbL havingE orderL limitE offsetE) = do
  tables  <- sqltablelist tableL
  selects <- sqlwhere tables whereE
  groupby <- sqlgroupby selects gbL
  having  <- sqlhaving groupby havingE
  sorted  <- sqlsort having orderL
  limited <- sqltopk sorted limitE offsetE
  sqlproject groupby selectL

sqlquery q = throwE $ "Unhandled query " ++ show q

sqltablelist :: TableRefList -> SQLParseM K3ExpType
sqltablelist [x] = sqltableexpr x
sqltablelist []  = throwE $ "Empty from clause"    -- TODO: empty collection?
sqltablelist l   = throwE $ "Multiple table list"  -- TODO: natural join?

-- TODO: aliases
-- TODO: join types and join predicate.
sqltableexpr :: TableRef -> SQLParseM K3ExpType
sqltableexpr (Tref _ nm _) = sqelkupM tnm >>= return . (EC.variable tnm,)
  where tnm = sqlnm nm

sqltableexpr t@(JoinTref _ lt nat jointy rt onE _) = do
  (le, _) <- sqltableexpr lt
  (re, _) <- sqltableexpr rt
  return . (, TC.unit) $ EC.applyMany (EC.project "join" le) [re, EC.lambda "x" $ EC.lambda "y" $ EC.constant $ CBool True]

sqltableexpr t@(SubTref _ subqE _) = throwE $ "Unhandled table ref: " ++ show t
sqltableexpr t@(FunTref _ funE _)  = throwE $ "Unhandled table ref: " ++ show t

-- TODO: argvars.
-- TODO: bind fields for expr.
sqlwhere :: K3ExpType -> MaybeBoolExpr -> SQLParseM K3ExpType
sqlwhere tables whereEOpt = flip (maybe $ return tables) whereEOpt $ \whereE -> do
  (expr, ty) <- sqlscalar (snd tables) whereE
  return . (, ty) $ EC.applyMany (EC.project "filter" $ fst tables) [EC.lambda "x" expr]

-- TODO: extract type from projections
sqlproject :: K3ExpType -> SelectList -> SQLParseM K3ExpType
sqlproject limits (SelectList _ projections) = do
  exprtypes <- mapM (sqlprojection $ snd limits) projections
  let (exprs, _) = unzip exprtypes
  return . (, TC.unit) $ EC.applyMany (EC.project "map" $ fst limits) [EC.lambda "x" $ EC.tuple exprs]

-- TODO: SelectItem name
sqlprojection :: K3 Type -> SelectItem -> SQLParseM K3ExpType
sqlprojection t (SelExp _ e)       = sqlscalar t e
sqlprojection t (SelectItem _ e _) = sqlscalar t e

-- TODO: aggregation function? Needs to extract functions from select list.
sqlgroupby :: K3ExpType -> ScalarExprList -> SQLParseM K3ExpType
sqlgroupby selects [] = return selects
sqlgroupby selects gbL = do
  gbets <- mapM (sqlscalar $ snd selects) gbL
  let (gbexprs, _) = unzip gbets
  return . (, TC.unit) $ EC.applyMany (EC.project "groupBy" $ fst selects) [EC.lambda "x" $ EC.tuple gbexprs]

-- TODO
sqlhaving :: K3ExpType -> MaybeBoolExpr -> SQLParseM K3ExpType
sqlhaving groupby havingE = return groupby

sqlsort :: K3ExpType -> ScalarExprDirectionPairList -> SQLParseM K3ExpType
sqlsort having orderL = return having

sqltopk :: K3ExpType -> MaybeBoolExpr -> MaybeBoolExpr -> SQLParseM K3ExpType
sqltopk sorted limitE offsetE = return sorted

-- TODO
sqlscalar :: K3 Type -> ScalarExpr -> SQLParseM K3ExpType
sqlscalar _ (BooleanLit _ b)  = return . (, TC.unit) $ EC.constant $ CBool b
sqlscalar _ (NumberLit _ i)   = return . (, TC.unit) $ EC.constant $ CInt $ read i  -- TODO: double?
sqlscalar _ (StringLit _ s)   = return . (, TC.unit) $ EC.constant $ CString s
sqlscalar _ (Identifier _ nc) = return . (, TC.unit) $ EC.variable $ sqlnmcomponent nc
sqlscalar t (FunCall _ nm args) = do
  let fn = sqlnm nm
  case sqloperator fn args of
    (Just (UnaryOp  o x))   -> (\a -> (EC.unop o $ fst a, snd a)) <$> sqlscalar t x
    (Just (BinaryOp o x y)) -> (\a b -> (EC.binop o (fst a) (fst b), TC.unit)) <$> sqlscalar t x <*> sqlscalar t y
    _ -> mapM (sqlscalar t) args >>= return . (, TC.unit) . EC.applyMany (EC.variable fn) . map fst

sqlscalar _ e = throwE $ "Unhandled scalar expr: " ++ show e

-- NullLit Annotation
-- Star Annotation

-- Unhandled:
-- AggregateFn Annotation Distinct ScalarExpr ScalarExprDirectionPairList
-- AntiScalarExpr String
-- Case Annotation CaseScalarExprListScalarExprPairList MaybeScalarExpr
-- CaseSimple Annotation ScalarExpr CaseScalarExprListScalarExprPairList MaybeScalarExpr
-- Cast Annotation ScalarExpr TypeName
-- Exists Annotation QueryExpr
-- Extract Annotation ExtractField ScalarExpr
-- FunCall Annotation Name ScalarExprList
-- Identifier Annotation NameComponent
-- InPredicate Annotation ScalarExpr Bool InList
-- Interval Annotation String IntervalField (Maybe Int)
-- LiftOperator Annotation String LiftFlavour ScalarExprList
-- NullLit Annotation
-- NumberLit Annotation String
-- Placeholder Annotation
-- PositionalArg Annotation Integer
-- QIdentifier Annotation [NameComponent]
-- QStar Annotation NameComponent
-- ScalarSubQuery Annotation QueryExpr
-- Star Annotation
-- StringLit Annotation String
-- TypedStringLit Annotation TypeName String
-- WindowFn Annotation ScalarExpr ScalarExprList ScalarExprDirectionPairList FrameClause


sqloperator :: String -> ScalarExprList -> Maybe OperatorFn
sqloperator "-"  [x]   = Just (UnaryOp  ONeg x)
sqloperator "+"  [x,y] = Just (BinaryOp OAdd x y)
sqloperator "-"  [x,y] = Just (BinaryOp OSub x y)
sqloperator "*"  [x,y] = Just (BinaryOp OMul x y)
sqloperator "/"  [x,y] = Just (BinaryOp ODiv x y)
sqloperator "==" [x,y] = Just (BinaryOp OEqu x y)
sqloperator "!=" [x,y] = Just (BinaryOp ONeq x y)
sqloperator "<"  [x,y] = Just (BinaryOp OLth x y)
sqloperator "<=" [x,y] = Just (BinaryOp OGeq x y)
sqloperator ">"  [x,y] = Just (BinaryOp OGth x y)
sqloperator ">=" [x,y] = Just (BinaryOp OGeq x y)
sqloperator _ _ = Nothing


-- | Type construction
sqltabletype :: AttributeDefList -> SQLParseM (K3 Type)
sqltabletype attrs = sqlrectype attrs >>= \rt -> return $ (TC.collection rt) @+ TAnnotation "Collection"

sqlrectype :: AttributeDefList -> SQLParseM (K3 Type)
sqlrectype attrs = mapM sqlattr attrs >>= return . TC.record

sqlattr :: AttributeDef -> SQLParseM (Identifier, K3 Type)
sqlattr (AttributeDef _ nm typ _ _) = sqltypename typ >>= sqltype >>= return . (sqlnmcomponent nm,)

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