{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

module Language.K3.Parser.SQL where

import Control.Monad

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


k3ofsql :: FilePath -> IO ()
k3ofsql path = do
	stmtE <- parseStatementsFromFile path
	either (putStrLn . show) k3program stmtE

  where
    k3program stmts = do
      let declsE = mapM sqlstmt stmts
      either putStrLn (putStrLn . pretty . DC.role "__global") declsE

    sqlstmt :: Statement -> Either String (K3 Declaration)
    sqlstmt (CreateTable _ nm attrs cstrs) = sqltabletype attrs >>= \t -> return $ DC.global (sqlnm nm) t Nothing
    sqlstmt (QueryStatement _ query) = sqlquerystmt query
    sqlstmt s = Left $ "Unimplemented SQL stmt: " ++ show s

    sqlquerystmt q = do
      expr <- sqlquery q
      return $ DC.trigger "query" TC.unit $ EC.applyMany (EC.variable "ignore") [expr]


    -- | Expression construction
    sqlquery q@(Select _ distinct selectL tableL whereE gbL havingE orderL limitE offsetE) = do
      tables <- sqltablelist tableL
      selects <- sqlwhere tables whereE
      groupby <- sqlgroupby selects gbL
      sqlproject groupby selectL

    sqlquery q = Left $ "Unhandled query " ++ show q

    sqltablelist []  = Left $ "Empty from clause"    -- TODO: empty collection?
    sqltablelist [x] = sqltableexpr x
    sqltablelist l   = Left $ "Multiple table list"  -- TODO: natural join?

    -- TODO: aliases
    -- TODO: join types.
    sqltableexpr (Tref _ nm _) = return $ EC.variable $ sqlnm nm
    sqltableexpr t@(JoinTref _ lt nat jointy rt onE _) = do
      le <- sqltableexpr lt
      re <- sqltableexpr rt
      return $ EC.applyMany (EC.variable "join") [le, re]

    sqltableexpr t@(SubTref _ sunqE _) = Left $ "Unhandled table ref: " ++ show t
    sqltableexpr t@(FunTref _ funE _) = Left $ "Unhandled table ref: " ++ show t

    -- TODO: argvars.
    sqlwhere tables whereEOpt = flip (maybe $ return tables) whereEOpt $ \whereE -> do
      expr <- sqlscalar whereE
      return $ EC.applyMany (EC.project "filter" tables) [EC.lambda "x" expr]

    sqlproject limits (SelectList _ projections) = do
      exprs <- mapM sqlprojection projections
      return $ EC.applyMany (EC.project "map" limits) [EC.lambda "x" $ EC.tuple exprs]

    -- TODO: SelectItem name
    sqlprojection (SelExp _ e) = sqlscalar e
    sqlprojection (SelectItem _ e _) = sqlscalar e

    sqlgroupby selects gbL = do
      gbexprs <- mapM sqlscalar gbL
      return $ EC.applyMany (EC.project "groupBy" selects) [EC.lambda "x" $ EC.tuple gbexprs]

    -- TODO
    sqlscalar e = return $ EC.constant $ CInt 1

    -- | Type construction
    sqltabletype attrs = sqlrec attrs >>= \rt -> return $ (TC.collection rt) @+ TAnnotation "Collection"
    sqlrec attrs = mapM sqlattr attrs >>= return . TC.record

    sqlattr (AttributeDef _ nm typ _ _) = sqltype (sqltypename typ) >>= return . (sqlnmcomponent nm,)

    sqltype s = case s of
      "int"              -> return TC.int
      "integer"          -> return TC.int
      "real"             -> return TC.real
      "double precision" -> return TC.real
      "text"             -> return TC.string
      _ -> Left $ "Invalid K3-SQL type: " ++ s

    sqlnm (Name _ comps) = concatMap sqlnmcomponent comps
    sqlnmcomponent (Nmc s) = s
    sqlnmcomponent (QNmc s) = s

    sqltypename (SimpleTypeName _ t) = t
    sqltypename t = error $ "Invalid sql typename " ++ show t