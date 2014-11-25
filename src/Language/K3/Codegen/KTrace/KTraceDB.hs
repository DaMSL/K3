{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

-- | Database-backed tracing for distributed K3 programs.
--   This module generates a database schema for a K3 program trace.
--   This includes a K3 message trace as well as a table of
--   per-peer state snapshots.

module Language.K3.Codegen.KTrace.KTraceDB where

import Control.Arrow
import Data.List
import Data.List.Split
import Data.Function

import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Annotation hiding ( Annotation )
import qualified Database.HsSqlPpp.Annotation as HA ( Annotation )
import Database.HsSqlPpp.Parser
import Database.HsSqlPpp.Pretty

import System.IO.Unsafe ( unsafePerformIO )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Utils.Pretty

type TypedAttrs  = [(Identifier, String)]
type Globals     = TypedAttrs  -- Global variable name and type
type Triggers    = TypedAttrs  -- Trigger name and type
data SchemaState = SchemaState { triggers :: Triggers, globals :: Globals }

fieldPadLength :: Int
fieldPadLength = 18

emptySchemaState :: SchemaState
emptySchemaState = SchemaState [] []

mkAttrs :: [(Identifier, String)] -> [AttributeDef]
mkAttrs = map (mk . (first $ pad fieldPadLength))
  where mk (i,t) = AttributeDef eA (Nmc i) (SimpleTypeName eA t) Nothing []
        pad l s = s ++ replicate (l - length s) ' '

eA :: HA.Annotation
eA = emptyAnnotation

schemaType :: K3 Type -> Either String (Maybe String)
schemaType t | isTPrimitive t =
                 case tag t of
                   TBool   -> Right . Just $ "boolean"
                   TByte   -> Right . Just $ "char"
                   TInt    -> Right . Just $ "int"
                   TReal   -> Right . Just $ "double precision"
                   TNumber -> Right . Just $ "double precision"
                   TString -> Right . Just $ "varchar"
                   _       -> Left $ boxToString $ ["Invalid primitive K3 type: "] %+ prettyLines t
             | isTFunction t = Right Nothing
             | isTEndpoint t = Right Nothing
             | otherwise = Right . Just $ "json"

schematize :: SchemaState -> K3 Declaration -> Either String (SchemaState, K3 Declaration)
schematize st d@(tag -> DGlobal n t _) = do
  t' <- schemaType t
  return $ maybe (st,d) (\t'' -> (,d) $ st { globals  = globals st ++ [(n, t'')] }) t'

schematize st d@(tag -> DTrigger n t _) = do
  t' <- schemaType t
  return $ maybe (st,d) (\t'' -> (,d) $ st { triggers = triggers st ++ [(n, t'')] }) t'

schematize st d = return (st, d)

createTable :: String -> TypedAttrs -> Statement
createTable name attrs = CreateTable eA (Name eA [Nmc name]) (mkAttrs attrs) []

createFunction :: String -> [(String, String)] -> String -> FnBody -> Statement
createFunction name params returnT body =
  CreateFunction eA (Name eA [Nmc name]) fnParams fnRetT Replace Plpgsql body Stable
  where fnParams = map (\(i,t) -> ParamDef eA (Nmc i) $ SimpleTypeName eA t) params
        fnRetT   = SimpleTypeName eA returnT

createStatements :: String -> Either String [Statement]
createStatements stmts = either (Left . show) Right $ parseStatements errFile stmts
  where errFile = "sql_error.txt"

createPlpgsqlStatements :: String -> Either String [Statement]
createPlpgsqlStatements stmts = either (Left . show) Right $ parsePlpgsql errFile stmts
  where errFile = "sql_error.txt"

extractProgramState :: K3 Declaration -> Either String SchemaState
extractProgramState prog = do
  (progSt, _) <- foldProgram schematize fId fId Nothing emptySchemaState prog
  return progSt
  where fId a b = return (a,b)

mkLoader :: String -> TypedAttrs -> Either String Statement
mkLoader name idSqlT = fnStmts >>= return . createFunction name loaderParams loaderReturnT
  where
    fnStmts = createPlpgsqlStatements (loaderBody idSqlT)
                >>= return . PlpgsqlFnBody eA . Block eA Nothing []

    loaderParams     = [("table", "text"), ("filename", "text")]
    loaderReturnT    = "void"
    loaderAttr       = "logEntry"
    loaderTemp       = "temp_" ++ name
    loaderFields idT = intercalate "," $ map (\(i,_) -> loaderAttr ++ "->" ++ "''" ++ i ++ "''") idT

    loaderBody (loaderFields -> fields) = concat [
         "execute 'drop table if exists " ++ loaderTemp ++ "';"
       , "execute 'create temporary unlogged table " ++ loaderTemp ++ " ( " ++ loaderAttr ++ " json )';"
       , "execute format('copy %s from ''%s''', table, filename);"
       , "execute format('insert into %s select " ++ fields ++ " from " ++ loaderTemp ++ "', table);" ]

{- Constants -}
-- Key for joining Globals and Messages
-- Prefixed with _ to help avoid name clashes with global vars
keyAttrs :: [(String, String)]
keyAttrs = [("_mess_id", "int"), ("_dest_peer", "text")]

globalsTable :: String
globalsTable = "Globals"

msgsTable :: String
msgsTable = "Messages"

resultsTable :: String
resultsTable = "Results"

loadResultsFn :: String
loadResultsFn = "load_results"

loadFlatResultsFn :: Identifier -> String
loadFlatResultsFn i = "load_" ++ i

{- SQL script construction -}
mkGlobalsSchema :: Globals -> Statement
mkGlobalsSchema attrs = createTable globalsTable $ keyAttrs ++ attrs

mkEventTraceSchema :: Triggers -> Statement
mkEventTraceSchema _ = createTable msgsTable $ keyAttrs ++ logAttrs
  where logAttrs =   [ ("trigger",     "text")
                     , ("source_peer", "text")
                     , ("contents",    "text")
                     , ("time"    ,    "text")]


mkResultSchema :: [Identifier] -> K3 Declaration -> Either String String
mkResultSchema vars prog = do
  progSt   <- extractProgramState prog
  varAttrs <- return $ filter (\(i,_) -> i `elem` vars) $ globals progSt
  loaderFn <- mkLoader loadResultsFn varAttrs
  return $ printStatements $ [createTable resultsTable varAttrs, loaderFn]

mkFlatSingletonResultSchema :: Identifier -> K3 Declaration -> Either String String
mkFlatSingletonResultSchema i prog = do
  (fieldsOpt, _) <- foldProgram onGlobal fId fId Nothing Nothing prog
  case fieldsOpt of
    Just fields -> do
      idSqlT   <- mapM (\(n,t) -> schemaType t >>= maybe schemaErr (return . (n,))) fields
      loaderFn <- mkLoader (loadFlatResultsFn i) idSqlT
      return $ printStatements [ createTable resultsTable idSqlT, loaderFn ]

    Nothing -> noIdFoundErr

  where fId a b = return (a,b)
        onGlobal acc d@(tag -> DGlobal n (tnc -> (TCollection,[(getFlatRecord -> Just fields)])) _)
          | n == i = return (Just fields, d)
          | otherwise = return (acc, d)
        onGlobal acc d = return (acc, d)

        getFlatRecord (tnc -> (TRecord ids, ch)) = if all isTPrimitive ch
                                                   then Just $ zip ids ch else Nothing
        getFlatRecord _ = Nothing

        schemaErr = Left $
          "Invalid flat record field type construction while creating result schema"

        noIdFoundErr = Left $ unwords
          ["Could not find a flat collection named ", i, "while creating result schema"]

-- TOOD: lift to IO monad
mkLoadCalls :: Maybe Identifier -> String -> Either String String
mkLoadCalls iOpt catalogFile = do
    let contents = lines $ unsafePerformIO (readFile catalogFile)
    stmts <- createStatements $ unlines [mkLoaderCall filePath | filePath <- contents ]
    return $ printStatements stmts

  where
    mkLoaderCall filePath = unwords [
        "select" , (maybe loadResultsFn loadFlatResultsFn iOpt)
                 , "(" , "'" ++ resultsTable ++ "'"
                 , "," , "'" ++ filePath ++ "'"
                 , ");" ]

mkProgramTraceSchema :: K3 Declaration -> Either String String
mkProgramTraceSchema prog = do
    progSt <- extractProgramState prog
    return $ printStatements [ mkGlobalsSchema $ sortBy (compare `on` fst) $ globals progSt
                             , mkEventTraceSchema $ triggers progSt ]

{- Entry point -}
kTrace :: [(String, String)] -> K3 Declaration -> Either String String
kTrace opts prog = return . unlines =<< sequence
                     [ mkProgramTraceSchema prog
                     , onOpt "flat-result-var" (\v -> mkFlatSingletonResultSchema v prog)
                     , onOpt "result-vars"     (\v -> mkResultSchema (splitOn "," v) prog)
                     , onOpt "files"           (\v -> mkLoadCalls (opt "flat-result-var") v) ]
  where
    onOpt key onValF = maybe (return "") onValF $ lookup key opts
    opt key = lookup key opts
