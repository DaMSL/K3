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

dropTable :: String -> Statement
dropTable name = DropSomething eA Table IfExists [Name eA [Nmc name]] Cascade

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

mkLoader :: String -> TypedAttrs -> [String]
mkLoader name idSqlt = ["create or replace function " ++ name
                 , "(" ++ intercalate "," (map mkArg loaderParams) ++ ")"
                 , "returns void as $$"
                 , "begin"
                 , "execute format('copy " ++ resultsTable ++ " (" ++ sortedColumns idSqlt ++ ") from ''%s'' with delimiter '','' ', filename);"
                 , "end;"
                 , "$$ language plpgsql volatile;"
                 ]
  where
    mkArg (n,t) = n ++ " " ++ t
    loaderParams     = [("filename", "text")]
    loaderReturnT    = "void"
    loaderAttr       = "logEntry"
    sortedColumns    = unwords . intersperse "," . sort . map fst

mkDiff :: String -> TypedAttrs -> Either String [String]
mkDiff name idSqlt = do
  comparisons <- fmap (unwords . intersperse "and") . mapM compareField $ idSqlt
  let p1 = oneWayDiff correctResultsTable resultsTable comparisons
  let p2 = oneWayDiff resultsTable correctResultsTable comparisons
  Right $ viewHeader ++ p1 ++ ["UNION ALL"] ++ p2 ++ [";"]
  where
    a +. b = a ++ " " ++ b
    viewHeader = ["drop view if exists" +. name +. "; create view" +. name +. "as"]
    oneWayDiff left right compares = [ "SELECT * FROM" +. left +. "as l"
                                     , "WHERE NOT EXISTS"
                                     ,     "(SELECT * FROM" +. right +. "as r"
                                     ,     "WHERE" +.  compares ++ ")"]

-- TODO relative error
compareField :: (String, String) -> Either String String
compareField (field, typ) | typ == varcharType = Right $ proj "l" ++ " = " ++ proj "r"
                          | typ == intType     = Right $ proj "l" ++ " = " ++ proj "r"
                          | typ == doubleType  = Right $ abs (proj "l" ++ "-" ++ proj "r")  ++ " < .01" 
                          | otherwise = Left $ "Invalid type for result comparison: " ++ typ

  where
    proj t = t ++ "." ++ field
    abs s = "ABS(" ++ s ++ ")"

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

correctResultsTable :: String
correctResultsTable = "CorrectResults"

loadResultsFn :: String
loadResultsFn = "load_results"

loadFlatResultsFn :: Identifier -> String
loadFlatResultsFn i = "load_" ++ i

boolType :: String
boolType = "boolean"

charType :: String
charType = "char"

intType :: String
intType = "int"

doubleType :: String
doubleType = "double precision"

varcharType :: String
varcharType = "varchar"



{- SQL script construction -}
mkGlobalsSchema :: Globals -> Statement
mkGlobalsSchema attrs = createTable globalsTable $ keyAttrs ++ attrs

mkEventTraceSchema :: Triggers -> Statement
mkEventTraceSchema _ = createTable msgsTable $ keyAttrs ++ logAttrs
  where logAttrs =   [ ("trigger",     "text")
                     , ("source_peer", "text")
                     , ("contents",    "text")
                     , ("time"    ,    "text")]


mkFlatSingletonResultSchema :: Identifier -> K3 Declaration -> Either String String
mkFlatSingletonResultSchema i prog = do
  (fieldsOpt, _) <- foldProgram onGlobal fId fId Nothing Nothing prog
  case fieldsOpt of
    Just fields -> do
      idSqlT   <- mapM (\(n,t) -> schemaType t >>= maybe schemaErr (return . (n,))) fields
      loaderLines <- return $ mkLoader (loadFlatResultsFn i) idSqlT
      diffLines <- mkDiff "compute_diff" idSqlT
      ast      <- return $  printStatements [ createTable resultsTable idSqlT , createTable correctResultsTable idSqlT]
      manual   <- return $ unlines loaderLines ++ unlines diffLines
      return $ unlines [ast, manual]
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
                 , "(" , "'" ++ filePath ++ "'"
                 , ");" ]

mkProgramTraceSchema :: K3 Declaration -> Either String String
mkProgramTraceSchema prog = do
    progSt <- extractProgramState prog
    let drops = concatMap drop [globalsTable, msgsTable, resultsTable, correctResultsTable]
    let statements = printStatements [ mkGlobalsSchema $ sortBy (compare `on` fst) $ globals progSt
                                     , mkEventTraceSchema $ triggers progSt ]
    return $ drops ++ statements
    where
      drop name = "drop table if exists " ++ name ++ " cascade;\n"
{- Entry point -}
kTrace :: [(String, String)] -> K3 Declaration -> Either String String
kTrace opts prog = return . unlines =<< sequence
                     [ mkProgramTraceSchema prog
                     , onOpt "flat-result-var" (\v -> mkFlatSingletonResultSchema v prog)
                     , onOpt "files"           (\v -> mkLoadCalls (opt "flat-result-var") v) ]
  where
    onOpt key onValF = maybe (return "") onValF $ lookup key opts
    opt key = lookup key opts
