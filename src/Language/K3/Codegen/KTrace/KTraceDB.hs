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
import Database.HsSqlPpp.Pretty

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
fieldPadLength = 15

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

extractProgramState :: K3 Declaration -> Either String SchemaState
extractProgramState prog = do
  (progSt, _) <- foldProgram schematize fId fId Nothing emptySchemaState prog
  return progSt
  where fId a b = return (a,b)

-- Key for joining Globals and Messages
-- Prefixed with _ to help avoid name clashes with global vars
keyAttrs :: [(String, String)]
keyAttrs = [("_mess_id", "int"), ("_dest_peer", "text")]

mkGlobalsSchema :: Globals -> Statement
mkGlobalsSchema attrs = createTable "Globals" $ keyAttrs ++ attrs

mkEventTraceSchema :: Triggers -> Statement
mkEventTraceSchema _ = createTable "Messages" $ keyAttrs ++ logAttrs
  where logAttrs =   [ ("trigger",     "text")
                     , ("source_peer", "text")
                     , ("contents",    "text")
                     , ("time"    ,    "text")]


mkResultSchema :: [Identifier] -> K3 Declaration -> Either String String
mkResultSchema vars prog = do
  progSt <- extractProgramState prog
  return $ printStatements $ [createTable "Results" $ filter (\(i,_) -> i `elem` vars) $ globals progSt]

mkFlatSingletonResultSchema :: Identifier -> K3 Declaration -> Either String String
mkFlatSingletonResultSchema i prog = do
  (fieldsOpt, _) <- foldProgram onGlobal fId fId Nothing Nothing prog
  case fieldsOpt of
    Just fields -> do
      idSqlT <- mapM (\(n,t) -> schemaType t >>= maybe schemaErr (return . (n,))) fields
      return $ printStatements $ [createTable "Results" idSqlT]
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

mkProgramTraceSchema :: K3 Declaration -> Either String String
mkProgramTraceSchema prog = do
    progSt <- extractProgramState prog
    return $ printStatements [ mkGlobalsSchema $ sortBy (compare `on` fst) $ globals progSt
                             , mkEventTraceSchema $ triggers progSt ]

kTrace :: [(String, String)] -> K3 Declaration -> Either String String
kTrace opts prog = return . unlines =<< sequence
                     [ mkProgramTraceSchema prog
                     , onOpt "flat-result-var" (\v -> mkFlatSingletonResultSchema v prog)
                     , onOpt "result-vars"     (\v -> mkResultSchema (splitOn "," v) prog) ]
  where onOpt key onValF = maybe (return "") onValF $ lookup key opts
