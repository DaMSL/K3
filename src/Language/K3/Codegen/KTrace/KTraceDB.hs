{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

-- | Database-backed tracing for distributed K3 programs.
--   This module generates a database schema for a K3 program trace.
--   This includes a K3 message trace as well as a table of
--   per-peer state snapshots.

module Language.K3.Codegen.KTrace.KTraceDB where

import Control.Arrow
import Data.List
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


type Globals     = [(Identifier, String)]  -- Global variable name and type
type Triggers    = [(Identifier, String)]  -- Trigger name and type
data SchemaState = SchemaState { triggers :: Triggers, globals :: Globals }

emptySchemaState :: SchemaState
emptySchemaState = SchemaState [] []

mkAttrs :: [(Identifier, String)] -> [AttributeDef]
mkAttrs = map (mk . (first $ pad 15))
  where mk (i,t) = AttributeDef eA (Nmc i) (SimpleTypeName eA t) Nothing []
        pad l s = s ++ replicate (l - length s) ' '

eA :: HA.Annotation
eA = emptyAnnotation

mkGlobalsSchema :: Globals -> Statement
mkGlobalsSchema g = CreateTable eA (Name eA [Nmc "Globals"]) (mkAttrs g) []

mkEventTraceSchema :: Triggers -> Statement
mkEventTraceSchema _ = CreateTable eA (Name eA [Nmc "Events"]) (mkAttrs logAttrs) []
  where logAttrs = [ ("time",    "date")
                   , ("peer",    "text")
                   , ("level",   "text")
                   , ("trigger", "text")
                   , ("entry",   "text") ]

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
             | otherwise = Right . Just $ "json"

mkProgramTraceSchema :: K3 Declaration -> Either String String
mkProgramTraceSchema prog = do
    (progSt, _) <- foldProgram schematize fId fId Nothing emptySchemaState prog
    return $ printStatements [ mkGlobalsSchema $ sortBy (compare `on` fst) $ globals progSt
                             , mkEventTraceSchema $ triggers progSt ]

  where fId a b = return (a,b)
        schematize st d@(tag -> DGlobal n t _) = do
          t' <- schemaType t
          return $ maybe (st,d) (\t'' -> (,d) $ st { globals  = globals st ++ [(n, t'')] }) t'

        schematize st d@(tag -> DTrigger n t _) = do
          t' <- schemaType t
          return $ maybe (st,d) (\t'' -> (,d) $ st { triggers = triggers st ++ [(n, t'')] }) t'

        schematize st d = return (st, d)

