{-# LANGUAGE TemplateHaskell, TupleSections, MultiParamTypeClasses #-}

{-|
  This module contains the Template Haskell routines used to generate indexed
  set structures and their associated query types.
-}
module Language.K3.Utils.IndexedSet.TemplateHaskell
( QueryDescriptor(..)
, createIndexedSet
) where

import Control.Applicative
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set
import Language.Haskell.TH
import Language.Haskell.TH.Syntax

import Language.K3.Utils.IndexedSet.Class as ISC
import Language.K3.Utils.IndexedSet.Common
import Language.K3.Utils.TemplateHaskell.Utils

-- |A data type used to describe a query on an indexed structure.  The query has
--  the following components: a name (used in the names of the query constructor
--  as well as the structure's index), the types of the query input and the
--  query response, and an analysis function accepting an element and returning
--  a list of tuples of those inputs and outputs.  This analysis function
--  indicates which entries are added to the indices as a result of the element.
data QueryDescriptor
  = QueryDescriptor
    { queryName :: String
    , queryInputType :: Q Type
    , queryOutputType :: Q Type
    , queryAnalyzer :: Q Exp
    }

{- |Generates an indexed data structure from the appropriate program fragments.
    This routine accepts an element data type and a list of @QueryDescriptor@.
    It defines types for the data structure and query types using the specified
    names; it also creates an instance of @IndexedSet@ for these types.
-}
createIndexedSet :: String
                            -- ^The name of the indexed structure type to
                            --  generate.
                       -> String
                            -- ^The name of the query type to generate.
                       -> Q Type
                            -- ^The type of element stored in the structure.
                       -> [QueryDescriptor]
                            -- ^The descriptions of the queries to generate.
                       -> Q [Dec]
                            -- ^The declarations which generate the
                            --  aforementioned.
createIndexedSet idxStrName queryTypName elTyp descs = do
  isD <- indexedStructureDeclaration
  qtD <- queryTypeDefinition
  inD <- instanceDefinition
  return $ [isD,qtD,inD]
  where
    indexedStructureDeclaration :: Q Dec
    indexedStructureDeclaration = do
      indices <- mapM mkIndexDefinition descs
      allField <- (allFieldName idxStrName, NotStrict,) <$>
                    [t| Set $(elTyp) |] 
      return $ DataD [] (mkName idxStrName) []
        [ RecC (mkName idxStrName) $ allField:indices ]  []
      where
        mkIndexDefinition :: QueryDescriptor -> Q VarStrictType
        mkIndexDefinition desc =
          (indexName desc, NotStrict,) <$>
            [t| MultiMap $(queryInputType desc) $(queryOutputType desc) |]
    queryTypeDefinition :: Q Dec
    queryTypeDefinition = do
      v <- newName "result"
      cons <- mapM (mkQueryConstructor v) descs
      return $ DataD [] (mkName queryTypName) [PlainTV v] cons []
      where
        mkQueryConstructor :: Name -> QueryDescriptor -> Q Con
        mkQueryConstructor varName desc = do
          inType <- queryInputType desc
          outType <- queryOutputType desc
          return $ ForallC [] [EqualP (VarT varName) outType] $
            NormalC (queryConName desc) [(NotStrict, inType)]
    instanceDefinition :: Q Dec
    instanceDefinition =
        instanceD (pure [])
          (foldl appT (conT $ ''IndexedSet)
            [ conT $ mkName idxStrName
            , elTyp
            , conT $ mkName queryTypName
            ])
          [ emptyDefinition
          , insertDefinition
          , unionDefinition
          , queryDefinition
          ]
      where
        mkIdxStrP :: Name -> [Name] -> Q Pat
        mkIdxStrP allName idxNames =
          conP (mkName idxStrName) $ map varP $ allName:idxNames
        emptyDefinition :: Q Dec
        emptyDefinition =
          let emptyExpr =
                foldl appE (appE (conE $ mkName idxStrName) [|Set.empty|]) $
                  map (const [|Map.empty|]) descs
          in valD (varP 'ISC.empty) (normalB emptyExpr) []
        insertDefinition :: Q Dec
        insertDefinition = do
          indexNames <- mapM snd $ zip descs $ mkPrefixNames "idx"
          allFieldNm <- newName "f"
          elName <- newName "e"
          let elP = varP elName
          let idxStrP = mkIdxStrP allFieldNm indexNames
          let expr =
                foldl appE (appE (conE $ mkName idxStrName)
                              [|Set.insert $(varE elName)
                                           $(varE allFieldNm)|]) $
                  map (uncurry $ mkFieldExpr elName) $ zip indexNames descs
          funD 'ISC.insert $ [ clause [elP, idxStrP] (normalB expr) []]
          where
            mkFieldExpr elNm fNm desc =
              [|  let entries = $(queryAnalyzer desc) $(varE elNm) in
                  let newMap = concatMultiMap $
                                map (mapToMultiMap . uncurry Map.singleton)
                                  entries in
                  appendMultiMap $(varE fNm) newMap
              |]
        unionDefinition :: Q Dec
        unionDefinition = do
          indexNames1 <- mapM snd $ zip descs $ mkPrefixNames "i1"
          indexNames2 <- mapM snd $ zip descs $ mkPrefixNames "i2"
          allName1 <- newName "a1"
          allName2 <- newName "a2"
          let arg1P = mkIdxStrP allName1 indexNames1
          let arg2P = mkIdxStrP allName2 indexNames2
          let expr =
                foldl appE (appE (conE $ mkName idxStrName)
                              [|Set.union $(varE allName1) $(varE allName2)|]) $
                  map (\(n1,n2) ->
                        [|appendMultiMap $(varE n1) $(varE n2)|]) $
                  zip indexNames1 indexNames2
          funD 'ISC.union [clause [arg1P, arg2P] (normalB expr) []]
        queryDefinition :: Q Dec
        queryDefinition = do
          qName <- newName "q"
          indexNames <- mapM snd $ zip descs $ mkPrefixNames "i"
          allName <- newName "_a"
          let idxStrP = mkIdxStrP allName indexNames
          let expr = caseE (varE qName) $
                        map (mkMatchForQuery qName) $ zip indexNames descs
          funD 'ISC.query [clause [varP qName, idxStrP] (normalB expr) []]
          where
            mkMatchForQuery qName (idxName, desc) = do
              argName <- newName "arg"
              match (conP (queryConName desc) [varP argName])
                (normalB $
                  [|multiMapLookup $(varE argName) $(varE $ idxName)|]
                ) []
            
-- |Determines the name of the field in the record of the indexed structure
--  which contains a set of all values in the structure.
allFieldName :: String -> Name
allFieldName idxStrName = mkName $ "elementsOf" ++ idxStrName

-- |Determines the name of an index given its descriptor and the name of the
--  type in which it appears.
indexName :: QueryDescriptor -> Name
indexName desc = mkName $ "index" ++ queryName desc

-- |Determines the name of the query constructor for a given index.
queryConName :: QueryDescriptor -> Name
queryConName desc = mkName $ "Query" ++ queryName desc
