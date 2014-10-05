{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.MultiIndex where

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Common
import Language.K3.Core.Type


import Language.K3.Codegen.CPP.Primitives (genCType)
import qualified Language.K3.Codegen.CPP.Representation as R
import Language.K3.Codegen.CPP.Types

import Control.Arrow ( (&&&) )

import Data.Functor ((<$>))
import Data.List (isInfixOf)
import Data.Maybe (catMaybes)




-- Given a list of annotations
-- Return a tuple
-- fst: List of index types to use for specializing K3::MultiIndex
-- snd: List of function definitions to attach as members for this annotation combination (lookup functions)
indexes ::  Identifier -> [(Identifier, [AnnMemDecl])] -> CPPGenM ([R.Type], [R.Definition])
indexes name ans = do
  let indexed = zip [1..] ans
  let flattened = concatMap (\(n, (i, mems)) -> zip (repeat (n,i)) mems) indexed
  index_types <- catMaybes <$> mapM index_type flattened
  --let base_name = R.Specialized ((R.Named $ R.Name "__CONTENT") : index_types) (R.Qualified (R.Name "K3") (R.Name "MultiIndex"))
  lookup_defns <- catMaybes <$> mapM lookup_fn (flattened)
  slice_defns <- catMaybes <$> mapM slice_fn (flattened)
  return (index_types, lookup_defns ++ slice_defns)
  where
    key_field = "name"
    elem_type = R.Named $ R.Name "__CONTENT"
    elem_r =  R.Name "&__CONTENT"
    bmi n = R.Qualified (R.Name "boost") (R.Qualified (R.Name "multi_index") n)

    get_key_type :: K3 Type -> CPPGenM (Maybe (R.Type))
    get_key_type ((tag &&& children) -> (TFunction, [k, _])) = Just <$> genCType k
    get_key_type _ = return Nothing

    index_type :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Type)
    index_type ((_,n), decl) = case "Index" `isInfixOf` n of
      False -> return Nothing
      True -> extract_type n decl

    -- Build a boost index type e.g. ordered_non_unique
    extract_type :: Identifier -> AnnMemDecl -> CPPGenM (Maybe R.Type)
    extract_type _ (Lifted _ _ t _ _) = do
        key_t <- get_key_type t
        let i_t k = R.Named $
                    R.Specialized
                      [ R.Named $ R.Specialized
                        [ elem_type
                        ,   k
                        , R.Named $ R.Qualified (elem_r) (R.Name key_field) --TODO extract from AST
                        ]
                        ( bmi $ R.Name "member")
                      ]
                      (bmi $ R.Name "ordered_non_unique")
        return $ maybe Nothing (Just . i_t) key_t
    extract_type _ _ = return Nothing


    -- Build a lookup function, wrapping boost 'find'
    lookup_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    lookup_fn ((i,_) ,(Lifted _ fname t _ _) ) = do

      key_t <- get_key_type t
      let this = R.Dereference $ R.Variable $ R.Name "this"

      let container = R.Call
                       (R.Project (this) (R.Name "getConstContainer") )
                       []

      let index = R.Call
                   (R.Project
                      container
                      (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get"))
                   )
                   []

      let look = R.Call
                   (R.Project (this) (R.Name "lookup_with_index") )
                   [index, R.Variable $ R.Name "key"]

      let defn k_t = R.FunctionDefn
                       (R.Name fname)
                       [("key", k_t)]
                       (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name "shared_ptr"))
                       []
                       [R.Return $ look]
      let result = maybe Nothing (Just . defn) key_t
      return $ if "lookup" `isInfixOf` fname then result else Nothing

    lookup_fn _ = return Nothing

    slice_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    slice_fn ((i,_),(Lifted _ fname t _ _) ) = do
      key_t <- get_key_type t
      let this = R.Dereference $ R.Variable $ R.Name "this"

      let container = R.Call
                       (R.Project (this) (R.Name "getConstContainer") )
                       []

      let index = R.Call
                   (R.Project
                      container
                      (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get"))
                   )
                   []

      let slice = R.Call
                   (R.Project (this) (R.Name "slice_with_index") )
                   [index, R.Variable $ R.Name "a", R.Variable $ R.Name "b"]

      let defn k_t = R.FunctionDefn
                       (R.Name fname)
                       [("a", k_t), ("b", k_t)]
                       (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name name))
                       []
                       [R.Return $ slice]
      let result = maybe Nothing (Just . defn) key_t
      return $ if "slice" `isInfixOf` fname then result else Nothing
    slice_fn _ = return Nothing


