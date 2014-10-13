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
import Data.List (isInfixOf, nub)
import Data.Maybe (catMaybes, fromMaybe)




-- Given a list of annotations
-- Return a tuple
-- fst: List of index types to use for specializing K3::MultiIndex
-- snd: List of function definitions to attach as members for this annotation combination (lookup functions)
indexes ::  Identifier -> [(Identifier, [AnnMemDecl])] -> CPPGenM ([R.Type], [R.Definition])
indexes name ans = do
  let indexed = zip [1..] ans
  let flattened = concatMap (\(n, (i, mems)) -> zip (repeat (n,i)) mems) indexed
  index_types <- (nub . catMaybes) <$> mapM index_type flattened
  --let base_name = R.Specialized ((R.Named $ R.Name "__CONTENT") : index_types) (R.Qualified (R.Name "K3") (R.Name "MultiIndex"))
  lookup_defns <- catMaybes <$> mapM lookup_fn flattened
  slice_defns <- catMaybes <$> mapM slice_fn flattened
  return (index_types, lookup_defns ++ slice_defns)
  where
    key_field = "name"
    elem_type = R.Named $ R.Name "__CONTENT"
    elem_r =  R.Name "&__CONTENT"
    bmi n = R.Qualified (R.Name "boost") (R.Qualified (R.Name "multi_index") n)

    get_key_type :: K3 Type -> Maybe (K3 Type)
    get_key_type (tag &&& children -> (TFunction, [k, _])) = Just k
    get_key_type _ = Nothing

    index_type :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Type)
    index_type ((_,n), decl) =
      if "Index" `isInfixOf` n
        then extract_type n decl
        else  return Nothing

    -- Build a boost index type e.g. ordered_non_unique
    extract_type :: Identifier -> AnnMemDecl -> CPPGenM (Maybe R.Type)
    extract_type _ (Lifted _ _ t _ _) = do
        let key_t = get_key_type t
        let fields = maybe Nothing get_fields key_t
        types <- maybe (return Nothing) (\x -> mapM single_field_type x >>= return . Just) fields
        let i_t ts =
             R.Named $
                    R.Specialized
                      [ R.Named $ R.Specialized
                        (elem_type : ts)
                        ( bmi $ R.Name "composite_key")
                      ]
                      (bmi $ R.Name "ordered_non_unique")
        return $ i_t <$> types
    extract_type _ _ = return Nothing

    get_fields :: K3 Type -> Maybe [(Identifier, K3 Type)]
    get_fields (tag &&& children -> (TRecord ids, ts) ) = Just $ zip ids ts
    get_fields _ = Nothing

    single_field_type :: (Identifier, K3 Type) -> CPPGenM R.Type
    single_field_type (n, t) = do
      cType <- genCType t
      return $
              R.Named $ R.Specialized
                [ elem_type
                ,   cType
                , R.Named $ R.Qualified elem_r (R.Name n)
                ]
                ( bmi $ R.Name "member")


    tuple :: R.Name -> K3 Type -> R.Expression
    tuple n t =
      let fields = fromMaybe (error "not a record") (get_fields t)
          ids = map fst fields
          projs = map (R.Project (R.Variable n) . R.Name) ids
      in R.Call (R.Variable $ R.Qualified (R.Name "boost") (R.Name "make_tuple")) projs


    -- Build a lookup function, wrapping boost 'find'
    lookup_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    lookup_fn ((i,_) ,Lifted _ fname t _ _ ) = do

      let key_t = get_key_type t
      let this = R.Dereference $ R.Variable $ R.Name "this"

      let container = R.Call
                       (R.Project this (R.Name "getConstContainer") )
                       []

      let index = R.Call
                   (R.Project
                      container
                      (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get"))
                   )
                   []

      let look k_t = R.Call
                   (R.Project this (R.Name "lookup_with_index") )
                   [index,  tuple (R.Name "key") k_t]

      let defn k_t c_t = R.FunctionDefn
                       (R.Name fname)
                       [("key", c_t)]
                       (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name "shared_ptr"))
                       []
                       False
                       [R.Return $ look k_t]
      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just) key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return $ if "lookup" `isInfixOf` fname then result else Nothing

    lookup_fn _ = return Nothing

    slice_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    slice_fn ((i,_),Lifted _ fname t _ _ ) = do
      let key_t = get_key_type t
      let this = R.Dereference $ R.Variable $ R.Name "this"

      let container = R.Call
                       (R.Project this (R.Name "getConstContainer") )
                       []

      let index = R.Call
                   (R.Project
                      container
                      (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get"))
                   )
                   []

      let slice k_t = R.Call
                   (R.Project this (R.Name "slice_with_index") )
                   [index, tuple (R.Name "a") k_t, tuple (R.Name "b") k_t]

      let defn k_t c_t = R.FunctionDefn
                       (R.Name fname)
                       [("a", c_t), ("b", c_t)]
                       (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name name))
                       []
                       False
                       [R.Return $ slice k_t]
      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return $ if "slice" `isInfixOf` fname then result else Nothing
    slice_fn _ = return Nothing


