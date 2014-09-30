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
indexes :: [(Identifier, [AnnMemDecl])] -> CPPGenM ([R.Type], [R.Definition])
indexes ans = do
  let flattened = concatMap (\(i, mems) -> zip (repeat i) mems) ans
  index_types <- catMaybes <$> mapM index_type flattened
  let base_name = R.Specialized ((R.Named $ R.Name "__CONTENT") : index_types) (R.Qualified (R.Name "K3") (R.Name "MultiIndex"))
  defns <- catMaybes <$> mapM (lookup_fn base_name) (zip [1..] flattened)
  return (index_types, defns)
  where
    key_field = "name"
    elem_type = R.Named $ R.Name "__CONTENT"
    elem_r =  R.Name "&__CONTENT"
    bmi n = R.Qualified (R.Name "boost") (R.Qualified (R.Name "multi_index") n)

    get_key_type :: K3 Type -> CPPGenM (Maybe (R.Type))
    get_key_type ((tag &&& children) -> (TFunction, [k, _])) = Just <$> genCType k
    get_key_type _ = return Nothing

    index_type :: (Identifier, AnnMemDecl) -> CPPGenM (Maybe R.Type)
    index_type (n, decl) = case "Index" `isInfixOf` n of
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
    lookup_fn :: R.Name -> (Integer, (Identifier, AnnMemDecl)) -> CPPGenM (Maybe R.Definition)
    lookup_fn base (i ,(_,(Lifted _ fname t _ _) )) = do
      key_t <- get_key_type t

      let container = R.Call
                       (R.Variable $ R.Qualified base $ R.Name "getContainer")
                       []

      let index = R.Call
                   (R.Project
                      container
                      (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get"))
                   )
                   []

      let find =  R.Call
                   (R.Project
                     index
                     (R.Name "find")
                   )
                   [(R.Variable $ R.Name "key")]

      let shared = R.Call
                    (R.Variable $ R.Specialized [R.Named $ R.Name "__CONTENT"] (R.Name "shared_ptr") )
                    [find]

      let defn k_t = R.FunctionDefn
                       (R.Name fname)
                       [("key", k_t)]
                       (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name "shared_ptr"))
                       []
                       [R.Return $ shared]
      return $ maybe Nothing (Just . defn) key_t

    lookup_fn _ _ = return Nothing