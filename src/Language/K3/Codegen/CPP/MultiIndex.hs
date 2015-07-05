{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.MultiIndex where

import Control.Arrow ( (&&&) )
import Control.Monad

import Data.Functor ((<$>))
import Data.List (elemIndex, isInfixOf, nub)
import Data.List.Split (splitOn)
import Data.Maybe (catMaybes, fromMaybe)

import Debug.Trace

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.Core.Literal

import Language.K3.Codegen.CPP.Primitives (genCType)
import qualified Language.K3.Codegen.CPP.Representation as R
import Language.K3.Codegen.CPP.Types


-- Given a list of annotations, return a pair of:
-- i.  index types to use for specializing K3::MultiIndex* classes
-- ii. function definitions to attach as members for this annotation combination (lookup functions)
indexes ::  Identifier -> [(Identifier, [AnnMemDecl])] -> [K3 Type] -> CPPGenM ([R.Type], [R.Definition])
indexes name ans content_ts = do
  let indexed   = zip [1..] ans
  let flattened = filter is_index_mem $ concatMap (\(n, (i, mems)) -> zip (repeat (n,i)) mems) indexed
  index_types  <- (nub . catMaybes) <$> mapM index_type flattened
  lookup_defns <- catMaybes <$> mapM lookup_fn flattened
  slice_defns  <- catMaybes <$> mapM slice_fn flattened
  range_defns  <- catMaybes <$> mapM range_fn flattened
  return (index_types, lookup_defns ++ slice_defns ++ range_defns)
  where
    elem_type = R.Named $ R.Name "__CONTENT"
    elem_r =  R.Name "&__CONTENT"
    bmi n = R.Qualified (R.Name "boost") (R.Qualified (R.Name "multi_index") n)

    is_index_mem ((_,n),_) = not ("MultiIndex" `isInfixOf` n) && ("Index" `isInfixOf` n)

    get_key_type :: K3 Type -> Maybe (K3 Type)
    get_key_type (tag &&& children -> (TFunction, [k, _])) = Just k
    get_key_type _ = Nothing

    index_type :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Type)
    index_type ((_,n), decl) =
      if "Index" `isInfixOf` n then extract_type n decl else return Nothing

    -- Build a boost index type e.g. ordered_non_unique
    extract_type :: Identifier -> AnnMemDecl -> CPPGenM (Maybe R.Type)
    extract_type n (Lifted _ _ t _ memanns) = do
      let key_t = get_key_type t
      let fields = maybe Nothing get_fields key_t
      let idx_t = if "Ordered" `isInfixOf` n then "ordered_non_unique" else "hashed_non_unique"
      if length content_ts /= 1
        then index_content_err $ length content_ts
        else case fields of
               Just fnt -> do
                 nxts <- get_extractors (head content_ts) memanns
                 extractor_types <- mapM (field_extractor_type nxts) fnt
                 i_t <- specialize_index_type idx_t extractor_types
                 return $ Just i_t
               _ -> return Nothing

    extract_type _ _ = return Nothing

    index_content_err :: Int -> CPPGenM (Maybe R.Type)
    index_content_err i = case i of
        0 -> throwE $ CPPGenE "No content type found while constructing multi-index"
        j | j >= 2 -> throwE $ CPPGenE "Multiple content types found while constructing multi-index"
        _ -> throwE $ CPPGenE "Invalid content type error argument"

    get_fields :: K3 Type -> Maybe [(Identifier, K3 Type)]
    get_fields (tag &&& children -> (TRecord ids, ts) ) = Just $ zip ids ts
    get_fields _ = Nothing

    get_extractors :: K3 Type -> [Annotation Declaration] -> CPPGenM [(Identifier, R.Type)]
    get_extractors kt anns = do
        xts <- mapM (parse_extractor kt) $ concatMap from_property anns
        return $ concat xts
      where
        from_property :: Annotation Declaration -> [K3 Literal]
        from_property (DProperty p@(dPropertyName -> n))
          | n == "IndexExtractor" = maybe [] (:[]) $ dPropertyValue p
        from_property _ = []

        parse_extractor :: K3 Type -> K3 Literal -> CPPGenM [(Identifier, R.Type)]
        parse_extractor t (tag -> LString (splitOn ";" -> l)) = concat <$> mapM (named_path_extractor t) l
        parse_extractor _ _ = return []

        named_path_extractor :: K3 Type -> String -> CPPGenM [(Identifier, R.Type)]
        named_path_extractor t s = case splitOn "=" s of
          [n, xspec] -> path_extractor t xspec >>= return . maybe [] (\x -> [(n, x)])
          _ -> return []

        path_extractor :: K3 Type -> String -> CPPGenM (Maybe R.Type)
        path_extractor t (splitOn "." -> hd:tl) = do
          init_tcxOpt  <- acc_type_path (Just (t, elem_type, [])) hd
          final_tcxOpt <- foldM acc_type_path init_tcxOpt tl
          maybe (return Nothing) (\(_,_,p) -> stitch_type_path p) final_tcxOpt

        path_extractor _ _ = return Nothing

        match_type_path :: String -> K3 Type -> R.Type -> CPPGenM (Maybe (K3 Type, R.Type, R.Type))
        match_type_path lbl (tnc -> (TRecord ids, ch)) ct = do
          case lbl `elemIndex` ids of
            Just i -> do
              let lbl_t = ch !! i
              let ct_ref = "&" ++ (displayS (renderCompact $ R.stringify ct) "")
              let lbl_path = R.Named $ R.Qualified (R.Name ct_ref) $ R.Name lbl
              lbl_ct <- genCType lbl_t
              return . Just $ (lbl_t, lbl_ct, member_extractor ct lbl_ct lbl_path)
            Nothing -> return Nothing

        match_type_path _ _ _ = return Nothing

        acc_type_path :: Maybe (K3 Type, R.Type, [R.Type]) -> String
                      -> CPPGenM (Maybe (K3 Type, R.Type, [R.Type]))
        acc_type_path Nothing _ = return Nothing
        acc_type_path (Just (t, ct, xts)) lbl = do
          txOpt <- match_type_path lbl t ct
          maybe (return Nothing) (\(lt, lct, lxt) -> return $ Just (lt, lct, xts ++ [lxt])) txOpt

        stitch_type_path :: [R.Type] -> CPPGenM (Maybe R.Type)
        stitch_type_path [] = return Nothing
        stitch_type_path tl = return . Just $ foldl1 (\x y -> R.Named $ R.Specialized [y,x] $ R.Name "K3::subkey") tl


    field_extractor_type :: [(Identifier, R.Type)] -> (Identifier, K3 Type) -> CPPGenM R.Type
    field_extractor_type extractors (fieldn, t) = do
      case lookup fieldn extractors of
        Just x_t  -> return x_t
        Nothing -> do
          c_type <- genCType t
          return $ member_extractor elem_type c_type $ R.Named $ R.Qualified elem_r (R.Name fieldn)

    member_extractor e_t c_t e_path =
      R.Named $ R.Specialized [e_t, c_t, e_path] (bmi $ R.Name "member")

    specialize_index_type :: Identifier -> [R.Type] -> CPPGenM R.Type
    specialize_index_type _ [] = throwE $ CPPGenE "Empty index members"
    specialize_index_type idx_t [x_t] =
      return $ R.Named $ R.Specialized [x_t] (bmi $ R.Name idx_t)

    specialize_index_type idx_t extractor_ts =
      return $ R.Named $ R.Specialized
        [ R.Named $ R.Specialized (elem_type : extractor_ts) (bmi $ R.Name "composite_key") ]
        (bmi $ R.Name idx_t)

    tuple :: R.Name -> K3 Type -> R.Expression
    tuple n t =
      let fields = fromMaybe (error "not a record") (get_fields t)
          ids = map fst fields
          projs = map (R.Project (R.Variable n) . R.Name) ids
      in case projs of
           [x] -> x
           _ -> R.Call (R.Variable $ R.Qualified (R.Name "boost") (R.Name "make_tuple")) projs

    lookup_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    lookup_fn ((i,_), Lifted _ fname t _ _) = do
      let key_t     = get_key_type t
      let this      = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    (R.Variable $ (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let look k_t = R.Call (R.Project this (R.Name "lookup_with_index") )
                            [index, tuple (R.Name "key") k_t]

      let defn k_t c_t = R.FunctionDefn (R.Name fname)
                            [("key", c_t)]
                            (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name "shared_ptr"))
                            []
                            False
                            [R.Return $ look k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just) key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return $ if "lookup_by" `isInfixOf` fname then result else Nothing

    lookup_fn _ = return Nothing

    slice_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    slice_fn ((i,_), Lifted _ fname t _ _) = do
      let key_t = get_key_type t
      let this = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    ((R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let slice k_t = R.Call (R.Project this (R.Name "slice_with_index") )
                             [index, tuple (R.Name "key") k_t]

      let defn k_t c_t = R.FunctionDefn (R.Name fname)
                           [("key", c_t)]
                           (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name name))
                           []
                           False
                           [R.Return $ slice k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return $ if "slice_by" `isInfixOf` fname then result else Nothing

    slice_fn _ = return Nothing

    range_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    range_fn ((i,_), Lifted _ fname t _ _) = do
      let key_t = get_key_type t
      let this = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    ((R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let range k_t = R.Call (R.Project this (R.Name "range_with_index") )
                             [index, tuple (R.Name "a") k_t, tuple (R.Name "b") k_t]

      let defn k_t c_t = R.FunctionDefn (R.Name fname)
                           [("a", c_t), ("b", c_t)]
                           (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name name))
                           []
                           False
                           [R.Return $ range k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return $ if "range_by" `isInfixOf` fname then result else Nothing

    range_fn _ = return Nothing
