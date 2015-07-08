{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.MultiIndex where

import Control.Arrow ( (&&&) )
import Control.Monad

import Data.Functor ((<$>))
import Data.List (elemIndex, isInfixOf, nub)
import Data.List.Split (splitOn)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Tree

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.Core.Literal

import qualified Language.K3.Core.Constructor.Type as TC

import Language.K3.Codegen.CPP.Primitives (genCType)
import qualified Language.K3.Codegen.CPP.Representation as R
import Language.K3.Codegen.CPP.Types


-- Given a list of annotations, return a pair of:
-- i.  index types to use for specializing K3::MultiIndex* classes
-- ii. function definitions to attach as members for this annotation combination (lookup functions)
indexes ::  Identifier -> [(Identifier, [AnnMemDecl])] -> [K3 Type] -> CPPGenM ([R.Type], [R.Definition])
indexes name ans content_ts = do
  let indexed   = zip [1..] $ filter is_index_mem ans
  let flattened = concatMap (\(i, (n, mems)) -> zip (repeat (i,n)) mems) indexed
  index_types       <- (map snd . nub . catMaybes) <$> mapM index_type flattened
  lookup_defns      <- catMaybes <$> mapM lookup_fn flattened
  lookup_with_defns <- catMaybes <$> mapM lookup_with_fn flattened
  slice_defns       <- catMaybes <$> mapM slice_fn flattened
  range_defns       <- catMaybes <$> mapM range_fn flattened
  fold_slice_defns  <- catMaybes <$> mapM fold_slice_fn flattened
  fold_range_defns  <- catMaybes <$> mapM fold_range_fn flattened
  return (index_types, lookup_defns ++ lookup_with_defns
                        ++ slice_defns ++ range_defns
                        ++ fold_slice_defns ++ fold_range_defns)
  where
    elem_type = R.Named $ R.Name "__CONTENT"
    elem_r =  R.Name "&__CONTENT"
    bmi n = R.Qualified (R.Name "boost") (R.Qualified (R.Name "multi_index") n)

    is_index_mem (n,_) = not ("MultiIndex" `isInfixOf` n) && ("Index" `isInfixOf` n)

    get_key_type :: Identifier -> K3 Type -> Maybe (K3 Type)
    get_key_type n (PTFunction k f _)
      | "VMap" `isInfixOf` n = case f of
                                 (PTFunction vk _ _) -> Just vk
                                 _ -> Nothing
      | otherwise = Just k

    get_key_type _ _ = Nothing

    index_type :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe (Integer, R.Type))
    index_type ((i,n), decl) = extract_type i n decl

    -- Build a boost index type e.g. ordered_non_unique
    extract_type :: Integer -> Identifier -> AnnMemDecl -> CPPGenM (Maybe (Integer, R.Type))
    extract_type i n (Lifted _ _ t _ memanns) = do
      let key_t = get_key_type n t
      let fields = maybe Nothing get_fields key_t
      let idx_t = if "Ordered" `isInfixOf` n then "ordered_non_unique" else "hashed_non_unique"
      if length content_ts /= 1
        then index_content_err $ length content_ts
        else case fields of
               Just fnt -> do
                 (rkt, rcovt) <- get_extractor_root_types n (head content_ts) elem_type
                 nxts <- get_extractors rkt elem_type rcovt memanns
                 extractor_types <- mapM (field_extractor_type nxts) fnt
                 i_t <- specialize_index_type n idx_t extractor_types
                 return $ Just (i, i_t)
               _ -> return Nothing

    extract_type _ _ _ = return Nothing

    index_content_err :: Int -> CPPGenM (Maybe (Integer, R.Type))
    index_content_err i = case i of
        0 -> throwE $ CPPGenE "No content type found while constructing multi-index"
        j | j >= 2 -> throwE $ CPPGenE "Multiple content types found while constructing multi-index"
        _ -> throwE $ CPPGenE "Invalid content type error argument"

    get_fields :: K3 Type -> Maybe [(Identifier, K3 Type)]
    get_fields (PTRecord ids ts _) = Just $ zip ids ts
    get_fields _ = Nothing

    get_extractor_root_types :: Identifier -> K3 Type -> R.Type -> CPPGenM (K3 Type, [(Identifier, R.Type)])
    get_extractor_root_types n t@(PTRecord ids ch _) ct
      | "VMap" `isInfixOf` n
        = return $ ( (TC.record $ filter ((== "key") . fst) $ zip ids ch) @<- annotations t
                   , [("key", R.Named $ R.Specialized [ct, R.Primitive $ R.PInt] $ R.Name "K3::VKeyExtractor")] )
      | otherwise = return (t, [])

    get_extractor_root_types _ t _ = return (t, [])

    get_extractors :: K3 Type -> R.Type -> [(Identifier, R.Type)] -> [Annotation Declaration]
                   -> CPPGenM [(Identifier, R.Type)]
    get_extractors ikt ict icovt anns = do
        xts <- mapM (parse_extractor ikt ict icovt) $ concatMap from_property anns
        return $ concat xts
      where
        from_property :: Annotation Declaration -> [K3 Literal]
        from_property (DProperty p@(dPropertyName -> n))
          | n == "IndexExtractor" = maybe [] (:[]) $ dPropertyValue p
        from_property _ = []

        parse_extractor :: K3 Type -> R.Type -> [(Identifier, R.Type)] -> K3 Literal -> CPPGenM [(Identifier, R.Type)]
        parse_extractor t ct covt (tag -> LString (splitOn ";" -> l)) = concat <$> mapM (named_path_extractor t ct covt) l
        parse_extractor _ _ _ _ = return []

        named_path_extractor :: K3 Type -> R.Type -> [(Identifier, R.Type)] -> String -> CPPGenM [(Identifier, R.Type)]
        named_path_extractor t ct covt s = case splitOn "=" s of
          [n, xspec] -> path_extractor t ct covt xspec >>= \x -> return [(n, x)]
          _ -> return []

        path_extractor :: K3 Type -> R.Type -> [(Identifier, R.Type)] -> String -> CPPGenM R.Type
        path_extractor t ct covt (splitOn "." -> hd:tl) = do
          init_tcx  <- acc_type_path (t, ct, [], covt) hd
          (_,_,p,_) <- foldM acc_type_path init_tcx tl
          stitch_type_path p

        path_extractor _ _ _ path = throwE $ CPPGenE $ "Invalid path extractor: " ++ path

        match_type_path :: [(Identifier, R.Type)] -> String -> K3 Type -> R.Type
                        -> CPPGenM (K3 Type, R.Type, R.Type, [(Identifier, R.Type)])
        match_type_path covt lbl (tnc -> (TRecord ids, ch)) ct = do
          case lbl `elemIndex` ids of
            Just i -> do
              let lbl_t = ch !! i
              lbl_ct <- genCType lbl_t
              let (lbl_xt, rcovt) = path_component_extractor covt lbl ct lbl_ct
              return (lbl_t, lbl_ct, lbl_xt, rcovt)

            Nothing -> throwE $ CPPGenE $ "Could not find index path extractor component " ++ lbl

        match_type_path _ lbl _ _ = throwE $ CPPGenE $ "Could not find index path extractor component " ++ lbl

        acc_type_path :: (K3 Type, R.Type, [R.Type], [(Identifier, R.Type)])
                      -> String
                      -> CPPGenM (K3 Type, R.Type, [R.Type], [(Identifier, R.Type)])
        acc_type_path (t, ct, xts, covt) lbl = do
          (lt, lct, lxt, rcovt) <- match_type_path covt lbl t ct
          return (lt, lct, xts ++ [lxt], rcovt)

        path_component_extractor :: [(Identifier, R.Type)] -> String -> R.Type -> R.Type
                                 -> (R.Type, [(Identifier, R.Type)])
        path_component_extractor covt lbl ct lbl_ct =
          case lookup lbl covt of
            Just xt -> (xt, filter ((/= lbl) . fst) covt)
            _ -> let ct_ref = "&" ++ (displayS (renderCompact $ R.stringify ct) "")
                     pct    = R.Named $ R.Qualified (R.Name ct_ref) $ R.Name lbl
                 in (member_extractor ct lbl_ct pct, covt)

        stitch_type_path :: [R.Type] -> CPPGenM R.Type
        stitch_type_path [] = throwE $ CPPGenE $ "Empty path extractor components"
        stitch_type_path tl = return $ foldl1 (\x y -> R.Named $ R.Specialized [y,x] $ R.Name "K3::subkey") tl


    field_extractor_type :: [(Identifier, R.Type)] -> (Identifier, K3 Type) -> CPPGenM R.Type
    field_extractor_type extractors (fieldn, t) = do
      case lookup fieldn extractors of
        Just x_t  -> return x_t
        Nothing -> do
          c_type <- genCType t
          return $ member_extractor elem_type c_type $ R.Named $ R.Qualified elem_r (R.Name fieldn)

    member_extractor :: R.Type -> R.Type -> R.Type -> R.Type
    member_extractor e_t c_t e_path =
      R.Named $ R.Specialized [e_t, c_t, e_path] (bmi $ R.Name "member")

    specialize_index_type :: Identifier -> Identifier -> [R.Type] -> CPPGenM R.Type
    specialize_index_type _ _ [] = throwE $ CPPGenE "Empty index members"
    specialize_index_type _ idx_t [x_t] =
      return $ R.Named $ R.Specialized [x_t] (bmi $ R.Name idx_t)

    specialize_index_type n idx_t extractor_ts = do
      let comp_ret_t = if "VMap" `isInfixOf` n
                         then (R.Named $ R.Specialized [elem_type, R.Primitive R.PInt] $ R.Name "K3::VElem")
                         else elem_type
      return $ R.Named $ R.Specialized
        [ R.Named $ R.Specialized (comp_ret_t : extractor_ts) (bmi $ R.Name "composite_key") ]
        (bmi $ R.Name idx_t)

    tuple :: R.Name -> K3 Type -> R.Expression
    tuple n t =
      let fields = fromMaybe (error "not a record") (get_fields t)
          ids = map fst fields
          projs = map (R.Project (R.Variable n) . R.Name) ids
      in case projs of
           [x] -> x
           _ -> R.Call (R.Variable $ R.Qualified (R.Name "boost") (R.Name "make_tuple")) projs

    call_args :: Identifier -> R.Expression -> [R.Expression] -> [R.Expression]
    call_args n idx_e el = if "VMap" `isInfixOf` n
                             then [idx_e, R.Variable $ R.Name "version"] ++ el
                             else [idx_e] ++ el

    defn_args :: Identifier -> [(Identifier, R.Type)] -> [(Identifier, R.Type)]
    defn_args n ntl = if "VMap" `isInfixOf` n then ("version", version_t) : ntl else ntl
      where version_t = R.Primitive R.PInt

    lookup_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    lookup_fn ((i,n), Lifted _ fname t _ _)
      | ("lookup_by" `isInfixOf` fname)
        || ("VMap" `isInfixOf` n && "lookup_before_by" `isInfixOf` fname)
      = do
        let key_t     = get_key_type n t
        let this      = R.Dereference $ R.Variable $ R.Name "this"
        let container = R.Call (R.Project this $ R.Name "getConstContainer") []

        let index = R.Call
                      (R.Variable $ (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                      [container]

        let c_fname = if "VMap" `isInfixOf` n then "lookup_before_by_index"
                                              else "lookup_by_index"

        let look k_t = R.Call (R.Project this $ R.Name c_fname)
                         $ call_args n index [tuple (R.Name "key") k_t]

        let defn k_t c_t = R.FunctionDefn (R.Name fname)
                              (defn_args n [("key", c_t)])
                              (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name "shared_ptr"))
                              []
                              False
                              [R.Return $ look k_t]

        cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just) key_t
        return (key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t)

    lookup_fn _ = return Nothing

    lookup_with_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    lookup_with_fn ((i,n), Lifted _ fname t _ _)
      | ("lookup_with_by" `isInfixOf` fname)
        || ("VMap" `isInfixOf` n && "lookup_with_before_by" `isInfixOf` fname)
      = do
        let key_t     = get_key_type n t
        let f_t       = R.Named $ R.Name "F"
        let g_t       = R.Named $ R.Name "G"
        let this      = R.Dereference $ R.Variable $ R.Name "this"
        let container = R.Call (R.Project this $ R.Name "getConstContainer") []

        let index = R.Call
                      (R.Variable $ (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                      [container]

        let c_fname = if "VMap" `isInfixOf` n then "lookup_with_before_by_index"
                                              else "lookup_with_by_index"

        let look k_t = R.Call (R.Project this $ R.Name c_fname)
                         $ call_args n index [ tuple (R.Name "key") k_t
                                             , R.Variable $ R.Name "f"
                                             , R.Variable $ R.Name "g" ]

        let defn k_t c_t = R.TemplateDefn [("F", Nothing), ("G", Nothing)] $
                           R.FunctionDefn (R.Name fname)
                              (defn_args n [("key", c_t), ("f", f_t), ("g", g_t)])
                              (Just $ R.Named $ R.Name "auto")
                              []
                              False
                              [R.Return $ look k_t]

        cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just) key_t
        return (key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t)

    lookup_with_fn _ = return Nothing

    slice_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    slice_fn (_, Lifted _ fname _ _ _) | "fold_slice_by" `isInfixOf` fname = return Nothing
    slice_fn ((i,n), Lifted _ fname t _ _) | "slice_by" `isInfixOf` fname = do
      let key_t = get_key_type n t
      let this = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    ((R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let slice k_t = R.Call (R.Project this $ R.Name "slice_by_index")
                        $ call_args n index [tuple (R.Name "key") k_t]

      let defn k_t c_t = R.FunctionDefn (R.Name fname)
                           (defn_args n [("key", c_t)])
                           (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name name))
                           []
                           False
                           [R.Return $ slice k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return result

    slice_fn _ = return Nothing

    range_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    range_fn (_, Lifted _ fname _ _ _) | "fold_range_by" `isInfixOf` fname = return Nothing
    range_fn ((i,n), Lifted _ fname t _ _) | "range_by" `isInfixOf` fname = do
      let key_t = get_key_type n t
      let this = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    ((R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let range k_t = R.Call (R.Project this (R.Name "range_by_index") )
                        $ call_args n index [tuple (R.Name "a") k_t, tuple (R.Name "b") k_t]

      let defn k_t c_t = R.FunctionDefn (R.Name fname)
                           (defn_args n [("a", c_t), ("b", c_t)])
                           (Just $ R.Named $ R.Specialized [R.Named $ R.Name  "__CONTENT"] (R.Name name))
                           []
                           False
                           [R.Return $ range k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return result

    range_fn _ = return Nothing


    fold_slice_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    fold_slice_fn ((i,n), Lifted _ fname t _ _) | "fold_slice_by" `isInfixOf` fname = do
      let key_t = get_key_type n t
      let f_t   = R.Named $ R.Name "Fun"
      let acc_t = R.Named $ R.Name "Acc"
      let this = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    ((R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let slice k_t = R.Call (R.Project this $ R.Name "fold_slice_by_index")
                        $ call_args n index [ tuple (R.Name "key") k_t
                                            , R.Variable $ R.Name "f", R.Variable $ R.Name "acc" ]

      let defn k_t c_t = R.TemplateDefn [("Fun", Nothing), ("Acc", Nothing)] $
                         R.FunctionDefn (R.Name fname)
                           (defn_args n [("key", c_t), ("f", f_t), ("acc", acc_t)])
                           (Just $ acc_t)
                           []
                           False
                           [R.Return $ slice k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return result

    fold_slice_fn _ = return Nothing


    fold_range_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    fold_range_fn ((i,n), Lifted _ fname t _ _) | "fold_range_by" `isInfixOf` fname = do
      let key_t = get_key_type n t
      let f_t   = R.Named $ R.Name "Fun"
      let acc_t = R.Named $ R.Name "Acc"
      let this = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    ((R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let range k_t = R.Call (R.Project this $ R.Name "fold_range_by_index")
                        $ call_args n index [ tuple (R.Name "a") k_t, tuple (R.Name "b") k_t
                                            , R.Variable $ R.Name "f", R.Variable $ R.Name "acc" ]

      let defn k_t c_t = R.TemplateDefn [("Fun", Nothing), ("Acc", Nothing)] $
                         R.FunctionDefn (R.Name fname)
                           (defn_args n [("a", c_t), ("b", c_t), ("f", f_t), ("acc", acc_t)])
                           (Just $ acc_t)
                           []
                           False
                           [R.Return $ range k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return result

    fold_range_fn _ = return Nothing


{- Pattern synonyms for index functions. -}

pattern PTRecord ids ch anns = Node (TRecord ids :@: anns) ch
pattern PTFunction arg rt anns = Node (TFunction :@: anns) [arg, rt]
