{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.CollectionMembers where

import Control.Monad

import Data.List (elemIndex, find, isInfixOf, isPrefixOf, isSuffixOf, nub)
import Data.List.Split (splitOn)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Tree

import Debug.Trace

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.Core.Literal

import qualified Language.K3.Core.Constructor.Type as TC

import Language.K3.Utils.Pretty

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
  slice_defns       <- catMaybes <$> mapM slice_fn flattened
  range_defns       <- catMaybes <$> mapM range_fn flattened
  fold_slice_defns  <- catMaybes <$> mapM fold_slice_fn flattened
  fold_range_defns  <- catMaybes <$> mapM fold_range_fn flattened
  fold_slice_vid_defns <- catMaybes <$> mapM fold_slice_vid_fn flattened
  return (index_types, lookup_defns
                        ++ slice_defns ++ range_defns
                        ++ fold_slice_defns ++ fold_range_defns ++ fold_slice_vid_defns)
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

    defn_args :: Identifier -> [(Maybe Identifier, R.Type)] -> [(Maybe Identifier, R.Type)]
    defn_args n ntl = if "VMap" `isInfixOf` n then (Just "version", version_t) : ntl else ntl
      where version_t = R.Primitive R.PInt

    lookup_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    lookup_fn ((i,n), Lifted _ fname t _ _)
      | ("lookup_by" `isInfixOf` fname)
        || ("VMap" `isInfixOf` n && "lookup_before_by" `isInfixOf` fname)
      = do
        let key_t     = get_key_type n t
        let f_t       = R.Named $ R.Name "F"
        let g_t       = R.Named $ R.Name "G"
        let this      = R.Dereference $ R.Variable $ R.Name "this"
        let container = R.Call (R.Project this $ R.Name "getConstContainer") []

        let index = R.Call
                      (R.Variable $ (R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                      [container]

        let c_fname = if "VMap" `isInfixOf` n then "lookup_before_by_index"
                                              else "lookup_by_index"

        let look k_t = R.Call (R.Project this $ R.Name c_fname)
                         $ call_args n index [ tuple (R.Name "key") k_t
                                             , R.Variable $ R.Name "f"
                                             , R.Variable $ R.Name "g" ]

        let defn k_t c_t = R.TemplateDefn [("F", Nothing), ("G", Nothing)] $
                           R.FunctionDefn (R.Name fname)
                              (defn_args n [(Just "key", c_t), (Just "f", f_t), (Just "g", g_t)])
                              (Just $ R.Named $ R.Name "auto")
                              []
                              False
                              [R.Return $ look k_t]

        cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just) key_t
        return (key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t)

    lookup_fn _ = return Nothing

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
                           (defn_args n [(Just "key", c_t)])
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
                           (defn_args n [(Just "a", c_t), (Just "b", c_t)])
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
                                            , R.Variable $ R.Name "f", R.Move $ R.Variable $ R.Name "acc" ]

      let defn k_t c_t = R.TemplateDefn [("Fun", Nothing), ("Acc", Nothing)] $
                         R.FunctionDefn (R.Name fname)
                           (defn_args n [(Just "key", c_t), (Just "f", f_t), (Just "acc", acc_t)])
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
                                            , R.Variable $ R.Name "f", R.Move $ R.Variable $ R.Name "acc" ]

      let defn k_t c_t = R.TemplateDefn [("Fun", Nothing), ("Acc", Nothing)] $
                         R.FunctionDefn (R.Name fname)
                           (defn_args n [(Just "a", c_t), (Just "b", c_t), (Just "f", f_t), (Just "acc", acc_t)])
                           (Just $ acc_t)
                           []
                           False
                           [R.Return $ range k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return result

    fold_range_fn _ = return Nothing

    fold_slice_vid_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    fold_slice_vid_fn ((i,n), Lifted _ fname t _ _) | "fold_slice_vid_by" `isInfixOf` fname = do
      let key_t = get_key_type n t
      let f_t   = R.Named $ R.Name "Fun"
      let acc_t = R.Named $ R.Name "Acc"
      let this = R.Dereference $ R.Variable $ R.Name "this"
      let container = R.Call (R.Project this $ R.Name "getConstContainer") []

      let index = R.Call
                    ((R.Variable $ R.Specialized [R.Named $ R.Name $ show i] (R.Name "get")))
                    [container]

      let slice k_t = R.Call (R.Project this $ R.Name "fold_slice_vid_by_index")
                        $ call_args n index [ tuple (R.Name "key") k_t
                                            , R.Variable $ R.Name "f", R.Move $ R.Variable $ R.Name "acc" ]

      let defn k_t c_t = R.TemplateDefn [("Fun", Nothing), ("Acc", Nothing)] $
                         R.FunctionDefn (R.Name fname)
                           (defn_args n [(Just "key", c_t), (Just "f", f_t), (Just "acc", acc_t)])
                           (Just $ acc_t)
                           []
                           False
                           [R.Return $ slice k_t]

      cType <- maybe (return Nothing) (\x -> genCType x >>= return . Just)  key_t
      let result = key_t >>= \k_t -> cType >>= \c_t -> Just $ defn k_t c_t
      return result

    fold_slice_vid_fn _ = return Nothing
   
-- Specialize for unit record
pattern RElemUnit = R.Named (R.Specialized [R.Named (R.Name "unit")] (R.Name "R_elem"))


-- Returns member definitions for a FlatPolyBuffer or UniquePolyBuffer
polybuffer :: Identifier -> [(Identifier, [AnnMemDecl])] -> CPPGenM [R.Definition]
polybuffer name ans  = do
  let indexed   = zip [1..] $ filter (is_polybuffer . fst) ans
  let flattened = concatMap (\(i, (n, mems)) -> zip (repeat (i,n)) mems) indexed
  at_defns                   <- catMaybes <$> mapM at_fn flattened
  safe_at_defns              <- catMaybes <$> mapM safe_at_fn flattened
  unsafe_at_defns            <- catMaybes <$> mapM unsafe_at_fn flattened
  iterate_tag_defns          <- catMaybes <$> mapM iterate_tag_fn flattened
  fold_tag_defns             <- catMaybes <$> mapM fold_tag_fn flattened
  (tgs, types, append_defns) <- unzip3 . catMaybes <$> mapM append_fn flattened
  skip_tag_defns             <- catMaybes <$> mapM skip_fn flattened
  skip_all_tag_defns         <- catMaybes <$> mapM skip_all_fn flattened
  extra_defns                <- extra_fns tgs types
  return $ super_defn ++ copy_ctor ++ move_ctor ++ copy_assign ++ move_assign ++ dtor
           ++ at_defns ++ safe_at_defns ++ unsafe_at_defns ++ append_defns
           ++ iterate_tag_defns ++ fold_tag_defns ++ skip_tag_defns ++ skip_all_tag_defns ++ extra_defns

  where
    is_polybuffer n = any (`isInfixOf` n) ["FlatPolyBuffer", "UniquePolyBuffer"]
    is_flat_polybuffer = "FlatPolyBuffer" `isInfixOf` name

    super_type = R.Qualified (R.Name "K3") $
                  if is_flat_polybuffer
                  then R.Specialized [elem_type, R.Named $ R.Name name] $ R.Name "FlatPolyBuffer"
                  else R.Specialized [elem_type, R.Named $ R.Name name] $ R.Name "UniquePolyBuffer"

    super_defn = if is_polybuffer name then
                  [R.GlobalDefn $ R.Forward $ R.UsingDecl
                    (Right $ R.Name "Super")
                    (Just $ super_type)]

                 else []

    -- TODO we need to complete the rule of 5 (move, copyAssign, moveAssign, destructor)
    copy_ctor :: [R.Definition]
    copy_ctor = if not (is_polybuffer name) then [] else
      [ R.FunctionDefn
          (R.Name name)
          [(Just "other", R.Reference $ R.Const $ R.Named $ R.Name name)]
          Nothing
          [R.Call (R.Variable super_type) [R.Variable $ R.Name "other"]]
          False
          [ R.IfThenElse
              (R.Project (R.Dereference $ R.Project (R.Variable $ R.Name "other") $ R.Name "container") $ R.Name "internalized")
              [R.Ignore $ R.Call
                (R.Variable $ R.Qualified (R.Name "Super") (R.Name "unpack"))
                [R.Initialization R.Unit []]
              ]
              []
          ]
       ]

    move_ctor :: [R.Definition]
    move_ctor = if not (is_polybuffer name) then [] else
      [ R.FunctionDefn
          (R.Name name)
          [(Just "other", R.RValueReference $ R.Named $ R.Name name)]
          Nothing
          [R.Call (R.Variable super_type) [R.Move $ R.Variable $ R.Name "other"]]
          False
          []
       ]

    copy_assign :: [R.Definition]
    copy_assign = if not (is_polybuffer name) then [] else
      [ R.FunctionDefn
          (R.Name "operator=")
          [(Just "other", R.Reference $ R.Const $ R.Named $ R.Name name)]
          (Just $ R.Reference $ R.Named $ R.Name name)
          []
          False
          [ R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "Super") (R.Name "operator=")) [R.Variable $ R.Name "other"],
            R.Return $ R.Dereference $ R.Variable $ R.Name "this"
          ]
       ]

    move_assign :: [R.Definition]
    move_assign = if not (is_polybuffer name) then [] else
      [ R.FunctionDefn
          (R.Name "operator=")
          [(Just "other", R.RValueReference $ R.Named $ R.Name name)]
          (Just $ R.Reference $ R.Named $ R.Name name)
          []
          False
          [ R.Ignore $ R.Call (R.Variable $ R.Qualified (R.Name "Super") (R.Name "operator=")) [R.Move $ R.Variable $ R.Name "other"],
            R.Return $ R.Dereference $ R.Variable $ R.Name "this"
          ]
       ]

    dtor :: [R.Definition]
    dtor = if not (is_polybuffer name) then [] else
      [ R.FunctionDefn
          (R.Name $ "~" ++ name)
          []
          Nothing
          []
          False
          []
       ]

    at_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    at_fn (_, Lifted _ fname _ _ _) | "_unsafe_at" `isSuffixOf` fname = return Nothing
    at_fn (_, Lifted _ fname _ _ _) | "_safe_at" `isSuffixOf` fname = return Nothing
    at_fn (_, Lifted _ fname t _ _) | "_at" `isSuffixOf` fname = do
      let (arg1, arg2) = ("idx", "offset")
      let int_t = R.Primitive R.PInt
      let at_rt = get_at_return_type t
      let typed_at ct = R.Call
                          (R.Variable $ suqualnm $ R.Specialized [ct] $ R.Name "template at")
                          [R.Variable $ R.Name arg1, R.Variable $ R.Name arg2]

      let defn ct = R.FunctionDefn (R.Name fname)
                      [(Just arg1, int_t), (Just arg2, int_t)]
                      (Just $ ct)
                      []
                      True
                      [R.Return $ typed_at ct]

      c_at_rt <- maybe (return Nothing) (\x -> genCType x >>= return . Just) at_rt
      return $ defn <$> c_at_rt

    at_fn _ = return Nothing

    safe_at_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    safe_at_fn (_, Lifted _ fname t _ _) | "_safe_at" `isSuffixOf` fname = do
      let (arg1, arg2, arg3, arg4) = ("idx", "offset", "f", "g")
      let int_t = R.Primitive R.PInt
      let f_t   = R.Named $ R.Name "F"
      let g_t   = R.Named $ R.Name "G"
      let safe_at_t = get_safe_at_type t
      let typed_safe_at ct = R.Call
                              (R.Variable $ suqualnm $ R.Specialized [ct, f_t, g_t] $ R.Name "template safe_at")
                              (map (R.Variable . R.Name) [arg1, arg2, arg3, arg4])

      let defn ct = R.TemplateDefn [("F", Nothing), ("G", Nothing)] $
                    R.FunctionDefn (R.Name fname)
                      [(Just arg1, int_t), (Just arg2, int_t), (Just arg3, f_t), (Just arg4, g_t)]
                      (Just $ R.Named $ R.Name "auto")
                      []
                      True
                      [R.Return $ typed_safe_at ct]

      c_safe_at_t <- maybe (return Nothing) (\x -> genCType x >>= return . Just) safe_at_t
      return $ defn <$> c_safe_at_t

    safe_at_fn _ = return Nothing

    unsafe_at_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    unsafe_at_fn (_, Lifted _ fname t _ _) | "_unsafe_at" `isSuffixOf` fname = do
      let (arg1, arg2, arg3) = ("idx", "offset", "f")
      let int_t = R.Primitive R.PInt
      let f_t   = R.Named $ R.Name "F"
      let unsafe_at_t = get_unsafe_at_type t
      let typed_unsafe_at ct = R.Call
                                (R.Variable $ suqualnm $ R.Specialized [ct, f_t] $ R.Name "template unsafe_at")
                                (map (R.Variable . R.Name) [arg1, arg2, arg3])

      let defn ct = R.TemplateDefn [("F", Nothing)] $
                    R.FunctionDefn (R.Name fname)
                      [(Just arg1, int_t), (Just arg2, int_t), (Just arg3, f_t)]
                      (Just $ R.Named $ R.Name "auto")
                      []
                      True
                      [R.Return $ typed_unsafe_at ct]

      c_unsafe_at_t <- maybe (return Nothing) (\x -> genCType x >>= return . Just) unsafe_at_t
      return $ defn <$> c_unsafe_at_t

    unsafe_at_fn _ = return Nothing

    append_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe (Int, R.Type, R.Definition))
    append_fn (_, Lifted _ fname t _ danns) | "append_" `isPrefixOf` fname = do
      let val_t = get_append_type t
      let typed_append ct_tag ct = R.Call
                                     (R.Variable $ suqualnm $ R.Specialized [ct] $ R.Name "template append")
                                     [R.Literal $ R.LInt ct_tag, R.Variable $ R.Name "elem"]

      let defn ct_tag ct = R.FunctionDefn (R.Name fname)
                             [(Just "elem", R.Reference $ R.Const ct)]
                             (Just $ R.Unit)
                             []
                             False
                             [R.Return $ typed_append ct_tag ct]

      c_val_t <- maybe (return Nothing) (\x -> genCType x >>= return . Just) val_t
      t_tag <- maybe (return Nothing) (return . tag_value) $ find is_tag danns
      return $ (\tg ty -> (tg, ty, defn tg ty)) <$> t_tag <*> c_val_t

    append_fn _ = return Nothing

    iterate_tag_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    iterate_tag_fn (_, Lifted _ fname t _ danns) | "iterate_" `isPrefixOf` fname = do
      let int_t = R.Primitive R.PInt
      let val_t = get_iterate_type t
      let f_t   = R.Named $ R.Name "F"
      let typed_iterate ct_tag ct = R.Call
                                     (R.Variable $ suqualnm $ R.Specialized [ct, f_t] $ R.Name "template iterate_tag")
                                     [R.Literal $ R.LInt ct_tag,
                                      R.Variable $ R.Name "idx",
                                      R.Variable $ R.Name "offset",
                                      R.Variable $ R.Name "f"]

      let defn ct_tag ct = R.TemplateDefn [("F", Nothing)] $
                           R.FunctionDefn (R.Name fname)
                             [(Just "idx", int_t), (Just "offset", int_t), (Just "f", f_t)]
                             (Just R.Unit)
                             []
                             True
                             [R.Return $ typed_iterate ct_tag ct]

      c_val_t <- maybe (return Nothing) (\x -> genCType x >>= return . Just) val_t
      t_tag <- maybe (return Nothing) (return . tag_value) $ find is_tag danns
      return $ defn <$> t_tag <*> c_val_t

    iterate_tag_fn _ = return Nothing

    fold_tag_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    fold_tag_fn (_, Lifted _ fname t _ danns) | "foldl_" `isPrefixOf` fname = do
      let int_t = R.Primitive R.PInt
      let val_t = get_fold_type t
      let f_t   = R.Named $ R.Name "Fun"
      let acc_t = R.Named $ R.Name "Acc"
      let typed_fold ct_tag ct = R.Call
                                     (R.Variable $ suqualnm $ R.Specialized [ct, f_t] $ R.Name "template foldl_tag")
                                     [R.Literal $ R.LInt ct_tag,
                                      R.Variable $ R.Name "idx",
                                      R.Variable $ R.Name "offset",
                                      R.Variable $ R.Name "f",
                                      R.Move $ R.Variable $ R.Name "acc"]

      let defn ct_tag ct = R.TemplateDefn [("Fun", Nothing), ("Acc", Nothing)] $
                           R.FunctionDefn (R.Name fname)
                             [(Just "idx", int_t), (Just "offset", int_t), (Just "f", f_t), (Just "acc", acc_t)]
                             (Just acc_t)
                             []
                             True
                             [R.Return $ typed_fold ct_tag ct]

      c_val_t <- maybe (return Nothing) (\x -> genCType x >>= return . Just) val_t
      t_tag <- maybe (return Nothing) (return . tag_value) $ find is_tag danns
      return $ defn <$> t_tag <*> c_val_t

    fold_tag_fn _ = return Nothing

    skip_common fname danns call_name = do
      let (arg1, arg2) = ("idx", "offset")
      let int_t = R.Primitive R.PInt
      let skip_rt = R.Named $ R.Specialized [int_t, int_t] $ R.Name "R_key_value"
      let skip_call ct_tag = R.Call (R.Variable $ suqualnm $ R.Name call_name)
                               [R.Literal $ R.LInt ct_tag,
                                R.Variable $ R.Name arg1,
                                R.Variable $ R.Name arg2]

      let defn ct_tag = R.FunctionDefn (R.Name fname)
                          [(Just arg1, int_t), (Just arg2, int_t)]
                          (Just $ skip_rt)
                          []
                          True
                          [R.Return $ skip_call ct_tag]

      t_tag <- maybe (return Nothing) (return . tag_value) $ find is_tag danns
      return $ defn <$> t_tag

    skip_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    skip_fn (_, Lifted _ fname _ _ _) | "skip_all_" `isPrefixOf` fname = return Nothing
    skip_fn (_, Lifted _ fname _ _ danns) | "skip_" `isPrefixOf` fname = skip_common fname danns "skip_tag"
    skip_fn _ = return Nothing

    skip_all_fn :: ((Integer, Identifier), AnnMemDecl) -> CPPGenM (Maybe R.Definition)
    skip_all_fn (_, Lifted _ fname _ _ danns) | "skip_all_" `isPrefixOf` fname = skip_common fname danns "skip_all_tag"
    skip_all_fn _ = return Nothing

    extra_fns :: [Int] -> [R.Type] -> CPPGenM [R.Definition]
    extra_fns [] [] = return []
    extra_fns tags types = catMaybes <$> mapM (\f -> f tags types)
      [equalelem_fn, hashelem_fn, elemsize_fn, elemappend_fn,
       externalize_fn, internalize_fn,
       yamlencode_fn, yamldecode_fn, jsonencode_fn]

    equalelem_fn :: [Int] -> [R.Type] -> CPPGenM (Maybe R.Definition)
    equalelem_fn tags types = do
      let void_ptr_t = R.Pointer $ R.Void
      return $ Just $ R.FunctionDefn (R.Name "equalelem")
                        [(Just "ltag", tag_t), (Just "lelem", void_ptr_t),
                         (Just "rtag", tag_t), (Just "relem", void_ptr_t)]
                        (Just $ R.Named $ R.Name "static bool")
                        []
                        False
                        [R.IfThenElse (R.Binary "==" (R.Variable $ R.Name "ltag") (R.Variable $ R.Name "rtag"))
                          [branch_chain "ltag" tags types elseStmt elemStmt]
                          [R.Return $ R.Literal $ R.LBool False]]

      where
        elemStmt _ ty = R.Return $ R.Binary "==" (deref "lelem" ty) (deref "relem" ty)

        deref n ty = R.Dereference $
                       R.Call (R.Variable $ R.Specialized [R.Pointer ty] $ R.Name "reinterpret_cast")
                         [R.Variable $ R.Name n]

        elseStmt = R.Ignore $ R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"

    hashelem_fn :: [Int] -> [R.Type] -> CPPGenM (Maybe R.Definition)
    hashelem_fn tags types = do
      let void_ptr_t = R.Pointer $ R.Void
      return $ Just $ R.FunctionDefn (R.Name "hashelem")
                        [(Just "tag", tag_t), (Just "elem", void_ptr_t)]
                        (Just $ R.Named $ R.Name "static size_t")
                        []
                        False
                        [branch_chain "tag" tags types elseStmt elemStmt]

      where elemStmt _ ty = R.Return $ R.Call (R.Variable $ R.Name "hash_value")
                              [R.Dereference $
                                R.Call (R.Variable $ R.Specialized [R.Pointer ty] $ R.Name "reinterpret_cast")
                                  [R.Variable $ R.Name "elem"]]

            elseStmt = R.Ignore $  R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"

    elemsize_fn :: [Int] -> [R.Type] -> CPPGenM (Maybe R.Definition)
    elemsize_fn tags types =
      return $ Just $ R.FunctionDefn (R.Name "elemsize")
                        [(Just "tag", tag_t)]
                        (Just $ R.Named $ R.Name "size_t")
                        []
                        True
                        [branch_chain "tag" tags types elseStmt elemStmt]

      where elemStmt _ RElemUnit = R.Return $ R.Literal $ R.LInt 0
            elemStmt _ ty = R.Return $ R.Call (R.Variable $ R.Name "sizeof") [R.ExprOnType ty]
            elseStmt = R.Ignore $  R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"

    elemappend_fn :: [Int] -> [R.Type] -> CPPGenM (Maybe R.Definition)
    elemappend_fn tags types =
      return $ Just $ R.FunctionDefn (R.Name "elemappend")
                        [(Just "tag", tag_t), (Just "data", char_ptr_t)]
                        (Just $ R.Void)
                        []
                        False
                        [branch_chain "tag" tags types elseStmt elemStmt]

      where elemStmt tg ty = R.Ignore $ R.Call
                              (R.Variable $ suqualnm $ R.Specialized [ty] $ R.Name "template append")
                              [R.Literal $ R.LInt tg, castExpr $ R.Pointer ty]
            castExpr ty = R.Dereference $ R.Call (reinterpret_cast_expr ty) [R.Variable $ R.Name "data"]
            elseStmt = R.Ignore $ R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"

    externalize_fn :: [Int] -> [R.Type] -> CPPGenM (Maybe R.Definition)
    externalize_fn tags types =
      return $ Just $ R.FunctionDefn (R.Name "externalize")
                        [(Just "e", R.Reference externalizer_t),
                         (Just "tag", tag_t),
                         (Just "data", char_ptr_t)]
                        (Just $ R.Named $ R.Name "static void")
                        []
                        False
                        [branch_chain "tag" tags types elseStmt elemStmt]

      where elemStmt _ ty = R.Ignore $ R.Call (R.Project (castExpr $ R.Pointer ty) $ R.Name "externalize") [R.Variable $ R.Name "e"]
            castExpr ty = R.Dereference $ R.Call (reinterpret_cast_expr ty) [R.Variable $ R.Name "data"]
            elseStmt = R.Ignore $  R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"


    internalize_fn :: [Int] -> [R.Type] -> CPPGenM (Maybe R.Definition)
    internalize_fn tags types =
      return $ Just $ R.FunctionDefn (R.Name "internalize")
                        [(Just "i", R.Reference internalizer_t),
                         (Just "tag", tag_t),
                         (Just "data", char_ptr_t)]
                        (Just $ R.Named $ R.Name "static void")
                        []
                        False
                        [branch_chain "tag" tags types elseStmt elemStmt]

      where elemStmt _ ty = R.Ignore $ R.Call (R.Project (castExpr $ R.Pointer ty) $ R.Name "internalize") [R.Variable $ R.Name "i"]
            castExpr ty = R.Dereference $ R.Call (reinterpret_cast_expr ty) [R.Variable $ R.Name "data"]
            elseStmt = R.Ignore $  R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"

    yamlencode_fn tags types =
      return $ Just $ R.FunctionDefn (R.Name "yamlencode")
                        [(Just "tag", tag_t), (Just "idx", R.Primitive R.PInt), (Just "offset", size_type)]
                        (Just yaml_node_type)
                        []
                        True
                        [R.Forward $ R.ScalarDecl (R.Name "node") yaml_node_type Nothing,
                         yaml_map_insert [R.Literal $ R.LString "tag", R.Variable $ R.Name "tag"],
                         branch_chain "tag" tags types elseStmt elemStmt,
                         R.Return $ R.Variable $ R.Name "node"]

      where elemStmt _ ty = yaml_map_insert [R.Literal $ R.LString "elem", typed_at ty]

            elseStmt = R.Ignore $ R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"

            typed_at ty = R.Call
                            (R.Variable $ suqualnm $ R.Specialized [ty] $ R.Name "template at")
                            [R.Variable $ R.Name "idx", R.Variable $ R.Name "offset"]


    yamldecode_fn tags types =
      return $ Just $ R.FunctionDefn (R.Name "yamldecode")
                        [(Just "node", R.Reference yaml_node_type)]
                        (Just $ R.Void)
                        []
                        False
                        [R.Forward $ R.ScalarDecl (R.Name "tag") tag_t $ Just $
                           R.Call (R.Project (R.Variable $ R.Name "node[\"tag\"]") $ R.Specialized [tag_t] $ R.Name "as") [],
                         branch_chain "tag" tags types elseStmt elemStmt]

      where elemStmt _ ty = R.Ignore $ R.Call
                              (R.Variable $ suqualnm $ R.Specialized [ty] $ R.Name "template append")
                              [R.Variable $ R.Name "tag", typed_elem ty]

            typed_elem ty = R.Call (R.Project (R.Variable $ R.Name "node[\"elem\"]") $ R.Specialized [ty] $ R.Name "as") []

            elseStmt = R.Ignore $ R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"


    jsonencode_fn tags types =
      return $ Just $ R.FunctionDefn (R.Name "jsonencode")
                  [(Just "tag", tag_t), (Just "idx", R.Primitive R.PInt), (Just "offset", size_type), (Just "al", R.Reference json_alloc_type)]
                  (Just json_node_type)
                  []
                  True
                  [R.Forward $ R.ScalarDecl (R.Name "node") json_node_type Nothing,
                   json_set_object "node",
                   json_add_member "node"
                      (R.Literal $ R.LString "tag")
                      (R.Variable $ R.Name "tag")
                      (R.Variable $ R.Name "al"),
                   branch_chain "tag" tags types elseStmt elemStmt,
                   R.Return $ R.Variable $ R.Name "node"]

      where elemStmt _ ty = json_add_member "node"
                              (R.Literal $ R.LString "elem")
                              (R.Call (json_convert_fn ty) [typed_at ty, R.Variable $ R.Name "al"])
                              (R.Variable $ R.Name "al")

            elseStmt = R.Ignore $ R.ThrowRuntimeErr $ R.Literal $ R.LString "Invalid poly buffer tag"

            typed_at ty = R.Call
                            (R.Variable $ suqualnm $ R.Specialized [ty] $ R.Name "template at")
                            [R.Variable $ R.Name "idx", R.Variable $ R.Name "offset"]


    elem_type = R.Named $ R.Name "__CONTENT"
    size_type = R.Named $ R.Name "size_t"

    ldqualnm n     = R.Qualified (R.Name "K3") $ R.Qualified (R.Name "Libdynamic") n
    suqualnm n     = R.Qualified (R.Name "Super") n

    tag_t          = R.Named $ R.Name "uint16_t" -- R.Named $ pbqualnm $ R.Name "Tag"
    externalizer_t = R.Named $ ldqualnm $ R.Name "BufferExternalizer" -- R.Named $ pbqualnm $ R.Name "ExternalizerT"
    internalizer_t = R.Named $ ldqualnm $ R.Name "BufferInternalizer" -- R.Named $ pbqualnm $ R.Name "InternalizerT"
    char_ptr_t = R.Pointer $ R.Named $ R.Name "char"

    reinterpret_cast_expr t = R.Variable $ R.Specialized [t] $ R.Name "reinterpret_cast"

    yaml_node_type = R.Named $ R.Qualified (R.Name "YAML") $ R.Name "Node"
    json_node_type = R.Named $ R.Qualified (R.Name "rapidjson") $ R.Name "Value"
    json_alloc_type = R.Named $ R.Qualified (R.Name "rapidjson") $ R.Qualified (R.Name "Value") $ R.Name "AllocatorType"

    yaml_map_insert args = R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "node") $ R.Name "force_insert") args

    json_convert_fn ty = R.Variable $ R.Qualified (R.Name "JSON") $ R.Qualified (R.Specialized [ty] $ R.Name "convert") $ R.Name "encode"
    json_set_object n = R.Ignore $ R.Call (R.Project (R.Variable $ R.Name n) $ R.Name "SetObject") []
    json_add_member n k v a = R.Ignore $ R.Call (R.Project (R.Variable $ R.Name n) $ R.Name "AddMember") [k, v, a]

    branch_chain n tags types elseStmt f = (\foldF -> foldl foldF elseStmt $ zip tags types) $
      \accStmt (tg,ty) -> R.IfThenElse (R.Binary "==" (R.Variable $ R.Name n) (R.Literal $ R.LInt tg)) [f tg ty] [accStmt]


    is_tag (DProperty (dPropertyName -> "Tag")) = True
    is_tag _ = False

    tag_value (DProperty (dPropertyValue -> Just (tag -> LInt i))) = Just i
    tag_value _ = Nothing

    get_at_return_type :: K3 Type -> Maybe (K3 Type)
    get_at_return_type (PTFunction _ (PTFunction _ rt _) _) = Just rt
    get_at_return_type _ = Nothing

    get_safe_at_type :: K3 Type -> Maybe (K3 Type)
    get_safe_at_type (PTFun4 _ _ _ (PTFunction rt _ _) _ _ _ _ _) = Just rt
    get_safe_at_type _ = Nothing

    get_unsafe_at_type :: K3 Type -> Maybe (K3 Type)
    get_unsafe_at_type (PTFun3 _ _ (PTFunction rt _ _) _ _ _ _) = Just rt
    get_unsafe_at_type _ = Nothing

    get_append_type :: K3 Type -> Maybe (K3 Type)
    get_append_type (PTFunction vt _ _) = Just vt
    get_append_type _ = Nothing

    get_iterate_type :: K3 Type -> Maybe (K3 Type)
    get_iterate_type (PTFun3 _ _ (PTFun3 _ _ rt _ _ _ _) _ _ _ _) = Just rt
    get_iterate_type _ = Nothing

    get_fold_type :: K3 Type -> Maybe (K3 Type)
    get_fold_type (PTFun3 _ _ (PTFun4 _ _ _ rt _ _ _ _ _) _ _ _ _) = Just rt
    get_fold_type _ = Nothing


{- Pattern synonyms for index functions. -}

pattern PTRecord ids ch anns = Node (TRecord ids :@: anns) ch
pattern PTFunction arg rt anns = Node (TFunction :@: anns) [arg, rt]
pattern PTFun2 arg1 arg2 rt anns1 anns2 = PTFunction arg1 (PTFunction arg2 rt anns2) anns1
pattern PTFun3 arg1 arg2 arg3 rt anns1 anns2 anns3 = PTFunction arg1 (PTFun2 arg2 arg3 rt anns2 anns3) anns1
pattern PTFun4 arg1 arg2 arg3 arg4 rt anns1 anns2 anns3 anns4 = PTFunction arg1 (PTFun3 arg2 arg3 arg4 rt anns2 anns3 anns4) anns1
