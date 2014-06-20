{-
  This module contains some handy Template Haskell utilities.
-}
module Language.K3.Utils.TemplateHaskell.Utils
( applyTypeCon
, mkPrefixNames
, mkFnType
, canonicalType
, getDataArgTypes
, getConstructors
, getNameOfType
) where

import Data.List

import Language.Haskell.TH

-- |A utility function to apply type constructors.  The given type is treated
--  as a type constructor and applied to each of the arguments in the list.
applyTypeCon :: Type -> [Type] -> Type
applyTypeCon = foldl AppT

-- |Creates a generic list of names with the provided prefix.  This list is
--  coinductive: it does not have an end.
mkPrefixNames :: String -> [Q Name]
mkPrefixNames s =
  map (newName . (s ++) . ("__" ++) . show) [0::Int ..]

-- |A utility function to create function types.  The result is a function of
--  all of the types in the list except the last, which is the output type.
mkFnType :: [Type] -> Type
mkFnType typs = case typs of
  [] -> error "mkFnType cannot operate with empty list of types"
  [typ] -> typ
  typ:typs' -> AppT (AppT ArrowT typ) $ mkFnType typs'

-- |Builds a canonical type from the given type name.
canonicalType :: Name -> Q (Type,[TyVarBndr])
canonicalType tname = do
  info <- reify tname
  case info of
    TyConI dec -> case dec of
      NewtypeD _ _  tvars _ _ ->
        simpleTvarsCase tvars
      DataD _ _ tvars _ _ ->
        simpleTvarsCase tvars
      TySynD _ bndrs typ -> return (typ,bndrs)
      _ -> error $ "canonicalType: " ++ show dec
              ++ " is not a normal constructor or alias to one"
    _ -> error $ "canonicalType: " ++ show tname ++ " is not a type constructor"
  where
    simpleTvarsCase tvars = do
      let (typs,bndrs) = unzip $ map perBinding tvars
      return (applyTypeCon (ConT tname) typs, bndrs)
      where
        perBinding :: TyVarBndr -> (Type,TyVarBndr)
        perBinding tvb = case tvb of
          PlainTV n -> (VarT n, PlainTV n)
          KindedTV n k -> (VarT n, KindedTV n k)

-- |Determines, for a given @Info@ representing a data type, *all* types of
--  arguments that any of its constructors could accept.
getDataArgTypes :: Info -> Q [Type]
getDataArgTypes info = do
  cons <- getConstructors info
  mapM return $ nub $ concatMap typesOfCon cons
  where
    -- |Retrieves the types of the arguments of a constructor.
    typesOfCon :: Con -> [Type]
    typesOfCon con = case con of
      NormalC _ sts -> map snd sts
      _ -> error $ "getDataArgTypes: " ++ show con
            ++ " is not a normal constructor"

-- |Retrieves data constructors for a type info.
getConstructors :: Info -> Q [Con]
getConstructors info = case info of
  TyConI dec -> case dec of
    NewtypeD _ _ _ con _ -> return [con]
    DataD _ _ _ cons _ -> return cons
    TySynD _ _ typ ->
      getConstructors =<< reify (getNameOfType typ)
    _ -> error $ "getConstructors: " ++ show info
            ++ " is not a data decl or alias to one"
  _ -> error $ "getConstructors: " ++ show info
        ++ " is not a type constructor"

-- |Obtains the name of a given type.
getNameOfType :: Type -> Name
getNameOfType t = case t of
  ForallT _ _ t' -> getNameOfType t'
  AppT t' _ -> getNameOfType t'
  SigT t' _ -> getNameOfType t'
  VarT n -> n
  ConT n -> n
  PromotedT n -> n
  TupleT _ -> error "don't know name for tuple types"
  UnboxedTupleT _ -> error "don't know name for tuple types"
  ArrowT -> error "don't know name for arrow types"
  ListT -> error "don't know name for list types"
  PromotedTupleT _ -> error "don't know name for tuple types"
  PromotedNilT -> error "don't know name for list types"
  PromotedConsT -> error "don't know name for list types"
  StarT -> error "don't know name for star types"
  ConstraintT -> error "don't know name for constraint types"
  LitT _ -> error "don't know name for literal types"
