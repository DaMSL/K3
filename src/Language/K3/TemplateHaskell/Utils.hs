{-
  This module contains some handy Template Haskell utilities.
-}
module Language.K3.TemplateHaskell.Utils
( applyTypeCon
, mkPrefixNames
, mkFnType
, canonicalType
, getDataArgTypes
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
      DataD _ _ tvars _ _ -> do
        let (typs,bndrs) = unzip $ map perBinding tvars
        return (applyTypeCon (ConT tname) typs, bndrs)
        where
          perBinding :: TyVarBndr -> (Type,TyVarBndr)
          perBinding tvb = case tvb of
            PlainTV n -> (VarT n, PlainTV n)
            KindedTV n k -> (VarT n, KindedTV n k)              
      _ -> error $ "canonicalType: " ++ show dec
              ++ " is not a normal constructor"
    _ -> error $ "canonicalType: " ++ show tname ++ " is not a type constructor"

-- |Determines, for a given @Info@ representing a data type, *all* types of
--  arguments that any of its constructors could accept.
getDataArgTypes :: Info -> [Q Type]
getDataArgTypes info = case info of
  TyConI dec -> case dec of
    DataD _ _ _ cons _ ->
      map return $ nub $ concatMap typesOfCon cons
    _ -> error $ "defineCatFunc: " ++ show info
            ++ " is not a data decl"
  _ -> error $ "defineCatFunc: " ++ show info
        ++ " is not a type constructor"
  where
    -- |Retrieves the types of the arguments of a constructor.
    typesOfCon :: Con -> [Type]
    typesOfCon con = case con of
      NormalC _ sts -> map snd sts
      _ -> error $ "defineCatFunc: " ++ show con
            ++ " is not a normal constructor"
