{-|
  Contains a generic type required by the K3 type system data types.
-}
module Language.K3.TypeSystem.Data.Coproduct
( Coproduct(..)
) where

import Language.K3.Pretty

{-|
  An unbiased form of @Either@.  This type is meant to represent a sum type
  which does /not/ have the left-side-error implications that @Either@ does: it
  is not a monad, for instance.
-}
data Coproduct a b
  = CLeft a
  | CRight b
  deriving (Eq, Ord, Read, Show)

instance (Pretty a, Pretty b) => Pretty (Coproduct a b) where
  prettyLines x = case x of
                    CLeft y -> prettyLines y
                    CRight y -> prettyLines y
