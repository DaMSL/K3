{-|
  Contains generic utility types required by the K3 type system data types.
-}
module Language.K3.TypeSystem.Data.Utils
( Coproduct(..)
) where

{-|
  An unbiased form of @Either@.  This type is meant to represent a sum type
  which, unlike @Either@, treats both sides equally.
-}
data Coproduct a b
  = CLeft a
  | CRight b
  deriving (Eq, Ord, Read, Show)
