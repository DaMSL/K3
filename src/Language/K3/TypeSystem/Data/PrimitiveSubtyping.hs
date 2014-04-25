{-|
  A module defining the primitive subtyping operation.
-}

module Language.K3.TypeSystem.Data.PrimitiveSubtyping
( isPrimitiveSubtype
) where

import Language.K3.TypeSystem.Data.Types

isPrimitiveSubtype :: ShallowType -> ShallowType -> Bool
isPrimitiveSubtype t1 t2 = case (t1,t2) of
  (SBool,SBool) -> True
  (SInt,SInt) -> True
  (SReal,SReal) -> True
  (SNumber,SNumber) -> True
  (SString,SString) -> True
  (SAddress,SAddress) -> True
  (SInt,SNumber) -> True
  (SReal,SNumber) -> True
  (SInt, SReal) -> True -- XXX: Is this the right way to do things?
  _ -> False
