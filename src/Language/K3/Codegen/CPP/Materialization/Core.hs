{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE StandaloneDeriving #-}

module Language.K3.Codegen.CPP.Materialization.Core where

import Data.Tree

import Text.Printf

import Language.K3.Core.Annotation
import Language.K3.Core.Common

import Language.K3.Utils.Pretty

import Language.K3.Codegen.CPP.Materialization.Hints (Method(..), Direction(..))

data MExpr
  = MVar Juncture Direction
  | MAtom Method
  | MIfThenElse (K3 MPred)
 deriving (Eq, Read, Show)

data instance Annotation MExpr = MEReason String

deriving instance Eq (Annotation MExpr)
deriving instance Read (Annotation MExpr)
deriving instance Show (Annotation MExpr)

isMEReason :: Annotation MExpr -> Bool
isMEReason (MEReason _) = True
isMEReason _ = False

instance Pretty (K3 MExpr) where
  prettyLines (Node (t :@: as) cs) = case t of
    MVar (Juncture u i) d -> [printf "MVar %d/%s/%s%s" (gUID u) i (show d) reason]
    MAtom m -> [printf "MAtom %s%s" (show m) reason]
    MIfThenElse p -> [printf "MIfThenElse%s" reason] ++ ["|"] ++ (shift "+- " "|  " $ prettyLines p) ++ drawSubTrees cs
   where
    reason = maybe "" (\(MEReason s) -> printf " -??- %s" s) (as @~ isMEReason)

ppShortE :: K3 MExpr -> String
ppShortE m = case tag m of
  MVar (Juncture u i) d -> printf "%d/%s/%s" (gUID u) i (show d)
  MAtom t -> show t
  MIfThenElse p ->
    let [t, e] = children m
    in printf "if %s then %s else %s" (ppShortP p) (ppShortE t) (ppShortE e)

data MPred
  = MNot
  | MAnd
  | MOr
  | MOneOf (K3 MExpr) [Method]
  | MBool Bool
 deriving (Eq, Read, Show)

data instance Annotation MPred = MPReason String

deriving instance Eq (Annotation MPred)
deriving instance Read (Annotation MPred)
deriving instance Show (Annotation MPred)

isMPReason :: Annotation MPred -> Bool
isMPReason (MPReason _) = True
isMPReason _ = False

instance Pretty (K3 MPred) where
  prettyLines (Node (t :@: as) cs) = case t of
    MBool b -> [printf "MBool %s%s" (show b) reason]
    MOneOf m ms -> [printf "MOneOf %s%s" (show ms) reason] ++ ["|"] ++ shift "`- " "   " (prettyLines m)
    _ -> [show t ++ reason] ++ drawSubTrees cs
   where
    reason = maybe "" (\(MPReason s) -> printf " -??- %s" s) (as @~ isMPReason)

ppShortP :: K3 MPred -> String
ppShortP p = case (tag p, children p) of
  (MNot, [c]) -> printf "-!- %s" (ppShortP c)
  (MAnd, [a, b]) -> printf "%s -&&- %s" (ppShortP a) (ppShortP b)
  (MOr, [a, b]) -> printf "%s -||- %s" (ppShortP a) (ppShortP b)
  (MOneOf m ms, _) -> printf "%s ∈ %s" (ppShortE m) (show ms)
  (MBool b, _) -> show b

data Juncture = Juncture UID Identifier deriving (Eq, Ord, Read, Show)

class Explainable a where
  (-??-) :: a -> String -> a

instance Explainable (K3 MExpr) where
  (-??-) e r = e @+ (MEReason r)

instance Explainable (K3 MPred) where
  (-??-) e r = e @+ (MPReason r)

infixl 9 -??-
