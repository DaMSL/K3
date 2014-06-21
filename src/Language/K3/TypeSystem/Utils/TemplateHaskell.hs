{-# LANGUAGE TemplateHaskell #-}

{-|
  A module containing Template Haskell utility functions which are useful to
  the type system.
-}
module Language.K3.TypeSystem.Utils.TemplateHaskell
( mkAssertChildren
) where

import Language.Haskell.TH

import Language.K3.Core.Annotation

-- |A function which produces a declaration for a function @assertNChildren@.
--  This assertion accepts a K3 tree and requires that it has that many
--  children.  If it does, the result is returned.  Otherwise, the provided
--  error expression is evaluated.
mkAssertChildren :: (Name -> Q Type -> Q Type)
                      -- ^A function which, given the name of the K3 tree type
                      --  parameter and the resulting tuple type, will produce
                      --  the type for the function.
                 -> (Q Exp -> Q Exp)
                      -- ^A function which, given an expression referring to the
                      --  tree, will produce an expression to be evaluated when
                      --  that tree does not have the correct number of
                      --  children.
                 -> (Q Exp -> Q Exp)
                      -- ^A function taking a correctly-generated tuple and
                      --  generating an expression representing the success
                      --  case.  This is useful if the result is in a monad.
                 -> Int -- ^The number of children to require
                 -> Q [Dec] -- ^The declarations for this function.
mkAssertChildren ftypeDeclFn errExp succExpFn n = do
  let suffixString = if n /= 1 then "Children" else "Child"
  let fname = mkName $ "assert" ++ show n ++ suffixString
  let checkChildrenFnName = mkName $ "check" ++ show n ++ suffixString
  typN <- newName "a"
  typ <- varT typN
  let tupTyp = foldl appT (tupleT n) $ replicate n $ [t|K3 $(return typ)|]
  ftype <- ftypeDeclFn typN tupTyp
  let signature = sigD fname $ return ftype
  let bodyExp =
        [| \tree -> case $(varE checkChildrenFnName) tree of
                      Nothing -> $(errExp [|tree|])
                      Just r -> $(succExpFn [|r|]) |]
  let cl = clause [] (normalB bodyExp) []
  let impl = funD fname [cl]
  sequence [signature,impl]
