{-# LANGUAGE ScopedTypeVariables #-}

{-|
  This module contains simple sanity checks for the type system..
-}
module Language.K3.TypeSystem.Sanity
( sanityCheck
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Data.Function
import Data.List
import Data.Maybe
import Data.Monoid
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.TypeError

-- |Performs basic sanity checking on a K3 AST.  This function eliminates
--  some basic error cases early, simplifying the other portions of the type
--  system.
sanityCheck :: forall m. (TypeErrorI m, Functor m, Monad m)
            => K3 Declaration -> m ()
sanityCheck decl =
  case tag decl of
    DRole _ -> do
      let decl's = subForest decl
      checkIdentifiedListForDupes
        (map (idOfDecl &&& id) decl's)
        MultipleDeclarationBindings
      -- TODO: check that annotations do not declare duplicate type identifiers
      mconcat <$> mapM checkMembersForDupes (mapMaybe unAnn decl's)
    _ -> internalTypeError $ TopLevelDeclarationNonRole decl
  where
    idOfDecl d = case tag d of
      DRole i -> i
      DGlobal i _ _ -> i
      DTrigger i _ _ -> i
      DDataAnnotation i _ _ -> i
      DCtrlAnnotation i _ _ -> i
    unAnn d = case tag d of
      DDataAnnotation _ _ mems -> Just mems
      _ -> Nothing
    checkMembersForDupes :: [AnnMemDecl] -> m ()
    checkMembersForDupes mems = do
      let (lmems,amems) = mconcat $ map processMem mems
      checkIdentifiedListForDupes lmems MultipleAnnotationBindings
      checkIdentifiedListForDupes amems MultipleAnnotationBindings
      where
        processMem mem = case mem of
          Lifted _ i _ _ _    -> ([(i, mem)], [])
          Attribute _ i _ _ _ -> ([], [(i, mem)])
          MAnnotation {}      -> ([],[])
    checkIdentifiedListForDupes
        :: forall a.
           [(Identifier, a)]
        -> (Identifier -> [a] -> TypeError)
        -> m ()
    checkIdentifiedListForDupes xs f =
      let dupes = filter ((>1) . length) $
                    groupBy ((==) `on` fst) $ sortBy (compare `on` fst) xs in
      unless (null dupes) $
        let (dupeId, ys) = first head $ unzip $ head dupes in
        typeError $ f dupeId ys

