{-# LANGUAGE ViewPatterns #-}

-- Profiling transformation code
module Language.K3.Transform.Profiling (
  addProfiling
) where

import Prelude hiding (id, exp)
import Control.Monad.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import Language.K3.Core.Constructor.Expression
import qualified Language.K3.Core.Constructor.Type as T
import qualified Language.K3.Core.Constructor.Declaration as D

sec :: String
sec = "sec"
nsec :: String
nsec = "nsec"

-- Get a a list of triggers and global functions
declMap :: K3 Declaration -> [String]
declMap ds = runIdentity $ foldTree add [] ds
  where
    add :: [String] -> K3 Declaration -> Identity [String]
    add acc (tag -> DGlobal id _ (Just e)) | isLambda e = return $ id:acc
    add acc (tag -> DTrigger id _ _) = return $ id:acc
    add acc _ = return $ acc

isLambda :: K3 Expression -> Bool
isLambda (tag -> ELambda _) = True
isLambda _                  = False

-- Convert a trigger/global_fn id to a global variable for time keeping
globalId :: String -> String
globalId id = "_time_"++id

-- Code to create a variable initialized to a timestamp of 0
globalVar :: String -> K3 Declaration
globalVar id = D.global (globalId id) typ (Just exp)
  where
    typ = T.mut $ T.record [(sec, T.mut T.int), (nsec, T.mut T.int)]
    exp = mut $ record [(sec, mut $ constant $ CInt 0), (nsec, mut $ constant $ CInt 0)]

-- Wrap declarations with time expressions before and after
wrapDecls :: K3 Declaration -> K3 Declaration
wrapDecls ds = runIdentity $ mapTree wrap ds
  where
    wrap ch (details -> (DGlobal id t (Just expr), _, annos)) | isLambda expr =
      return $ Node (DGlobal id t (Just $ wrapCode id expr) :@: annos) ch
    wrap ch (details -> (DTrigger id t expr, _, annos)) =
      return $ Node (DTrigger id t (wrapCode id expr) :@: annos) ch
    wrap ch (Node x _) = return $ Node x ch

-- Wrap a specific expression before and after
wrapCode :: String -> K3 Expression -> K3 Expression
wrapCode id expr =
  let tempVar = "__timeVar"
      gTimeVar = globalId id  -- global var for this declaration
  in
  letIn tempVar (immut $ binop OApp (variable "now") $ immut unit) $
    block
      [expr,
      bindAs
        (immut $ variable gTimeVar)
        (BRecord [("time", "t"), ("count", "c")]) $
          block
            [assign "t" $
              binop OApp
                (binop OApp (variable "add_time")
                  (variable "t")) $
                binop OApp
                  (binop OApp (variable "sub_time")
                    (binop OApp (variable "now") $ immut unit)) $
                  (variable tempVar),
              -- Increment the counter
              assign "c" $
                binop OAdd
                  (variable "c") $
                  immut $ constant $ CInt 1]]

-- Transform code to have profiling code bits added
addProfiling :: K3 Declaration -> K3 Declaration
addProfiling p = appendGlobals $ wrapDecls p
  where
    appendGlobals decs@(details -> (DRole id, ds, annos)) =
      -- Get a list of declarations, and add the variables
      let vars = map globalVar $ declMap decs
      in Node (DRole id :@: annos) (vars++ds)

    appendGlobals x = error $ "Expected role but found "++show x

