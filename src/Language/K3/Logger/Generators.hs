{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

{-|
  This module contains Template Haskell utilities which will produce
  module-specific logging functions.
-}
module Language.K3.Logger.Generators
( loggingFunctions
) where

import Control.Applicative
import Language.Haskell.TH
import System.Log
import System.Log.Logger

import Language.K3.Logger.Operations
import Language.K3.Pretty

{-|
  A set of logging functions for the current module.  These logging functions
  are generated as follows:
    * Inline logging functions of the form @_debugI@ with type
      @String -> a -> a@.  The priority of the message is described
      by the function name; the logger's name is the same as the module's name.
      Similar functions exist for each logging level (e.g. @_warningI@).
    * Inline value logging functions of the form @_debugIPretty@ with type
      @(Pretty a) => String -> a -> a@.  In this case, the string is merely
      a message prefix; the logged message is that prefix followed by the
      display of the given value.
    * Monadic logging functions of the form @_debug@ with type
      @(Monad m) => String -> m ()@.
    * Monadic value logging functions of the form @_debugPretty@ with type
      @(Monad m, Display a) => String -> a -> m()@.
-}
loggingFunctions :: Q [Dec]
loggingFunctions = do
  let levels = [ ("_debug", [|DEBUG|])
               , ("_info", [|INFO|])
               , ("_notice", [|NOTICE|])
               , ("_warning", [|WARNING|])
               , ("_error", [|ERROR|])
               , ("_critical", [|CRITICAL|])
               , ("_alert", [|ALERT|])
               , ("_emergency", [|EMERGENCY|])
               ]
  let modes = [ ("I", [|k3logI|],
                  [t| forall a. String -> a -> a |])
              , ("IPretty", [|k3logIPretty|],
                  [t| forall a. (Pretty a) => String -> a -> a |])
              , ("", [|k3logM|],
                  [t| forall m. (Monad m) => String -> m () |])
              , ("Pretty", [|k3logMPretty|],
                  [t| forall m a. (Monad m, Pretty a) => String -> a -> m () |])
              ]
  concat <$> sequence [ loggingFunction mode level
                      | mode <- modes
                      , level <- levels
                      ]
  where
    moduleNameExpr :: Q Exp
    moduleNameExpr = LitE <$> StringL <$> loc_module <$> location
    loggingFunction :: (String,Q Exp,Q Type) -> (String, Q Exp) -> Q [Dec]
    loggingFunction (nameSuffix,baseFn,typ) (namePart,prioExpr) = do
      let name = mkName $ namePart ++ nameSuffix
      let signature = sigD name typ
      let decl = funD name [clause [] (normalB
                    [| $(baseFn) $(moduleNameExpr) $(prioExpr) |]) []]
      sequence [signature, decl]
