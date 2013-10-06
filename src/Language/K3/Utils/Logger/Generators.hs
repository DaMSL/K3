{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

{-|
  This module contains Template Haskell utilities which will produce
  module-specific logging functions.
-}
module Language.K3.Utils.Logger.Generators
( loggerGenerator,
  loggingFunctions,
  customLoggingFunctions
) where

import Control.Applicative
import Data.List
import Language.Haskell.TH
import System.Log
import System.Log.Logger

import Language.K3.Utils.Logger.Operations
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
loggerGenerator :: String -> Q [Dec]
loggerGenerator suffixTag = do
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
  concat <$> sequence [ loggingFunction suffixTag mode level
                      | mode <- modes
                      , level <- levels
                      ]
  where
    moduleNameExpr :: String -> Q Exp
    moduleNameExpr tag = 
      let modNameE = LitE <$> StringL <$> loc_module <$> location in
      if null tag
         then [| $modNameE |]
         else [| ( $modNameE ++ $(litE $ stringL $ "#" ++ tag) ) |] 
    

    loggingFunction :: String -> (String,Q Exp,Q Type) -> (String, Q Exp) -> Q [Dec]
    loggingFunction tag (nameSuffix,baseFn,typ) (namePart,prioExpr) = do
      let fnName    = mkName $ namePart ++ nameSuffix ++ (if null tag then "" else "_"++tag)
      let logName   = moduleNameExpr tag
      let signature = sigD fnName typ
      let decl      = funD fnName [clause [] (normalB [| $(baseFn) $(logName) $(prioExpr) |]) []]
      sequence [signature, decl]

loggingFunctions :: Q [Dec]
loggingFunctions = loggerGenerator ""

customLoggingFunctions :: [String] -> Q [Dec]
customLoggingFunctions tags = concat <$> (mapM loggerGenerator $ nub tags)
