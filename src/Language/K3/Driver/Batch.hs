{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Batch where

import Control.Arrow

import System.Console.Haskeline

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Expression as E
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Runtime.Engine
import Language.K3.Interpreter

import Language.K3.Driver.Common
import Language.K3.Driver.Console
import Language.K3.Driver.Options


setDefaultRole :: K3 Declaration -> String -> String -> K3 Declaration
setDefaultRole (tag &&& children -> (DRole roleName, subDecls)) targetName newDefault
    | roleName == targetName = D.role roleName (newDefaultDecl : filter matchRole subDecls)
  where
    newDefaultDecl = D.global "role" T.string (Just $ E.constant $ E.CString newDefault)
    matchRole (tag -> DGlobal "role" _ _) = False
    matchRole _ = True

setDefaultRole d _ _ = d

runBatch :: Options -> InterpretOptions -> K3 Declaration -> IO ()
runBatch _ interpOpts@(Batch asNetwork _ _ parallel printConf consoleOn) prog =
   if not asNetwork then do
      pStatus <- runProgram printConf parallel (sysEnv interpOpts) prog
      either (\err -> putStrLn $ message err)
             (\ns -> if consoleOn then runConsole $ map Right ns else return ())
             pStatus
   else do
      nodeStatuses <- runNetwork printConf parallel (sysEnv interpOpts) prog
      if consoleOn then runConsole nodeStatuses else return ()

  where runConsole = runInputT defaultSettings . console defaultPrompt 

runBatch _ _ _ = error "Invalid batch processing mode"


