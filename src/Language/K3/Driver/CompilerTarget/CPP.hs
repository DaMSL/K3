{-# LANGUAGE LambdaCase #-}

module Language.K3.Driver.CompilerTarget.CPP (compile) where

import Prelude hiding ((*>))

import Control.Monad

import qualified Data.List as L
import Data.Maybe

import System.FilePath (joinPath)

import Development.Shake
import Development.Shake.FilePath hiding ( joinPath, replaceExtension, takeBaseName )
import Development.Shake.Util

import Text.PrettyPrint.ANSI.Leijen hiding ((</>), (<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import qualified Language.K3.Utils.Pretty as P
import qualified Language.K3.Utils.Pretty.Syntax as PS

import qualified Language.K3.Codegen.Imperative as I
import qualified Language.K3.Codegen.CPP as CPP

import Language.K3.Driver.Common
import Language.K3.Driver.Options

type CompileContinuation = (K3 Declaration -> IO ()) -> K3 Declaration -> IO ()

buildOutputFilePath :: String -> Options -> CompileOptions -> Either String (FilePath, FilePath)
buildOutputFilePath ext opts copts = case buildDir copts of
    Nothing   -> Left "Error: no build directory specified."
    Just path -> Right $ outputFilePath path (inputProgram $ input opts) ext

cppOutFile :: Options -> CompileOptions -> Either String [FilePath]
cppOutFile opts copts = either Left (\(_,f) -> Right [f]) $ buildOutputFilePath "cpp" opts copts

-- Generate C++ code for a given K3 program.
cppCodegenStage :: Options -> CompileOptions -> (CompileContinuation, K3 Declaration) -> IO ()
cppCodegenStage opts copts (cont, prog) = genCPP irRes
  where
    --attach trigger symbols. TODO: mangle names before applying this transformation.
    -- TODO move this into core. Must happen before imperative generation.

    (irRes, initSt) = I.runImperativeM (I.declaration prog) I.defaultImperativeS

    genCPP (Right cppIr) = cont genCPPCont cppIr
    genCPP (Left _)      = putStrLn "Error in Imperative Transformation."

    genCPPCont p = do
      saveASTOutputs p
      outputCPP $ fst $ CPP.runCPPGenM (CPP.transitionCPPGenS initSt) (CPP.stringifyProgram p)

    saveASTOutputs p = do
      when (saveAST    $ input opts) (outputAST P.pretty "k3ast" (astPrintMode copts) p)
      when (saveRawAST $ input opts) (outputAST show     "k3ar"  (astPrintMode copts) p)
      when (saveSyntax $ input opts) (outputAST show     "k3s"   PrintSyntax          p)

    outputCPP (Right doc) =
      either putStrLn (outputDoc doc) $ buildOutputFilePath "cpp" opts copts

    outputCPP (Left (CPP.CPPGenE e)) = putStrLn e

    outputAST toStr ext printMode p =
      either putStrLn (outputStrFile $ formatAST toStr printMode p)
        $ buildOutputFilePath ext opts copts

    -- Print out the program
    formatAST toStr (PrintAST st se sc sp) p =
      let filterF = catMaybes $
                     [if st && se then Just stripTypeAndEffectAnns
                      else if st  then Just stripTypeAnns
                      else if se  then Just stripEffectAnns
                      else Nothing]
                      ++ [if sc then Just stripComments else Nothing]
                      ++ [if sp then Just stripProperties else Nothing]
      in toStr $ foldl (flip ($)) p filterF

    formatAST _ PrintSyntax p = either syntaxError id $ PS.programS p
    syntaxError s = "Could not print program: " ++ s

    outputDoc doc (path, file) = outputStrFile (displayS (renderPretty 1.0 100 doc) "") (path, file)


cppBinaryStage :: Options -> CompileOptions -> [FilePath] -> IO ()
cppBinaryStage _ copts sourceFiles =
  case buildDir copts of
    Nothing   -> putStrLn "No build directory specified."
    Just path -> binary path >> putStrLn ("Created binary file: " ++ joinPath [path, pName])

  where binary bDir =
          shake shakeOptions{shakeFiles = bDir, shakeThreads = buildJobs copts} $ do
            want [bDir </> pName <.> exe]

            phony "clean" $ removeFilesAfter bDir ["//*"]

            bDir </> pName <.> exe *> \out -> do
              runtimeFiles <- getDirectoryFiles (runtimePath copts) ["//*.cpp"]
              let runtimeFiles' = [runtimePre </> f | f <- pruneBadSubDirs runtimeFiles]
                  allFiles = runtimeFiles' ++ sourceFiles
                  objects = [bDir </> src -<.> "o" | src <- allFiles]
              need objects
              cmd cc ["-o"] [out] objects (filterLinkOptions $ words $ cppOptions copts)

            bDir ++ "//*.o" *> \out -> do
              let source = fixRuntime $ dropDirectory1 $ out -<.> "cpp"
              let deps   = out -<.> "m"
              () <- cmd cc ["-std=c++1y"] ["-c"] [source] ["-o"] [out] ["-MMD", "-MF"] [deps] (filterCompileOptions $ words $ cppOptions copts)
              needMakefileDependencies deps

        fixRuntime x   = if isRuntime x then substRuntime x else x
        substRuntime x = runtimePath copts ++ drop runtimeLen x
        isRuntime x    = take runtimeLen x == runtimePre
        runtimeLen     = length runtimePre
        runtimePre     = "runtime"

        badSubDirs = [  "Eigen",
                        "dataspace",
                        "test"
                     ]

        hasBadSubDir s = foldr (\bad acc -> acc || bad `L.isInfixOf` s) False badSubDirs

        pruneBadSubDirs = filter (not . hasBadSubDir)

        -- Options that aren't in the lists are always included in both
        compilePrefix = ["-I", "-D"]
        linkPrefix = ["-l", "-L"]

        hasPrefixIn l x = foldr (\pre acc -> acc || pre `L.isPrefixOf` x) False l

        filterLinkOptions = filter (not . hasPrefixIn compilePrefix)
        filterCompileOptions = filter (not . hasPrefixIn linkPrefix)

        pName    = programName copts
        cc       = case ccCmd copts of
                    GCC   -> "g++"
                    Clang -> "clang++"

-- Generate C++ code for a given K3 program.
compile :: Options -> CompileOptions -> CompileContinuation -> K3 Declaration -> IO ()
compile opts copts ccont prog =
    case ccStage copts of
      Stage1    -> do
        cppCodegenStage opts copts (ccont, prog)
        putStrLn $ "Created source file: " ++ programName copts ++ ".cpp"
      Stage2    -> do
        either putStrLn (cppBinaryStage opts copts) $ cppOutFile opts copts
      AllStages -> do
        cppCodegenStage opts copts (ccont, prog)
        either putStrLn (cppBinaryStage opts copts) $ cppOutFile opts copts
