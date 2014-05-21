{-# LANGUAGE LambdaCase #-}

module Language.K3.Compiler.CPP (compile) where

import Data.Functor

import Control.Monad

import System.Directory (createDirectoryIfMissing)
import System.FilePath (joinPath, replaceExtension, takeBaseName)

import qualified Data.Sequence as S

import Development.Shake
import Development.Shake.FilePath hiding ( joinPath, replaceExtension, takeBaseName )
import Development.Shake.Util

import Text.PrettyPrint.ANSI.Leijen hiding ((</>), (<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import Language.K3.TypeSystem (typecheckProgram)

import qualified Language.K3.Codegen.Imperative as I
import qualified Language.K3.Codegen.CPP as CPP

import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck

type CompilerStage a b = Options -> CompileOptions -> a -> IO (Either String b)

continue :: Either String a -> (a -> IO (Either String b)) -> IO (Either String b)
continue e f = either (return . Left) f e

finalize :: (a -> String) -> Either String a -> IO ()
finalize f = either putStrLn (putStrLn . f)

prefixError :: String -> IO (Either String a) -> IO (Either String a)
prefixError message m = m >>= \case
  Left err -> return . Left $ message ++ " " ++ err
  Right r  -> return $ Right r

outputFilePath :: String -> Options -> CompileOptions -> Either String (FilePath, FilePath)
outputFilePath ext opts copts = case buildDir copts of
    Nothing   -> Left "Error: no build directory specified."
    Just path -> Right (path, joinPath [path, replaceExtension (takeBaseName $ input opts) ext])

typecheckStage :: CompilerStage (K3 Declaration) (K3 Declaration)
typecheckStage _ _ prog = prefixError "Type error:" $ return $
    if not $ S.null typeErrors
      then Left $ prettyTCErrors typedProgram typeErrors
      else Right typedProgram

  where (typeErrors, _, typedProgram) = typecheckProgram prog

cppCodegenStage :: CompilerStage (K3 Declaration) ()
cppCodegenStage opts copts typedProgram = prefixError "Code generation error:" $ genCPP irRes
  where
    (irRes, initSt)      = I.runImperativeM (I.declaration typedProgram) I.defaultImperativeS

    genCPP (Right cppIr) = outputCPP $ fst $ CPP.runCPPGenM (CPP.transitionCPPGenS initSt) (CPP.program cppIr)
    genCPP (Left _)      = return $ Left "Error in Imperative Transformation."

    outputCPP (Right doc) =
      either (return . Left) (\x -> Right <$> outputDoc doc x)
        $ outputFilePath "cpp" opts copts

    outputCPP (Left (CPP.CPPGenE e)) = return $ Left e

    outputDoc doc (path, file) = do
      createDirectoryIfMissing True path
      writeFile file (displayS (renderPretty 1.0 100 doc) "")

-- Generate C++ code for a given K3 program.
cppSourceStage :: Options -> CompileOptions -> K3 Declaration -> IO (Either String [FilePath])
cppSourceStage opts copts prog = do
    tcStatus    <- typecheckStage opts copts prog
    cgStatus    <- continue tcStatus $ cppCodegenStage opts copts
    continue cgStatus $ const $ return outFile

  where outFile = either Left (\(_,f) -> Right [f]) $ outputFilePath "cpp" opts copts

cppBinaryStage :: Options -> CompileOptions -> [FilePath] -> IO (Either String ())
cppBinaryStage _ copts sourceFiles = prefixError "Binary compilation error:" $
  case buildDir copts of
    Nothing   -> return $ Left "No build directory specified."
    Just path -> Right <$> binary path

  where binary bDir =
          shake shakeOptions{shakeFiles = bDir} $ do
            want [bDir </> pName <.> exe]

            phony "clean" $ removeFilesAfter bDir ["//*"]

            bDir </> pName <.> exe *> \out -> do
              let objects = [bDir </> src -<.> "o" | src <- sourceFiles]
              need objects
              cmd cc ["-o"] [out] objects incDirs libFlags

            bDir ++ "//*.o" *> \out -> do
              let source = dropDirectory1 $ out -<.> "cpp"
              let deps   = out -<.> "m"
              () <- cmd cc ["-std=c++11"] ["-c"] [source] ["-o"] [out] ["-MMD", "-MF"] [deps] incDirs
              needMakefileDependencies deps

        pName    = programName copts
        incDirs  = map ("-I"++) $ includeDirs copts
        libFlags = map (\(b,p) -> (if b then "-L" else "-l") ++ p) $ libraryOpts copts
        cc       = case ccCmd copts of
                    GCC   -> "g++"
                    Clang -> "clang++"
                    Source -> "true" -- Technically unreachable.

-- Generate C++ code for a given K3 program.
compile :: Options -> CompileOptions -> K3 Declaration -> IO ()
compile opts copts prog = do
    sourceStatus <- cppSourceStage opts copts prog
    unless (ccCmd copts == Source) $ do
        binStatus <- continue sourceStatus $ cppBinaryStage opts copts
        finalize (const $ "Created binary file: " ++ programName copts) binStatus
