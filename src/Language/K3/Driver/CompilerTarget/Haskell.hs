module Language.K3.Driver.CompilerTarget.Haskell where

import qualified Data.Sequence as Seq

import Distribution.Package
import Distribution.PackageDescription hiding ( includes )
import Distribution.PackageDescription.Parse
import Distribution.Version

import System.Directory
import System.FilePath

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import qualified Language.K3.Codegen.Haskell as CG
import Language.K3.TypeSystem

import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck

compile :: Options -> CompileOptions -> K3 Declaration -> IO ()
compile _ cOpts prog =
    let (errs, _, typedP) = typecheckProgram prog
    in if Seq.null errs then
      let source = mkSource name typedP in
      case source of
        Left e' -> compileError e'
        Right s -> case (outputFile cOpts, buildDir cOpts) of
                      (Just outP, Just buildP) -> doStages outP buildP s
                      (_,_)                    -> outputError

    else putStrLn $ prettyTCErrors typedP errs

  where
    name = programName cOpts

    compileError s = putStrLn $ "Could not generate code: " ++ s
    outputError    = putStrLn $ "No valid output file or build directory"

    mkSource n p = CG.compile (CG.generate n p)
    mkBuilder n = k3PackageDescription n

    doStages outP buildP src = do
      prepare name outP buildP src $ mkBuilder name
      build name buildP

k3PackageDescription :: String -> PackageDescription
k3PackageDescription progName = emptyPackageDescription {
      package = PackageIdentifier { pkgName    = PackageName progName
                                  , pkgVersion = Version [0] []}
    , author       = "The K3 Team"
    , synopsis     = "K3 binary for \"" ++ progName ++ "\""
    , description  = "K3 binary for \"" ++ progName ++ "\""
    , buildType    = Just Simple
    , buildDepends =
                [ Dependency (PackageName "base") anyVersion
                , Dependency (PackageName "containers") anyVersion
                , Dependency (PackageName "optparse-applicative") anyVersion
                , Dependency (PackageName "mtl") anyVersion
                , Dependency (PackageName "transformers") anyVersion
                , Dependency (PackageName "K3-Core") anyVersion ]
    , executables  = [Executable progName (progName <.> "hs") k3BuildInfo]
  }

  where k3BuildInfo = emptyBuildInfo {
              hsSourceDirs       = []
            , targetBuildDepends = []
          }

prepare :: String -> FilePath -> FilePath -> String -> PackageDescription -> IO ()
prepare name outP buildP source buildDesc = do
    createDirectoryIfMissing False buildP
    writePackageDescription buildFile buildDesc
    writeFile outFile source

  where buildFile = buildP </> name <.> "cabal"
        outFile   = buildP </> outP <.> "hs"

build :: String -> FilePath -> IO ()
build _ _ = return ()
