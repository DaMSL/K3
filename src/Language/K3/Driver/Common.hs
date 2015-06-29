{-# LANGUAGE DeriveGeneric #-}

module Language.K3.Driver.Common where

import GHC.Generics ( Generic )

import System.Directory (createDirectoryIfMissing)
import System.FilePath (joinPath, replaceExtension, takeBaseName)
import System.IO

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Parser ( parseK3, stitchK3 )
import Language.K3.Runtime.Common ( SystemEnvironment )
import Language.K3.Stages ( CompilerSpec(..) )
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

{- Common data types used throughout the driver. -}

-- | Variant type used to define compilation stages.
data CompilerType = LocalCompiler
                  | ServicePrepare
                  | ServiceParallel
                  | ServiceFinal
                  | ServiceClient
                  | ServiceClientRemote
                  deriving (Eq, Read, Show, Generic)

-- | Coarse-grained compilation stages.
data CompileStage = SBatchOpt
                  | SDeclPrepare
                  | SDeclOpt CompilerSpec
                  | SCodegen
                  deriving (Eq, Ord, Read, Show, Generic)

type CompileStages = [CompileStage]

-- | Printing specification.
data PrintMode
    = PrintAST    { stripEffects :: Bool
                  , stripTypes   :: Bool
                  , stripCmts    :: Bool
                  , stripProps   :: Bool }
    | PrintSyntax
  deriving (Eq, Read, Show, Generic)

{- Constants -}
defaultPrompt :: String
defaultPrompt = "k3> "

{- Defaults -}
defaultOutLanguage :: String
defaultOutLanguage = "cpp"

defaultProgramName :: String
defaultProgramName = "A"

defaultOutputFile :: FilePath
defaultOutputFile = "a.out"

defaultBuildDir :: FilePath
defaultBuildDir = "__build"

defaultBuildJobs :: Int
defaultBuildJobs = 1

defaultRuntimeDir :: FilePath
defaultRuntimeDir = "runtime"

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

outputFilePath :: FilePath -> String -> String -> (FilePath, FilePath)
outputFilePath path input ext = (path, joinPath [path, replaceExtension (takeBaseName $ input) ext])

outputStrFile :: String -> (FilePath, FilePath) -> IO ()
outputStrFile str (path, file) = do
  createDirectoryIfMissing True path
  writeFile file str

readK3Input :: [FilePath] -> FilePath -> IO [String]
readK3Input searchPaths path = do
    h <- openFileOrStdIn path
    s <- hGetContents h
    return [s]
    -- stitchK3 searchPaths s

parseK3Input :: Bool -> [FilePath] -> FilePath -> IO (Either String (K3 Declaration))
parseK3Input includeOverride searchPaths path = do
    h <- openFileOrStdIn path
    s <- hGetContents h
    parseK3 includeOverride searchPaths s

parseSplicedASTInput :: FilePath -> IO (K3 Declaration)
parseSplicedASTInput path = do
    h <- openFileOrStdIn path
    astStr <- hGetContents h
    return ((read astStr) :: K3 Declaration)

prettySysEnv :: SystemEnvironment -> [String]
prettySysEnv env = ["System environment: "] ++ concatMap prettyEnvEntry env
  where
    prettyEnvEntry (addr, bs) = prettyLines addr ++ (indent 2 $ prettyBootstrap bs)
    prettyBootstrap bs        = concatMap (prettyPair $ maxNameLength bs) bs
    prettyPair w (a,b)        = [a ++ replicate (w - length a) ' ' ++ " => "
                                   ++ (either (const "<syntax error>") id $ literalS b)]
    maxNameLength l           = maximum $ map (length . fst) l
