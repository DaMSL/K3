module Language.K3.Compiler.CPP (compile) where

import System.Directory (createDirectoryIfMissing)
import System.FilePath (joinPath, replaceExtension, takeBaseName)

import qualified Data.Sequence as S

import Text.PrettyPrint.ANSI.Leijen

import Language.K3.TypeSystem (typecheckProgram)

import qualified Language.K3.Codegen.Imperative as IMP
import qualified Language.K3.Codegen.CPP as CPP

import Language.K3.Driver.Common (parseK3Input)
import Language.K3.Driver.Options (Options(..), CompileOptions(..), PathOptions(..))
import Language.K3.Driver.Typecheck (prettyTCErrors)

-- Generate C++ code for a given K3 program.
compile :: Options -> CompileOptions -> IO ()
compile opts copts = do
    program <- parseK3Input (includes $ paths opts) (input opts)
    case program of
        Left e -> putStrLn $ "Parse Error: " ++ e
        Right d -> do
            let (typeErrors, _, typedProgram) = typecheckProgram d
            if not (S.null typeErrors)
                then putStrLn $ prettyTCErrors typedProgram typeErrors
                else do
                    let (i, _) = IMP.runImperativeM (IMP.declaration typedProgram) ()
                    case i of
                        Left () -> print "Imperative Transformation Error"
                        Right i' -> do
                            let (r, _) = CPP.runCPPGenM CPP.defaultCPPGenS (CPP.program i')
                            case r of
                                Left e -> print e
                                Right s -> case buildDir copts of
                                    Nothing -> print "Error: No build directory specified."
                                    Just b -> do
                                        createDirectoryIfMissing True b
                                        let oFile = joinPath [b, replaceExtension (takeBaseName $ input opts) "cpp"]
                                        writeFile oFile (displayS (renderPretty 1.0 100 s) "")
