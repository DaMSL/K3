{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Language.K3.Metaprogram.DataTypes where

import Control.Monad.State
import Control.Monad.Trans.Either

import qualified Data.Map as Map
import Data.Map ( Map )
import qualified Data.Set as Set
import Data.Set ( Set )

import Language.Haskell.Interpreter ( Interpreter )
import qualified Language.Haskell.Interpreter as HI

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal
import Language.K3.Core.Metaprogram

import Language.K3.Parser.DataTypes

{-| Metaprogram environment -}
data K3Generator = Splicer  (SpliceEnv -> SpliceResult GeneratorM)
                 | TypeRewriter (K3 Type -> SpliceEnv -> SpliceResult GeneratorM)
                 | ExprRewriter (K3 Expression -> SpliceEnv -> SpliceResult GeneratorM)
                 | DeclRewriter (K3 Declaration -> SpliceEnv -> SpliceResult GeneratorM)

data GeneratorEnv = GeneratorEnv { dataAGEnv :: Map Identifier K3Generator
                                 , ctrlAGEnv :: Map Identifier K3Generator }

data GeneratorDecls = GeneratorDecls { dataADecls :: Map Identifier [K3 Declaration]
                                     , ctrlADecls :: Map Identifier (Set (K3 Declaration)) }

-- | Haskell-K3 metaprogramming bridge options.
data MPEvalOptions = MPEvalOptions { mpInterpArgs  :: [String]
                                   , mpSearchPaths :: [String]
                                   , mpLoadPaths   :: [String]
                                   , mpImportPaths :: [String] }

data GeneratorState = GeneratorState { generatorUid    :: Int
                                     , generatorEnv    :: GeneratorEnv
                                     , spliceCtxt      :: SpliceContext
                                     , generatorDecls  :: GeneratorDecls
                                     , mpEvalOpts      :: MPEvalOptions
                                     , initInterpreter :: Interpreter () }

type GeneratorM = EitherT String (StateT GeneratorState IO)

type TypeGenerator    = GeneratorM (K3 Type)
type ExprGenerator    = GeneratorM (K3 Expression)
type LiteralGenerator = GeneratorM (K3 Literal)
type DeclGenerator    = GeneratorM (K3 Declaration)

type AnnMemGenerator  = GeneratorM AnnMemDecl

type DeclAnnGenerator = GeneratorM (Annotation Declaration)
type ExprAnnGenerator = GeneratorM (Annotation Expression)


{- Generator monad helpers -}
runGeneratorM :: GeneratorState -> GeneratorM a -> IO (Either String a, GeneratorState)
runGeneratorM st action = flip runStateT st $ runEitherT action

-- | Run a parser and return its result in the generator monad.
liftParser :: (Show a) => String -> K3Parser a -> GeneratorM a
liftParser s p = either throwG return $ stringifyError $ runK3Parser Nothing p s

-- | Raise an exception in the generator monad.
throwG :: String -> GeneratorM a
throwG msg = Control.Monad.Trans.Either.left msg

{- Generator state constructors and accessors -}
emptyGeneratorEnv :: GeneratorEnv
emptyGeneratorEnv = GeneratorEnv Map.empty Map.empty

emptySpliceContext :: SpliceContext
emptySpliceContext = []

emptyGeneratorDecls :: GeneratorDecls
emptyGeneratorDecls = GeneratorDecls Map.empty Map.empty

emptyGeneratorState :: GeneratorState
emptyGeneratorState = GeneratorState 0 emptyGeneratorEnv emptySpliceContext emptyGeneratorDecls
                                       defaultMPEvalOptions (initializeInterpreter defaultMPEvalOptions)

mkGeneratorState :: MPEvalOptions -> GeneratorState
mkGeneratorState evalOpts = GeneratorState 0 emptyGeneratorEnv emptySpliceContext emptyGeneratorDecls
                                             evalOpts (initializeInterpreter evalOpts)

getGeneratorUID :: GeneratorState -> Int
getGeneratorUID  = generatorUid

getGeneratorEnv :: GeneratorState -> GeneratorEnv
getGeneratorEnv = generatorEnv

getSpliceContext :: GeneratorState -> SpliceContext
getSpliceContext = spliceCtxt

getGeneratedDecls :: GeneratorState -> GeneratorDecls
getGeneratedDecls = generatorDecls

getMPEvalOptions :: GeneratorState -> MPEvalOptions
getMPEvalOptions = mpEvalOpts

getInterpreter :: GeneratorState -> Interpreter ()
getInterpreter = initInterpreter

modifyGeneratorEnv :: (GeneratorEnv -> Either String (GeneratorEnv, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifyGeneratorEnv f gs = either Left (\(nge,r) -> Right (gs {generatorEnv = nge}, r)) $ f $ getGeneratorEnv gs

modifySpliceContext :: (SpliceContext -> Either String (SpliceContext, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifySpliceContext f gs = either Left (\(nsc,r) -> Right (gs {spliceCtxt = nsc}, r)) $ f $ getSpliceContext gs

modifyGeneratedDecls :: (GeneratorDecls -> Either String (GeneratorDecls, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifyGeneratedDecls f gs = either Left (\(ngd,r) -> Right (gs {generatorDecls = ngd}, r)) $ f $ getGeneratedDecls gs

modifyGeneratorState :: (GeneratorState -> Either String (GeneratorState, a)) -> GeneratorM a
modifyGeneratorState f = get >>= \st -> either throwG (\(nst,r) -> put nst >> return r) $ f st

generatorWithGUID :: (Int -> GeneratorM a) -> GeneratorM a
generatorWithGUID f = get >>= \gs -> put (gs {generatorUid = (getGeneratorUID gs + 1)}) >> (f $ getGeneratorUID gs)

generatorWithGEnv :: (GeneratorEnv -> GeneratorM a) -> GeneratorM a
generatorWithGEnv f = get >>= f . getGeneratorEnv

generatorWithSCtxt :: (SpliceContext -> GeneratorM a) -> GeneratorM a
generatorWithSCtxt f = get >>= f . getSpliceContext

generatorWithEvalOptions :: (MPEvalOptions -> GeneratorM a) -> GeneratorM a
generatorWithEvalOptions f = get >>= f . getMPEvalOptions

generatorWithInterpreter :: (Interpreter () -> GeneratorM a) -> GeneratorM a
generatorWithInterpreter f = get >>= f . getInterpreter

withGUID :: (Int -> a) -> GeneratorM a
withGUID f = generatorWithGUID $ return . f

withGEnv :: (GeneratorEnv -> a) -> GeneratorM a
withGEnv f = generatorWithGEnv $ return . f

withSCtxt :: (SpliceContext -> a) -> GeneratorM a
withSCtxt f = generatorWithSCtxt $ return . f

modifyGEnv :: (GeneratorEnv -> (GeneratorEnv, a)) -> GeneratorM a
modifyGEnv f = modifyGEnvF $ Right . f

modifyGEnv_ :: (GeneratorEnv -> GeneratorEnv) -> GeneratorM ()
modifyGEnv_ f = modifyGEnv $ (,()) . f

modifyGEnvF :: (GeneratorEnv -> Either String (GeneratorEnv, a)) -> GeneratorM a
modifyGEnvF f = modifyGeneratorState $ modifyGeneratorEnv f

modifyGEnvF_ :: (GeneratorEnv -> Either String GeneratorEnv) -> GeneratorM ()
modifyGEnvF_ f = modifyGEnvF $ (>>= Right . (,())) . f

modifySCtxtF :: (SpliceContext -> Either String (SpliceContext, a)) -> GeneratorM a
modifySCtxtF f = modifyGeneratorState $ modifySpliceContext f

modifySCtxtF_ :: (SpliceContext -> Either String SpliceContext) -> GeneratorM ()
modifySCtxtF_ f = modifySCtxtF $ (>>= Right . (,())) . f

modifyGDeclsF :: (GeneratorDecls -> Either String (GeneratorDecls, a)) -> GeneratorM a
modifyGDeclsF f = modifyGeneratorState $ modifyGeneratedDecls f

modifyGDeclsF_ :: (GeneratorDecls -> Either String GeneratorDecls) -> GeneratorM ()
modifyGDeclsF_ f = modifyGDeclsF $ (>>= Right . (,())) . f

generateInSpliceEnv :: SpliceEnv -> GeneratorM a -> GeneratorM a
generateInSpliceEnv spliceEnv g = do
   modifySCtxtF_ $ \ctxt -> Right $ pushSCtxt spliceEnv ctxt
   r <- g
   modifySCtxtF_ $ Right . popSCtxt
   return r

{- Generator environment accessors -}
lookupDGenE :: Identifier -> GeneratorEnv -> Maybe K3Generator
lookupDGenE n (GeneratorEnv env _)= Map.lookup n env

lookupCGenE :: Identifier -> GeneratorEnv -> Maybe K3Generator
lookupCGenE n (GeneratorEnv _ env)= Map.lookup n env

addDGenE :: Identifier -> K3Generator -> GeneratorEnv -> GeneratorEnv
addDGenE n g (GeneratorEnv d c) = GeneratorEnv (Map.insert n g d) c

addCGenE :: Identifier -> K3Generator -> GeneratorEnv -> GeneratorEnv
addCGenE n g (GeneratorEnv d c) = GeneratorEnv d (Map.insert n g c)

lookupDSPGenE :: Identifier -> GeneratorEnv -> Maybe (SpliceEnv -> SpliceResult GeneratorM)
lookupDSPGenE n env = lookupDGenE n env >>= \case { Splicer f -> Just f; _ -> Nothing }

lookupTRWGenE :: Identifier -> GeneratorEnv -> Maybe (K3 Type -> SpliceEnv -> SpliceResult GeneratorM)
lookupTRWGenE n env = lookupCGenE n env >>= \case { TypeRewriter f -> Just f; _ -> Nothing }

lookupERWGenE :: Identifier -> GeneratorEnv -> Maybe (K3 Expression -> SpliceEnv -> SpliceResult GeneratorM)
lookupERWGenE n env = lookupCGenE n env >>= \case { ExprRewriter f -> Just f; _ -> Nothing }

lookupDRWGenE :: Identifier -> GeneratorEnv -> Maybe (K3 Declaration -> SpliceEnv -> SpliceResult GeneratorM)
lookupDRWGenE n env = lookupCGenE n env >>= \case { DeclRewriter f -> Just f; _ -> Nothing }


{- Generated declaration accessors -}
addDGenDecl :: Identifier -> K3 Declaration -> GeneratorDecls -> GeneratorDecls
addDGenDecl n decl (GeneratorDecls dd cd) =
  GeneratorDecls (Map.insertWith (flip (++)) n [decl] dd) cd

addCGenDecls :: Identifier -> [K3 Declaration] -> GeneratorDecls -> GeneratorDecls
addCGenDecls n decls (GeneratorDecls dd cd) =
  GeneratorDecls dd (Map.insertWith Set.union n (Set.fromList decls) cd)

generatorDeclsToList :: GeneratorDecls -> ([K3 Declaration], [K3 Declaration])
generatorDeclsToList (GeneratorDecls dd cd) =
  (concat $ Map.elems dd, Set.toList $ Set.unions $ Map.elems cd)


{- Haskell-K3 metaprogram evaluation options -}

defaultMPEvalOptions :: MPEvalOptions
defaultMPEvalOptions = MPEvalOptions dInterpArgs dSearchPaths dLoadPaths dImportPaths
  where
    dInterpArgs  = ["-package-db", ".cabal-sandbox/x86_64-windows-ghc-7.8.3-packages.conf.d"]
    dSearchPaths = [".", "../K3-Core/src"]
    dLoadPaths   = ["Language.K3.Core.Metaprogram"]
    dImportPaths = [ "Prelude"
                   , "Data.Map"
                   , "Data.Tree"
                   , "Language.K3.Core.Annotation"
                   , "Language.K3.Core.Common"
                   , "Language.K3.Core.Type"
                   , "Language.K3.Core.Expression"
                   , "Language.K3.Core.Declaration"
                   , "Language.K3.Core.Metaprogram" ]

initializeInterpreter :: MPEvalOptions -> Interpreter ()
initializeInterpreter evalOpts = do
  void $  HI.set [HI.searchPath HI.:= (mpSearchPaths evalOpts)]
  void $  HI.loadModules $ (mpLoadPaths evalOpts)
  void $  HI.setImports $ (mpImportPaths evalOpts)
  mods <- HI.getLoadedModules
  logVoid $ ("Loaded: " ++ show mods)
