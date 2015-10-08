{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Language.K3.Metaprogram.DataTypes where

import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Map ( Map )
import Data.Set ( Set )
import qualified Data.Map as Map
import qualified Data.Set as Set

import Language.Haskell.Interpreter ( Interpreter )
import qualified Language.Haskell.Interpreter as HI
import qualified Language.Haskell.Interpreter.Unsafe as HIU

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal
import Language.K3.Core.Metaprogram
import Language.K3.Core.Utils

import Language.K3.Parser.DataTypes

import Language.K3.Utils.Logger

{-| Metaprogram environment -}
data K3Generator = Splicer      (SpliceEnv -> SpliceResult GeneratorM)
                 | TypeRewriter (K3 Type -> SpliceEnv -> SpliceResult GeneratorM)
                 | ExprRewriter (K3 Expression -> SpliceEnv -> SpliceResult GeneratorM)
                 | DeclRewriter (K3 Declaration -> SpliceEnv -> SpliceResult GeneratorM)

data GeneratorEnv = GeneratorEnv { dataAGEnv :: Map Identifier K3Generator
                                 , ctrlAGEnv :: Map Identifier K3Generator }

data GeneratorDecls = GeneratorDecls { dataADecls :: Map Identifier [K3 Declaration]
                                     , ctrlADecls :: Map Identifier (Set (K3 Declaration)) }

-- | Haskell-K3 metaprogramming bridge options.
data MPEvalOptions = MPEvalOptions { mpInterpArgs   :: [String]
                                   , mpSearchPaths  :: [String]
                                   , mpLoadPaths    :: [String]
                                   , mpImportPaths  :: [String]
                                   , mpQImportPaths :: [(String, Maybe String)] }

-- | Annotation generation is performed per name-parameter combination.
type MPGeneratorKey = (Identifier, SpliceEnv, Maybe (K3 Type))
type MPGensymS = Map MPGeneratorKey Int

-- | Metaprogramming state.
data GeneratorState = GeneratorState { mpGensymS       :: MPGensymS
                                     , generatorEnv    :: GeneratorEnv
                                     , spliceCtxt      :: SpliceContext
                                     , generatorDecls  :: GeneratorDecls
                                     , mpEvalOpts      :: MPEvalOptions }

type GeneratorM = EitherT String (StateT GeneratorState Interpreter)

type TypeGenerator    = GeneratorM (K3 Type)
type ExprGenerator    = GeneratorM (K3 Expression)
type LiteralGenerator = GeneratorM (K3 Literal)
type DeclGenerator    = GeneratorM (K3 Declaration)

type AnnMemGenerator  = GeneratorM AnnMemDecl

type DeclAnnGenerator = GeneratorM (Annotation Declaration)
type ExprAnnGenerator = GeneratorM (Annotation Expression)


{- Generator monad helpers -}
runGeneratorM :: GeneratorState -> GeneratorM a -> IO (Either String a)
runGeneratorM st action = do
  actE <- HIU.unsafeRunInterpreterWithArgs (mpInterpArgs $ mpEvalOpts st) $
            flip runStateT st $ runEitherT
              ( (initializeInterpreter $ mpEvalOpts st) >> action )
  return $ either (Left . show) fst actE

-- | Run a parser and return its result in the generator monad.
liftParser :: (Show a) => String -> K3Parser a -> GeneratorM a
liftParser s p = either throwG return $ stringifyError $ runK3Parser Nothing p s

-- | Lift a Haskell interpreter action into the generator monad.
liftHI :: Interpreter a -> GeneratorM a
liftHI = lift . lift

-- | Raise an exception in the generator monad.
throwG :: String -> GeneratorM a
throwG msg = Control.Monad.Trans.Either.left msg

{- Annotation symbol generation -}
mpgensym :: Identifier -> SpliceEnv -> Maybe (K3 Type) -> MPGensymS -> (Either Int Int, MPGensymS)
mpgensym n env tOpt symS = maybe newsymS (\i -> (Left i, symS)) $ Map.lookup k symS
  where k = (n, Map.map stripSCompare env, maybe Nothing (Just . stripTCompare) tOpt)
        newsym  = 1 + Map.foldl max 0 symS
        newsymS = (Right newsym, Map.insert k newsym symS)


{- Generator state constructors and accessors -}
emptyGeneratorEnv :: GeneratorEnv
emptyGeneratorEnv = GeneratorEnv Map.empty Map.empty

emptySpliceContext :: SpliceContext
emptySpliceContext = []

emptyGeneratorDecls :: GeneratorDecls
emptyGeneratorDecls = GeneratorDecls Map.empty Map.empty

emptyGeneratorState :: GeneratorState
emptyGeneratorState =
  GeneratorState Map.empty emptyGeneratorEnv emptySpliceContext emptyGeneratorDecls defaultMPEvalOptions

mkGeneratorState :: MPEvalOptions -> GeneratorState
mkGeneratorState evalOpts =
  GeneratorState Map.empty emptyGeneratorEnv emptySpliceContext emptyGeneratorDecls evalOpts

getMPGensymS :: GeneratorState -> MPGensymS
getMPGensymS  = mpGensymS

getGeneratorEnv :: GeneratorState -> GeneratorEnv
getGeneratorEnv = generatorEnv

getSpliceContext :: GeneratorState -> SpliceContext
getSpliceContext = spliceCtxt

getGeneratedDecls :: GeneratorState -> GeneratorDecls
getGeneratedDecls = generatorDecls

getMPEvalOptions :: GeneratorState -> MPEvalOptions
getMPEvalOptions = mpEvalOpts

modifyGeneratorEnv :: (GeneratorEnv -> Either String (GeneratorEnv, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifyGeneratorEnv f gs = either Left (\(nge,r) -> Right (gs {generatorEnv = nge}, r)) $ f $ getGeneratorEnv gs

modifySpliceContext :: (SpliceContext -> Either String (SpliceContext, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifySpliceContext f gs = either Left (\(nsc,r) -> Right (gs {spliceCtxt = nsc}, r)) $ f $ getSpliceContext gs

modifyGeneratedDecls :: (GeneratorDecls -> Either String (GeneratorDecls, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifyGeneratedDecls f gs = either Left (\(ngd,r) -> Right (gs {generatorDecls = ngd}, r)) $ f $ getGeneratedDecls gs

modifyGeneratorState :: (GeneratorState -> Either String (GeneratorState, a)) -> GeneratorM a
modifyGeneratorState f = get >>= \st -> either throwG (\(nst,r) -> put nst >> return r) $ f st

generatorWithMPGensymS :: Identifier -> SpliceEnv -> Maybe (K3 Type) -> (Either Int Int -> GeneratorM a) -> GeneratorM a
generatorWithMPGensymS n env tOpt f = get >>= \gs -> do
  let (iE, nsymS) = mpgensym n env tOpt $ getMPGensymS gs
  put (gs {mpGensymS = nsymS}) >> f iE

generatorWithGEnv :: (GeneratorEnv -> GeneratorM a) -> GeneratorM a
generatorWithGEnv f = get >>= f . getGeneratorEnv

generatorWithGDecls :: (GeneratorDecls -> GeneratorM a) -> GeneratorM a
generatorWithGDecls f = get >>= f . getGeneratedDecls

generatorWithSCtxt :: (SpliceContext -> GeneratorM a) -> GeneratorM a
generatorWithSCtxt f = get >>= f . getSpliceContext

generatorWithEvalOptions :: (MPEvalOptions -> GeneratorM a) -> GeneratorM a
generatorWithEvalOptions f = get >>= f . getMPEvalOptions

withGUID :: Identifier -> SpliceEnv -> Maybe (K3 Type) -> (Either Int Int -> a) -> GeneratorM a
withGUID n env tOpt f = generatorWithMPGensymS n env tOpt $ return . f

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

generateInExtendedSpliceEnv :: Identifier -> SpliceValue -> GeneratorM a -> GeneratorM a
generateInExtendedSpliceEnv i spliceValue g = do
   modifySCtxtF_ $ \ctxt -> Right $ addSCtxt i spliceValue ctxt
   r <- g
   modifySCtxtF_ $ Right . removeSCtxt i
   return r

generateInSpliceCtxt :: SpliceContext -> GeneratorM a -> GeneratorM a
generateInSpliceCtxt ctxtExt g = generatorWithSCtxt $ \ctxtBefore -> do
   modifySCtxtF_ $ \ctxt -> Right $ concatCtxt ctxt ctxtExt
   r <- g
   modifySCtxtF_ $ \_ -> Right $ ctxtBefore
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
defaultMPEvalOptions = MPEvalOptions dInterpArgs dSearchPaths dLoadPaths dImportPaths dQImportPaths
  where
    dInterpArgs   = ["-package-db", ".cabal-sandbox/x86_64-windows-ghc-7.8.3-packages.conf.d"]
    dSearchPaths  = [".", "../K3/src"]
    dLoadPaths    = [ "Language.K3.Core.Metaprogram"
                    , "Language.K3.Core.Constructor.Type"
                    , "Language.K3.Core.Constructor.Expression"
                    , "Language.K3.Core.Constructor.Declaration"
                    , "Language.K3.Metaprogram.Primitives.Values"
                    , "Language.K3.Metaprogram.Primitives.Distributed" ]
    dImportPaths  = [ "Prelude"
                    , "Control.Monad"
                    , "Data.Map"
                    , "Data.Maybe"
                    , "Data.Tree"
                    , "Language.K3.Core.Annotation"
                    , "Language.K3.Core.Common"
                    , "Language.K3.Core.Type"
                    , "Language.K3.Core.Expression"
                    , "Language.K3.Core.Declaration"
                    , "Language.K3.Core.Literal"
                    , "Language.K3.Core.Metaprogram"
                    , "Language.K3.Metaprogram.Primitives.Values"
                    , "Language.K3.Metaprogram.Primitives.Distributed" ]
    dQImportPaths = [ ("Language.K3.Core.Constructor.Type",        Just "TC")
                    , ("Language.K3.Core.Constructor.Expression",  Just "EC")
                    , ("Language.K3.Core.Constructor.Declaration", Just "DC")
                    ]

initializeInterpreter :: MPEvalOptions -> GeneratorM ()
initializeInterpreter evalOpts = liftHI $ do
  void $  HI.set [HI.searchPath HI.:= (mpSearchPaths evalOpts)]
  void $  HI.loadModules $ (mpLoadPaths evalOpts)
  void $  HI.setImportsQ $ (map (,Nothing) (mpImportPaths evalOpts) ++ mpQImportPaths evalOpts)
  mods <- HI.getLoadedModules
  logVoid True $ ("Loaded: " ++ show mods)
