{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Language.K3.Metaprogram.DataTypes where

import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Functor.Identity
import qualified Data.Map as Map
import Data.Map ( Map )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Metaprogram

{-| Metaprogram environment -}
data K3Generator = Splicer  (SpliceEnv -> SpliceResult GeneratorM)
                 | TypeRewriter (K3 Type -> SpliceEnv -> SpliceResult GeneratorM)
                 | ExprRewriter (K3 Expression -> SpliceEnv -> SpliceResult GeneratorM)
                 | DeclRewriter (K3 Declaration -> SpliceEnv -> SpliceResult GeneratorM)

data GeneratorEnv = GeneratorEnv { dataAGEnv :: Map Identifier K3Generator
                                 , ctrlAGEnv :: Map Identifier K3Generator }

type GeneratorState = (Int, GeneratorEnv, SpliceContext, [K3 Declaration])

type GeneratorM = EitherT String (StateT GeneratorState Identity)

type TypeGenerator = GeneratorM (K3 Type)
type ExprGenerator = GeneratorM (K3 Expression)
type DeclGenerator = GeneratorM (K3 Declaration)

{- Generator monad helpers -}
runGeneratorM :: GeneratorState -> GeneratorM a -> (Either String a, GeneratorState)
runGeneratorM st action = runIdentity . flip runStateT st $ runEitherT action

-- | Raise an exception in the generator monad.
throwE :: String -> GeneratorM a
throwE msg = Control.Monad.Trans.Either.left msg

{- Generator state constructors and accessors -}
emptyGeneratorEnv :: GeneratorEnv
emptyGeneratorEnv = GeneratorEnv Map.empty Map.empty

emptySpliceContext :: SpliceContext
emptySpliceContext = []

emptyGeneratorState :: GeneratorState
emptyGeneratorState = (0, emptyGeneratorEnv, emptySpliceContext, [])

getGeneratorUID :: GeneratorState -> Int
getGeneratorUID (c, _, _, _) = c

getGeneratorEnv :: GeneratorState -> GeneratorEnv
getGeneratorEnv (_, env, _, _) = env

getSpliceContext :: GeneratorState -> SpliceContext
getSpliceContext (_, _, ctxt, _) = ctxt

getGeneratedDecls :: GeneratorState -> [K3 Declaration]
getGeneratedDecls (_, _,_,decls) = decls

modifyGeneratorEnv :: (GeneratorEnv -> Either String (GeneratorEnv, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifyGeneratorEnv f (ac,ge,sc,gd) = either Left (\(nge,r) -> Right ((ac,nge,sc,gd), r)) $ f ge

modifySpliceContext :: (SpliceContext -> Either String (SpliceContext, a)) -> GeneratorState -> Either String (GeneratorState, a)
modifySpliceContext f (ac,ge,sc,gd) = either Left (\(nsc,r) -> Right ((ac,ge,nsc,gd),r)) $ f sc

modifyGeneratedDecls :: ([K3 Declaration] -> Either String ([K3 Declaration], a)) -> GeneratorState -> Either String (GeneratorState, a)
modifyGeneratedDecls f (ac,ge,sc,gd) = either Left (\(ngd,r) -> Right ((ac,ge,sc,ngd), r)) $ f gd

modifyGeneratorState :: (GeneratorState -> Either String (GeneratorState, a)) -> GeneratorM a
modifyGeneratorState f = get >>= \st -> either throwE (\(nst,r) -> put nst >> return r) $ f st

generatorWithGUID :: (Int -> GeneratorM a) -> GeneratorM a
generatorWithGUID f = get >>= (\(ac,ge,sc,gd) -> put (ac+1,ge,sc,gd) >> f ac)

generatorWithGEnv :: (GeneratorEnv -> GeneratorM a) -> GeneratorM a
generatorWithGEnv f = get >>= f . getGeneratorEnv

generatorWithSCtxt :: (SpliceContext -> GeneratorM a) -> GeneratorM a
generatorWithSCtxt f = get >>= f . getSpliceContext

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

modifyGDeclsF :: ([K3 Declaration] -> Either String ([K3 Declaration], a)) -> GeneratorM a
modifyGDeclsF f = modifyGeneratorState $ modifyGeneratedDecls f

modifyGDeclsF_ :: ([K3 Declaration] -> Either String [K3 Declaration]) -> GeneratorM ()
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
