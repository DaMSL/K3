{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

module Language.K3.Analysis.Properties where

import Control.Monad.State
import Control.Monad.Trans.Either

import Data.List
import Data.Maybe
import Data.Tree

import Data.Monoid

import Data.Map              ( Map )
import qualified Data.Map    as Map

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal
import Language.K3.Core.Constructor.Expression as EC
import Language.K3.Core.Utils

type NamedEnv a = Map Identifier [a]
type PList = [(Identifier, Maybe (K3 Literal))]
type PEnv  = NamedEnv PList
type PAEnv = NamedEnv PEnv
data PIEnv = PIEnv { penv :: PEnv, paenv :: PAEnv }
type PInfM = EitherT String (State PIEnv)

instance Monoid PIEnv where
  mempty = PIEnv mempty mempty
  mappend (PIEnv e a) (PIEnv e' a') = PIEnv (e <> e') (a <> a')

{- NamedEnv helpers -}
neenv0 :: NamedEnv a
neenv0 = Map.empty

nelkup :: NamedEnv a -> Identifier -> Either String a
nelkup env x = maybe err safeHead $ Map.lookup x env
 where safeHead l = if null l then err else Right $ head l
       err = Left $ "Unbound identifier in named environment: " ++ x

nmem :: NamedEnv a -> Identifier -> Bool
nmem env n = Map.member n env

neext :: NamedEnv a -> Identifier -> a -> NamedEnv a
neext env x v = Map.insertWith (++) x [v] env

nedel :: NamedEnv a -> Identifier -> NamedEnv a
nedel env x = Map.update safeTail x env
  where safeTail []  = Nothing
        safeTail [_] = Nothing
        safeTail l   = Just $ tail l

{- PList helpers -}
pl0 :: PList
pl0 = []

pllkup :: PList -> Identifier -> Either String (Maybe (K3 Literal))
pllkup pl x = maybe err Right $ lookup x pl
 where err = Left $ "Unbound identifier in property list: " ++ x

plext :: PList -> Identifier -> Maybe (K3 Literal) -> PList
plext pl x p = replaceAssoc pl x p

pldel :: PList -> Identifier -> PList
pldel pl x = removeAssoc pl x

{- PEnv helpers -}
penv0 :: PEnv
penv0 = neenv0

plkup :: PEnv -> Identifier -> Either String PList
plkup = nelkup

pmem :: PEnv -> Identifier -> Bool
pmem = nmem

pext :: PEnv -> Identifier -> PList -> PEnv
pext = neext

pdel :: PEnv -> Identifier -> PEnv
pdel env x = nedel env x

pextp :: PEnv -> Identifier -> Identifier -> Maybe (K3 Literal) -> PEnv
pextp env x y v = (\f -> Map.alter f x env) $ \case
  Nothing    -> Just [[(y,v)]]
  Just []    -> Just [[(y,v)]]
  Just (h:t) -> Just $ (plext h y v):t

pdelp :: PEnv -> Identifier -> Identifier -> PEnv
pdelp env x y = (\f -> Map.alter f x env) $ \case
  Nothing -> Nothing
  Just [] -> Just []
  Just (h:t) -> Just $ (pldel h y):t

{- PAEnv helpers -}
paenv0 :: PAEnv
paenv0 = Map.empty

palkup :: PAEnv -> Identifier -> Either String PEnv
palkup = nelkup

paext :: PAEnv -> Identifier -> PEnv -> PAEnv
paext = neext

padel :: PAEnv -> Identifier -> PAEnv
padel env x = nedel env x

paextm :: PAEnv -> Identifier -> Identifier -> PList -> PAEnv
paextm env x y pl = Map.alter extF x env
  where extF Nothing   = Just [pext penv0 y pl]
        extF (Just []) = Just [pext penv0 y pl]
        extF (Just (h:t)) = Just $ (pext h y pl):t

padelm :: PAEnv -> Identifier -> Identifier -> PAEnv
padelm env x y = Map.alter delF x env
  where delF Nothing      = Nothing
        delF (Just [])    = Just []
        delF (Just (h:t)) = Just $ (pdel h y):t


{- PIEnv helpers -}
pienv0 :: PIEnv
pienv0 = PIEnv {penv=penv0, paenv=paenv0}

pilkupe :: PIEnv -> Identifier -> Either String PList
pilkupe env = plkup $ penv env

pimeme :: PIEnv -> Identifier -> Bool
pimeme env = pmem $ penv env

pilkupa :: PIEnv -> Identifier -> Either String PEnv
pilkupa env = palkup $ paenv env

piexte :: PIEnv -> Identifier -> PList -> PIEnv
piexte env i pl = env {penv=pext (penv env) i pl}

piextep :: PIEnv -> Identifier -> Identifier -> Maybe (K3 Literal) -> PIEnv
piextep env i j p = env {penv=pextp (penv env) i j p}

piexta :: PIEnv -> Identifier -> PEnv -> PIEnv
piexta env i ape = env {paenv=paext (paenv env) i ape}

piextam :: PIEnv -> Identifier -> Identifier -> PList -> PIEnv
piextam env i j pl = env {paenv=paextm (paenv env) i j pl}

pidele :: PIEnv -> Identifier -> PIEnv
pidele env i = env {penv=pdel (penv env) i}

pidelep :: PIEnv -> Identifier -> Identifier -> PIEnv
pidelep env i j = env {penv=pdelp (penv env) i j}

pidela :: PIEnv -> Identifier -> PIEnv
pidela env i = env {paenv=padel (paenv env) i}

pidelam :: PIEnv -> Identifier -> Identifier -> PIEnv
pidelam env i j = env {paenv=padelm (paenv env) i j}


{- PInfM helpers -}
runPInfM :: PIEnv -> PInfM a -> (Either String a, PIEnv)
runPInfM pienv m = flip runState pienv $ runEitherT m

runPInfE :: PIEnv -> PInfM a -> Either String (a, PIEnv)
runPInfE env m = let (a,b) = runPInfM env m in a >>= return . (,b)

liftEitherM :: Either String a -> PInfM a
liftEitherM = either left return

{- Analysis entry point. -}
inferProgramUsageProperties :: K3 Declaration -> Either String (K3 Declaration, PIEnv)
inferProgramUsageProperties prog = do
    (_, initEnv) <- runPInfE pienv0 $ mapProgram initializeDeclUsageProperties return return Nothing prog
    runPInfE initEnv $ mapProgram return return inferExprUsageProperties Nothing prog

-- | Repeat property propagation on a global with an initializer.
reinferProgDeclUsageProperties :: PIEnv -> Identifier -> K3 Declaration -> Either String (K3 Declaration, PIEnv)
reinferProgDeclUsageProperties env dn prog = runPInfE env inferNamedDecl
  where
    inferNamedDecl = mapProgramWithDecl onNamedDecl (const return) (const return) Nothing prog

    onNamedDecl d@(tag -> DGlobal  n _ (Just e)) | dn == n = inferDecl n d e
    onNamedDecl d@(tag -> DTrigger n _ e)        | dn == n = inferDecl n d e
    onNamedDecl d = return d

    inferDecl n d e = do
      modify (\env' -> if pimeme env' n then (pidele env' n) else env')
      nd   <- initializeDeclUsageProperties d
      ne   <- inferExprUsageProperties e
      rebuildDecl ne nd

    rebuildDecl e d@(tnc -> (DGlobal  n t (Just _), ch)) = return $ Node (DGlobal  n t (Just e) :@: annotations d) ch
    rebuildDecl e d@(tnc -> (DTrigger n t _, ch))        = return $ Node (DTrigger n t e        :@: annotations d) ch
    rebuildDecl _ d = return d


initializeDeclUsageProperties :: K3 Declaration -> PInfM (K3 Declaration)
initializeDeclUsageProperties d = case tna d of
    (DGlobal  n _ _, anns)        -> extDeclProps n anns >> return d
    (DTrigger n _ _, anns)        -> extDeclProps n anns >> return d
    (DDataAnnotation n _ mems, _) -> extAnnProps n mems >> return d
    _ -> return d

  where
    extDeclProps n anns = modify (\env -> piexte env n $ mapMaybe extractDProperty anns)

    extAnnProps n mems = modify (\env -> piexta env n $ Map.fromList $ mapMaybe memF mems)
    memF (Lifted      _ mn _ _ mAnns) = Just $ (mn, [mapMaybe extractDProperty mAnns])
    memF (Attribute   _ mn _ _ mAnns) = Just $ (mn, [mapMaybe extractDProperty mAnns])
    memF (MAnnotation _ _ _) = Nothing

    extractDProperty (DProperty (dPropertyV -> pv)) = Just pv
    extractDProperty _ = Nothing

inferExprUsageProperties :: K3 Expression -> PInfM (K3 Expression)
inferExprUsageProperties prog = mapIn1RebuildTree lambdaProp sidewaysProp inferProp prog
  where
    lambdaProp :: K3 Expression -> K3 Expression -> PInfM ()
    lambdaProp _ (tag -> ELambda i) = extNullExprProps i
    lambdaProp _ _ = return ()

    sidewaysProp :: K3 Expression -> K3 Expression -> PInfM [PInfM ()]
    sidewaysProp ch1 (tag -> ELetIn  i) = extExprProps i ch1 >> return [iu]
    sidewaysProp _ (tag -> ECaseOf i) = return [extNullExprProps i, pruneExprProps i]
    sidewaysProp _ (tag -> EBindAs b) = case b of
      BIndirection i -> return [extNullExprProps i]
      BTuple     ids -> return [mapM_ extNullExprProps ids]
      BRecord    ijs -> return [mapM_ extNullExprProps $ map snd ijs]
    sidewaysProp _ (Node _ ch) = return $ replicate (length ch - 1) iu

    inferProp :: [K3 Expression] -> K3 Expression -> PInfM (K3 Expression)
    inferProp _ e@(tag -> EVariable i) = do
      pl <- get >>= \env -> liftEitherM $ pilkupe env i
      return $ foldl (@+) (EC.variable i) $ rebuildAnnotations e pl

    inferProp ch e@(tag -> ELambda i) = do
      void $ pruneExprProps i
      return $ foldl (@+) (EC.lambda i $ head ch) $ rebuildAnnotations e pl0

    inferProp ch e@(tag -> ELetIn i) = do
      void $ pruneExprProps i
      return $ foldl (@+) (EC.letIn i (head ch) $ last ch) $ rebuildAnnotations e pl0

    inferProp ch e@(tag -> EBindAs b) = do
      _ <- case b of
             BIndirection i -> pruneExprProps i
             BTuple     ids -> mapM_ pruneExprProps ids
             BRecord    ijs -> mapM_ pruneExprProps (map snd ijs)
      return $ foldl (@+) (EC.bindAs (head ch) b $ last ch) $ rebuildAnnotations e pl0

    inferProp ch e@(tag -> EProject i) = do
      let srcAnns = annotations $ head ch
      srcT <- case find isEType srcAnns of
                Just (EType t) -> return t
                _              -> left "Untyped projection while inferring properties"
      memPL <- case tag srcT of
                 TRecord _   -> return pl0
                 TCollection -> do
                   let tAnns = namedTAnnotations $ annotations srcT
                   annEnvs <- get >>= \env -> mapM (liftEitherM . pilkupa env) tAnns
                   case lookup i $ concatMap headPEnv annEnvs of
                     Nothing -> invalidAnnMemErr i tAnns
                     Just pl -> return pl
                 _ -> left $ "Invalid projected type while inferring properties"
      return $ foldl (@+) (EC.project i $ head ch) $ rebuildAnnotations e memPL

    inferProp ch (Node (tg :@: anns) _) = return $ Node (tg :@: anns) ch

    headPEnv :: PEnv -> [(Identifier, PList)]
    headPEnv pe = flip concatMap (Map.toList pe) $ \case
      (_,[]) -> []
      (n,(h:_)) -> [(n,h)]

    extNullExprProps :: Identifier -> PInfM ()
    extNullExprProps i = modify $ \env -> piexte env i pl0

    extExprProps :: Identifier -> K3 Expression -> PInfM ()
    extExprProps i (annotations -> anns) = modify (\env -> piexte env i $ mapMaybe extractEProperty anns)

    pruneExprProps :: Identifier -> PInfM ()
    pruneExprProps i = modify (\env -> pidele env i)

    rebuildAnnotations :: K3 Expression -> PList -> [Annotation Expression]
    rebuildAnnotations e pl =
      let (annProps,rest) = partition isEProperty $ annotations e
      in rest ++ (nub $ annProps ++ map (\(n,lopt) -> EProperty $ Right (n,lopt)) pl)

    extractEProperty :: Annotation Expression -> Maybe (Identifier, Maybe (K3 Literal))
    extractEProperty (EProperty (ePropertyV -> pv)) = Just pv
    extractEProperty _ = Nothing

    iu :: PInfM ()
    iu = return ()

    invalidAnnMemErr i tAnns = left $ unwords ["Invalid annotation member while inferring properties: ", i, "in", show tAnns]
