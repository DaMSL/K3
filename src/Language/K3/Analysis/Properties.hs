{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Analysis.Properties where

import Control.Arrow ( (&&&) )
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Function
import Data.List
import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal
import Language.K3.Core.Constructor.Expression as EC
import Language.K3.Core.Utils

type NamedEnv a = [(Identifier, a)]
type PList = [(Identifier, Maybe (K3 Literal))]
type PEnv  = NamedEnv PList
type PAEnv = NamedEnv PEnv
data PIEnv = PIEnv { penv :: PEnv, paenv :: PAEnv }
type PInfM = EitherT String (State PIEnv)

{- NamedEnv helpers -}
neenv0 :: NamedEnv a
neenv0 = []

nelkup :: NamedEnv a -> Identifier -> Either String a
nelkup env x = maybe err Right $ lookup x env
 where err = Left $ "Unbound identifier in named environment: " ++ x

neext :: NamedEnv a -> Identifier -> a -> NamedEnv a
neext env x v = (x,v) : env

nedel :: NamedEnv a -> Identifier -> a -> NamedEnv a
nedel env x v = deleteBy ((==) `on` fst) (x, v) env

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

pext :: PEnv -> Identifier -> PList -> PEnv
pext = neext

pdel :: PEnv -> Identifier -> PEnv
pdel env x = nedel env x pl0

pextp :: PEnv -> Identifier -> Identifier -> Maybe (K3 Literal) -> PEnv
pextp env x y v = snd $ modifyAssoc env x $ \case
  Nothing -> ((), Just $ [(y,v)])
  Just pl -> ((), Just $ plext pl y v)

pdelp :: PEnv -> Identifier -> Identifier -> PEnv
pdelp env x y = snd $ modifyAssoc env x $ \case
  Nothing -> ((), Nothing)
  Just pl -> ((), Just $ pldel pl y)

{- PAEnv helpers -}
paenv0 :: PAEnv
paenv0 = []

palkup :: PAEnv -> Identifier -> Either String PEnv
palkup = nelkup

paext :: PAEnv -> Identifier -> PEnv -> PAEnv
paext = neext

padel :: PAEnv -> Identifier -> PAEnv
padel env x = nedel env x penv0

paextm :: PAEnv -> Identifier -> Identifier -> PList -> PAEnv
paextm env x y pl = snd $
  modifyAssoc env x  $ \peOpt -> ((), Just $ pext (maybe penv0 id peOpt) y pl)

padelm :: PAEnv -> Identifier -> Identifier -> PAEnv
padelm env x y = snd $
  modifyAssoc env x $ \peOpt -> ((), maybe Nothing (\pe -> Just $ pdel pe y) peOpt)


{- PIEnv helpers -}
pienv0 :: PIEnv
pienv0 = PIEnv {penv=penv0, paenv=paenv0}

pilkupe :: PIEnv -> Identifier -> Either String PList
pilkupe env = plkup $ penv env

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

liftEitherM :: Either String a -> PInfM a
liftEitherM = either left return


inferProgramUsageProperties :: K3 Declaration -> Either String (K3 Declaration)
inferProgramUsageProperties prog =
    let (result, initEnv) = runPInfM pienv0 $ mapProgram initDeclF return return Nothing prog
    in result >> (fst $ runPInfM initEnv $ mapProgram declF annMemF exprF Nothing prog)
  where
        initDeclF d@(tag &&& annotations -> (DGlobal n _ _, anns))  = extDeclProps n anns >> return d
        initDeclF d@(tag &&& annotations -> (DTrigger n _ _, anns)) = extDeclProps n anns >> return d
        initDeclF d@(tag -> DDataAnnotation n _ mems) = extAnnProps n mems >> return d
        initDeclF d = return d

        declF   d = return d
        annMemF m = return m
        exprF   e = inferExprUsageProperties e

        extDeclProps n anns = modify (\env -> piexte env n $ mapMaybe extractDProperty anns)

        extAnnProps n mems = modify (\env -> piexta env n $ mapMaybe memF mems)
        memF (Lifted      _ mn _ _ mAnns) = Just $ (mn, mapMaybe extractDProperty mAnns)
        memF (Attribute   _ mn _ _ mAnns) = Just $ (mn, mapMaybe extractDProperty mAnns)
        memF (MAnnotation _ _ _) = Nothing

        extractDProperty (DProperty n lopt) = Just (n, lopt)
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
                   case lookup i $ concat annEnvs of
                     Nothing -> invalidAnnMemErr i tAnns
                     Just pl -> return pl
                 _ -> left $ "Invalid projected type while inferring properties"
      return $ foldl (@+) (EC.project i $ head ch) $ rebuildAnnotations e memPL

    inferProp ch (Node (tg :@: anns) _) = return $ Node (tg :@: anns) ch

    extNullExprProps :: Identifier -> PInfM ()
    extNullExprProps i = modify $ \env -> piexte env i pl0

    extExprProps :: Identifier -> K3 Expression -> PInfM ()
    extExprProps i (annotations -> anns) = modify (\env -> piexte env i $ mapMaybe extractEProperty anns)

    pruneExprProps :: Identifier -> PInfM ()
    pruneExprProps i = modify (\env -> pidele env i)

    rebuildAnnotations :: K3 Expression -> PList -> [Annotation Expression]
    rebuildAnnotations e pl = nub $ annotations e ++ map (uncurry EProperty) pl

    extractEProperty :: Annotation Expression -> Maybe (Identifier, Maybe (K3 Literal))
    extractEProperty (EProperty n lopt) = Just (n, lopt)
    extractEProperty _ = Nothing

    iu :: PInfM ()
    iu = return ()

    invalidAnnMemErr i tAnns = left $ unwords ["Invalid annotation member while inferring properties: ", i, "in", show tAnns]
