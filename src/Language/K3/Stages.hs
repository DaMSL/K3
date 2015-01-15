{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Arrow ( second )
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.List
import Debug.Trace

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import Language.K3.Analysis.HMTypes.Inference hiding ( liftEitherM, tenv, inferDeclTypes )

import qualified Language.K3.Analysis.Properties           as Properties
import qualified Language.K3.Analysis.CArgs                as CArgs
import qualified Language.K3.Analysis.Provenance.Inference as Provenance
import qualified Language.K3.Analysis.SEffects.Inference   as SEffects

import Language.K3.Analysis.Effects.InsertEffects(EffectEnv)
import qualified Language.K3.Analysis.Effects.InsertEffects as Effects
import qualified Language.K3.Analysis.Effects.Purity        as Purity
import qualified Language.K3.Analysis.InsertMembers         as InsertMembers

import Language.K3.Transform.LambdaForms
import Language.K3.Transform.NRVOMove
import Language.K3.Transform.Simplification
import Language.K3.Transform.Writeback
import Language.K3.Transform.Common

import Language.K3.Utils.Pretty

import Data.Text ( Text )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

-- | The program transformation composition monad
data TransformSt = TransformSt { maxuid :: Int
                               , tenv   :: TIEnv
                               , prenv  :: Properties.PIEnv
                               , penv   :: Provenance.PIEnv
                               , fenv   :: SEffects.FIEnv
                               , ofenv  :: Maybe EffectEnv }

type TransformM = EitherT String (State TransformSt)

st0 :: K3 Declaration -> Either String TransformSt
st0 prog = mkEnv >>= \(stpe, stfe) ->
  return $ TransformSt puid tienv0 Properties.pienv0 stpe stfe Nothing
  where puid = ((\(UID i) -> i) $ maxProgramUID prog) + 1
        mkEnv = do
          lcenv <- lambdaClosures prog
          let pe = Provenance.pienv0 lcenv
          return (pe, SEffects.fienv0 (Provenance.ppenv pe) lcenv)

runTransformStM :: TransformSt -> TransformM a -> Either String (a, TransformSt)
runTransformStM st m = let (a,b) = runState (runEitherT m) st in a >>= return . (,b)

runTransformM :: TransformSt -> TransformM a -> Either String a
runTransformM st m = runTransformStM st m >>= return . fst

liftEitherM :: Either String a -> TransformM a
liftEitherM = either left return

{- Transform utilities -}
type ProgramTransform = K3 Declaration -> TransformM (K3 Declaration)

type TrF  = K3 Declaration -> K3 Declaration
type TrE  = K3 Declaration -> Either String (K3 Declaration)
type TrSF = TransformSt -> K3 Declaration -> K3 Declaration
type TrSE = TransformSt -> K3 Declaration -> Either String (K3 Declaration)

type TrEE  = EffectEnv -> K3 Declaration -> K3 Declaration
type TrEEO = (K3 Declaration, Maybe EffectEnv) -> Either String (K3 Declaration, Maybe EffectEnv)

runPasses :: [ProgramTransform] -> ProgramTransform
runPasses passes p = foldM (flip ($)) p passes

bracketPasses :: ProgramTransform -> [ProgramTransform] -> [ProgramTransform]
bracketPasses f l = [f] ++ l ++ [f]

type PrpTrE = Properties.PIEnv -> K3 Declaration -> Either String (K3 Declaration, Properties.PIEnv)
type TypTrE = TIEnv -> K3 Declaration -> Either String (K3 Declaration, TIEnv)
type EffTrE = Provenance.PIEnv -> SEffects.FIEnv -> K3 Declaration
              -> Either String (K3 Declaration, Provenance.PIEnv, SEffects.FIEnv)

withPropertyTransform :: PrpTrE -> ProgramTransform
withPropertyTransform f p = do
  st <- get
  (np, pre) <- liftEitherM $ f (prenv st) p
  void $ put $ st {prenv=pre}
  return np

withTypeTransform :: TypTrE -> ProgramTransform
withTypeTransform f p = do
  st <- get
  (np, te) <- liftEitherM $ f (tenv st) p
  void $ put $ st {tenv=te}
  return np

withEffectTransform :: EffTrE -> ProgramTransform
withEffectTransform f p = do
  st <- get
  (np,pe,fe) <- liftEitherM (f (penv st) (fenv st) p)
  void $ {-debugEffectTransform (penv st) (fenv st) pe fe $-} put (st {penv=pe, fenv=fe})
  return np

  where debugEffectTransform oldp oldf newp newf r =
          trace (T.unpack $ PT.boxToString $ [T.pack "Effect xform"]
                  PT.%$ [T.pack "Old P"] PT.%$ PT.prettyLines oldp
                  PT.%$ [T.pack "New P"] PT.%$ PT.prettyLines newp
                  PT.%$ [T.pack "Old F"] PT.%$ PT.prettyLines oldf
                  PT.%$ [T.pack "New F"] PT.%$ PT.prettyLines newf)
            $ r


{- Transform constructors -}
transformF :: TrF -> ProgramTransform
transformF f p = return $ f p

transformE :: TrE -> ProgramTransform
transformE f p = liftEitherM $ f p

transformEDbg :: String -> TrE -> ProgramTransform
transformEDbg tg f p = do
  p' <- mkTg "Before " p $ transformE f p
  mkTg "After " p' $ return p'
  where mkTg pfx p' = trace (boxToString $ [pfx ++ tg] %$ prettyLines p')

transformSF :: TrSF -> ProgramTransform
transformSF f p = get >>= return . flip f p

transformSE :: TrSE -> ProgramTransform
transformSE f p = get >>= liftEitherM . flip f p

transformEE :: TrEE -> ProgramTransform
transformEE f p = do
  st <- get
  ee <- maybe (left "Invalid effect env") return $ ofenv st
  let np = f ee p
  return np

transformEEO :: TrEEO -> ProgramTransform
transformEEO f p = do
  st       <- get
  (np, ne) <- liftEitherM $ f (p, ofenv st)
  void $ put $ st {ofenv = ne}
  return np

transformFixpoint :: ProgramTransform -> ProgramTransform
transformFixpoint f p = do
  np <- f p
  (if compareDAST np p then return else transformFixpoint f) np

transformFixpointI :: [ProgramTransform] -> ProgramTransform -> ProgramTransform
transformFixpointI interF f p = do
  np <- f p
  if compareDAST np p then return np
  else runPasses interF np >>= transformFixpointI interF f

fixpointF :: TrF -> ProgramTransform
fixpointF f = transformFixpoint $ transformF f

fixpointE :: TrE -> ProgramTransform
fixpointE f = transformFixpoint $ transformE f

fixpointSF :: TrSF -> ProgramTransform
fixpointSF f = transformFixpoint $ transformSF f

fixpointSE :: TrSE -> ProgramTransform
fixpointSE f = transformFixpoint $ transformSE f

-- Fixpoint constructors with intermediate transformations between rounds.
fixpointIF :: [ProgramTransform] -> TrF -> ProgramTransform
fixpointIF interF f = transformFixpointI interF $ transformF f

fixpointIE :: [ProgramTransform] -> TrE -> ProgramTransform
fixpointIE interF f = transformFixpointI interF $ transformE f

fixpointISF :: [ProgramTransform] -> TrSF -> ProgramTransform
fixpointISF interF f = transformFixpointI interF $ transformSF f

fixpointISE :: [ProgramTransform] -> TrSE -> ProgramTransform
fixpointISE interF f = transformFixpointI interF $ transformSE f


{- Whole program analyses -}
inferTypes :: ProgramTransform
inferTypes prog = do
  (p, tienv) <- liftEitherM $ inferProgramTypes prog
  p' <- liftEitherM $ translateProgramTypes p
  void $ modify $ \st -> st {tenv = tienv}
  return p'

inferCEffects :: ProgramTransform
inferCEffects prog = do
  let pe = Effects.runConsolidatedAnalysis prog
  void $ modify $ \st -> st {ofenv = Just $ snd pe}
  return $ fst pe

inferSEffects :: ProgramTransform
inferSEffects prog = do
  (p,  pienv) <- liftEitherM $ Provenance.inferProgramProvenance prog
  (p', fienv) <- liftEitherM $ SEffects.inferProgramEffects Nothing (Provenance.ppenv pienv) p
  void $ modify $ \st -> st {penv = pienv, fenv = fienv}
  return p'

-- | Effect algorithm selection.
inferEffects :: ProgramTransform
inferEffects = inferSEffects

-- | Property propagation
inferProperties :: ProgramTransform
inferProperties prog = do
  (p, prienv) <- liftEitherM $ Properties.inferProgramUsageProperties prog
  void $ modify $ \st -> st {prenv = prienv}
  return p

inferTypesAndEffects :: ProgramTransform
inferTypesAndEffects p = inferTypes p >>= inferEffects

inferFreshTypes :: ProgramTransform
inferFreshTypes = inferTypes . stripTypeAnns

inferFreshEffects :: ProgramTransform
inferFreshEffects = inferEffects . stripEffectAnns

inferFreshTypesAndEffects :: ProgramTransform
inferFreshTypesAndEffects = inferTypesAndEffects . stripTypeAndEffectAnns

withTypecheck :: ProgramTransform -> ProgramTransform
withTypecheck f prog = inferTypes prog >>= f

withEffects :: ProgramTransform -> ProgramTransform
withEffects f prog = inferEffects prog >>= f

withTypeAndEffects :: ProgramTransform -> ProgramTransform
withTypeAndEffects f prog = f =<< inferEffects =<< inferTypes prog

withProperties :: ProgramTransform -> ProgramTransform
withProperties f p = transformE propF p >>= f
  where propF p' = Properties.inferProgramUsageProperties p' >>= return . fst

withRepair :: String -> ProgramTransform -> ProgramTransform
withRepair msg f prog = f prog >>= return . repairProgram msg

{- Whole program optimizations -}
simplify :: ProgramTransform
simplify = transformFixpoint $ runPasses simplifyPasses
  where simplifyPasses = intersperse inferFreshTypesAndEffects $
                           map (mkXform False) [ ("CF", foldProgramConstants)
                                               , ("BR", betaReductionOnProgram)
                                               , ("DCE", eliminateDeadProgramCode) ]
        mkXform asDebug (i,f) = withRepair i $ (if asDebug then transformEDbg i else transformE) f

simplifyWCSE :: ProgramTransform
simplifyWCSE p = simplify p >>= transformE commonProgramSubexprElim

streamFusion :: ProgramTransform
streamFusion = withProperties $ \p -> fusionEncode p >>= fusionFixpoint
  where mkXform       i f = withRepair i $ transformE f
        fusionEncode      = mkXform "fusionEncode"    encodeProgramTransformers
        fusionTransform   = mkXform "fusionTransform" fuseProgramFoldTransformers
        fusionReduce      = mkXform "fusionReduce"    betaReductionOnProgram
        fusionFixpoint    = transformFixpointI fusionInterF fusionTransform
        fusionInterF      = bracketPasses inferFreshTypesAndEffects [fusionReduce]


{- Whole program pass aliases -}
optPasses :: [ProgramTransform]
optPasses = map prepareOpt [ (simplify,     "opt-simplify-prefuse")
                           , (streamFusion, "opt-fuse")
                           , (simplify,     "opt-simplify-final") ]
  where prepareOpt (f,i) = runPasses [inferFreshTypesAndEffects, withRepair i f]

cgPasses :: Int -> [ProgramTransform]
-- no moves
cgPasses 3 = [inferFreshEffects,
              transformEE   Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEE   nrvoMoveOpt,
              transformEEO  writebackOptPT,
              transformEE $ lambdaFormOpt noMovesTransConfig
             ]

-- no refs
cgPasses 2 = [inferFreshEffects,
              transformEE   Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEE   nrvoMoveOpt,
              transformEEO  writebackOptPT,
              transformEE $ lambdaFormOpt noRefsTransConfig
             ]

cgPasses 1 = [inferFreshEffects,
              transformEE   Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEE   nrvoMoveOpt,
              transformEEO  writebackOptPT,
              transformEE $ lambdaFormOpt defaultTransConfig
             ]

cgPasses _ = [transformF InsertMembers.runAnalysis,
              transformF CArgs.runAnalysis
             ]

runOptPassesM :: ProgramTransform
runOptPassesM prog = runPasses optPasses $ stripTypeAndEffectAnns prog

runCGPassesM :: Int -> ProgramTransform
runCGPassesM lvl prog = runPasses (cgPasses lvl) prog

-- Legacy methods.
runOptPasses :: K3 Declaration -> Either String (K3 Declaration, Maybe EffectEnv)
runOptPasses prog = st0 prog >>= flip runTransformStM (runOptPassesM prog) >>= return . second ofenv

runCGPasses :: K3 Declaration -> Int -> Either String (K3 Declaration)
runCGPasses prog lvl = st0 prog >>= flip runTransformM (runCGPassesM lvl prog)


{- Declaration-at-a-time analyses and optimizations. -}

-- | Program traversal with fixpoints per declaration.
mapProgramDecls :: (K3 Declaration -> [ProgramTransform]) -> ProgramTransform
mapProgramDecls passesF prog = mapProgram declFixpoint return return Nothing prog
  where declFixpoint d = runPasses (passesF d) d

mapProgramDeclFixpoint :: (K3 Declaration -> [ProgramTransform]) -> ProgramTransform
mapProgramDeclFixpoint passesF prog = mapProgram declFixpoint return return Nothing prog
  where declFixpoint d = (transformFixpoint $ runPasses $ passesF d) d

inferDeclProperties :: Identifier -> ProgramTransform
inferDeclProperties n = withPropertyTransform $ \pre p ->
  Properties.reinferProgDeclUsageProperties pre n p

inferDeclTypes :: Identifier -> ProgramTransform
inferDeclTypes n = withTypeTransform $ \te p -> reinferProgDeclTypes te n p

inferDeclEffects :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
inferDeclEffects extInfOpt n = withEffectTransform $ \pe fe p -> do
  (nlc, _)   <- lambdaClosuresDecl n (Provenance.plcenv pe) p
  let pe'     = pe {Provenance.plcenv = nlc}
  (np,  npe) <- {-debugPretty ("Reinfer P\n" ++ show nlc) p $-} Provenance.reinferProgDeclProvenance pe' n p
  let fe'     = fe {SEffects.fppenv = Provenance.ppenv npe, SEffects.flcenv = nlc}
  (np', nfe) <- {-debugPretty "Reinfer F" fe' $-} SEffects.reinferProgDeclEffects extInfOpt fe' n np
  return (np', npe, nfe)
  where debugPretty tg a b = trace (T.unpack $ PT.boxToString $ [T.pack tg] PT.%$ PT.prettyLines a) b

inferFreshDeclTypesAndEffects :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
inferFreshDeclTypesAndEffects extInfOpt n =
  runPasses [inferDeclTypes n, inferDeclEffects extInfOpt n] . stripDeclTypeAndEffectAnns n

-- TODO: each transformation here should be a local fixpoint, as well as the current pipeline fixpoint.
simplifyDecl :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
simplifyDecl extInfOpt n = runPasses simplifyPasses
  where simplifyPasses = intersperse (inferFreshDeclTypesAndEffects extInfOpt n) $
                           map (mkXform False) [ ("Decl-CF",  foldConstants)
                                               , ("Decl-BR",  betaReduction)
                                               , ("Decl-DCE", eliminateDeadCode) ]
        mkXform asDebug (i,f) = withRepair i $
          (if asDebug then transformEDbg i else transformE) $ mapNamedDeclExpression n f

-- TODO: incremental version of withProperties
streamFusionDecl :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
streamFusionDecl extInfOpt n = runPasses [inferDeclProperties n, fusionEncode, fusionFixpoint]
  where mkXform       i f = withRepair i $ transformE $ mapNamedDeclExpression n f
        fusionEncode      = mkXform "fusionEncode"    encodeTransformers
        fusionTransform   = mkXform "fusionTransform" fuseFoldTransformers
        fusionReduce      = mkXform "fusionReduce"    betaReduction
        fusionFixpoint    = transformFixpointI fusionInterF fusionTransform
        fusionInterF      = bracketPasses (inferFreshDeclTypesAndEffects extInfOpt n) [fusionReduce]

declOptPasses :: Maybe (SEffects.ExtInferF a, a) -> K3 Declaration -> [ProgramTransform]
declOptPasses extInfOpt d = case nameOfDecl d of
  Nothing -> []
  Just n -> map (prepareOpt n) [ (simplifyDecl     extInfOpt n, "opt-decl-simplify-prefuse")
                               , (streamFusionDecl extInfOpt n, "opt-decl-fuse")
                               , (simplifyDecl     extInfOpt n, "opt-decl-simplify-final") ]
  where prepareOpt n (f,i) = runPasses [inferFreshDeclTypesAndEffects extInfOpt n, withRepair i f]
        nameOfDecl (tag -> DGlobal  n _ (Just _)) = Just n
        nameOfDecl (tag -> DTrigger n _ _) = Just n
        nameOfDecl _ = Nothing

runDeclOptPassesM :: Maybe (SEffects.ExtInferF a, a) -> ProgramTransform
runDeclOptPassesM extInfOpt =
  runPasses [inferFreshTypesAndEffects, inferProperties, mapProgramDeclFixpoint (declOptPasses extInfOpt)]

runDeclOptPasses :: Maybe (SEffects.ExtInferF a, a) -> K3 Declaration -> Either String (K3 Declaration)
runDeclOptPasses extInfOpt prog = st0 prog >>= flip runTransformM (runDeclOptPassesM extInfOpt prog)