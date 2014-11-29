{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Provenance (i.e., dataflow) analysis for K3 programs.
module Language.K3.Analysis.Provenance.Inference where

import Control.Arrow hiding ( left )
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.List
import Data.Maybe
import Data.Tree

import Data.Map    ( Map    )
import Data.IntMap ( IntMap )
import qualified Data.Map    as Map
import qualified Data.IntMap as IntMap

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Constructors

import Data.Text ( Text )
import Language.K3.Utils.PrettyText ( Pretty, (%$), (%+) )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

prvTraceLogging :: Bool
prvTraceLogging = False

localLog :: (Functor m, Monad m) => String -> m ()
localLog = logVoid prvTraceLogging

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
localLogAction = logAction prvTraceLogging


-- | A provenance bindings environment, layered to support shadowing.
type PEnv = Map Identifier [K3 Provenance]

-- | A provenance bindings environment for annotations,
--   indexed by annotation and attribute name.
type PAEnv = Map Identifier PMEnv
type PMEnv = Map Identifier (K3 Provenance, Bool)

-- | A provenance "pointer" environment
type PPEnv = IntMap (K3 Provenance)

-- | A lambda closure environment: ELambda UID => identifiers of closure variables.
type PLCEnv = IntMap [Identifier]

-- | A mapping of expression ids and provenance ids.
type EPMap = IntMap (K3 Provenance)

-- | A provenance inference environment.
data PIEnv = PIEnv {
               pcnt    :: Int,
               ppenv   :: PPEnv,
               penv    :: PEnv,
               paenv   :: PAEnv,
               plcenv  :: PLCEnv,
               epmap   :: EPMap
            }

-- | The type inference monad
type PInfM = EitherT Text (State PIEnv)

{- Data.Text helpers -}
mkErr :: String -> Either Text a
mkErr msg = Left $ T.pack msg

mkErrP :: PT.Pretty a => String -> a -> Either Text b
mkErrP msg a = Left $ T.unlines [T.pack msg, PT.pretty a]

{- PPEnv helpers -}
ppenv0 :: PPEnv
ppenv0 = IntMap.empty

pplkup :: PPEnv -> Int -> Either Text (K3 Provenance)
pplkup env x = maybe err Right $ IntMap.lookup x env
  where err = mkErrP msg env
        msg = "Unbound variable in lineage pointer environment: " ++ show x

ppext :: PPEnv -> Int -> K3 Provenance -> PPEnv
ppext env x p = IntMap.insert x p env

ppdel :: PPEnv -> Int -> PPEnv
ppdel env x = IntMap.delete x env


{- PEnv helpers -}
penv0 :: PEnv
penv0 = Map.empty

plkup :: PEnv -> Identifier -> Either Text (K3 Provenance)
plkup env x = maybe err safeHead $ Map.lookup x env
  where 
    safeHead l = if null l then err else Right $ head l
    err = mkErrP msg env
    msg = "Unbound variable in lineage binding environment: " ++ x

pext :: PEnv -> Identifier -> K3 Provenance -> PEnv
pext env x p = Map.insertWith (++) x [p] env

pdel :: PEnv -> Identifier -> PEnv
pdel env x = Map.update safeTail x env
  where safeTail []  = Nothing
        safeTail [_] = Nothing
        safeTail l   = Just $ tail l


{- PAEnv helpers -}
paenv0 :: PAEnv
paenv0 = Map.empty

pmenv0 :: PMEnv
pmenv0 = Map.empty

palkup :: PAEnv -> Identifier -> Identifier -> Either Text (K3 Provenance)
palkup env x y = maybe err (maybe err (Right . fst) . Map.lookup y) $ Map.lookup x env
  where err = mkErr $ "Unbound annotation member in lineage environment: " ++ x

paext :: PAEnv -> Identifier -> Identifier -> K3 Provenance -> Bool -> PAEnv
paext env x y p l = Map.insertWith Map.union x (Map.fromList [(y,(p,l))]) env

palkups :: PAEnv -> Identifier -> Either Text PMEnv
palkups env x = maybe err Right $ Map.lookup x env
  where err = mkErr $ "Unbound annotation in lineage environment: " ++ x

paexts :: PAEnv -> Identifier -> PMEnv -> PAEnv
paexts env x ap' = Map.insertWith Map.union x ap' env

padel :: PAEnv -> Identifier -> Identifier -> PAEnv
padel env x y = (\f -> Map.alter f x env) $ \case
  Nothing -> Nothing
  Just aenv -> let naenv = Map.delete y aenv
               in if Map.null naenv then Nothing else Just naenv

{- PLCEnv helpers -}
plcenv0 :: PLCEnv
plcenv0 = IntMap.empty

plclkup :: PLCEnv -> Int -> Either Text [Identifier]
plclkup env x = maybe err Right $ IntMap.lookup x env
  where err = mkErr $ "Unbound variable in lineage environment: " ++ show x

plcext :: PLCEnv -> Int -> [Identifier] -> PLCEnv
plcext env x p = IntMap.insert x p env

plcdel :: PLCEnv -> Int -> PLCEnv
plcdel env x = IntMap.delete x env

plcunions :: [PLCEnv] -> PLCEnv
plcunions = IntMap.unions

{- EPMap helpers -}
epmap0 :: EPMap
epmap0 = IntMap.empty

eplkup :: EPMap -> K3 Expression -> Either Text (K3 Provenance)
eplkup epm e@((@~ isEUID) -> Just (EUID (UID uid))) = maybe lookupErr Right $ IntMap.lookup uid epm
  where lookupErr = Left $ PT.boxToString $ msg %$ PT.prettyLines e
        msg = [T.unwords $ map T.pack ["No provenance found for", show uid]]

eplkup _ e = Left $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e

epext :: EPMap -> K3 Expression -> K3 Provenance -> Either Text EPMap
epext epm e p = case e @~ isEUID of
  Just (EUID (UID i)) -> Right $ IntMap.insert i p epm
  _ -> Left $ PT.boxToString $ [T.pack "No UID found on "] %+ PT.prettyLines e

{- PIEnv helpers -}
pienv0 :: PLCEnv -> PIEnv
pienv0 lcenv = PIEnv 0 ppenv0 penv0 paenv0 lcenv epmap0

-- | Modifiers.
mpiee :: (PEnv -> PEnv) -> PIEnv -> PIEnv
mpiee f env = env {penv = f $ penv env}

mpiep :: (PPEnv -> PPEnv) -> PIEnv -> PIEnv
mpiep f env = env {ppenv = f $ ppenv env}

pilkupe :: PIEnv -> Identifier -> Either Text (K3 Provenance)
pilkupe env x = plkup (penv env) x

piexte :: PIEnv -> Identifier -> K3 Provenance -> PIEnv
piexte env x p = env {penv=pext (penv env) x p}

pidele :: PIEnv -> Identifier -> PIEnv
pidele env i = env {penv=pdel (penv env) i}

pilkupp :: PIEnv -> Int -> Either Text (K3 Provenance)
pilkupp env x = pplkup (ppenv env) x

piextp :: PIEnv -> Int -> K3 Provenance -> PIEnv
piextp env x p = env {ppenv=ppext (ppenv env) x p}

pidelp :: PIEnv -> Int -> PIEnv
pidelp env i = env {ppenv=ppdel (ppenv env) i}

pilkupas :: PIEnv -> Identifier -> Either Text PMEnv
pilkupas env x = palkups (paenv env) x

piextas :: PIEnv -> Identifier -> PMEnv -> PIEnv
piextas env x p = env {paenv=paexts (paenv env) x p}

pilkupm :: PIEnv -> K3 Expression -> Either Text (K3 Provenance)
pilkupm env e = eplkup (epmap env) e

piextm :: PIEnv -> K3 Expression -> K3 Provenance -> Either Text PIEnv
piextm env e p = let epmapE = epext (epmap env) e p
                 in either Left (\nep -> Right $ env {epmap=nep}) epmapE

pilkupc :: PIEnv -> Int -> Either Text [Identifier]
pilkupc env i = plclkup (plcenv env) i


{- Fresh pointer and binding construction. -}

-- | Self-referential provenance pointer construction
pifreshfp :: PIEnv -> VarLoc -> (K3 Provenance, PIEnv)
pifreshfp pienv vl = 
  let i = pcnt pienv
      p = pbvar vl i
  in (p, piextp (pienv {pcnt=i+1}) i p)

pifreshbp :: PIEnv -> VarLoc -> K3 Provenance -> (K3 Provenance, PIEnv)
pifreshbp pienv vl p = 
  let i  = pcnt pienv
      p' = pbvar vl i
  in (p', piextp (pienv {pcnt=i+1}) i p)

-- | Self-referential provenance pointer construction
--   This adds a new named pointer to both the named and pointer environments.
pifreshs :: PIEnv -> Identifier -> UID -> (K3 Provenance, PIEnv)
pifreshs pienv n u = 
  let (p, nenv) = pifreshfp pienv (n,u) in (p, piexte nenv n p)

-- | Provenance linked pointer construction.
pifresh :: PIEnv -> Identifier -> UID -> K3 Provenance -> (K3 Provenance, PIEnv)
pifresh pienv n u p = 
  let (p', nenv) = pifreshbp pienv (n,u) p in (p', piexte nenv n p')

pifreshAs :: PIEnv -> Identifier -> [(Identifier, UID, Bool)] -> (PMEnv, PIEnv)
pifreshAs pienv n memN = 
  let mkMemP lacc l p             = lacc++[(p,l)]
      extMemP (lacc,eacc) (i,u,l) = first (mkMemP lacc l) $ pifreshfp eacc (i,u)
      (memP, npienv)              = foldl extMemP ([], pienv) memN
      memNP                       = Map.fromList $ zip (map (\(a,_,_) -> a) memN) memP
  in (memNP, piextas npienv n memNP)

{- Provenance pointer helpers -}

-- | Retrieves the provenance value referenced by a named pointer
piload :: PIEnv -> Identifier -> Either Text (K3 Provenance)
piload pienv n = do
  p <- pilkupe pienv n
  case tag p of
    PBVar _ i -> pilkupp pienv i
    _ -> Left $ PT.boxToString $ [T.pack "Invalid load on pointer"] %$ PT.prettyLines p

-- | Sets the provenance value referenced by a named pointer
pistore :: PIEnv -> Identifier -> UID -> K3 Provenance -> Either Text PIEnv
pistore pienv n u p = do
  p' <- pilkupe pienv n
  case tag p' of
    PBVar vl i | vl == (n,u) -> return $ piextp pienv i p
    _ -> Left $ PT.boxToString $ [T.pack "Invalid store on pointer"] %$ PT.prettyLines p'

pistorea :: PIEnv -> Identifier -> [(Identifier, UID, K3 Provenance, Bool)] -> Either Text PIEnv
pistorea pienv n memP = do
  pmenv <- pilkupas pienv n
  foldM (storemem pmenv) pienv memP

  where storemem pmenv eacc (i,u,p,_) = maybe (invalidMem i) (\(p',_) -> store eacc i u p' p) $ Map.lookup i pmenv
        store eacc i u (tag -> PBVar vl j) p | vl == (i,u) = return $ piextp eacc j p
        store _ i u p _ = invalidStore (i,u) p
        
        invalidMem   i    = mkErr $ "Invalid store on annotation member" ++ i
        invalidStore vl p = Left $ PT.boxToString $ storeMsg vl %$ PT.prettyLines p
        storeMsg vl       = [T.unwords $ map T.pack ["Invalid store on pointer", "@loc", show vl]]

-- | Traverses all pointers until reaching a non-pointer. 
--   This function stops on any cycles detected.
pichase :: PIEnv -> K3 Provenance -> Either Text (K3 Provenance)
pichase pienv cp = aux [] cp
  where aux path p@(tag -> PBVar _ i) | i `elem` path = return p
                                      | otherwise = pilkupp pienv i >>= aux (i:path)
        aux _ p = return p

-- Capture-avoiding substitution of any free variable with the given identifier.
pisub :: PIEnv -> Identifier -> K3 Provenance -> K3 Provenance -> Either Text (K3 Provenance)
pisub pienv i dp sp = acyclicSub [] sp
  where
    acyclicSub _ (tag -> PFVar j) | i == j = return dp

    acyclicSub path p@(tag -> PBVar _ j)
      | j `elem` path = return p
      | otherwise     = pilkupp pienv j >>= acyclicSub (j:path)

    -- TODO: we can short-circuit descending into the body if we
    -- are willing to stash the expression uid in a PLambda, using
    -- this uid to lookup i's presence in our precomputed closures.
    acyclicSub _ p@(tag -> PLambda j) | i == j = return p

    acyclicSub path n@(Node _ ch) = mapM (acyclicSub path) ch >>= return . replaceCh n 


{- PInfM helpers -}

runPInfM :: PIEnv -> PInfM a -> (Either Text a, PIEnv)
runPInfM env m = flip runState env $ runEitherT m

reasonM :: (Text -> Text) -> PInfM a -> PInfM a
reasonM errf = mapEitherT $ \m -> m >>= \case
  Left  err -> get >>= \env -> (return . Left $ errf $ T.unlines [err, T.pack "Provenance environment:", PT.pretty env])
  Right r   -> return $ Right r

errorM :: Text -> PInfM a
errorM msg = reasonM id $ left msg

liftEitherM :: Either Text a -> PInfM a
liftEitherM = either left return

pifreshbpM :: VarLoc -> K3 Provenance -> PInfM (K3 Provenance)
pifreshbpM vl p = get >>= return . (\env -> pifreshbp env vl p) >>= \(p',nenv) -> put nenv >> return p'

pifreshsM :: Identifier -> UID -> PInfM (K3 Provenance)
pifreshsM n u = get >>= return . (\env -> pifreshs env n u) >>= \(p,env) -> put env >> return p

pifreshM :: Identifier -> UID -> K3 Provenance -> PInfM (K3 Provenance)
pifreshM n u p = get >>= return . (\env -> pifresh env n u p) >>= \(p',env) -> put env >> return p'

pifreshAsM :: Identifier -> [(Identifier, UID, Bool)] -> PInfM PMEnv
pifreshAsM n mems = get >>= return . (\env -> pifreshAs env n mems) >>= \(p,env) -> put env >> return p

pilkupeM :: Identifier -> PInfM (K3 Provenance)
pilkupeM n = get >>= liftEitherM . flip pilkupe n

piexteM :: Identifier -> K3 Provenance -> PInfM ()
piexteM n p = get >>= \env -> return (piexte env n p) >>= put

pideleM :: Identifier -> PInfM ()
pideleM n = get >>= \env -> return (pidele env n) >>= put

pilkupasM :: Identifier -> PInfM PMEnv
pilkupasM n = get >>= liftEitherM . flip pilkupas n

pilkupmM :: K3 Expression -> PInfM (K3 Provenance)
pilkupmM e = get >>= liftEitherM . flip pilkupm e

piextmM :: K3 Expression -> K3 Provenance -> PInfM ()
piextmM e p = get >>= liftEitherM . (\env -> piextm env e p) >>= put

piloadM :: Identifier -> PInfM (K3 Provenance)
piloadM n = get >>= liftEitherM . flip piload n

pistoreM :: Identifier -> UID -> K3 Provenance -> PInfM ()
pistoreM n u p = get >>= liftEitherM . (\env -> pistore env n u p) >>= put

pistoreaM :: Identifier -> [(Identifier, UID, K3 Provenance, Bool)] -> PInfM ()
pistoreaM n memP = get >>= liftEitherM . (\env -> pistorea env n memP) >>= put

pichaseM :: K3 Provenance -> PInfM (K3 Provenance)
pichaseM p = get >>= liftEitherM . flip pichase p

pisubM :: Identifier -> K3 Provenance -> K3 Provenance -> PInfM (K3 Provenance)
pisubM i rep p = get >>= liftEitherM . (\env -> pisub env i rep p)

pilkupcM :: Int -> PInfM [Identifier]
pilkupcM n = get >>= liftEitherM . flip pilkupc n


-- | Compute the lambda closure environment by tracking free variables at lambdas.
--   This is a one-pass bottom-up implementation.
extractLambdaClosures :: K3 Declaration -> Either Text PLCEnv
extractLambdaClosures p = foldExpression lambdaClosure plcenv0 p >>= return . fst
  where
    lambdaClosure :: PLCEnv -> K3 Expression -> Either Text (PLCEnv, K3 Expression) 
    lambdaClosure lcAcc expr = do
      (lcenv,_) <- biFoldMapTree bind extract [] (plcenv0, []) expr
      return $ (plcunions [lcAcc, lcenv], expr)

    bind :: [Identifier] -> K3 Expression -> Either Text ([Identifier], [[Identifier]])
    bind l (tag -> ELambda i) = return (l, [i:l])
    bind l (tag -> ELetIn  i) = return (l, [l, i:l])
    bind l (tag -> EBindAs b) = return (l, [l, bindingVariables b ++ l])
    bind l (tag -> ECaseOf i) = return (l, [l, i:l, l])
    bind l (children -> ch)   = return (l, replicate (length ch) l)

    extract :: [Identifier] -> [(PLCEnv, [Identifier])] -> K3 Expression -> Either Text (PLCEnv, [Identifier])
    extract _ chAcc (tag -> EVariable i) = rt chAcc (++[i])
    extract _ chAcc (tag -> EAssign i)   = rt chAcc (++[i])
    extract l (concatLc -> (lcAcc,chAcc)) e@(tag -> ELambda n) = extendLc lcAcc e $ filter (onlyLocals n l) $ concat chAcc
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> EBindAs b) = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> ELetIn i)  = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> ECaseOf i) = return . (lcAcc,) $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extract _ chAcc _ = rt chAcc id

    onlyLocals n l i = i /= n && i `elem` l

    concatLc :: [(PLCEnv, [Identifier])] -> (PLCEnv, [[Identifier]])
    concatLc subAcc = let (x,y) = unzip subAcc in (plcunions x, y)

    extendLc :: PLCEnv -> K3 Expression -> [Identifier] -> Either Text (PLCEnv, [Identifier])
    extendLc lcenv e ids = case e @~ isEUID of
      Just (EUID (UID i)) -> return $ (plcext lcenv i ids, ids)
      _ -> Left $ PT.boxToString $ [T.pack "No UID found on lambda"] %$ PT.prettyLines e

    rt subAcc f = return $ second (f . concat) $ concatLc subAcc

inferProgramProvenance :: K3 Declaration -> Either String (K3 Declaration)
inferProgramProvenance p = do
  lcenv <- liftEitherTM $ extractLambdaClosures p
  let (npE,initEnv) = runPInfM (pienv0 lcenv) (globalsProv p)
  np <- liftEitherTM npE
  liftEitherTM $ fst $ runPInfM initEnv $ mapExpression inferExprProvenance np

  where
    liftEitherTM = either (Left . T.unpack) Right

    -- TODO: handle provenance annotations from the parser.
    globalsProv :: K3 Declaration -> PInfM (K3 Declaration)
    globalsProv d = do
      nd <- mapProgram rcrDeclProv return return Nothing d
      mapProgram declProv return return Nothing nd

    -- Add "blind" provenance pointers to every global for recursive calls.
    rcrDeclProv d@(tag -> DGlobal  n _ _) = uidOf d >>= \u -> pifreshsM n u >> return d
    rcrDeclProv d@(tag -> DTrigger n _ _) = uidOf d >>= \u -> pifreshsM n u >> return d
    rcrDeclProv d@(tag -> DDataAnnotation n _ mems) = mapM freshMems mems >>= pifreshAsM n . catMaybes >> return d
    rcrDeclProv d = return d

    freshMems m@(Lifted      Provides mn _ _ mas) = memUID m mas >>= \u -> return (Just (mn, u, True))
    freshMems m@(Attribute   Provides mn _ _ mas) = memUID m mas >>= \u -> return (Just (mn, u, False))
    freshMems _ = return Nothing

    -- Infer, replace pid->initializer
    declProv :: K3 Declaration -> PInfM (K3 Declaration)
    declProv d@(tag -> DGlobal n t eOpt) = do
      u <- uidOf d
      p' <- maybe (provOfType [] t) inferProvenance eOpt
      void $ pistoreM n u $ pglobal n p'
      return d

    declProv d@(tag -> DTrigger n _ e) = do
      u <- uidOf d
      p' <- inferProvenance e
      void $ pistoreM n u $ pglobal n p'
      return d

    declProv d@(tag -> DDataAnnotation n _ mems) = mapM inferMems mems >>= pistoreaM n . catMaybes >> return d
    declProv d = return d

    inferMems m@(Lifted      Provides mn mt meOpt mas) = memUID m mas >>= \u -> inferMember True  mn mt meOpt u
    inferMems m@(Attribute   Provides mn mt meOpt mas) = memUID m mas >>= \u -> inferMember False mn mt meOpt u
    inferMems _ = return Nothing

    inferMember lifted mn mt meOpt u = do
      mp <- maybe (provOfType [] mt) inferProvenance meOpt
      return $ Just (mn,u,mp,lifted)

    uidOf  n = maybe (uidErr n) (\case {(DUID u) -> return u ; _ ->  uidErr n}) $ n @~ isDUID
    uidErr n = errorM $ PT.boxToString $ [T.pack "No uid found on "] %+ PT.prettyLines n

    memUID memDecl as = case filter isDUID as of
                          [DUID u] -> return u
                          _ -> memUIDErr memDecl
    memUIDErr memDecl = errorM $ T.append (T.pack "Invalid member UID") $ PT.pretty memDecl


-- | Compute a provenance tree in a single pass, tracking expression-provenance associations.
--   Then, apply a second pass to attach associated provenances to each expression node.
inferExprProvenance :: K3 Expression -> PInfM (K3 Expression)
inferExprProvenance expr = inferProvenance expr >> substituteProvenance expr

-- | Computes a single extended provenance tree for an expression.
--   This includes an extra child at each PApply indicating the return value provenance
--   of the apply. The provenance associated with each subexpression is stored as a
--   separate relation in the environment rather than directly attached to each node.
inferProvenance :: K3 Expression -> PInfM (K3 Provenance)
inferProvenance expr = mapIn1RebuildTree topdown sideways inferAndSave expr
  where 
    iu = return ()
    uidOf  e = maybe (uidErr e) (\case {(EUID u) -> return u ; _ ->  uidErr e}) $ e @~ isEUID
    uidErr e = errorM $ PT.boxToString $ [T.pack "No uid found on "] %+ PT.prettyLines e

    topdown _ e@(tag -> ELambda i) = piexteM i (pfvar i) >> pushClosure e
    topdown _ _ = iu

    sideways p e@(tag -> ELetIn  i) = uidOf e >>= \u -> return [freshM i u p]
    sideways p e@(tag -> ECaseOf i) = uidOf e >>= \u -> return [freshM i u $ poption p, pideleM i]
    sideways p e@(tag -> EBindAs b) = do
      u <- uidOf e
      case b of
        BIndirection i -> return [freshM i u $ pindirect p]
        BTuple is      -> return . (:[]) $ mapM_ (\(i,n) -> freshM n u $ ptuple i p) $ zip [0..length is -1] is
        BRecord ivs    -> return . (:[]) $ mapM_ (\(src,dest) -> freshM dest u $ precord src p) ivs

    sideways _ (children -> ch) = return $ replicate (length ch - 1) iu

    -- Compute provenance and stash in EPMap.
    inferAndSave ch e = infer ch e >>= \p -> piextmM e p >> return p

    infer _ (tag -> EConstant _) = return $ ptemp
    infer _ e@(tag -> EVariable i) = varErr e $ pilkupeM i

    infer [p] e@(tag -> ELambda i) = popClosure e >> pideleM i >> return (plambda i p)

    -- Return a papply with three children: the lambda, argument, and return value provenance.
    infer [lp, argp] e@(tag -> EOperate OApp) = do
      lp'  <- chaseApply [] lp
      case tnc lp' of
        (PLambda i, [bp]) -> do
          ip <- uidOf e >>= \u -> pifreshbpM (i,u) argp
          rp <- pisubM i ip bp
          return $ papply lp argp rp

        -- Handle recursive functions and forward declarations by using an opaque return value.
        (PBVar _ _, _) -> return $ papply lp argp ptemp

        _ -> appLambdaErr e lp'

    infer [psrc] (tnc -> (EProject i, [esrc])) = 
      case esrc @~ isEType of
        Just (EType t) ->
          case tag t of
            TCollection -> collectionMemberProvenance i psrc esrc t
            TRecord ids ->
              case tnc psrc of
                (PData _, pdch) -> do
                  idx <- maybe (memErr i esrc) return $ elemIndex i ids
                  return $ pproject i psrc $ Just $ pdch !! idx
                (_,_) -> return $ pproject i psrc Nothing
            _ -> prjErr esrc
        _ -> prjErr esrc

    infer pch   (tag -> EIfThenElse)   = return $ pset pch
    infer pch   (tag -> EOperate OSeq) = return $ last pch
    infer [p]   (tag -> EAssign i)     = return $ passign i p
    infer [_,p] (tag -> EOperate OSnd) = return $ psend p

    infer [_,lb]  (tag -> ELetIn  i) = pideleM i >> return lb
    infer [_,bb]  (tag -> EBindAs b) = mapM_ pideleM (bindingVariables b) >> return bb
    infer [_,s,n] (tag -> ECaseOf _) = return $ pset [s,n]

    -- Data constructors.
    infer pch (tag -> ESome)       = return $ pdata Nothing pch
    infer pch (tag -> EIndirect)   = return $ pdata Nothing pch
    infer pch (tag -> ETuple)      = return $ pdata Nothing pch
    infer pch (tag -> ERecord ids) = return $ pdata (Just ids) pch

    -- Operators and untracked primitives.
    infer pch (tag -> EOperate _)  = return $ derived pch
    infer pch (tag -> EAddress)    = return $ derived pch

    -- TODO: ESelf
    infer _ e = inferErr e

    -- | Closure variable management
    pushClosure ((@~ isEUID) -> Just (EUID (UID i))) = do
      clv <- pilkupcM i
      mapM_ (\n -> pilkupeM n >>= \p -> pideleM n >> piexteM n (pclosure p)) clv

    pushClosure e = errorM $ PT.boxToString $ [T.pack "Invalid UID on "] %+ PT.prettyLines e

    popClosure ((@~ isEUID) -> Just (EUID (UID i))) = do
      clv <- pilkupcM i
      mapM_ (\n -> pilkupeM n >>= \p -> pideleM n >> unwrapClosure p >>= piexteM n) clv

    popClosure e = errorM $ PT.boxToString $ [T.pack "Invalid UID on "] %+ PT.prettyLines e

    unwrapClosure (tnc -> (PClosure, [p])) = return p
    unwrapClosure p = errorM $ PT.boxToString $ [T.pack "Invalid closure variable "] %+ PT.prettyLines p

    -- TODO: this does not handle PSet, e.g., when applying the return value of an if-then-else
    chaseApply _ p@(tag -> PLambda _) = return p

    chaseApply path p@(tag -> PBVar _ i) 
      | i `elem` path = return p
      | otherwise     = pichaseM p >>= chaseApply (i:path)

    chaseApply path (tnc -> (PApply, [_,_,r]))   = chaseApply path r
    chaseApply path (tnc -> (PProject _, [_,r])) = chaseApply path r
    chaseApply path (tnc -> (PGlobal _, [r]))    = chaseApply path r
    chaseApply path (tnc -> (PChoice, r:_))      = chaseApply path r
    chaseApply _ p = errorM $ PT.boxToString $ [T.pack "Invalid application or lambda: "] %$ PT.prettyLines p

    freshM i u p = void $ pifreshM i u p

    derived ch = let ntch = filter (not . isPTemporary) ch
                 in if null ntch then ptemp else pderived ntch

    isPTemporary (tag -> PTemporary) = True
    isPTemporary _ = False

    inferErr e = errorM $ PT.boxToString $ [T.pack "Could not infer provenance for "] %$ PT.prettyLines e
    varErr e   = reasonM (\s -> T.unlines [s, T.pack "Variable access on", PT.pretty e])
    prjErr e   = errorM $ PT.boxToString $ [T.pack "Invalid type on "] %+ PT.prettyLines e
    appLambdaErr e p = errorM $ PT.boxToString
                              $  [T.pack "Invalid function provenance on:"] %$ PT.prettyLines e
                              %$ [T.pack "Provenance:"]                     %$ PT.prettyLines p

    memErr i e = errorM $ PT.boxToString $  [T.unwords $ map T.pack ["Failed to project", i, "from"]]
                                         %$ PT.prettyLines e

substituteProvenance :: K3 Expression -> PInfM (K3 Expression)
substituteProvenance expr = modifyTree injectProvenance expr
  where injectProvenance e = pilkupmM e >>= \p -> return (e @+ (EProvenance p))

-- | Construct the provenance of a collection field member projected from the given source.
--   TODO: this implementation currently recomputes a merged realization environment on every
--   call. This should be lifted to a cached realization environment.
collectionMemberProvenance :: Identifier -> K3 Provenance -> K3 Expression -> K3 Type -> PInfM (K3 Provenance)
collectionMemberProvenance i psrc e t =
  let annIds = namedTAnnotations $ annotations t in do
    memsEnv <- mapM pilkupasM annIds >>= return . Map.unions
    (mp, lifted) <- maybe memErr return $ Map.lookup i memsEnv
    if not lifted then attrErr else return $ pproject i psrc $ Just mp

  where memErr  = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Unknown projection of ", i, "on"]]           %+ PT.prettyLines e
        attrErr = errorM $ PT.boxToString $ [T.unwords $ map T.pack ["Invalid attribute projection of ", i, "on"]] %+ PT.prettyLines e

provOfType :: [Identifier] -> K3 Type -> PInfM (K3 Provenance)
provOfType args t | isTFunction t =
   case tnc t of
    (TForall _, [ch])      -> provOfType args ch
    (TFunction, [_, retT]) -> let a = mkArg (length args + 1)
                              in provOfType (args++[a]) retT >>= return . plambda a
    _ -> errorM $ PT.boxToString $ [T.pack "Invalid function type"] %+ PT.prettyLines t
  where mkArg i = "__arg" ++ show i

provOfType [] _   = return ptemp
provOfType args _ = return $ pderived $ map pfvar args


instance Pretty PIEnv where
  prettyLines (PIEnv c p e a cl ep) =
    [T.pack $ "PCnt: " ++ show c] ++
    [T.pack "PEnv: "  ]  %$ (PT.indent 2 $ PT.prettyLines e)  ++
    [T.pack "PPEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines p)  ++
    [T.pack "PAEnv: " ]  %$ (PT.indent 2 $ PT.prettyLines a)  ++
    [T.pack "PLCEnv: "]  %$ (PT.indent 2 $ PT.prettyLines cl) ++
    [T.pack "EPMap: " ]  %$ (PT.indent 2 $ PT.prettyLines ep)

instance Pretty (IntMap (K3 Provenance)) where
  prettyLines pp = IntMap.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] pp

instance Pretty PEnv where
  prettyLines pe = Map.foldlWithKey (\acc k v -> acc ++ prettyFrame k v) [] pe
    where prettyFrame k v = concatMap prettyPair $ flip zip v $ replicate (length v) k

instance Pretty PAEnv where
  prettyLines pa = Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k,v)) [] pa

instance Pretty PMEnv where
  prettyLines pm = Map.foldlWithKey (\acc k v -> acc ++ prettyPair (k, fst v)) [] pm

instance Pretty PLCEnv where
  prettyLines plc = IntMap.foldlWithKey (\acc k v -> acc ++ [T.pack $ show k ++ " => " ++ show v]) [] plc

prettyPair :: (Show a, Pretty b) => (a,b) -> [Text]
prettyPair (a,b) = [T.pack $ show a ++ " => "] %+ PT.prettyLines b
