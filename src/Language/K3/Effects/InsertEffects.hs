-- | Insertion of effects into the expression/declaration tree
--
--   We rely on the fact that we run after typechecking and after
--   the InsertMembers analysis, so that collection annotation effects
--   are present in the expression tree
--
--  TODO: handle cyclic scope properly
--        cyclic scope can create loops

module Language.K3.Effects.Insert (
  runAnalysis
)
where

import Control.Arrow ( (&&&) )

data Env = Env {
                globalEnv :: Map Identfier (K3 Effect),
                count :: Int,
                bindEnv :: Map Identifier [(K3 Effect, K3 Symbol)]
               }

startEnv :: Env
startEnv = Env {
             globalEnv=Map.empty,
             count = 1,
             bindEnv=Map.empty
           }

insertGlobal :: Identifier -> Effect -> Env -> Env
insertGlobal id e env = env {globalEnv=Map.insert id e globalEnv env}

getEffectId :: Env -> (Int, Env)
getEffectId env = (count env, env {count = 1 + count env})

insertBind :: Identifier -> Maybe (K3 Effect) -> K3 Symbol -> Env -> Env
insertBind id e s env =
  env {bindEnv=Map.insertWith (++) id [(e, s)] $ bindEnv env}

deleteBind :: Identifier -> Env -> Env
deleteBind id env =
  let m' = case Map.lookup id $ bindEnv env of
    []   -> Map.delete id env
    [_]  -> Map.delete id env
    _:xs -> Map.insert id xs env
  in
  env {bindEnv=m'}

globalSym :: Identifier -> K3 Symbol
globalSym id = symbol id FGlobal

-- Lookup either in the bind environment or the global environment
lookupBind :: Identifier -> Env -> Maybe (Maybe (K3 Effect), K3 Symbol)
lookupBind id env =
  case Map.lookup id $ bindEnv env of
    Nothing -> maybe Nothing (\e -> (Just e, globalSym i)) $ Map.lookup id $ globalEnv env
    e       -> e

type MEnv = State Env

insertGlobalM :: Identifier -> Effect -> MEnv ()
insertGlobalM id eff = modify $ insertGlobal id eff

insertBindM :: Identifier -> Maybe (K3 Effect) -> K3 Symbol -> MEnv ()
insertBindM id e s = modify $ insertBind id e s

deleteBindM :: Identifier -> MEnv ()
deleteBindM id = modify $ deleteBind id

lookupBindM :: Identifier -> MEnv (Maybe (Maybe(K3 Effect), K3 Symbol))
lookupBindM id = do
  env <- get
  lookupBind id env

getEffectIdM :: MEnv Int
getEffectIdM = do
  e <- get
  let (i, e') = getEffectId e
  put e'
  return i

singleton :: a -> [a]
singleton x = [x]

-- Add an id to an effect
addId :: K3 Effect -> K3 Effect
addId eff = do
  i <- getEffectIdM
  eff @+ FId i

runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis prog = flip evalState startEnv $
  mapProgram handleDecl m_id handleExprs prog
  where
    m_id x = return x

    -- TODO: handle recursive scope
    -- external functions
    handleDecl n@(tag -> DGlobal id _ Nothing) =
      case n @~ isDEffect of
        Nothing -> return n
        Just (DEffect e) -> insertGlobal id e

    -- global functions
    handleDecl n@(tag -> DGlobal id _ (Just e)) =
      case e @~ isEEffect of
        Nothing -> return n
        Just (EEffect e) -> insertGlobal id e

    -- triggers
    handleDecl n@(tag -> DTrigger id _ e) =
      case e @~ isEEffect of
        Nothing -> return n
        Just (EEffect e) -> insertGlobal id e

    handleExprs n = mapIn1RebuildTree pre sideways handleExpr n

    extractBindData BIndirection i = [(i, FIndirection)]
    extractBindData BTuple ids     = zip ids [FTuple j | j <- [0..length ids - 1]]
    extractBindData BRecord ijs    = map (\(i, i') -> (i', FRecord i)) ijs

    pre _ n@(tag -> ELambda i) =
      -- Add to the environment
      addEnvM i $ symbol i FVar

    sideways ch1 n@(tag -> ELetIn i) = do
      -- Look for read effect symbols in ch1
      let effs = maybe err extractReadSym $ ch1 @~ isEEffect
          err  = error "Missing effect in 1st child of LetIn"
      -- Add to the environment
      addEnvM i $ symbol i $ Node (FLet :@: []) effs

    sideways _ n@(tag -> EBindAs b) = do
      -- Look for read effect symbols in ch1
      let iProvs = extractBindData b
          effs = maybe err extractReadSym $ ch1 @~ isEEffect
          err  = error "Missing effect in 1st child of BindAs"
      mapM_ (\(i, prov) -> insertBindM i $ symbol i $ Node (prov :@: [])) effs

    sidewyas ch1 n@(tag -> ECaseIn i) = do
      -- Case always binds to a temporary
      let syms = maybe err extractReadSym $ ch1 @~ isEEffect
          err = error "Missing effect in 1st child of CaseOf"
      [insertBindM i $ symbol i $ Node (FTemporary :@: []) [],
       deleteBindM i]

    -- A variable access looks up in the environemnt and generates a read
    handleExpr _ n@(tag -> EVariable i) = do
      mEffSym <- lookupBindM i
      let n' = case mEffSym of
        Just (Just e, sym)  -> do
          r  <- addId $ read sym
          e' <- createSeq e r
          n @+ e'
        Just (Nothing, sym) -> do
          r <- addId $ read sym
          n @+ r
        Nothing -> error "variable "++show i++" not found in environment"
      return n'

    -- An assinment generates a write
    handleExpr ch@[e] n@(tag -> EAssign i) = do
      sym <- lookupBindM i
      num <- getEffectId
      -- Check if we have existing child effects
      case ch @~ isEEffect of
        Nothing     -> replaceCh (n @+ write num sym) ch
        Just chEff  -> do
          num' <- getEffectId
          replaceCh (n @+ seq num' chEff $ write num sym) ch

    -- For ifThenElse be pessimistic: include effects of both paths
    handleExpr [p,t,e] n@(tag -> EIfThenElse) = do
      let (pE, tE, eE) = (p @~ isEEffect, tE @~ isEEffect, eE @~ isEEffect)
          -- Get the childrens' effects
          chE = case (tE, eE) of
                  Just tE', Just eE' -> do
                    i <- getEffectIdM
                    set i [tE', eE']
                  Just tE', _        -> tE'
                  _       , Just eE' -> eE'
          -- If's effects come first
          case pE of
            Nothing -> return $ n @+ chE
            Just e  -> getEffectIdM >>= \i -> return $ n @+ effectM $ seq i e chE

    -- ELambda simply wraps up the child effect
    handleExpr ch@[e] n@(tag -> ELambda i) = do
      deleteBindM i
      num <- getEffectIdM
      let chEff = maybe [] (\eff -> [eff]) $ e @~ isEEffect
      return $ replaceCh (n @+ lambda num i chEff) ch

    -- Apply creates a scope and substitutes
    -- We only directly apply any lambda that is unnamed
    -- Any other lambda is left for the optimizer to apply
    handleExpr [l,a] n@(tag -> EOperate(OApp)) = do
      case a @~ isEEffect of
        Nothing -> substitute Nothing l
        Just e  -> 
      

    -- Bind
    handleExpr ch@[b,e] n@(tag -> EBindAs b) = do
      let bindData = extractBindData b
      -- get the symbols for the bind env
      bindSyms <- mapM (lookupBindM . fst) bindData
      -- Remove binds from env
      mapM_ deleteBindM $ map fst $ bindData
      -- Generate the new effects
      let eEff = maybe [] singleton $ e @~ isEEffect
      i <- getEffectIdM
      let bScope = scope i bindSyms eEff
          nEff = case b @~ isEEffect of
                   Nothing   -> bScope
                   Just bEff -> getEffectIdM >>= \j -> seq j bEff bScope
      return $ replaceCh (n @+ nEff) ch

    -- CaseOf
    handleExpr ch@[e,s,none] n@(tag -> ECaseOf i) = do
      eff  <- createSet (e @~ isEEffect) (s @~ isEEffect)
      eff' <- createSeq e eff
      let n' = maybe n (n @+) eff'
      return $ replaceCh n' ch

    -- Combine 2 effects if they're present. Otherwise keep whatever we have
    combine constF (Just e1) (Just e2) = do
      i <- getEffectIdM
      return $ Just $ (constF [e1, e2]) :@: FId i
    combine (Just e1) _ = return $ Just e1
    combine _ (Just e2) = return $ Just e2
    combine _ _         = return Nothing

    createSet = combine set
    createSeq = combine seq

    -- Extract the symbols of a read effect
    extractReadSym :: Effect -> [K3 Symbol]
    extractReadSym eff = nub $ loop eff
      where loop eff = case tag &&& children eff of
        (FRead x, _)   -> [x]
        (FSeq, [_,ch]) -> extractRead ch
        (FSet, ch)     -> concatMap extractRead ch
        _              -> [FTemporary]
        -- TODO: do we need any other cases?

