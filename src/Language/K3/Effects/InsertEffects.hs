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
lookupBind :: Identifier -> Env -> (Maybe (K3 Effect), K3 Symbol)
lookupBind i env =
  case Map.lookup i $ bindEnv env of
    Nothing -> maybe (err i) (Just e, globalSym i) $ Map.lookup id $ globalEnv env
    Just e  -> e
  where err i = error "failed to find " ++ id ++ " in environment"

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
addId :: K3 Effect -> MEnv (K3 Effect)
addId eff = do
  i <- getEffectIdM
  eff @+ FId i

getUID :: K3 Expression -> UID
getUID n = maybe (error "No UID found") extract $ n @~ isEUID
  where extract (EUID uid) = uid
        extract _          = error "unexpected"

-- Generate a symbol
genSym :: K3 Expression -> [K3 Symbol] -> K3 Symbol
genSym n ch = replaceCh (symbol ("_"++show uid) FTemporary) ch

genTempSym :: K3 Expression -> K3 Symbol
genTempSym = genSym FTemporary

getEEffect :: K3 Expression -> K3 Effect
getEEffect n = case n @~ isEEffect of
                 Just (EEffect e) -> Just e
                 Nothing          -> Nothing
                 _                -> error "unexpected"

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

    extractBindData (BIndirection i) = [(i, FIndirection)]
    extractBindData (BTuple ids)     = zip ids [FTuple j | j <- [0..length ids - 1]]
    extractBindData (BRecord ijs)    = map (\(i, i') -> (i', FRecord i)) ijs

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
    -- It also creates a symbol
    handleExpr _ n@(tag -> EVariable i) = do
      (mEff, sym) <- lookupBindM i
      r  <- addId $ read sym
      let eff = case mEff of
        Just e  -> do
          e' <- createSeq e r
          return $ EEffect e'
        Nothing -> EEffect r
      return $ n @+ eff @+ sym

    noEffectErr = error "Expected an effect but got none"

    -- An assignment generates a write, and no symbol
    handleExpr ch@[e] n@(tag -> EAssign i) = do
      (_, sym) <- lookupBindM i
      w        <- addId $ write sym
      -- Add the write to any existing child effects
      nEff     <- createSeq (getEEffect e) $ Just w
      let n' = maybe noEffectErr (n @+ EEffect) nEff
      return $ replaceCh n' ch

    -- For ifThenElse be pessimistic: include effects and symbols of both paths
    handleExpr ch@[p,t,f] n@(tag -> EIfThenElse) = do
      tfEff <- createSet (t @~ isEEffect) (f @~ isEEffect)
      -- Combine with predicate effects
      nEff  <- createSeq (p @~ isEEffect) chE
      -- Combine path symbols into a new symbol
      nSym  <- combineSym (t @~ isESymbol) (f @~ isESymbol)
      let n'  = maybe n  (n  @+ EEffect) nEff
          n'' = maybe n' (n' @+ ESymbol) nSym
      return $ replaceCh n'' ch

    -- ELambda wraps up the child effect. A new scope will be created at application
    handleExpr ch@[e] n@(tag -> ELambda i) = do
      deleteBindM i
      let eEff = maybe [] singleton $ getEEffect e
      lE <- addId $ lambda i eEff 
      return $ replaceCh (n @+ EEffect lE) ch

    -- On application, Apply creates a scope and substitutes into it
    -- We only create the effect of apply
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
      eff  <- createSet (getEEffect e) (s @~ isEEffect)
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

    createSeq :: Maybe (K3 Effect) -> Maybe (K3 Effect) -> Maybe (K3 Effect)
    createSeq = combine seqF
      where seqF [x,y] = seq x y
            seqF _     = error "Bad input to seqF"

    -- Combine 2 symbols into 1 temporary symbol (if needed)
    -- otherwise just use one of the symbols/don't generate anything
    combineSym n (Just e) (Just e') = Just $ genSym n [e, e']
    combineSym _ (Just e) _         = Just e
    combineSym _ _ (Just e)         = Just e
    combineSym _ _ _                = Nothing

    -- Extract the symbols of a read effect
    extractReadSym :: Effect -> [K3 Symbol]
    extractReadSym eff = nub $ loop eff
      where loop eff = case tag &&& children eff of
        (FRead x, _)   -> [x]
        (FSeq, [_,ch]) -> extractRead ch
        (FSet, ch)     -> concatMap extractRead ch
        _              -> [FTemporary]
        -- TODO: do we need any other cases?

