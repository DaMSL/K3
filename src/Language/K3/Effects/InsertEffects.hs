-- | Insertion of effects into the expression/declaration tree
--
--   We rely on the fact that we run after typechecking and after
--   the InsertMembers analysis, so that collection annotation effects
--   are present in the expression tree
--
--  TODO: handle cyclic scope properly
--        cyclic scope can create loops
--
--  TODO: switch symbol gensym to use monad

module Language.K3.Effects.Insert (
  runAnalysis
)
where

import Control.Arrow ( (&&&), second )

data Env = Env {
                globalEnv :: Map Identfier (K3 Symbol),
                count :: Int,
                bindEnv :: Map Identifier [K3 Symbol]
               }

startEnv :: Env
startEnv = Env {
             globalEnv=Map.empty,
             count = 1,
             bindEnv=Map.empty
           }

insertGlobal :: Identifier -> Effect -> Env -> Env
insertGlobal id e env = env {globalEnv=Map.insert id e globalEnv env}

getId :: Env -> (Int, Env)
getId env = (count env, env {count = 1 + count env})

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
lookupBind :: Identifier -> Env -> Maybe (K3 Symbol)
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

lookupBindM :: Identifier -> MEnv (Maybe (K3 Symbol))
lookupBindM id = do
  env <- get
  lookupBind id env

getIdM :: MEnv Int
getIdM = do
  e <- get
  let (i, e') = getId e
  put e'
  return i

singleton :: a -> [a]
singleton x = [x]

-- Add an id to an effect
addIdE :: K3 Effect -> MEnv (K3 Effect)
addIdE eff = do
  i <- getIdM
  eff @+ FId i

addIdS :: K3 Symbol -> MEnv (K3 Symbol)
addIdS eff = do
  i <- getIdM
  eff @+ SId i

getUID :: K3 Expression -> UID
getUID n = maybe (error "No UID found") extract $ n @~ isEUID
  where extract (EUID uid) = uid
        extract _          = error "unexpected"

-- Generate a symbol
symbolM :: Identifier -> Provenance -> [K3 Symbol] -> MEnv (K3 Symbol)
symbolM name prov ch = do
  i <- getIdM
  let s = symbol name prov @+ SId i
  return $ replaceCh s ch

genSym :: Provenance -> [K3 Symbol] -> MEnv (K3 Symbol)
genSym p ch = do
   i <- getIdM
   let s = symbol ("sym_"++show i) p @+ SId i
   return $ replaceCh s ch

genSymTemp :: K3 Expression -> [K3 Symbol] -> MEnv (K3 Symbol)
genSymTemp n ch = genSym FTemporary

getEEffect :: K3 Expression -> Maybe (K3 Effect)
getEEffect n = case n @~ isEEffect of
                 Just (EEffect e) -> Just e
                 Nothing          -> Nothing
                 _                -> error "unexpected"

getESymbol :: K3 Expression -> Maybe (K3 Symbol)
getESymbol n = case n @~ isESymbol of
                 Just (ESymbol e) -> Just e
                 Nothing          -> Nothing
                 _                -> error "unexpected"

forceGetSymbol :: K3 Expression -> K3 Symbol
forceGetSymbol n = maybe err id $ getESymbol n
  where err = error "Expected a necessary symbol but didn't find any"

-- If we don't have a symbol, we automatically gensym one
getOrGenSymbol :: K3 Expression -> MEnv (K3 Symbol)
getOrGenSymbol n = case getESymbol n of
                     Nothing -> genSymTemp n [] >>= return
                     Just i  -> return i

runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis prog = flip evalState startEnv $
  mapProgram handleDecl mId handleExprs prog
  where
    mId x = return x
    listOfMaybe m = maybe [] singleton m

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
    extractBindData (BRecord ijs)    = map (\(i, j) -> (j, FRecord i)) ijs

    pre _ n@(tag -> ELambda i) =
      -- Add to the environment
      addEnvM i $ symbol i FVar

    -- We take the first child's symbol and bind to it
    sideways ch1 n@(tag -> ELetIn i) = do
      chSym <- getOrGenSymbol ch1
      s     <- symbolM i PLet chSym
      insertBindM i s

    -- We take the first child's symbol and bind to it
    sideways _ n@(tag -> EBindAs b) = do
      chSym <- getOrGenSymbol ch1
      let iProvs = extractBindData b
          syms   = mapM (second $ symbolM i prov chSym) iProvs
      mapM_ (\(i, sym) -> insertBindM i sym) syms

    -- We take the first child's symbol and bind to it
    sidewyas ch1 n@(tag -> ECaseOf i) = do
      chSym <- getOrGenSymbol ch1
      s     <- symbolM i PLet chSym
      [insertBindM i s, deleteBindM i]

    -- A variable access looks up in the environemnt and generates a read
    -- It also creates a symbol
    handleExpr _ n@(tag -> EVariable i) = do
      (mEff, sym) <- lookupBindM i
      r  <- addIdE $ read sym
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
      w        <- addIdE $ write sym
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
      return $ addEffSymCh nEff nSym ch

    -- ELambda wraps up the child effect and sticks it in a symbol.
    -- A new scope will be created at application
    handleExpr ch@[e] n@(tag -> ELambda i) = do
      deleteBindM i
      -- Create a gensym for the lambda, containing the effects, and leading to the symbols
      let eSym = maybe [] singleton $ getESymbol e
          lSym = gensym (Flambda i $ getEEffect e) eSym
      return $ replaceCh (n @+ ESymbol lSym) ch

    -- On application, Apply creates a scope and substitutes into it
    -- We only create the effect of apply here
    handleExpr ch@[l,a] n@(tag -> EOperate(OApp)) = do
      seqE    <- createSeq (getEEffect l) $ getEEffect a
      -- Create the effect of application
      appE    <- addIdE $ apply (forceGetSymbol l) $ getOrGenSymbol a
      fullEff <- createSeq seqE $ Just appE
      fullSym <- combineSym (Just $ forceGetSymbol l) $ getOrGenSymbol a
      return $ addEffSymCh fullEff fullSym ch

    -- Bind
    handleExpr ch@[bind,e] n@(tag -> EBindAs b) = do
      let iProvs = extractBindData b
          ids    = map fst iProvs
      -- Get the scope info
      bindSyms <- mapM lookupBindM ids
      -- Remove binds from env
      mapM_ deleteBindM ids
      -- TODO: peel off until you get to local bind
      let fullSym = getESymbol e
          eEff = maybe [] singleton $ getEEffect e
      bScope  <- addId $ scope bindSyms eEff
      fullEff <- createSeq (getEEffects bind) $ Just bScope
      return $ addEffSymCh fullEff fullSym ch

    -- CaseOf
    handleExpr ch@[e,some,none] n@(tag -> ECaseOf i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      -- Wrap some in a scope
      someEff = listOfMaybe $ getEEffect some
      scopeEff <- addId $ scope [bindSym] someEff
      -- Conservative approximation
      setEff   <- createSet (getEEffect none) (Just scopeEff)
      -- TODO: peel off until you get to local binds
      fullSym  <- combineSym (getESymbol some) (getESymbol none)
      fullEff  <- createSeq (getEEffect e) setEff
      return $ addEffSymCh fullEff fullSym ch

    -- LetIn
    handleExpr ch@[l,e] n@(tag -> ELetIn i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      eEff = listOfMaybe $ getEEffect e
      scopeEff <- addId $ scope [bindSym] eEff
      fullEff  <- createSeq (getEEffect l) (Just scopeEff)
      let fullSym = bindSym -- TODO: peel off top layer
      return $ addEffSymCh fullEff fullSym ch

    ------ Utilities ------
    -- Combine 2 effects if they're present. Otherwise keep whatever we have
    combine constF (Just e1) (Just e2) = do
      i <- getEffectIdM
      return $ Just $ (constF [e1, e2]) :@: FId i
    combine (Just e) _ = return $ Just e
    combine _ (Just e) = return $ Just e
    combine _ _        = return Nothing

    createSet = combine set

    createSeq :: Maybe (K3 Effect) -> Maybe (K3 Effect) -> MEnv (Maybe (K3 Effect))
    createSeq = combine seqF
      where seqF [x,y] = seq x y
            seqF _     = error "Bad input to seqF"

    -- Combine 2 symbols into 1 set symbol (if needed)
    -- otherwise just use one of the symbols/don't generate anything
    combineSym :: Maybe (K3 Symbol) -> Maybe (K3 Symbol) -> MEnv (Maybe (K3 Symbol))
    combineSym (Just s) (Just s') = genSym PSet [s, s'] >>= return
    combineSym (Just s) _         = return $ Just s
    combineSym _ (Just s)         = return $ Just s
    combineSym _ _                = return Nothing

    -- Extract the symbols of a read effect
    extractReadSym :: Effect -> [K3 Symbol]
    extractReadSym eff = nub $ loop eff
      where loop eff = case tag &&& children eff of
        (FRead x, _)   -> [x]
        (FSeq, [_,ch]) -> extractRead ch
        (FSet, ch)     -> concatMap extractRead ch
        _              -> [FTemporary]
        -- TODO: do we need any other cases?

    -- Common procedure for adding back the symbols, effects and children
    addEffSymCh :: Maybe(K3 Effect) -> Maybe(K3 Symbol) -> [K3 Expression] -> K3 Expression
    addEffSymCh eff sym ch =
      let n'  = maybe n  (n  @+ EEffect) eff
          n'' = maybe n' (n' @+ ESymbol) sym
      in replaceCh n'' ch
