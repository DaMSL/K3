-- | Insertion of effects into the expression/declaration tree
--
--   We rely on the fact that we run after typechecking and after
--   the InsertMembers analysis, so that collection annotation effects
--   are present in the expression tree
--
--  TODO: handle recursive scope properly

module Language.K3.Effects.Insert where (
  runAnalysis
)

import Control.Arrow ( (&&&) )

data Env = Env {
                globalEnv :: Map Identfier Effect,
                count :: Int
               }

startEnv :: Env
startEnv = Env {
             globalEnv=Map.empty,
             count = 1,
             bindEnv=Map.empty
           }

insertGlobal :: Identifier -> Effect -> Env -> Env
insertGlobal id e env = env {globalEnv=Map.insert id e globalEnv env}

getEffId :: Env -> (Int, Env)
getEffId env = (count env, env {count = 1 + count env})

insertBind :: Identifier -> K3 Symbol -> Env -> Env
insertBind id s env =
  env {bindEnv=Map.insertWith (++) id [s] $ bindEnv env}

deleteBind :: Identifier -> Env -> Env
deleteBind id env =
  let m' = case Map.lookup id $ bindEnv env of
    []   -> Map.delete id env
    [_]  -> Map.delete id env
    _:xs -> Map.insert id xs env
  in
  env {bindEnv=m'}

lookupBind :: Identifier -> Env -> Env
lookupBind id env = head $ Map.lookup id $ bindEnv env

type MEnv = State Env

insertGlobalM :: Identifier -> Effect -> MEnv ()
insertGlobalM id eff = modify $ insertGlobal id eff

insertBindM :: Identifier -> K3 Symbol -> MEnv ()
insertBindM id s = modify $ insertBind id s

deleteBindM :: Identifier -> MEnv ()
deleteBindM id = modify $ deleteBind id

lookupBindM :: Identifier -> MEnv (K3 Symbol)
lookupBindM id = do
  env <- get
  lookupBind id env

getEffIdM :: MEnv Int
getEffIdM = do
  e <- get
  let (i, e') = getEffId e
  put e'
  return i

effectM :: Effect -> MEnv (Effect)
effectM eff = do
  i <- getEffIdM
  eff :@: FId i

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

    pre _ n@(tag -> ELambda i) =
      -- Add to the environment
      addEnvM i $ symbol i FLambdaVar

    sideways ch1 n@(tag -> ELetIn i) = do
      -- Look for read effect symbols in ch1
      let effs = maybe err extractReadSym $ ch1 @~ isEEffect
          err  = error "Missing effect in 1st child of LetIn"
      -- Add to the environment
      addEnvM i $ symbol i $ Node (FLet :@: []) effs

    sideways _ n@(tag -> EBindAs b) =
      -- Look for read effect symbols in ch1
      let iProvs = case b of
            BIndirection i -> [(i, FIndirection)]
            BTuple ids     -> zip ids [FTuple j | j <- [0..length ids - 1]]
            BRecord ijs    -> map (\(i, i') -> (i', FRecord i)) ijs
          effs = maybe err extractReadSym $ ch1 @~ isEEffect
          err  = error "Missing effect in 1st child of BindAs"
      mapM (\(i, prov) -> insertBindM i $ symbol i $ Node (prov :@: [])) effs

    sidewyas ch1 n@(tag -> ECaseIn i) = do
      -- Look for read effect symbols in ch1
      let syms = maybe err extractReadSym $ ch1 @~ isEEffect
          err = error "Missing effect in 1st child of CaseOf"
      [insertBindM i $ symbol i $ Node (FCase :@: []) syms,
       deleteBindM i]

    handleExpr n@(tag &&& children -> (EIfThenElse, [p, t, e])) = do
      -- Be pessimistic: include effects of both paths
      let (pE, tE, eE) = (p @~ isEEffect, tE @~ isEEffect, eE @~ isEEffect)
          -- Get the childrens' effects
          chE = case (tE, eE) of
                  Just tE', Just eE' -> effectM $ set [tE', eE']
                  Just tE', _        -> tE'
                  _       , Just eE' -> eE'
          -- If's effects come first
          case pE of
            Nothing -> return $ n @+ chE
            Just e  -> return $ n @+ effectM $ seq e chE

    -- Extract the symbols of a read effect
    extractReadSym :: Effect -> [K3 Symbol]
    extractReadSym eff = nub $ loop eff
      where loop eff = case tag &&& children eff of
        (FRead x, _)   -> [x]
        (FSeq, [_,ch]) -> extractRead ch
        (FSet, ch)     -> concatMap extractRead ch
        _              -> [FTemporary]
        -- TODO: do we need any other cases?




















