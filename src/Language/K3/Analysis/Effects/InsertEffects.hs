{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TupleSections #-}
-- | Insertion of effects into the expression/declaration tree
--
--   We rely on the fact that we run after typechecking and after
--   the InsertMembers analysis, so that collection annotation effects
--   are present in the expression tree
--
--  TODO: handle cyclic scope properly
--        cyclic scope can create loops
--
--  TODO: handle collection attributes (pass lambda var of self immediate)
--  TODO: handle recursive scope
--  TODO: lambda needs to filter effects for closure/formal args

module Language.K3.Analysis.Effects.InsertEffects (
  Env,
  preprocessBuiltins,
  runAnalysis,
  runAnalysisEnv,
  buildEnv,
  applyLambda,
  applyLambdaEnv,
  runConsolidatedAnalysis
)
where

import Prelude hiding (read, seq)
import Control.Arrow ( (&&&) )
import Control.Monad.State.Lazy
import Data.Maybe
import Data.Map(Map)
import Data.List(nub, delete)
import qualified Data.Map as Map
import Data.Foldable hiding (mapM_, any, all, concatMap, concat)
import Debug.Trace(trace)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Declaration
import Language.K3.Core.Utils
import Language.K3.Core.Type

import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.Constructors

import qualified Language.K3.Analysis.InsertMembers as IM

type GlobalEnv = Map Identifier (K3 Symbol)
type LocalEnv  = Map Identifier [K3 Symbol]

data Env = Env {
                globalEnv :: GlobalEnv,
                count :: Int,
                bindEnv :: LocalEnv
               }

startEnv :: Env
startEnv = Env {
             globalEnv=Map.empty,
             count = 1,
             bindEnv=Map.empty
           }

insertGlobal :: Identifier -> K3 Symbol -> Env -> Env
insertGlobal i s env = env {globalEnv=Map.insert i s $ globalEnv env}

getId :: Env -> (Int, Env)
getId env = (count env, env {count = 1 + count env})

insertBind :: Identifier -> K3 Symbol -> Env -> Env
insertBind i s env =
  env {bindEnv=Map.insertWith (++) i [s] $ bindEnv env}

deleteBind :: Identifier -> Env -> Env
deleteBind i env =
  let m  = bindEnv env
      m' = case Map.lookup i m of
             Just []     -> Map.delete i m
             Just [_]    -> Map.delete i m
             Just (_:xs) -> Map.insert i xs m
             Nothing     -> m
  in
  env {bindEnv=m'}

emptyClosure :: ClosureInfo
emptyClosure = ([],[],[])

-- Lookup either in the bind environment or the global environment
lookupBindInner :: Identifier -> Env -> Maybe (K3 Symbol)
lookupBindInner i env =
  case Map.lookup i $ bindEnv env of
    Nothing -> Map.lookup i $ globalEnv env
    s       -> liftM head s

lookupBindInnerM :: Identifier -> MEnv (Maybe (K3 Symbol))
lookupBindInnerM i = do
  env <- get
  return $ lookupBindInner i env


lookupBind :: Identifier -> Env -> K3 Symbol
lookupBind i env = fromMaybe err $ lookupBindInner i env
  where err = error $ "failed to find " ++ i ++ " in environment"

type MEnv = State Env

insertGlobalM :: Identifier -> K3 Symbol -> MEnv ()
insertGlobalM i s = modify $ insertGlobal i s

insertBindM :: Identifier -> K3 Symbol -> MEnv ()
insertBindM i s = modify $ insertBind i s

deleteBindM :: Identifier -> MEnv ()
deleteBindM i = modify $ deleteBind i

lookupBindM :: Identifier -> MEnv (K3 Symbol)
lookupBindM i = do
  env <- get
  return $ lookupBind i env

getIdM :: MEnv Int
getIdM = do
  e <- get
  let (i, e') = getId e
  put e'
  return i

singleton :: a -> [a]
singleton x = [x]

-- Add an id to an effect
addFID :: K3 Effect -> MEnv (K3 Effect)
addFID eff = do
  i <- getIdM
  return $ eff @+ FID i

addSID :: K3 Symbol -> MEnv (K3 Symbol)
addSID sym = do
  i <- getIdM
  return $ sym @+ SID i

getSID :: K3 Symbol -> Int
getSID sym = maybe (error "no SID found") extract $ sym @~ isSID
  where extract (SID i) = i

-- Generate a symbol
symbolM :: Identifier -> Provenance -> [K3 Symbol] -> MEnv (K3 Symbol)
symbolM name prov ch = do
  i <- getIdM
  let s = symbol name prov @+ SID i
  return $ replaceCh s ch

genSym :: Provenance -> [K3 Symbol] -> MEnv (K3 Symbol)
genSym p ch = do
   i <- getIdM
   let s = symbol ("sym_"++show i) p @+ SID i
   return $ replaceCh s ch

genSymTemp :: TempType -> [K3 Symbol] -> MEnv (K3 Symbol)
genSymTemp tempType = genSym $ PTemporary tempType

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
forceGetSymbol n = fromMaybe err $ getESymbol n
  where err = error "Expected a necessary symbol but didn't find any"

-- If we don't have a symbol, we automatically gensym one
getOrGenSymbol :: K3 Expression -> MEnv (K3 Symbol)
getOrGenSymbol n = case getESymbol n of
                     Nothing -> genSymTemp TTemp []
                     Just i  -> return i

-- Create a closure of symbols read, written, or applied that are relevant to the current env
createClosure :: K3 Effect -> MEnv ClosureInfo
createClosure n = liftM nubTuple $ foldTree addClosure ([],[],[]) n
  where
    nubTuple (a,b,c) = (nub a, nub b, nub c)

    addClosure :: ClosureInfo -> K3 Effect -> MEnv ClosureInfo
    addClosure (a,b,c) (tag -> FRead s)     = do
      s' <- getClosureSyms s
      return (s' ++ a,b,c)
    addClosure (a,b,c) (tag -> FWrite s)    = do
      s' <- getClosureSyms s
      return (a, s' ++ b, c)
    addClosure (a,b,c) (tag -> FApply s s') = do
      s'' <- getClosureSyms s'
      handleApply (a, b, s'' ++ c) s
    addClosure acc     _                    = return acc

    getClosureSyms :: K3 Symbol -> MEnv [K3 Symbol]
    getClosureSyms (tag -> Symbol _ (PTemporary TTemp))    = return []
    getClosureSyms (tag -> Symbol _ (PTemporary TUnbound)) = return []
    getClosureSyms s@(tnc -> (Symbol i _, ch)) = do
      x <- lookupBindInnerM i
      case x of
        Just (tag -> Symbol _ (PTemporary TTemp))    -> return []
        Just (tag -> Symbol _ (PTemporary TUnbound)) -> return []
        Just (tag -> Symbol _ PGlobal) -> return []
        Just s' | s `symEqual` s' -> return [s']
        _ ->
          -- if we haven't found a match, it might be deeper in the tree
          liftM concat $ mapM getClosureSyms ch



    handleApply :: ClosureInfo -> K3 Symbol -> MEnv ClosureInfo
    handleApply acc (tag -> Symbol _ (PLambda _ eff))  = foldTree addClosure acc eff
    handleApply acc n@(tnc -> (Symbol _ PSet, ch)) = foldrM (flip handleApply) acc ch
    handleApply acc _ = return acc


addAllGlobals :: K3 Declaration -> MEnv (K3 Declaration)
addAllGlobals n = mapProgram preHandleDecl mId mId Nothing n
  where
    -- add everything to global environment for cyclic/recursive scope
    -- we'll fix it up the second time through
    addGeni i = symbolM i (PTemporary TUnbound) [] >>= insertGlobalM i

    preHandleDecl n@(tag -> DGlobal i _ _)  = addGeni i >> return n
    preHandleDecl n@(tag -> DTrigger i _ _) = addGeni i >> return n
    preHandleDecl n = return n

mId :: Monad m => a -> m a
mId = return

symEqual :: K3 Symbol -> K3 Symbol -> Bool
symEqual (getSID -> s) (getSID -> s') = s == s'

strip f n =
  case n @~ f of
    Nothing -> n
    Just x  -> strip f (n @- x)


-- map over symbols and effects, starting at an effect
mapEff :: Monad m => (K3 Effect -> m (K3 Effect)) -> (K3 Symbol -> m (K3 Symbol)) -> K3 Effect -> m (K3 Effect)
mapEff effFn symFn eff = modifyTree (wrapEffFn effFn symFn) eff

-- map over symbols and effects, starting at a symbol
mapSym :: Monad m => (K3 Effect -> m (K3 Effect)) -> (K3 Symbol -> m (K3 Symbol)) -> K3 Symbol -> m (K3 Symbol)
mapSym effFn symFn sym = modifyTree (wrapSymFn effFn symFn) sym

wrapEffFn :: Monad m => (K3 Effect -> m (K3 Effect)) -> (K3 Symbol -> m (K3 Symbol)) -> K3 Effect -> m (K3 Effect)
wrapEffFn effFn symFn n =
  case tag n of
    FRead s -> do
      s' <- mapSym effFn symFn s
      effFn $ replaceTag n $ FRead s'
    FWrite s -> do
      s' <- mapSym effFn symFn s
      effFn $ replaceTag n $ FWrite s'
    FScope ss (xs,ys,zs) -> do
      ss' <- mapM (mapSym effFn symFn) ss
      xs' <- mapM (mapSym effFn symFn) xs
      ys' <- mapM (mapSym effFn symFn) ys
      zs' <- mapM (mapSym effFn symFn) zs
      effFn $ replaceTag n $ FScope ss' (xs', ys', zs')
    FApply sL sA -> do
      sL' <- mapSym effFn symFn sL
      sA' <- mapSym effFn symFn sA
      effFn $ replaceTag n $ FApply sL' sA'
    _ -> effFn n

wrapSymFn :: Monad m => (K3 Effect -> m (K3 Effect)) -> (K3 Symbol -> m (K3 Symbol)) -> K3 Symbol -> m (K3 Symbol)
wrapSymFn effFn symFn n =
  case tag n of
    Symbol x (PLambda y e) -> do
      e' <- mapEff effFn symFn e
      symFn $ replaceTag n $ Symbol x $ PLambda y e'
    _ -> symFn n

-------- Preprocessing phase --------
--
-- Fill in the effect symbols missing in any builtins
-- Number any existing symbols with SIDs and FIDs
-- This must be called before effects are lifted into the expression tree
preprocessBuiltins :: K3 Declaration -> (K3 Declaration, Env)
preprocessBuiltins prog = flip runState startEnv $ modifyTree addMissingDecl prog
  where
    addMissingDecl :: K3 Declaration -> MEnv(K3 Declaration)

    addMissingDecl n@(tag -> (DDataAnnotation i t attrs)) = do
      attrs' <- mapM handleAttrs attrs
      return $ replaceTag n $ DDataAnnotation i t attrs'

    -- A global without an effect symbol
    addMissingDecl n@(tag -> (DGlobal i t@(tag -> TFunction) Nothing))
      | isNothing (n @~ isDSymbol) = do
        s <- symOfFunction t
        return $ n @+ DSymbol s

    -- If we have a symbol, number it
    addMissingDecl n = case n @~ isDSymbol of
                         Nothing -> return n
                         Just ds@(DSymbol s) -> do
                           s' <- liftM DSymbol $ numberSyms s
                           return (n @- ds @+ s')

    -- Handle lifted/unlifted attributes without symbols
    handleAttrs :: AnnMemDecl -> MEnv AnnMemDecl
    handleAttrs (Lifted x y t@(tag -> TFunction) Nothing annos)
      | isNothing (find isDSymbol annos) = do
          s <- symOfFunction t >>= symOfAttr
          return $ Lifted x y t Nothing $ DSymbol s:annos

    -- If we have a sumbol, number it
    handleAttrs (Lifted x y z u as) = liftM (Lifted x y z u) $ handleAttrsInner as

    handleAttrs (Attribute x y t@(tag -> TFunction) Nothing annos)
      | isNothing(find isDSymbol annos) = do
          s <- symOfFunction t >>= symOfAttr
          return $ Attribute x y t Nothing $ DSymbol s:annos

    -- If we have a symbol, number it
    handleAttrs (Attribute x y z u as) = liftM (Attribute x y z u) $ handleAttrsInner as

    handleAttrs a = return a

    -- handle common attribute symbol/effect renumbering functionality
    -- Also, wrap existing lambda symbols in "self" lambdas
    handleAttrsInner as =
      case find isDSymbol as of
        Nothing -> return as
        Just ds@(DSymbol s) -> do
          s' <- liftM DSymbol (numberSyms s >>= wrapAttr)
          let as' = delete ds as
          return $ s':as'

    -- Number existing symbols/effects
    numberSyms :: K3 Symbol -> MEnv(K3 Symbol)
    numberSyms s = modifyTree addNumSym s
    numberEffs :: K3 Effect -> MEnv(K3 Effect)
    numberEffs e = modifyTree addNumEff e

    addNumSym :: K3 Symbol -> MEnv (K3 Symbol)
    addNumSym n@(tag -> Symbol ii (PLambda i eff)) = do
      eff' <- numberEffs eff
      addSID $ replaceTag n $ Symbol ii $ PLambda i eff'
    addNumSym s = addSID s

    addNumEff :: K3 Effect -> MEnv (K3 Effect)
    addNumEff n@(tag -> FRead sym) = do
      sym' <- numberSyms sym
      addFID $ replaceTag n $ FRead sym'
    addNumEff n@(tag -> FWrite sym) = do
      sym' <- numberSyms sym
      addFID $ replaceTag n $ FWrite sym'
    addNumEff n@(tag -> FScope syms (sx,sy,sz)) = do
      syms' <- mapM numberSyms syms
      sx' <- mapM numberSyms sx
      sy' <- mapM numberSyms sy
      sz' <- mapM numberSyms sz
      addFID $ replaceTag n $ FScope syms' (sx', sy', sz')
    addNumEff n@(tag -> FApply s1 s2) = do
      s1' <- numberSyms s1
      s2' <- numberSyms s2
      addFID $ replaceTag n $ FApply s1' s2'
    addNumEff n = addFID n

    -- Create a symbol for a function based on type
    -- If we're an attribute, we need to also write to self
    symOfFunction :: K3 Type -> MEnv (K3 Symbol)
    symOfFunction t = liftM head $ symOfFunction' t 1

    symOfAttr :: K3 Symbol -> MEnv (K3 Symbol)
    symOfAttr s = createSym [s] "self"

    symOfFunction' :: K3 Type -> Int -> MEnv [K3 Symbol]
    symOfFunction' t@(tnc -> (TFunction, [_, ret])) i = do
      s  <- symOfFunction' ret $ i + 1
      s' <- createSym s $ "__"++show i
      return [s']
    symOfFunction' t _ = return []

    -- Create a default conservative symbol for the function
    createSym subSym nm = do
      sym <- symbolM nm PVar []
      r   <- addFID $ read sym
      w   <- addFID $ write sym
      seq <- combineEffSeq [Just w, Just r]
      lp  <- addFID $ loop $ fromJust seq
      sc  <- addFID $ scope [sym] emptyClosure [lp]
      genSym (PLambda nm sc) subSym

    -- Wrap an attribute in a self scope
    -- Substitute it for self inside the symbol
    wrapAttr subSym = do
      selfSym <- symbolM "self" PVar []
      sc      <- addFID $ scope [selfSym] emptyClosure []
      subSym' <- mapSym mId (matchSelf selfSym) subSym
      genSym (PLambda "self" sc) [subSym']
      where
        matchSelf selfSym (tag -> Symbol "self" PVar) = return selfSym
        matchSelf _ n = return n

----- Actual effect insertion ------
-- Requires an environment built up by the preprocess phase

runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis = runAnalysisEnv startEnv

runConsolidatedAnalysis :: K3 Declaration -> K3 Declaration
runConsolidatedAnalysis d = let (p, env) = preprocessBuiltins d in runAnalysisEnv env (IM.runAnalysis p)

runAnalysisEnv :: Env -> K3 Declaration -> K3 Declaration
runAnalysisEnv env prog = flip evalState env $
  -- for cyclic scope, add temporaries for all globals
  addAllGlobals prog >>=
  -- actual modification of AST (no need to decorate declarations here)
  mapProgram handleDecl mId handleExprs Nothing >>=
  -- fix up any globals that couldn't be looked up due to cyclic scope
  mapProgram handleDecl mId fixUpExprs Nothing

  where
    -- Add all globals and decorate tree
    handleDecl :: K3 Declaration -> MEnv (K3 Declaration)
    handleDecl n =
      case tag n of
        DGlobal i _ Nothing  -> addSym i []
        DGlobal i _ (Just e) -> addE i e
        DTrigger i _ e       -> addE i e
        _                    -> return n
      where
        addE i e = case e @~ isESymbol of
                     Just (ESymbol s)  -> addSym i [s]
                     _                 -> addSym i []

        addSym i ss = do
          sym <- symbolM i PGlobal ss
          insertGlobalM i sym
          return $ (strip isDSymbol n) @+ DSymbol sym

    handleExprs :: K3 Expression -> MEnv (K3 Expression)
    handleExprs n = mapIn1RebuildTree pre sideways handleExpr n

    extractBindData (BIndirection i) = [(i, PIndirection)]
    extractBindData (BTuple ids)     = zip ids [PTuple j | j <- [0..fromIntegral $ length ids - 1]]
    extractBindData (BRecord ijs)    = map (\(i, j) -> (j, PRecord i)) ijs

    doNothing = return ()

    doNothings n = return $ replicate n doNothing

    pre :: K3 Expression -> K3 Expression -> MEnv ()
    pre _ (tag -> ELambda i) = do
      -- Add to the environment
      sym <- symbolM i PVar []
      insertBindM i sym

    pre _ _ = doNothing

    sideways :: K3 Expression -> K3 Expression -> MEnv [MEnv ()]

    -- We take the first child's symbol and bind to it
    sideways ch1 (tag -> ELetIn i) = do
      chSym <- getOrGenSymbol ch1
      s     <- symbolM i PLet [chSym]
      return [insertBindM i s]

    -- We take the first child's symbol and bind to it
    sideways ch1 (tag -> EBindAs b) = do
      chSym <- getOrGenSymbol ch1
      let iProvs = extractBindData b
      syms <- mapM (\(i, prov) -> liftM (i,) $ symbolM i prov [chSym]) iProvs
      return [mapM_ (uncurry insertBindM) syms]

    -- We take the first child's symbol and bind to it
    sideways ch1 (tag -> ECaseOf i) = do
      chSym <- getOrGenSymbol ch1
      s     <- symbolM i PLet [chSym]
      return [insertBindM i s, deleteBindM i, insertBindM i s]

    sideways _ (children -> ch) = doNothings (length ch - 1)

    -- A variable access looks up in the environemnt and generates a read
    -- It also creates a symbol
    handleExpr :: [K3 Expression] -> K3 Expression -> MEnv (K3 Expression)

    handleExpr _ n@(tag -> EVariable i) = do
      sym <- lookupBindM i
      eff <- addFID $ read sym
      return $ addEffSymCh (Just eff) (Just sym) [] n

    -- An assignment generates a write, and no symbol
    handleExpr ch@[e] n@(tag -> EAssign i) = do
      sym    <- lookupBindM i
      w      <- addFID $ write sym
      -- Add the write to any existing child effects
      nEff   <- combineEffSeq [getEEffect e, Just w]
      return $ addEffSymCh nEff Nothing ch n

    -- For ifThenElse be pessimistic: include effects and symbols of both paths
    handleExpr ch@[p,t,f] n@(tag -> EIfThenElse) = do
      tfEff <- combineEffSet [getEEffect t, getEEffect f]
      -- combineEff with predicate effects
      nEff  <- combineEffSeq [getEEffect p, tfEff]
      -- combineEff path symbols into a new symbol
      nSym  <- combineSymSet [getESymbol t, getESymbol f]
      return $ addEffSymCh nEff nSym ch n

    -- ELambda wraps up the child effect and sticks it in a symbol, but has no effect per se
    -- A new scope will be created at application
    handleExpr ch@[e] n@(tag -> ELambda i) = do
      bindSym <- lookupBindM i
      deleteBindM i
      let eEff = getEEffect e
          eSym = maybeToList $ getESymbol e
      -- Create a closure for the lambda by finding every read/written/applied closure variable
      closure <- case eEff of
                   Nothing -> return emptyClosure
                   Just e' -> createClosure e'
      -- Create a gensym for the lambda, containing the effects of the child, and leading to the symbols
      eScope  <- addFID $ scope [bindSym] closure $ maybeToList eEff
      lSym    <- genSym (PLambda i eScope) eSym
      return $ addEffSymCh Nothing (Just lSym) ch n

    -- For collection attributes, we need to create and apply a lambda
    -- containing 'self'
    -- NOTE: We assume that the effect for this function has been inserted locally
    --       on the project

    -- On application, Apply creates a scope and substitutes into it
    -- We only create the effect of apply here
    handleExpr ch@[l,a] n@(tag -> EOperate OApp) = do
      seqE    <- combineEffSeq [getEEffect l, getEEffect a]
      -- Create the effect of application
      aSym    <- getOrGenSymbol a
      case getESymbol l of
        Nothing   -> trace (show n) $ error "failed to find symbol at lambda"
        Just lSym -> do
          appE    <- addFID $ apply lSym aSym
          fullEff <- combineEffSeq [seqE, Just appE]
          fullSym <- combineSymApply (Just lSym) (Just aSym)
          return $ addEffSymCh fullEff fullSym ch n

    -- Bind
    handleExpr ch@[bind,e] n@(tag -> EBindAs b) = do
      let iProvs = extractBindData b
          ids    = map fst iProvs
      -- Get the scope info
      bindSyms <- mapM lookupBindM ids
      -- Remove binds from env
      mapM_ deleteBindM ids
      -- peel off until we get to a scope we know
      fullSym <- peelSymbol [] $ getESymbol e
      let eEff = maybe [] singleton $ getEEffect e
      bScope  <- addFID $ scope bindSyms emptyClosure eEff
      fullEff <- combineEffSeq [getEEffect bind, Just bScope]
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- CaseOf
    handleExpr ch@[e,some,none] n@(tag -> ECaseOf i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      -- Wrap some in a scope
      let someEff = maybeToList $ getEEffect some
      scopeEff <- addFID $ scope [bindSym] emptyClosure someEff
      -- Conservative approximation
      setEff   <- combineEffSet [getEEffect none, Just scopeEff]
      combSym  <- combineSymSet [getESymbol some, getESymbol none]
      -- peel off symbols until we get ones in our outer scope
      -- for case, we need to special-case, making sure that the particular symbol
      -- is always gensymed away
      fullSym  <- peelSymbol [bindSym] combSym
      fullEff  <- combineEffSeq [getEEffect e, setEff]
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- LetIn
    handleExpr ch@[l,e] n@(tag -> ELetIn i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      let eEff = maybeToList $ getEEffect e
      scopeEff <- addFID $ scope [bindSym] emptyClosure eEff
      fullEff  <- combineEffSeq [getEEffect l, Just scopeEff]
      -- peel off symbols until we get to ones in our outer scope
      fullSym  <- peelSymbol [] $ getESymbol e
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- Projection
    handleExpr ch@[e] n@(tag -> EProject i) =
      -- Check in the type system for a function in a collection
      case (e @~ isEType, n @~ isEType) of
        (Just (EType(tag -> TCollection)), Just (EType(tag -> TFunction))) ->
          -- We can't have effects here -- we only have symbols
          case getESymbol n of
            Nothing   -> trace (show n) $ error $ "Missing symbol for projection of " ++ i
            Just sLam -> do
              eSym    <- getOrGenSymbol e
              -- Create a lambda application for self
              sApp    <- genSym PApply [sLam, eSym]
              return $ addEffSymCh Nothing (Just sApp) ch n

        _ -> do -- not a collection member function
          eSym <- getOrGenSymbol e
          nSym <- genSym (PProject i) $ maybeToList $ getESymbol e
          return $ addEffSymCh (getEEffect e) (Just nSym) ch n

    -- handle seq (last symbol)
    handleExpr ch n@(tag -> EOperate OSeq) = do
      let chSym = getESymbol $ last ch
      eff   <- combineEffSeq $ map getEEffect ch
      return $ addEffSymCh eff chSym ch n

    handleExpr ch n = genericExpr ch n

    -- Generic case: combineEff effects, ignore symbols
    genericExpr ch n = do
      eff <- combineEffSeq $ map getEEffect ch
      return $ addEffSymCh eff Nothing ch n

    -- Post-processing for cyclic scope
    fixUpExprs :: K3 Expression -> MEnv (K3 Expression)
    fixUpExprs node = modifyTree fixupAll node
      where
        fixupAll n = fixupExprEff n >>= fixupExprSym

        fixupExprSym n@(getESymbol -> Nothing) = return n
        fixupExprSym n@(getESymbol -> Just s)  =
          liftM (\x -> (n @- ESymbol s) @+ ESymbol x) $
            mapSym mId fixupSym s
        fixupExprEff n@(getEEffect -> Nothing) = return n
        fixupExprEff n@(getEEffect -> Just e)  =
          liftM (\x -> (n @- EEffect e) @+ EEffect x) $
            mapEff mId fixupSym e

        -- Any unbound globals should be translated
        fixupSym :: K3 Symbol -> MEnv (K3 Symbol)
        fixupSym (tag -> Symbol i (PTemporary TUnbound)) = lookupBindM i
        fixupSym s = return s

    ------ Utilities ------
    -- Common procedure for adding back the symbols, effects and children
    addEffSymCh :: Maybe (K3 Effect) -> Maybe (K3 Symbol) -> [K3 Expression] -> K3 Expression -> K3 Expression
    addEffSymCh eff sym ch n =
      let n'  = maybe n  ((@+) n  . EEffect) eff
          n'' = maybe n' ((@+) n' . ESymbol) sym
      in replaceCh n'' ch

    -- If necessary, remove layers of symbols to get to those just above the bind symbols
    -- This function assumes that the direct bindsymbols have only one child each
    -- @exclude: always delete this particular symbol (for CaseOf)
    peelSymbol :: [K3 Symbol] -> Maybe (K3 Symbol) -> MEnv (K3 Symbol)
    peelSymbol _ Nothing = genSymTemp TTemp []
    peelSymbol excludes (Just sym) = loop Nothing sym >>= symOfSymList
      where
        -- Sending last symbol along allows us to know which kind of temporary to make
        loop :: Maybe Symbol -> K3 Symbol -> MEnv [(Maybe Symbol, K3 Symbol)]

        -- Applys require that we check their children and include the apply instead
        loop _ n@(tag &&& children -> (s@(Symbol i PApply), [lam, arg])) = do
          lS <- loop Nothing lam
          lA <- loop Nothing arg
          case (lS, lA) of
            -- If both arguments are local, elide the Apply
            ([], []) -> return []
            -- Otherwise, keep the apply and its possible tree
            _        -> do
              s  <- symOfSymList lS
              s' <- symOfSymList lA
              return [(Nothing, replaceCh n [s, s'])]

        -- Sets need to be kept if they refer to anything important
        loop _ n@(tnc -> (s@(Symbol i PSet), ch)) = do
          lCh <- mapM (loop (Just s)) ch
          -- If all children are local, elide
          if all ((==) []) lCh then return [] else do
            ss <- mapM symOfSymList lCh
            return [(Nothing, replaceCh n ss)]

        loop mLast n@(tnc -> (s@(Symbol i _), ch)) =
          -- Check for exclusion. Terminate this branch if we match
          if any (n `symEqual`) excludes then return []
          else do
            env <- get
            -- If we don't find the symbol in the environment, it's beneath our scope
            -- So continue to look in the children
            case lookupBindInner i env of
              Nothing -> doLoops
              -- If we find a match, something is in our scope so report back
              -- Just make sure it's really equivalent
              Just s'  -> if s' `symEqual` n then return [(mLast, n)]
                          else doLoops

          where doLoops = liftM concat $ mapM (loop $ Just s) ch


    -- Convert a list of symbols to a combination symbol (if possible)
    -- Also take care of creating the right temporaries based on the last symbol found leading
    -- to the particular symbol
    symOfSymList :: [(Maybe Symbol, K3 Symbol)] -> MEnv (K3 Symbol)
    symOfSymList []               = genSymTemp TTemp []
    -- Anything else just gets a pure temporary
    symOfSymList [x]              = tempOfSym x
    symOfSymList syms             = mapM tempOfSym syms >>= genSym PSet

    tempOfSym (Nothing, s)                      = genSymTemp TTemp [s]
    tempOfSym (Just (Symbol _ PLet), s)         = genSymTemp TAlias [s]
    tempOfSym (Just (Symbol _ PIndirection), s) = genSymTemp TIndirect [s]
    tempOfSym (Just (Symbol _ (PRecord _)), s)  = genSymTemp TSub [s]
    tempOfSym (Just (Symbol _ (PTuple _)), s)   = genSymTemp TSub [s]
    tempOfSym (Just _, s)                       = genSymTemp TTemp [s]

---- Utilities to work with effects dynamically

-- combineEff effects if they're present. Otherwise keep whatever we have
combineEff :: ([K3 Effect] -> K3 Effect) -> [Maybe (K3 Effect)] -> MEnv (Maybe (K3 Effect))
combineEff constF es =
  case filter isJust es of
    []  -> return Nothing
    [e] -> return e
    es' -> liftM Just $ addFID $ constF $ map fromJust es'

combineEffSet :: [Maybe (K3 Effect)] -> MEnv (Maybe (K3 Effect))
combineEffSet = combineEff set
combineEffSeq :: [Maybe (K3 Effect)] -> MEnv (Maybe (K3 Effect))
combineEffSeq = combineEff seq

-- combineSym symbols into 1 symbol
combineSym :: Provenance -> [Maybe (K3 Symbol)] -> MEnv (Maybe (K3 Symbol))
combineSym p ss =
  -- if there's no subsymbol at all, just gensym a temp
  if all (Nothing ==) ss then
    liftM Just $ genSym (PTemporary TTemp) []
  -- if we have some symbols, we must preserve them
  else do
    ss' <- mapM maybeGen ss
    liftM Just $ genSym p ss'
    where
      maybeGen (Just s) = return s
      maybeGen Nothing  = genSym (PTemporary TTemp) []

combineSymSet :: [Maybe (K3 Symbol)] -> MEnv (Maybe (K3 Symbol))
combineSymSet = combineSym PSet
combineSymApply :: Maybe (K3 Symbol) -> Maybe (K3 Symbol) -> MEnv (Maybe (K3 Symbol))
combineSymApply l a = combineSym PApply [l,a]


-- Build a global environment for using state monad functions dynamically
buildEnv :: K3 Declaration -> Env
buildEnv n = snd $ flip runState startEnv $
               addAllGlobals n >>
               mapProgram handleDecl mId highestExprId Nothing n
  where
    handleDecl n@(tag -> DGlobal i _ _)  = possibleInsert n i
    handleDecl n@(tag -> DTrigger i _ _) = possibleInsert n i
    handleDecl n = return n

    possibleInsert n i = do
      highestDeclId n
      case n @~ isDSymbol of
        Just (DSymbol s) -> do
          insertGlobalM i s
          return n
        _          -> return n
    highestDeclId n =
      case n @~ isDSymbol of
        Just (DSymbol s) -> highestSymId s
        _ -> return ()

    highestExprId n = do
      case n @~ isESymbol of
        Just (ESymbol s) -> highestSymId s
        _ -> return ()
      case n @~ isEEffect of
        Just (EEffect e) -> highestEffId e >> return n
        _ -> return n

    highestSymId :: K3 Symbol -> MEnv ()
    highestSymId n = do
      env <- get
      let c = count env
          maxCount = case n @~ isSID of
                       Just (SID n) -> max n c
                       Nothing      -> c
      put $ env {count=maxCount}

    highestEffId :: K3 Effect -> MEnv ()
    highestEffId n = do
      env <- get
      let c = count env
          maxCount = case n @~ isFID of
                        Just (FID n) -> max n c
                        Nothing      -> c
      put $ env {count=maxCount}

applyLambdaEnv :: Env -> K3 Symbol -> K3 Symbol -> (Maybe (K3 Effect, K3 Symbol), Env)
applyLambdaEnv env sArg sLam = flip runState env $ applyLambda sArg sLam

-- If the symbol is a global, substitute from the global environment
-- Apply (substitute) a symbol into a lambda symbol, generating effects and a new symbol
-- If we return Nothing, we cannot apply yet because of a missing lambda
applyLambda :: K3 Symbol -> K3 Symbol -> MEnv (Maybe (K3 Effect, K3 Symbol))
applyLambda sArg sLam =
    case tnc sLam of
      (Symbol _ (PLambda _ e@(tnc -> (FScope [sOld] _, [lamEff]))), [chSym]) -> do
        -- Dummy substitute into the argument, in case there's an application there
        -- Any effects won't be substituted in and will be visible outside
        sArg'   <- mapSym (subEff sOld sOld) (subSym sOld sOld) sArg
        -- Substitute into the old effects and symbol
        e'      <- mapEff (subEff sOld sArg') (subSym sOld sArg') lamEff
        chSym'  <- mapSym (subEff sOld sArg') (subSym sOld sArg') chSym
        return $ Just (e', chSym')

      (Symbol _ PGlobal, [ch]) -> applyLambda sArg ch

      -- For a set 'lambda', we need to combine results
      (Symbol _ PSet, ch)      -> do
        xs <- mapM (applyLambda sArg) ch
        let (es, ss) = unzip $ catMaybes xs
            (es', ss') = (map Just es, map Just ss)
        sSet <- combineSymSet ss'
        eSet <- combineEffSet es'
        return $ Just (fromJust eSet, fromJust sSet)

      _ -> return Nothing

  where
    wrap f x = return $ f x

    -- Substitute a symbol: old, new, symbol in which to replace
    subSym :: K3 Symbol -> K3 Symbol -> K3 Symbol -> MEnv (K3 Symbol)
    subSym s s' n@(tag -> Symbol _ PVar) | n `symEqual` s = return $ replaceCh n [s']
    -- Apply: recurse (we already substituted into the children)
    subSym s s' n@(tnc -> (Symbol _ PApply, [sL, sA])) = do
        x <- applyLambda sA sL
        return $ maybe n snd x
    subSym _ _ n = return n

    -- Substitute one symbol for another in an effect
    subEff :: K3 Symbol -> K3 Symbol -> K3 Effect -> MEnv (K3 Effect)
    subEff sym sym' n@(tag -> FApply sL sA) = do
        x <- applyLambda sA sL
        return $ maybe n fst x
    subEff _ _ n = return n
