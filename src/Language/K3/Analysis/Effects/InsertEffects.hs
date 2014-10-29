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
  EffectEnv(..),
  preprocessBuiltins,
  runAnalysis,
  runAnalysisEnv,
  applyLambda,
  applyLambdaEnv,
  runConsolidatedAnalysis,
  symRWAQuery,
  eE,
  eS
)
where

import Prelude hiding (read, seq)
import Control.Monad.State.Lazy
import Data.Maybe
import Data.Map(Map)
import qualified Data.Map as Map
import Data.IntMap(IntMap)
import qualified Data.IntMap as IntMap
import Data.List(nub, delete)
import Data.Foldable hiding (mapM_, any, all, concatMap, concat, elem)
import Debug.Trace(trace)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Declaration
import Language.K3.Core.Utils
import Language.K3.Core.Type

import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.Constructors
import Language.K3.Utils.Pretty(pretty)

import qualified Language.K3.Analysis.InsertMembers as IM

debug = True

type GlobalEnv  = Map Identifier (K3 Symbol)
type LocalEnv   = Map Identifier [K3 Symbol]
type SymbolMap  = IntMap (K3 Symbol)
type EffectMap  = IntMap (K3 Effect)

data EffectEnv = EffectEnv {
                globalEnv :: GlobalEnv,
                count :: Int,
                bindEnv :: LocalEnv,
                symEnv :: SymbolMap,
                effEnv :: EffectMap
               }

startEnv :: EffectEnv
startEnv = EffectEnv {
             globalEnv=Map.empty,
             count = 1,
             bindEnv=Map.empty,
             symEnv=IntMap.empty,
             effEnv=IntMap.empty
           }

insertGlobal :: Identifier -> K3 Symbol -> EffectEnv -> EffectEnv
insertGlobal i s env = env {globalEnv=Map.insert i s $ globalEnv env}

getId :: EffectEnv -> (Int, EffectEnv)
getId env = (count env, env {count = 1 + count env})

insertBind :: Identifier -> K3 Symbol -> EffectEnv -> EffectEnv
insertBind i s env =
  env {bindEnv=Map.insertWith (++) i [s] $ bindEnv env}

deleteBind :: Identifier -> EffectEnv -> EffectEnv
deleteBind i env =
  let m  = bindEnv env
      m' = case Map.lookup i m of
             Just []     -> Map.delete i m
             Just [_]    -> Map.delete i m
             Just (_:xs) -> Map.insert i xs m
             Nothing     -> m
  in
  env {bindEnv=m'}

clearBinds :: EffectEnv -> EffectEnv
clearBinds env = env {bindEnv=Map.empty}

emptyClosure :: ClosureInfo
emptyClosure = ([],[],[])

-- Lookup either in the bind environment or the global environment
lookupBindInner :: Identifier -> EffectEnv -> Maybe (K3 Symbol)
lookupBindInner i env =
  case Map.lookup i $ bindEnv env of
    Nothing -> lookupGlobalInner i env
    s       -> liftM head s

lookupGlobalInner :: Identifier -> EffectEnv -> Maybe (K3 Symbol)
lookupGlobalInner i env = Map.lookup i $ globalEnv env

lookupBindInnerM :: Identifier -> MEnv (Maybe (K3 Symbol))
lookupBindInnerM i = do
  env <- get
  return $ lookupBindInner i env


lookupBind :: Identifier -> EffectEnv -> K3 Symbol
lookupBind i env = fromMaybe err $ lookupBindInner i env
  where err = error $ "failed to find " ++ i ++ " in environment"

lookupGlobal :: Identifier -> EffectEnv -> K3 Symbol
lookupGlobal i env = fromMaybe err $ lookupGlobalInner i env
  where err = error $ "failed to find " ++ i ++ " in global environment"

lookupEffect :: Int -> EffectEnv -> Maybe (K3 Effect)
lookupEffect i env = IntMap.lookup i $ effEnv env

lookupSymbol :: Int -> EffectEnv -> Maybe (K3 Symbol)
lookupSymbol i env = IntMap.lookup i $ symEnv env

insertEffect :: Int -> K3 Effect -> EffectEnv -> EffectEnv
insertEffect i e env = env {effEnv=IntMap.insert i e $ effEnv env}

insertSymbol :: Int -> K3 Symbol -> EffectEnv -> EffectEnv
insertSymbol i s env = env {symEnv=IntMap.insert i s $ symEnv env}

type MEnv = State EffectEnv

insertGlobalM :: Identifier -> K3 Symbol -> MEnv ()
insertGlobalM i s = modify $ insertGlobal i s

insertBindM :: Identifier -> K3 Symbol -> MEnv ()
insertBindM i s = modify $ insertBind i s

deleteBindM :: Identifier -> MEnv ()
deleteBindM i = modify $ deleteBind i

clearBindsM :: MEnv ()
clearBindsM = modify clearBinds

lookupBindM :: Identifier -> MEnv (K3 Symbol)
lookupBindM i = do
  env <- get
  return $ lookupBind i env

lookupGlobalM :: Identifier -> MEnv (K3 Symbol)
lookupGlobalM i = do
  env <- get
  return $ lookupGlobal i env

getIdM :: MEnv Int
getIdM = do
  e <- get
  let (i, e') = getId e
  put e'
  return i

lookupEffectM :: Int -> MEnv (Maybe (K3 Effect))
lookupEffectM i = liftM (lookupEffect i) get

lookupSymbolM :: Int -> MEnv (Maybe (K3 Symbol))
lookupSymbolM i = liftM (lookupSymbol i) get

insertEffectM :: Int -> K3 Effect -> MEnv ()
insertEffectM i e = modify $ insertEffect i e

insertSymbolM :: Int -> K3 Symbol -> MEnv ()
insertSymbolM i s = modify $ insertSymbol i s

-- Shallow substitution of symbol/effects id from env
expandEffM :: K3 Effect -> MEnv (K3 Effect)
expandEffM (tag -> FEffId i) = liftM (fromMaybe $ error $ "effect " ++ show i ++ " not in environment") (lookupEffectM i) >>= expandEffM
expandEffM eff               = return eff

expandSymM :: K3 Symbol -> MEnv (K3 Symbol)
expandSymM (tag -> SymId i)  = liftM (fromMaybe $ error $ "symbol " ++ show i ++ " not in environment") (lookupSymbolM i) >>= expandSymM
expandSymM sym               = return sym

expandEff :: EffectEnv -> K3 Effect -> K3 Effect
expandEff env eff = flip evalState env $ expandEffM eff

expandSym :: EffectEnv -> K3 Symbol -> K3 Symbol
expandSym env sym = flip evalState env $ expandSymM sym

expandEffDeepM :: K3 Effect -> MEnv (K3 Effect)
expandEffDeepM eff = do
  eff' <- expandEffM eff
  tg   <- case tag eff' of
             FRead s  -> liftM FRead  $ exSym s
             FWrite s -> liftM FWrite $ exSym s
             FScope ss (Right (a,b,c)) -> liftM2 FScope (exSyms ss) $ liftM Right $ liftM3 (,,) (exSyms a) (exSyms b) (exSyms c)
             FScope ss x -> liftM2 FScope (exSyms ss) (return x)
             FApply s s' -> liftM2 FApply (exSym s) (exSym s')
             x -> return x
  let eff'' = replaceTag eff' tg
  handleCh eff''
  where
    handleCh eff'@(children -> ch) = do
      ch'  <- mapM expandEffDeepM ch
      return $ replaceCh eff' ch'
    exSym  = expandSymDeepM
    exSyms = mapM expandSymDeepM

expandSymDeepM :: K3 Symbol -> MEnv (K3 Symbol)
expandSymDeepM sym@(children -> ch) = do
  sym' <- expandSymM sym
  tg   <- case tag sym' of
             Symbol i (PScope ss (Right (a,b,c))) -> liftM (Symbol i) $ liftM2 PScope (exSyms ss) $ liftM Right $ liftM3 (,,) (exSyms a) (exSyms b) (exSyms c)
             Symbol i (PScope ss x)       -> liftM (Symbol i) $ liftM2 PScope (exSyms ss) (return x)
             Symbol i (PLambda i' e)      -> liftM (Symbol i . PLambda i') (expandEffDeepM e)
             x -> return x
  let sym'' = replaceTag sym' tg
  handleCh sym''
  where
    handleCh sym'@(children -> ch) = do
      ch'  <- mapM expandSymDeepM ch
      return $ replaceCh sym' ch'
    exSym  = expandSymDeepM
    exSyms = mapM expandSymDeepM

expandProg :: K3 Declaration -> MEnv (K3 Declaration)
expandProg n = mapProgram mId mId expandExprs Nothing n

expandExprs :: K3 Expression -> MEnv (K3 Expression)
expandExprs n = modifyTree expandExpr n
  where
    expandExpr :: K3 Expression -> MEnv (K3 Expression)
    expandExpr n = do
      let e = getEEffect n
          s = getESymbol n
      e' <- case e of
              Nothing  -> return Nothing
              Just eff -> liftM Just $ expandEffDeepM eff
      s' <- case s of
              Nothing  -> return Nothing
              Just sym -> liftM Just $ expandSymDeepM sym
      return $ addEffSym e' s' n

-- Common procedure for adding back the symbols, effects and children
addEffSym :: Maybe (K3 Effect) -> Maybe (K3 Symbol) -> K3 Expression -> K3 Expression
addEffSym eff sym n =
  let n'   = stripAnno (\x -> isEEffect x || isESymbol x) n
      n''  = maybe n'  ((@+) n'  . EEffect) eff
      n''' = maybe n'' ((@+) n'' . ESymbol) sym
  in n'''

addEffSymCh :: Maybe (K3 Effect) -> Maybe (K3 Symbol) -> [K3 Expression] -> K3 Expression -> K3 Expression
addEffSymCh eff sym ch n =
  let n'   = addEffSym eff sym n
  in replaceCh n' ch

-- Shortcuts for expandeffect & expandSym
eE :: EffectEnv -> K3 Effect -> K3 Effect
eE = expandEff

eS :: EffectEnv -> K3 Symbol -> K3 Symbol
eS = expandSym

-- Update symbols/effects: old, new, whether they're ids or not
updateEffM :: K3 Effect -> K3 Effect -> MEnv (K3 Effect)
updateEffM e@(tag -> FEffId i) e' = insertEffectM i e' >> return e
updateEffM _ e@(tag -> FEffId _)  = return e
updateEffM e e' = {- trace ("bad update: \n"++show e++"\n to: \n"++show e') $-} do
  i <- getIdM
  let e' = stripAnno isFID e
  insertEffectM i $ e' @+ FID i
  return $ effId i @+ FID i

updateSymM :: K3 Symbol -> K3 Symbol -> MEnv (K3 Symbol)
updateSymM e@(tag -> SymId i) e' = insertSymbolM i e' >> return e
updateSymM _ e@(tag -> SymId _)  = return e
updateSymM e e' = {- trace ("bad update: \n"++show e++"\n to: \n"++show e') $-} do
  i <- getIdM
  let e' = stripAnno isSID e
  insertSymbolM i $ e' @+ SID i
  return $ symId i @+ SID i

singleton :: a -> [a]
singleton x = [x]

-- Create an effect and add it to the map
genEff :: K3 Effect -> MEnv (K3 Effect)
genEff eff = do
  i <- getIdM
  insertEffectM i $ eff @+ FID i
  return $ effId i @+ FID i

-- Add an id to an effect
addFID :: K3 Effect -> MEnv (K3 Effect)
addFID eff = do
  i <- getIdM
  return $ eff @+ FID i

addSID :: K3 Symbol -> MEnv (K3 Symbol)
addSID sym = do
  i <- getIdM
  return $ sym @+ SID i

getSID :: K3 Symbol -> Maybe Int
getSID sym = liftM extract $ sym @~ isSID
  where extract (SID i) = i

-- Generate a symbol
symbolM :: Identifier -> Provenance -> [K3 Symbol] -> MEnv (K3 Symbol)
symbolM name prov ch = do
  i <- getIdM
  let s = symbol name prov @+ SID i
  insertSymbolM i $ replaceCh s ch
  return $ symId i @+ SID i

genSym :: Provenance -> [K3 Symbol] -> MEnv (K3 Symbol)
genSym prov ch = do
   i <- getIdM
   let s = symbol ("sym_"++show i) prov @+ SID i
   insertSymbolM i $ replaceCh s ch
   return $ symId i @+ SID i

genSymTemp :: TempType -> [K3 Symbol] -> MEnv (K3 Symbol)
genSymTemp tempType = genSym $ PTemporary tempType

getEEffect :: K3 Expression -> Maybe (K3 Effect)
getEEffect n = case n @~ isEEffect of
                 Just (EEffect e) -> Just e
                 _                -> Nothing

getESymbol :: K3 Expression -> Maybe (K3 Symbol)
getESymbol n = case n @~ isESymbol of
                 Just (ESymbol e) -> Just e
                 _                -> Nothing

getEEffectFull :: K3 Expression -> MEnv (Maybe (K3 Effect))
getEEffectFull n = case getEEffect n of
                     Nothing                -> return Nothing
                     Just (tag -> FEffId i) -> lookupEffectM i
                     e                      -> return e

getESymbolFull :: K3 Expression -> MEnv (Maybe (K3 Symbol))
getESymbolFull n = case getESymbol n of
                     Nothing                -> return Nothing
                     Just (tag -> SymId i)  -> lookupSymbolM i
                     s                      -> return s

-- If we don't have a symbol, we automatically gensym one
getOrGenSymbol :: K3 Expression -> MEnv (K3 Symbol)
getOrGenSymbol n = case getESymbol n of
                     Nothing -> genSymTemp TTemp []
                     Just i  -> return i

-- Create a closure of symbols read, written, or applied that are relevant to the current env
-- @optScope: optimize using other scopes
createClosure :: Bool -> Maybe (K3 Effect) -> Maybe (K3 Symbol) -> MEnv ClosureInfo
createClosure optScope mEff mSym = liftM nubTuple $ do
  acc  <- case mSym of
           Nothing  -> return emptyClosure
           Just sym -> addClosureSym emptyClosure sym
  case mEff of
    Nothing  -> return acc
    Just eff -> addClosureEff acc eff
  where
    nubTuple (a,b,c) = (nub a, nub b, nub c)

    addClosureEff :: ClosureInfo -> K3 Effect -> MEnv ClosureInfo
    addClosureEff acc n' = do
      n <- expandEffM n'
      case (acc, tag n) of
        (_, FRead s) -> do
          (a,b,c) <- addClosureSym acc s
          s'      <- getClosureSyms [] s
          return (s' ++ a,b,c)
        (_, FWrite s) -> do
          (a,b,c) <- addClosureSym acc s
          s'      <- getClosureSyms [] s
          return (a, s' ++ b, c)
        (_, FApply s s') -> do
          acc'    <- addClosureSym acc  s
          (a,b,c) <- addClosureSym acc' s'
          s''     <- getClosureSyms [] s'
          return (a, b, s'' ++ c)
        -- Try to be efficient by reusing results from previous scopes
        ((a', b', c'), FScope _ (Right cl@(a,b,c))) | cl /= emptyClosure && optScope -> do
          a'' <- unite a
          b'' <- unite b
          c'' <- unite c
          return (a'' ++ a', b'' ++ b', c'' ++ c')
          where unite x = foldrM (flip getClosureSyms) [] x
        (_, _) -> foldrM (flip addClosureEff) acc $ children n

    addClosureSym acc n' = do
      n <- expandSymM n'
      case tag n of
         Symbol _ (PLambda _ eff) -> do
           acc' <- foldrM (flip addClosureSym) acc $ children n
           addClosureEff acc' eff
         Symbol _ PLet | not optScope -> return acc

         _ -> foldrM (flip addClosureSym) acc $ children n

    -- The method for searching for valid symbols
    getClosureSyms acc n' = do
      n <- expandSymM n'
      case tnc n of
        (SymId _, _)                        -> error "unexpected symId1"
        (Symbol _ (PTemporary TTemp),    _) -> return acc
        (Symbol _ (PTemporary TUnbound), _) -> return acc
        (Symbol _ PLet, _) | not optScope   -> return acc
        (Symbol i _, ch)                    -> do
          x' <- lookupBindInnerM i
          case x' of
            -- if we haven't found a match, it might be deeper in the tree
            Nothing  -> foldrM (flip getClosureSyms) acc ch
            Just x'' -> do
              x  <- expandSymM x''
              case tag x of
                Symbol _ (PTemporary TTemp)    -> return acc
                Symbol _ (PTemporary TUnbound) -> return acc
                Symbol _ PGlobal               -> return acc
                _ | n `symEqual` x             -> return $ n':acc
                -- These 2 symbols' children aren't really further provenances
                Symbol _ (PLambda _ _)         -> return acc
                Symbol _ PApply                -> return acc
                -- if we haven't found a match, it might be deeper in the tree
                _ -> foldrM (flip getClosureSyms) acc ch

addAllGlobals :: K3 Declaration -> MEnv (K3 Declaration)
addAllGlobals node = mapProgram preHandleDecl mId mId Nothing node
  where
    -- add everything to global environment for cyclic/recursive scope
    -- we'll fix it up the second time through
    preHandleDecl n@(tag -> DGlobal i _ _)  =
      case n @~ isDSymbol of
        Just (DSymbol s) -> addGlobal i s >> return n
        _                -> addGeni i >> return n

    preHandleDecl n@(tag -> DTrigger i _ _) =
      case n @~ isDSymbol of
        Just (DSymbol s) -> addGlobal i s >> return n
        _                -> addGeni i >> return n

    preHandleDecl n = return n

    addGeni i     = symbolM i (PTemporary TUnbound) [] >>= insertGlobalM i
    addGlobal i s = symbolM i PGlobal [s]              >>= insertGlobalM i

mId :: Monad m => a -> m a
mId = return

symEqual :: K3 Symbol -> K3 Symbol -> Bool
symEqual (getSID -> s) (getSID -> s')    = s == s'

-- map over symbols and effects, starting at an effect
mapEff :: (K3 Effect -> MEnv (K3 Effect)) -> (K3 Symbol -> MEnv (K3 Symbol)) -> K3 Effect -> MEnv (K3 Effect)
mapEff effFn symFn eff = wrapMapEffFn effFn symFn eff

mapSym :: (K3 Effect -> MEnv (K3 Effect)) -> (K3 Symbol -> MEnv (K3 Symbol)) -> K3 Symbol -> MEnv (K3 Symbol)
mapSym effFn symFn sym = wrapMapSymFn effFn symFn sym

wrapMapEffFn :: (K3 Effect -> MEnv (K3 Effect)) -> (K3 Symbol -> MEnv (K3 Symbol)) -> K3 Effect -> MEnv (K3 Effect)
wrapMapEffFn effFn symFn n' = do
  n2  <- expandEffM n'
  ch <- mapM (wrapMapEffFn effFn symFn) $ children n2
  let n = replaceCh n2 ch
  n3 <- case tag n of
          FRead s -> do
            s' <- doMap s
            effFn $ replaceTag n $ FRead s'
          FWrite s -> do
            s' <- doMap s
            effFn $ replaceTag n $ FWrite s'
          FScope ss (Right (xs,ys,zs)) -> do
            ss' <- mapM doMap ss
            xs' <- mapM doMap xs
            ys' <- mapM doMap ys
            zs' <- mapM doMap zs
            effFn $ replaceTag n $ FScope ss' (Right (xs', ys', zs'))
          FScope ss x -> do
            ss' <- mapM doMap ss
            effFn $ replaceTag n $ FScope ss' x
          FApply sL sA -> do
            sL' <- doMap sL
            sA' <- doMap sA
            effFn $ replaceTag n $ FApply sL' sA'
          _ -> effFn n
  updateEffM n' n3
  where doMap = mapSym effFn symFn

wrapMapSymFn :: (K3 Effect -> MEnv (K3 Effect)) -> (K3 Symbol -> MEnv (K3 Symbol)) -> K3 Symbol -> MEnv (K3 Symbol)
wrapMapSymFn effFn symFn n' = do
  n2  <- expandSymM n'
  ch <- mapM (wrapMapSymFn effFn symFn) $ children n2
  let n = replaceCh n2 ch
  n3 <- case tag n of
          Symbol x (PScope ss (Right (xs,ys,zs))) -> do
            ss' <- mapM (mapSym effFn symFn) ss
            xs' <- mapM (mapSym effFn symFn) xs
            ys' <- mapM (mapSym effFn symFn) ys
            zs' <- mapM (mapSym effFn symFn) zs
            symFn $ replaceTag n $ Symbol x (PScope ss' (Right (xs', ys', zs')))
          Symbol x (PScope ss y) -> do
            ss' <- mapM (mapSym effFn symFn) ss
            symFn $ replaceTag n $ Symbol x (PScope ss' y)
          Symbol x (PLambda y e) -> do
            e' <- mapEff effFn symFn e
            symFn $ replaceTag n $ Symbol x $ PLambda y e'
          _ -> symFn n
  updateSymM n' n3

-------- Preprocessing phase --------
--
-- Fill in the effect symbols missing in any builtins
-- Number any existing symbols with SIDs and FIDs
-- This must be called before effects are lifted into the expression tree
preprocessBuiltins :: K3 Declaration -> (K3 Declaration, EffectEnv)
preprocessBuiltins prog = flip runState startEnv $ modifyTree addMissingDecl prog
  where
    addMissingDecl :: K3 Declaration -> MEnv(K3 Declaration)
    addMissingDecl n =
      case (tag n, n @~ isDSymbol) of
        (DDataAnnotation i t attrs, _) -> do
          attrs' <- mapM handleAttrs attrs
          return $ replaceTag n $ DDataAnnotation i t attrs'
        -- A global without an effect symbol
        (DGlobal _ t@(tag -> TFunction) Nothing, Nothing) -> handleFunctions t
        (DGlobal _ t@(tag -> TForall _) Nothing, Nothing) -> handleFunctions t
        -- If we have a symbol, number it
        (_, Just (DSymbol s)) -> do
          s' <- liftM DSymbol $ numberSyms s
          return $ stripAnno isDSymbol n @+ s'
        _ -> return n
      where handleFunctions t = do
              s <- symOfFunction False t
              return $ n @+ DSymbol s

    -- Handle lifted/unlifted attributes without symbols
    handleAttrs :: AnnMemDecl -> MEnv AnnMemDecl
    handleAttrs (Lifted x y t@(tag -> TFunction) Nothing annos)
      | isNothing (find isDSymbol annos) = do
          s <- symOfFunction True t
          return $ Lifted x y t Nothing $ DSymbol s:annos

    -- If we have a sumbol, number it
    handleAttrs (Lifted x y z u as) = liftM (Lifted x y z u) $ handleAttrsInner as

    handleAttrs (Attribute x y t Nothing annos)
      | isNothing (find isDSymbol annos) = do
          s <- symOfFunction True t
          return $ Attribute x y t Nothing $ DSymbol s:annos

    -- If we have a symbol, number it
    handleAttrs (Attribute x y z u as) = liftM (Attribute x y z u) $ handleAttrsInner as

    handleAttrs a = return a

    -- handle common attribute symbol/effect renumbering functionality
    handleAttrsInner as =
      case find isDSymbol as of
        Just ds@(DSymbol s) -> do
          s' <- liftM DSymbol $ numberSyms s
          let as' = delete ds as
          return $ s':as'
        _ -> return as

    -- Number existing symbols/effects
    numberSyms s = do
      s' <- mapSym addNumEff addNumSym s
      clearBindsM  -- binds are only temporary here
      return s'

    -- For symbols, we only need a very simple binding pattern
    addNumSym s@(tag -> Symbol i PVar) = do
      l <- lookupBindInnerM i
      case l of
        Nothing -> do
          s' <- addSID s
          insertBindM i s'
          return s'
        Just s' -> return s'
    addNumSym s = addSID s

    addNumEff = addFID

    -- Create a symbol for a function based on type
    -- If we're an attribute, we need to also write to self
    symOfFunction :: Bool -> K3 Type -> MEnv (K3 Symbol)
    symOfFunction addSelf t = liftM head $ symOfFunction' addSelf t 1

    symOfFunction' :: Bool -> K3 Type -> Int -> MEnv [K3 Symbol]
    symOfFunction' a (tnc -> (TForall _,   [ch])) i = symOfFunction' a ch i
    symOfFunction' addSelf (tnc -> (TFunction, [_, ret])) i = do
      s  <- symOfFunction' addSelf ret $ i + 1
      s' <- createConservativeSym (addSelf && i==1) s $ "__"++show i
      return [s']
    symOfFunction' _ _ _ = return []

    -- Create a default conservative symbol for the function
    -- @addSelf: add a r/w to 'self' (for attributes)
    createConservativeSym addSelf subSym' nm = do
      sym   <- symbolM nm PVar []
      r     <- genEff $ read sym
      w     <- genEff $ write sym
      seq'  <- if addSelf then do
                 selfSym <- symbolM "self" PVar []
                 rSelf   <- genEff $ read selfSym
                 wSelf   <- genEff $ write selfSym
                 return [Just w, Just r, Just wSelf, Just rSelf]
               else
                 return [Just w, Just r]
      seq'' <- combineEffSeq seq'
      lp    <- genEff $ loop $ fromMaybe (error "createConservativeSym") seq''
      sc    <- genEff $ scope [sym] (Right emptyClosure) [lp]
      genSym (PLambda nm sc) subSym'

----- Actual effect insertion ------
-- Requires an environment built up by the preprocess phase

runAnalysis :: K3 Declaration -> (K3 Declaration, EffectEnv)
runAnalysis = runAnalysisEnv startEnv

runConsolidatedAnalysis :: K3 Declaration -> (K3 Declaration, EffectEnv)
runConsolidatedAnalysis d =
  let (p, env) = preprocessBuiltins d in
  runAnalysisEnv env $ IM.runAnalysis p

runAnalysisEnv :: EffectEnv -> K3 Declaration -> (K3 Declaration, EffectEnv)
runAnalysisEnv env1 prog = flip runState env1 $ do
  -- for cyclic scope, add temporaries for all globals
  p1 <- addAllGlobals prog
  -- actual modification of AST (no need to decorate declarations here)
  p2 <- mapProgram handleDecl mId handleExprs Nothing p1
  -- apply all lambdas
  p3 <- mapProgram handleDecl mId applyLambdaExprs Nothing p2
  -- update closures
  p4 <- mapProgram mId mId substClosureExprs Nothing p3
  p4' <- expandProg p4
  -- trace (pretty p4') $ return p4
  return p4
  where
    -- Add all globals and decorate tree
    handleDecl :: K3 Declaration -> MEnv (K3 Declaration)
    handleDecl n =
      case tag n of
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
          return $ stripAnno isDSymbol n @+ DSymbol sym

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
      eff <- genEff $ read sym
      return $ addEffSymCh (Just eff) (Just sym) [] n

    -- An assignment generates a write, and no symbol
    handleExpr ch@[e] n@(tag -> EAssign i) = do
      sym    <- lookupBindM i
      w      <- genEff $ write sym
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
      let eEff = getEEffect e
          eSym = maybeToList $ getESymbol e
      -- Create a gensym for the lambda, containing the effects of the child, and leading to the symbols
      env     <- get
      eScope  <- genEff $ scope [bindSym] (Left (bindEnv env, getEEffect e, getESymbol e)) $ maybeToList eEff
      deleteBindM i
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
        Nothing   -> error $ "failed to find symbol at lambda: " ++ show n
        Just lSym -> do
          appE    <- genEff $ apply lSym aSym
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
      bScope  <- genEff $ scope bindSyms (Right emptyClosure) eEff
      fullEff <- combineEffSeq [getEEffect bind, Just bScope]
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- CaseOf
    handleExpr ch@[e,some,none] n@(tag -> ECaseOf i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      -- Wrap some in a scope
      let someEff = maybeToList $ getEEffect some
      scopeEff <- genEff $ scope [bindSym] (Right emptyClosure) someEff
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
      scopeEff <- genEff $ scope [bindSym] (Right emptyClosure) eEff
      fullEff  <- combineEffSeq [getEEffect l, Just scopeEff]
      -- peel off symbols until we get to ones in our outer scope
      fullSym  <- peelSymbol [] $ getESymbol e
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- Projection
    handleExpr ch@[e] n@(tag -> EProject i) =
      -- Check in the type system for a function in a collection
      case (e @~ isEType, n @~ isEType) of
        (Just (EType(tag -> TCollection)), Just (EType(tag -> TFunction))) ->
          case getESymbol n of
            Just nSym -> do
              eSym  <- getOrGenSymbol e
              nSym' <- mapSym mId (subSelf eSym) nSym
              return $ addEffSymCh (getEEffect e) (Just nSym') ch n

            _   -> error $ "Missing symbol for projection of " ++ i

        _ -> do -- not a collection member function
          nSym <- genSym (PProject i) $ maybeToList $ getESymbol e
          return $ addEffSymCh (getEEffect e) (Just nSym) ch n
      where
        subSelf s n'@(tag -> Symbol "self" PVar)    = return $ replaceCh n' [s]
        subSelf s n'@(tag -> Symbol "content" PVar) = return $ replaceCh n' [s]
        subSelf _ n'                                = return n'

    -- handle seq (last symbol)
    handleExpr ch n@(tag -> EOperate OSeq) = do
      let chSym = getESymbol $ last ch
      eff   <- combineEffSeq $ map getEEffect ch
      return $ addEffSymCh eff chSym ch n

    handleExpr ch n@(tag -> EOperate OSnd) = do
      eff  <- genEff fio
      eff' <- combineEffSeq $ map getEEffect ch ++ [Just eff]
      return $ addEffSymCh eff' Nothing ch n

    handleExpr ch n = genericExpr ch n

    -- go over the tree and apply all lambdas
    applyLambdaExprs n = modifyTree applyLambdaExpr n

    applyLambdaExpr n = do
      let mSym = getESymbol n
          mEff = getEEffect n
      mSym' <- case mSym of
                 Nothing -> return Nothing
                 Just s  -> liftM Just $ mapSym (subEff Nothing) (subSym Nothing) s
      mEff' <- case mEff of
                 Nothing -> return Nothing
                 Just s  -> liftM Just $ mapEff (subEff Nothing) (subSym Nothing) s
      return $ addEffSym mEff' mSym' n

    -- go over the tree and calculate all closures
    substClosureExprs n = modifyTree substClosureExpr n

    substClosureExpr n = do
      let mSym = getESymbol n
          mEff = getEEffect n
      mSym' <- case mSym of
                 Nothing -> return Nothing
                 Just (s@(tag -> SymId _)) -> liftM Just $ mapSym doSubEff mId s
                 -- Just s  -> trace ("sym in tree:"++show s) $ liftM Just $ mapSym doSubEff mId s
      mEff' <- case mEff of
                 Nothing -> return Nothing
                 Just (s@(tag -> FEffId _))  -> liftM Just $ mapEff doSubEff mId s
                 -- Just s  -> trace ("esym in tree:"++show s) $ liftM Just $ mapEff doSubEff mId s
      return $ addEffSym mEff' mSym' n
        where
          doSubEff :: K3 Effect -> MEnv (K3 Effect)
          doSubEff n@(tnc -> (FScope s (Left (binds, eff, sym)), ch)) = do
            modify (\env -> env {bindEnv=binds})
            closure <- createClosure True eff sym
            return $ replaceTag n $ FScope s (Right closure)
          doSubEff n = return n

    -- Generic case: combineEff effects, ignore symbols
    genericExpr ch n = do
      eff <- combineEffSeq $ map getEEffect ch
      return $ addEffSymCh eff Nothing ch n

    ------ Utilities ------

    -- TODO: from here -----

    -- If necessary, remove layers of symbols to get to those just above the bind symbols
    -- This function assumes that the direct bindsymbols have only one child each
    -- @exclude: always delete this particular symbol (for CaseOf)
    peelSymbol :: [K3 Symbol] -> Maybe (K3 Symbol) -> MEnv (K3 Symbol)
    peelSymbol _ Nothing = genSymTemp TTemp []
    peelSymbol excludes (Just sym) =
      loops Nothing sym >>= symOfSymList
      where
        -- Sending last symbol along allows us to know which kind of temporary to make
        loops :: Maybe (K3 Symbol) -> K3 Symbol -> MEnv [(Maybe (K3 Symbol), K3 Symbol)]
        loops mLast n' = do
          n <- expandSymM n'
          case tnc n of
            -- Applys require that we check their children and include the apply instead
            (Symbol _ PApply, [lam, arg]) -> do
              lS <- loops Nothing lam
              lA <- loops Nothing arg
              case (lS, lA) of
                -- If both arguments are local, elide the Apply
                ([], []) -> return []
                -- Otherwise, keep the apply and its possible tree
                _        -> do
                  s  <- symOfSymList lS
                  s' <- symOfSymList lA
                  return [(Nothing, replaceCh n [s, s'])]

            -- Sets need to be kept if they refer to anything important
            (Symbol _ PSet, ch) -> do
              lCh <- mapM (loops (Just n')) ch
              -- If all children are local, elide
              if all ([] ==) lCh then return [] else do
                ss <- mapM symOfSymList lCh
                return [(Nothing, replaceCh n ss)]

            (Symbol i _, ch) ->
              -- Check for exclusion. Terminate this branch if we match
              if any (n `symEqual`) excludes then return []
              else do
                env' <- get
                -- If we don't find the symbol in the environment, it's beneath our scope
                -- So continue to look in the children
                case lookupBindInner i env' of
                  Nothing -> doLoops
                  -- If we find a match, something is in our scope so report back
                  -- Just make sure it's really equivalent
                  Just s'  -> if s' `symEqual` n then return [(mLast, n')]
                              else doLoops

              where doLoops = liftM concat $ mapM (loops $ Just n') ch

            (_, _) -> error "unexpected"


    -- Convert a list of symbols to a combination symbol (if possible)
    -- Also take care of creating the right temporaries based on the last symbol found leading
    -- to the particular symbol
    symOfSymList :: [(Maybe (K3 Symbol), K3 Symbol)] -> MEnv (K3 Symbol)
    symOfSymList []               = genSymTemp TTemp []
    -- Anything else just gets a pure temporary
    symOfSymList [x]              = tempOfSym x
    symOfSymList syms             = mapM tempOfSym syms >>= genSym PSet

    tempOfSym (Nothing, s)     = genSymTemp TTemp [s]
    tempOfSym (Just p', s) = do
      p <- expandSymM p'
      case tag p of
        Symbol _ PLet         -> genSymTemp TAlias [s]
        Symbol _ PIndirection -> genSymTemp TIndirect [s]
        Symbol _ (PRecord _)  -> genSymTemp TSub [s]
        Symbol _ (PTuple _)   -> genSymTemp TSub [s]
        _                     -> genSymTemp TTemp [s]

---- Utilities to work with effects dynamically

-- combineEff effects if they're present. Otherwise keep whatever we have
combineEff :: ([K3 Effect] -> K3 Effect) -> [Maybe (K3 Effect)] -> MEnv (Maybe (K3 Effect))
combineEff constF es =
  case filter isJust es of
    []  -> return Nothing
    [e] -> return e
    es' -> liftM Just $ genEff $ constF $ map (fromMaybe $ error "combineEff") es'

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
    if length ss' == 1 then return $ Just $ head ss'
      else liftM Just $ genSym p ss'
    where
      maybeGen (Just s) = return s
      maybeGen Nothing  = genSym (PTemporary TTemp) []

combineSymSet :: [Maybe (K3 Symbol)] -> MEnv (Maybe (K3 Symbol))
combineSymSet = combineSym PSet
combineSymApply :: Maybe (K3 Symbol) -> Maybe (K3 Symbol) -> MEnv (Maybe (K3 Symbol))
combineSymApply l a = combineSym PApply [l,a]

applyLambdaEnv :: EffectEnv -> K3 Symbol -> K3 Symbol -> Maybe (K3 Effect, K3 Symbol)
applyLambdaEnv env l a = flip evalState env $ applyLambda l a

-- If the symbol is a global, substitute from the global environment
-- Apply (substitute) a symbol into a lambda symbol, generating effects and a new symbol
-- If we return Nothing, we cannot apply yet because of a missing lambda
applyLambda :: K3 Symbol -> K3 Symbol -> MEnv (Maybe (K3 Effect, K3 Symbol))
applyLambda sLam' sArg = do
  env  <- get
  sLam <- expandSymM sLam'
  case tnc sLam of
    (Symbol _ (PLambda _ lamEff@(tag . eE env -> FScope [sOld] _)), chSym) -> do
      -- Dummy substitute into the argument, in case there's an application there
      -- Any effects won't be substituted in and will be visible outside
      sArg'    <- mapSym (subEff Nothing) (subSym Nothing) sArg
      -- Substitute into the old effects and symbol
      lamEff'  <- mapEff (subEff $ Just (sOld, sArg')) (subSym $ Just (sOld, sArg')) lamEff
      lamEff'' <- expandEffM lamEff'
      chSym'   <- case (chSym, tag lamEff'') of
                    -- Lift the scope of the effect and put it in a pscope after substituting in
                    ([ch], FScope s cl) -> mapSym (subEff $ Just (sOld, sArg')) (subSym $ Just (sOld, sArg')) ch >>=
                                           genSym (PScope s cl) . singleton
                    (_, _)              -> genSymTemp TTemp []
      -- For debugging
      sLam2 <- expandSymDeepM sLam
      sArg2 <- expandSymDeepM sArg'
      lamEff2 <- expandEffDeepM lamEff''
      chSym2 <- expandSymDeepM chSym'
      --trace ("applied lambda: \n"++pretty sLam2++"\nto arg: \n"++pretty sArg2++"\nresult effect: \n"++pretty lamEff2++"\nresult symbol: \n"++pretty chSym2) $ return $ Just (lamEff', chSym')
      -- For debugging
      return $ Just (lamEff', chSym')

    (Symbol _ PGlobal, [ch])  -> applyLambda ch sArg

    -- Glue a pscope onto a released effect
    (Symbol _ (PScope ss cl), [ch]) -> do
      ch' <- applyLambda ch sArg
      case ch' of
        Nothing     -> return Nothing
        Just (e, s) -> do
          e' <- genEff $ scope ss cl [e]
          return $ Just (e', s)

    -- For a set 'lambda', we need to combine results
    (Symbol _ PSet, ch)      -> do
      xs <- mapM (`applyLambda` sArg) ch
      let (es, ss) = unzip $ catMaybes xs
          (es', ss') = (map Just es, map Just ss)
      sSet <- combineSymSet ss'
      eSet <- combineEffSet es'
      return $ Just (fromMaybe (error "applyLambda: 1st") eSet, fromMaybe (error "applyLambda: 2nd") sSet)

    _ -> return Nothing

-- Substitute a symbol for another in a symbol: old, new, symbol in which to replace
-- NOTE: We assume the effects and symbols here don't need the environment
subSym :: Maybe (K3 Symbol, K3 Symbol) -> K3 Symbol -> MEnv (K3 Symbol)
subSym (Just (s, s')) n@(tag -> Symbol _ PVar) | n `symEqual` s = return $ replaceCh n [s']
-- Apply: recurse (we already substituted into the children)
subSym _ n@(tnc -> (Symbol _ PApply, [sL, sA])) = do
    x <- applyLambda sL sA
    return $ maybe n snd x
subSym _ n = return n

-- Substitute one symbol for another in an effect
-- mapSym already handled sL and sA
-- NOTE: We assume the effects and symbols here don't need the environment
subEff :: Maybe (K3 Symbol, K3 Symbol) -> K3 Effect -> MEnv (K3 Effect)
subEff _ n@(tag -> FApply sL sA) = do
    x <- applyLambda sL sA
    return $ maybe n fst x
subEff _ n = return n

-- Query whether certain symbols are read, written, applied
symRWAQuery :: K3 Effect -> [K3 Symbol] -> EffectEnv -> ClosureInfo
symRWAQuery eff syms env = flip evalState env $ do
  clearBindsM
  -- Use the symbols as a bind environment
  mapM_ addToEnv syms
  -- Substitute any lambdas inside
  eff' <- mapEff (subEff Nothing) mId eff
  -- Get the general closure
  createClosure False (Just eff') Nothing
  where
    -- For superstructure, we add parents
    addToEnv s' = do
      s <- expandSymM s'
      case tnc s of
        (Symbol i (PRecord _),  ch) -> insertBindM i s' >> mapM_ addToEnv ch
        (Symbol i (PTuple _),   ch) -> insertBindM i s' >> mapM_ addToEnv ch
        (Symbol i (PProject _), ch) -> insertBindM i s' >> mapM_ addToEnv ch
        -- Otherwise, we just add the individual symbol
        (Symbol i _, _)             -> insertBindM i s'
        (SymId _, _)                -> error "unexpected symId2"

