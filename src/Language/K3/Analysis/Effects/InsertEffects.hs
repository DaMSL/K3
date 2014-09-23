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
--  TODO: switch symbol gensym to use monad

module Language.K3.Analysis.Effects.InsertEffects (
  runAnalysis
)
where

import Prelude hiding (read, seq)
import Control.Arrow ( (&&&), second )
import Control.Monad.State.Lazy
import Data.Maybe
import Data.Map(Map)
import qualified Data.Map as Map
import Data.Foldable hiding (mapM_, any, concatMap, concat)

import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Declaration
import Language.K3.Core.Annotation

import Language.K3.Analysis.Common
import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.Constructors

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
insertGlobal id s env = env {globalEnv=Map.insert id s $ globalEnv env}

getId :: Env -> (Int, Env)
getId env = (count env, env {count = 1 + count env})

insertBind :: Identifier -> K3 Symbol -> Env -> Env
insertBind i s env =
  env {bindEnv=Map.insertWith (++) i [s] $ bindEnv env}

deleteBind :: Identifier -> Env -> Env
deleteBind id env =
  let m  = bindEnv env
      m' = case Map.lookup id m of
             Just []     -> Map.delete id m
             Just [_]    -> Map.delete id m
             Just (_:xs) -> Map.insert id xs m
             Nothing     -> m
  in
  env {bindEnv=m'}

globalSym :: Identifier -> K3 Symbol
globalSym id = symbol id PGlobal

-- Lookup either in the bind environment or the global environment
lookupBindInner :: Identifier -> Env -> Maybe (K3 Symbol)
lookupBindInner i env =
  case Map.lookup i $ bindEnv env of
    Nothing -> Map.lookup i $ globalEnv env
    s       -> liftM head s

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
addSID s = do
  i <- getIdM
  return $ s @+ SID i

getUID :: K3 Expression -> UID
getUID n = maybe (error "No UID found") extract $ n @~ isEUID
  where extract (EUID uid) = uid
        extract _          = error "unexpected"

getSID :: K3 Symbol -> Int
getSID sym = maybe (error "no SID found") extract $ sym @~ isSID
  where extract (SID i) = i
        extract _       = error "symbol id not found!"

getFID :: K3 Effect -> Int
getFID sym = maybe (error "no FID found") extract $ sym @~ isFID
  where extract (FID i) = i
        extract _       = error "effect id not found!"

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

genSymTemp :: [K3 Symbol] -> MEnv (K3 Symbol)
genSymTemp = genSym PTemporary

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
                     Nothing -> genSymTemp [] >>= return
                     Just i  -> return i

addAllGlobals :: K3 Declaration -> MEnv (K3 Declaration)
addAllGlobals n = mapProgram preHandleDecl mId mId n
  where
    -- add everything to global environment for cyclic/recursive scope
    -- we'll fix it up the second time through
    addGenId id = genSymTemp [] >>= insertGlobalM id >> return n
    preHandleDecl n@(tag -> DGlobal id _ _)  = addGenId id
    preHandleDecl n@(tag -> DTrigger id _ _) = addGenId id
    preHandleDecl n = return n

mId :: Monad m => a -> m a
mId x = return x

runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis prog = flip evalState startEnv $
  -- handle recursive/cyclic scope
  addAllGlobals prog >>=
  -- actual modification of AST
  mapProgram handleDecl mId handleExprs
  where
    listOfMaybe m = maybe [] singleton m

    -- TODO: handle recursive scope
    -- external functions
    handleDecl :: K3 Declaration -> MEnv (K3 Declaration)

    handleDecl n@(tag -> DGlobal id _ Nothing) =
      case n @~ isDSymbol of
        Nothing          -> return n
        Just (DSymbol s) -> insertGlobalM id s >> return n

    -- global functions
    handleDecl n@(tag -> DGlobal id _ (Just e)) =
      case e @~ isESymbol of
        Nothing          -> return n
        Just (ESymbol s) -> insertGlobalM id s >> return (n @+ DSymbol s)

    -- triggers
    handleDecl n@(tag -> DTrigger id _ e) =
      case e @~ isESymbol of
        Nothing          -> return n
        Just (ESymbol s) -> insertGlobalM id s >> return (n @+ DSymbol s)

    handleDecl n = return n

    handleExprs :: K3 Expression -> MEnv (K3 Expression)
    handleExprs n = mapIn1RebuildTree pre sideways handleExpr n

    extractBindData (BIndirection i) = [(i, PIndirection)]
    extractBindData (BTuple ids)     = zip ids [PTuple j | j <- [0..fromIntegral $ length ids - 1]]
    extractBindData (BRecord ijs)    = map (\(i, j) -> (j, PRecord i)) ijs

    doNothing = return ()

    doNothings n = return $ replicate n doNothing

    pre :: K3 Expression -> K3 Expression -> MEnv ()
    pre _ n@(tag -> ELambda i) =
      -- Add to the environment
      insertBindM i $ symbol i PVar

    pre _ _ = doNothing

    sideways :: K3 Expression -> K3 Expression -> MEnv [MEnv ()]

    -- We take the first child's symbol and bind to it
    sideways ch1 n@(tag -> ELetIn i) = do
      chSym <- getOrGenSymbol ch1
      s     <- symbolM i PLet [chSym]
      return [insertBindM i s]

    -- We take the first child's symbol and bind to it
    sideways ch1 n@(tag -> EBindAs b) = do
      chSym <- getOrGenSymbol ch1
      let iProvs = extractBindData b
      syms <- mapM (\(i, prov) -> liftM (i,) $ symbolM i prov [chSym]) iProvs
      return [mapM_ (uncurry insertBindM) syms]

    -- We take the first child's symbol and bind to it
    sideways ch1 n@(tag -> ECaseOf i) = do
      chSym <- getOrGenSymbol ch1
      s     <- symbolM i PLet [chSym]
      return [insertBindM i s, deleteBindM i]

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
      nEff   <- combineEffSeq (getEEffect e) $ Just w
      return $ addEffSymCh nEff Nothing ch n

    -- For ifThenElse be pessimistic: include effects and symbols of both paths
    handleExpr ch@[p,t,f] n@(tag -> EIfThenElse) = do
      tfEff <- combineEffSet (getEEffect t) (getEEffect f)
      -- combineEff with predicate effects
      nEff  <- combineEffSeq (getEEffect p) tfEff
      -- combineEff path symbols into a new symbol
      nSym  <- combineSymSet (getESymbol t) (getESymbol f)
      return $ addEffSymCh nEff nSym ch n

    -- ELambda wraps up the child effect and sticks it in a symbol, but has no effect per se
    -- A new scope will be created at application
    handleExpr ch@[e] n@(tag -> ELambda i) = do
      deleteBindM i
      -- Create a gensym for the lambda, containing the effects of the child, and leading to the symbols
      let eSym = maybe [] singleton $ getESymbol e
      lSym    <- genSym (PLambda i $ getEEffect e) eSym
      return $ addEffSymCh Nothing (Just lSym) ch n

    -- On application, Apply creates a scope and substitutes into it
    -- We only create the effect of apply here
    handleExpr ch@[l,a] n@(tag -> EOperate(OApp)) = do
      seqE    <- combineEffSeq (getEEffect l) $ getEEffect a
      -- Create the effect of application
      aSym    <- getOrGenSymbol a
      appE    <- addFID $ apply (forceGetSymbol l) aSym
      fullEff <- combineEffSeq seqE $ Just appE
      fullSym <- combineSymApply (Just $ forceGetSymbol l) (Just aSym)
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
      bScope  <- addFID $ scope bindSyms eEff
      fullEff <- combineEffSeq (getEEffect bind) $ Just bScope
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- CaseOf
    handleExpr ch@[e,some,none] n@(tag -> ECaseOf i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      -- Wrap some in a scope
      let someEff = listOfMaybe $ getEEffect some
      scopeEff <- addFID $ scope [bindSym] someEff
      -- Conservative approximation
      setEff   <- combineEffSet (getEEffect none) (Just scopeEff)
      combSym  <- combineSymSet (getESymbol some) (getESymbol none)
      -- peel off symbols until we get ones in our outer scope
      -- for case, we need to special-case, making sure that the particular symbol
      -- is always gensymed away
      fullSym  <- peelSymbol [bindSym] combSym
      fullEff  <- combineEffSeq (getEEffect e) setEff
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- LetIn
    handleExpr ch@[l,e] n@(tag -> ELetIn i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      let eEff = listOfMaybe $ getEEffect e
      scopeEff <- addFID $ scope [bindSym] eEff
      fullEff  <- combineEffSeq (getEEffect l) (Just scopeEff)
      -- peel off symbols until we get to ones in our outer scope
      fullSym  <- peelSymbol [] $ getESymbol e
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- Generic case: combineEff effects, ignore symbols
    handleExpr ch n = do
      eff <- foldrM combineEffSeq Nothing $ map getEEffect ch
      return $ addEffSymCh eff Nothing ch n

    ------ Utilities ------
    noEffectErr = error "Expected an effect but got none"

    -- combineEff 2 effects if they're present. Otherwise keep whatever we have
    combineEff :: ([K3 Effect] -> K3 Effect) -> Maybe (K3 Effect) -> Maybe (K3 Effect) -> MEnv (Maybe (K3 Effect))
    combineEff constF (Just e1) (Just e2) = do
      i <- getIdM
      return $ Just ((constF [e1, e2]) @+ FID i)
    combineEff _ (Just e) _               = return $ Just e
    combineEff _ _ (Just e)               = return $ Just e
    combineEff _ _ _                      = return Nothing

    combineEffSet = combineEff set

    combineEffSeq :: Maybe (K3 Effect) -> Maybe (K3 Effect) -> MEnv (Maybe (K3 Effect))
    combineEffSeq = combineEff seqF
      where seqF [x,y] = seq x y
            seqF _     = error "Bad input to seqF"

    -- combineEff 2 symbols into 1 set symbol (if needed)
    -- otherwise just use one of the symbols/don't generate anything
    combineSym :: Provenance -> Maybe (K3 Symbol) -> Maybe (K3 Symbol) -> MEnv (Maybe (K3 Symbol))
    combineSym p (Just s) (Just s') = liftM Just $ genSym p [s, s']
    combineSym _ (Just s) _         = return $ Just s
    combineSym _ _ (Just s)         = return $ Just s
    combineSym _ _ _                = return Nothing

    combineSymSet   = combineSym PSet
    combineSymApply = combineSym PApply

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
    peelSymbol _ Nothing = genSymTemp []
    peelSymbol excludes (Just sym) = loop sym >>= symOfSymList True
      where
        loop :: K3 Symbol -> MEnv [K3 Symbol]
        -- Apply's demand that we check their children and include the apply instead
        loop n@(tag &&& children -> (Symbol i PApply, [lam, arg])) = do
          lS <- loop lam
          lA <- loop arg
          case (lS, lA) of
            -- If both arguments are local, elide the Apply
            ([], []) -> return []
            -- Otherwise, keep the apply and it's possible tree
            (xs, ys) -> do
              s  <- symOfSymList False xs
              s' <- symOfSymList False ys
              return $ [replaceCh n [s, s']]

        loop n@(tag &&& children -> (Symbol i prov, ch)) =
          -- Check for exclusion
          if any (n `symEqual`) excludes then return []
          else do
            env <- get
            case lookupBindInner i env of
              Nothing -> mapM loop ch >>= return . concat
              -- If we find a match, report back
              -- Just make sure it's really equivalent
              Just s  -> if symEqual s n then return [n] else
                         mapM loop ch >>= return . concat


    -- Convert a list of symbols to a symbol (if possible)
    symOfSymList :: Bool -> [K3 Symbol] -> MEnv (K3 Symbol)
    symOfSymList _ []            = genSymTemp []
    symOfSymList alwaysGen [sym] =
      if alwaysGen then genSymTemp [sym] else return sym
    symOfSymList _ syms          = genSym PSet syms

    symEqual :: K3 Symbol -> K3 Symbol -> Bool
    symEqual s s' = getSID s == getSID s'

---- Utilities to work with effects dynamically

-- Build a global environment for using state monad functions dynamically
buildEnv :: K3 Declaration -> Env
buildEnv n = snd $ flip runState startEnv $
               addAllGlobals n >>
               mapProgram handleDecl mId mId n
  where
    handleDecl n@(tag -> DGlobal i _ _)  = possibleInsert n i
    handleDecl n@(tag -> DTrigger i _ _) = possibleInsert n i

    possibleInsert n i =
      case n @~ isDSymbol of
        Nothing          -> return n
        Just (DSymbol s) -> insertGlobalM i s >> return n

