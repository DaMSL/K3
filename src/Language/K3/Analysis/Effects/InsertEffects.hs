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
  runAnalysis
)
where

import Prelude hiding (read, seq)
import Control.Arrow ( (&&&), first, second )
import Control.Monad.State.Lazy
import Data.Maybe
import Data.Map(Map)
import Data.List(nub)
import qualified Data.Map as Map
import Data.Foldable hiding (mapM_, any, concatMap, concat)

import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Declaration
import Language.K3.Core.Annotation
import Language.K3.Core.Type

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

listOfMaybe :: Maybe a -> [a]
listOfMaybe (Just x) = [x]
listOfMaybe Nothing  = []

maybeOfList []  = Nothing
maybeOfList [x] = Just x
maybeOfList _   = error "unexpected"

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
createClosure n = foldTree addClosure ([],[],[]) n >>=
                  \(a,b,c) -> return (nub a, nub b, nub c)
  where
    nubtuple (a,b,c) = return (nub a, nub b, nub c)

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
    getClosureSyms s@(tnc -> (Symbol i _, ch)) = do
      x <- lookupBindInnerM i
      case x of
        Nothing ->
          -- if we haven't found a match, it might be deeper in the tree
          liftM concat $ mapM getClosureSyms ch

        Just s' | s `symEqual` s' -> return [s']

    handleApply :: ClosureInfo -> K3 Symbol -> MEnv ClosureInfo
    handleApply acc (tag -> Symbol i (PLambda _ eff)) = foldTree addClosure acc eff


addAllGlobals :: K3 Declaration -> MEnv (K3 Declaration)
addAllGlobals n = mapProgram preHandleDecl mId mId n
  where
    -- add everything to global environment for cyclic/recursive scope
    -- we'll fix it up the second time through
    addGenId id = symbolM id (PTemporary TUnbound) [] >>= insertGlobalM id

    preHandleDecl n@(tag -> DGlobal id _ _)  = addGenId id >> return n
    preHandleDecl n@(tag -> DTrigger id _ _) = addGenId id >> return n
    preHandleDecl n = return n

mId :: Monad m => a -> m a
mId x = return x

symEqual :: K3 Symbol -> K3 Symbol -> Bool
symEqual (getSID -> s) (getSID -> s') = s == s'

runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis prog = flip evalState startEnv $
  -- for cyclic scope, add temporaries for all globals
  addAllGlobals prog >>=
  -- actual modification of AST (no need to decorate declarations here)
  mapProgram mId mId handleExprs >>=
  -- fix up any globals that couldn't be looked up due to cyclic scope
  mapProgram handleDecl mId fixUpExprs

  where
    listOfMaybe m = maybe [] singleton m

    -- Add all globals and decorate tree
    handleDecl :: K3 Declaration -> MEnv (K3 Declaration)
    handleDecl n =
      case tag n of
        DGlobal i _ Nothing  -> addSym i []
        DGlobal i _ (Just e) -> addE i e
        DTrigger i _ e       -> addE i e
        _                     -> return n
      where
        addE i e = case e @~ isESymbol of
                     Nothing           -> addSym i []
                     Just (ESymbol s)  -> addSym i [s]

        addSym i ss = do
          sym <- symbolM i PGlobal ss
          insertGlobalM i sym
          return (n @+ DSymbol sym)

    handleExprs :: K3 Expression -> MEnv (K3 Expression)
    handleExprs n = mapIn1RebuildTree pre sideways handleExpr n

    extractBindData (BIndirection i) = [(i, PIndirection)]
    extractBindData (BTuple ids)     = zip ids [PTuple j | j <- [0..fromIntegral $ length ids - 1]]
    extractBindData (BRecord ijs)    = map (\(i, j) -> (j, PRecord i)) ijs

    doNothing = return ()

    doNothings n = return $ replicate n doNothing

    pre :: K3 Expression -> K3 Expression -> MEnv ()
    pre _ n@(tag -> ELambda i) = do
      -- Add to the environment
      sym <- symbolM i PVar []
      insertBindM i sym

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
          eSym = listOfMaybe $ getESymbol e
      -- Create a closure for the lambda by finding every read/written/applied closure variable
      closure <- case eEff of
                   Nothing -> return emptyClosure
                   Just e  -> createClosure e
      -- Create a gensym for the lambda, containing the effects of the child, and leading to the symbols
      eScope  <- addFID $ scope [bindSym] closure $ listOfMaybe eEff
      lSym    <- genSym (PLambda i eScope) eSym
      return $ addEffSymCh Nothing (Just lSym) ch n

    -- For collection attributes, we need to create and apply a lambda
    -- containing 'self'
    -- NOTE: We assume that the effect for this function has been inserted locally
    --       on the project

      -- Check in the type system for a function in a collection
      case (e @~ isEType, n @~ isEType) of
        (Just (EType(tag -> TCollection)), Just (EType(tag -> TFunction))) ->
          -- We can't have effects here -- we only have symbols
          case getESymbol n of
            Nothing   -> error $ "Missing symbol for projection " ++ i
            Just nSym -> do
              eSym    <- getOrGenSymbol e
              selfSym <- symbolM "self" PVar []
              scope'  <- addFID $ scope [selfSym] emptyClosure []
              sLam    <- genSym (PLambda "self" scope') [nSym]
              sApp    <- genSym PApply [sLam, eSym]
              return $ addEffSymCh Nothing (Just sApp) ch n

        _ -> genericExpr ch n  -- not a collection member function

    -- On application, Apply creates a scope and substitutes into it
    -- We only create the effect of apply here
    handleExpr ch@[l,a] n@(tag -> EOperate OApp) = do
      seqE    <- combineEffSeq [getEEffect l, getEEffect a]
      -- Create the effect of application
      aSym    <- getOrGenSymbol a
      appE    <- addFID $ apply (forceGetSymbol l) aSym
      fullEff <- combineEffSeq [seqE, Just appE]
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
      bScope  <- addFID $ scope bindSyms emptyClosure eEff
      fullEff <- combineEffSeq [getEEffect bind, Just bScope]
      return $ addEffSymCh fullEff (Just fullSym) ch n

    -- CaseOf
    handleExpr ch@[e,some,none] n@(tag -> ECaseOf i) = do
      bindSym <- lookupBindM i
      deleteBindM i -- remove bind from env
      -- Wrap some in a scope
      let someEff = listOfMaybe $ getEEffect some
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
      let eEff = listOfMaybe $ getEEffect e
      scopeEff <- addFID $ scope [bindSym] emptyClosure eEff
      fullEff  <- combineEffSeq [getEEffect l, Just scopeEff]
      -- peel off symbols until we get to ones in our outer scope
      fullSym  <- peelSymbol [] $ getESymbol e
      return $ addEffSymCh fullEff (Just fullSym) ch n

    handleExpr ch n = genericExpr ch n

    -- Generic case: combineEff effects, ignore symbols
    genericExpr ch n = do
      eff <- combineEffSeq $ map getEEffect ch
      return $ addEffSymCh eff Nothing ch n

    -- Post-processing for cyclic scope
    fixUpExprs :: K3 Expression -> MEnv (K3 Expression)
    fixUpExprs n = modifyTree fixupExpr n
      where
        fixupExpr n =
          case getESymbol n of
            Nothing -> return n
            Just s  -> do
              s' <- modifyTree fixupSym s
              return $ (n @- ESymbol s) @+ ESymbol s'

        -- Any unbound globals should be translated
        fixupSym :: K3 Symbol -> MEnv (K3 Symbol)
        fixupSym (tag -> Symbol i (PTemporary TUnbound)) = lookupBindM i
        fixupSym s = return s

    ------ Utilities ------
    noEffectErr = error "Expected an effect but got none"

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
    peelSymbol _ Nothing = genSymTemp TUnbound []
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

        loop mLast n@(tnc -> (s@(Symbol i prov), ch)) =
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

-- combineEff symbols into 1 set symbol (if needed)
-- otherwise just use one of the symbols/don't generate anything
combineSym :: Provenance -> [Maybe (K3 Symbol)] -> MEnv (Maybe (K3 Symbol))
combineSym p ss =
  case filter isJust ss of
    []  -> return Nothing
    [s] -> return s
    ss' -> liftM Just $ genSym p $ map fromJust ss'

combineSymSet :: [Maybe (K3 Symbol)] -> MEnv (Maybe (K3 Symbol))
combineSymSet   = combineSym PSet
combineSymApply :: Maybe (K3 Symbol) -> Maybe (K3 Symbol) -> MEnv (Maybe (K3 Symbol))
combineSymApply l a = combineSym PApply [l,a]


-- Build a global environment for using state monad functions dynamically
buildEnv :: K3 Declaration -> Env
buildEnv n = snd $ flip runState startEnv $
               addAllGlobals n >>
               mapProgram handleDecl mId highestExprId n
  where
    handleDecl n@(tag -> DGlobal i _ _)  = possibleInsert n i
    handleDecl n@(tag -> DTrigger i _ _) = possibleInsert n i

    possibleInsert n i = do
      highestDeclId n
      case n @~ isDSymbol of
        Nothing          -> return n
        Just (DSymbol s) -> do
          insertGlobalM i s
          return n

    highestDeclId n = do
      case n @~ isDSymbol of
        Nothing -> return ()
        Just (DSymbol s) -> highestSymId s

    highestExprId n = do
      case n @~ isESymbol of
        Nothing -> return ()
        Just (ESymbol s) -> highestSymId s
      case n @~ isEEffect of
        Nothing -> return n
        Just (EEffect e) -> highestEffId e >> return n

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


-- TODO: we need to substitute for every (effect,symbol) pair inside the lambda
-- AST, given a target id to substitute for
--
-- If the symbol is a global, substitute from the global environment

-- Apply (substitute) a symbol into a lambda symbol, generating effects and a new symbol
-- If we return Nothing, we cannot apply yet because of a missing lambda
applyLambda :: K3 Symbol -> K3 Symbol -> MEnv (Maybe (K3 Effect, K3 Symbol))
applyLambda sArg sLam =
    case tnc sLam of
      (Symbol _ (PLambda _ e@(tnc -> (FScope [sOld] _, [lamEff]))), [chSym]) -> do
        -- Substitute into the old effects and symbol
        e'      <- subEffTree sOld sArg lamEff
        chSym'  <- subSymTree sOld sArg chSym
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

    -- substitute in one symbol for another
    -- effect can only be from application of lambdas
    subSymTree :: K3 Symbol -> K3 Symbol -> K3 Symbol -> MEnv (K3 Symbol)
    subSymTree s s' n = modifyTree (subSym s s') n

    -- Substitute a symbol: old, new, symbol in which to replace
    -- Returns possible result effects and the new symbol
    subSym :: K3 Symbol -> K3 Symbol -> K3 Symbol -> MEnv (K3 Symbol)

    subSym s s' n@(tag -> Symbol _ PVar) | n `symEqual` s = return s'

    -- Apply: recurse
    subSym s s' n@(tnc -> (Symbol _ PApply, [sL, sA])) = do
      x <- applyLambda sA sL
      return $ maybe n snd x

    subSym _ _ n = return n

    -- Substitute a symbol into an effect tree
    subEffTree :: K3 Symbol -> K3 Symbol -> K3 Effect -> MEnv(K3 Effect)
    subEffTree s s' e = modifyTree (subEff s s') e

    -- TODO: occurs check (somehow)
    -- Substitute one symbol for another in an effect
    subEff :: K3 Symbol -> K3 Symbol -> K3 Effect -> MEnv (K3 Effect)
    subEff sym sym' n =
      case tag n of
        FRead s      -> sub s >>= newTag . FRead
        FWrite s     -> sub s >>= newTag . FWrite
        FScope ss x  -> do
          ss' <- mapM sub ss
          newTag $ FScope ss' x
        FApply sL sA -> do
          sA' <- sub sA
          sL' <- sub sL
          x <- applyLambda sA' sL'
          return $ maybe n fst x

        _            -> return n
      where
        newTag x = return $ replaceTag n x
        sub = subSymTree sym sym'
