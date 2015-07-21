{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ViewPatterns #-}

-- | Machinery for making decisions about C++ level materialization for K3.
module Language.K3.Codegen.CPP.Materialization where

import Prelude hiding (concat, mapM, mapM_, or, and)

import Control.Arrow

import Control.Monad.Identity (Identity(..), runIdentity)
import Control.Monad.State (StateT(..), MonadState(..), modify, gets)
import Control.Monad.Writer

import Language.K3.Analysis.Core

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.Provenance.Inference (PIEnv(..))

import qualified Language.K3.Analysis.Provenance.Constructors as P

import Language.K3.Analysis.SEffects.Core
import Language.K3.Analysis.SEffects.Inference (FIEnv(..))

import Language.K3.Codegen.CPP.Materialization.Hints

import Data.Foldable

import Data.Maybe (fromMaybe, maybeToList, fromJust)
import Data.Ord (comparing)
import Data.List (elemIndex, sortBy, tails, zip4)
import Data.Tree

import qualified Data.Map as M
import qualified Data.IntMap as I

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Common hiding (getUID)

import Text.Printf

type Table = I.IntMap (M.Map (Identifier, Maybe Int) Decision)

data MaterializationS = MaterializationS { decisionTable :: Table
                                         , pienv :: PIEnv
                                         , fienv :: FIEnv
                                         , downstreams :: [K3 Expression]
                                         , currentGlobal :: Bool
                                         -- | Identifiers currently in scope, and their bind points.
                                         , scopeMap :: M.Map Identifier Int
                                         }

-- Reporting

dumpMaterializationReport :: Bool
dumpMaterializationReport = False

data MaterializationR
  = MRLambda { currentlyGlobal :: Bool
             , argReadOnly :: Bool
             , nrvoRequired :: Bool
             , argD :: Decision
             , closureReport :: [(Provenance, Provenance, Bool, Bool, Decision)]
             , downstreamsConsidered :: [Int]
             }
  | MRApply
  | MRBind
  | MRLet
  | MRCase
  | MRSend
  | MRRecord [(Identifier, Bool, [Int], Decision)]
 deriving (Eq, Read, Show)

formatMR :: [(Int, MaterializationR)] -> IO ()
formatMR r = putStrLn "Materialization Report" >> mapM_ (uncurry formatR) r
  where
   formatR :: Int -> MaterializationR -> IO ()
   formatR u t = case t of
     MRLambda cg aro nr ad cr dc -> do
       printf "Materialized lambda at UID %d\n" u
       printf "  Lambda is part of currently global chain: %s\n" (show cg)
       printf "  Argument is read-only inside lambda: %s\n" (show aro)
       printf "  Lambda requires manual NRVO: %s\n" (show nr)
       printf "  Incoming decision for argument: %s\n" (show $ inD ad)
       printf "  Outgoing decision for argument: %s\n" (show $ outD ad)
       printf "  Downstreams considered for all decisions: %s\n" (show dc)
       unless (null cr) $ do
         printf "  Closure Variables:\n"
         forM_ cr $ \(ip, op, hw, b, d) -> do
           printf "    Variable: %s\n" $ case ip of
             PBVar (PMatVar { pmvn }) -> pmvn
             _ -> error "Incorrect Closure Provenance"
           printf "      Captured from: %d\n" $ case op of
             PBVar (PMatVar { pmvloc = (UID i) }) -> "identifier bound at " ++ show i
             PFVar _ -> "free variable in parent scope."
           printf "      Is written to inside lambda: %s\n" (show hw)
           printf "      Moveable: %s\n" (show b)
           printf "      Decision: %s\n" (show $ inD d)
     MRRecord imnds -> do
       printf "Materialized record at UID %d\n" u
       forM_ imnds $ \(i, mn, ds, d) -> do
         printf "  Field %s:\n" i
         printf "    Moveable: %s\n" (show mn)
         printf "      Downstreams considered: %s\n" (show ds)
         printf "    Decision: %s\n" (show $ inD d)

say :: Int -> MaterializationR -> MaterializationM ()
say u r = tell [(u, r)]

type MaterializationM = WriterT [(Int, MaterializationR)] (StateT MaterializationS Identity)

-- State Accessors

lookupFst :: Eq k => k -> M.Map (k, a) v -> Maybe v
lookupFst k m = case M.toList (M.filterWithKey (\(i, _) _ -> i == k) m) of
  [] -> Nothing
  [(_, v)] -> Just v
  _ -> error "Non-unique mapping."

dLookup :: Int -> Identifier -> MaterializationM Decision
dLookup u i = decisionTable <$> get >>= \t -> return $ fromMaybe defaultDecision (I.lookup u t >>= lookupFst i)

dLookupAll :: Int -> MaterializationM (M.Map Identifier Decision)
dLookupAll u = dLookupAllWithBindings u >>= \d -> return $ M.fromList [(k, v) | ((k, _), v) <- M.toList d]

dLookupAllWithBindings :: Int -> MaterializationM (M.Map (Identifier, Maybe Int) Decision)
dLookupAllWithBindings u = gets decisionTable >>= \t -> return $ I.findWithDefault M.empty u t

pLookup :: PPtr -> MaterializationM (Maybe (K3 Provenance))
pLookup p = pienv <$> get >>= \e -> return (I.lookup p (ppenv e))

-- A /very/ rough approximation of ReaderT's ~local~ for StateT.
withLocalDS :: [K3 Expression] -> MaterializationM a -> MaterializationM a
withLocalDS nds m = do
  s <- get
  put (s { downstreams = nds ++ (downstreams s)})
  r <- m
  s' <- get
  put (s' { downstreams = downstreams s})
  return r

withLocalBindings :: [Identifier] -> Int -> MaterializationM a -> MaterializationM a
withLocalBindings is u m = do
  s <- get
  put $ s { scopeMap = M.union (M.fromList $ zip is (repeat u)) (scopeMap s)}
  r <- m
  s' <- get
  put $ s' { scopeMap = scopeMap s }
  return r

getBindingUID :: Identifier -> MaterializationM (Maybe Int)
getBindingUID i = M.lookup i . scopeMap <$> get

getUID :: K3 Expression -> Int
getUID e = let EUID (UID u) = fromMaybe (error "No UID on expression.")
                        (e @~ \case { EUID _ -> True; _ -> False }) in u

getProvenance :: K3 Expression -> K3 Provenance
getProvenance e = let EProvenance p = fromMaybe (error "No provenance on expression.")
                                      (e @~ \case { EProvenance _ -> True; _ -> False}) in p


getEffects :: K3 Expression -> K3 Effect
getEffects e = let ESEffect f = fromMaybe (error "No effects on expression.")
                                (e @~ \case { ESEffect _ -> True; _ -> False }) in f

getFStructure :: K3 Expression -> K3 Effect
getFStructure e = let EFStructure f = fromMaybe (error "No effects on expression.")
                                      (e @~ \case { EFStructure _ -> True; _ -> False }) in f


setNullDecision :: Int -> Identifier -> Decision -> MaterializationM ()
setNullDecision u i d = setFullDecision u (i, Nothing) d

setDecision :: Int -> Identifier -> Decision -> MaterializationM ()
setDecision u i d = gets scopeMap >>= \sm -> case M.lookup i sm of
  Nothing -> setFullDecision u (i, Nothing) d
  Just bp -> setFullDecision u (i, Just bp) d

setFullDecision :: Int -> (Identifier, Maybe Int) -> Decision -> MaterializationM ()
setFullDecision u i d = modify $ \s -> s { decisionTable = I.insertWith M.union u (M.singleton i d) (decisionTable s)}

getClosureSymbols :: Int -> MaterializationM [Identifier]
getClosureSymbols i = (pvpenv . pienv) <$> get >>= \e -> return $ concat $ maybeToList (I.lookup i $ lcenv e)

pmvloc' :: PMatVar -> Int
pmvloc' pmv = let UID u = pmvloc pmv in u

setCurrentGlobal :: Bool -> MaterializationM ()
setCurrentGlobal b = modify $ \s -> s { currentGlobal = b }

-- Table Construction/Attachment

runMaterializationM :: MaterializationM a -> MaterializationS -> ((a, [(Int, MaterializationR)]), MaterializationS)
runMaterializationM m s = runIdentity $ runStateT (runWriterT m) s

optimizeMaterialization :: (PIEnv, FIEnv) -> K3 Declaration -> IO (K3 Declaration)
optimizeMaterialization (p, f) d = do
  let ((nd, report), _) = runMaterializationM (materializationD d) (MaterializationS I.empty p f [] True M.empty)
  when dumpMaterializationReport $ formatMR report
  return nd

materializationD :: K3 Declaration -> MaterializationM (K3 Declaration)
materializationD (Node (d :@: as) cs)
  = case d of
      DGlobal i t me -> do
        setCurrentGlobal True
        me' <- traverse materializationE me
        Node (DGlobal i t me' :@: as) <$> cs'
      DTrigger i t e -> materializationE e >>= \e' -> Node (DTrigger i t e' :@: as) <$> cs'
      DRole i -> Node (DRole i :@: as) <$> cs'
      _ -> Node (d :@: as) <$> cs'
 where
   cs' = mapM materializationD cs

materializationE :: K3 Expression -> MaterializationM (K3 Expression)
materializationE e@(Node (t :@: as) cs)
  = case t of
      ERecord is -> do
        let decisionForField c ds = withLocalDS ds $ do
              rf <- materializationE c
              mn <- isMoveableNow (getProvenance c)
              return (if mn then defaultDecision { inD = Moved } else defaultDecision, rf, mn)
        let (is', cs') = unzip . sortBy (comparing fst) $ zip is cs
        (decisions, fs, mns) <- unzip3 <$> zipWithM decisionForField (reverse cs') (reverse $ tail $ tails cs')
        zipWithM_ (setDecision (getUID e)) is' decisions
        ds <- dLookupAll (getUID e)
        let fs' = map (\i -> fs !! (fromJust $ elemIndex i $ reverse is')) is
        eds <- downstreams <$> get
        say (getUID e) $ MRRecord $ zip4 is' mns (map (map getUID . (eds ++)) $ tail $ tails cs') decisions
        return (Node (t :@: (EMaterialization ds:as)) fs')

      EOperate OApp -> do
        let [f, x] = cs
        x' <- materializationE x
        f' <- withLocalDS [x] $ materializationE f

        let applicationEffects = getFStructure e
        let executionEffects = getEffects e
        let formalParameter =
              case applicationEffects of
                (tag -> FApply (Just fmv)) -> fmv
                _ -> error "Invalid effect structure"

        conservativeDoMoveLocal <- hasWriteInIF (fmvn formalParameter) executionEffects

        conservativeDoMoveReturn <-
          case f' of
            (tag &&& children -> (ELambda i, [f''])) ->
              case f'' of
                (tag -> ELambda _) -> do
                  let f''id = getUID f''
                  f''d <- dLookup f''id i
                  return $ inD f''d == Moved
                _ -> return False
            _ -> return False

        moveable <- isMoveableNow (getProvenance x)

        let applicationDecision d =
              if (conservativeDoMoveLocal || conservativeDoMoveReturn) && moveable
                then d { inD = Moved }
                else d

        setDecision (getUID e) "" $ applicationDecision defaultDecision

        decisions <- dLookupAll (getUID e)

        return (Node (t :@: (EMaterialization decisions:as)) [f', x'])

      EOperate OSnd -> do
        let [h, m] = cs
        m' <- materializationE m
        h' <- withLocalDS [m] $ materializationE h

        moveable <- isMoveableNow (getProvenance m')
        let decision = if moveable then defaultDecision { inD = Moved } else defaultDecision
        setDecision (getUID e) "" decision
        ds <- dLookupAll (getUID e)

        return (Node (t :@: (EMaterialization ds:as)) [h', m'])

      EOperate _ -> do
        case cs of
          [x] -> do
            x' <- materializationE x
            return $ (Node (t :@: as)) [x']
          [x, y] -> do
            y' <- materializationE y
            x' <- withLocalDS [y] $ materializationE x
            return $ (Node (t :@: as)) [x', y']
          _ -> error "Invalid argument form for operator."

      ELambda x -> do
        cg <- currentGlobal <$> get
        let fp = getProvenance e
        let closureSymbols = case tag fp of
              PLambda _ mvs -> mvs
              _ -> error "Invalid provenance on lambda form."

        when cg $ case tag (head cs) of
                    ELambda _ -> return ()
                    _ -> setCurrentGlobal False

        [b] <- withLocalBindings (x : map pmvn closureSymbols) (getUID e) $ mapM materializationE cs

        setCurrentGlobal cg

        let returnedProvenance =
              case getProvenance e of
                (tag &&& children -> (PLambda _ _, [rp])) -> rp
                _ -> error "Invalid provenance structure"

        readOnly <- not <$> hasWriteInP (P.pfvar x) b

        let nrvoProvenance q =
              case q of
                (tag -> PFVar _) -> return True
                (tag -> PBVar _) -> not <$> isGlobalP q
                (tag -> PSet) -> anyM nrvoProvenance (children q)
                _ -> return False

        nrvo <- nrvoProvenance returnedProvenance

        let readOnlyDecision d = if readOnly then d { inD = ConstReferenced } else d
        let nrvoDecision d = if nrvo then d { outD = Moved } else d

        setDecision (getUID e) x $ readOnlyDecision $ nrvoDecision $ defaultDecision

        cr <- forM closureSymbols $ \s -> do
          let innerProxyProvenance = P.pbvar s
          outerProxyProvenance <- fromMaybe (error "Dangling closure provenance.") <$> pLookup (pmvptr s)
          closureHasWrite <- hasWriteInP innerProxyProvenance b
          moveable <- isMoveableNow outerProxyProvenance
          let closureDecision d =
                if closureHasWrite
                  then if moveable
                         then d { inD = Moved }
                         else d { inD = Copied }
                  else if cg
                         then d { inD = Moved }
                         else d { inD = Referenced }

          let decisionSetter = case tag outerProxyProvenance of
                PBVar _ -> setDecision
                PFVar _ -> setNullDecision

          decisionSetter (getUID e) (pmvn s) $ closureDecision defaultDecision

          return ( tag innerProxyProvenance
                 , tag outerProxyProvenance
                 , closureHasWrite
                 , moveable
                 , closureDecision defaultDecision
                 )
        decisions <- dLookupAll (getUID e)

        eds <- map getUID . downstreams <$> get
        say (getUID e) $ MRLambda cg readOnly nrvo (M.findWithDefault defaultDecision x decisions) cr eds
        return $ (Node (t :@: (EMaterialization decisions:as)) [b])

      EBindAs b -> do
        let [x, y] = cs
        let newBindings = case b of { BIndirection i -> [i]; BTuple is -> is; BRecord iis -> snd (unzip iis) }
        y' <- withLocalBindings newBindings (getUID e) $ materializationE y
        x' <- withLocalDS [y] (materializationE x)

        let xp = getProvenance x
        writeMention <- hasWriteInP xp y'

        let referenceBind d = if not writeMention then d { inD = Referenced, outD = Referenced } else d

        case b of
          BIndirection i -> setDecision (getUID e) i $ referenceBind defaultDecision
          BTuple is -> mapM_ (\i -> setDecision (getUID e) i $ referenceBind defaultDecision) is
          BRecord iis -> mapM_ (\(_, i) -> setDecision (getUID e) i $ referenceBind defaultDecision) iis

        decisions <- dLookupAll (getUID e)
        return (Node (t :@: (EMaterialization decisions:as)) [x', y'])

      ECaseOf i -> do
        let [x, s, n] = cs
        n' <- materializationE n
        s' <- withLocalBindings [i] (getUID e) $ materializationE s
        x' <- withLocalDS [s, n] (materializationE x)

        let xp = getProvenance x

        -- TODO: Slightly conservative, although it takes reasonably unusual code to trigger
        -- those cases.
        noMention <- do
          sMention <- (||) <$> hasReadInP xp s' <*> hasWriteInP xp s'
          nMention <- (||) <$> hasReadInP xp n' <*> hasWriteInP xp n'

          return $ not (sMention || nMention)

        let referenceBind d = if noMention then d { inD = Referenced, outD = Referenced } else d

        setDecision (getUID e) i $ referenceBind defaultDecision
        decisions <- dLookupAll (getUID e)
        return (Node (t :@: (EMaterialization decisions:as)) [x', s', n'])

      ELetIn i -> do
        let [x, b] = cs
        b' <- withLocalBindings [i] (getUID e) $ materializationE b
        (x', d) <- withLocalDS [b] $ do
          x'' <- materializationE x
          m <- isMoveableNow (getProvenance x'')
          return (x'', if m then defaultDecision { inD = Moved } else defaultDecision)

        setDecision (getUID e) i d
        decisions <- dLookupAll (getUID e)
        return (Node (t :@: (EMaterialization decisions:as)) [x', b'])

      _ -> (Node (t :@: as)) <$> mapM materializationE cs

-- Queries

anyM :: (Functor m, Applicative m, Monad m) => (a -> m Bool) -> [a] -> m Bool
anyM f xs = or <$> mapM f xs

allM :: (Functor m, Applicative m, Monad m) => (a -> m Bool) -> [a] -> m Bool
allM f xs = and <$> mapM f xs

-- Determine if a piece of provenance 'occurs in' another. The answer can be influenced by 'width
-- flag', determining whether or not the provenance of superstructure occurs in the provenance of
-- its substructure.
occursIn :: Bool -> K3 Provenance -> K3 Provenance -> MaterializationM Bool
occursIn wide a b
  = case tag b of

      -- Everything occurs in itself.
      _ | a =*= b -> return True

      -- Something occurs in a bound variable if it occurs in anything that was used to initialize
      -- that bound variable, and that bound variable was initialized using a non-isolating method.
      PBVar mv -> do
             decision <- dLookup (pmvloc' mv) (pmvn mv)
             if inD decision == Referenced || inD decision == ConstReferenced
               then pLookup (pmvptr mv) >>=
                    maybe (error "Attempted to occurs-check a dangling provenance pointer.") (occursIn wide a)
               else return False

      -- Something occurs in substructure if it occurs in any superstructure, and wide effects are
      -- set.
      POption | wide -> anyM (occursIn wide a) (children b)
      PIndirection | wide -> anyM (occursIn wide a) (children b)
      PTuple _ | wide -> anyM (occursIn wide a) (children b)
      PProject _ | wide -> anyM (occursIn wide a) (children b)
      PRecord _ | wide -> anyM (occursIn wide a) (children b)

      -- TODO: Add more intelligent handling of substructure + PData combinations.

      _ -> return False

isReadIn :: K3 Provenance -> K3 Expression -> MaterializationM Bool
isReadIn x f = isReadInF x (getEffects f)

isReadInF :: K3 Provenance -> K3 Effect -> MaterializationM Bool
isReadInF xp ff =
  case ff of
    (tag -> FRead yp) -> occursIn False xp yp

    (tag -> FScope _) -> anyM (isReadInF xp) (children ff)
    (tag -> FSeq) -> anyM (isReadInF xp) (children ff)
    (tag -> FSet) -> anyM (isReadInF xp) (children ff)

    _ -> return False

isWrittenIn :: K3 Provenance -> K3 Expression -> MaterializationM Bool
isWrittenIn x f = isWrittenInF x (getEffects f)

isWrittenInF :: K3 Provenance -> K3 Effect -> MaterializationM Bool
isWrittenInF xp ff =
  case ff of
    (tag -> FWrite yp) -> occursIn True xp yp

    (tag -> FScope _) -> anyM (isWrittenInF xp) (children ff)
    (tag -> FSeq) -> anyM (isWrittenInF xp) (children ff)
    (tag -> FSet) -> anyM (isWrittenInF xp) (children ff)

    _ -> return False

hasWriteInIF :: Identifier -> K3 Effect -> MaterializationM Bool
hasWriteInIF ident effect =
  case effect of
    (tag -> FWrite (tag -> PFVar i)) | i == ident -> return True
    (tag -> FWrite (tag -> PBVar m)) | pmvn m == ident -> return True

    (tag -> FScope _) -> anyM (hasWriteInIF ident) (children effect)
    (tag -> FSeq) -> anyM (hasWriteInIF ident) (children effect)
    (tag -> FSet) -> anyM (hasWriteInIF ident) (children effect)

    _ -> return False

hasWriteInI :: Identifier -> K3 Expression -> MaterializationM Bool
hasWriteInI ident expr =
  case expr of
    (tag -> ELambda i) | i == ident -> return False
    (tag &&& children -> (ELambda _, [body])) -> do
       lambdaDecisions <- dLookupAll (getUID expr)
       case M.lookup ident lambdaDecisions of
         Nothing -> return False
         Just cd ->
           case inD cd of
             ConstReferenced -> return False
             Referenced -> hasWriteInI ident body
             Moved -> return True
             Copied -> return False

    (tag &&& children -> (ELetIn i, [e, _])) | i == ident -> hasWriteInI ident e
    (tag &&& children -> (ELetIn _, [e, b])) -> do
      eHasWriteInI <- hasWriteInI ident e
      bHasWriteInI <- hasWriteInI ident b
      (||) <$> hasWriteInI ident e <*> hasWriteInI ident b

    -- TODO: Other shadow cases.

    _ -> do
      moveDecisions <- dLookupAll (getUID expr)
      let localHasWriteByMove = maybe False (\d -> inD d == Moved) (M.lookup ident moveDecisions)
      localHasWrite <- hasWriteInIF ident (getEffects expr)
      childHasWrite <- anyM (hasWriteInI ident) (children expr)
      return (localHasWriteByMove || localHasWrite || childHasWrite)

pBindInfo :: K3 Provenance -> Maybe (Identifier, Maybe Int)
pBindInfo p =
  case p of
    (tag -> PFVar j) -> Just (j, Nothing)
    (tag -> PBVar (PMatVar { pmvn = n, pmvloc = (UID u) })) -> Just (n, Just u)
    _ -> Nothing

hasWriteInP :: K3 Provenance -> K3 Expression -> MaterializationM Bool
hasWriteInP prov expr =
  case expr of
    (tag &&& children -> (ELambda _, [b])) -> do
      closureDecisions <- dLookupAllWithBindings (getUID expr)
      let writeInClosure = fromMaybe False $ do
            (j, mb) <- pBindInfo prov
            d <- M.lookup (j, mb) closureDecisions
            return $ inD d == Moved
      childHasWrite <- hasWriteInP prov b
      return (writeInClosure || childHasWrite)

    (tag &&& children -> (EOperate OApp, [f, x])) -> do
      let argProv = getProvenance x
      argOccurs <- occursIn True prov argProv
      appDecision <- dLookup (getUID expr) ""

      functionHasWrite <- hasWriteInP prov f
      argHasWrite <- hasWriteInP prov x
      let appHasWrite = inD appDecision == Moved && argOccurs

      appHasIntrinsicWrite <- isWrittenInF prov (getEffects expr)

      return (functionHasWrite || argHasWrite || appHasWrite || appHasIntrinsicWrite)

    (tag &&& children -> (EOperate OSnd, [_, x])) -> do
      let messageProv = getProvenance x
      messageOccurs <- occursIn True prov messageProv
      sendDecision <- dLookup (getUID expr) ""
      messageHasWrite <- hasWriteInP prov x
      let sendHasWrite = inD sendDecision == Moved && messageOccurs
      return (messageHasWrite || sendHasWrite)

    (tag &&& children -> (ERecord is, cs)) -> do
      childrenHaveWrite <- anyM (hasWriteInP prov) cs

      moveDecisions <- dLookupAll (getUID expr)
      let f i c = do
            let currentDecision = M.findWithDefault defaultDecision i moveDecisions
            if inD currentDecision == Moved
               then occursIn True prov (getProvenance c)
               else return False
      constructorsHaveMoveWrite <- or <$> zipWithM f is cs
      return (constructorsHaveMoveWrite || childrenHaveWrite)

    _ -> do
      genericHasWrite <- isWrittenInF prov (getEffects expr)
      childHasWrite <- anyM (hasWriteInP prov) (children expr)
      return (genericHasWrite || childHasWrite)

hasReadInP :: K3 Provenance -> K3 Expression -> MaterializationM Bool
hasReadInP prov expr =
  case expr of
    (tag &&& children -> (ELambda _, [b])) -> do
      closureDecisions <- dLookupAllWithBindings (getUID expr)
      let readInClosure = fromMaybe False $ do
            (j, mb) <- pBindInfo prov
            d <- M.lookup (j, mb) closureDecisions
            return $ inD d == Copied || inD d == Moved

      childHasRead <- hasReadInP prov b
      return (readInClosure || childHasRead)

    (tag &&& children -> (EOperate OApp, [f, x])) -> do
      let argProv = getProvenance x
      argOccurs <- occursIn True prov argProv
      appDecision <- dLookup (getUID expr) ""

      functionHasRead <- hasReadInP prov f
      argHasRead <- hasReadInP prov x
      let appHasRead = inD appDecision == Moved && argOccurs

      return (functionHasRead || argHasRead || appHasRead)

    _ -> (||) <$> isReadInF prov (getEffects expr) <*> anyM (hasReadInP prov) (children expr)

isGlobalP :: K3 Provenance -> MaterializationM Bool
isGlobalP ep =
  case ep of
    (tag -> PGlobal _) -> return True
    (tag -> PBVar pmv) -> pLookup (pmvptr pmv) >>= maybe (return False) isGlobalP

    (tag &&& children -> (PProject _, [pp])) -> isGlobalP pp

    _ -> return False

isMoveable :: K3 Provenance -> MaterializationM Bool
isMoveable p = case tag p of
                 PLambda _ _ -> return False
                 _ -> not <$> isGlobalP p

isMoveableIn :: K3 Provenance -> K3 Expression -> MaterializationM Bool
isMoveableIn x c = do
  isRead <- hasReadInP x c
  isWritten <- hasWriteInP x c
  return $ not (isRead || isWritten)

isMoveableNow :: K3 Provenance -> MaterializationM Bool
isMoveableNow p = do
  ds <- downstreams <$> get
  isMoveable1 <- isMoveable p
  allMoveable <- allM (isMoveableIn p) ds
  return $ isMoveable1 && allMoveable

(=*=) :: K3 Provenance -> K3 Provenance -> Bool
a =*= b = case (tag a, tag b) of
            (PBVar mva, PBVar mvb) -> pmvn mva == pmvn mvb && pmvloc mva == pmvloc mvb
            (PFVar ia, PFVar ib) -> ia == ib
            (PGlobal ia, PGlobal ib) -> ia == ib

            -- TODO: Handle more cases.
            _ -> a == b
