{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Analysis.Core where

import Control.Arrow
import Control.Monad

import Data.Binary
import Data.Serialize
import Data.Vector.Serialize

import Data.Bits
import Data.Bits.Extras
import Data.Char
import Data.List
import Data.Tree
import Data.Word ( Word8 )
import Numeric

import Data.IntMap ( IntMap )
import qualified Data.IntMap as IntMap

import Data.Vector.Binary ()
import Data.Vector.Unboxed ( Vector, (!?) )
import qualified Data.Vector.Unboxed as Vector

import GHC.Generics ( Generic )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import Data.Text ( Text )
import qualified Data.Text as T
import Language.K3.Utils.Pretty
import qualified Language.K3.Utils.PrettyText as PT


-- | Lambda closures, as a map of lambda expression UIDs to closure variable identifiers.
type ClosureEnv = IntMap [Identifier]

-- | Metadata for indexing on variables at a given scope
data IndexedScope = IndexedScope { scids :: [Identifier], scsz :: Int }
                    deriving (Eq, Ord, Read, Show, Generic)

-- | Binding point scopes, as a map of binding point expression UIDS to current scope
--   including the new binding.
type ScopeEnv = IntMap IndexedScope

-- | Vector for binding usage bits
type BVector = Vector Word8

-- | Scope usage bitmasks, as a map of expression UID to a pair of scope uid and bitvector.
type ScopeUsageEnv = IntMap (UID, BVector)

-- | Expression-variable metadata container.
data VarPosEnv = VarPosEnv { lcenv :: ClosureEnv, scenv :: ScopeEnv, vuenv :: ScopeUsageEnv }
                 deriving (Eq, Ord, Read, Show, Generic)

-- | Traversal indexes, indicating subtree variable (from abstract interpretation or K3) usage.
type TrIndex = K3 BVector

data instance Annotation BVector = BUID UID deriving (Eq, Ord, Read, Show, Generic)

-- | Traversal environment, from an abstract interpretation (AI) index to
--   either another AI index, or a traversal index.
type AVTraversalEnv = IntMap (TrIndex, Maybe Int)

-- Abstract interpretation environment.
data AIVEnv = AIVEnv { avtenv :: AVTraversalEnv }
              deriving (Eq, Read, Show, Generic)

{- Instances -}
instance Binary    IndexedScope
instance Binary    VarPosEnv
instance Binary    AIVEnv
instance Binary    (Annotation BVector)

instance Serialize IndexedScope
instance Serialize VarPosEnv
instance Serialize AIVEnv
instance Serialize (Annotation BVector)


{- BVector helpers -}
showbv :: BVector -> String
showbv v = foldl showBinary "" $ Vector.toList v
  where showBinary acc i = acc ++ pad (showIntAtBase 2 intToDigit i "")
        pad s = replicate (szb - length s) '0' ++ s
        szb = finiteBitSize (0 :: Word8)

szbv :: Int -> Int
szbv varSz = scdiv + (if scmod == 0 then 0 else 1)
  where (scdiv,scmod) = varSz `divMod` (finiteBitSize (0 :: Word8))

emptybv :: BVector
emptybv = Vector.empty

zerobv :: Int -> BVector
zerobv sz = Vector.replicate (szbv sz) 0

bzerobv :: Int -> BVector
bzerobv sz = Vector.replicate sz 0

lastbv :: Int -> BVector
lastbv sz = singbv (sz-1) sz

-- | Returns a bit vector of the desired size with the given bit set.
singbv :: Int -> Int -> BVector
singbv pos sz = Vector.generate (szbv sz) (\i -> if i == bpos then bit (w8sz - (posinb+1)) else 0)
  where w8sz = finiteBitSize (0 :: Word8)
        (bpos, posinb) = pos `divMod` w8sz

suffixbv :: Int -> Int -> BVector
suffixbv rpos sz = andbv vset vsz
  where w8sz   = finiteBitSize (0 :: Word8)
        bsz    = szbv sz

        -- position of the first bit.
        startpos = sz - (rpos+1)
        (bstart, sposinb) = startpos `divMod` w8sz

        -- # bits in last byte.
        bend    = bsz - 1
        eposinb = sz - (bend * w8sz)

        lb i = shiftL (oneBits :: Word8) $ w8sz - i
        rb i = shiftR (oneBits :: Word8) $ i

        vset   = Vector.generate bsz bytei
        bytei i | i == bstart = rb $ sposinb
                | i < bstart = zeroBits :: Word8
                | otherwise = oneBits :: Word8

        vsz    = Vector.generate bsz szgen
        szgen i | i == bend = lb $ eposinb
                | otherwise = oneBits :: Word8

anybv :: BVector -> Bool
anybv = Vector.any (\i -> popCount i > 0)

orbv :: BVector -> BVector -> BVector
orbv a b = Vector.map (uncurry (.|.)) $ Vector.zip a b

andbv :: BVector -> BVector -> BVector
andbv a b = Vector.map (uncurry (.&.)) $ Vector.zip a b

orsbv :: [BVector] -> BVector
orsbv l = case l of
            [] -> Vector.empty
            _ -> let maxv = maximumBy longer l
                 in Vector.imap (\i e -> foldl (onIndex i) e l) maxv
  where onIndex i acc v = maybe acc (acc .|.) $ v !? i
        longer a b = Vector.length a `compare` Vector.length b

andsbv :: [BVector] -> BVector
andsbv l = case l of
             [] -> Vector.empty
             _ -> let maxv = maximumBy longer l
                  in Vector.imap (\i e -> foldl (onIndex i) e l) maxv
  where onIndex i acc v = maybe acc (acc .&.) $ v !? i
        longer a b = Vector.length a `compare` Vector.length b

truncateLast :: BVector -> BVector
truncateLast v = Vector.take (Vector.length v - 1) v

truncateSuffix :: Int -> BVector -> BVector
truncateSuffix n v = Vector.take (Vector.length v - n) v

{- TrIndex helpers -}
rootbv :: TrIndex -> BVector
rootbv = tag

tilen :: TrIndex -> Int
tilen ti = Vector.length $ rootbv ti

emptyti :: TrIndex
emptyti = tileaf emptybv

tinode :: BVector -> [TrIndex] -> TrIndex
tinode v ch = Node (v :@: []) ch

tileaf :: BVector -> TrIndex
tileaf v = Node (v :@: []) []

tising :: BVector -> TrIndex -> TrIndex
tising v c = Node (v :@: []) [c]

tisdup :: TrIndex -> TrIndex
tisdup c@(Node (v :@: _) _) = Node (v :@: []) [c]

orti :: [TrIndex] -> TrIndex
orti tich = tinode (orsbv $ map rootbv tich) tich

unaryti :: TrIndex -> TrIndex
unaryti ti = tising (rootbv ti) ti

{- VarPosEnv helpers -}
vp0 :: VarPosEnv
vp0 = VarPosEnv IntMap.empty IntMap.empty IntMap.empty

vpunion :: VarPosEnv -> VarPosEnv -> VarPosEnv
vpunion a b = VarPosEnv { lcenv = IntMap.union (lcenv a) (lcenv b)
                        , scenv = IntMap.union (scenv a) (scenv b)
                        , vuenv = IntMap.union (vuenv a) (vuenv b) }

vpunions :: [VarPosEnv] -> VarPosEnv
vpunions l = VarPosEnv { lcenv = IntMap.unions (map lcenv l)
                       , scenv = IntMap.unions (map scenv l)
                       , vuenv = IntMap.unions (map vuenv l) }

vplkuplc :: VarPosEnv -> Int -> Either Text [Identifier]
vplkuplc env x = maybe err Right $ IntMap.lookup x $ lcenv env
  where err = Left . T.pack $ "Unbound id in vplc environment: " ++ show x

vpextlc :: VarPosEnv -> Int -> [Identifier] -> VarPosEnv
vpextlc env x l = env { lcenv = IntMap.insert x l $ lcenv env }

vpdellc :: VarPosEnv -> Int -> VarPosEnv
vpdellc env x = env { lcenv = IntMap.delete x $ lcenv env }

vpmemlc :: VarPosEnv -> Int -> Bool
vpmemlc env x = IntMap.member x $ lcenv env

vplkupsc :: VarPosEnv -> Int -> Either Text IndexedScope
vplkupsc env x = maybe err Right $ IntMap.lookup x $ scenv env
  where err = Left . T.pack $ "Unbound id in vpsc environment: " ++ show x

vpextsc :: VarPosEnv -> Int -> IndexedScope -> VarPosEnv
vpextsc env x s = env { scenv = IntMap.insert x s $ scenv env }

vpdelsc :: VarPosEnv -> Int -> VarPosEnv
vpdelsc env x = env { scenv = IntMap.delete x $ scenv env }

vpmemsc :: VarPosEnv -> Int -> Bool
vpmemsc env x = IntMap.member x $ scenv env

vplkupvu :: VarPosEnv -> Int -> Either Text (UID, BVector)
vplkupvu env x = maybe err Right $ IntMap.lookup x $ vuenv env
  where err = Left . T.pack $ "Unbound id in vpvu environment: " ++ show x

vpextvu :: VarPosEnv -> Int -> UID -> BVector -> VarPosEnv
vpextvu env x u v = env { vuenv = IntMap.insert x (u,v) $ vuenv env }

vpdelvu :: VarPosEnv -> Int -> VarPosEnv
vpdelvu env x = env { vuenv = IntMap.delete x $ vuenv env }

vpmemvu :: VarPosEnv -> Int -> Bool
vpmemvu env x = IntMap.member x $ vuenv env


{- AIEnv helpers -}
aivenv0 :: AIVEnv
aivenv0 = AIVEnv IntMap.empty

aivlkup :: AIVEnv -> Int -> Either Text (TrIndex, Maybe Int)
aivlkup env x = maybe err Right $ IntMap.lookup x $ avtenv env
  where err = Left . T.pack $ "Unbound pointer in aiv environment: " ++ show x

aivext :: AIVEnv -> Int -> TrIndex -> Maybe Int -> AIVEnv
aivext env x ti pOpt = env { avtenv = IntMap.insert x (ti, pOpt) $ avtenv env }

aivdel :: AIVEnv -> Int -> AIVEnv
aivdel env x = env { avtenv = IntMap.delete x $ avtenv env }

aivmem :: AIVEnv -> Int -> Bool
aivmem env x = IntMap.member x $ avtenv env

{- Indexed traversals -}
indexMapRebuildTree :: (Monad m, Pretty (K3 a))
                    => ([TrIndex] -> [K3 a] -> TrIndex -> K3 a -> m (TrIndex, K3 a))
                    -> BVector -> TrIndex -> K3 a -> m (TrIndex, K3 a)
indexMapRebuildTree f mask ti t =
  if match then
    let (tich, ch) = (children ti, children t) in
    if length tich == length ch
      then do
        npairs <- mapM (uncurry rcr) (zip tich ch)
        let (ntich, nch) = unzip npairs
        f ntich nch ti t
      else fail $ msg (length tich) (length ch) ti t
    else return (ti, t)
  where match = anybv $ mask `andbv` (tag ti)
        rcr = indexMapRebuildTree f mask
        msg lx ly a b =
          boxToString $ [unwords ["Mismatched index and tree children", show lx, show ly]]
                      %$ prettyLines a
                      %$ prettyLines b


{- Common program analyses -}

-- | Retrieves all free variables in an expression.
freeVariables :: K3 Expression -> [Identifier]
freeVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EVariable n) = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> EAssign i)   = return $ concat chAcc ++ [i]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extractVariable chAcc (tag -> ELetIn i)    = return $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc

-- | Retrieves all variables introduced by a binder
bindingVariables :: Binder -> [Identifier]
bindingVariables (BIndirection i) = [i]
bindingVariables (BTuple is)      = is
bindingVariables (BRecord ivs)    = snd (unzip ivs)

-- | Retrieves all variables modified in an expression.
modifiedVariables :: K3 Expression -> [Identifier]
modifiedVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EAssign n)   = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extractVariable chAcc (tag -> ELetIn i)    = return $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc


-- | Computes the closure variables captured at lambda expressions.
--   This is a one-pass bottom-up implementation.
lambdaClosures :: K3 Declaration -> Either String ClosureEnv
lambdaClosures p = foldExpression lambdaClosuresExpr IntMap.empty p >>= return . fst

lambdaClosuresDecl :: Identifier -> ClosureEnv -> K3 Declaration -> Either String (ClosureEnv, K3 Declaration)
lambdaClosuresDecl n lc p = foldNamedDeclExpression n lambdaClosuresExpr lc p

lambdaClosuresExpr :: ClosureEnv -> K3 Expression -> Either String (ClosureEnv, K3 Expression)
lambdaClosuresExpr lc expr = do
  (nlc,_) <- biFoldMapTree bind extract [] (IntMap.empty, []) expr
  return $ (IntMap.union nlc lc, expr)

  where
    bind :: [Identifier] -> K3 Expression -> Either String ([Identifier], [[Identifier]])
    bind l (tag -> ELambda i) = return (l, [i:l])
    bind l (tag -> ELetIn  i) = return (l, [l, i:l])
    bind l (tag -> EBindAs b) = return (l, [l, bindingVariables b ++ l])
    bind l (tag -> ECaseOf i) = return (l, [l, i:l, l])
    bind l (children -> ch)   = return (l, replicate (length ch) l)

    extract :: [Identifier] -> [(ClosureEnv, [Identifier])] -> K3 Expression -> Either String (ClosureEnv, [Identifier])
    extract _ chAcc (tag -> EVariable i) = rt chAcc (++[i])
    extract _ chAcc (tag -> EAssign i)   = rt chAcc (++[i])
    extract l (concatLc -> (lcAcc,chAcc)) e@(tag -> ELambda n) = extendLc lcAcc e $ filter (onlyLocals n l) $ concat chAcc
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> EBindAs b) = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> ELetIn i)  = return . (lcAcc,) $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extract _ (concatLc -> (lcAcc,chAcc))   (tag -> ECaseOf i) = return . (lcAcc,) $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extract _ chAcc _ = rt chAcc id

    onlyLocals n l i = i /= n && i `elem` l

    concatLc :: [(ClosureEnv, [Identifier])] -> (ClosureEnv, [[Identifier]])
    concatLc subAcc = let (x,y) = unzip subAcc in (IntMap.unions x, y)

    extendLc :: ClosureEnv -> K3 Expression -> [Identifier] -> Either String (ClosureEnv, [Identifier])
    extendLc elc e ids = case e @~ isEUID of
      Just (EUID (UID i)) -> return $ (IntMap.insert i (nub ids) elc, ids)
      _ -> Left $ boxToString $ ["No UID found on lambda"] %$ prettyLines e

    rt subAcc f = return $ second (f . concat) $ concatLc subAcc


-- | Compute lambda closures and binding point scopes in a single pass.
variablePositions :: K3 Declaration -> Either String VarPosEnv
variablePositions p = foldExpression variablePositionsExpr vp0 p >>= return . fst

variablePositionsDecl :: Identifier -> VarPosEnv -> K3 Declaration -> Either String (VarPosEnv, K3 Declaration)
variablePositionsDecl n vp p = foldNamedDeclExpression n variablePositionsExpr vp p

variablePositionsExpr :: VarPosEnv -> K3 Expression -> Either String (VarPosEnv, K3 Expression)
variablePositionsExpr vp expr = do
  (nvp,_) <- biFoldMapTree bind extract ([], -1) (vp0, []) expr
  return $ (vpunion nvp vp, expr)

  where
    uidOf :: K3 Expression -> Either String Int
    uidOf ((@~ isEUID) -> Just (EUID (UID i))) = return i
    uidOf e = Left $ boxToString $ ["No UID found for uidOf"] %$ prettyLines e

    bind :: ([Identifier], Int) -> K3 Expression -> Either String (([Identifier], Int), [([Identifier], Int)])
    bind l e@(tag -> ELambda i) = uidOf e >>= \u -> return (l, [((i:fst l), u)])
    bind l e@(tag -> ELetIn  i) = uidOf e >>= \u -> return (l, [l, ((i:fst l), u)])
    bind l e@(tag -> EBindAs b) = uidOf e >>= \u -> return (l, [l, (bindingVariables b ++ fst l, u)])
    bind l e@(tag -> ECaseOf i) = uidOf e >>= \u -> return (l, [l, ((i:fst l), u), l])
    bind l (children -> ch)   = return (l, replicate (length ch) l)

    extract :: ([Identifier], Int) -> [(VarPosEnv, [Identifier])] -> K3 Expression -> Either String (VarPosEnv, [Identifier])
    extract td        chAcc e@(tag -> EVariable i) = rt td chAcc e [] $ const $ var i
    extract td        chAcc e@(tag -> EAssign i)   = rt td chAcc e [] $ const $ var i
    extract td@(sc,_) chAcc e@(tag -> ELambda n)   = rt td chAcc e [n] $ scope Nothing [n] sc
    extract td@(sc,_) chAcc e@(tag -> EBindAs b)   = let bvs = bindingVariables b in
                                                     rt td chAcc e bvs $ scope (Just 1) bvs sc
    extract td@(sc,_) chAcc e@(tag -> ELetIn i)    = rt td chAcc e [i] $ scope (Just 1) [i] sc
    extract td@(sc,_) chAcc e@(tag -> ECaseOf i)   = rt td chAcc e [i] $ scope (Just 1) [i] sc
    extract td        chAcc e                      = rt td chAcc e [] $ const concatvi

    concatvi :: [VarPosEnv] -> [[Identifier]] -> (VarPosEnv, [Identifier])
    concatvi vps subvars = (vpunions vps, concat subvars)

    var :: Identifier -> [VarPosEnv] -> [[Identifier]] -> (VarPosEnv, [Identifier])
    var i vps subvars = (vpunions vps, concat subvars ++ [i])

    scope :: Maybe Int -> [Identifier] -> [Identifier] -> Int -> [VarPosEnv] -> [[Identifier]] -> (VarPosEnv, [Identifier])
    scope pruneIdxOpt n sc i vps subvars = case pruneIdxOpt of
        Nothing       -> (vpextlc (vpextsc vp' i scentry) i clvars, clvars)
        Just pruneIdx -> (vpextsc vp' i scentry, prune pruneIdx n subvars)
      where vp' = vpunions vps
            scentry = IndexedScope scvars $ length scvars
            scvars = sc ++ n
            clvars = subvarsInScope n sc subvars

    varusage :: [Identifier] -> [Identifier] -> Int -> Int -> [VarPosEnv] -> [[Identifier]] -> VarPosEnv
    varusage n sc scu u vps subvars = vpextvu vp' u (UID scu) usedmask
      where vp' = vpunions vps
            usedvars = subvarsInScope n sc subvars
            usedmask = Vector.fromList $ snd $ foldl mkmask (0,[]) sc

            mkmask (c, m)   i | ix c == 0 = (c+1, fromBool (used i) : m)
            mkmask (c, h:t) i             = (c+1, (if used i then h `setBit` (ix c) else h):t)
            mkmask _ _ = error "Mask initialization failed"

            sz = finiteBitSize (0 :: Word8)
            ix c = c `mod` sz
            used i = i `elem` usedvars
            fromBool False = 0
            fromBool True  = bit 0

    prune :: Int -> [Identifier] -> [[Identifier]] -> [Identifier]
    prune i vars subvars = concatMap (\(j,l) -> if i == j then (filter (`notElem` vars) l) else l) $ zip [0..(length subvars)] subvars

    subvarsInScope :: [Identifier] -> [Identifier] -> [[Identifier]] -> [Identifier]
    subvarsInScope n sc subvars = nub $ filter (onlyLocals n sc) $ concat subvars

    onlyLocals :: [Identifier] -> [Identifier] -> Identifier -> Bool
    onlyLocals n l i = i `notElem` n && i `elem` l

    rt (sc,scu) (unzip -> (acc, iAcc)) e bnds accF = do
      u <- uidOf e
      let (nvpe, nids) = accF u acc iAcc
      return (varusage bnds sc scu u [nvpe] iAcc, nids)

-- | Compute all global declarations used by the supplied list of declaration identifiers.
--   This method returns all transitive dependencies.
minimalProgramDecls :: [Identifier] -> K3 Declaration -> Either String [Identifier]
minimalProgramDecls declIds prog = fixpointAcc declGlobals (declIds, declIds)
  where fixpointAcc f (acc, next) = do
          deltaAcc <- foldM f [] next
          let nacc = nub $ acc ++ deltaAcc
          if nacc == acc then return acc else fixpointAcc f (nacc, deltaAcc)

        declGlobals acc i = foldNamedDeclExpression i extractGlobals acc prog >>= return . fst
        extractGlobals acc e = return (acc ++ freeVariables e, e)


instance Pretty TrIndex where
  prettyLines (Node (tg :@: _) ch) = [showbv tg] ++ drawSubTrees ch

instance PT.Pretty TrIndex where
  prettyLines (Node (tg :@: _) ch) = [T.pack $ showbv tg] ++ PT.drawSubTrees ch
