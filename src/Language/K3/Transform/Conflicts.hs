{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.Conflicts (startAnnotate,startConflicts,buildFrontier) where

import Control.Arrow hiding ( (+++) )
import Data.List
import Data.Map as M (Map,insert, empty,findWithDefault,lookup,toList) 
import Data.Maybe
import Data.Tree
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Common


-- Build Frontier Top Level. Given AST Annotated with Conflicts
buildFrontier :: K3 Declaration -> (Maybe (K3 Declaration), [K3 Expression])
buildFrontier x@(Node (DRole r :@: as) cs)
  | hasUConflict as =  (Just x, [])
  | otherwise       =  (Nothing, (concat $ map buildEFrontier (map (getTriggerExpression) (getTriggers cs))))

-- Filter subForest of Declarations to only Trigger Declarations 
getTriggers :: [K3 Declaration] -> [K3 Declaration]
getTriggers [] = []
getTriggers (x@(Node (DTrigger _ _ _ :@: _) _):rest) = x:(getTriggers rest)
getTriggers                                  (_:rest)  = (getTriggers rest)

-- Get the top level expression from a Trigger
getTriggerExpression :: K3 Declaration -> K3 Expression
getTriggerExpression (Node (DTrigger n t e :@: as) cs) = e
getTriggerExpression _                                 = error "Cannot get Trigger Expression from Non-Trigger Declaration"

-- Build Frontier at expression level. 
buildEFrontier :: K3 Expression -> [K3 Expression]
buildEFrontier x@(Node (_ :@: as) cs) 
  | hasConflict as = [x]
  | otherwise      = concat $ map buildEFrontier cs


hasConflict :: [Annotation Expression] -> Bool
hasConflict [] = False
hasConflict ((EConflict x):t) = True
hasConflict (_:t)             = hasConflict t

hasUConflict :: [Annotation Declaration] -> Bool
hasUConflict [] = False
hasUConflict ((DConflict x):t) = True
hasUConflict (_:t) = hasUConflict t

-- Annotate Data Accesses
startAnnotate :: K3 Declaration -> K3 Declaration
startAnnotate x@(Node (DRole r :@: as) cs) = (Node (DRole r :@: as) (map annotateTrigger cs))

annotateTrigger :: K3 Declaration -> K3 Declaration
annotateTrigger (Node (DTrigger n t e :@: as) cs) =  (Node (DTrigger n t (annotateExpression e) :@: as) cs)
annotateTrigger x = x

annotateExpression :: K3 Expression -> K3 Expression
annotateExpression exp@(Node (EVariable i :@: as) cs) = let
   uid = getUID as 
     in Node (EVariable i :@: ((ERead  i uid):as)) (map annotateExpression cs)
annotateExpression exp@(Node (EAssign i :@: as) cs) = let
   uid = getUID as 
     in Node (EAssign   i :@: ((EWrite i uid):as)) (map annotateExpression cs)
annotateExpression exp = exp {subForest = (map annotateExpression (subForest exp))}

getUID :: [Annotation Expression] -> UID
getUID []            = error "no UID found!"
getUID ((EUID x):as) = x 
getUID (a:as)        = getUID as

-- utils 
globalVars :: K3 Declaration -> [String]
globalVars (tag &&& children -> (DRole r, ch)) = foldl appendIfGlobal [] ch

appendIfGlobal :: [String] -> K3 Declaration  -> [String]
appendIfGlobal lst (tag -> (DGlobal n _ _)) = n:lst
appendIfGlobal lst _ = lst

-- Annotate Conflicts
startConflicts :: K3 Declaration -> K3 Declaration
startConflicts (Node (DRole r :@: as) cs) = 
  let 
  childresults = map conflictsTrigger (cs)
  newcs       = map fst childresults
  annll       = map snd childresults
  childaccll  = map getAccesses annll
  fullaccll   = childaccll
  x           = groupMap fullaccll
  tuplesll    = map (snd) (M.toList x)
  full        = map combineReads tuplesll
  confs       = concat $ map buildConflicts full
  dConfs      = map convertToUnordered confs
  in (Node (DRole r :@: (dConfs ++ as)) newcs)

conflictsTrigger :: K3 Declaration -> (K3 Declaration, [Annotation Expression])
conflictsTrigger (Node (DTrigger n t e :@: as) cs) = 
  let
  (newE, anns)  = conflictsExpression e
  in ((Node (DTrigger n t newE :@: as) cs), anns)
conflictsTrigger x = (x,[])

conflictsExpression :: K3 Expression -> (K3 Expression, [Annotation Expression])
conflictsExpression  (Node (exp :@: as) cs) = 
  let
  childresults = map conflictsExpression (cs)
  newcs        = map fst childresults
  annll        = map snd childresults
  childaccll   = map getAccesses annll  
  fullaccll    = childaccll ++ [(getAccesses as)]
  x            = groupMap fullaccll   
  tuplesll     = map (snd) (M.toList x) -- values from hash map (group by)
  full         = map combineReads tuplesll
  confs        = concat $ map buildConflicts full
  in ((Node (exp :@: (confs++(concat $ (map (map fst) tuplesll))++as)) newcs), (concat fullaccll)) 

-- Utils 
getAccesses :: [Annotation Expression] -> [Annotation Expression]
getAccesses as  = foldl accessfilter [] as

accessfilter :: [Annotation Expression] -> Annotation Expression  -> [Annotation Expression]
accessfilter acc a@(ERead x _)   = acc ++ [a]
accessfilter acc a@(EWrite x _)  = acc ++ [a]
accessfilter acc _  = acc

-- Utilities for Grouping

groupMap :: [[Annotation Expression]] -> Map String [(Annotation Expression, Int)]
groupMap ll = snd $ foldl grouper (0,empty) ll

grouper :: (Int, Map String [(Annotation Expression, Int)]) ->  [Annotation Expression] ->  (Int, Map String [(Annotation Expression, Int)])
grouper (i,map) as = let
  newmap = foldl (grouper' i) map as 
  in (i+1, newmap)

grouper' :: Int ->  Map String [(Annotation Expression, Int)]  -> Annotation Expression-> Map String [(Annotation Expression, Int)]
grouper' i map a@(ERead x _) = let
  getList key  = case (M.lookup key map) of Nothing  -> []
                                            Just x -> x
  currlist = getList x
  newlist  = currlist ++ [(a,i)]
  in M.insert x newlist map
grouper' i map a@(EWrite x _)  = let
  getList key  = case (M.lookup key map) of Nothing  -> []
                                            Just x -> x
  currlist = getList x
  newlist  = currlist ++ [(a,i)]
  in M.insert x newlist map
grouper' _ map _ = map 

-- Util to combine adjacent reads into a list of reads
combineReads :: [(Annotation Expression,Int)] -> [[(Annotation Expression, Int)]]
combineReads lst = let 
   (end,result) = foldl combineReads' ([],[]) lst
  in result ++ [end]

-- remember to attach last list to result when done
combineReads' ::  ([(Annotation Expression, Int)],[[(Annotation Expression, Int)]]) -> (Annotation Expression, Int) -> ([(Annotation Expression, Int)],[[(Annotation Expression, Int)]])
combineReads' ([], result) curr                 = ([curr], result)
combineReads' (currlst, result) c@((EWrite _ _),_) = ([c], result++[currlst])
-- curr must be read
combineReads' (currlst, result) curr          = case currlst of 
  (((ERead _ _), _):_) ->   (currlst++[curr], result)
  _ -> ([curr], result++[currlst]) 

-- Utilities for Building Conflicts
convertToUnordered :: Annotation Expression -> Annotation Declaration
convertToUnordered (EConflict  (RW a b)) = (DConflict  (URW a b))
convertToUnordered (EConflict  (WR b a)) = (DConflict  (URW a b))
convertToUnordered (EConflict  (WW a b)) = (DConflict  (UWW a b))


diffChild :: Int -> (Annotation Expression, Int) -> Bool
diffChild n (_, n2) = (n/=n2)

isRead :: [(Annotation Expression, Int)] -> Bool
isRead ((ERead _ _, _):_) = True
isRead _ = False

buildConflicts :: [[(Annotation Expression, Int)]] -> [Annotation Expression]
buildConflicts []  = []
buildConflicts [_] = []
buildConflicts (a:b:lst) = 
  case (isRead a) of 
    -- acc was reads -> Potential RW Conflict
    True  -> (buildRW a b) ++ (buildConflicts (b:lst))
    False -> case (isRead b) of 
                True  -> (buildWR a b) ++ (buildConflicts (b:lst))
                False -> (buildWW a b) ++ (buildConflicts (b:lst))

buildRW :: [(Annotation Expression, Int)] -> [(Annotation Expression, Int)] -> [Annotation Expression]
buildRW a b = let 
  remaining = filter (diffChild ((snd .head) b)) a
  in case remaining of
    [] -> []
    _  -> [EConflict $ RW (map fst remaining) ((fst . head) b)]

buildWR :: [(Annotation Expression, Int)] -> [(Annotation Expression, Int)] -> [Annotation Expression]
buildWR a b = let 
  remaining = filter (diffChild ((snd .head) a)) b
  in case remaining of
    [] -> []
    _  -> [EConflict $ WR ((fst . head) a) (map fst remaining)]

buildWW :: [(Annotation Expression, Int)] -> [(Annotation Expression, Int)] -> [Annotation Expression]
buildWW a b = let 
  remaining = filter (diffChild ((snd .head) a)) b
  in case remaining of
    [] -> []
    _  -> [EConflict $ WW ((fst . head) a) ((fst .head) b)]
