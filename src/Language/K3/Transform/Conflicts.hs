{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.Conflicts (getAllConflicts,getAllTasks,getProgramTasks) where

import Control.Arrow hiding ( (+++) )
import Data.List hiding (transpose)
import Data.Map as M (Map,insert, empty,lookup,toList) 
import Data.Graph.Wrapper
import Data.Tree
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.Core.Constructor.Declaration
import Language.K3.Core.Constructor.Expression

-- TODO:
-- 1) calling acrossTriggerConflicts after insideTriggerConflicts re-labels all inside-trigger-conflicts (no real harm, just redundant)
-- 2) run alpha-renaming before detecting conflicts (to avoid shadowing issues)
-- 3) break up within-trigger conflicts into more fine grained tasks
-- 4) fix addUID function to actually assign a unique ID, instead of 999

-- Interface
-- get all conflicts in the given AST
getAllConflicts :: K3 Declaration -> K3 Declaration
getAllConflicts d = (acrossTriggerConflicts . labelDataAccess) d

-- break triggers into smaller tasks (triggers) based on inside-trigger-conflicts
getAllTasks :: K3 Declaration -> K3 Declaration
getAllTasks d = (getTasks . insideTriggerConflicts . labelDataAccess) d

-- group small tasks into 'cohorts' that will run on the same thread
getProgramTasks :: K3 Declaration -> [[String]]
getProgramTasks d = (groupTasks . acrossTriggerConflicts . getAllTasks) d
 
-- Build Conflict Graph for tasks, then return its connected components.
groupTasks :: K3 Declaration -> [[String]]
groupTasks d@(Node (DRole _ :@: _) cs) = let
  newd = acrossTriggerConflicts d
  e1 = getEdges newd
  used  = map fst e1
  trignames = map (getTrigName) (getTriggers cs)
  restnames = trignames \\ used
  rest      = map buildEmptyEdge restnames
  alledges = regroup $ e1 ++ rest
  in getConnComponents $ toGraph alledges
  where
    -- find edges for trigger conflict graph
    getEdges :: K3 Declaration -> [(String, [String])]
    getEdges (Node (DRole _ :@: as) cs2) = let
      trigs = getTriggers cs2
      confs = getConflicts as
      e1 = concat $ map (edgesFromConflict trigs) confs
      e2    = reverseEdges e1
      alledges   = e1 ++ e2
      in groupEdges alledges
    getEdges _ = error "Expecting Role Declaration in getEdges"
 
    -- Determine edges in data-access graph that arise from a given conflict
    edgesFromConflict :: [K3 Declaration] -> Annotation Declaration -> [(String,String)]
    edgesFromConflict trigs (DConflict (URW rs w))  = let
      rids   = map uidFromDataAccess rs
      rtrigs = map (findParentTrig trigs) rids 
      wid    = uidFromDataAccess w
      wtrig  = findParentTrig trigs wid
      in crossProduct rtrigs [wtrig]
    edgesFromConflict trigs (DConflict (UWW w1 w2)) = let
      wid1 = uidFromDataAccess w1
      wid2 = uidFromDataAccess w2
      wtrig1 = findParentTrig trigs wid1
      wtrig2 = findParentTrig trigs wid2
      in [(wtrig1,wtrig2)]
    edgesFromConflict _ _ = error "not a valid conflict"  

    reverseEdges :: [(String,String)] -> [(String,String)]
    reverseEdges [] = []
    reverseEdges (x:xs) = (snd x,fst x):(reverseEdges xs)

    buildEmptyEdge :: String -> (String, [String])
    buildEmptyEdge s = (s,[]) 
    
    regroup :: [(String, [String])] -> [(String, [String])]
    regroup l = let
      grouped = groupByFst l
      in map fixer2 grouped
    
    fixer2 :: [(String,[String])] -> (String, [String])
    fixer2 [] = error "fixer2: list should not be empty!"
    fixer2 ((a,b):xs) = let
      lst = concat $ b : (map snd xs) 
      in (a,lst)
    
    toGraph :: [(String,[String])] -> Graph String String
    toGraph alist = fromListSimple alist

    -- Group all edges by the first vertex in the tuple
    groupEdges :: [(String,String)] -> [(String,[String])] 
    groupEdges l = let
      groups = groupByFst l
      in map fixer groups
      
    -- each list is already grouped by common first element, so only keep list of second elements 
    fixer :: [(String,String)] -> (String,[String])
    fixer [] = error "fixer: list should not be empty!"
    fixer ((a,b):xs) = let
      lst = b : (map snd xs)
      in (a, lst)
    
    -- Group List of String tuples by First 
    groupByFst :: [(String,x)] -> [[(String,x)]]
    groupByFst l = groupBy groupCheck $ sortBy byFst l
    
    byFst :: (String,x) -> (String,x) -> Ordering
    byFst a b = compare (fst a) (fst b)
    
    groupCheck :: (String, x) -> (String, x) -> Bool
    groupCheck a b = (fst a) == (fst b)
    
    -- get Connected Components of Graph
    getConnComponents :: Graph String String -> [[String]]
    getConnComponents g = let
      vs = vertices g 
      in snd $ foldl (f g) ([],[]) vs
    
    f :: Graph String String -> ([String],[[String]]) -> String -> ([String],[[String]])
    f g (seen,components) v 
        | v `elem` seen = (seen,components)
        | otherwise     = 
          let
            newvs = (reachableVertices g v) 
          in (newvs ++ seen, newvs:components) 
groupTasks _ = error "Expecting Role Declaration in groupTasks"

-- Transform triggers into tasks (more triggers)
getTasks :: K3 Declaration -> K3 Declaration
getTasks (Node (DRole r :@: as) cs) = let 
  (trigs,nontrigs) = triggersNonTriggers cs
  newtrigs         = concat $ map splitTrigger trigs
  newcs            = newtrigs ++ nontrigs
  in (Node (DRole r :@: as) newcs) 
  where
    splitTrigger :: K3 Declaration -> [K3 Declaration]
    splitTrigger (Node (DTrigger n t (Node (ELambda n2 :@: _) [e]) :@: _) _) = let 
      lambdas = map (lambda n2) (splitSeq e)  
      lambdas2 = map addUID lambdas
      z = zip [1..] lambdas2
      trigs = map (buildTriggerTask t n) z      
      in trigs
    splitTrigger _ = error "Expecting Trigger Declaration in splitTrigger"

    buildTriggerTask :: K3 Type -> String -> (Int, K3 Expression) -> K3 Declaration
    buildTriggerTask t name (num,e) = let
      newid = name ++ "_" ++ (show num)
      in trigger newid t e 
    
    splitSeq :: K3 Expression -> [K3 Expression]
    splitSeq x@(Node (EOperate OSeq :@: as2) (lft:rght:_)) 
      | hasConflict as2 = [x]
      | otherwise     = (rght:(splitSeq lft))
    splitSeq x = [x]
getTasks _ = error "Expecting Role Declaration in getTasks"

-- Label Conflicts
-- Label all Conflicts in the AST
acrossTriggerConflicts :: K3 Declaration -> K3 Declaration
acrossTriggerConflicts (Node (DRole r :@: as) cs) = 
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
acrossTriggerConflicts _ = error "Expecting Role Declaration in acrossTriggerConflicts"

-- Label all conflicts in the AST except the top level (across-triggers)
insideTriggerConflicts :: K3 Declaration -> K3 Declaration
insideTriggerConflicts (Node (DRole r :@: as) cs) = 
  let 
  childresults = map conflictsTrigger (cs)
  newcs       = map fst childresults
  in (Node (DRole r :@: as) newcs)
insideTriggerConflicts _ = error "Expecting Role Declaration in insideTriggerConflicts"

-- Utils for Conflicts
conflictsTrigger :: K3 Declaration -> (K3 Declaration, [Annotation Expression])
conflictsTrigger (Node (DTrigger n t e :@: as) cs) = 
  let
  (newE, anns)  = conflictsExpression e
  in ((Node (DTrigger n t newE :@: as) cs), anns)
conflictsTrigger x = (x,[])

conflictsExpression :: K3 Expression -> (K3 Expression, [Annotation Expression])
conflictsExpression  (Node (expr :@: as) cs) = 
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
  in ((Node (expr :@: (confs++as)) newcs), (concat fullaccll)) 

-- Label Data Accesses
labelDataAccess :: K3 Declaration -> K3 Declaration
labelDataAccess (Node (DRole rol :@: anns) chs) = (Node (DRole rol :@: anns) (map labelTrigger chs))
  where
    labelTrigger :: K3 Declaration -> K3 Declaration
    labelTrigger (Node (DTrigger n t e :@: as) cs) =  (Node (DTrigger n t (labelExpression e) :@: as) cs)
    labelTrigger x = x
    
    labelExpression :: K3 Expression -> K3 Expression
    labelExpression (Node (EVariable i :@: as) cs) = let
       uid = getUID as 
         in Node (EVariable i :@: ((ERead  i uid):as)) (map labelExpression cs)
    labelExpression (Node (EAssign i :@: as) cs) = let
       uid = getUID as 
         in Node (EAssign   i :@: ((EWrite i uid):as)) (map labelExpression cs)
    labelExpression e = e {subForest = (map labelExpression (subForest e))}
labelDataAccess _ = error "Expecting Role Declaration in Label Data Access"

-- Utils 
-- Returns True if argument is a Sequence Operation
isSeq :: K3 Expression -> Bool
isSeq (Node (EOperate OSeq :@: _) _) = True
isSeq _ = False

-- Get the parent trigger name for a given UID
findParentTrig :: [K3 Declaration] -> UID -> String
findParentTrig [] _ = error "UID not found in any trigger"
findParentTrig (x@(Node (DTrigger n _ _ :@: _) _):xs) u
  | trigChildUID u x = n
  | otherwise        = findParentTrig xs u
findParentTrig _ _ = error "Expecting Trigger Declaration in findParentTrig"

-- Returns True if the given UID is a child of the given Trigger
trigChildUID :: UID -> K3 Declaration -> Bool
trigChildUID u (Node (DTrigger _ _ e :@: _) _) = hasChildUID u e
trigChildUID _ _ = error "Expecting Trigger Declaration in trigChildUID"

-- Returns True if the given UID is a child of the given Expression
hasChildUID :: UID -> K3 Expression -> Bool
hasChildUID u (Node (_ :@: as) cs) = let
  eID = getUID as 
  in (eID == u) || True `elem` (map (hasChildUID u) cs)

-- Returns True if the given Expression has a sequence operation as a descendant
hasChildSeq :: K3 Expression -> Bool
hasChildSeq (Node (EOperate OSeq :@: _) _) = True
hasChildSeq (Node (_ :@: _) cs@(_:_)) = True `elem` (map hasChildSeq cs)
hasChildSeq (Node (_ :@: _) []) = False 

addUID :: K3 Expression -> K3 Expression
addUID e = e @+ (EUID (UID 999)) 

--- Split Declarations into Triggers vs Non Triggers
triggersNonTriggers :: [K3 Declaration] -> ([K3 Declaration],[K3 Declaration])
triggersNonTriggers l = foldl checker ([],[]) l
  where 
    checker (trigs,nontrigs) x
      | isTrigger x = (x:trigs,nontrigs)
      | otherwise   = (trigs,x:nontrigs)

isTrigger :: K3 Declaration -> Bool
isTrigger (Node (DTrigger _ _ _ :@: _) _) = True
isTrigger _ = False

-- Filter subForest of Declarations to only Trigger Declarations 
getTriggers :: [K3 Declaration] -> [K3 Declaration]
getTriggers [] = []
getTriggers (x@(Node (DTrigger _ _ _ :@: _) _):rest) = x:(getTriggers rest)
getTriggers                                  (_:rest)  = (getTriggers rest)

-- Get the top level expression from a Trigger
getTriggerExpression :: K3 Declaration -> K3 Expression
getTriggerExpression (Node (DTrigger _ _ e :@: _) _) = e
getTriggerExpression _                                 = error "Cannot get Trigger Expression from Non-Trigger Declaration"

uidFromDataAccess :: Annotation Expression -> UID
uidFromDataAccess (ERead _ u)  = u
uidFromDataAccess (EWrite _ u) = u
uidFromDataAccess _            = error "not a valid data access"

-- cross product of two lists of same type
crossProduct :: [a] -> [a] -> [(a,a)]
crossProduct xs ys = [(x,y) | x<-xs, y<-ys]

getTrigName :: K3 Declaration -> String
getTrigName (Node (DTrigger n _ _ :@: _) _) = n
getTrigName _ = error "not a trigger"

getConflicts :: [Annotation Declaration] -> [Annotation Declaration]
getConflicts [] =  []
getConflicts ((a@(DConflict _)):xs) = a:(getConflicts xs)
getConflicts (_:xs) = getConflicts xs

hasConflict :: [Annotation Expression] -> Bool
hasConflict [] = False
hasConflict ((EConflict _):_) = True
hasConflict (_:t)             = hasConflict t

hasUConflict :: [Annotation Declaration] -> Bool
hasUConflict [] = False
hasUConflict ((DConflict _):_) = True
hasUConflict (_:t) = hasUConflict t

getUID :: [Annotation Expression] -> UID
getUID []            = error "no UID found!"
getUID ((EUID x):_) = x 
getUID (_:as)        = getUID as

globalVars :: K3 Declaration -> [String]
globalVars (tag &&& children -> (DRole _, ch)) = foldl appendIfGlobal [] ch
globalVars _ = error "Expecting Role Declaration in globalVars"

appendIfGlobal :: [String] -> K3 Declaration  -> [String]
appendIfGlobal lst (tag -> (DGlobal n _ _)) = n:lst
appendIfGlobal lst _ = lst

-- Utils 
getAccesses :: [Annotation Expression] -> [Annotation Expression]
getAccesses as  = foldl accessfilter [] as

accessfilter :: [Annotation Expression] -> Annotation Expression  -> [Annotation Expression]
accessfilter acc a@(ERead _ _)   = acc ++ [a]
accessfilter acc a@(EWrite _ _)  = acc ++ [a]
accessfilter acc _  = acc

-- Utilities for Grouping

groupMap :: [[Annotation Expression]] -> Map String [(Annotation Expression, Int)]
groupMap ll = snd $ foldl grouper (0,M.empty) ll

grouper :: (Int, Map String [(Annotation Expression, Int)]) ->  [Annotation Expression] ->  (Int, Map String [(Annotation Expression, Int)])
grouper (i,themap) as = let
  newmap = foldl (grouper' i) themap as 
  in (i+1, newmap)

grouper' :: Int ->  Map String [(Annotation Expression, Int)]  -> Annotation Expression-> Map String [(Annotation Expression, Int)]
grouper' i themap a@(ERead x _) = let
  getList key  = case (M.lookup key themap) of Nothing  -> []
                                               Just n -> n
  currlist = getList x
  newlist  = currlist ++ [(a,i)]
  in M.insert x newlist themap
grouper' i themap a@(EWrite x _)  = let
  getList key  = case (M.lookup key themap) of Nothing  -> []
                                               Just n -> n
  currlist = getList x
  newlist  = currlist ++ [(a,i)]
  in M.insert x newlist themap
grouper' _ themap _ = themap 

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
convertToUnordered _ = error "Not a valid EConflict"

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
