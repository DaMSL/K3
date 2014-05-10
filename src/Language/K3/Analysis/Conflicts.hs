{-# LANGUAGE ViewPatterns #-}

module Language.K3.Analysis.Conflicts (getAllConflicts,getAllTasks,getProgramTasks,getNewProgram) where

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
import Language.K3.Core.Constructor.Declaration as D
import Language.K3.Core.Constructor.Expression as E
import Language.K3.Core.Constructor.Type as T
-- TODO:
-- 1) calling acrossTriggerConflicts after insideTriggerConflicts re-labels all inside-trigger-conflicts (no real harm, just redundant)
-- 2) run alpha-renaming before detecting conflicts (to avoid shadowing issues)
-- 3) break up within-trigger conflicts into more fine grained tasks
-- 4) fix addUID function to actually assign a unique ID, instead of 999

wordsWhen     :: (Char -> Bool) -> String -> [String]
wordsWhen p s =  case dropWhile p s of
                      "" -> []
                      s' -> w : wordsWhen p s''
                            where (w, s'') = break p s'
-- Interface
-- get all conflicts in the given AST
getAllConflicts :: K3 Declaration -> K3 Declaration
getAllConflicts d = (acrossTriggerConflicts . labelDataAccess) d

-- break triggers into smaller tasks (triggers) based on inside-trigger-conflicts
getAllTasks :: K3 Declaration -> K3 Declaration
getAllTasks d = let 
  (trigs,newtrigs) = (getTasks . insideTriggerConflicts . labelDataAccess) d
  (_,nontrigs) = triggersNonTriggers (subForest d)
  in d {subForest = nontrigs ++ newtrigs}

-- group small tasks into 'cohorts' that will run on the same thread
getProgramTasks :: K3 Declaration -> [(String, [String])]
getProgramTasks d = (groupTasks . getAllTasks) d
 
-- transform original program into parallel version
getNewProgram :: K3 Declaration -> K3 Declaration
getNewProgram d@(Node (DRole r :@: as) cs) = let
  (trigs,nontrigs) = triggersNonTriggers cs
  (_, tasks) = (getTasks . insideTriggerConflicts . labelDataAccess) d
  groups = getProgramTasks d
  gnames = map fst groups
  tnames = map getTriggerName trigs
  ts = map getTriggersForGroup (map snd groups)
  types = map (getGroupType trigs) (map snd groups)
  newtrigs = map (replaceTrigger groups) trigs
  dispatchers = zipWith newDispatcher groups types
  in (Node (DRole r :@: as) (newtrigs++dispatchers++tasks++nontrigs)) 
  where 
    z :: a -> b -> (a,b)
    z x y = (x,y)
    
    -- Filter list of groups: keep those that contain pieces of the given trigger
    getGroupsForTrigger :: [(String,[String])] -> String -> [(String,[String])]
    getGroupsForTrigger gs tname = filter (\(gname,group) ->
      if True `elem` (map (isPrefixOf tname) group)
      then True
      else False
      ) gs  

    -- Get the trigger names for a list of tasks
    getTriggersForGroup :: [String] -> [String]
    getTriggersForGroup lst = nub $ map (\x -> head (wordsWhen (=='_') x)) lst

    -- Create a Type for a group. Needs list of all original triggers.
    getGroupType :: [K3 Declaration] -> [String] -> K3 Type
    getGroupType alltrigs group = let
      tnames = getTriggersForGroup group
      thetrigs = map (getTrigByName alltrigs) tnames
      types = map getTrigType thetrigs
      r = T.tuple types
      in T.tuple $ [T.string] ++ [r]
    
    -- filter tasks that match the provided trigger name
    getTasksForTrigger :: String -> [String] -> [String]
    getTasksForTrigger n ns = filter (\x -> n `isPrefixOf` x) ns
    
    -- replace a trigger with a trigger that simply forwards messages to 'group' triggers
    replaceTrigger :: [(String,[String])] -> K3 Declaration -> K3 Declaration
    replaceTrigger groups tr@(Node (DTrigger n t e :@: as) cs) = let
      mGroups = getGroupsForTrigger groups n
      gnames = map fst mGroups
      gs = map snd mGroups
      rs = map (getMessage tr) gs
      es = zipWith newSend gnames rs
      seq = E.block es
      l = E.lambda "x" seq
      in (Node (DTrigger n t l :@: as) cs) 
      
    -- create a send expression for given destination trigger, expression
    newSend :: String -> K3 Expression -> K3 Expression
    newSend gname e = E.send (E.variable gname) (E.variable "me") e 

    -- search for the given trigger name in the list of triggers
    getTrigByName :: [K3 Declaration] -> String -> K3 Declaration
    getTrigByName [] _ = error "Trigger not found"
    getTrigByName (x@(Node (DTrigger n _ _ :@: as) _):xs) s 
      | s == n = x
      | otherwise = getTrigByName xs s 
    getTrigByName _ _ = error "Not a valid Trigger"

    -- build the proper tuple to send for the given trigger and list of tasks
    getMessage :: K3 Declaration -> [String] -> K3 Expression
    getMessage (Node (DTrigger n t _ :@: _) _) group = let
      tnames = getTriggersForGroup group
      elist = map (\x -> if x == n then (variable "x") else E.unit) tnames 
      in E.tuple $ [E.constant (CString n)] ++ [E.tuple elist]
    
    -- return the type of the provided trigger
    getTrigType :: K3 Declaration -> K3 Type
    getTrigType (Node (DTrigger _ t _ :@: _) _) = t
    getTrigType _ = error "not a valid trigger"

    -- create a new dispatcher for the provided group and type
    newDispatcher :: (String,[String]) -> K3 Type -> K3 Declaration
    newDispatcher (gname,ns) t = D.trigger (gname) t (dispatchTable ns)  
    
    -- build the dispatch Table for the provided group
    dispatchTable :: [String] -> K3 Expression
    dispatchTable gs = let
      ts = getTriggersForGroup gs
      argnames = map (\t -> t ++ "_args") ts
      b1 = bindAs (E.variable "x") (BTuple ["tname","y"]) b2
      b2 = bindAs (E.variable "y") (BTuple argnames) block
      block = E.block $ map (\t ->
        let
          tasks = getTasksForTrigger t gs
          pred = E.binop OEqu (E.variable "tname") (E.constant $ CString t) 
          in E.block $ map (\task -> 
            E.ifThenElse pred 
              (E.send (E.variable task) (E.variable "me") (E.variable (t++"_args"))) 
              (E.unit)
          ) tasks 
        ) ts
      in E.lambda "x" b1

-- Build Conflict Graph for tasks, then return its connected components.
groupTasks :: K3 Declaration -> [(String,[String])]
groupTasks d@(Node (DRole _ :@: _) cs) = let
  newd = acrossTriggerConflicts d
  e1 = getEdges newd
  used  = map fst e1
  trignames = map (getTrigName) (getTriggers cs)
  restnames = trignames \\ used
  rest      = map buildEmptyEdge restnames
  alledges = regroup $ e1 ++ rest
  groups =  getConnComponents $ toGraph alledges
  names = getNames groups
  in zipWith z names groups
  where
    getNames :: [[String]] -> [String]
    getNames gs = let 
      n = (length gs)
      nums = [1..n]
      in map (\x -> "group"++(show x)) nums
    z :: String -> [String] -> (String, [String])
    z a b = (a,b)
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

-- Transform triggers into tasks (more triggers). return (oldtrigs, newtasks)
getTasks :: K3 Declaration -> ([K3 Declaration],[K3 Declaration])
getTasks (Node (DRole r :@: as) cs) = let 
  (trigs,nontrigs) = triggersNonTriggers cs
  newtrigs         = concat $ map splitTrigger trigs
  in (trigs, newtrigs)
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
      in D.trigger newid t e 
    
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
       uid = uidOfAnnos as 
         in Node (EVariable i :@: ((ERead  i uid):as)) (map labelExpression cs)
    labelExpression (Node (EAssign i :@: as) cs) = let
       uid = uidOfAnnos as 
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
  eID = uidOfAnnos as 
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

getTriggerName :: K3 Declaration -> String
getTriggerName (Node (DTrigger n _ _ :@: _) _) = n
getTriggerName _ = error "Not a trigger"

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

uidOfAnnos :: [Annotation Expression] -> UID
uidOfAnnos []            = error "no UID found!"
uidOfAnnos ((EUID x):_) = x 
uidOfAnnos (_:as)        = uidOfAnnos as

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
