{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Language.K3.Analysis.CArgs (convertProgram, eCArgs, isECArgs) where

import Control.Monad.Identity
import Control.Monad.State
import Control.Arrow ((&&&))
import Data.Maybe (catMaybes)
import Data.Tree
import Data.List (delete)

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Analysis.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

-- Map variable names (Function Declarations) to the expected number of arguments in the backend implementation.
type CArgsEnv           = [(Identifier, Int)]
type AnnotationCArgsEnv = [(Identifier, CArgsEnv)]

--                       globals   lifted attributes
data AllCArgs = AllCArgs CArgsEnv AnnotationCArgsEnv
                deriving (Eq, Read, Show)

type CArgsM a = State AllCArgs a

-- Top Level
convertProgram :: K3 Declaration -> K3 Declaration
convertProgram prog =
  fst $ runState (mapProgram m_id m_id (transformTriggerExpr globals) prog) cargs
  where
    cargs = allCArgs prog
    globals = catMaybes $ map globalVarName (children prog)
    m_id = (\x -> return x)

-- For each trigger expression, perform 3 passes.
-- 1) Attach CArgs to globally declared function call sites
-- 2) Attach CArgs to lifted attribute function call sites
-- 3) Propogate CArgs throughout the tree
transformTriggerExpr :: [Identifier] -> K3 Expression -> CArgsM (K3 Expression)
transformTriggerExpr globals expr =
      attachToGlobals globals expr
  >>= modifyTree attachToLifteds
  >>= modifyTree propogateCArgs

-- First pass: (Full tree transformation) Attach CArgs to globally declared functions
-- Keeping track of shadowing
attachToGlobals :: [Identifier] -> K3 Expression -> CArgsM (K3 Expression)
attachToGlobals gs e = do
  cargs  <- get
  new_e  <- return $ if should_attach then maybeAttachCargs cargs e else e
  new_cs <- mapM (attachToGlobals new_gs) (children e)
  return $ replaceCh new_e new_cs
  where
    new_gs = tryBind e
    should_attach = isGlobal new_gs e
    maybeAttachCargs cargs (tag -> EVariable x) = maybe e (\n -> e @+ (makeCArgs n)) $ lookupGlobalFun cargs x
    maybeAttachCargs _ e' = e'

    -- Return the list of globals that are still un-shadowed
    tryBind ::  K3 Expression -> [Identifier]
    tryBind (tag -> ELetIn i)                  = delete i gs
    tryBind (tag -> ELambda i)                 = delete i gs
    tryBind (tag -> EBindAs (BTuple ns))       = foldl (flip delete) gs ns
    tryBind (tag -> EBindAs (BIndirection i)) = delete i gs
    tryBind (tag -> EBindAs (BRecord tups)) = foldl (\acc -> \(a,b) -> (delete a) . (delete b) $ acc) gs tups
    tryBind _ = gs

    -- Return True if this expression is an EVariable in the provided list of globals
    isGlobal :: [Identifier] -> K3 Expression -> Bool
    isGlobal globs (tag -> EVariable i) = i `elem` globs
    isGlobal _ _                    = False

-- Second Pass: For use with modifyTree
-- Attach CArgs to lifted attributes of Collections
attachToLifteds :: K3 Expression -> CArgsM (K3 Expression)
attachToLifteds e@(tag &&& children -> (EProject name, [src])) = getN >>= \n -> return $
  if isTCollection e_type
    then (if n > 1 then e @+ (makeCArgs n) else e)
    else e
  where
    e_type  = getEType src
    tAnns   = catMaybes $ map tAnnotationId (annotations $ e_type)
    results = get >>= \cargs -> return $ catMaybes $ map (lookupLiftedFun cargs name) tAnns
    getN = results >>= \r -> case r of
        []    -> return 1
        (x:_) -> return x
attachToLifteds e = return e

-- Third Pass: For use with modifyTree
-- Propogate the existing CArgs properties attached to the tree
-- TODO Aliases
propogateCArgs :: K3 Expression -> CArgsM (K3 Expression)
propogateCArgs e@(tag &&& children -> (EOperate OApp, (l:_)))
  | eCArgs l > 0 = return $ e @+ makeCArgs ((eCArgs l) - 1)
  | otherwise    = return e
propogateCArgs e = return e

-- Build the CArgs Environment for a K3 Program. Mapping globally declared function names
-- to the number of arguments expected by the backend implementation (For those expecting >1)
globalCArgsEnv :: K3 Declaration -> CArgsEnv
globalCArgsEnv program = map makeEnvEntry cArgsDecls
  where
    cArgsDecls = fst . runIdentity $ result -- All declarations with an attached 'CArgs' property

    result = foldProgram accumCArgs m_id m_id [] program

    m_id acc x = return (acc, x)

    accumCArgs :: [K3 Declaration] -> K3 Declaration -> Identity ([K3 Declaration], K3 Declaration)
    accumCArgs acc dec = return (if containsCArgs dec then dec:acc else acc, dec)

    extractFunName (tag -> DGlobal n _ _) = n
    extractFunName _ = error "Expecting a global variable (builtin function definition)"

    makeEnvEntry :: K3 Declaration -> (Identifier, Int)
    makeEnvEntry dec = (extractFunName dec, numCArgs (annotations dec))

-- Build the Annotation CArgs Environment for a K3 Program. Mapping Annotation name to a map from function name
-- to the number of arguments expected by the backend implementation (For those expecting >1)
annotationCArgsEnv :: K3 Declaration -> AnnotationCArgsEnv
annotationCArgsEnv program = map makeAnnotationEnvEntry annotationCArgsDecls
  where
    makeAnnotationEnvEntry (tag -> (DDataAnnotation name _ _ members)) = (name, (concatMap getCArgs members))
    makeAnnotationEnvEntry _ = error "Expecting Annotation Declaration"

    annotationCArgsDecls = fst . runIdentity $ result

    result = foldProgram accumAnnotationCArgs m_id m_id [] program

    m_id acc x = return (acc, x)

    accumAnnotationCArgs :: [K3 Declaration] -> K3 Declaration -> Identity ([K3 Declaration], K3 Declaration)
    accumAnnotationCArgs acc dec = return (if annotationContainsCArgs dec then dec:acc else acc, dec)

    getCArgs (Lifted _ ident _ _ anns) =
    	let num = numCArgs anns
    	in if num > 1 then [(ident, numCArgs anns)] else []
    getCArgs _ = []

    annotationContainsCArgs :: K3 Declaration -> Bool
    annotationContainsCArgs (tag -> DDataAnnotation _ _ _ members) = any memberHasCArgs members
    annotationContainsCArgs _ = False

    memberHasCArgs (Lifted _ _ _ _ anns) = any isCArgs anns
    memberHasCArgs _ = False

lookupHelper :: [(Identifier, Int)] -> Identifier -> Maybe Int
lookupHelper xs fun =
  case (filter (\(name, _) -> name == fun) xs) of
    [] -> Nothing
    ((_,n):_) -> Just n

lookupGlobalFun :: AllCArgs -> Identifier -> Maybe Int
lookupGlobalFun (AllCArgs globals _) fun = lookupHelper globals fun

lookupLiftedFun :: AllCArgs -> Identifier -> Identifier -> Maybe Int
lookupLiftedFun (AllCArgs _ anncargs) fun ann =
	case filter (\(name, _) -> name == ann) anncargs of
		[] -> Nothing
		((_,env):_) -> lookupHelper env fun

-- Utilities
globalVarName :: K3 Declaration -> Maybe Identifier
globalVarName (tag -> DGlobal x _ _) = Just x
globalVarName _ = Nothing

-- Get the K3 Type of an expression. Relies on type-manifestation to have attached an EType
-- annotation to the expression ahead of time.
getEType :: K3 Expression -> K3 Type
getEType e = case e @~ \case { EType _ -> True; _ -> False } of
    Just (EType t) -> t
    _ -> error $ "Absent type at " ++ show e

tAnnotationId :: Annotation Type -> Maybe Identifier
tAnnotationId tann =
  case tann of
    TAnnotation i -> Just i
    _ -> Nothing

isTCollection :: K3 Type -> Bool
isTCollection (tag -> TCollection) = True
isTCollection _ = False

-- CArgs Utilities:
allCArgs :: K3 Declaration -> AllCArgs
allCArgs prog = AllCArgs globals anns
  where
    globals = globalCArgsEnv prog
    anns = annotationCArgsEnv prog

containsCArgs :: K3 Declaration -> Bool
containsCArgs (annotations -> anns) = any isCArgs anns

-- Look for a CArgs property specifying the number of arguments expected by the backend implementation of a declared function.
-- Functions without a CArgs property default to 1.
numCArgs :: [Annotation Declaration] -> Int
numCArgs anns =
  case filter isCArgs anns of
        ((DProperty "CArgs" (Just literal)):_) -> extractN literal
        _ -> 1
  where
    extractN (tag -> (LInt n)) = n
    extractN _ = error "Invalid Literal for CArgs Property. Specify an Int."

isCArgs :: Annotation Declaration -> Bool
isCArgs (DProperty "CArgs" (Just _)) = True
isCArgs _ = False

makeCArgs :: Int -> Annotation Expression
makeCArgs n = EProperty "CArgs" (Just lit)
  where lit = Node ((LInt n) :@: []) []

-- Look for a CArgs property specifying the number of arguments expected by the backend implementation of a declared function.
-- Functions without a CArgs property default to 1.
eCArgs :: K3 Expression -> Int
eCArgs (annotations -> anns) =
	case filter isECArgs anns of
        ((EProperty "CArgs" (Just literal)):_) -> extractN literal
        _ -> 1
  where
  	extractN (tag -> (LInt n)) = n
  	extractN _ = error "Invalid Literal for CArgs Property. Specify an Int."

isECArgs :: Annotation Expression -> Bool
isECArgs (EProperty "CArgs" (Just _)) = True
isECArgs _ = False
