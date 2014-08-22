{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Language.K3.Analysis.CArgs where

import Control.Monad.Identity
import Control.Arrow ((&&&))
import Data.Maybe (catMaybes)
import Data.Tree
import Debug.Trace

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Analysis.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Utils.Pretty

-- Map variable names (Function Declarations) to the expected number of arguments in the backend implementation.
type CArgsEnv = [(Identifier, Int)]
type AnnotationCArgsEnv = [(Identifier, CArgsEnv)]

data AllCArgs = AllCArgs CArgsEnv AnnotationCArgsEnv 
                deriving (Eq, Read, Show)

isCArgs :: Annotation Declaration -> Bool
isCArgs (DProperty "CArgs" (Just _)) = True
isCArgs _ = False

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
    makeAnnotationEnvEntry (tag -> (DAnnotation name _ members)) = (name, (concatMap getCArgs members))
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
    annotationContainsCArgs (tag -> DAnnotation _ _ members) = any memberHasCArgs members
    annotationContainsCArgs _ = False

    memberHasCArgs (Lifted _ _ _ _ anns) = any isCArgs anns
    memberHasCArgs _ = False

allCArgs :: K3 Declaration -> AllCArgs
allCArgs prog = AllCArgs globals anns
	where
		globals = globalCArgsEnv prog
		anns = annotationCArgsEnv prog

convertProgram :: K3 Declaration -> K3 Declaration
convertProgram prog = 
	let cargs = allCArgs prog in (trace (show cargs)) $ runIdentity $ mapProgram m_id m_id (\x -> return (transformTriggerExpr cargs x)) prog
  where
  	m_id = (\x -> return x)


propogateCArgs :: K3 Declaration -> AllCArgs -> K3 Declaration
propogateCArgs a b = a

transformTriggerExpr :: AllCArgs -> K3 Expression -> K3 Expression
transformTriggerExpr cargs expr = runIdentity $ modifyTree m_transform expr
	where
		m_transform = return . (transformExpr cargs)  


transformExpr :: AllCArgs -> K3 Expression -> K3 Expression
transformExpr cargs e@(tag &&& children -> (EProject name, [src])) = 
	if (trace "checking") (isTCollection . getEType) src
	then (trace "Hello") (if n > 1 then e @+ (makeCArgs n) else e)
    else (trace "Goodbye") e
  where
  	results = catMaybes $ map (lookupLiftedFun cargs name) (catMaybes $ map toAnnotationId (annotations $ getEType src))
  	n = case results of 
  		  []    -> 1
  		  (x:_) -> x
transformExpr cargs e@(tag -> EVariable x) = if (isGlobal x) then maybeAttachCargs else e
  where
  	isGlobal = const True -- TODO: =D
  	maybeAttachCargs = maybe e (\n -> e @+ (makeCArgs n)) $ lookupGlobalFun cargs x

transformExpr _ e@(tag &&& children -> (EOperate OApp, [l,_]))
  | eCArgs l > 2 = e @+ makeCArgs ((eCArgs l) - 1)
  | otherwise = e
transformExpr _ e = (trace $ "Screw you" ++ (pretty e)) e

toAnnotationId :: Annotation Type -> Maybe Identifier
toAnnotationId tann = 
  case tann of
    TAnnotation i -> Just i
    _ -> Nothing


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

-- | Get the K3 Type of an expression. Relies on type-manifestation to have attached an EType
-- annotation to the expression ahead of time.
getEType :: K3 Expression -> K3 Type
getEType e = case e @~ \case { EType _ -> True; _ -> False } of
    Just (EType t) -> t
    _ -> error $ "Absent type at " ++ show e

isTCollection :: K3 Type -> Bool
isTCollection (tag -> TCollection) = True
isTCollection _ = False


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
