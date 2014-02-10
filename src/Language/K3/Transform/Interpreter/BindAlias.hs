{-# LANGUAGE ViewPatterns #-}

-- | Alias analysis for bind expressions.
--   This is used by the interpreter to ensure consistent modification
--   of alias variables following the interpretation of a bind expression.
module Language.K3.Transform.Interpreter.BindAlias (
  labelBindAliases
)
where

import Control.Arrow ( (&&&) )

import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Common
import Language.K3.Core.Expression

labelBindAliases :: K3 Expression -> K3 Expression
labelBindAliases e@(tag &&& children -> (EBindAs b, [s, t])) =
  Node (EBindAs b :@: annotations e) $ [annotateVariableAliases (exprUIDs $ extractReturns s) s, t]
  where exprUIDs       = concatMap asUID . mapMaybe (@~ isEUID)
        asUID (EUID x) = [x]
        asUID _        = []

-- Recur through all other operations.
labelBindAliases (Node t cs) = Node t $ map labelBindAliases cs

-- | Returns subexpression UIDs for return values of the argument expression.
extractReturns :: K3 Expression -> [K3 Expression]
extractReturns (tag &&& children -> (EOperate OSeq, [_, r])) = extractReturns r

extractReturns e@(tag &&& children -> (EProject _, [r])) = [e] ++ extractReturns r

extractReturns (tag &&& children -> (ELetIn i, [_, b])) =
  filter (notElem i . freeVariables) $ extractReturns b

extractReturns (tag &&& children -> (ECaseOf i, [_, s, n])) = 
  (filter (notElem i . freeVariables) $ extractReturns s) ++ extractReturns n

extractReturns (tag &&& children -> (EBindAs b, [_, f])) =
  filter (and . map (`notElem` (bindingVariables b)) . freeVariables) $ extractReturns f

extractReturns (tag &&& children -> (EIfThenElse, [_, t, e])) = concatMap extractReturns [t, e]

-- Structural error cases.
extractReturns (tag -> EProject _)  = error "Invalid project expression"
extractReturns (tag -> ELetIn _)    = error "Invalid let expression"
extractReturns (tag -> ECaseOf _)   = error "Invalid case expression"
extractReturns (tag -> EBindAs _)   = error "Invalid bind expression"
extractReturns (tag -> EIfThenElse) = error "Invalid branch expression"

extractReturns e = [e]

-- | Annotates any variables in the candidate expressions as alias points in the argument K3 expression.
annotateVariableAliases :: [UID] -> K3 Expression -> K3 Expression
annotateVariableAliases candidateExprs = snd . aux
  where 
    aux e@(tag -> EVariable _) =
      case e @~ isEUID of 
        Just (EUID x) -> if x `elem` candidateExprs then (True, e @+ (EAnalysis BindAlias))
                                                    else (False, e)
        
        Just _        -> error "Invalid EUID annotation match"
        Nothing       -> error "No UID found on variable expression"

    aux e@(tag &&& children -> (EProject i, [r])) =
      case e @~ isEUID of
        Just (EUID x) ->  if not(x `elem` candidateExprs && subAlias)
                            then (subAlias, newExpr)
                            else (True, newExpr @+ (EAnalysis $ BindAliasExtension i))
        
        Just _        -> error "Invalid EUID annotation match"
        Nothing       -> error "No UID found on project expression"

      where newExpr             = Node (EProject i :@: annotations e) [subExpr]
            (subAlias, subExpr) = aux r

    aux (Node t cs) = (or x, Node t y)
      where (x,y) = unzip $ map aux cs
