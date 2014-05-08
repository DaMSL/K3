{-# LANGUAGE ViewPatterns #-}

-- | Alias analysis for bind expressions.
--   This is used by the interpreter to ensure consistent modification
--   of alias variables following the interpretation of a bind expression.
module Language.K3.Transform.Interpreter.BindAlias (
    labelBindAliases
  , labelBindAliasesExpr
)
where

import Control.Arrow ( (&&&) )

import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

labelBindAliases :: K3 Declaration -> K3 Declaration
labelBindAliases prog = snd $ labelDecl 0 prog
  where
    threadCntChildren cnt ch ch_f f =
      f $ (\(x,y) -> (maxCnt cnt x, y)) . unzip $ foldl (threadCnt ch_f cnt) [] ch

    withDeclChildren cnt ch = threadCntChildren cnt ch labelDecl
    withAnnMems cnt mems    = threadCntChildren cnt mems labelAnnMem id

    labelDecl :: Int -> K3 Declaration -> (Int, K3 Declaration)
    labelDecl cnt d@(tag &&& children -> (DGlobal n t eOpt, ch)) = 
      withDeclChildren cnt ch (\(ncnt,nch) -> 
        let (ncnt2, neOpt) = maybe (ncnt, Nothing) (fmap Just . labelBindAliasesExpr ncnt) eOpt
        in (ncnt2, Node (DGlobal n t neOpt :@: annotations d) nch))

    labelDecl cnt d@(tag &&& children -> (DTrigger n t e, ch)) =
      withDeclChildren cnt ch (\(ncnt,nch) ->
        let (ncnt2, ne) = labelBindAliasesExpr ncnt e
        in (ncnt2, Node (DTrigger n t ne :@: annotations d) nch))

    labelDecl cnt d@(tag &&& children -> (DAnnotation n tVars annMems, ch)) =
      withDeclChildren cnt ch (\(ncnt,nch) -> 
        let (ncnt2, nAnnMems) = withAnnMems ncnt annMems
        in (ncnt2, Node (DAnnotation n tVars nAnnMems :@: annotations d) nch))

    labelDecl cnt (Node t ch) = withDeclChildren cnt ch (\(ncnt,nch) -> (ncnt, Node t nch))

    labelAnnMem :: Int -> AnnMemDecl -> (Int, AnnMemDecl)
    labelAnnMem cnt (Lifted p n t eOpt uid) =
      let (ncnt, neOpt) = maybe (cnt, Nothing) (fmap Just . labelBindAliasesExpr cnt) eOpt
      in (ncnt, Lifted p n t neOpt uid)
    
    labelAnnMem cnt (Attribute p n t eOpt uid) =
      let (ncnt, neOpt) = maybe (cnt, Nothing) (fmap Just . labelBindAliasesExpr cnt) eOpt
      in (ncnt, Attribute p n t neOpt uid)

    labelAnnMem cnt annMem = (cnt, annMem)

    threadCnt f cnt acc c = acc++[f (maxCnt cnt $ map fst acc) c]
    maxCnt i l            = if l == [] then i else last l


labelBindAliasesExpr :: Int -> K3 Expression -> (Int, K3 Expression)
labelBindAliasesExpr counter expr = labelExpr counter expr
  where 
    labelExpr cnt e@(tag &&& children -> (EBindAs b, [s, t])) =
      (ncnt2, Node (EBindAs b :@: annotations e) [ns, nt])
      where (ncnt, ns)     = labelProxyPath $ labelExpr cnt s
            (ncnt2, nt)    = labelExpr ncnt t

    labelExpr cnt e@(tag &&& children -> (ECaseOf i, [c, s, n])) =
      (ncnt3, Node (ECaseOf i :@: annotations e) [nc, ns, nn])
      where (ncnt, nc)  = labelProxyPath $ labelExpr cnt c
            (ncnt2, ns) = labelExpr ncnt s
            (ncnt3, nn) = labelExpr ncnt2 n

    -- Recur through all other operations.
    labelExpr cnt (Node t cs) =
      (\(x,y) -> (maxCnt cnt x, Node t y)) $ unzip $ foldl (threadCnt labelExpr cnt) [] cs
    
    labelProxyPath (i, e) = annotateAliases i (exprUIDs $ extractReturns e) e
      where exprUIDs       = concatMap asUID . mapMaybe (@~ isEUID)
            asUID (EUID x) = [x]
            asUID _        = []

    threadCnt f cnt acc c = acc++[f (maxCnt cnt $ map fst acc) c]
    maxCnt i l          = if l == [] then i else last l

-- | Returns subexpression UIDs for return values of the argument expression.
extractReturns :: K3 Expression -> [K3 Expression]
extractReturns (tag &&& children -> (EOperate OSeq, [_, r])) = extractReturns r

extractReturns e@(tag &&& children -> (EProject _, [r])) = [e] ++ extractReturns r

extractReturns e@(tag &&& children -> (ELetIn i, [_, b])) =
  let r = filter (notElem i . freeVariables) $ extractReturns b
  in if r == [] then [e] else r

extractReturns e@(tag &&& children -> (ECaseOf i, [_, s, n])) = 
  let r    = extractReturns s
      depR = filter (notElem i . freeVariables) r
  in (if r /= depR then [e] else r ++ extractReturns n)

extractReturns e@(tag &&& children -> (EBindAs b, [_, f])) =
  let r = filter (and . map (`notElem` (bindingVariables b)) . freeVariables) $ extractReturns f
  in if r == [] then [e] else r

extractReturns (tag &&& children -> (EIfThenElse, [_, t, e])) = concatMap extractReturns [t, e]

-- Structural error cases.
extractReturns (tag -> EProject _)  = error "Invalid project expression"
extractReturns (tag -> ELetIn _)    = error "Invalid let expression"
extractReturns (tag -> ECaseOf _)   = error "Invalid case expression"
extractReturns (tag -> EBindAs _)   = error "Invalid bind expression"
extractReturns (tag -> EIfThenElse) = error "Invalid branch expression"

extractReturns e = [e]

-- | Annotates variables and temporaries in the candidate expressions as 
--   alias points in the argument K3 expression.
annotateAliases :: Int -> [UID] -> K3 Expression -> (Int, K3 Expression)
annotateAliases i candidateExprs expr = (\((j, _), ne) -> (j, ne)) $ annotate i expr
  where
    annotate :: Int -> K3 Expression -> ((Int, Bool), K3 Expression)
    annotate cnt e@(tag -> EVariable v) = withEUID e (\x ->
      if x `elem` candidateExprs then ((cnt, True), e @+ (EAnalysis $ BindAlias v))
                                 else ((cnt, False), e))

    annotate cnt e@(tag &&& children -> (EProject f, [r])) = withEUID e (\x ->
      if not(x `elem` candidateExprs && subAlias)
        then ((ncnt, subAlias), newExpr)
        else ((ncnt, True), newExpr @+ (EAnalysis $ BindAliasExtension f)))

      where newExpr = Node (EProject f :@: annotations e) [subExpr]
            ((ncnt, subAlias), subExpr) = annotate cnt r

    annotate cnt e@(Node t cs) = withEUID e (\uid -> 
      if uid `elem` candidateExprs
        then ((ncnt+1, True), (Node t subExprs) @+ (EAnalysis . BindFreshAlias $ "tmp"++(show ncnt)))
        else ((ncnt, subAlias), Node t subExprs))
      
      where (ncnt, subAlias) = (if x == [] then cnt else fst $ last x, or $ map snd x)
            (x,subExprs)     = unzip $ foldl threadCnt [] cs
            threadCnt acc c  = acc++[annotate (if acc == [] then cnt else (fst . fst . last $ acc)) c]

    withEUID e f    = case e @~ isEUID of 
      Just (EUID x) -> f x
      Just _        -> error "Invalid EUID annotation match"
      Nothing       -> error "No UID found on variable expression"
