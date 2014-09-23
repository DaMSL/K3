{-# LANGUAGE ViewPatterns #-}

-- | A program analysis to construct a directed graph of annotation provisions.
module Language.K3.Analysis.AnnotationGraph where

import Data.Graph.Wrapper ( Graph )
import qualified Data.Graph.Wrapper as G

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Type

type GraphExtractor a = Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> [AnnMemDecl] -> (a, [Identifier])

annotationGraph :: GraphExtractor a -> K3 Declaration -> (Graph Identifier a)
annotationGraph annotationF prog = G.fromList $ maybe [] id $ foldMapTree concatChildAcc [] prog
  where concatChildAcc childAccs (tag -> DDataAnnotation n svars tvars mems) =
          let (node, edges) = annotationF n svars tvars mems
          in Just $ [(n, node, edges)] ++ concat childAccs
        concatChildAcc childAccs _ = Just $ concat childAccs

providesGraph :: K3 Declaration -> Graph Identifier ([TypeVarDecl], [AnnMemDecl])
providesGraph prog = annotationGraph extractProvides prog
  where extractProvides _ [] tvars mems = (\(x,y) -> ((tvars,y), x)) $ foldl memberOrProvide ([], []) mems
        extractProvides n _ _ _ = error $ unwords ["Invalid annotation for providesGraph", n, ":", "non-empty splice parameters"]
        memberOrProvide (provideAcc, memberAcc) (MAnnotation Provides n _) = (n:provideAcc,     memberAcc)
        memberOrProvide (provideAcc, memberAcc) (MAnnotation _ _ _)        = (  provideAcc,     memberAcc)
        memberOrProvide (provideAcc, memberAcc) mem                        = (  provideAcc, mem:memberAcc)

requiresGraph :: K3 Declaration -> Graph Identifier ([TypeVarDecl], [AnnMemDecl])
requiresGraph prog = annotationGraph extractRequires prog
  where extractRequires _ [] tvars mems = (\(x,y) -> ((tvars,y), x)) $ foldl memberOrRequire ([], []) mems
        extractRequires n _ _ _ = error $ unwords ["Invalid annotation for requiresGraph", n, ":", "non-empty splice parameters"]
        memberOrRequire (requireAcc, memberAcc) (MAnnotation Requires n _) = (n:requireAcc,     memberAcc)
        memberOrRequire (requireAcc, memberAcc) (MAnnotation _ _ _)        = (  requireAcc,     memberAcc)
        memberOrRequire (requireAcc, memberAcc) mem                        = (  requireAcc, mem:memberAcc)

annotationProvides :: K3 Declaration -> [(Identifier, Identifier)]
annotationProvides prog = G.edges $ providesGraph prog

annotationRequires :: K3 Declaration -> [(Identifier, Identifier)]
annotationRequires prog = G.edges $ requiresGraph prog

-- | Extracts annotation definitions from the given K3 program,
--   returning flattened definitions with all subannotation provisions inlined.
flattenAnnotations :: K3 Declaration -> [(Identifier, [([TypeVarDecl], [AnnMemDecl])])]
flattenAnnotations prog = foldl flattenAnnotation [] $ G.vertices pGraph
  where pGraph = providesGraph prog
        flattenAnnotation annAcc annId = annAcc ++ [(annId, foldl concatMembers [] $ G.reachableVertices pGraph annId)]
        concatMembers memAcc annId = memAcc ++ [G.vertex pGraph annId]
