{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- Common utilities in writing program transformations.
module Language.K3.Analysis.Common where

import Control.Arrow
import Control.Monad.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

-- | Fold a declaration and expression reducer and accumulator over the given program.
foldProgram :: (Monad m)
            => (a -> K3 Declaration -> m (a, K3 Declaration))
            -> (a -> AnnMemDecl     -> m (a, AnnMemDecl))
            -> (a -> K3 Expression  -> m (a, K3 Expression))
            -> a -> K3 Declaration
            -> m (a, K3 Declaration)
foldProgram declF annMemF exprF a prog = foldRebuildTree rebuildDecl a prog
  where
    rebuildDecl acc ch (tag &&& annotations -> (DGlobal i t eOpt, anns)) = do
      (acc2, neOpt) <- rebuildInitializer acc eOpt
      declF acc2 $ Node (DGlobal i t neOpt :@: anns) ch

    rebuildDecl acc ch (tag &&& annotations -> (DTrigger i t e, anns)) = do
      (acc2, ne) <- exprF acc e
      declF acc2 $ Node (DTrigger i t ne :@: anns) ch

    rebuildDecl acc ch (tag &&& annotations -> (DAnnotation i tVars mems, anns)) = do
      (acc2, nMems) <- foldM rebuildAnnMem (acc, []) mems
      declF acc2 $ Node (DAnnotation i tVars nMems :@: anns) ch

    rebuildDecl acc ch (Node t _) = declF acc $ Node t ch

    rebuildAnnMem (acc, memAcc) (Lifted p n t eOpt anns) =
      rebuildMem acc memAcc eOpt $ \neOpt -> Lifted p n t neOpt anns
    
    rebuildAnnMem (acc, memAcc) (Attribute p n t eOpt anns) =
      rebuildMem acc memAcc eOpt $ \neOpt -> Attribute p n t neOpt anns
    
    rebuildAnnMem (acc, memAcc) (MAnnotation p n anns) = do
      (acc2, nMem) <- annMemF acc $ MAnnotation p n anns
      return (acc2, memAcc ++ [nMem])
    
    rebuildMem acc memAcc eOpt rebuildF = do
      (acc2, neOpt) <- rebuildInitializer acc eOpt
      (acc3, nMem)  <- annMemF acc2 $ rebuildF neOpt
      return (acc3, memAcc ++ [nMem])

    rebuildInitializer acc eOpt =
      maybe (return (acc, Nothing)) (\e -> exprF acc e >>= return . fmap Just) eOpt


-- | Fold a function and accumulator over all expressions in the given program.
foldExpression :: (Monad m)
                => (a -> K3 Expression -> m (a, K3 Expression)) -> a -> K3 Declaration
                -> m (a, K3 Declaration)
foldExpression exprF a prog = foldProgram returnPair returnPair exprF a prog
  where returnPair x y = return (x,y)

-- | Map a function over all expressions in the program tree.
mapExpression :: (Monad m)
              => (K3 Expression -> m (K3 Expression)) -> K3 Declaration -> m (K3 Declaration)
mapExpression exprF prog = foldProgram returnPair returnPair wrapExprF () prog >>= return . snd
  where returnPair a b = return (a,b)
        wrapExprF a e = exprF e >>= return . (a,)


{-
type BindingEnv a = Map Identifier a

data BindingEnvExtender a b
      = BindingEnvExtender
          { extract :: BindingEnv a -> K3 b -> Maybe (Identifier, a)
          , extend  :: Identifier -> a -> BindingEnv a -> BindingEnv a }

type BEnvDeclExtender a = BindingEnvExtender a Declaration
type BEnvExprExtender a = BindingEnvExtender a Expression

-- | A generic analysis routine.
-- TODO: make this a type class?
analyzeProgram :: (Monad m)
               => (env -> K3 Declaration -> m (env, K3 Declaration))
               -> (env -> AnnMemDecl     -> m (env, AnnMemDecl))
               -> (env -> K3 Expression  -> m (env, K3 Expression))
               -> env -> K3 Declaration -> m (K3 Declaration)
analyzeProgram declF memF exprF initEnv prog =
  foldProgram declF memF exprF initEnv prog >>= return . snd

analyzeExpression :: (Monad m)
                  => env -> K3 Expression -> m (env, K3 Expression)
analyzeExpression preCh1F postCh1F mergeF allChF expr =
  foldIn1RebuildTree preCh1F postCh1F mergeF allChF env expr


analyzeWithEnv :: AnalysisEnvExtender a b
               -> (AnalysisEnv a -> c -> K3 b -> c) 
               -> AnalysisEnv a -> c -> K3 b -> c
analyzeWithEnv extender analyzeF initEnv initAcc tree =


type Matcher2 node a b c   = AnalysisEnv a -> [Annotation node] -> b -> c -> Maybe (Identifier, a) 
type Matcher3 node a b c d = AnalysisEnv a -> [Annotation node] -> b -> c -> d -> Maybe (Identifier, a) 

type DGlobalMatcher  a = Matcher3 Declaration a Identifier (K3 Type) (Maybe (K3 Expression))
type DTriggerMatcher a = Matcher3 Declaration a Identifier (K3 Type) (K3 Expression)
type DAnnotMatcher   a = Matcher3 Declaration a Identifier [TypeVarDecl] [AnnMemDecl]

type ELambdaMatcher a = Matcher2 Expression a Identifier (K3 Expression)
type ELetInMatcher  a = Matcher3 Expression a Identifier (K3 Expression) (K3 Expression)
type EBindAsMatcher a = Matcher3 Expression a Binder     (K3 Expression) (K3 Expression)
type ECaseOfMatcher a = Matcher3 Expression a Identifier (K3 Expression) (K3 Expression)

scopeExtender :: ELambdaMatcher a -> ELetInMatcher a -> EBindAsMatcher a -> ECaseOfMatcher a
              -> (Identifier -> a -> AnalysisEnv a -> AnalysisEnv a)
              -> AEnvExprExtender a
scopeExtender lambdaF letInF bindAsF caseOfF extendF =
    AnalysisEnvExtender extractF extendOptF
  where extractF env (details -> (ELambda i, [b],   anns)) = lambdaF env anns i b
        extractF env (details -> (ELetIn  i, [e,b], anns)) = letInF  env anns i e b
        extractF env (details -> (EBindAs b, [s,e], anns)) = bindAsF env anns b s e
        extractF env (details -> (ECaseOf i, [s,n], anns)) = caseOfF env anns i s n
        extractF env _                                     = Nothing

        extendOptF k (Just v) env = extendF k v env
        extendOptF k Nothing  env = env

scopeUniqueExtender :: ELambdaMatcher a -> ELetInMatcher a -> EBindAsMatcher a -> ECaseOfMatcher a
                    -> AEnvExprExtender a
scopeUniqueExtender lambdaF letInF bindAsF caseOfF =
  scopeExtender lambdaF letInF bindAsF caseOfF insert

scopeMergeExtender :: ELambdaMatcher [a] -> ELetInMatcher [a] -> EBindAsMatcher [a] -> ECaseOfMatcher [a]
                   -> AEnvExprExtender [a]
scopeMergeExtender lambdaF letInF bindAsF caseOfF =
    scopeExtender lambdaF letInF bindAsF caseOfF merge
  where merge k v env = insertWith (++) k v env

-}