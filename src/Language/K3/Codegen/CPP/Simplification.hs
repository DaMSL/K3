module Language.K3.Codegen.CPP.Simplification (
  simplifyCPP,
) where

import Control.Applicative

import Control.Monad.Identity
import Control.Monad.State

import Language.K3.Codegen.CPP.Representation

type SimplificationS = ()
type SimplificationM = StateT SimplificationS Identity

runSimplificationM :: SimplificationM a -> SimplificationS -> (a, SimplificationS)
runSimplificationM s t = runIdentity $ runStateT s t

simplifyCPP :: [Definition] -> [Definition]
simplifyCPP ds = fst $ runSimplificationM (mapM simplifyCPPDefinition ds) ()


simplifyCPPStatement :: Statement -> SimplificationM Statement
simplifyCPPStatement stmt =
  case stmt of
    Assignment x y -> Assignment <$> simplifyCPPExpression x <*> simplifyCPPExpression y
    Block ss -> Block <$> mapM simplifyCPPStatement ss
    ForEach i t e s -> ForEach i t <$> simplifyCPPExpression e <*> simplifyCPPStatement s
    Forward d -> return (Forward d)
    IfThenElse p ts es -> IfThenElse <$> simplifyCPPExpression p <*> mapM simplifyCPPStatement ts
                                                                 <*> mapM simplifyCPPStatement es
    Ignore e -> Ignore <$> simplifyCPPExpression e
    Pragma s -> return (Pragma s)
    Return e -> Return <$> simplifyCPPExpression e

simplifyCPPDefinition :: Definition -> SimplificationM Definition
simplifyCPPDefinition defn =
  case defn of
    ClassDefn cn ts ps publics privates protecteds -> ClassDefn cn ts ps <$> mapM simplifyCPPDefinition publics
                                                                         <*> mapM simplifyCPPDefinition privates
                                                                         <*> mapM simplifyCPPDefinition protecteds
    FunctionDefn fn as mrt is c bd -> FunctionDefn fn as mrt is c <$> mapM simplifyCPPStatement bd
    GlobalDefn s -> GlobalDefn <$> simplifyCPPStatement s
    GuardedDefn i d -> GuardedDefn i <$> simplifyCPPDefinition d
    IncludeDefn i -> return (IncludeDefn i)
    NamespaceDefn i ds -> NamespaceDefn i <$> mapM simplifyCPPDefinition ds
    TemplateDefn is d -> TemplateDefn is <$> simplifyCPPDefinition d

