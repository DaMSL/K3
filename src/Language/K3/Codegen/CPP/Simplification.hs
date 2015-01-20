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

simplifyCPPExpression :: Expression -> SimplificationM Expression
simplifyCPPExpression expr =
  case expr of
    Binary i x y -> Binary i <$> simplifyCPPExpression x <*> simplifyCPPExpression y

    Bind f vs 0 -> simplifyCPPExpression (Call f vs)
    Bind f vs n -> do
      f' <- simplifyCPPExpression f
      vs' <- mapM simplifyCPPExpression vs

      case f' of
        Bind f'' vs'' n' -> return (Bind f'' (vs'' ++ vs') n)
        _ -> return (Bind f' vs' n)

    Call f vs -> do
      f' <- simplifyCPPExpression f
      vs' <- mapM simplifyCPPExpression vs

      case f' of
        Bind f'' vs'' n | n == length vs' -> return (Call f'' (vs'' ++ vs'))
        _ -> return (Call f' vs')

    Dereference e -> Dereference <$> simplifyCPPExpression e
    TakeReference e -> TakeReference <$> simplifyCPPExpression e
    Initialization t es -> Initialization t <$> mapM simplifyCPPExpression es
    Lambda cs as m mrt bd -> Lambda cs as m mrt <$> mapM simplifyCPPStatement bd
    Literal lt -> return (Literal lt)
    Project e n -> Project <$> simplifyCPPExpression e <*> return n
    Subscript a x -> Subscript <$> simplifyCPPExpression a <*> simplifyCPPExpression x
    Unary i e -> Unary i <$> simplifyCPPExpression e
    Variable n -> return (Variable n)

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

