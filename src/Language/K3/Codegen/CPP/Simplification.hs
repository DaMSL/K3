{-# LANGUAGE TupleSections #-}

module Language.K3.Codegen.CPP.Simplification (
  simplifyCPP,
) where

import Data.List (isPrefixOf, sortBy)
import Data.Ord (comparing)

import Control.Monad.Identity
import Control.Monad.State

import qualified Data.Map as M

import Language.K3.Codegen.CPP.Representation

data SimplificationS = SimplificationS { rTypeCtr :: Int
                                       , simpleTypeMap :: M.Map Name Name
                                       , simplifyTypesInContext :: Bool
                                       }

bumpPopRCtr :: SimplificationM Int
bumpPopRCtr = do
  s <- get
  put $ s { rTypeCtr = succ (rTypeCtr s) }
  return $ rTypeCtr s

withSimplifiedTypes :: SimplificationM a -> SimplificationM a
withSimplifiedTypes m = do
  g <- gets simplifyTypesInContext
  r <- withState (\s -> s { simplifyTypesInContext = True }) m
  modify $ \s -> s { simplifyTypesInContext = g }
  return r

addTypeAlias :: Name -> Name -> SimplificationM ()
addTypeAlias old new = modify $ \s -> s { simpleTypeMap = M.insert old new (simpleTypeMap s) }

flushCurrentAliases :: SimplificationM [Definition]
flushCurrentAliases = do
  stMap <- gets simpleTypeMap
  return $ map (\(old, new) -> GlobalDefn $ Forward $ UsingDecl (Right new) (Just old))
               (sortBy (comparing snd) $ M.toList stMap)

type SimplificationM = StateT SimplificationS Identity

runSimplificationM :: SimplificationM a -> SimplificationS -> (a, SimplificationS)
runSimplificationM s t = runIdentity $ runStateT s t

defaultSimplificationS :: SimplificationS
defaultSimplificationS = SimplificationS { rTypeCtr = 0
                                         , simpleTypeMap = M.empty
                                         , simplifyTypesInContext = False
                                         }

simplifyCPP :: [Definition] -> [Definition]
simplifyCPP ds = fst $ runSimplificationM (simplifyCPPProgram ds) defaultSimplificationS

simplifyCPPProgram :: [Definition] -> SimplificationM [Definition]
simplifyCPPProgram ds = concat <$> mapM simplifyCPPDefinitionWithAliases ds

simplifyCPPType :: Type -> SimplificationM Type
simplifyCPPType k =
  case k of
    Const t -> Const <$> simplifyCPPType t
    Function ts t -> Function <$> mapM simplifyCPPType ts <*> simplifyCPPType t
    Pointer t -> Pointer <$> simplifyCPPType t
    Reference t -> Reference <$> simplifyCPPType t
    RValueReference t -> RValueReference <$> simplifyCPPType t
    Static t -> Static <$> simplifyCPPType t
    Named n -> Named <$> simplifyCPPName n
    _ -> return k

simplifyCPPName :: Name -> SimplificationM Name
simplifyCPPName n = do
  g <- gets simplifyTypesInContext
  if not g then return n else case n of
    Specialized ts (Name sn)
      | "R_" `isPrefixOf` sn -> do
        stMap <- simpleTypeMap <$> get
        case M.lookup n stMap of
          Just t -> return t
          Nothing -> do
            i <- bumpPopRCtr
            let alias = Name $ "R" ++ show i
            addTypeAlias n alias
            return alias
      | otherwise -> mapM simplifyCPPType ts >>= \ts' -> return (Specialized ts' (Name sn))
    Specialized ts nn -> Specialized <$> mapM simplifyCPPType ts <*> simplifyCPPName nn
    Qualified fn sn -> Qualified <$> simplifyCPPName fn <*> simplifyCPPName sn
    Name _ -> return n

simplifyCPPExpression :: Expression -> SimplificationM Expression
simplifyCPPExpression expr =
  case expr of
    Binary i x y -> Binary i <$> simplifyCPPExpression x <*> simplifyCPPExpression y

    Bind f vs 0 -> simplifyCPPExpression (Call f vs)
    Bind f vs n -> do
      f' <- simplifyCPPExpression f
      vs' <- mapM simplifyCPPExpression vs

      case f' of
        Bind f'' vs'' _ -> return (Bind f'' (vs'' ++ vs') n)
        _ -> return (Bind f' vs' n)

    Call f vs -> do
      f' <- simplifyCPPExpression f
      vs' <- mapM simplifyCPPExpression vs

      case f' of
        Bind f'' vs'' n | n == length vs' -> return (Call f'' (vs'' ++ vs'))
        _ -> return (Call f' vs')

    Dereference e -> Dereference <$> simplifyCPPExpression e
    TakeReference e -> TakeReference <$> simplifyCPPExpression e
    Initialization t es -> Initialization <$> simplifyCPPType t <*> mapM simplifyCPPExpression es
    Lambda cs as m mrt bd -> do
      as' <- mapM (\(i, t) -> (i,) <$> simplifyCPPType t) as
      mrt' <- mapM simplifyCPPType mrt
      Lambda cs as' m mrt' <$> mapM simplifyCPPStatement bd
    Literal lt -> return (Literal lt)
    Project e n -> Project <$> simplifyCPPExpression e <*> simplifyCPPName n
    Subscript a x -> Subscript <$> simplifyCPPExpression a <*> simplifyCPPExpression x
    Unary i e -> Unary i <$> simplifyCPPExpression e
    Variable n -> Variable <$> simplifyCPPName n
    ExprOnType t -> return $ ExprOnType t

simplifyCPPDeclaration :: Declaration -> SimplificationM Declaration
simplifyCPPDeclaration decl =
  case decl of
    FunctionDecl n ts t -> FunctionDecl n <$> mapM simplifyCPPType ts <*> simplifyCPPType t
    ScalarDecl n Inferred (Just (Literal (LString s))) ->
      return $ ScalarDecl n Inferred (Just $ Initialization (Named $ Qualified (Name "K3") (Name "base_string"))
                                               [Literal $ LString s])
    ScalarDecl n t (Just e) -> simplifyCPPType t >>= \t' -> ScalarDecl n t' . Just <$> simplifyCPPExpression e
    TemplateDecl imts d -> TemplateDecl <$> mapM (\(i, mt) -> (i,) <$> mapM simplifyCPPType mt) imts
                                                                   <*> simplifyCPPDeclaration d
    _ -> return decl

simplifyCPPStatement :: Statement -> SimplificationM Statement
simplifyCPPStatement stmt =
  case stmt of
    Assignment x y -> Assignment <$> simplifyCPPExpression x <*> simplifyCPPExpression y
    Block ss -> Block <$> mapM simplifyCPPStatement ss
    Comment s -> return (Comment s)
    ForEach i t e s -> ForEach i <$> simplifyCPPType t <*> simplifyCPPExpression e <*> simplifyCPPStatement s
    Forward d -> Forward <$> simplifyCPPDeclaration d
    IfThenElse p ts es -> IfThenElse <$> simplifyCPPExpression p <*> mapM simplifyCPPStatement ts
                                                                 <*> mapM simplifyCPPStatement es
    Ignore e -> Ignore <$> simplifyCPPExpression e
    Pragma s -> return (Pragma s)
    Return e -> Return <$> simplifyCPPExpression e

simplifyCPPDefinition :: Definition -> SimplificationM Definition
simplifyCPPDefinition defn =
  case defn of
    ClassDefn cn ts ps publics privates protecteds ->
      ClassDefn cn ts ps <$> mapM simplifyCPPDefinition publics
                         <*> mapM simplifyCPPDefinition privates
                         <*> mapM simplifyCPPDefinition protecteds
    FunctionDefn fn as mrt is c bd -> do
      as' <- mapM (\(i, t) -> (i,) <$> simplifyCPPType t) as
      mrt' <- mapM simplifyCPPType mrt
      FunctionDefn fn as' mrt' is c <$> mapM simplifyCPPStatement bd
    GlobalDefn s -> GlobalDefn <$> simplifyCPPStatement s
    GuardedDefn i d -> GuardedDefn i <$> simplifyCPPDefinition d
    IncludeDefn i -> return (IncludeDefn i)
    NamespaceDefn i ds -> NamespaceDefn i <$> mapM simplifyCPPDefinition ds
    TemplateDefn imts d -> TemplateDefn <$> mapM (\(i, mt) -> (i,) <$> mapM simplifyCPPType mt) imts
                                        <*> simplifyCPPDefinition d
    TypeDefn t i -> return $ TypeDefn t i

simplifyCPPDefinitionWithAliases :: Definition -> SimplificationM [Definition]
simplifyCPPDefinitionWithAliases d = case d of
  ClassDefn cn _ _ _ _ _ | cn == Name "__global_context" -> do
                             d' <- withSimplifiedTypes (simplifyCPPDefinition d)
                             ds <- flushCurrentAliases
                             return (ds ++ [d'])
  _ -> (:[]) <$> simplifyCPPDefinition d
