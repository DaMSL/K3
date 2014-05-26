{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Hindley-Milner type inference.
--
--   For K3, this does not support subtyping, nor does it check annotation
--   composition. Instead it treats annotated collections as ad-hoc external types,
--   and requires structural type equality to hold on function application.
--   Both of these functionalities are not required for K3-Mosaic code.
--
--   Most of the scaffolding is taken from Oleg Kiselyov's tutorial:
--   http://okmij.org/ftp/Computation/FLOLAC/TInfLetP.hs
--
module Language.K3.Analysis.HMTypes.Inference where

import Control.Monad.Identity
import Control.Monad.State
import Control.Monad.Trans.Either

import Data.List
import Data.Map ( Map )
import qualified Data.Map as Map
import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Analysis.Common
import Language.K3.Analysis.HMTypes.DataTypes

-- | Misc. helpers
(.+) :: K3 Expression -> K3 QType -> K3 Expression
(.+) e qt = e @+ (EQType $ mutabilityE e qt)

infixr 5 .+

immutQT :: K3 QType -> K3 QType
immutQT = (@+ QTImmutable)

mutQT :: K3 QType -> K3 QType
mutQT = (@+ QTMutable)

mutabilityE :: K3 Expression -> K3 QType -> K3 QType
mutabilityE e qt = case e @~ isEQualified of
                     Nothing -> qt
                     Just EImmutable -> immutQT qt
                     Just EMutable   -> mutQT qt
                     _ -> error "Invalid qualifier annotation"

-- | A type environment.
type TEnv = [(Identifier, QPType)]

-- | Annotation type environment
type TAEnv = Map Identifier TEnv

-- | Type inference environment
type TIEnv = (TEnv, TAEnv)

-- | A type variable environment.
data TVEnv = TVEnv QTVarId (Map QTVarId (K3 QType)) deriving Show

-- | A state monad for the type variable environment.
type TVEnvM = EitherT String (State TVEnv)

{- TEnv helpers -}
tenv0 :: TEnv
tenv0 = []

tlkup :: TEnv -> Identifier -> Either String QPType
tlkup env x = maybe err Right $ lookup x env 
 where err = Left $ "Unbound variable in type environment: " ++ x

text :: TEnv -> Identifier -> QPType -> TEnv
text env x t = (x,t) : env


{- TAEnv helpers -}
taenv0 :: TAEnv 
taenv0 = Map.empty

talkup :: TAEnv -> Identifier -> Either String TEnv
talkup env x = maybe err Right $ Map.lookup x env
  where err = Left $ "Unbound variable in annotation environment: " ++ x

taext :: TAEnv -> Identifier -> TEnv -> TAEnv
taext env x te = Map.insert x te env


{- TIEnv helpers -}
tilkupe :: TIEnv -> Identifier -> Either String QPType
tilkupe (te,_) x = tlkup te x

tilkupa :: TIEnv -> Identifier -> Either String TEnv
tilkupa (_,ta) x = talkup ta x

tiexte :: TIEnv -> Identifier -> QPType -> TIEnv
tiexte (te,ta) x t = (text te x t, ta)

tiexta :: TIEnv -> Identifier -> TEnv -> TIEnv
tiexta (te,ta) x ate = (te, taext ta x ate) 


{- TVEnvM helpers -}

-- Allocate a fresh type variable
newtv :: TVEnvM (K3 QType)
newtv = do
 TVEnv n s <- get
 put (TVEnv (succ n) s)
 return $ tvar n

tvenv0 :: TVEnv
tvenv0 = TVEnv 0 Map.empty

tvlkup :: TVEnv -> QTVarId -> Maybe (K3 QType)
tvlkup (TVEnv _ s) v = Map.lookup v s

tvext :: TVEnv -> QTVarId -> K3 QType -> TVEnv
tvext (TVEnv c s) v t = TVEnv c $ Map.insert v t s

-- TVE domain predicate: check to see if a TVarName is in the domain of TVE
tvdomainp :: TVEnv -> QTVarId -> Bool
tvdomainp (TVEnv _ s) v = Map.member v s

-- Give the list of all type variables that are allocated in TVE but
-- not bound there
tvfree :: TVEnv -> [QTVarId]
tvfree (TVEnv c s) = filter (\v -> not (Map.member v s)) [0..c-1]

-- Deep substitute, throughout type structure
tvsub :: TVEnv -> K3 QType -> K3 QType
tvsub tve qt = runIdentity $ mapTree sub qt
  where
    sub ch t@(tag -> QTCon d) = return $ foldl (@+) (tdata d ch) $ annotations t
    sub _    (tag -> QTVar v) | Just t <- tvlkup tve v = return $ tvsub tve t
    sub _ t = return t

-- `Shallow' substitution
tvchase :: TVEnv -> K3 QType -> K3 QType
tvchase tve (tag -> QTVar v) | Just t <- tvlkup tve v = tvchase tve t
tvchase _ t = t

-- Compute (quite unoptimally) the characteristic function of the set 
--  forall tvb \in fv(tve_before). Union fv(tvsub(tve_after,tvb))
tvdependentset :: TVEnv -> TVEnv -> (QTVarId -> Bool)
tvdependentset tve_before tve_after = 
    \tv -> any (\tvb -> occurs tv (tvar tvb) tve_after) tvbs
 where
   tvbs = tvfree tve_before

-- Return the list of type variables in t (possibly with duplicates)
freevars :: K3 QType -> [QTVarId]
freevars t = runIdentity $ foldMapTree extractVars [] t
  where
    extractVars _ (tag -> QTVar v) = return [v]
    extractVars chAcc _ = return $ concat chAcc

-- The unification. If unification failed, return the reason
unify :: K3 QType -> K3 QType -> TVEnv -> Either String TVEnv
unify t1 t2 tve = unify' (tvchase tve t1) (tvchase tve t2) tve

-- If either t1 or t2 are type variables, they are definitely unbound
unify' :: K3 QType -> K3 QType -> TVEnv -> Either String TVEnv
unify' (tag -> QTPrimitive p1) (tag -> QTPrimitive p2) tve
  | p1 == p2 = Right tve
  | ([p1, p2] `intersect` [QTNumber, QTInt, QTReal]) == [p1,p2] = Right tve
  | otherwise = Left $ unwords ["Unification mismatch on primitives: ", show p1, "and", show p2]

unify' t1@(tag -> QTCon d1) t2@(tag -> QTCon d2) tve
  | d1 == d2 && (length $ children t1) == (length $ children t2)
      = foldM (\a (b,c) -> unify b c a) tve $ zip (children t1) $ children t2
  | otherwise = Left $ unwords ["Unification mismatch on datatypes: ", show d1, "and", show d2]

unify' (tag -> QTVar v) t tve = unifyv v t tve
unify' t (tag -> QTVar v) tve = unifyv v t tve

unify' (tag -> QTTop) (tag -> QTTop)       tve = Right tve
unify' (tag -> QTBottom) (tag -> QTBottom) tve = Right tve

unify' t1 t2 _ = Left $ unwords ["Unification mismatch:", show t1, "and", show t2]

-- Unify a free variable v1 with t2
unifyv :: QTVarId -> K3 QType -> TVEnv -> Either String TVEnv
unifyv v1 t@(tag -> QTVar v2) tve =
    if v1 == v2
      then Right tve
      else Right $ tvext tve v1 t

unifyv v t tve =
    if occurs v t tve
      then Left $ unwords ["occurs check:", show v, "in", show $ tvsub tve t]
      else Right $ tvext tve v t

-- The occurs check: if v appears free in t
occurs :: QTVarId -> K3 QType -> TVEnv -> Bool
occurs v t@(tag -> QTCon _) tve = or $ map (flip (occurs v) tve) $ children t
occurs v (tag -> QTVar v2)  tve = maybe (v == v2) (flip (occurs v) tve) $ tvlkup tve v2
occurs _ _ _ = False

-- Monadic version of unify
unifyM :: K3 QType -> K3 QType -> (String -> String) -> TVEnvM ()
unifyM t1 t2 errf = do
  tve <- get
  case unify t1 t2 tve of 
    Right tve2 -> put tve2
    Left  err  -> left $ errf err

-- | Given a polytype, for every polymorphic type var, replace all of
--   its occurrences in t with a fresh type variable. We do this by
--   creating a substitution tve and applying it to t.
--   We also strip any mutability qualifiers here.
instantiate :: QPType -> TVEnvM (K3 QType)
instantiate (QPType tvs t) = do
  tve <- associate_with_freshvars tvs
  (Node (tg :@: anns) ch) <- return $ tvsub tve t
  return $ Node (tg :@: filter (not . isQTQualified) anns) ch
 where
 associate_with_freshvars [] = return tvenv0
 associate_with_freshvars (tv:rtvs) = do
   tve     <- associate_with_freshvars rtvs
   tvfresh <- newtv
   return $ tvext tve tv tvfresh

-- | Generalization for let-polymorphism.
generalize :: TVEnvM (K3 QType) -> TVEnvM QPType
generalize ta = do
 tve_before <- get
 t          <- ta
 tve_after  <- get
 let t'    = tvsub tve_after t
 let tvdep = tvdependentset tve_before tve_after
 let fv    = filter (not . tvdep) $ nub $ freevars t'
 return $ QPType fv t'

monomorphize :: (Monad m) => K3 QType -> m QPType
monomorphize t = return $ QPType [] t

inferProgramTypes :: K3 Declaration -> Either String (K3 Declaration)
inferProgramTypes prog = do
    initEnv    <- initializeTypeEnv
    (_, nProg) <- foldProgram declF annMemF exprF initEnv prog
    return nProg
  where 
    -- TODO: for functions, initialize with declared types
    initializeTypeEnv :: Either String TIEnv
    initializeTypeEnv = return (tenv0, taenv0)

    -- TODO: unify declared K3 type and inferred QType?
    declF :: TIEnv -> K3 Declaration -> Either String (TIEnv, K3 Declaration)
    declF env d@(tag -> DGlobal n t _) = qpType t >>= \qpt -> return (tiexte env n qpt, d)
      -- ^ TODO: for global collections, additionally unify final and self types.
    
    declF env d@(tag -> DTrigger n t _) = trigType t >>= \qpt -> return (tiexte env n qpt, d)
      where trigType x = qType x >>= monomorphize
              -- ^ TODO: the type inferred for the body should be a lambda. Substitute the
              --   trigger argument type, and unify.
    
    -- TODO: builtin types, since annotation members may include K3's content, horizon,
    -- final and self variables.
    declF env d@(tag -> DAnnotation n _ mems) = annType env n mems >>= \at -> return (at, d)
      where annType env_ n_ mems_ = mapM memType mems_ >>= return . tiexta env_ n_ . catMaybes
            memType (Lifted      _ mn mt _ _) = qpType mt >>= return . Just . (mn,)
            memType (Attribute   _ mn mt _ _) = qpType mt >>= return . Just . (mn,)
            memType (MAnnotation _ _ _) = return Nothing

    declF env d = return (env, d)

    annMemF :: TIEnv -> AnnMemDecl -> Either String (TIEnv, AnnMemDecl)
    annMemF env mem = return (env, mem)

    exprF :: TIEnv -> K3 Expression -> Either String (TIEnv, K3 Expression)
    exprF env e = inferExprTypes env e >>= return . (env,)


inferExprTypes :: TIEnv -> K3 Expression -> Either String (K3 Expression)
inferExprTypes tienv expr =
    fst $ flip runState tvenv0 $ runEitherT
        $ mapIn1RebuildTree lambdaBinding sidewaysBinding inferQType tienv expr
  where
    qTypeOf :: K3 Expression -> TVEnvM (K3 QType)
    qTypeOf e = case e @~ isEQType of
                  Just (EQType qt) -> return qt
                  _ -> left $ "Untyped expression: " ++ show e

    exprQtSub :: TVEnv -> K3 Expression -> K3 Expression
    exprQtSub tve e = runIdentity $ mapTree subNode e
      where subNode ch (Node (tg :@: anns) _) = return $ Node (tg :@: map subAnns anns) ch
            subAnns (EQType qt) = (EQType $ tvsub tve qt)
            subAnns x = x

    extMonoQT :: TIEnv -> Identifier -> K3 QType -> TVEnvM TIEnv
    extMonoQT env i t = monomorphize t >>= return . tiexte env i

    lambdaBinding :: TIEnv -> K3 Expression -> K3 Expression -> TVEnvM TIEnv
    lambdaBinding env _ (tag -> ELambda i) = newtv >>= monomorphize >>= return . tiexte env i
    lambdaBinding env _ _ = return env

    sidewaysBinding :: TIEnv -> K3 Expression -> K3 Expression -> TVEnvM (TIEnv, [TIEnv])
    sidewaysBinding env ch1 (tag -> ELetIn i) = do
      ipt <- generalize $ qTypeOf ch1
      let nEnv = tiexte env i ipt
      return (env, [nEnv])

    sidewaysBinding env ch1 (tag -> EBindAs b) = do
        ch1T <- qTypeOf ch1
        nEnv <- case b of
                  BIndirection i -> do
                    itv <- newtv
                    void $ unifyM ch1T (tind itv) $ bindErr "indirection"
                    extMonoQT env i itv
                  
                  BTuple ids -> do
                    idtvs <- mapM (const newtv) ids
                    void  $  unifyM ch1T (ttup idtvs) $ bindErr "tuple"
                    tve   <- get
                    foldM (\x (y,z) -> extMonoQT x y $ tvsub tve z) env $ zip ids idtvs

                  -- TODO: partial bindings?
                  BRecord ijs -> do
                    jtvs <- mapM (const newtv) ijs
                    void $  unifyM ch1T (trec $ flip zip jtvs $ map fst ijs) $ bindErr "record"
                    tve  <- get
                    foldM (\x (y,z) -> extMonoQT x y $ tvsub tve z) env $ flip zip jtvs $ map snd ijs

        return (env, [nEnv])

      where
        bindErr kind reason = unwords ["Invalid", kind, "bind-as:", reason]
    
    sidewaysBinding env ch1 (tag -> ECaseOf i) = do
      ch1T <- qTypeOf ch1
      itv  <- newtv
      void $  unifyM ch1T (topt itv) $ (("Invalid case-of source expression: ")++)
      tve  <- get
      nEnv <- extMonoQT env i $ tvsub tve itv
      return (env, [nEnv, env])
    
    sidewaysBinding env _ (children -> ch) = return (env, replicate (length ch - 1) env)

    inferQType :: TIEnv -> [K3 Expression] -> K3 Expression -> TVEnvM (K3 Expression)
    inferQType _ _ n@(tag -> EConstant (CBool   _)) = return $ n .+ tbool
    inferQType _ _ n@(tag -> EConstant (CByte   _)) = return $ n .+ tbyte
    inferQType _ _ n@(tag -> EConstant (CInt    _)) = return $ n .+ tint
    inferQType _ _ n@(tag -> EConstant (CReal   _)) = return $ n .+ treal
    inferQType _ _ n@(tag -> EConstant (CString _)) = return $ n .+ tstr

    inferQType _ _ n@(tag -> EConstant (CNone nm)) = do
      tv <- newtv
      let ntv = case nm of { NoneMut -> mutQT tv; NoneImmut -> immutQT tv }
      return $ n .+ (topt ntv)
    
    -- TODO: final and self types.
    inferQType _ _ n@(tag -> EConstant (CEmpty  t)) = do
        cqt <- either (const contentErr) return $ qType t 
        return $ n .+ (tcol cqt $ namedEAnnotations $ annotations n)
      where contentErr = left "No type inferred for empty collection content type"

    -- | Variable specialization. Note that instantiate strips qualifiers.
    inferQType env _ n@(tag -> EVariable i) =
        either (lookupError i) instantiate (tilkupe env i) >>= return . (n .+)
      where lookupError j reason = left $ unwords ["No type environment binding for ", j, ":", reason]

    -- | Data structures. Qualifiers are taken from child expressions by rebuildE.
    inferQType _ ch n@(tag -> ESome)       = qTypeOf (head ch) >>= return . ((rebuildE n ch) .+) . topt
    inferQType _ ch n@(tag -> EIndirect)   = qTypeOf (head ch) >>= return . ((rebuildE n ch) .+) . topt
    inferQType _ ch n@(tag -> ETuple)      = mapM qTypeOf ch   >>= return . ((rebuildE n ch) .+) . ttup 
    inferQType _ ch n@(tag -> ERecord ids) = mapM qTypeOf ch   >>= return . ((rebuildE n ch) .+) . trec . zip ids

    -- | Lambda expressions are passed the post-processed environment, 
    --   so the type variable for the identifier is bound in the type environment.
    inferQType env ch n@(tag -> ELambda i) = do
        ipt  <- either lambdaBindingErr return $ tilkupe env i
        chqt <- qTypeOf $ head ch
        case ipt of 
          QPType [] iqt -> return $ rebuildE n ch .+ tfun iqt chqt
          _             -> polyBindingErr
      where lambdaBindingErr reason = left $ unwords ["Could not find typevar for lambda binding: ", i, reason]
            polyBindingErr = left "Invalid forall type in lambda binding"

    -- | Assignment expressions unify their source and target types, as well as 
    --   ensuring that the source is mutable.
    inferQType env ch n@(tag -> EAssign i) = do
      ipt <- either assignBindingErr return $ tilkupe env i
      eqt <- qTypeOf $ head ch
      case ipt of 
        QPType [] iqt
          | (iqt @~ isQTQualified) == Just QTMutable ->
              do { void $ unifyM iqt eqt (("Invalid assignment to " ++ i ++ ": ") ++);
                   return $ rebuildE n ch .+ tunit }
          | otherwise -> mutabilityErr i
        
        _ -> polyBindingErr
      where assignBindingErr reason = left $ unwords ["Could not find binding type for assignment: ", i, reason]
            polyBindingErr          = left "Invalid forall type in assignment"
            mutabilityErr i         = left $ "Invalid assigment to non-mutable binding: " ++ i

    -- TODO: any kind of unification here if the source is a type variable?
    inferQType env ch n@(tag -> EProject i) = do
      chqt <- qTypeOf $ head ch
      case tag chqt of
        QTCon (QTRecord ids)        -> undefined
        QTCon (QTCollection annIds) -> undefined
        _ -> left $ "Invalid projection argument: " ++ show chqt

    inferQType _ ch n@(tag -> EOperate OApp) = do
      fnqt  <- qTypeOf $ head ch
      argqt <- qTypeOf $ last ch
      retqt <- newtv
      void $ unifyM fnqt (tfun argqt retqt) (("Invalid function application: ") ++)
      tve   <- get
      return $ rebuildE n (map (exprQtSub tve) ch) .+ (tvsub tve retqt)

    inferQType _ ch n@(tag -> EOperate OSeq) = do
        lqt <- qTypeOf $ head ch
        rqt <- qTypeOf $ last ch
        void $ unifyM tunit lqt (("Invalid left sequence operand: ") ++)
        return $ rebuildE n ch .+ rqt

    -- | Check trigger-address pair and unify trigger type and message argument.
    inferQType _ ch n@(tag -> EOperate OSnd) = do
        trgtv <- newtv
        void $ unifyBinaryM (ttup [ttrg trgtv, taddr]) trgtv ch sndError
        return $ rebuildE n ch .+ tunit
      where sndError side reason = "Invalid " ++ side ++ " send operand: " ++ reason

    -- | Unify operand types based on the kind of operator.
    -- TODO: better numeric promotion?
    inferQType _ ch n@(tag -> EOperate op) 
      | numeric op = do
            (lqt, rqt) <- unifyBinaryM tnum tnum ch numericError
            resultqt   <- case (tag lqt, tag rqt) of
                            (QTPrimitive QTInt, QTPrimitive QTInt) -> return $ tint
                            (_, _) -> return $ treal
            return $ rebuildE n ch .+ resultqt

      | comparison op || logic op = do
            void $ unifyBinaryM tbool tbool ch boolError
            return $ rebuildE n ch .+ tbool

      | textual op = do
            void $ unifyBinaryM tstr tstr ch stringError
            return $ rebuildE n ch .+ tstr

      | op == ONeg = do
            chqt <- unifyUnaryM tnum ch (("Invalid unary minus operand: ") ++)
            resultqt <- case tag chqt of
                          QTPrimitive QTInt -> return tint
                          _ -> return treal
            return $ rebuildE n ch .+ resultqt

      | op == ONot = do
            void $ unifyUnaryM tbool ch (("Invalid negation operand: ") ++)
            return $ rebuildE n ch .+ tbool

      | otherwise = left $ "Invalid operation: " ++ show op

      where numericError side reason = "Invalid " ++ side ++ " numeric operand: " ++ reason
            stringError side reason = "Invalid " ++ side ++ " string operand: " ++ reason
            boolError side reason =
              let kind = if comparison op then "comparsion" else "logic" 
              in unwords ["Invalid", side, kind, "operand:", reason]

    -- First child generation has already been performed in sidewaysBinding
    inferQType _ ch n@(tag -> ELetIn _) = do
      bqt <- qTypeOf $ last ch
      return $ rebuildE n ch .+ bqt
    
    -- First child unification has already been performed in sidewaysBinding
    inferQType _ ch n@(tag -> EBindAs _) = do
      bqt <- qTypeOf $ last ch
      return $ rebuildE n ch .+ bqt
    
    -- First child unification has already been performed in sidewaysBinding
    inferQType _ ch n@(tag -> ECaseOf _) = do
      sqt   <- qTypeOf $ ch !! 1
      nqt   <- qTypeOf $ last ch
      void  $  unifyM sqt nqt (("Mismatched case-of branch types: ") ++)
      return $ rebuildE n ch .+ sqt

    inferQType _ ch n@(tag -> EIfThenElse) = do
      pqt <- qTypeOf $ head ch
      tqt <- qTypeOf $ ch !! 1
      eqt <- qTypeOf $ last ch
      void $ unifyM pqt tbool $ (("Invalid if-then-else predicate: ") ++)
      void $ unifyM tqt eqt   $ (("Mismatched condition branches: ") ++)
      return $ rebuildE n ch .+ tqt

    inferQType _ _ n@(tag -> EAddress) = return $ n .+ taddr

    inferQType _ ch n  = return $ rebuildE n ch
      -- ^ TODO unhandled: ESelf, EImperative

    rebuildE (Node t _) ch = Node t ch

    unifyBinaryM lexpected rexpected ch errf = do
      lqt <- qTypeOf $ head ch
      rqt <- qTypeOf $ last ch
      void $ unifyM lexpected lqt (errf "left")
      void $ unifyM rexpected rqt (errf "right")
      return (lqt, rqt)

    unifyUnaryM expected ch errf = do
        chqt <- qTypeOf $ head ch
        void $ unifyM chqt expected errf
        return chqt

    numeric    op = op `elem` [OAdd, OSub, OMul, ODiv, OMod]
    comparison op = op `elem` [OEqu, ONeq, OLth, OLeq, OGth, OGeq]
    logic      op = op `elem` [OAnd, OOr]
    textual    op = op `elem` [OConcat]


{- Type conversion -}

-- TODO: better handling of top-level forall types.
qpType :: K3 Type -> Either String QPType
qpType t = qType t >>= monomorphize -- TODO: generalize?

-- TODO: better handling of foralls and declaredvars.
-- We can return a mapping of new type vars to declared var identifiers, and then
-- substitute this in foralls.
qType :: K3 Type -> Either String (K3 QType)
qType t = foldMapTree mkQType (ttup []) t
  where 
    mkQType _ (tag -> TTop)    = return ttop
    mkQType _ (tag -> TBottom) = return tbot

    mkQType _ (tag -> TBool)    = return tbool
    mkQType _ (tag -> TByte)    = return tbyte
    mkQType _ (tag -> TInt)     = return tint
    mkQType _ (tag -> TReal)    = return treal
    mkQType _ (tag -> TString)  = return tstr
    mkQType _ (tag -> TNumber)  = return tnum
    mkQType _ (tag -> TAddress) = return taddr

    mkQType ch (tag -> TOption)       = return $ topt $ head ch
    mkQType ch (tag -> TIndirection)  = return $ tind $ head ch
    mkQType ch (tag -> TTuple)        = return $ ttup ch
    mkQType ch (tag -> TRecord ids)   = return $ trec $ zip ids ch
    
    mkQType ch n@(tag -> TCollection) = return $ tcol (head ch) $ namedTAnnotations $ annotations n
      -- ^ TODO: construct final and self types as 2nd and 3rd children.

    mkQType ch (tag -> TFunction) = return $ tfun (head ch) $ last ch
    mkQType ch (tag -> TTrigger)  = return $ ttrg $ head ch
    mkQType ch (tag -> TSource)   = return $ tsrc $ head ch
    mkQType ch (tag -> TSink)     = return $ tsnk $ head ch
    
    mkQType _ (tag -> TDeclaredVar x) = Left $ "Invalid type variable for QType: " ++ x
    mkQType _ (tag -> TForall _)      = Left $ "Invalid forall type for QType"
    mkQType _ t_ = Left $ "No QType construction for " ++ show t_
      -- ^ TODO: builtin types for annotation member signatures: content, horizon, final, self

