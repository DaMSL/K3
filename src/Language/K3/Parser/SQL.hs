{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

module Language.K3.Parser.SQL where

import Control.Arrow ( (***), (&&&), first )
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Except

import Data.Functor.Identity
import Data.Monoid
import Data.Either ( isLeft, partitionEithers )
import Data.List ( find, isInfixOf, nub, reverse )
import Data.Tuple ( swap )

import Data.Map ( Map )
import Data.Set ( Set )
import qualified Data.Map as Map
import qualified Data.Set as Set

import Debug.Trace

import Database.HsSqlPpp.Ast
import Database.HsSqlPpp.Annotation hiding ( Annotation )
import qualified Database.HsSqlPpp.Annotation as HA ( Annotation )
import Database.HsSqlPpp.Parser
import Database.HsSqlPpp.Pretty

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Declaration as DC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Literal     as LC
import qualified Language.K3.Core.Constructor.Type        as TC

import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

data OperatorFn = UnaryOp Operator ScalarExpr
                | BinaryOp Operator ScalarExpr ScalarExpr
                deriving (Eq, Show)

-- | Relation names and types.
type RTypeEnv = Map Identifier (K3 Type)

-- | Attribute dependency graph.
type ADGPtr = Int

data ADGNode = ADGNode { adnn  :: Identifier
                       , adnr  :: Identifier
                       , adne  :: Maybe ScalarExpr
                       , adnch :: [ADGPtr] }
              deriving (Eq, Show)

type ADGraph = Map ADGPtr ADGNode

-- | Parsing state environment.
data SQLEnv = SQLEnv { relations :: RTypeEnv
                     , adgraph   :: ADGraph
                     , adpsym    :: ParGenSymS }
            deriving (Eq, Show)

-- | A stateful SQL parsing monad.
type SQLParseM = ExceptT String (State SQLEnv)

-- | Relational-K3 type mapping, indicating
--   i. no type prefix
--   ii. common type path prefix for all fields
--   iii. per-field full paths
type TypePath      = [Identifier]
type TypePathMap   = Map Identifier TypePath
type TLabelMapping = Maybe (Either Identifier TypePathMap)

-- | Relational value-type mappings, indicating a relational type as a
--   i. single variable-and-type mapping
--   ii. per-field variable-and-type mapping
type VTMapping = (Identifier, TLabelMapping)
type FieldVTMap = Map Identifier VTMapping
data TValueMapping = TVMType      TLabelMapping
                   | TVMNamed     VTMapping
                   | TVMComposite FieldVTMap
                   deriving (Eq, Show)

-- | Mapped relational types.
type MRType  = (K3 Type, TLabelMapping, [ADGPtr])
type MERType = (K3 Type, TValueMapping)

-- | Qualified attribute env
type AQEnv = Map Identifier MRType

-- | Attribute pointer env
type ADGPtrEnv = ([ADGPtr], [(Identifier, [ADGPtr])])

-- | Attribute binding env
data AEnv = AEnv { aeuq :: MERType, aeq :: AQEnv }
            deriving (Eq, Show)

data ParseResult = ParseResult { pexpr  :: K3 Expression
                               , prt    :: K3 Type
                               , pkt    :: K3 Type
                               , ptmap  :: TLabelMapping
                               , palias :: Maybe Identifier
                               , pptrs  :: [ADGPtr] }
                  deriving (Eq, Show)


{- Data.Text helpers -}

sqlenv0 :: SQLEnv
sqlenv0 = SQLEnv Map.empty Map.empty contigsymS

-- | Relation type accessors
stlkup :: RTypeEnv -> Identifier -> Except String (K3 Type)
stlkup env x = maybe err return $ Map.lookup x env
  where err = throwE $ "Unknown relation in sql parser environment: " ++ show x

stext :: RTypeEnv -> Identifier -> K3 Type -> RTypeEnv
stext env x t = Map.insert x t env

stdel :: RTypeEnv -> Identifier -> RTypeEnv
stdel env x = Map.delete x env

-- | Dependency graph accessors.
sglkup :: ADGraph -> ADGPtr -> Except String ADGNode
sglkup g p = maybe err return $ Map.lookup p g
  where err = throwE $ "Unknown attribute node in sql parser environment: " ++ show p

sgext :: ADGraph -> ADGNode -> ParGenSymS -> (ADGPtr, ParGenSymS, ADGraph)
sgext g n sym = (ptr, nsym, Map.insert ptr n g)
  where (nsym, ptr) = gensym sym


-- | State accessors.
sqelkup :: SQLEnv -> Identifier -> Except String (K3 Type)
sqelkup senv n = stlkup (relations senv) n

sqeext :: SQLEnv -> Identifier -> K3 Type -> SQLEnv
sqeext senv n t = senv { relations = stext (relations senv) n t }

sqedel :: SQLEnv -> Identifier -> SQLEnv
sqedel senv n = senv { relations = stdel (relations senv) n }

sqglkup :: SQLEnv -> ADGPtr -> Except String ADGNode
sqglkup senv p = sglkup (adgraph senv) p

sqgext :: SQLEnv -> ADGNode -> (ADGPtr, SQLEnv)
sqgext senv n = (ptr, senv {adgraph = ng, adpsym = nsym})
  where (ptr, nsym, ng) = sgext (adgraph senv) n (adpsym senv)

-- | Monadic accessors.
sqelkupM :: Identifier -> SQLParseM (K3 Type)
sqelkupM n = get >>= liftExceptM . (\env -> sqelkup env n)

sqeextM :: Identifier -> K3 Type -> SQLParseM ()
sqeextM n t = get >>= \env -> return (sqeext env n t) >>= put

sqglkupM :: ADGPtr -> SQLParseM ADGNode
sqglkupM p = get >>= liftExceptM . (\env -> sqglkup env p)

sqgextM :: ADGNode -> SQLParseM ADGPtr
sqgextM n = get >>= \env -> return (sqgext env n) >>= \(r, nenv) -> put nenv >> return r

sqgidxM :: [ADGPtr] -> SQLParseM (Map Identifier ADGPtr)
sqgidxM ptrs = do
  l <- mapM (\p -> sqglkupM p >>= \node -> return (adnn node, p)) ptrs
  return $ Map.fromList l


-- | Adds base relation attributes to the ADG.
sqgextSchemaM :: Maybe Identifier -> K3 Type -> SQLParseM [ADGPtr]
sqgextSchemaM (Just n) t = do
    rt <- telemM t
    case tag rt of
      TRecord ids -> mapM sqgextM $ map (\i -> ADGNode i n Nothing []) ids
      _ -> throwE $ boxToString $ ["Invalid relational element type"] %$ prettyLines rt

sqgextSchemaM _ _ = throwE "No relation name specified when extending attribute graph"

-- | Adds unchanged derived schema attributes to the ADG
sqgextIdentityM :: Maybe Identifier -> K3 Type -> [ADGPtr] -> SQLParseM [ADGPtr]
sqgextIdentityM (Just n) t ptrs = do
  rt <- telemM t
  case tag rt of
    TRecord ids -> mapM sqgextM $ map (\(i, p) -> ADGNode i n (Just $ Identifier emptyAnnotation $ Nmc i) [p])
                                $ zip ids ptrs
    _ -> throwE $ boxToString $ ["Invalid relational element type"] %$ prettyLines rt

sqgextIdentityM _ _ _ = throwE "Invalid identity arguments when extending attribute graph"

-- | Extend the attribute graph for a relation type derived directly from another relation.
sqgextAliasM :: Maybe Identifier -> K3 Type -> K3 Type -> [ADGPtr] -> SQLParseM [ADGPtr]
sqgextAliasM (Just n) (tag -> TRecord dstids) (tag -> TRecord srcids) inputptrs
  | length dstids == length srcids
  = do { idx <- sqgidxM inputptrs;
         nodes <- mapM (mknode idx) $ zip dstids srcids;
         mapM sqgextM nodes }

  where mknode ptridx (d, s) = do
          p <- maybe (lkuperr s) return $ Map.lookup s ptridx
          return $ ADGNode d n (Just $ Identifier emptyAnnotation $ Nmc s) [p]

        lkuperr v = throwE $ "No attribute graph node found for " ++ v

sqgextAliasM _ _ _ _ = throwE "Invalid alias arguments when extending attribute graph"

-- | Extend the attribute graph for a relation type computed from the given expressions.
sqgextExprM :: Maybe Identifier -> K3 Type -> [ScalarExpr] -> ADGPtrEnv -> SQLParseM [ADGPtr]
sqgextExprM (Just n) t exprs penv = do
  rt <- telemM t
  case tag rt of
    TRecord ids | length ids == length exprs -> do
      uidx <- sqgidxM $ fst penv
      qidx <- Map.fromList <$> mapM (\(i,ps) -> sqgidxM ps >>= return . (i,)) (snd penv)
      nodes <- mapM (mknode uidx qidx) (zip ids exprs)
      mapM sqgextM nodes

    _ -> throwE $ boxToString $ ["Invalid relational element type"] %$ prettyLines rt

  where mknode uidx qidx (i, e) = do
          let (uv, qv) = extractvars e
          ps <- mapM (lkupu uidx) $ uv
          qps <- mapM (lkupq qidx) qv
          return $ ADGNode i n (Just e) $ ps ++ qps

        lkupu uidx v = maybe (lkuperr v) return $ Map.lookup v uidx
        lkupq qidx [x,y] = maybe (lkuperr x) (\idx -> lkupu idx y) $ Map.lookup x qidx
        lkupq _ _ = throwE $ "Invalid pair qualified identifer"

        lkuperr v = throwE $ "No attribute graph node found for " ++ v

sqgextExprM _ _ _ _ = throwE "Invalid expr arguments when extending attribute graph"

{- SQLParseM helpers. -}

runSQLParseM :: SQLEnv -> SQLParseM a -> (Either String a, SQLEnv)
runSQLParseM env m = flip runState env $ runExceptT m

evalSQLParseM :: SQLEnv -> SQLParseM a -> Either String a
evalSQLParseM env m = fst $ runSQLParseM env m

reasonM :: (String -> String) -> SQLParseM a -> SQLParseM a
reasonM errf = mapExceptT $ \m -> m >>= \case
  Left  err -> return $ Left $ errf err
  Right r   -> return $ Right r

errorM :: String -> SQLParseM a
errorM msg = reasonM id $ throwE msg

liftExceptM :: Except String a -> SQLParseM a
liftExceptM = mapExceptT (return . runIdentity)

liftEitherM :: Either String a -> SQLParseM a
liftEitherM = either throwE return


{- Attribute environment helpers. -}
mrtype :: ParseResult -> SQLParseM MRType
mrtype r = telemM (prt r) >>= \ret -> return (ret, ptmap r, pptrs r)

mertypeR :: K3 Type -> TLabelMapping -> Maybe Identifier -> SQLParseM MERType
mertypeR t tmap alias = do
  ret <- telemM t
  return (ret, maybe (TVMType tmap) (\i -> TVMNamed (i, tmap)) alias)

mertype :: ParseResult -> SQLParseM MERType
mertype r = mertypeR (prt r) (ptmap r) $ palias r

aqenv0 :: [(Identifier, MRType)] -> AQEnv
aqenv0 l = Map.fromList l

aqenv1 :: ParseResult -> SQLParseM AQEnv
aqenv1 r = do
  mrt <- mrtype r
  return $ maybe Map.empty (\i -> aqenv0 [(i, mrt)]) $ palias r

aqenvl :: [ParseResult] -> SQLParseM AQEnv
aqenvl l = mapM aqenv1 l >>= return . mconcat

adgenv1R :: [ADGPtr] -> Maybe Identifier -> SQLParseM ADGPtrEnv
adgenv1R ptrs al = return (ptrs, maybe [] (\i -> [(i, ptrs)]) al)

adgenv1 :: ParseResult -> SQLParseM ADGPtrEnv
adgenv1 r = return (pptrs r, maybe [] (\i -> [(i, pptrs r)]) $ palias r)

adgenvql :: [ParseResult] -> SQLParseM [(Identifier, [ADGPtr])]
adgenvql rl = return $ concatMap (\r -> maybe [] (\i -> [(i, pptrs r)]) $ palias r) rl

aenv0 :: MERType -> AQEnv -> AEnv
aenv0 u q = AEnv u q

aenv1 :: ParseResult -> SQLParseM (AEnv, ADGPtrEnv)
aenv1 r = (\mert q p -> (aenv0 mert q, p)) <$> mertype r <*> aqenv1 r <*> adgenv1 r

aenvl :: [ParseResult] -> SQLParseM (AEnv, ADGPtrEnv)
aenvl l = (\(mert, p) q pq -> (aenv0 mert q, (p, pq))) <$> mr0 <*> aqenvl l <*> adgenvql l
  where mr0 = do
          merl <- mapM mertype l
          let (ids, t, tvm) = concatmer merl
          p <- pp0 ids
          return ((t,tvm),p)

        pp0 uids = do
          idx <- sqgidxM $ concatMap pptrs l
          mapM (\i -> maybe (lkuperr i) return $ Map.lookup i idx) $ Set.toList uids

        concatmer merl =
          let (uniqids, idt, idvt) = foldl accmer (Set.empty, [], []) merl
              uidt  = filter ((`Set.member` uniqids) . fst) idt
              uidvt = concatMap (fieldvt uniqids) idvt
          in (uniqids, recT uidt, TVMComposite $ Map.fromList uidvt)

        accmer (accS, accL, tvmaccL) (tnc -> (TRecord ids, ch), tvm) =
          let new     = Set.fromList ids
              common  = Set.intersection accS new
              newaccS = Set.difference (Set.union accS new) common
          in (newaccS, accL ++ zip ids ch, tvmaccL ++ map (, extractvt tvm) ids)

        accmer acc _ = acc

        fieldvt uniqids (i,vt) = if i `Set.member` uniqids then maybe [] (\x -> [(i,x)]) vt else []

        extractvt (TVMNamed vt) = Just vt
        extractvt _ = Nothing

        lkuperr v = throwE $ "No attribute graph node found for " ++ v


{- SQL AST helpers. -}

projectionexprs :: SelectItem -> ScalarExpr
projectionexprs (SelExp _ e) = e
projectionexprs (SelectItem _ e _) = e

-- TODO: correlated variables in subqueries.
extractvars :: ScalarExpr -> ([Identifier], [[Identifier]])
extractvars e = case e of
  (Identifier _ (sqlnmcomponent -> i)) -> ([i], [])

  (QIdentifier _ nmcl) -> case nmcl of
                            [x,y] -> ([], [["__" ++ sqlnmcomponent x, sqlnmcomponent y]])
                            _ -> zero

  (FunCall _ _ args) -> maprcr args
  (Case _ whense elsee) ->
    many (\(whenel, thene) -> maprcr whenel `rconcat` extractvars thene) whense
      `rconcat` maybe zero extractvars elsee

  (CaseSimple _ ce whense elsee) ->
    extractvars ce
      `rconcat` many (\(whenel, thene) -> maprcr whenel `rconcat` extractvars thene) whense
      `rconcat` maybe zero extractvars elsee

  _ -> ([], [])

  where maprcr l = (concat *** concat) $ unzip $ map extractvars l
        many f l = (concat *** concat) $ unzip $ map f l
        rconcat (a,c) (b,d) = (a++b, c++d)
        zero = ([], [])


{- Type and alias helpers. -}

tblaliasM :: ParseResult -> SQLParseM Identifier
tblaliasM (palias -> (Just a)) = return a
tblaliasM _ = throwE $ "Invalid table alias"

expraliasesGenM :: Int -> [ParseResult] -> SQLParseM (Int, [Identifier])
expraliasesGenM start l = foldM ensure (start,[]) l
  where ensure (i, acc) r = return $ maybe (i+1, acc ++ ["f" ++ show i]) (\a -> (i,acc++[a])) $ palias r

telemM :: K3 Type -> SQLParseM (K3 Type)
telemM (tnc -> (TCollection, [t])) = return t
telemM t = throwE $ boxToString $ ["Invalid relation type"] %$ prettyLines t

tcolM :: K3 Type -> SQLParseM (K3 Type)
tcolM t = return $ (TC.collection t) @<- [TAnnotation "Collection", TImmutable]

taliaselemM :: TableAlias -> K3 Type -> SQLParseM (K3 Type)
taliaselemM alias t@(tnc -> (TRecord ids, ch)) =
  case alias of
    NoAlias _      -> return t
    TableAlias _ _ -> return t
    FullAlias _ _ fnc | length fnc == length ids -> return $ recT $ zip (map sqlnmcomponent fnc) ch
    FullAlias _ _ _ -> throwE $ "Mismatched alias fields length"

taliaselemM _ t = throwE $ boxToString $ ["Invalid relation element type"] %$ prettyLines t

taliascolM :: TableAlias -> K3 Type -> SQLParseM (K3 Type)
taliascolM alias t@(tnc -> (TCollection, [et@(tag -> TRecord _)])) =
  taliaselemM alias et >>= \net -> return $ (TC.collection net) @<- (annotations t)

taliascolM _ t = throwE $ boxToString $ ["Invalid relation type"] %$ prettyLines t

-- | Wraps a K3 record type with a 'elem' label
twrapelemM :: K3 Type -> SQLParseM (K3 Type)
twrapelemM t@(tag -> TRecord _) = return $ recT [("elem", t)]
twrapelemM t = throwE $ boxToString $ ["Invalid element type for wrapping:"] %$ prettyLines t

-- | Wraps a collection's element type with an 'elem' label
twrapcolelemM :: K3 Type -> SQLParseM (K3 Type)
twrapcolelemM t@(tnc -> (TCollection, [et])) = twrapelemM et >>= \net -> return $ (TC.collection net) @<- annotations t
twrapcolelemM t = throwE $ boxToString $ ["Invalid collection type for wrapping:"] %$ prettyLines t

-- | Creates a type map for an 'elem' wrapped record.
telemtmapM :: SQLParseM TLabelMapping
telemtmapM = return $ Just $ Left "elem"

tresolvetmapM :: Identifier -> TLabelMapping -> SQLParseM [Identifier]
tresolvetmapM i tlm = maybe (return []) resolve tlm
  where resolve (Left j) = return [j, i]
        resolve (Right tpm) = maybe resolveerr return $ Map.lookup i tpm
        resolveerr = throwE $ "Unknown type mapping field " ++ i


{- Expression construction helpers -}

-- | Property construction helper
sqlAttrProp :: ADGPtr -> Annotation Expression
sqlAttrProp p = EProperty $ Left ("SQLAttr", Just $ LC.int p)

immutE :: K3 Expression -> K3 Expression
immutE e = e @<- ((filter (not . isEQualified) $ annotations e) ++ [EImmutable])

mutE :: K3 Expression -> K3 Expression
mutE e = e @<- ((filter (not . isEQualified) $ annotations e) ++ [EMutable])

recE :: [(Identifier, K3 Expression)] -> K3 Expression
recE ide = EC.record $ map (\(i,e) -> (i, e @<- ((filter (not . isEQualified) $ annotations e) ++ [EImmutable]))) ide

recT :: [(Identifier, K3 Type)] -> K3 Type
recT idt = TC.record $ map (\(i,t) -> (i, t @<- ((filter (not . isTQualified) $ annotations t) ++ [TImmutable]))) idt

projectE :: Identifier -> Identifier -> (Identifier, K3 Expression)
projectE i f = (f, EC.project f $ EC.variable i)

projectPathE :: K3 Expression -> [Identifier] -> K3 Expression
projectPathE e p = foldl (\accE i -> EC.project i accE) e p

fieldE :: Identifier -> TLabelMapping -> Identifier -> SQLParseM (K3 Expression)
fieldE v tmap i = do
  path <- tresolvetmapM i tmap
  return $ if null path then EC.project i $ EC.variable v
                        else projectPathE (EC.variable v) path

namedRecordE :: Identifier -> TLabelMapping -> [Identifier] -> SQLParseM (K3 Expression)
namedRecordE v (Just (Left prefix)) _ = return $ EC.project prefix $ EC.variable v
namedRecordE v tmap ids = foldM field [] ids >>= return . recE
  where field acc i = fieldE v tmap i >>= \f -> return $ acc ++ [(i,f)]


compositeRecordE :: FieldVTMap -> [Identifier] -> SQLParseM (K3 Expression)
compositeRecordE fvm ids = foldM fieldOpt [] ids >>= return . recE
  where fieldOpt acc i = maybe (compositeErr i) (field acc i) $ Map.lookup i fvm
        field acc i (v, tmap) = fieldE v tmap i >>= \f -> return $ acc ++ [(i,f)]

        compositeErr i = throwE $ "Cannot find field-specific mapping for " ++ i


bindE :: AEnv -> Identifier -> K3 Expression -> SQLParseM (Maybe Identifier, K3 Expression)
bindE (AEnv (tag -> TRecord ids, tvm) _) backupvar e = do
  case tvm of
    TVMType tmap -> do
      initE <- namedRecordE backupvar tmap ids
      return (Just backupvar, EC.bindAs initE (BRecord $ zip ids ids) e)

    TVMNamed (i, tmap) -> do
      initE <- namedRecordE i tmap ids
      return (Just i, EC.bindAs initE (BRecord $ zip ids ids) e)

    TVMComposite fvm -> do
      initE <- compositeRecordE fvm ids
      return (Nothing, EC.bindAs initE (BRecord $ zip ids ids) e)

bindE (AEnv (t, _) _) _ _ = throwE $ boxToString $ ["Unable to bind type"] %$ prettyLines t


concatE :: AEnv -> SQLParseM (K3 Expression)
concatE (AEnv (tag -> TRecord ids, tvm) _) = do
  case tvm of
    TVMType _ -> throwE $ "Cannot concat type-only attribute env"
    TVMNamed (i, tmap) -> namedRecordE i tmap ids
    TVMComposite fvm -> compositeRecordE fvm ids

concatE (AEnv (t, _) _) = throwE $ boxToString $ ["Unable to concat type"] %$ prettyLines t


zeroE :: [(Identifier, K3 Type)] -> SQLParseM (K3 Expression)
zeroE [] = return EC.unit
zeroE [(_,t)] = either throwE return $ defaultExpression t
zeroE l = either throwE return $ defaultExpression $ recT l


aggE :: Identifier -> (Identifier, K3 Expression) -> K3 Expression
aggE i (f, e) = EC.applyMany e [EC.project f $ EC.variable i]

-- TODO: gensym
memoE :: K3 Expression -> (K3 Expression -> K3 Expression) -> K3 Expression
memoE srcE bodyF = case tag srcE of
  EConstant _ -> bodyF srcE
  EVariable _ -> bodyF srcE
  _ -> EC.letIn "__memo" (immutE srcE) $ bodyF $ EC.variable "__memo"


matchE :: K3 Expression -> K3 Expression -> K3 Expression
matchE elemexpr colexpr =
  memoE (immutE elemexpr) $ \e -> EC.applyMany (EC.project "filter" $ colexpr) [matchF e]
  where matchF e = EC.lambda "__x" $ EC.binop OEqu (EC.variable "__x") e

memberE :: Bool -> K3 Expression -> K3 Expression -> K3 Expression
memberE asMem elemexpr colexpr  = emptyE (not asMem) $ matchE elemexpr colexpr

emptyE :: Bool -> K3 Expression -> K3 Expression
emptyE asEmpty colexpr = EC.binop (if asEmpty then OEqu else ONeq)
  (EC.applyMany (EC.project "size" colexpr) [EC.unit]) $ EC.constant $ CInt 0


{- Parsing toplevel. -}
k3ofsql :: Bool -> Bool -> FilePath -> IO ()
k3ofsql asSyntax printParse path = do
  stmtE <- parseStatementsFromFile path
  either (putStrLn . show) k3program stmtE

  where
    k3program stmts = do
      void $ if printParse then printStmts stmts else return ()
      let declsM = mapM sqlstmt stmts
      let (progE, finalSt) = first (either Left (Right . DC.role "__global" . concat)) $ runSQLParseM sqlenv0 declsM
      if asSyntax
        then either putStrLn (either putStrLn putStrLn . programS) progE
        else either putStrLn (putStrLn . pretty) progE
      printState finalSt

    printStmts stmts = do
      putStrLn $ replicate 40 '='
      void $ forM stmts $ \s -> putStrLn $ show s
      putStrLn $ replicate 40 '='

    printState st = do
      putStrLn $ replicate 40 '=' ++ " Dependency Graph"
      forM_ (Map.toList $ adgraph st) $ \(p, node) ->
        putStrLn $ unwords [show p, show $ adnn node, show $ adnr node, show $ adnch node ]


sqlstmt :: Statement -> SQLParseM [K3 Declaration]
sqlstmt (CreateTable _ nm attrs _) = do
  t <- sqltabletype attrs
  wt <- twrapcolelemM t
  sqeextM (sqlnm nm) t
  return [DC.global (sqlnm nm) wt Nothing]

sqlstmt (QueryStatement _ query) = sqlquerystmt query
sqlstmt s = throwE $ "Unimplemented SQL stmt: " ++ show s


sqlquerystmt :: QueryExpr -> SQLParseM [K3 Declaration]
sqlquerystmt q = do
  qpr <- sqlquery q
  return [ DC.global "result" (pkt qpr) Nothing
         , DC.trigger "query" TC.unit $ EC.lambda "_" $ EC.assign "result" $ pexpr qpr ]


-- | Expression construction, and inlined type inference.
sqlquery :: QueryExpr -> SQLParseM ParseResult
sqlquery (Select _ distinct selectL tableL whereE gbL havingE orderL limitE offsetE) = do
  tables  <- sqltablelist tableL
  selects <- sqlwhere tables whereE
  (groupby, nselectL) <- sqlgroupby selects selectL havingE gbL
  sorted  <- sqlsort groupby orderL
  limited <- sqltopk sorted limitE offsetE
  sqlproject limited nselectL

sqlquery q = throwE $ "Unhandled query " ++ show q


sqltablelist :: TableRefList -> SQLParseM ParseResult
sqltablelist [] = throwE $ "Empty from clause"    -- TODO: empty collection?
sqltablelist [x] = sqltableexpr x
sqltablelist (h:t) = do
  hpr <- sqltableexpr h
  foldM cartesian_product hpr t

  where cartesian_product lpr r = do
          rpr <- sqltableexpr r
          (aenv@(AEnv (ct, _) _), penv) <- aenvl [lpr, rpr]
          (al, ar) <- (,) <$> tblaliasM lpr <*> tblaliasM rpr
          ce   <- concatE aenv
          rt   <- tcolM ct
          kt   <- twrapcolelemM rt
          tmap <- telemtmapM
          let matchF   = EC.lambda "_" $ EC.lambda "_" $ EC.constant $ CBool True
          let combineF = EC.lambda al $ EC.lambda ar ce
          let rexpr    = EC.applyMany (EC.project "join" $ pexpr lpr) [pexpr rpr, matchF, combineF]
          let tid = Just "__CP"
          aptrs <- sqgextAliasM tid ct ct $ fst penv
          return $ ParseResult rexpr rt kt tmap tid aptrs


sqltableexpr :: TableRef -> SQLParseM ParseResult
sqltableexpr (Tref _ nm al) = do
    let tid = sqltablealias ("__" ++ sqlnm nm) al
    t  <- sqelkupM tnm
    rt <- taliascolM al t
    kt <- twrapcolelemM rt
    tmap <- telemtmapM
    aptrs <- sqgextSchemaM tid rt
    return $ ParseResult (EC.variable tnm) rt kt tmap tid aptrs
  where tnm = sqlnm nm

-- TODO: nat, jointy
sqltableexpr (JoinTref _ jlt nat jointy jrt onE jal) = do
  lpr <- sqltableexpr jlt
  rpr <- sqltableexpr jrt
  (aenv@(AEnv (ct, _) _), penv) <- aenvl [lpr, rpr]
  (al, ar) <- (,) <$> tblaliasM lpr <*> tblaliasM rpr
  joinpr   <- maybe (return joinpr0) (sqljoinexpr aenv penv) onE
  ce   <- concatE aenv
  rt   <- tcolM ct >>= taliascolM jal
  kt   <- twrapcolelemM rt
  tmap <- telemtmapM
  (Nothing, be) <- bindE aenv "x" (pexpr joinpr)
  let matchF   = EC.lambda al $ EC.lambda ar be
  let combineF = EC.lambda al $ EC.lambda ar ce
  let tid = sqltablealias "__JR" jal
  ret   <- telemM rt
  aptrs <- sqgextAliasM tid ret ct $ fst penv
  return $ ParseResult (EC.applyMany (EC.project "join" $ pexpr lpr) [pexpr rpr, matchF, combineF]) rt kt tmap tid aptrs

  where joinpr0 = ParseResult (EC.constant $ CBool True) TC.bool TC.bool Nothing Nothing []


sqltableexpr (SubTref _ query al) = do
  qpr  <- sqlquery query
  rt   <- taliascolM al (prt qpr)
  kt   <- twrapcolelemM rt
  tmap <- telemtmapM

  (aenv@(AEnv (tag -> TRecord ids, _) _), penv) <- aenv1 qpr
  let tid = sqltablealias "__RN" al
  ret <- telemM rt
  sqet <- telemM $ prt qpr
  aptrs <- sqgextAliasM tid ret sqet $ fst penv
  case tag ret of
    TRecord nids | ids == nids -> return qpr { palias = tid, pptrs = aptrs }

    TRecord nids | length ids == length nids -> do
      (iOpt, be) <- bindE aenv "x" $ recE $ map (\(n,o) -> (n, EC.variable o)) $ zip nids ids
      mapF <- maybe (tblaliasM qpr) return iOpt >>= \a -> return $ EC.lambda a be
      let rexpr = EC.applyMany (EC.project "map" $ pexpr qpr) [mapF]
      return $ ParseResult rexpr rt kt tmap tid aptrs

    _ -> throwE $ "Invalid subquery alias"

sqltableexpr t@(FunTref _ funE _)  = throwE $ "Unhandled table ref: " ++ show t


sqlwhere :: ParseResult -> MaybeBoolExpr -> SQLParseM ParseResult
sqlwhere tables whereEOpt = flip (maybe $ return tables) whereEOpt $ \whereE -> do
  (aenv, penv) <- aenv1 tables
  wpr  <- sqlscalar aenv penv whereE
  (iOpt, be) <- bindE aenv "x" (pexpr wpr)
  filterF <- maybe (tblaliasM tables) return iOpt >>= \a -> return $ EC.lambda a be
  aptrs <- sqgextIdentityM (palias tables) (prt tables) $ fst penv
  return $ tables { pexpr = EC.applyMany (EC.project "filter" $ pexpr tables) [filterF], pptrs = aptrs }


sqlproject :: ParseResult -> SelectList -> SQLParseM ParseResult
sqlproject limits (SelectList _ projections) = do
  (aenv, penv) <- aenv1 limits
  aggsE <- mapM (sqlaggregate aenv penv) projections

  case partitionEithers aggsE of
    ([], prs) -> aggregate aenv penv aggsE prs
    (prjl, []) -> project aenv penv prjl
    (_, _) -> throwE $ "Invalid mixed select list of aggregates and non-aggregates"

  where
    aggregate aenv penv aggsE aggprs = do
      let (aggexprs, aggtypes) = unzip $ map (pexpr &&& prt) aggprs
      (_, aAliases) <- expraliasesGenM 0 aggprs
      let aidt = zip aAliases aggtypes
      (aiOpt, abe) <- bindE aenv "x" $ case aggexprs of
                        []  -> EC.variable "acc"
                        [e] -> EC.applyMany e [EC.variable "acc"]
                        _   -> recE $ zip aAliases $ map (aggE "acc") $ zip aAliases aggexprs

      aggF <- maybe (tblaliasM limits) return aiOpt >>= \a -> return $ EC.lambda "acc" $ EC.lambda a abe
      zF   <- zeroE aidt

      let rexpr = EC.applyMany (EC.project "fold" $ pexpr limits) [aggF, zF]
      let ral = Just "__R"
      rt <- tcolM $ recT aidt
      let aggprojections = zip aggsE $ map projectionexprs projections
      let sqlaggs = concatMap (\(ae, e) -> if isLeft ae then [] else [e]) aggprojections
      aptrs <- sqgextExprM ral rt sqlaggs penv
      return $ ParseResult rexpr rt rt Nothing ral aptrs

    project aenv penv prjl = do
      eprs <- mapM (sqlprojection aenv penv) prjl
      (_, eAliases) <- expraliasesGenM 0 eprs
      let (exprs, types) = unzip $ map (pexpr &&& prt) eprs
      (iOpt, be) <- bindE aenv "x" $ recE $ zip eAliases exprs
      mapF <- maybe (tblaliasM limits) return iOpt >>= \a -> return $ EC.lambda a be
      let rexpr  = EC.applyMany (EC.project "map" $ pexpr limits) [mapF]
      let ralias = Just "__R"
      rt   <- tcolM $ recT $ zip eAliases types
      kt   <- twrapcolelemM rt
      tmap <- telemtmapM
      aptrs <- sqgextExprM ralias rt (map projectionexprs prjl) penv
      return $ ParseResult rexpr rt kt tmap ralias aptrs


sqlgroupby :: ParseResult -> SelectList -> MaybeBoolExpr -> ScalarExprList -> SQLParseM (ParseResult, SelectList)
sqlgroupby selects (SelectList slann projections) having [] =
  return (selects, SelectList slann $ projections ++ maybe [] (\e -> [SelExp emptyAnnotation e]) having)

sqlgroupby selects (SelectList slann projections) having gbL = do
  (aenv, penv) <- aenv1 selects
  gbprs <- mapM (sqlscalar aenv penv) gbL
  aggsE <- mapM (sqlaggregate aenv penv) projections
  let (nprojects, aggprs) = partitionEithers aggsE

  let ugbprs  = nub gbprs
  let uaggprs = nub aggprs

  let (gbexprs, gbtypes)   = unzip $ map (pexpr &&& prt) ugbprs
  let (aggexprs, aggtypes) = unzip $ map (pexpr &&& prt) uaggprs

  (gaid, gAliases) <- expraliasesGenM 0 ugbprs
  (_, aAliases)    <- expraliasesGenM (if null ugbprs then 1 else gaid) uaggprs

  (hcnt, nhaving, havingaggs) <- maybe (return (0, Nothing, [])) (havingaggregates (zip aAliases uaggprs) aenv penv) having
  ([], havingprs) <- mapM (sqlaggregate aenv penv) havingaggs >>= return . partitionEithers

  let uhvprs = nub havingprs
  let (hvexprs, hvtypes) = unzip $ map (pexpr &&& prt) uhvprs
  let hAliases = if null havingaggs then [] else ["h" ++ show i | i <- [0..hcnt-1]]

  (giOpt, gbe) <- bindE aenv "x" $ case gbexprs of
                    [] -> EC.unit
                    [e] -> e
                    _ -> recE $ zip gAliases gbexprs

  groupF <- maybe (tblaliasM selects) return giOpt >>= \a -> return $ EC.lambda a gbe

  let auaggexprs = aggexprs ++ hvexprs
  let auaggtypes = aggtypes ++ hvtypes
  let auAliases  = aAliases ++ hAliases

  (aiOpt, abe) <- bindE aenv "x" $ case auaggexprs of
                    []  -> EC.variable "acc"
                    [e] -> EC.applyMany e [EC.variable "acc"]
                    _   -> recE $ zip auAliases $ map (aggE "acc") $ zip auAliases auaggexprs

  aggF <- maybe (tblaliasM selects) return aiOpt >>= \a -> return $ EC.lambda "acc" $ EC.lambda a abe
  zF <- zeroE $ zip auAliases auaggtypes

  let tpmap p idtl = map (\(i,_) -> (i, [p, i])) idtl

  let gidt = zip gAliases gbtypes
  let (kidt, keyT, ktpl) = case gbtypes of
                             []  -> ([("f0", TC.unit)], TC.unit, [("f0", ["key"])])
                             [t] -> (gidt, t, [(fst $ head gidt, ["key"])])
                             _   -> (gidt, recT gidt, tpmap "key" gidt)

  let aid0 = if null ugbprs then "f1" else "f" ++ show gaid
  let aidt = zip auAliases auaggtypes
  let (vidt, valT, vtpl) = case auaggtypes of
                             []  -> ([(aid0, TC.unit)], TC.unit, [(aid0, ["value"])])
                             [t] -> (aidt, t, [(fst $ head aidt, ["value"])])
                             _   -> (aidt, recT aidt, tpmap "value" aidt)

  let rexpr  = EC.applyMany (EC.project "groupBy" $ pexpr selects) [groupF, aggF, zF]
  let tmap = Just $ Right $ (Map.fromList ktpl) <> (Map.fromList vtpl)
  let ral = Just "__R"

  rt <- tcolM $ recT $ kidt ++ vidt
  kt <- tcolM $ recT [("key", keyT), ("value", valT)]

  hrexpr <- havingexpr rt tmap ral (fst penv) aenv nhaving rexpr

  let aggprojections = zip aggsE $ map projectionexprs projections
  let sqlaggs = concatMap (\(ae, e) -> if isLeft ae then [] else [e]) aggprojections
  aptrs <- sqgextExprM ral rt (sqlaggs ++ maybe [] (:[]) having) penv

  let naggprojects = map (\(i,_) -> SelExp emptyAnnotation $ Identifier emptyAnnotation $ Nmc i) vidt
  return (ParseResult hrexpr rt kt tmap ral aptrs, SelectList slann $ nprojects ++ naggprojects)

  where havingexpr _ _ _ _ _ Nothing e = return e
        havingexpr rt tmap alias ptrs (AEnv _ q) (Just he) e = do
          nmert <- mertypeR rt tmap alias
          let aenv = AEnv nmert q
          penv <- adgenv1R ptrs alias
          hpr <- sqlscalar aenv penv he
          (iOpt, be) <- bindE aenv "x" (pexpr hpr)
          filterF <- maybe (return "__R") return iOpt >>= \a -> return $ EC.lambda a be
          return $ EC.applyMany (EC.project "filter" $ e) [filterF]

        havingaggregates aggaprs aenv penv e = do
          (a,b,c) <- extractaggregates aggaprs aenv penv 0 e
          nc <- mapM (\ae -> return $ SelExp emptyAnnotation ae) c
          return (a, Just b, nc)


-- TODO
sqlsort :: ParseResult -> ScalarExprDirectionPairList -> SQLParseM ParseResult
sqlsort having orderL = return having

sqltopk :: ParseResult -> MaybeBoolExpr -> MaybeBoolExpr -> SQLParseM ParseResult
sqltopk sorted limitE offsetE = return sorted


sqljoinexpr :: AEnv -> ADGPtrEnv -> JoinExpr -> SQLParseM ParseResult
sqljoinexpr aenv penv (JoinOn _ e) = sqlscalar aenv penv e
sqljoinexpr _ _ je@(JoinUsing _ _) = throwE $ "Unhandled join expression" ++ show je

sqlaggregate :: AEnv -> ADGPtrEnv -> SelectItem -> SQLParseM (Either SelectItem ParseResult)
sqlaggregate aenv penv si@(SelExp _ e) = sqlaggexpr aenv penv e >>= return . maybe (Left si) Right
sqlaggregate aenv penv si@(SelectItem _ e nm) = do
  prOpt <- sqlaggexpr aenv penv e
  return $ maybe (Left si) (\pr -> Right $ pr { palias = Just (sqlnmcomponent nm) }) prOpt

sqlprojection :: AEnv -> ADGPtrEnv -> SelectItem -> SQLParseM ParseResult
sqlprojection aenv penv (SelExp _ e) = sqlscalar aenv penv e
sqlprojection aenv penv (SelectItem _ e nm) = do
  pr <- sqlscalar aenv penv e
  return pr { palias = Just (sqlnmcomponent nm) }


pr0 :: K3 Type -> K3 Expression -> SQLParseM ParseResult
pr0 t e = return $ ParseResult e t t Nothing Nothing []

pri0 :: Identifier -> K3 Type -> K3 Expression -> SQLParseM ParseResult
pri0 i t e = return $ ParseResult e t t Nothing (Just i) []

-- TODO: avg
sqlaggexpr :: AEnv -> ADGPtrEnv -> ScalarExpr -> SQLParseM (Maybe ParseResult)
sqlaggexpr aenv penv (FunCall _ nm args) = do
  let fn = sqlnm nm
  case (fn, args) of
    ("sum"  , [e]) -> sqlscalar aenv penv e >>= aggop "sum_" (\a b -> EC.binop OAdd a b)
    ("count", [e]) -> sqlscalar aenv penv e >>= aggop "cnt_" (\a _ -> EC.binop OAdd a $ EC.constant $ CInt 1)
    ("min"  , [e]) -> sqlscalar aenv penv e >>= aggop "min_" (\a b -> EC.applyMany (EC.variable "min") [a, b])
    ("max"  , [e]) -> sqlscalar aenv penv e >>= aggop "max_" (\a b -> EC.applyMany (EC.variable "max") [a, b])
    (_, _) -> return Nothing

  where
    aggop i f pr = return $ Just $ pr { pexpr  = EC.lambda "aggacc" $ f (EC.variable "aggacc") (pexpr pr)
                                      , palias = maybe Nothing (\j -> Just $ i ++ j) $ palias pr }

sqlaggexpr _ _ _ = return Nothing


extractaggregates :: [(Identifier, ParseResult)] -> AEnv -> ADGPtrEnv -> Int -> ScalarExpr
                  -> SQLParseM (Int, ScalarExpr, [ScalarExpr])
extractaggregates aggaprs aenv penv i e@(FunCall ann nm args) = do
  aggeopt <- sqlaggexpr aenv penv e
  case aggeopt of
    Nothing -> do
      (ni, nxagg, nargs) <- foldM foldOnChild (i,[],[]) args
      return (ni, FunCall ann nm nargs, nxagg)

    Just pr -> let aprOpt = find (\(_,aggpr) -> pr == aggpr) aggaprs
                   (nid, nex) = maybe (aggnm i, [e]) (\(a,_) -> (Nmc a, [])) aprOpt
               in return (i+1, Identifier emptyAnnotation nid, nex)

  where foldOnChild (j,acc,argacc) ce = do
          (nj, nce, el) <- extractaggregates aggaprs aenv penv j ce
          return (nj, acc++el, argacc++[nce])

        aggnm j = Nmc $ "h" ++ show j

extractaggregates _ _ _ i e = return (i,e,[])


-- TODO: variable access in correlated subqueries, nulls
sqlscalar :: AEnv -> ADGPtrEnv -> ScalarExpr -> SQLParseM ParseResult
sqlscalar _ _ (BooleanLit _ b) = pr0 TC.bool $ EC.constant $ CBool b

sqlscalar _ _ (NumberLit _ i) = if "." `isInfixOf` i
                                  then pr0 TC.real $ EC.constant $ CReal $ read i
                                  else pr0 TC.int  $ EC.constant $ CInt $ read i

sqlscalar _ _ (StringLit _ s) = pr0 TC.string $ EC.constant $ CString s

sqlscalar (AEnv (ret@(tnc -> (TRecord ids, ch)), _) _) penv (Identifier _ (sqlnmcomponent -> i)) = do
    idx <- sqgidxM $ fst penv
    p <- maybe (plkuperr i) return $ Map.lookup i idx
    maybe (varerror i) (\t -> pri0 i t $ EC.variable i @+ (sqlAttrProp p)) $ lookup i (zip ids ch)

  where
    varerror n = throwE $ boxToString $ ["Unknown unqualified variable " ++ n] %$ prettyLines ret
    plkuperr n = throwE $ "No attribute graph node found for " ++ n


sqlscalar (AEnv _ q) penv e@(QIdentifier _ nmcl) =
  case nmcl of
    [x,y] -> qpair x y
    _ -> qiderr

  where qpair (sqlnmcomponent -> nx) (sqlnmcomponent -> ny) = do
          let knx = "__" ++ nx
          qidx <- Map.fromList <$> mapM (\(i,ps) -> sqgidxM ps >>= return . (i,)) (snd penv)
          p <- maybe (lkuperr knx) (\idx -> maybe (lkuperr ny) return $ Map.lookup ny idx) $ Map.lookup knx qidx
          case Map.lookup knx q of
            Just (tnc -> (TRecord ids, ch), _, _) ->
              let idt = zip ids ch in
              maybe qidterr (\t -> pr0 t $ (EC.project ny $ EC.variable knx) @+ sqlAttrProp p) $ lookup ny idt

            Just _  -> qidelemerr
            Nothing -> qidlkuperr

        qiderr     = throwE $ unwords ["Invalid qualified identifier:", show e, show $ Map.keys q]
        qidterr    = throwE $ unwords ["Unknown qualified identifier type:", show e, show $ Map.keys q]
        qidelemerr = throwE $ unwords ["Invalid element for qid:", show e, show $ Map.keys q]
        qidlkuperr = throwE $ unwords ["Unknown qid component:", show e, show $ Map.keys q]
        lkuperr  v = throwE $ "No attribute graph node found for " ++ v

sqlscalar aenv penv (FunCall _ nm args) = do
  let fn = sqlnm nm
  case sqloperator fn args of
    (Just (UnaryOp  o x))   -> do
      xpr <- sqlscalar aenv penv x
      return (xpr {pexpr = EC.unop o $ pexpr xpr})

    (Just (BinaryOp o x y)) -> do
      xpr <- sqlscalar aenv penv x
      ypr <- sqlscalar aenv penv y
      pr0 (prt xpr) $ EC.binop o (pexpr xpr) $ pexpr ypr

    _ -> do
      case (fn, args) of
        ("!between", [_,_,_]) -> do
          [apr,bpr,cpr] <- mapM (sqlscalar aenv penv) args
          pr0 TC.bool $ EC.binop OAnd (EC.binop OLeq (pexpr bpr) $ pexpr apr)
                                     $ EC.binop OLeq (pexpr apr) $ pexpr cpr

        (_, _) -> do
          aprl <- mapM (sqlscalar aenv penv) args
          pr0 TC.unit $ EC.applyMany (EC.variable fn) $ map pexpr aprl -- TODO: return type?

sqlscalar (AEnv (ret@(tag -> TRecord ids), _) _) _ (Star _) =
  pr0 ret $ recE $ map (\i -> (i, EC.variable i)) ids

sqlscalar (AEnv _ q) _ (QStar _ (sqlnmcomponent -> n)) = do
  let kn = "__" ++ n
  case Map.lookup kn q of
    Just (qt@(tag -> TRecord ids), _, _) -> pr0 qt $ recE $ map (\i -> (i, EC.project i $ EC.variable kn)) ids
    Nothing -> throwE $ "Invalid qualified name in qstar"
    Just _ -> throwE $ "Invalid element type in qualified environment"

sqlscalar aenv penv (Case _ whense elsee) = do
  prl <- forM whense $ \(whenel, thene) -> (,) <$> mapM (sqlscalar aenv penv) whenel <*> sqlscalar aenv penv thene
  (eT, eE, wprl) <- case (prl, elsee) of
    ([], Nothing) -> throwE $ "Invalid case expression"
    (l, Nothing) -> let t = prt $ snd $ head l
                    in either throwE (\de -> return (t, de, l)) $ defaultExpression t
    (l, Just e) -> sqlscalar aenv penv e >>= \epr -> return (prt epr, pexpr epr, l)

  pr0 eT $ foldl (\accE (wprs, tpr) -> EC.ifThenElse (andE wprs) (pexpr tpr) accE) eE $ reverse wprl

  where andE [] = EC.constant $ CBool False
        andE (h:t) = foldl (\accE wpr -> EC.binop OAnd accE $ pexpr wpr) (pexpr h) t

sqlscalar aenv penv (CaseSimple _ e whense elsee) = do
  vpr <- sqlscalar aenv penv e
  prl <- forM whense $ \(whenel, thene) -> (,) <$> mapM (sqlscalar aenv penv) whenel <*> sqlscalar aenv penv thene
  (eT, eE, wprl) <- case (prl, elsee) of
    ([], Nothing) -> throwE $ "Invalid case expression"
    (l, Nothing) -> let t = prt $ snd $ head l
                    in either throwE (\de -> return (t, de, l)) $ defaultExpression t
    (l, Just ee) -> sqlscalar aenv penv ee >>= \epr -> return (prt epr, pexpr epr, l)

  pr0 eT $ memoE (pexpr vpr) $ \ve ->
    foldl (\accE (wprs, tpr) -> EC.ifThenElse (andE ve wprs) (pexpr tpr) accE) eE $ reverse wprl

  where andE _ [] = EC.constant $ CBool False
        andE ve (h:t) = foldl (\accE wpr -> EC.binop OAnd accE $ EC.binop OEqu ve $ pexpr wpr) (EC.binop OEqu ve $ pexpr h) t

sqlscalar _ _ (ScalarSubQuery _ qe) = sqlquery qe

sqlscalar _ _ (Exists _ qe) = do
  qpr <- sqlquery qe
  pr0 TC.bool $ emptyE False $ pexpr qpr

sqlscalar aenv penv (InPredicate _ e isIn inl) = do
  epr <- sqlscalar aenv penv e
  case inl of
    InList _ el -> do
      eprl <- mapM (sqlscalar aenv penv) el
      case eprl of
        [] -> pr0 TC.bool $ EC.constant $ CBool $ not isIn
        (h:t) -> pr0 TC.bool $ memoE (immutE $ pexpr epr) $
                      \ve -> foldl (\accE p -> mergeE accE $ testE ve p) (testE ve h) t

    InQueryExpr _ q -> do
      qpr <- sqlquery q
      pr0 TC.bool $ memberE isIn (pexpr epr) $ pexpr qpr

  where testE ve pr = EC.binop (if isIn then OEqu else ONeq) ve $ pexpr pr
        mergeE accE nE = EC.binop (if isIn then OOr else OAnd) accE nE

sqlscalar _ _ e = throwE $ "Unhandled scalar expr: " ++ show e


sqloperator :: String -> ScalarExprList -> Maybe OperatorFn
sqloperator "-"    [x]   = Just (UnaryOp  ONeg x)
sqloperator "!not" [x]   = Just (UnaryOp  ONot x)
sqloperator "+"    [x,y] = Just (BinaryOp OAdd x y)
sqloperator "-"    [x,y] = Just (BinaryOp OSub x y)
sqloperator "*"    [x,y] = Just (BinaryOp OMul x y)
sqloperator "/"    [x,y] = Just (BinaryOp ODiv x y)
sqloperator "%"    [x,y] = Just (BinaryOp OMod x y)
sqloperator "="    [x,y] = Just (BinaryOp OEqu x y)
sqloperator "!="   [x,y] = Just (BinaryOp ONeq x y)
sqloperator "<>"   [x,y] = Just (BinaryOp ONeq x y)
sqloperator "<"    [x,y] = Just (BinaryOp OLth x y)
sqloperator "<="   [x,y] = Just (BinaryOp OGeq x y)
sqloperator ">"    [x,y] = Just (BinaryOp OGth x y)
sqloperator ">="   [x,y] = Just (BinaryOp OGeq x y)
sqloperator "!and" [x,y] = Just (BinaryOp OAnd x y)
sqloperator "!or"  [x,y] = Just (BinaryOp OOr  x y)
sqloperator _ _ = Nothing


-- | Type construction
sqltabletype :: AttributeDefList -> SQLParseM (K3 Type)
sqltabletype attrs = sqlrectype attrs >>= tcolM

sqlrectype :: AttributeDefList -> SQLParseM (K3 Type)
sqlrectype attrs = mapM sqlattr attrs >>= \ts -> return (recT ts)

sqlattr :: AttributeDef -> SQLParseM (Identifier, K3 Type)
sqlattr (AttributeDef _ nm t _ _) = sqltypename t >>= sqltype >>= return . (sqlnmcomponent nm,)

sqltype :: String -> SQLParseM (K3 Type)
sqltype s = case s of
  "int"              -> return TC.int
  "integer"          -> return TC.int
  "real"             -> return TC.real
  "double precision" -> return TC.real
  "text"             -> return TC.string
  _ -> throwE $ "Invalid K3-SQL type: " ++ s

sqlnm :: Name -> String
sqlnm (Name _ comps) = concatMap sqlnmcomponent comps

sqlnmcomponent :: NameComponent -> String
sqlnmcomponent (Nmc s) = s
sqlnmcomponent (QNmc s) = s

sqltypename :: TypeName -> SQLParseM String
sqltypename (SimpleTypeName _ t) = return t
sqltypename t = throwE $ "Invalid sql typename " ++ show t

sqltablealias :: Identifier -> TableAlias -> Maybe Identifier
sqltablealias def alias = case alias of
    NoAlias _        -> Just def
    TableAlias _ nc  -> Just $ "__" ++ sqlnmcomponent nc
    FullAlias _ nc _ -> Just $ "__" ++ sqlnmcomponent nc
