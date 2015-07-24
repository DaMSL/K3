{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

module Language.K3.Parser.SQL where

import Control.Arrow ( (&&&), second )
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Except

import Data.Functor.Identity
import Data.Monoid
import Data.Either ( partitionEithers )
import Data.List ( find, nub )
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
import qualified Language.K3.Core.Constructor.Type        as TC

import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

data OperatorFn = UnaryOp Operator ScalarExpr
                | BinaryOp Operator ScalarExpr ScalarExpr
                deriving (Eq, Show)

-- | Relation names and types.
type RTypeEnv = Map Identifier (K3 Type)

data SQLEnv = SQLEnv { relations :: RTypeEnv }
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
type MRType  = (K3 Type, TLabelMapping)
type MERType = (K3 Type, TValueMapping)

-- | Qualified attribute env
type AQEnv = Map Identifier (Either MRType [Identifier])

-- | Attribute binding env
data AEnv = AEnv { aeuq :: MERType, aeq :: AQEnv }
            deriving (Eq, Show)

data ParseResult = ParseResult { pexpr  :: K3 Expression
                               , prt    :: K3 Type
                               , pkt    :: K3 Type
                               , ptmap  :: TLabelMapping
                               , palias :: Maybe Identifier }
                  deriving (Eq, Show)


{- Data.Text helpers -}

sqlenv0 :: SQLEnv
sqlenv0 = SQLEnv Map.empty

stlkup :: RTypeEnv -> Identifier -> Except String (K3 Type)
stlkup env x = maybe err return $ Map.lookup x env
  where err = throwE msg
        msg = "Unknown relation in sql parser environment: " ++ show x

stext :: RTypeEnv -> Identifier -> K3 Type -> RTypeEnv
stext env x t = Map.insert x t env

stdel :: RTypeEnv -> Identifier -> RTypeEnv
stdel env x = Map.delete x env

sqelkup :: SQLEnv -> Identifier -> Except String (K3 Type)
sqelkup senv n = stlkup (relations senv) n

sqeext :: SQLEnv -> Identifier -> K3 Type -> SQLEnv
sqeext senv n t = senv { relations = stext (relations senv) n t }

sqedel :: SQLEnv -> Identifier -> SQLEnv
sqedel senv n = senv { relations = stdel (relations senv) n }

sqelkupM :: Identifier -> SQLParseM (K3 Type)
sqelkupM n = get >>= liftExceptM . (\env -> sqelkup env n)

sqeextM :: Identifier -> K3 Type -> SQLParseM ()
sqeextM n t = get >>= \env -> return (sqeext env n t) >>= put


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
mrtype r = telemM (prt r) >>= \ret -> return (ret, ptmap r)

mertypeR :: K3 Type -> TLabelMapping -> Maybe Identifier -> SQLParseM MERType
mertypeR t tmap alias = do
  ret <- telemM t
  return (ret, maybe (TVMType tmap) (\i -> TVMNamed (i, tmap)) alias)

mertype :: ParseResult -> SQLParseM MERType
mertype r = mertypeR (prt r) (ptmap r) $ palias r

aqenv0 :: [(Identifier, MRType)] -> AQEnv
aqenv0 l = Map.fromList $ map (second Left) l

aqenv1 :: ParseResult -> SQLParseM AQEnv
aqenv1 r = do
  mrt <- mrtype r
  return $ maybe Map.empty (\i -> aqenv0 [(i, mrt)]) $ palias r

aqenvl :: [ParseResult] -> SQLParseM AQEnv
aqenvl l = mapM aqenv1 l >>= return . mconcat

aenv0 :: MERType -> AQEnv -> AEnv
aenv0 u q = AEnv u q

aenv1 :: ParseResult -> SQLParseM AEnv
aenv1 r = (\mert q -> aenv0 mert q) <$> mertype r <*> aqenv1 r

aenvl :: [ParseResult] -> SQLParseM AEnv
aenvl l = (\mert q -> aenv0 mert q) <$> mr0 <*> aqenvl l
  where mr0 = mapM mertype l >>= return . concatmer
        concatmer merl =
          let (uniqids, idt, idvt) = foldl accmer (Set.empty, [], []) merl
              uidt  = filter ((`Set.member` uniqids) . fst) idt
              uidvt = concatMap (fieldvt uniqids) idvt
          in (recT uidt, TVMComposite $ Map.fromList uidvt)

        accmer (accS, accL, tvmaccL) (tnc -> (TRecord ids, ch), tvm) =
          let new     = Set.fromList ids
              common  = Set.intersection accS new
              newaccS = Set.difference (Set.union accS new) common
          in (newaccS, accL ++ zip ids ch, tvmaccL ++ map (, extractvt tvm) ids)

        accmer acc _ = acc

        fieldvt uniqids (i,vt) = if i `Set.member` uniqids then maybe [] (\x -> [(i,x)]) vt else []

        extractvt (TVMNamed vt) = Just vt
        extractvt _ = Nothing


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


{- Parsing toplevel. -}
k3ofsql :: Bool -> FilePath -> IO ()
k3ofsql asSyntax path = do
  stmtE <- parseStatementsFromFile path
  either (putStrLn . show) k3program stmtE

  where
    k3program stmts = do
      let declsM = mapM sqlstmt stmts
      let progE = do { decls <- evalSQLParseM sqlenv0 declsM;
                        return $ DC.role "__global" $ concat decls }
      if asSyntax
        then either putStrLn (either putStrLn putStrLn . programS) progE
        else either putStrLn (putStrLn . pretty) progE

sqlstmt :: Statement -> SQLParseM [K3 Declaration]
sqlstmt (CreateTable _ nm attrs _) = do
  t <- sqltabletype attrs
  sqeextM (sqlnm nm) t
  return [DC.global (sqlnm nm) t Nothing]

sqlstmt (QueryStatement _ query) = sqlquerystmt query
sqlstmt s = throwE $ "Unimplemented SQL stmt: " ++ show s


sqlquerystmt :: QueryExpr -> SQLParseM [K3 Declaration]
sqlquerystmt q = do
  qpr <- sqlquery q
  return [ DC.global "result" (pkt qpr) Nothing
         , DC.trigger "query" TC.unit $ EC.assign "result" $ pexpr qpr ]


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
          aenv@(AEnv (ct, _) _) <- aenvl [lpr, rpr]
          (al, ar) <- (,) <$> tblaliasM lpr <*> tblaliasM rpr
          ce   <- concatE aenv
          rt   <- tcolM ct
          kt   <- twrapcolelemM rt
          tmap <- telemtmapM
          let matchF   = EC.lambda "_" $ EC.lambda "_" $ EC.constant $ CBool True
          let combineF = EC.lambda al $ EC.lambda ar ce
          let rexpr    = EC.applyMany (EC.project "join" $ pexpr lpr) [pexpr rpr, matchF, combineF]
          return $ ParseResult rexpr rt kt tmap $ Just "CP"


sqltableexpr :: TableRef -> SQLParseM ParseResult
sqltableexpr (Tref _ nm al) = do
    t  <- sqelkupM tnm
    rt <- taliascolM al t
    kt <- twrapcolelemM rt
    tmap <- telemtmapM
    return $ ParseResult (EC.variable tnm) rt kt tmap $ sqltablealias (sqlnm nm) al
  where tnm = sqlnm nm

-- TODO: nat, jointy
sqltableexpr (JoinTref _ jlt nat jointy jrt onE jal) = do
  lpr <- sqltableexpr jlt
  rpr <- sqltableexpr jrt
  aenv@(AEnv (ct, _) _) <- aenvl [lpr, rpr]
  (al, ar) <- (,) <$> tblaliasM lpr <*> tblaliasM rpr
  joinpr   <- maybe (return joinpr0) (sqljoinexpr aenv) onE
  ce   <- concatE aenv
  rt   <- tcolM ct >>= taliascolM jal
  kt   <- twrapcolelemM rt
  tmap <- telemtmapM
  (Nothing, be) <- bindE aenv "x" (pexpr joinpr)
  let matchF   = EC.lambda al $ EC.lambda ar be
  let combineF = EC.lambda al $ EC.lambda ar ce
  return $ ParseResult (EC.applyMany (EC.project "join" $ pexpr lpr) [pexpr rpr, matchF, combineF]) rt kt tmap $ sqltablealias "JR" jal

  where joinpr0 = ParseResult (EC.constant $ CBool True) TC.bool TC.bool Nothing Nothing

sqltableexpr (SubTref _ query al) = do
  qpr <- sqlquery query
  rt   <- taliascolM al (prt qpr)
  kt   <- twrapcolelemM rt
  tmap <- telemtmapM

  aenv@(AEnv (tag -> TRecord ids, _) _) <- aenv1 qpr
  case tag rt of
    TRecord nids | length ids == length nids -> do
      (iOpt, be) <- bindE aenv "x" $ recE $ map (\(n,o) -> (n, EC.variable o)) $ zip nids ids
      mapF <- maybe (tblaliasM qpr) return iOpt >>= \a -> return $ EC.lambda a be
      let rexpr = EC.applyMany (EC.project "map" $ pexpr qpr) [mapF]
      return $ ParseResult rexpr rt kt tmap $ sqltablealias "RN" al

    _ -> throwE $ "Invalid subquery alias"

sqltableexpr t@(FunTref _ funE _)  = throwE $ "Unhandled table ref: " ++ show t


sqlwhere :: ParseResult -> MaybeBoolExpr -> SQLParseM ParseResult
sqlwhere tables whereEOpt = flip (maybe $ return tables) whereEOpt $ \whereE -> do
  aenv <- aenv1 tables
  wpr  <- sqlscalar aenv whereE
  (iOpt, be) <- bindE aenv "x" (pexpr wpr)
  filterF <- maybe (tblaliasM tables) return iOpt >>= \a -> return $ EC.lambda a be
  return $ tables { pexpr = EC.applyMany (EC.project "filter" $ pexpr tables) [filterF] }


sqlproject :: ParseResult -> SelectList -> SQLParseM ParseResult
sqlproject limits (SelectList _ projections) = do
  aenv     <- aenv1 limits
  (nprojects, aggprs) <- mapM (sqlaggregate aenv) projections >>= return . partitionEithers
  case (nprojects, aggprs) of
    ([], prs) -> aggregate aenv prs
    (prjl, []) -> project aenv prjl
    (_, _) -> throwE $ "Invalid mixed select list of aggregates and non-aggregates"

  where
    aggregate aenv aggprs = do
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
      let ral = Just "R"
      rt <- tcolM $ recT aidt
      return $ ParseResult rexpr rt rt Nothing ral

    project aenv prjl = do
      eprs <- mapM (sqlprojection aenv) prjl
      (_, eAliases) <- expraliasesGenM 0 eprs
      let (exprs, types) = unzip $ map (pexpr &&& prt) eprs
      (iOpt, be) <- bindE aenv "x" $ recE $ zip eAliases exprs
      mapF <- maybe (tblaliasM limits) return iOpt >>= \a -> return $ EC.lambda a be
      let rexpr  = EC.applyMany (EC.project "map" $ pexpr limits) [mapF]
      let ralias = Just "R"
      rt   <- tcolM $ recT $ zip eAliases types
      kt   <- twrapcolelemM rt
      tmap <- telemtmapM
      return $ ParseResult rexpr rt kt tmap ralias


sqlgroupby :: ParseResult -> SelectList -> MaybeBoolExpr -> ScalarExprList -> SQLParseM (ParseResult, SelectList)
sqlgroupby selects (SelectList slann projections) having [] =
  return (selects, SelectList slann $ projections ++ maybe [] (\e -> [SelExp emptyAnnotation e]) having)

sqlgroupby selects (SelectList slann projections) having gbL = do
  aenv  <- aenv1 selects
  gbprs <- mapM (sqlscalar aenv) gbL
  (nprojects, aggprs) <- mapM (sqlaggregate aenv) projections >>= return . partitionEithers

  let ugbprs  = nub gbprs
  let uaggprs = nub aggprs

  let (gbexprs, gbtypes)   = unzip $ map (pexpr &&& prt) ugbprs
  let (aggexprs, aggtypes) = unzip $ map (pexpr &&& prt) uaggprs

  (gaid, gAliases) <- expraliasesGenM 0 ugbprs
  (_, aAliases)    <- expraliasesGenM (if null ugbprs then 1 else gaid) uaggprs

  (hcnt, nhaving, havingaggs) <- maybe (return (0, Nothing, [])) (havingaggregates (zip aAliases uaggprs) aenv) having
  ([], havingprs) <- mapM (sqlaggregate aenv) havingaggs >>= return . partitionEithers

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
  let ral = Just "R"

  rt <- tcolM $ recT $ kidt ++ vidt
  kt <- tcolM $ recT [("key", keyT), ("value", valT)]

  hrexpr <- havingexpr rt tmap ral aenv nhaving rexpr

  let naggprojects = map (\(i,_) -> SelExp emptyAnnotation $ Identifier emptyAnnotation $ Nmc i) vidt
  return (ParseResult hrexpr rt kt tmap ral, SelectList slann $ nprojects ++ naggprojects)

  where havingexpr _ _ _ _ Nothing e = return e
        havingexpr rt tmap alias (AEnv _ q) (Just he) e = do
          nmert <- mertypeR rt tmap alias
          let aenv = AEnv nmert q
          hpr <- sqlscalar aenv he
          (iOpt, be) <- bindE aenv "x" (pexpr hpr)
          filterF <- maybe (return "R") return iOpt >>= \a -> return $ EC.lambda a be
          return $ EC.applyMany (EC.project "filter" $ e) [filterF]

        havingaggregates aggaprs aenv e = do
          (a,b,c) <- extractaggregates aggaprs aenv 0 e
          nc <- mapM (\e -> return $ SelExp emptyAnnotation e) c
          return (a, Just b, nc)


-- TODO
sqlsort :: ParseResult -> ScalarExprDirectionPairList -> SQLParseM ParseResult
sqlsort having orderL = return having

sqltopk :: ParseResult -> MaybeBoolExpr -> MaybeBoolExpr -> SQLParseM ParseResult
sqltopk sorted limitE offsetE = return sorted


sqljoinexpr :: AEnv -> JoinExpr -> SQLParseM ParseResult
sqljoinexpr aenv (JoinOn _ e) = sqlscalar aenv e
sqljoinexpr _ je@(JoinUsing _ _) = throwE $ "Unhandled join expression" ++ show je

sqlaggregate :: AEnv -> SelectItem -> SQLParseM (Either SelectItem ParseResult)
sqlaggregate aenv si@(SelExp _ e) = sqlaggexpr aenv e >>= return . maybe (Left si) Right
sqlaggregate aenv si@(SelectItem _ e nm) = do
  prOpt <- sqlaggexpr aenv e
  return $ maybe (Left si) (\pr -> Right $ pr { palias = Just (sqlnmcomponent nm) }) prOpt

sqlprojection :: AEnv -> SelectItem -> SQLParseM ParseResult
sqlprojection aenv (SelExp _ e) = sqlscalar aenv e
sqlprojection aenv (SelectItem _ e nm) = do
  pr <- sqlscalar aenv e
  return pr { palias = Just (sqlnmcomponent nm) }


pr0 :: K3 Type -> K3 Expression -> SQLParseM ParseResult
pr0 t e = return $ ParseResult e t t Nothing Nothing

pri0 :: Identifier -> K3 Type -> K3 Expression -> SQLParseM ParseResult
pri0 i t e = return $ ParseResult e t t Nothing $ Just i

-- TODO: avg
sqlaggexpr :: AEnv -> ScalarExpr -> SQLParseM (Maybe ParseResult)
sqlaggexpr aenv (FunCall _ nm args) = do
  let fn = sqlnm nm
  case (fn, args) of
    ("sum"  , [e]) -> sqlscalar aenv e >>= aggop "sum_" (\a b -> EC.binop OAdd a b)
    ("count", [e]) -> sqlscalar aenv e >>= aggop "cnt_" (\a _ -> EC.binop OAdd a $ EC.constant $ CInt 1)
    ("min"  , [e]) -> sqlscalar aenv e >>= aggop "min_" (\a b -> EC.applyMany (EC.variable "min") [a, b])
    ("max"  , [e]) -> sqlscalar aenv e >>= aggop "max_" (\a b -> EC.applyMany (EC.variable "max") [a, b])
    (_, _) -> return Nothing

  where
    aggop i f pr = return $ Just $ pr { pexpr  = EC.lambda "aggacc" $ f (EC.variable "aggacc") (pexpr pr)
                                      , palias = maybe Nothing (\j -> Just $ i ++ j) $ palias pr }

sqlaggexpr _ _ = return Nothing

extractaggregates :: [(Identifier, ParseResult)] -> AEnv -> Int -> ScalarExpr
                  -> SQLParseM (Int, ScalarExpr, [ScalarExpr])
extractaggregates aggaprs aenv i e@(FunCall ann nm args) = do
  aggeopt <- sqlaggexpr aenv e
  case aggeopt of
    Nothing -> do
      (ni, nxagg, nargs) <- foldM foldOnChild (i,[],[]) args
      return (ni, FunCall ann nm nargs, nxagg)

    Just pr -> let aprOpt = find (\(_,aggpr) -> pr == aggpr) aggaprs
                   (nid, nex) = maybe (aggnm i, [e]) (\(a,_) -> (Nmc a, [])) aprOpt
               in return (i+1, Identifier emptyAnnotation nid, nex)

  where foldOnChild (j,acc,argacc) ce = do
          (nj, nce, el) <- extractaggregates aggaprs aenv j ce
          return (nj, acc++el, argacc++[nce])

        aggnm j = Nmc $ "h" ++ show j

extractaggregates _ _ i e = return (i,e,[])


-- TODO
sqlscalar :: AEnv -> ScalarExpr -> SQLParseM ParseResult
sqlscalar _ (BooleanLit _ b)  = pr0 TC.bool   $ EC.constant $ CBool b
sqlscalar _ (NumberLit _ i)   = pr0 TC.int    $ EC.constant $ CInt $ read i  -- TODO: double?
sqlscalar _ (StringLit _ s)   = pr0 TC.string $ EC.constant $ CString s

sqlscalar (AEnv (ret@(tnc -> (TRecord ids, ch)), _) _) (Identifier _ (sqlnmcomponent -> i)) =
    maybe (varerror i) (\t -> pri0 i t $ EC.variable i) $ lookup i (zip ids ch)
  where varerror n = throwE $ boxToString $ ["Unknown unqualified variable " ++ n] %$ prettyLines ret

sqlscalar aenv (FunCall _ nm args) = do
  let fn = sqlnm nm
  case sqloperator fn args of
    (Just (UnaryOp  o x))   -> do
      xpr <- sqlscalar aenv x
      return (xpr {pexpr = EC.unop o $ pexpr xpr})

    (Just (BinaryOp o x y)) -> do
      xpr <- sqlscalar aenv x
      ypr <- sqlscalar aenv y
      pr0 (prt xpr) $ EC.binop o (pexpr xpr) $ pexpr ypr

    _ -> do
      aprl <- mapM (sqlscalar aenv) args
      pr0 TC.unit $ EC.applyMany (EC.variable fn) $ map pexpr aprl -- TODO: return type?

sqlscalar (AEnv (ret@(tag -> TRecord ids), _) _) (Star _) =
  pr0 ret $ recE $ map (\i -> (i, EC.variable i)) ids

sqlscalar _ e = throwE $ "Unhandled scalar expr: " ++ show e


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
    TableAlias _ nc  -> Just $ sqlnmcomponent nc
    FullAlias _ nc _ -> Just $ sqlnmcomponent nc
