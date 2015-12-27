{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Metaprogram.Primitives.Values where
import Control.Monad.Identity
import Control.Arrow ( (&&&), (***), first )

import Data.Char
import Data.Function
import Data.List
import Data.Maybe
import Data.Tree
import qualified Data.Map as Map
import qualified Data.Set as Set

import Debug.Trace

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal
import Language.K3.Core.Utils

import Language.K3.Core.Constructor.Type       as TC
import Language.K3.Core.Constructor.Literal    as LC
import Language.K3.Core.Constructor.Expression as EC

import Language.K3.Core.Metaprogram

import Language.K3.Utils.Pretty

{- Annotation propagation. -}
rewriteChildren :: SpliceValue -> [String] -> SpliceValue
rewriteChildren (SExpr (Node tg ch)) ns = SExpr $ Node tg $ map (\c -> foldl (\c' i -> c' @+ (EApplyGen True i emptySpliceEnv)) c ns) ch
rewriteChildren _ _ = error "Invalid expression for annotateChildren"


{- Splice value extractors -}
idOfSLabel :: SpliceValue -> Maybe Identifier
idOfSLabel (SLabel i) = Just i
idOfSLabel _ = Nothing

tyOfSType :: SpliceValue -> Maybe (K3 Type)
tyOfSType (SType t) = Just t
tyOfSType _ = Nothing

expOfSExpr :: SpliceValue -> Maybe (K3 Expression)
expOfSExpr (SExpr e) = Just e
expOfSExpr _ = Nothing

elemsOfSList :: SpliceValue -> Maybe [SpliceValue]
elemsOfSList (SList svs) = Just svs
elemsOfSList _ = Nothing


{- Splice value operations -}
concatLabel :: SpliceValue -> SpliceValue -> SpliceValue
concatLabel (SLabel a) (SLabel b) = SLabel $ a ++ b
concatLabel _ _ = error "Invalid splice labels"

concatLabels :: SpliceValue -> SpliceValue
concatLabels (SList vs) = foldl concatLabel (SLabel "") vs
concatLabels _ = error "Invalid splice value container for concatLabels"

appendLabel :: SpliceValue -> String -> SpliceValue
appendLabel (SLabel a) s = SLiteral $ LC.string $ a ++ s
appendLabel _ _ = error "Invalid label/literal concatenation."

mkRecord :: SpliceValue -> SpliceValue
mkRecord (SList vs) = maybe err (SType . TC.record) $ mapM mkRecField vs
  where mkRecField v = do
          (SLabel n) <- ltLabel v
          (SType t)  <- ltType v
          return (n,t)
        err = error "Invalid splice container elements for mkRecord"

mkRecord _ = error "Invalid splice value container for mkRecord"

mkTuple :: SpliceValue -> SpliceValue
mkTuple (SList vs) = maybe err (SType . TC.tuple) $ mapM asType vs
  where asType (SType t) = Just t
        asType _ = Nothing
        err = error "Invalid splice container elements for mkTuple"

mkTuple _ = error "Invalid splice value container for mkTuple"

mkEmpty :: SpliceValue -> SpliceValue
mkEmpty (SType ct@(tnc -> (TCollection, [t]))) = SExpr $ EC.empty t @<- (map EAnnotation $ namedTAnnotations $ annotations ct)
mkEmpty _ = error "Invalid container type for constructing an empty collection."

mkEmptyByElem :: Identifier -> SpliceValue -> SpliceValue
mkEmptyByElem n (SType t) = SExpr $ EC.empty t @+ EAnnotation n
mkEmptyByElem _ _ = error "Invalid element type for constructing an empty collection."

listLabels :: SpliceValue -> SpliceValue
listLabels (SList vs) = maybe err SList $ mapM ltLabel vs
  where err = error "Invalid splice container elements for listLabels"

listLabels _ = error "Invalid splice value container for listLabels"

listTypes :: SpliceValue -> SpliceValue
listTypes (SList vs) = maybe err SList $ mapM ltType vs
  where err = error "Invalid splice container elements for listTypes"

listTypes _ = error "Invalid splice value container for listTypes"

{- Conversions -}
labelExpr :: SpliceValue -> SpliceValue
labelExpr (SExpr (tag -> EConstant (CString i))) = SLabel i
labelExpr (SExpr (tag -> EConstant (CInt i)))    = SLabel $ show $ abs i
labelExpr (SExpr (tag -> EConstant (CReal r)))   = SLabel $ show $ abs r
labelExpr (SExpr (tag -> EConstant (CBool b)))   = SLabel $ show b
labelExpr (SExpr (tag -> EVariable i ))          = SLabel i
labelExpr x = error $ "Invalid splice expression for labelExpr: " ++ show x

labelLiteral :: SpliceValue -> SpliceValue
labelLiteral (SLiteral (tag -> LString i)) = SLabel i
labelLiteral (SLiteral (tag -> LInt i))    = SLabel $ show $ abs i
labelLiteral (SLiteral (tag -> LBool b))   = SLabel $ show b
labelLiteral _ = error $ "Invalid splice literal for labelLiteral"

literalLabel :: SpliceValue -> SpliceValue
literalLabel (SLabel i) = SLiteral $ LC.string i
literalLabel _ = error "Invalid splice label for literalLabel"

literalExpr :: SpliceValue -> SpliceValue
literalExpr (SExpr (tag -> EConstant (CString i))) = SLiteral $ LC.string i
literalExpr (SExpr (tag -> EConstant (CInt i)))    = SLiteral $ LC.int i
literalExpr (SExpr (tag -> EConstant (CReal r)))   = SLiteral $ LC.real r
literalExpr (SExpr (tag -> EConstant (CBool b)))   = SLiteral $ LC.bool b
literalExpr (SExpr e) = SLiteral $ LC.string $ show e
literalExpr _ = error "Invalid splice expression for labelExpr"

literalType :: SpliceValue -> SpliceValue
literalType (SType t) = SLiteral . LC.string $ show t
literalType _ = error "Invalid splice label for literalType"

exprLabel :: SpliceValue -> SpliceValue
exprLabel (SLabel i) = SExpr $ EC.constant $ CString i
exprLabel _ = error "Invalid splice label for exprLabel"

exprType :: SpliceValue -> SpliceValue
exprType (SType t) = SExpr  $ EC.constant $ CString $ show t
exprType _ =  error "Invalid splice type for exprType"


{- Compile-time arithmetic -}
incrLiteral :: SpliceValue -> SpliceValue
incrLiteral (SLiteral (tag -> LInt i)) = SLiteral $ LC.int $ i+1
incrLiteral (SLiteral (tag -> LReal i)) = SLiteral $ LC.real $ i + 1.0
incrLiteral _ = error "Invalid splice value for incrLiteral"

incrExpr :: SpliceValue -> SpliceValue
incrExpr (SExpr (tag -> EConstant (CInt i)))  = SExpr $ EC.constant $ CInt  $ i+1
incrExpr (SExpr (tag -> EConstant (CReal i))) = SExpr $ EC.constant $ CReal $ i + 1.0
incrExpr _ = error "Invalid splice value for incrExpr"

decrExpr :: SpliceValue -> SpliceValue
decrExpr (SExpr (tag -> EConstant (CInt i)))  = SExpr $ EC.constant $ CInt  $ i-1
decrExpr (SExpr (tag -> EConstant (CReal i))) = SExpr $ EC.constant $ CReal $ i - 1.0
decrExpr _ = error "Invalid splice value for decrExpr"

{- Map specialization helpers. -}
specializeMapTypeByKV :: SpliceValue -> SpliceValue
specializeMapTypeByKV (SType t@(tnc -> (TRecord ["key", "value"], [(tag -> kt), _]))) =
  case kt of
    TInt    -> SType $ (TC.collection t) @+ TAnnotation "IntMap"
    TString -> SType $ (TC.collection t) @+ TAnnotation "StrMap"
    _       -> SType $ (TC.collection t) @+ TAnnotation "Map"

specializeMapTypeByKV sv = error $ "Invalid key-value record element for specializeMapTypeByKV: " ++ show sv

specializeMapEmptyByKV :: SpliceValue -> SpliceValue
specializeMapEmptyByKV (SType t@(tnc -> (TRecord ["key", "value"], [(tag -> kt), _]))) =
  case kt of
    TInt    -> SExpr $ (EC.empty t) @+ EAnnotation "IntMap"
    TString -> SExpr $ (EC.empty t) @+ EAnnotation "StrMap"
    _       -> SExpr $ (EC.empty t) @+ EAnnotation "Map"

specializeMapEmptyByKV sv = error $ "Invalid key-value record element for specializeMapEmptyByKV " ++ show sv

{- Index extractor spec contruction helpers. -}
mkIndexExtractor :: SpliceValue -> SpliceValue -> SpliceValue
mkIndexExtractor (SList keyLT) (SList specLL) =
  if length matches /= length keyLT
    then error "Invalid index extractor specifiers"
    else SLiteral $ LC.string $ intercalate ";" $ map extractor matches

  where extractor (SLabel k, _, SLiteral pathlit) =
          case tag pathlit of
            LString path -> k ++ "=" ++ path
            _ -> error "Invalid extractor field path"

        extractor _ = error "Invalid extractor spec"

        matches = join (map extractLT keyLT) (map extractLL specLL)

        join lt ll = catMaybes $ map (\(lbl, t) -> lookup lbl ll >>= \lit -> return (lbl, t, lit)) lt

        extractLL (SRecord fields) =
          case (Map.lookup spliceVIdSym fields, Map.lookup spliceVLSym fields) of
            (Just lbl, Just lit) -> (lbl, lit)
            (_, _) -> error "Invalid index extractor"

        extractLT (SRecord fields) =
          case (Map.lookup spliceVIdSym fields, Map.lookup spliceVTSym fields) of
            (Just lbl, Just typ) -> (lbl, typ)
            (_, _) -> error "Invalid index key"

mkIndexExtractor _ _ = error "Invalid index extraction arguments"

{- Distributed join constructors -}

equijoinMapType :: SpliceValue -> SpliceValue -> SpliceValue
equijoinMapType (SType (tnc -> (TFunction, [_, kt]))) (SType vt) = SType $ (TC.collection $ TC.record [("key", kt), ("value", vt)]) @+ TAnnotation "Map"
equijoinMapType _ _ = error "Invalid key/value types for equijoin map type"

equijoinEmptyMap :: SpliceValue -> SpliceValue -> SpliceValue
equijoinEmptyMap (SType (tnc -> (TFunction, [_, kt]))) (SType vt) = SExpr $ (EC.empty $ TC.record [("key", kt), ("value", vt)]) @+ EAnnotation "Map"
equijoinEmptyMap _ _ = error "Invalid key/value types for equijoin empty map"

broadcastJoinLHSVar :: SpliceValue -> SpliceValue -> SpliceValue
broadcastJoinLHSVar (SLabel lbl) (SExpr e) = case tag e of
    EVariable _ -> SExpr e
    _ -> SExpr $ EC.variable $ lbl ++ "_lhs"

broadcastJoinLHSVar _ _ = error "Invalid broadcast join lhsvar arguments"

broadcastjoinMaterialize :: SpliceValue -> SpliceValue -> SpliceValue
broadcastjoinMaterialize (SLabel lbl) (SExpr e) = case tag e of
    EVariable _ -> SExpr $ EC.unit
    _ -> SExpr $ EC.assign (lbl ++ "_lhs") e

broadcastjoinMaterialize _ _ = error "Invalid broadcast join materialization arguments"


{- Column-store helpers -}
columnJoinUpperBound :: Int
columnJoinUpperBound = 14

joinRange :: SpliceValue -> SpliceValue
joinRange (SLiteral (tag -> LInt i)) = SList $ map (SLiteral . LC.int) [1..(columnJoinUpperBound - i)]
joinRange _ = error "Invalid integer literal in joinRange"

{- BulkFlatCollection helpers -}
baseTableBFC :: SpliceValue -> K3 Expression -> SpliceValue
baseTableBFC (SExpr e) var = runIdentity $ mapTree replaceNode e >>= return . SExpr
  where
    replaceNode :: [K3 Expression] -> K3 Expression -> Identity (K3 Expression)
    replaceNode cs n@(annotations -> anns) = return . ((flip replaceCh) cs) $ if any hasProperty anns then var else n

    hasProperty :: Annotation Expression -> Bool
    hasProperty (EProperty (ePropertyName -> name)) = name == "BaseTable"
    hasProperty _ = False
baseTableBFC _ _ = error "Invalid expression in baseTableBFC"

extractPathBFC :: SpliceValue -> SpliceValue
extractPathBFC (SExpr e) = maybe (error "No path found in BaseTable property") (SExpr . EC.variable) (runIdentity $ foldTree nodeFn Nothing e)
  where
    nodeFn :: Maybe (String) -> K3 Expression -> Identity (Maybe String)
    nodeFn acc n@(annotations -> anns) = return $ case (foldl extract Nothing anns) of
                                                    Nothing -> acc
                                                    Just x -> Just x

    extract :: Maybe String -> Annotation Expression -> Maybe String
    extract acc (EProperty ( (ePropertyName &&& ePropertyValue) -> ("BaseTable", Just (tag -> LString s)) )) = Just s
    extract acc _ = acc
extractPathBFC _ = error "Invalid expression in extractPathBFC"

collectionContentType :: SpliceValue -> SpliceValue
collectionContentType (SType (tag &&& children -> (TCollection , (t:_) ))) = SType t
collectionContentType _ = error "Invalid type in collectionContentType"

{- Mosaic helpers -}
partitionConstraint :: SpliceValue -> SpliceValue -> SpliceValue
partitionConstraint (SExpr e1) (SExpr e2) = SLiteral $ LC.string $ show [(e1, e2)]
partitionConstraint _ _ = error "Invalid initPartition argument"

-- TODO: outer join on missing matches
propagatePartition :: SpliceValue -> SpliceValue
propagatePartition (SExpr e) = SExpr $ runIdentity $ do
    (rels,ne) <- biFoldMapRebuildTree mkBindings propagate ([],[]) [] e
    let constraints = unionConstraints $ filter isConstraint $ annotations ne
    let rprop = relationProperty rels constraints
    let jprop = joinOrderProperty rels constraints
    return $ (ne @+ rprop) @+ jprop

  where
    mkColT elem_t = (TC.collection elem_t) @+ TAnnotation "Collection"
    mkColEmpty elem_t = (EC.empty elem_t) @+ EAnnotation "Collection"

    mkMapT elem_t = (TC.collection elem_t) @+ TAnnotation "Map"
    mkMapEmpty elem_t = (EC.empty elem_t) @+ EAnnotation "Map"

    shortNames l = snd $ foldl (\(sacc,lacc) x -> let c = candidates sacc x in (Set.insert (head c) sacc, lacc++[head c])) (Set.empty, []) l
      where candidates acc x = [pfx | pfx <- (map (filter isAlphaNum) $ tail $ inits x), pfx `Set.notMember` acc && any isAlpha pfx]

    accumByMaxRel relWeights (acc, rem) (l,r) =
      maybe (acc, rem++[(l,r)]) (\(maxrel, _) -> (Map.insertWith (++) maxrel [(l,r)] acc, rem)) score

      where score = scoreRelAttr =<< ((,) <$> extractCRelAttr l <*> extractCRelAttr r)
            scoreRelAttr ((lrel, lattr), (rrel, rattr)) =
              argmaxW lrel rrel <$> lookup lrel relWeights <*> lookup rrel relWeights
            argmaxW a b c d = if c >= d then (a, c) else (b, d)

    accumByRel (acc, rem) (l,r) = accumCRelAttr (accumCRelAttr (acc, rem) l) r

    accumCRelAttr (acc,rem) (PRConstraint rel attr) = (Map.insertWith (++) rel [attr] acc, rem)
    accumCRelAttr (acc,rem) x = (acc, rem ++ [x])

    extractCRelAttr (PRConstraint rel attr) = Just (rel, attr)
    extractCRelAttr _ = Nothing

    joinOrderProperty relts (filter (not . isConstantConstraint) -> []) = EProperty (Left ("NoPilot", Nothing))
    joinOrderProperty relts (filter (not . isConstantConstraint) -> cl) =
        if not $ (null remConstraints && null jremConstraints)
          then error "Found unhandled constraints"
          else let codedOrder = show $ SList joinOrderParams
               in EProperty (Left ("JoinOrder", Just $ LC.string $ codedOrder))

      where
        mkName l = intercalate "_" $ shortNames l
        mkAttrs ras = zip ras $ map (\(a,b) -> a ++ "_" ++ b) $ uncurry zip $ first shortNames $ unzip ras

        relWeights = zip (map fst relts) [0..]
        (relSchemas, remConstraints) = foldl accumByRel (Map.fromList $ map ((,[]) . fst) relts, []) cl
        (jconstraintsByRel, jremConstraints) = foldl (accumByMaxRel relWeights) (Map.fromList $ map ((,[]) . fst) relts, []) cl

        joinOrderCA = fst $ foldl accumJoinOrder ([], []) relts
        accumJoinOrder (acc,relpfx) (n,_) =
          (acc++[(n, (jconstraintsByRel Map.! n, relSchemas Map.! n, relpfx))], relpfx++[n])

        joinOrderSchemas = fst $ foldr accumJOSchema ([], []) joinOrderCA
        accumJOSchema (n, (jc, nSchema, nPfx)) (acc, dsSchema) = (elem:acc, nub $ jcSchema ++ dsSchema)
          where jcSchema = maybe [] concat $ mapM (\(l,r) -> ((\a b -> [a,b]) <$> extractCRelAttr l <*> extractCRelAttr r)) jc
                (roSchema, loSchema) = partition ((n ==) . fst) dsSchema
                (riSchema, liSchema) = partition ((n ==) . fst) $ nub $ jcSchema ++ dsSchema
                (rkSchema, lkSchema) = partition ((n ==) . fst) $ jcSchema
                schemas = (mkAttrs liSchema, mkAttrs loSchema, mkAttrs lkSchema,
                           mkAttrs riSchema, mkAttrs roSchema, mkAttrs rkSchema,
                           debugDS dsSchema $ mkAttrs dsSchema)
                elem = (n, (jc, nPfx, (nSchema, schemas)))

                debugDS x y = if True then y
                              else flip trace y $ boxToString $ ["DS"] ++ [show x] ++ [show y]

        joinOrderParams = debugJoinOrderSchemas $ if length joinOrderSchemas <= 1
                            then []
                            else (\(a,_,_) -> a) $ foldl accJOParams (initParam $ head joinOrderSchemas) $ tail joinOrderSchemas

          where debugJoinOrderSchemas r = if True then r
                                          else flip trace r $ boxToString $ map prettySchemas joinOrderSchemas

        prettySchemas (_, (_, _, (_, (li, lo, lk, ri, ro, rk, ds)))) = boxToString $ ["JOS:"] ++ map show [li, lo, lk, ri, ro, rk, ds]

        origSchema l = map (snd . fst) l
        origRelSchema reln l = map (snd . fst) $ filter (\((r,_),_) -> r == reln) l

        renamedSchema l = map snd l
        renamedRelSchema reln l = map snd $ filter (\((r,_),_) -> r == reln) l

        schemaExprTypes asDerived rel_t sch = do
            idts <- fieldTypes (origSchema sch) rel_t
            return (fields, zip (renamedSchema sch) $ map snd idts)
          where fields = map (mkField asDerived) sch

        schemaRelExprTypes asDerived reln rel_t sch = do
            idts <- fieldTypes (origRelSchema reln sch) rel_t
            return (fields, zip (renamedRelSchema reln sch) $ map snd idts)
          where fields = map (mkField asDerived) sch

        mkField derived ((_, a), da) = let na = if derived then da else a
                                       in (da, EC.project na $ EC.project "key" $ EC.variable "x")

        initParam (reln, (_, relpfx, schemas)) = ([], resultKeySchema, Left resultValSchema)
          where (_, (_, _, _, _, _, _, oSchema)) = schemas
                resultKeySchema = maybe [] id $ do
                  (rel_t, _) <- lookup reln relts
                  (_, sch) <- schemaRelExprTypes False reln rel_t oSchema
                  return sch

                resultValSchema = map (\i -> ("part_"++i, TC.int)) $ relpfx ++ [reln]

        accJOParams (acc, prevKeySchema, prevValSchemaE) (reln, (jc, relpfx, schemas)) =
          (acc ++ [spliceRecord param_list], kSchema, vSchema)
          where
            (nSchema, (liSchema, loSchema, lkSchema, riSchema, roSchema, rkSchema, oSchema)) = schemas
            (param_list, kSchema, vSchema) = maybe ([],[],Right []) id $ do
                (rel_t, _) <- lookup reln relts

                let pn = mkName $ relpfx
                let sn = mkName $ relpfx ++ [reln]

                let ht_lbl = reln ++ "_ht"
                let rv_lbl = "part_"++reln

                lquery_v <- case relpfx of
                              []  -> Nothing
                              [_] -> Just $ EC.variable $ reln ++ "_pidx"
                              _   -> Just $ EC.variable $ "out_" ++ pn

                let rquery_v = EC.variable $ reln ++ "_pidx"


                lvAsDerived <- case relpfx of
                                 []  -> Nothing
                                 [_] -> Just False
                                 _   -> Just True

                rjoin_fields <- fieldTypes (origSchema rkSchema) rel_t
                let join_key_t = map (\(x,(_,t)) -> let n = "k" ++ show x in (n, t)) $ zip [0..] rjoin_fields

                let mkJoinFields derived l = map (\(i, (n,t), x) -> ("k" ++ show i,) $ snd $ mkField derived x)
                                                     $ zip3 [0..] join_key_t l

                let lvfields = map (mkField lvAsDerived) liSchema
                let lvfields_t = prevKeySchema

                (rvfields, rvfields_t) <- schemaExprTypes False rel_t riSchema

                let lval_t = either TC.record TC.record prevValSchemaE
                let ljoin_val_t = TC.record [("key", TC.record lvfields_t), ("value", mkColT lval_t)]
                let rjoin_val_t = TC.record [("key", TC.record rvfields_t), ("value", mkColT $ TC.record [(rv_lbl, TC.int)])]

                let lhs_ht_elem_t = TC.record [("key", TC.record join_key_t), ("value", mkMapT $ ljoin_val_t)]
                let rhs_ht_elem_t = TC.record [("key", TC.record join_key_t), ("value", mkMapT $ rjoin_val_t)]

                let lgbfields = mkJoinFields lvAsDerived lkSchema
                let rgbfields = mkJoinFields False rkSchema

                let lz_e   = mkMapEmpty ljoin_val_t
                let lgbf_e = EC.lambda "x" $ EC.record lgbfields
                let lnst_e = mkColEmpty lval_t
                let luke_e = EC.record [("key", EC.record lvfields), ("value", lnst_e)]

                let slval l = case l of
                                     [(n,_)] -> EC.record [(n, EC.project "value" $ EC.variable "x")]
                                     _ -> error "Invalid singleton identifier for join base"

                let lval_e = either slval (const $ error "Invalid lval_e") prevValSchemaE

                let lnew_base_e = EC.lambda "_" $ EC.letIn "z" lnst_e $
                                    EC.binop OSeq (EC.applyMany (EC.project "insert" $ EC.variable "z") [lval_e])
                                                  (EC.record [("key", EC.record lvfields), ("value", EC.variable "z")])

                let lnew_derv_e = EC.lambda "_" $ (EC.record [("key", EC.record lvfields), ("value", EC.project "value" $ EC.variable "x")])

                let lupd_base_e = EC.lambda "y" $ EC.binop OSeq (EC.applyMany (EC.project "insert" $ EC.project "value" $ EC.variable "y") [lval_e])
                                                                (EC.variable "y")

                let lupd_derv_e = EC.lambda "y" $ EC.binop OSeq (EC.applyMany (EC.project "extend" $ EC.project "value" $ EC.variable "y")
                                                                   [EC.project "value" $ EC.variable "x"])
                                                                (EC.variable "y")

                let lacc_e = EC.lambda "acc" $ EC.lambda "x" $
                               EC.binop OSeq (EC.applyMany (EC.project "upsert_with" $ EC.variable "acc")
                                                [luke_e
                                                 , either (const lnew_base_e) (const lnew_derv_e) prevValSchemaE
                                                 , either (const lupd_base_e) (const lupd_derv_e) prevValSchemaE])
                                             (EC.variable "acc")

                let rz_e   = mkMapEmpty rjoin_val_t
                let rgbf_e = EC.lambda "x" $ EC.record rgbfields
                let rnst_e = mkColEmpty $ TC.record [(rv_lbl, TC.int)]
                let ruke_e = EC.record [("key", EC.record rvfields), ("value", rnst_e)]

                let rval_e = EC.record [(rv_lbl, EC.project "value" $ EC.variable "x")]

                let rnew_e = EC.lambda "_" $ EC.letIn "z" rnst_e
                                (EC.binop OSeq (EC.applyMany (EC.project "insert" $ EC.variable "z") [rval_e])
                                               (EC.record [("key", EC.record rvfields), ("value", EC.variable "z")]))

                let rupd_e = EC.lambda "y" $ EC.binop OSeq (EC.applyMany (EC.project "insert" $ EC.project "value" $ EC.variable "y") [rval_e])
                                                           (EC.variable "y")

                let racc_e = EC.lambda "acc" $ EC.lambda "x" $
                               EC.binop OSeq (EC.applyMany (EC.project "upsert_with" $ EC.variable "acc") [ruke_e, rnew_e, rupd_e])
                                             (EC.variable "acc")

                let lquery_e = EC.applyMany (EC.project "group_by" lquery_v) [lgbf_e, lacc_e, lz_e]
                let rquery_e = EC.applyMany (EC.project "group_by" rquery_v) [rgbf_e, racc_e, rz_e]

                -- Join result
                let (orfields, olfields) = partition ((== reln) . fst . fst . snd) $ zip [0..] oSchema

                olfields_it <- mapM (\(i, ((r, a), da)) -> maybe Nothing (\t -> Just (i, (r, a, da, t))) $ lookup da prevKeySchema) olfields

                orfields_t <- fieldTypes (origSchema $ map snd orfields) rel_t
                let orfields_it = map (\((i,((r, a), da)), (n,t)) -> (i, (r, a, da, t))) $ zip orfields orfields_t

                let result_id = "out_" ++ sn
                let resultKeySchema = case olfields_it ++ orfields_it of
                                        [] -> [("ra", TC.unit)]
                                        l -> map (\(_, (_, _, da, t)) -> (da, t)) $ sortBy (compare `on` fst) l
                let resultValSchema = map (\i -> ("part_"++i, TC.int)) $ relpfx ++ [reln]

                let result_elem_key_t = TC.record resultKeySchema
                let result_elem_val_t = TC.record resultValSchema
                let result_elem_t = TC.record [("key", result_elem_key_t), ("value", mkColT result_elem_val_t)]
                let result_t = mkMapT result_elem_t

                -- Join merge
                let lhs_nilambda_e = EC.lambda "old2" $ EC.lambda "new2" $
                                       EC.binop OSeq (EC.applyMany (EC.project "extend" $ EC.project "value" $ EC.variable "old2")
                                                                   [EC.project "value" $ EC.variable "new2"])
                                                     (EC.variable "old2")

                let lhs_nflambda_e = EC.lambda "acc" $ EC.lambda "y" $
                                       EC.binop OSeq (EC.applyMany (EC.project "insert_with" $ EC.variable "acc")
                                                                   [EC.variable "y", lhs_nilambda_e])
                                                     (EC.variable "acc")

                let lhs_merge_e = EC.applyMany (EC.project "fold" $ EC.project "value" $ EC.variable "new")
                                       [lhs_nflambda_e, EC.project "value" $ EC.variable "old"]

                let lhs_insert_e = EC.lambda "old" $ EC.lambda "new" $ EC.record
                                     [("key", EC.project "key" $ EC.variable "old"), ("value", lhs_merge_e)]


                -- Join probe
                let rkeys_e = case oSchema of
                                [] -> EC.record [("ra", EC.unit)]
                                l -> EC.record $ map (\((r, _), da) -> (da, EC.project da $ EC.project "key" $ EC.variable $ if r == reln then "x" else "y")) l

                let rvals_e = EC.record $ (map (\i -> ("part_"++i, EC.project ("part_" ++ i) $ EC.variable "lcp")) relpfx)
                                            ++ [("part_" ++ reln, EC.project ("part_" ++ reln) $ EC.variable "rcp")]

                let lhs_pmlam_e  = EC.lambda "_" $ EC.unit -- TODO missing case for outer join

                let lhs_rcplam_e = EC.lambda "cpacc" $ EC.lambda "rcp" $
                                      EC.binop OSeq (EC.applyMany (EC.project "insert" $ EC.variable "cpacc") [rvals_e])
                                                    (EC.variable "cpacc")

                let lhs_lcplam_e = EC.lambda "cpacc" $ EC.lambda "lcp" $
                                      EC.applyMany (EC.project "fold" $ EC.project "value" $ EC.variable "x") [lhs_rcplam_e, EC.variable "cpacc"]

                let lhs_cpz_e = mkColEmpty result_elem_val_t

                let lhs_cpnval_e = EC.applyMany (EC.project "fold" $ EC.project "value" $ EC.variable "y") [lhs_lcplam_e, lhs_cpz_e]
                let lhs_cpnlam_e = EC.lambda "y"  $
                                      EC.applyMany (EC.project "insert" $ EC.variable result_id)
                                        [EC.record
                                          [("key", rkeys_e),
                                           ("value", lhs_cpnval_e)]]

                let lhs_cplam_e  = EC.lambda "x"  $ EC.applyMany (EC.project "iterate" $ EC.project "value" $ EC.variable "lv") [lhs_cpnlam_e]
                let lhs_pplam_e  = EC.lambda "lv" $ EC.applyMany (EC.project "iterate" $ EC.project "value" $ EC.variable "rv") [lhs_cplam_e]
                let lhs_prkey_e  = EC.record [("key", EC.project "key" $ EC.variable "rv"), ("value", mkMapEmpty $ ljoin_val_t)]
                let lhs_probe_e  = EC.lambda "rv" $ EC.applyMany (EC.project "lookup" $ EC.variable ht_lbl) [lhs_prkey_e, lhs_pmlam_e, lhs_pplam_e]

                return ([("i", SLabel reln)]
                          ++ [("lhs_ht_id",        SLabel   ht_lbl)]
                          ++ [("lhs_query",        SExpr    lquery_e)]
                          ++ [("rhs_query",        SExpr    rquery_e)]
                          ++ [("lhs_ht_ty",        SType    lhs_ht_elem_t)]
                          ++ [("rhs_ht_ty",        SType    rhs_ht_elem_t)]
                          ++ [("lhs_insert_with",  SExpr    lhs_insert_e)]
                          ++ [("lhs_probe",        SExpr    lhs_probe_e)]
                          ++ [("result_id",        SLabel   result_id)]
                          ++ [("result_ty",        SType    result_t)]
                          ++ [("lhs_query_clear",  SExpr    EC.unit)]
                          ++ [("rhs_query_clear",  SExpr    EC.unit)]
                          ++ [("lhs_ht_clear",     SExpr    EC.unit)],
                        resultKeySchema, Right resultValSchema)


    relationProperty relts cl =
        if not $ null remConstraints
          then error "Found unhandled constraints"
          else let codedRels = show $ SList $ Map.foldlWithKey (relationRecord relts) [] relSchemas
               in EProperty (Left ("Relations", Just $ LC.string codedRels))

      where
        (relSchemas, remConstraints) = foldl accumByRel (Map.empty, []) cl

        -- TODO: avoid wrapping singleton keys in records.
        relationRecord _ acc "__collection" _ = acc
        relationRecord relts acc rel attrs = acc ++ [spliceRecord param_list]
          where
            param_list = maybe [] id $ do
              (rel_t, rel_eopt) <- lookup rel relts
              base_rel_t <- relType rel_t
              key_t <- TC.record <$> fieldTypes attrs rel_t
              val_t <- relElemType rel_t

              let elem_t = TC.record [("key", key_t), ("value", base_rel_t)]

              let gbf_e  = EC.lambda "x" $ EC.record $ map (\i -> (i, EC.project i $ EC.variable "x")) attrs
              let acc_e  = case rel_eopt of
                             Nothing ->
                               EC.lambda "acc" $ EC.lambda "x" $
                                 EC.binop OSeq (EC.applyMany (EC.project "insert" $ EC.variable "acc") [EC.variable "x"])
                                               (EC.variable "acc")
                             Just e -> e

              let query_e = EC.applyMany (EC.project "group_by" $ EC.variable rel) [gbf_e, acc_e, mkColEmpty val_t]

              let q_param = [("query", SExpr query_e)]
              let c_param = [("clear", SExpr EC.unit)]

              return $ [("i", SLabel rel)]
                        ++ [("key_type", SType key_t)]
                        ++ [("val_type", SType val_t)]
                        ++ [("elem_type", SType elem_t)]
                        ++ q_param
                        ++ c_param

    relType (PTCollection (PTKVRecord _ base_rel_ty _) _) = Just base_rel_ty
    relType brt@(PTCollection _ _) = Just brt
    relType _ = error "Invalid relation collection type"

    relElemType (PTCollection (PTKVRecord _ (PTCollection idx_elem_ty _) _) _) = Just idx_elem_ty
    relElemType (PTCollection base_elem_ty _) = Just base_elem_ty
    relElemType _ = error "Invalid relation collection type"

    fieldTypes attrs (PTCollection (PTKVRecord _ (PTCollection (PTRecord ids fields _) _) _) _) =
      mapM (\i -> (i,) <$> (lookup i $ zip ids fields)) attrs

    fieldTypes attrs (PTCollection (PTRecord ids fields _) _) =
      mapM (\i -> (i,) <$> (lookup i $ zip ids fields)) attrs

    fieldTypes attrs _ = error $ "Invalid relation collection type"

    mkBindings bnds@(rels,vars) (PFRelation (n,_) ijs) = return (bnds, [nbnds, bnds])
      where nbnds = (rels++[n], vars ++ map (\(i,j) -> (j, (n, i))) ijs)

    mkBindings bnds@(rels,vars) (PGRelation (n,_) ijs) = return (bnds, [nbnds, bnds])
      where nbnds = (rels++[n], vars ++ map (\(i,j) -> (j, (n, i))) ijs)

    mkBindings bnds n = return (bnds, flip replicate bnds $ length $ children n)

    propagate bnds rels ch n =
      return . (nrels,) $ if not $ null chpp
        then debugConstraint $ constraintProp chpp $ replaceCh n nch
        else replaceCh n ch
      where
        (chpp, nch) = first concat $ unzip $ map (rebuildCh bnds) ch

        nrels = (++ concat rels) $ case n of
                  PFRelation nte ijs -> [nte]
                  PGRelation nte ijs -> [nte]
                  _ -> []

        debugConstraint r =
          if True then r
          else flip trace r $ boxToString $ ["Partition constraint on: "]
                                         %$ (indent 2 $ concatMap (prettyLines . strip) ch)

    rebuildCh bnds c =
      let (pp, rest) = first (map $ translateBindings bnds) $ partition isConstraint $ annotations c
      in (pp, replaceAnnos c (pp++rest))

    translateBindings (rels, vars) p@(EProperty (Left ("PartitionConstraint", Just (tag -> LString s)))) =
      case kl of
        [(k1, k2)] -> EProperty (Left ("PartitionConstraint", Just $ LC.string $ show [(invertBinding k1, invertBinding k2)]))
        _ -> error "Invalid basic partition key constraint"
      where
        kl = (read s :: [(K3 Expression, K3 Expression)])

        invertBinding (tnc -> (ERecord ["key"], [(tag -> EVariable (flip lookup vars -> Just (r,v)))])) =
          EC.record [("key", EC.project v $ EC.variable r)]

        invertBinding (tnc -> (ERecord ["key"], [tnc -> (EProject v, [tag -> EVariable "t"])])) =
          EC.record [("key", EC.project v $ EC.variable $ if null rels then "t" else last rels)]

        invertBinding e = e

    translateBindings _ p = p

    constraintProp pp c = c @+ (EProperty (Left ("PartitionConstraint", Just $ LC.string $ show $ unionConstraints pp)))

    isConstraint (EProperty (ePropertyName -> "PartitionConstraint")) = True
    isConstraint _ = False

    unionConstraints = concatMap rebuildPConstraint
    rebuildPConstraint (EProperty (Left ("PartitionConstraint", Just (tag -> LString s)))) = (read s :: [(K3 Expression, K3 Expression)])
    rebuildPConstraint _ = []

    isConstantConstraint (PRConstraint ln la, PRConstraint rn ra) = ln == "__collection" || rn == "__collection"
    isConstantConstraint _ = False

    strip = stripExprAnnotations cleanExpr cleanType
      where cleanExpr a = not (isEQualified a || isEUserProperty a || isEAnnotation a || isEApplyGen a)
            cleanType a = not (isTAnnotation a || isTUserProperty a)

propagatePartition _ = error "Invalid expr arg for propagatePartition"

relOrIndexId :: K3 Expression -> Maybe (Identifier, (K3 Type, Maybe (K3 Expression)))
relOrIndexId n@(tag &&& (@~ isConstantRelation) -> (ELetIn i, Just _)) = (\t -> (i, (t, Nothing))) <$> relationType n
relOrIndexId n@(tag &&& (@~ isBaseOrMaterializedRelation) -> (EVariable i, Just _)) = (\t -> (i, (t, Nothing))) <$> relationType n

relOrIndexId (PPrjApp3 n@(tag -> EVariable (("_index" `isSuffixOf`) -> True)) "lookup" _ _ _ _ _ _ _) =
  case (n @~ hasRelationName, n @~ hasProjectionAccExpr) of
    ( Just (EProperty (ePropertyValue -> Just (tag -> LString s))), Just (EProperty (ePropertyValue -> Just (tag -> LString s2))) )
      -> (\t -> (s, (t,Just ((read s2) :: K3 Expression)))) <$> relationType n

    ( Just (EProperty (ePropertyValue -> Just (tag -> LString s))), Nothing )
      -> (\t -> (s, (t,Nothing))) <$> relationType n

    _ -> Nothing

relOrIndexId _ = Nothing

hasRelationName :: Annotation Expression -> Bool
hasRelationName (EProperty (ePropertyName -> "RelationName")) = True
hasRelationName _ = False

hasProjectionAccExpr :: Annotation Expression -> Bool
hasProjectionAccExpr (EProperty (ePropertyName -> "ProjectionAccExpr")) = True
hasProjectionAccExpr _ = False

isConstantRelation :: Annotation Expression -> Bool
isConstantRelation (EProperty (ePropertyName -> "ConstantRelation")) = True
isConstantRelation _ = False

isBaseOrMaterializedRelation :: Annotation Expression -> Bool
isBaseOrMaterializedRelation (EProperty (ePropertyName -> n)) = n `elem` ["BaseRelation", "MaterializedRelation"]
isBaseOrMaterializedRelation _ = False

relationType :: K3 Expression -> Maybe (K3 Type)
relationType ((@~ isEType) -> Just (EType t)) = return t
relationType _ = Nothing

pattern PVar     i           iAs   = Node (EVariable i   :@: iAs)   []
pattern PLam     i   bodyE   iAs   = Node (ELambda i     :@: iAs)   [bodyE]
pattern PApp     fE  argE    appAs = Node (EOperate OApp :@: appAs) [fE, argE]
pattern PPrj     cE  fId     fAs   = Node (EProject fId  :@: fAs)   [cE]

pattern PBindAs      srcE bnd bodyE bAs          = Node (EBindAs bnd   :@: bAs) [srcE, bodyE]

pattern PPrjApp2 cE fId fAs fArg1 fArg2 app1As app2As
  = PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As

pattern PPrjApp3 cE fId fAs fArg1 fArg2 fArg3 app1As app2As app3As
  = PApp (PApp (PApp (PPrj cE fId fAs) fArg1 app1As) fArg2 app2As) fArg3 app3As

-- | Mosaic specific patterns
pattern PKRec ke ras = Node (ERecord ["key"] :@: ras) [ke]

pattern PRConstraint rel attr <- PKRec (PPrj (PVar rel _) attr _) _

pattern PFRelation n ijs <-
  PPrjApp2 (relOrIndexId -> Just n) "fold" _ (PLam _ (PLam _ (PBindAs _ (BRecord ijs) _ _) _) _) _ _ _

pattern PGRelation n ijs <-
  PPrjApp3 (relOrIndexId -> Just n) "group_by" _ _ (PLam _ (PLam _ (PBindAs _ (BRecord ijs) _ _) _) _) _ _ _ _

pattern PTRecord ids fields tas = Node (TRecord ids :@: tas) fields

pattern PTKVRecord kt vt tas = Node (TRecord ["key", "value"] :@: tas) [kt, vt]

pattern PTCollection elem tas = Node (TCollection :@: tas) [elem]


mosaicGMRKey :: SpliceValue -> String -> SpliceValue
mosaicGMRKey (SType t@(tag -> (TRecord ids))) s = SExpr $ EC.record $ map (\x -> (x, EC.project x $ EC.variable s)) $ init ids

mosaicGMRMultiplicity :: SpliceValue -> String -> SpliceValue
mosaicGMRMultiplicity (SType t@(tag -> (TRecord ids))) s = SExpr $ EC.project (last ids) (EC.variable s)

mosaicGMRFlatten :: SpliceValue -> String -> SpliceValue
mosaicGMRFlatten (SType t@(tag -> (TRecord ids))) s = SExpr $ EC.record $ map (\x -> (x, EC.project x $ EC.project "key" $ EC.variable s)) (init ids)
                                                                         ++ [(last ids, EC.project "value" $ EC.variable s)]

mosaicExtractRelations :: SpliceValue -> SpliceValue
mosaicExtractRelations sv = maybe notFoundError id $ mosaicTryExtractRelations sv
  where notFoundError = error $ boxToString $ ["Could not extract relations from"] %$ prettyLines sv

mosaicTryExtractRelations :: SpliceValue -> Maybe SpliceValue
mosaicTryExtractRelations (SExpr e) = maybe Nothing (Just . extract) $ e @~ isERelationProp
  where isERelationProp (EProperty (ePropertyName -> n)) = n == "Relations"
        isERelationProp _ = False

        extract (EProperty ( (ePropertyName &&& ePropertyValue) -> ("Relations", Just (tag -> LString s)) )) = (read s) :: SpliceValue
        extract _ = notFoundError

        notFoundError = error $ boxToString $ ["Could not extract relations from"] %$ prettyLines e

mosaicTryExtractRelations sv = error $ boxToString $ ["Invalid expression argument for mosaicExtractRelations"] %$ prettyLines sv

mosaicExtractJoinOrder :: SpliceValue -> SpliceValue
mosaicExtractJoinOrder sv = maybe notFoundError id $ mosaicTryExtractJoinOrder sv
  where notFoundError = error $ boxToString $ ["Could not extract join order from"] %$ prettyLines sv

mosaicTryExtractJoinOrder :: SpliceValue -> Maybe SpliceValue
mosaicTryExtractJoinOrder (SExpr e) = maybe Nothing (Just . extract) $ e @~ isEJoinOrderProp
  where isEJoinOrderProp (EProperty (ePropertyName -> n)) = n == "JoinOrder"
        isEJoinOrderProp _ = False

        extract (EProperty ( (ePropertyName &&& ePropertyValue) -> ("JoinOrder", Just (tag -> LString s)) )) = (read s) :: SpliceValue
        extract _ = notFoundError

        notFoundError = error $ boxToString $ ["Could not extract join order from"] %$ prettyLines e

mosaicTryExtractJoinOrder sv = error $ boxToString $ ["Invalid expression argument for mosaicExtractJoinOrder"] %$ prettyLines sv


mosaicStartMultiExchange :: SpliceValue -> K3 Expression -> SpliceValue
mosaicStartMultiExchange (SList relations) addrE = SExpr $ foldl exchangeRelation EC.unit relations
  where exchangeRelation acc (SRecord rr) = EC.binop OSeq acc (EC.send (exchangeTrigger rr) addrE EC.unit)
        exchangeRelation _ _ = error "Invalid relation record for mosaic"

        exchangeTrigger rr = case rr Map.! "i" of
                               SLabel i -> EC.variable $ i ++ "_exchange"
                               _ -> error "Invalid mosaic relation label"

mosaicStartMultiExchange _ _ = error "Invalid mosaic relations in mosaicStartMultiExchange"

mosaicExchangeBarrierCount :: SpliceValue -> SpliceValue
mosaicExchangeBarrierCount (SList relations) = SExpr $
  EC.binop OMul (EC.applyMany (EC.project "size" $ EC.variable "peers") [EC.unit])
               $ EC.constant $ CInt $ length relations

mosaicExchangeBarrierCount _ = error "Invalid mosaic relations in mosaicExchangeBarrierCount"

mosaicStartMultiwayJoin :: SpliceValue -> K3 Expression -> SpliceValue
mosaicStartMultiwayJoin (SList svs) addrE = case svs of
  [] -> SExpr EC.unit
  (SRecord nsvs):_ -> case Map.lookup "i" nsvs of
                        Just (SLabel lbl) -> SExpr $ EC.send (EC.variable $ lbl ++ "_lhs_redistribute") addrE EC.unit
                        _ -> error "No label found in join order element"
  _ -> error "Invalid join order head element"

mosaicStartMultiwayJoin _ _ = error "Invalid join orders in mosaicStartMultiwayJoin"

mosaicMultiwayJoinResult :: SpliceValue -> SpliceValue
mosaicMultiwayJoinResult (SList svs) =
  if null svs
    then error "Invalid join order list"
    else case last svs of
           SRecord nsvs -> case Map.lookup "result_id" nsvs of
                             Just (SLabel lbl) -> SExpr $ EC.variable lbl
                             _ -> error "Invalid join order result label"
           _ -> error "Invalid join order last element"

mosaicMultiwayJoinResult _ = error "Invalid join orders in mosaicMultiwayJoinResult"

mosaicGlobalNextMultiwayJoin :: SpliceValue -> SpliceValue -> SpliceValue -> String -> SpliceValue
mosaicGlobalNextMultiwayJoin (SLabel lbl) (SList svs) (SRecord current) masterAddrVar =
  if null svs
    then error "Invalid join order list"
    else case pickNext of
           (True, _) -> SExpr $ EC.send (EC.variable $ lbl ++ "_mjoin_done") (EC.variable masterAddrVar) EC.unit
           (False, Just rsv) -> rsv

  where pickNext = foldl (pickNextAcc $ maybe nameError labelStr $ Map.lookup "i" current) (False, Nothing) svs
        pickNextAcc _ (True, _) (SRecord sv) = (False, maybe nameError (Just . nextJoin . labelStr) $ Map.lookup "i" sv)
        pickNextAcc _ (_, Just rsv) (SRecord sv) = (False, Just rsv)
        pickNextAcc cn _ (SRecord sv) = maybe nameError (matchLabel cn) $ Map.lookup "i" sv
        pickNextAcc _ _ _ = error "Invalid join order element in mosaicGlobalNextMultiwayJoin"

        nextJoin i = SExpr $ EC.applyMany (EC.project "iterate" $ EC.variable "peers")
                               [EC.lambda "p" $ EC.send (EC.variable $ i ++ "_lhs_redistribute") (EC.project "addr" $ EC.variable "p") EC.unit]
        matchLabel cn sv = (labelStr sv == cn, Nothing)

        labelStr (SLabel i) = i
        labelStr _ = nameError

        nameError = error "Invalid join order name in mosaicGlobalNextMultiwayJoin"

mosaicGlobalNextMultiwayJoin _ _ _ _ = error "Invalid arguments to mosaicGlobalNextMultiwayJoin"

mosaicFetchPartitionBarrierKeyType :: SpliceValue -> SpliceValue
mosaicFetchPartitionBarrierKeyType (SList relsvs) = SType $ TC.record $ map relationPartitionKeyElem relsvs
  where relationPartitionKeyElem (SRecord nsvs) = case Map.lookup "i" nsvs of
                                                    Just (SLabel i) -> ("part_" ++ i, TC.int)
                                                    _ -> error "Invalid relation label in mosaicFetchPartitionBarrierKeyType"

        relationPartitionKeyElem _ = error "Invalid relation param record in mosaicFetchPartitionBarrierKeyType"

mosaicFetchPartitionBarrierKeyType _ = error "Invalid relations in mosaicFetchPartitionBarrierKeyType"

mosaicFetchPartitionBarrierSize :: SpliceValue -> SpliceValue
mosaicFetchPartitionBarrierSize (SList relsvs) = SExpr $ EC.constant $ CInt $ length relsvs
mosaicFetchPartitionBarrierSize _ = error "Invalid relations in mosaicFetchPartitionBarrierSize"


mosaicFetchPartition :: SpliceValue -> String -> SpliceValue
mosaicFetchPartition (SList relsvs) keyVar = SExpr $ foldl fetchAcc EC.unit relsvs
  where fetchAcc accE (SRecord nsvs) =
          case Map.lookup "i" nsvs of
            Just (SLabel i) -> EC.binop OSeq
                                (EC.letIn ("pi_" ++ i) (partId i) $
                                 EC.send
                                  (EC.variable $ i ++ "_fetch_part")
                                  (EC.project "addr" $ partAddr i)
                                  (EC.record [ ("part_id",     EC.variable $ "pi_"++i)
                                             , ("dest",        EC.variable "me")
                                             , ("barrier_key", EC.variable keyVar)]))
                                accE
            _ -> nameError
        fetchAcc _ _ = error "Invalid relation param record in mosaicFetchPartition"

        partId i = EC.project ("part_" ++ i) $ EC.variable keyVar
        partAddr i = EC.applyMany (EC.project "at" $ EC.variable "peers")
                       [EC.binop OMod (EC.variable $ "pi_" ++ i) (EC.variable $ i ++ "_stride")]

        nameError = error "Invalid relation name in mosaicFetchPartition"

mosaicFetchPartition _ _ = error "Invalid relations in mosaicFetchPartition"


mosaicAssignInputPartitions :: SpliceValue -> String -> String -> String -> SpliceValue
mosaicAssignInputPartitions (SList relsvs) partIdVar partMapSuffix currentPartSuffix =
    SExpr $ foldl (\accE rsv -> EC.binop OSeq (assign rsv) accE) EC.unit relsvs

  where assign (SRecord nsvs) = case (Map.lookup "i" nsvs, Map.lookup "val_type" nsvs) of
                                  (Just (SLabel i), Just (SType ty))
                                    -> EC.applyMany (EC.project "lookup" $ EC.variable $ i ++ partMapSuffix)
                                         [ EC.record [ ("key", EC.project ("part_" ++ i) $ EC.variable partIdVar)
                                                     , ("value", (EC.constant $ CEmpty ty) @+ EAnnotation "Collection" )]
                                         , EC.lambda "_" EC.unit
                                         , EC.lambda "part" $ EC.assign (i ++ currentPartSuffix) $ EC.project "value" $ EC.variable "part" ]

                                  _ -> error "Invalid relation name param in mosaicAssignInputPartitions"
        assign _ = error "Invalid relation param record in mosaicAssignInputPartitions"

mosaicAssignInputPartitions _ _ _ _ = error "Invalid expression for mosaicAssignInputPartitions"


mosaicAccumulatePartition :: SpliceValue -> SpliceValue -> SpliceValue -> SpliceValue
mosaicAccumulatePartition (SLabel v) (SExpr e) (SType ty) =
  case tnc ty of
    (TCollection, [telem@(tnc -> (TRecord ids, tch))])
      -> case (ty @~ isTAnnotation, ty @~ isMultiIndexVMap) of
           (Just (TAnnotation "Collection"), _) ->
             let vgb_e = EC.applyMany (EC.project "group_by" $ EC.variable v)
                           [ EC.lambda "x" $ EC.record $ map (\i -> (i, EC.project i $ EC.variable "x")) $ init ids
                           , EC.lambda "acc" $ EC.lambda "x" $ EC.binop OAdd (EC.variable "acc") $ EC.project (last ids) $ EC.variable "x"
                           , either error id $ defaultExpression $ last tch]

                 vcolacc_e = EC.lambda "acc" $ EC.lambda "x" $ EC.binop OSeq
                               (EC.applyMany (EC.project "insert" $ EC.variable "acc")
                                 [EC.record $ (map (\i -> (i, EC.project i $ EC.project "key" $ EC.variable "x")) $ init ids)
                                                ++ [(last ids, EC.project "value" $ EC.variable "x")]])
                               (EC.variable "acc")
                 vcolz_e = (EC.constant $ CEmpty telem) @+ EAnnotation "Collection"
                 vcol_e = EC.applyMany (EC.project "fold" vgb_e) [vcolacc_e, vcolz_e]
             in
             SExpr $ EC.binop OSeq
               (EC.applyMany (EC.project "iterate" e)
                 [EC.lambda "x" $ EC.applyMany (EC.project "insert" $ EC.variable v) [EC.variable "x"]])
               (EC.assign v vcol_e)

           (Just (TAnnotation "Map"), _) ->
             SExpr $ EC.applyMany (EC.project "iterate" e)
              [EC.lambda "x" $ EC.applyMany (EC.project "insert_with" $ EC.variable v)
                [ EC.variable "x"
                , EC.lambda "old" $ EC.lambda "new" $
                    EC.record [ ("key", EC.project "key" $ EC.variable "old")
                              , ("value", EC.binop OAdd (EC.project "value" $ EC.variable "old") $ EC.project "value" $ EC.variable "new")] ]]

           (_, Just (TAnnotation "MultiIndexVMap")) ->
             SExpr $ EC.applyMany (EC.project "iterate" e)
              [EC.lambda "x" $ EC.applyMany (EC.project "insert_with" $ EC.variable v)
                [ EC.constant $ CInt 0
                , EC.variable "x"
                , EC.lambda "old" $ EC.lambda "new" $
                    EC.record [ ("key", EC.project "key" $ EC.variable "old")
                              , ("value", EC.binop OAdd (EC.project "value" $ EC.variable "old") $ EC.project "value" $ EC.variable "new")] ]]

           _ -> error $ boxToString $ ["Invalid partition collection in mosaicAccumulatePartition: "] %$ prettyLines ty

    (TInt, [])  -> SExpr $ EC.assign v $ EC.binop OAdd (EC.variable v) e
    (TReal, []) -> SExpr $ EC.assign v $ EC.binop OAdd (EC.variable v) e
    _ -> error "Invalid accumulator type in mosaicAccumulatePartition"

  where isMultiIndexVMap (TAnnotation "MultiIndexVMap") = True
        isMultiIndexVMap _ = False

mosaicAccumulatePartition _ _ _ = error "Invalid arguments for mosaicAccumulatePartition"

mosaicDistributedPlanner :: SpliceValue -> SpliceValue
mosaicDistributedPlanner sv@(SExpr e) =
  case (relsvs, josvs) of
    ([], []) -> SExpr e
    ([_], []) -> SExpr $ e @+ (EApplyGen True "MosaicExecuteSingleton" singletonParams)
    (_, _) -> SExpr $ e @+ (EApplyGen True "MosaicExecuteJoin" joinParams)

  where (SList relsvs) = maybe (SList []) id $ mosaicTryExtractRelations sv
        (SList josvs) = maybe (SList []) id $ mosaicTryExtractJoinOrder sv
        lbl = case tnc e of
                (EOperate OSeq, [tnc -> (EAssign i, [asgne]), snde]) -> i
                _ -> error "Invalid mosaic planner attachment point"

        singletonParams = Map.fromList [ ("lbl", SLabel lbl), ("relations", SList relsvs) ]
        joinParams = Map.fromList [ ("lbl", SLabel lbl), ("relations", SList relsvs), ("joinOrder", SList josvs) ]

mosaicDistributedPlanner sv = error $ boxToString $ ["Invalid mosaicDistributedPlanner argument:"] %$ prettyLines sv

mosaicLogStaging :: SpliceValue -> SpliceValue -> SpliceValue -> SpliceValue
mosaicLogStaging (SLabel lbl) (SLabel op) (SExpr e) = trace (unwords ["Stage", lbl, "op:", op]) $ SExpr e
