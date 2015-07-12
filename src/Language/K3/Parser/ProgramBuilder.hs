{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}

-- | K3 Program constructor
module Language.K3.Parser.ProgramBuilder (
  defaultRoleName,
  processInitsAndRoles,
  endpointMethods,
  bindSource,
  mkRunSourceE,
  mkRunSinkE,
  declareBuiltins,

  resolveFn
) where

import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

-- | Type synonyms, copied from the parser.
type EndpointInfo = (EndpointSpec, Maybe [Identifier], Identifier, Maybe (K3 Expression))

{- Names -}
defaultRoleName :: Identifier
defaultRoleName = "__global"

myId :: Identifier
myId = "me"

peersId :: Identifier
peersId = "peers"

myAddr :: K3 Expression
myAddr = EC.variable myId

chrName :: Identifier -> Identifier
chrName n = n++"HasRead"

crName :: Identifier -> Identifier
crName n = n++"Read"

cmhrName :: Identifier -> Identifier
cmhrName n = n++"MuxHasRead"

cmrName :: Identifier -> Identifier
cmrName n = n++"MuxRead"

chwName :: Identifier -> Identifier
chwName n = n++"HasWrite"

cwName :: Identifier -> Identifier
cwName n = n++"Write"

ciName :: Identifier -> Identifier
ciName n = n++"Init"

csName :: Identifier -> Identifier
csName n = n++"Start"

cpName :: Identifier -> Identifier
cpName n = n++"Process"

cfName :: Identifier -> Identifier
cfName n = n++"Feed"

ccName :: Identifier -> Identifier
ccName n = n++"Controller"

cmcName :: Identifier -> Identifier
cmcName n = n++"MuxCounter"

cfiName :: Identifier -> Identifier
cfiName n = n++"FileIndex"

cfpName :: Identifier -> Identifier
cfpName n = n++"FilePath"

cfmpName :: Identifier -> Identifier
cfmpName n = n++"FilePositions"

cfmbName :: Identifier -> Identifier
cfmbName n = n++"MuxBuffer"

cfmdName :: Identifier -> Identifier
cfmdName n = n++"DefaultElem"

{- Runtime functions -}
openBuiltinFn :: K3 Expression
openBuiltinFn = EC.variable "openBuiltin"

openFileFn :: K3 Expression
openFileFn = EC.variable "openFile"

openSocketFn :: K3 Expression
openSocketFn = EC.variable "openSocket"

closeFn :: K3 Expression
closeFn = EC.variable "close"

resolveFn :: K3 Expression
resolveFn = EC.variable "resolve"

registerSocketDataTriggerFn :: K3 Expression
registerSocketDataTriggerFn = EC.variable "registerSocketDataTrigger"

{- Top-level functions -}
roleId :: Identifier
roleId = "role"

roleVar :: K3 Expression
roleVar = EC.variable roleId

roleElemId :: Identifier
roleElemId = "rolerec"

roleElemVar :: K3 Expression
roleElemVar = EC.variable roleElemId

roleElemLbl :: Identifier
roleElemLbl = "n"

roleFnId :: Identifier
roleFnId = "processRole"

{- Declaration construction -}
builtinGlobal :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3 Declaration
builtinGlobal n t eOpt = (DC.global n t eOpt) @+ (DSpan $ GeneratedSpan "builtin")

builtinTrigger :: Identifier -> K3 Type -> K3 Expression -> K3 Declaration
builtinTrigger n t e = (DC.trigger n t e) @+ (DSpan $ GeneratedSpan "builtin")

{- Type qualification -}
qualifyT :: K3 Type -> K3 Type
qualifyT t = if null $ filter isTQualified $ annotations t then t @+ TImmutable else t

qualifyE :: K3 Expression -> K3 Expression
qualifyE e = if null $ filter isEQualified $ annotations e then e @+ EImmutable else e

{- Desugaring methods -}
-- TODO: replace with Template Haskell

processInitsAndRoles :: K3 Declaration -> [(Identifier, EndpointInfo)] -> K3 Declaration
processInitsAndRoles (Node t c) endpointBQGs = Node t $ c ++ initializerFns
  where
    (sinkEndpoints, sourceEndpoints) = partition matchSink endpointBQGs
    matchSink (_,(_, Nothing, _, _)) = True
    matchSink _ = False

    initializerFns =
        [ builtinGlobal roleFnId (qualifyT unitFnT)
            $ Just . qualifyE $ mkRoleBody sourceEndpoints sinkEndpoints ]

    sinkInitE acc (_,(_, Nothing, _, Just e)) = acc ++ [e]
    sinkInitE acc _ = acc

    -- TODO handle empty sinks or sources
    mkRoleBody sources sinks =
      EC.lambda "_" $ EC.block $
        (foldl sinkInitE [] sinks) ++
        [EC.applyMany (EC.project "iterate" roleVar)
           [EC.lambda roleElemId $ foldl dispatchId EC.unit sources]]

    dispatchId elseE (n,(_,_,y,goE)) = EC.ifThenElse (eqRole y) (runE n goE) elseE

    eqRole n = EC.binop OEqu (EC.project roleElemLbl roleElemVar) (EC.constant $ CString n)

    runE _ (Just goE) = goE
    runE n Nothing    = EC.applyMany (EC.variable $ cpName n) [EC.unit]

    unitFnT = TC.function TC.unit TC.unit


{- Code generation methods-}
-- TODO: replace with Template Haskell

endpointMethods :: Bool -> EndpointSpec -> K3 Expression -> K3 Expression -> Identifier -> K3 Type
                -> (EndpointSpec, Maybe (K3 Expression), [K3 Declaration])
endpointMethods isSource eSpec argE formatE n t =
  if isSource then sourceDecls else sinkDecls
  where
    sourceDecls = (eSpec, Nothing,) $
         sourceExtraDecls
      ++ (map mkMethod $ [mkInit, mkStart, mkFinal] ++ sourceReadDecls)
      ++ [sourceController]

    sinkDecls = (eSpec, Just sinkImpl, map mkMethod [mkInit, mkFinal, sinkHasWrite, sinkWrite])

    -- Endpoint-specific declarations
    sourceReadDecls = case eSpec of
                        FileMuxEP    _ _ _ -> [sourceMuxHasRead, sourceMuxRead]
                        FileMuxseqEP _ _ _ -> [sourceMuxHasRead, sourceMuxRead]
                        _ -> [sourceHasRead, sourceRead]

    sourceExtraDecls = case eSpec of
      FileSeqEP _ _ _ -> [ builtinGlobal (cfiName n) (TC.int    @+ TMutable) Nothing
                         , builtinGlobal (cfpName n) (TC.string @+ TMutable) Nothing ]
      FileMuxEP _ _ _ -> [ builtinGlobal (cfmpName n) muxPosMap Nothing
                         , builtinGlobal (cfmbName n) muxBuffer Nothing
                         , builtinGlobal (cfmdName n) t Nothing
                         , builtinGlobal (cmcName n) (TC.int @+ TMutable) (Just $ EC.constant $ CInt 0) ]
      FileMuxseqEP _ _ _ -> [ builtinGlobal (cfmpName n) muxPosMap Nothing
                            , builtinGlobal (cfmbName n) muxBuffer Nothing
                            , builtinGlobal (cfmdName n) t Nothing
                            , builtinGlobal (cfiName n) muxSeqIdxMap Nothing
                            , builtinGlobal (cfpName n) muxSeqPathMap Nothing
                            , builtinGlobal (cmcName n) (TC.int @+ TMutable) (Just $ EC.constant $ CInt 0) ]
      _ -> []

    mkMethod (m, argT, retT, eOpt) =
      builtinGlobal (n++m) (qualifyT $ TC.function argT retT)
        $ maybe Nothing (Just . qualifyE) eOpt

    mkInit  = ("Init",  TC.unit, TC.unit, Just $ EC.lambda "_" $ initE) 
    mkStart = ("Start", TC.unit, TC.unit, Just $ EC.lambda "_" $ startE) 
    mkFinal = ("Final", TC.unit, TC.unit, Just $ EC.lambda "_" $ closeE) 
    mkCollection fields ctype = (TC.collection $ TC.record $ map (qualifyT <$>) fields) @+ TAnnotation ctype

    muxSeqLabel  = "order"
    muxDataLabel = "value"

    muxPosMap = mkCollection [("key", TC.int), ("value", TC.int)] "SortedMap"
    muxBuffer = mkCollection [("key", TC.int), ("value", t)] "Map"
    muxFullT  = TC.record [("order", TC.int), ("value", t)]

    muxSeqIdxMap  = mkCollection [("key", TC.int), ("value", TC.int)] "Map"
    muxSeqPathMap = mkCollection [("key", TC.int), ("value", TC.string)] "Map"

    sourceController = case eSpec of
      FileSeqEP    _ txt _ -> seqSrcController txt
      FileMuxEP    _ txt _ -> muxSrcController
      FileMuxseqEP _ txt _ -> muxSeqSrcController txt
      _ -> singleSrcController

    sinkImpl =
      EC.lambda "__msg"
        (EC.ifThenElse
          (EC.applyMany (EC.variable $ chwName n) [EC.unit])
          (EC.applyMany (EC.variable $ cwName n) [EC.variable "__msg"])
          (EC.unit))

    singleSrcController = builtinTrigger (ccName n) TC.unit $
      EC.lambda "_" $
        EC.ifThenElse
          (EC.applyMany (EC.variable $ chrName n) [EC.unit])
          (controlE $ EC.applyMany (EC.variable $ cpName n) [EC.unit])
          EC.unit

    {-----------------------------------------
     - File sequence controler and utilities.
     -----------------------------------------}
    seqSrcController txt = builtinTrigger (ccName n) TC.unit $
      EC.lambda "_" $
        EC.ifThenElse
          (EC.applyMany (EC.variable $ chrName n) [EC.unit])
          (controlE $ EC.applyMany (EC.variable $ cpName n) [EC.unit])
          $ EC.block
              [ nextFileIndexE
              , EC.ifThenElse notLastFileIndexE
                  (EC.block [openSeqNextFileE False True openFileFn txt, controlRcrE])
                  EC.unit ]

    nextFileIndexE =
      EC.assign (cfiName n) $ EC.binop OAdd (EC.variable $ cfiName n) (EC.constant $ CInt 1)

    notLastFileIndexE =
      EC.binop OLth (EC.variable $ cfiName n) (EC.applyMany (EC.project "size" argE) [EC.unit])

    openSeqNextFileE withTest withClose openFn txt = openSeqWithTest withTest $
      EC.block $
        [ EC.applyMany (EC.project "at_with" argE) [EC.variable $ cfiName n, assignSeqPathE] ]
        ++ (if withClose then [closeE] else [])
        ++ [ EC.applyMany openFn [EC.variable "me", sourceId n, EC.variable (cfpName n), formatE, EC.constant $ CBool txt, modeE] ]

    assignSeqPathE = EC.lambda "r" $ EC.assign (cfpName n) (EC.project "path" $ EC.variable "r")

    openSeqWithTest withTest openE =
      if withTest then EC.ifThenElse notLastFileIndexE openE EC.unit else openE


    {-----------------------------------------
     - File multiplexer controler and utilities.
     -----------------------------------------}
    muxSrcControllerTrig onFileDoneE = builtinTrigger (ccName n) TC.unit $
      EC.lambda "_" $
        EC.ifThenElse
          (EC.binop OLth (EC.constant $ CInt 0) $
             EC.applyMany (EC.project "size" $ EC.variable $ cfmpName n) [EC.unit])
          (controlE $ EC.applyMany (EC.project "min_with" $ EC.variable $ cfmpName n)
                        [EC.lambda muxid $ doMuxNext onFileDoneE])
          EC.unit

    muxSrcController = muxSrcControllerTrig muxFinishChan
    muxSeqSrcController txt = muxSrcControllerTrig $ muxSeqNextChan openFileFn txt

    muxid        = "muxnext"
    muxvar       = EC.variable muxid
    muxidx       = EC.project "value" muxvar

    muxChanIdE e = EC.binop OConcat (EC.constant $ CString $ n ++ "_")
                                (EC.applyMany (EC.variable "itos") [e])

    globalMuxChanIdE = muxChanIdE $ EC.variable $ cmcName n
    cntrlrMuxChanIdE = muxChanIdE muxidx

    doMuxNext onFileDoneE =
      EC.block [ muxNextFromChan, muxSafeRefreshChan True onFileDoneE muxidx ]

    muxNextFromChan =
      EC.applyMany (EC.project "lookup_with" $ EC.variable $ cfmbName n)
        [ EC.record [("key", muxidx), ("value", EC.variable $ cfmdName n)]
        , EC.lambda "x" $ EC.applyMany (EC.variable $ cfName n) [EC.project "value" $ EC.variable "x"] ]

    muxSafeRefreshChan withErase onFileDoneE muxIdE =
      EC.ifThenElse (EC.applyMany (EC.variable $ cmhrName n) [muxIdE])
                    (muxRefreshChan withErase muxIdE)
                    onFileDoneE

    muxRefreshChan withErase muxIdE =
      EC.applyMany
        (EC.lambda "next" $ EC.block $
          [ EC.applyMany (EC.project "insert" $ EC.variable $ cfmbName n)
              [EC.record [("key", muxIdE), ("value", EC.project muxDataLabel $ EC.variable "next")]]
          , EC.applyMany (EC.project "insert" $ EC.variable $ cfmpName n)
              [EC.record [("key", EC.project muxSeqLabel $ EC.variable "next"), ("value", muxIdE)]]
          ] ++ (if withErase
                then [EC.applyMany (EC.project "erase" $ EC.variable $ cfmpName n) [muxvar]]
                else []))
        [EC.applyMany (EC.variable $ cmrName n) [muxIdE]]

    muxFinishChan = EC.block
      [ EC.applyMany (EC.project "erase" $ EC.variable $ cfmbName n)
          [EC.record [("key", muxidx), ("value", EC.variable $ cfmdName n)]]
      , EC.applyMany (EC.project "erase" $ EC.variable $ cfmpName n) [muxvar]]

    muxSeqNextChan openFn txt =
      EC.applyMany (EC.project "at_with" $ argE)
        [ muxidx
        , EC.lambda "seqc" $
            EC.applyMany (EC.project "lookup_with" $ EC.variable $ cfiName n)
              [ muxSeqIdx $ EC.constant $ CInt 0
              , EC.lambda "seqidx" $
                  EC.ifThenElse (muxSeqNotLastFileIndexE "seqc" "seqidx")
                    (EC.block [muxSeqNextFileE openFn "seqc" "seqidx" txt])
                    (EC.block [muxFinishChan, muxSeqFinishChan]) ]]

    muxSeqNextFileE openFn seqvar idxvar txt =
      EC.letIn "nextidx"
        (EC.binop OAdd (EC.project "value" $ EC.variable idxvar) $ EC.constant $ CInt 1)
        (EC.applyMany (EC.project "at_with" $ EC.project "seq" $ EC.variable seqvar)
          [ EC.variable "nextidx"
          , EC.lambda "f" $ EC.block
              [ EC.applyMany closeFn [EC.variable "me", cntrlrMuxChanIdE]
              , EC.applyMany (EC.project "insert" $ EC.variable $ cfiName n) [muxSeqIdx $ EC.variable "nextidx"]
              , EC.applyMany (EC.project "insert" $ EC.variable $ cfpName n) [muxSeqIdx $ EC.project "path" $ EC.variable "f"]
              , EC.applyMany openFn [EC.variable "me", cntrlrMuxChanIdE, EC.project "path" $ EC.variable "f", formatE, EC.constant $ CBool txt, modeE]
              , muxSafeRefreshChan True EC.unit muxidx]])

    muxSeqNotLastFileIndexE seqvar idxvar =
      EC.binop OLth (EC.project "value" $ EC.variable idxvar) $
        EC.binop OSub (EC.applyMany (EC.project "size" $ EC.project "seq" $ EC.variable seqvar) [EC.unit])
                      (EC.constant $ CInt 1)

    muxSeqFinishChan = EC.block
      [ EC.applyMany (EC.project "erase" $ EC.variable $ cfiName n)
          [EC.record [("key", muxidx), ("value", EC.constant $ CInt 0)]]
      , EC.applyMany (EC.project "erase" $ EC.variable $ cfpName n)
          [EC.record [("key", muxidx), ("value", EC.constant $ CString "")]]]

    muxSeqIdx e = EC.record [("key", muxidx), ("value", e)]


    -- External functions
    cleanT = stripTUIDSpan $ case eSpec of
               FileMuxEP    _ _ _ -> muxFullT
               FileMuxseqEP _ _ _ -> muxFullT
               _ -> t

    sourceHasRead = ("HasRead", TC.unit, TC.bool, Nothing)
    sourceRead    = ("Read",    TC.unit, cleanT,  Nothing)

    sourceMuxHasRead = ("MuxHasRead", TC.int, TC.bool, Nothing)
    sourceMuxRead    = ("MuxRead",    TC.int, cleanT,  Nothing)

    sinkHasWrite = ("HasWrite",   TC.unit, TC.bool, Nothing)
    sinkWrite    = ("Write",      cleanT,  TC.unit, Nothing)

    initE = case eSpec of
      BuiltinEP    _ _ -> EC.applyMany openBuiltinFn [sourceId n, argE, formatE]
      FileEP       _ txt _ -> openFnE openFileFn txt
      FileSeqEP    _ txt _ -> openFileSeqFnE openFileFn txt
      FileMuxEP    _ txt _ -> openFileMuxChanFnE openFileFn txt
      FileMuxseqEP _ txt _ -> openFileMuxSeqChanFnE openFileFn txt
      NetworkEP    _ txt _ -> openFnE openSocketFn txt
      _                -> error "Invalid endpoint argument"

    openFnE openFn txt = EC.applyMany openFn [EC.variable "me", sourceId n, argE, formatE, EC.constant $ CBool txt, modeE]

    openFileSeqFnE openFn txt =
      EC.block [ EC.assign (cfiName n) (EC.constant $ CInt 0), openSeqNextFileE True False openFn txt]

    openMuxSeqIdx e = EC.record [("key", EC.variable $ cmcName n), ("value", e)]

    openFileMuxChanFnE openFn txt =
      EC.applyMany (EC.project "iterate" argE)
        [EC.lambda "f" $ EC.block
          [ EC.applyMany openFn [EC.variable "me", globalMuxChanIdE, EC.project "path" $ EC.variable "f", formatE, EC.constant $ CBool txt, modeE]
          , muxSafeRefreshChan False EC.unit $ EC.variable $ cmcName n
          , EC.assign (cmcName n) $ EC.binop OAdd (EC.variable $ cmcName n) (EC.constant $ CInt 1) ]]

    openFileMuxSeqChanFnE openFn txt =
      EC.applyMany (EC.project "iterate" argE)
        [ EC.lambda "seqc" $
          EC.applyMany (EC.project "at_with" $ EC.project "seq" $ EC.variable "seqc")
          [ EC.constant $ CInt 0
          , EC.lambda "f" $ EC.block
            [ EC.applyMany (EC.project "insert" $ EC.variable $ cfiName n) [openMuxSeqIdx $ EC.constant $ CInt 0]
            , EC.applyMany (EC.project "insert" $ EC.variable $ cfpName n) [openMuxSeqIdx $ EC.project "path" $ EC.variable "f"]
            , EC.applyMany openFn [EC.variable "me", globalMuxChanIdE, EC.project "path" $ EC.variable "f", formatE, EC.constant $ CBool txt, modeE]
            , muxSafeRefreshChan False EC.unit $ EC.variable $ cmcName n
            , EC.assign (cmcName n) $ EC.binop OAdd (EC.variable $ cmcName n) (EC.constant $ CInt 1) ]]]

    modeE = EC.constant . CString $ if isSource then "r" else "w"

    startE = case eSpec of
      BuiltinEP    _ _ -> fileStartE
      FileEP       _ _ _ -> fileStartE
      FileSeqEP    _ _ _ -> fileStartE
      FileMuxEP    _ _ _ -> fileStartE
      FileMuxseqEP _ _ _ -> fileStartE
      NetworkEP    _ _ _ -> EC.applyMany registerSocketDataTriggerFn [sourceId n, EC.variable $ ccName n]
      _                -> error "Invalid endpoint argument"

    fileStartE = EC.send (EC.variable $ ccName n) myAddr EC.unit

    closeE = case eSpec of
      FileMuxEP    _ _ _ -> closeMuxE
      FileMuxseqEP _ _ _ -> closeMuxE
      _ -> EC.applyMany closeFn [EC.variable "me", sourceId n]

    closeMuxE = EC.applyMany (EC.project "iterate" $ EC.applyMany (EC.variable "range") [EC.variable $ cmcName n])
                  [EC.lambda "r" $ EC.applyMany closeFn [EC.variable "me", muxChanIdE $ EC.project "i" $ EC.variable "r"]]

    controlE processE = case eSpec of
      BuiltinEP    _ _ -> fileControlE processE
      FileEP       _ _ _ -> fileControlE processE
      FileSeqEP    _ _ _ -> fileControlE processE
      FileMuxEP    _ _ _ -> fileControlE processE
      FileMuxseqEP _ _ _ -> fileControlE processE
      NetworkEP    _ _ _ -> processE
      _                -> error "Invalid endpoint argument"

    fileControlE processE = EC.block [processE, controlRcrE]
    controlRcrE = EC.send (EC.variable $ ccName n) myAddr EC.unit

    sourceId n' = EC.constant $ CString n'


-- | Rewrites a source declaration's process method to access and
--   dispatch the next available event to all its bindings.
bindSource :: [(Identifier, EndpointSpec)] -> [(Identifier, Identifier)] -> K3 Declaration -> (K3 Declaration, [K3 Declaration])
bindSource specs bindings d
  | DGlobal src t eOpt <- tag d
  , TSource <- tag t
  = (d, [mkDispatchFn src eOpt t])

  | otherwise = (d, [])

  where
    -- | Constructs a dispatch function declaration for a source.
    mkDispatchFn n eOpt t = case lookup n specs of
                              Just (FileMuxEP    _ _ _) -> mkFeedFn n $ head $ children t
                              Just (FileMuxseqEP _ _ _) -> mkFeedFn n $ head $ children t
                              Just _  -> mkProcessFn n eOpt
                              Nothing -> error $ "Could not find source endpoint spec for " ++ n

    mkProcessFn n eOpt = builtinGlobal (cpName n) (qualifyT unitFnT) (Just . qualifyE $ pbody n eOpt)
    mkFeedFn n t = builtinGlobal (cfName n) (qualifyT $ feedFnT t) (Just . qualifyE $ fbody n)

    pbody n eOpt = EC.lambda "_" $ EC.applyMany (processFnE n) [nextE n eOpt]
    fbody n = processFnE n

    processFnE n = EC.lambda "next" $ EC.block $
      map (\(_,dest) -> sendNextE dest) $ filter ((n ==) . fst) bindings

    nextE _ (Just e) = stripEUIDSpan e
    nextE n Nothing  = EC.applyMany (EC.variable $ crName n) [EC.unit]
    sendNextE dest   = EC.send (EC.variable dest) myAddr (EC.variable "next")

    unitFnT          = TC.function TC.unit TC.unit
    feedFnT argT     = TC.function argT TC.unit

-- | Constructs an "atInit" expression for initializing and starting sources.
mkRunSourceE :: Identifier -> K3 Expression
mkRunSourceE n = EC.block [EC.applyMany (EC.variable $ ciName n) [EC.unit],
                           EC.applyMany (EC.variable $ csName n) [EC.unit]]

-- | Constructs an "atInit" expression for initializing sinks.
mkRunSinkE :: Identifier -> K3 Expression
mkRunSinkE n = EC.applyMany (EC.variable $ ciName n) [EC.unit]


-- TODO: at_exit function body
declareBuiltins :: K3 Declaration -> K3 Declaration
declareBuiltins d
  | DRole n <- tag d, n == defaultRoleName = replaceCh d new_children
  | otherwise = d
  where new_children = peerDecls ++ (children d)

        peerDecls = [
          mkGlobal myId    TC.address Nothing,
          mkGlobal peersId peersT     Nothing,
          mkGlobal roleId  roleT      Nothing]

        peersT = mkCollection [("addr", TC.address)] "Collection"
        roleT  = mkCollection [(roleElemLbl, TC.string)] "Set"

        mkGlobal n t eOpt = builtinGlobal n (qualifyT t) $ maybe Nothing (Just . qualifyE) eOpt

        mkCurriedFnT tl = foldr1 TC.function tl

        --mkAUnitFnT at = TC.function at TC.unit
        --mkRUnitFnT rt = TC.function TC.unit rt
        --unitFnT       = TC.function TC.unit TC.unit

        mkCollection fields ann = (TC.collection $ TC.record $ map (qualifyT <$>) fields) @+ TAnnotation ann
