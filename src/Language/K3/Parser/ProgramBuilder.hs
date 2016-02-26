{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}

-- | K3 Program constructor
module Language.K3.Parser.ProgramBuilder (
  defaultIncludes,

  defaultRoleName,
  processInitsAndRoles,
  endpointMethods,
  bindSource,
  mkRunSourceE,
  mkRunSinkE,
  declareBuiltins,

  resolveFn
) where

import Data.Char ( isPunctuation )
import Data.Hashable
import Data.List
import Data.Tree

import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Utils.Pretty

-- | Type synonyms, copied from the parser.
type EndpointInfo = (EndpointSpec, Maybe [Identifier], Identifier, Maybe (K3 Expression))

{- Default includes required by the program builder. -}
defaultIncludes :: [String]
defaultIncludes = map (\s -> unwords ["include", show s])
                    [ "Annotation/Collection.k3"
                    , "Annotation/Set.k3"
                    , "Annotation/Map.k3"
                    , "Annotation/Maps/SortedMap.k3" ]

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

cpohrName :: Identifier -> Identifier
cpohrName n = n++"POrdHasRead"

cporName :: Identifier -> Identifier
cporName n = n++"POrdRead"

cpdhrName :: Identifier -> Identifier
cpdhrName n = n++"PDataHasRead"

cpdrName :: Identifier -> Identifier
cpdrName n = n++"PDataRead"

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

cfpcompleteName :: Identifier -> Identifier
cfpcompleteName n = n++"FilesComplete"

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
roleElemLbl = "i"

roleFnId :: Identifier
roleFnId = "processRole"

{- Declaration construction -}
builtinGlobal :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3 Declaration
builtinGlobal n t eOpt = (DC.global n t eOpt) @+ (DSpan $ GeneratedSpan $ fromIntegral $ hash "builtin")

builtinTrigger :: Identifier -> K3 Type -> K3 Expression -> K3 Declaration
builtinTrigger n t e = (DC.trigger n t e) @+ (DSpan $ GeneratedSpan $ fromIntegral $ hash "builtin")

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

endpointMethods :: Bool -> EndpointSpec -> K3 Expression -> K3 Expression
                -> Identifier -> K3 Type
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
                        PolyFileMuxEP _ _ _ _ _ -> [sourcePOrdHasRead, sourcePOrdRead, sourcePolyHasRead, sourcePolyRead]
                        PolyFileMuxSeqEP _ _ _ _ _ -> [sourcePOrdHasRead, sourcePOrdRead, sourcePolyHasRead, sourcePolyRead]
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

      PolyFileMuxEP _ _ _ _ _ -> [ builtinGlobal (cfpcompleteName n) pmuxDoneMap Nothing
                                   , builtinGlobal (cmcName n) (TC.int @+ TMutable) (Just $ EC.constant $ CInt 0) ]

      PolyFileMuxSeqEP _ _ _ _ _ -> [ builtinGlobal (cfpcompleteName n) pmuxDoneMap Nothing
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

    pmuxDoneMap = mkCollection [("key", TC.int), ("value", TC.bool)] "Map"

    sourceController = case eSpec of
      FileSeqEP    _ txt _          -> seqSrcController txt
      FileMuxEP    _ _ _            -> muxSrcController
      FileMuxseqEP _ txt _          -> muxSeqSrcController txt
      PolyFileMuxEP _ _ _ _ sv      -> pmuxSrcController sv
      PolyFileMuxSeqEP _ txt _ _ sv -> pmuxSeqSrcController txt sv
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
        [ assignSeqPathE ]
        ++ (if withClose then [closeE] else [])
        ++ [ EC.applyMany openFn [EC.variable "me", sourceId n, EC.variable (cfpName n), formatE, EC.constant $ CBool txt, modeE] ]

    assignSeqPathE = EC.assign (cfpName n) $ EC.project "path" $ EC.applyMany (EC.project "at" argE) [EC.variable $ cfiName n]

    openSeqWithTest withTest openE =
      if withTest then EC.ifThenElse notLastFileIndexE openE EC.unit else openE


    {---------------------------------------------
     - File multiplexer controller and utilities.
     ---------------------------------------------}
    muxSrcControllerTrig onFileDoneE = builtinTrigger (ccName n) TC.unit $
      EC.lambda "_" $
        EC.ifThenElse
          (EC.binop OLth (EC.constant $ CInt 0) $
             EC.applyMany (EC.project "size" $ EC.variable $ cfmpName n) [EC.unit])
          (controlE $ EC.applyMany (EC.project "min" $ EC.variable $ cfmpName n)
                        [EC.lambda "_" EC.unit, EC.lambda muxid $ doMuxNext onFileDoneE])
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
      EC.applyMany (EC.project "lookup" $ EC.variable $ cfmbName n)
        [ EC.record [("key", muxidx), ("value", EC.variable $ cfmdName n)]
        , ignoreE
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
      EC.applyMany (EC.project "safe_at" $ argE)
        [ muxidx
        , ignoreE
        , EC.lambda "seqc" $
            EC.applyMany (EC.project "lookup" $ EC.variable $ cfiName n)
              [ muxSeqIdx $ EC.constant $ CInt 0
              , ignoreE
              , EC.lambda "seqidx" $
                  EC.ifThenElse (muxSeqNotLastFileIndexE "seqc" "seqidx")
                    (EC.block [muxSeqNextFileE openFn "seqc" "seqidx" txt])
                    (EC.block [muxFinishChan, muxSeqFinishChan muxSeqIdx]) ]]

    muxSeqNextFileE openFn seqvar idxvar txt =
      EC.letIn "nextidx"
        (EC.binop OAdd (EC.project "value" $ EC.variable idxvar) $ EC.constant $ CInt 1)
        (EC.applyMany (EC.project "safe_at" $ EC.project "seq" $ EC.variable seqvar)
          [ EC.variable "nextidx"
          , ignoreE
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

    muxSeqFinishChan muxSeqFn = EC.block
      [ EC.applyMany (EC.project "erase" $ EC.variable $ cfiName n) [muxSeqFn $ EC.constant $ CInt 0]
      , EC.applyMany (EC.project "erase" $ EC.variable $ cfpName n) [muxSeqFn $ EC.constant $ CString ""]]

    muxSeqIdx e = EC.record [("key", muxidx), ("value", e)]


    {-------------------------------------------------
     - Poly-File multiplexer controller and utilities.
     -------------------------------------------------}

    pmuxidx     = "pmuxidx"
    pmuxvar     = EC.variable pmuxidx
    pmuxnext    = "pmuxnext"
    pmuxnextvar = EC.variable pmuxnext

    pmuxOrderChanIdE = EC.constant $ CString $ n ++ "_order"
    cntrlrPMuxChanIdE = muxChanIdE pmuxvar

    pmuxSrcControllerTrig onFileDoneE rbsizeV = builtinTrigger (ccName n) TC.unit $
      EC.lambda "_" $
        EC.ifThenElse
          (EC.binop OGth
            (EC.applyMany (EC.project "size" argE) [EC.unit]) $
             EC.applyMany (EC.project "size" $ EC.variable $ cfpcompleteName n) [EC.unit])
          (EC.ifThenElse
              (EC.applyMany (EC.variable $ cpohrName n) [EC.unit])
              (controlE $ EC.applyMany
                            (EC.lambda pmuxidx $ pmuxNextOrderE onFileDoneE rbsizeV)
                            [EC.applyMany (EC.variable $ cporName n) [EC.unit]])
              EC.unit)
          EC.unit

    {- Polyfile controllers. -}
    pmuxSrcController rbsizeV = pmuxSrcControllerTrig pmuxFinishChan rbsizeV

    pmuxSeqSrcController txt rbsizeV =
      pmuxSrcControllerTrig (pmuxSeqNextChan openFileFn txt rbsizeV) rbsizeV

    {- Polyfile controller codegen. -}
    pmuxNextOrderE onFileDoneE rbsizeV =
      EC.ifThenElse (EC.binop OOr
                      (EC.binop OGeq pmuxvar $ EC.applyMany (EC.project "size" argE) [EC.unit])
                      $ (EC.applyMany (EC.project "member" $ EC.variable $ cfpcompleteName n)
                                      [EC.record [("key", pmuxvar), ("value", EC.constant $ CBool True)]]))
        EC.unit
        (pmuxSafeNextChan onFileDoneE rbsizeV)

    pmuxSafeNextChan onFileDoneE rbsizeV =
      EC.ifThenElse (EC.applyMany (EC.variable $ cpdhrName n) [pmuxvar])
                    (pmuxNextChan rbsizeV)
                    onFileDoneE

    pmuxNextChan rbsizeV =
      EC.applyMany
        (EC.lambda pmuxnext $
          EC.letIn "buffer" defaultBuffer $
            EC.block
            [ EC.applyMany (EC.project "load" $ EC.variable "buffer") [pmuxnextvar]
            , EC.ifThenElse
                (EC.binop OEqu (EC.variable rbsizeV) $ EC.constant $ CInt 0)
                noRebufferE
                rebufferE])
        [EC.applyMany (EC.variable $ cpdrName n) [pmuxvar]]

      where
        feedBufferE bufE = EC.applyMany (EC.variable $ cfName n) [bufE]
        noRebufferE = feedBufferE $ EC.variable "buffer"

        rebufferE =
          EC.applyMany (EC.project "iterate" $ EC.applyMany (EC.project "splitMany" $ EC.variable "buffer")
                                                            [EC.variable rbsizeV])
            [EC.lambda "rebuf" $ EC.block
              [ EC.applyMany (EC.project "unpack" $ EC.project "elem" $ EC.variable "rebuf") [EC.unit]
              , feedBufferE $ EC.project "elem" $ EC.variable "rebuf"]]

        defaultBuffer = either error debugDefault $ defaultExpression cleanT
        debugDefault dt = if True then dt else trace (boxToString $
          ["Default buffer expr: "] ++ prettyLines dt ++
          ["CleanT: "] ++ prettyLines cleanT) dt


    pmuxFinishChan =
      EC.applyMany (EC.project "insert" $ EC.variable $ cfpcompleteName n)
        [EC.record [("key", pmuxvar), ("value", EC.constant $ CBool True)]]

    pmuxSeqNextChan openFn txt rbsizeV =
      EC.applyMany (EC.project "safe_at" $ argE)
        [ pmuxvar
        , ignoreE
        , EC.lambda "seqc" $
            EC.applyMany (EC.project "lookup" $ EC.variable $ cfiName n)
              [ pmuxSeqIdx $ EC.constant $ CInt 0
              , ignoreE
              , EC.lambda "seqidx" $
                  EC.ifThenElse (muxSeqNotLastFileIndexE "seqc" "seqidx")
                    (EC.block [pmuxSeqNextFileE openFn txt rbsizeV "seqc" "seqidx"])
                    (EC.block [pmuxFinishChan, muxSeqFinishChan pmuxSeqIdx]) ]]

    pmuxSeqNextFileE openFn txt rbsizeV seqvar idxvar =
      EC.letIn "nextidx"
        (EC.binop OAdd (EC.project "value" $ EC.variable idxvar) $ EC.constant $ CInt 1)
        (EC.applyMany (EC.project "safe_at" $ EC.project "seq" $ EC.variable seqvar)
          [ EC.variable "nextidx"
          , ignoreE
          , EC.lambda "f" $ EC.block
              [ EC.applyMany closeFn [EC.variable "me", cntrlrPMuxChanIdE]
              , EC.applyMany (EC.project "insert" $ EC.variable $ cfiName n) [pmuxSeqIdx $ EC.variable "nextidx"]
              , EC.applyMany (EC.project "insert" $ EC.variable $ cfpName n) [pmuxSeqIdx $ EC.project "path" $ EC.variable "f"]
              , EC.applyMany openFn [EC.variable "me", cntrlrPMuxChanIdE, EC.project "path" $ EC.variable "f", formatE, EC.constant $ CBool txt, modeE]
              , pmuxSafeNextChan EC.unit rbsizeV]])

    pmuxSeqIdx e = EC.record [("key", pmuxvar), ("value", e)]

    -- External functions
    cleanT = stripTUIDSpan $ case eSpec of
               FileMuxEP    _ _ _ -> muxFullT
               FileMuxseqEP _ _ _ -> muxFullT
               _ -> t

    sourceHasRead = ("HasRead", TC.unit, TC.bool, Nothing)
    sourceRead    = ("Read",    TC.unit, cleanT,  Nothing)

    sourceMuxHasRead = ("MuxHasRead", TC.int, TC.bool, Nothing)
    sourceMuxRead    = ("MuxRead",    TC.int, cleanT,  Nothing)

    sourcePOrdHasRead = ("POrdHasRead", TC.unit, TC.bool, Nothing)
    sourcePOrdRead    = ("POrdRead",    TC.unit, TC.int,  Nothing)

    sourcePolyHasRead = ("PDataHasRead", TC.int, TC.bool,   Nothing)
    sourcePolyRead    = ("PDataRead",    TC.int, TC.string, Nothing)

    sinkHasWrite = ("HasWrite",   TC.unit, TC.bool, Nothing)
    sinkWrite    = ("Write",      cleanT,  TC.unit, Nothing)

    initE = case eSpec of
      BuiltinEP     _ _ -> EC.applyMany openBuiltinFn [sourceId n, argE, formatE]
      FileEP        _ txt _                -> openFnE openFileFn txt
      NetworkEP     _ txt _                -> openFnE openSocketFn txt
      FileSeqEP     _ txt _                -> openFileSeqFnE openFileFn txt
      FileMuxEP     _ txt _                -> openFileMuxChanFnE openFileFn txt
      FileMuxseqEP  _ txt _                -> openFileMuxSeqChanFnE openFileFn txt
      PolyFileMuxEP _ txt _ orderpath _    -> openPolyFileFnE openFileFn orderpath txt
      PolyFileMuxSeqEP _ txt _ orderpath _ -> openPolyFileSeqFnE openFileFn orderpath txt
      _ -> error "Invalid endpoint argument"

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
          EC.applyMany (EC.project "safe_at" $ EC.project "seq" $ EC.variable "seqc")
          [ EC.constant $ CInt 0
          , ignoreE
          , EC.lambda "f" $ EC.block
            [ EC.applyMany (EC.project "insert" $ EC.variable $ cfiName n) [openMuxSeqIdx $ EC.constant $ CInt 0]
            , EC.applyMany (EC.project "insert" $ EC.variable $ cfpName n) [openMuxSeqIdx $ EC.project "path" $ EC.variable "f"]
            , EC.applyMany openFn [EC.variable "me", globalMuxChanIdE, EC.project "path" $ EC.variable "f", formatE, EC.constant $ CBool txt, modeE]
            , muxSafeRefreshChan False EC.unit $ EC.variable $ cmcName n
            , EC.assign (cmcName n) $ EC.binop OAdd (EC.variable $ cmcName n) (EC.constant $ CInt 1) ]]]

    {- Order file constants for polyfiles. -}
    orderFormatE = EC.constant $ CString "csv"
    orderTxtE    = EC.constant $ CBool True

    orderPathE orderpath = if (not $ null orderpath) && (isPunctuation $ head orderpath)
                              then EC.constant $ CString orderpath
                              else EC.variable orderpath

    openPolyFileFnE openFn orderpath txt =
      EC.block [
        EC.applyMany openFn [EC.variable "me", pmuxOrderChanIdE, orderPathE orderpath, orderFormatE, orderTxtE, modeE],
        EC.applyMany (EC.project "iterate" argE)
          [ EC.lambda "f" $ EC.block
            [ EC.applyMany openFn [EC.variable "me", globalMuxChanIdE, EC.project "path" $ EC.variable "f", formatE, EC.constant $ CBool txt, modeE]
            , EC.assign (cmcName n) $ EC.binop OAdd (EC.variable $ cmcName n) (EC.constant $ CInt 1) ]]]

    openPolyFileSeqFnE openFn orderpath txt =
      EC.block [
        EC.applyMany openFn [EC.variable "me", pmuxOrderChanIdE, orderPathE orderpath, orderFormatE, orderTxtE, modeE],
        EC.applyMany (EC.project "iterate" argE)
          [ EC.lambda "seqc" $
            EC.applyMany (EC.project "safe_at" $ EC.project "seq" $ EC.variable "seqc")
            [ EC.constant $ CInt 0
            , ignoreE
            , EC.lambda "f" $ EC.block
              [ EC.applyMany (EC.project "insert" $ EC.variable $ cfiName n) [openMuxSeqIdx $ EC.constant $ CInt 0]
              , EC.applyMany (EC.project "insert" $ EC.variable $ cfpName n) [openMuxSeqIdx $ EC.project "path" $ EC.variable "f"]
              , EC.applyMany openFn [EC.variable "me", globalMuxChanIdE, EC.project "path" $ EC.variable "f", formatE, EC.constant $ CBool txt, modeE]
              , EC.assign (cmcName n) $ EC.binop OAdd (EC.variable $ cmcName n) (EC.constant $ CInt 1) ]]]]


    modeE = EC.constant . CString $ if isSource then "r" else "w"

    startE = case eSpec of
      BuiltinEP    _ _   -> fileStartE
      FileEP       _ _ _ -> fileStartE
      NetworkEP    _ _ _ -> EC.applyMany registerSocketDataTriggerFn [sourceId n, EC.variable $ ccName n]
      FileSeqEP    _ _ _ -> fileStartE
      FileMuxEP    _ _ _ -> fileStartE
      FileMuxseqEP _ _ _ -> fileStartE
      PolyFileMuxEP _ _ _ _ _    -> fileStartE
      PolyFileMuxSeqEP _ _ _ _ _ -> fileStartE
      _                -> error "Invalid endpoint argument"

    fileStartE = EC.send (EC.variable $ ccName n) myAddr EC.unit

    closeE = case eSpec of
      FileMuxEP    _ _ _ -> closeMuxE
      FileMuxseqEP _ _ _ -> closeMuxE
      PolyFileMuxEP _ _ _ _ _ -> closePMuxE
      PolyFileMuxSeqEP _ _ _ _ _ -> closePMuxE
      _ -> EC.applyMany closeFn [EC.variable "me", sourceId n]

    closeMuxE = EC.applyMany (EC.project "iterate" $ EC.applyMany (EC.variable "range") [EC.variable $ cmcName n])
                  [EC.lambda "r" $ EC.applyMany closeFn [EC.variable "me", muxChanIdE $ EC.project "elem" $ EC.variable "r"]]

    closePMuxE = EC.block [ EC.applyMany closeFn [EC.variable "me", pmuxOrderChanIdE], closeMuxE ]

    controlE processE = case eSpec of
      BuiltinEP     _ _ -> fileControlE processE
      FileEP        _ _ _ -> fileControlE processE
      NetworkEP     _ _ _ -> processE
      FileSeqEP     _ _ _ -> fileControlE processE
      FileMuxEP     _ _ _ -> fileControlE processE
      FileMuxseqEP  _ _ _ -> fileControlE processE
      PolyFileMuxEP _ _ _ _ _ -> fileControlE processE
      PolyFileMuxSeqEP _ _ _ _ _ -> fileControlE processE
      _                -> error "Invalid endpoint argument"

    fileControlE processE = EC.block [processE, controlRcrE]
    controlRcrE = EC.send (EC.variable $ ccName n) myAddr EC.unit

    sourceId n' = EC.constant $ CString n'

    ignoreE = EC.variable "ignore"


-- | Rewrites a source declaration's process method to access and
--   dispatch the next available event to all its bindings.
bindSource :: [(Identifier, EndpointSpec)] -> [(Identifier, Identifier)] -> K3 Declaration -> (K3 Declaration, [K3 Declaration])
bindSource specs bindings d
  | DGlobal src t eOpt <- tag d
  , TSource <- tag t
  = (d, mkDispatchFn src eOpt t)

  | otherwise = (d, [])

  where
    -- | Constructs a dispatch function declaration for a source.
    mkDispatchFn n eOpt t = case lookup n specs of
                              Just (FileMuxEP    _ _ _)    -> [mkFeedFn n $ head $ children t]
                              Just (FileMuxseqEP _ _ _)    -> [mkFeedFn n $ head $ children t]
                              Just (PolyFileMuxEP _ _ _ _ _) -> [mkFeedFn n $ head $ children t]
                              Just (PolyFileMuxSeqEP _ _ _ _ _) -> [mkFeedFn n $ head $ children t]
                              Just _  -> [mkProcessFn n eOpt]
                              Nothing -> []

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
