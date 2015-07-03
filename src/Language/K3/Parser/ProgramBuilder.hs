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

import Control.Applicative

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

-- | Type synonyms, copied from the parser.
type EndpointInfo = (EndpointSpec, Maybe [Identifier], Identifier, Maybe (K3 Expression))

{- Names -}
defaultRoleName :: Identifier
defaultRoleName = "__global"

myId :: Identifier
myId = "me"

peersId :: Identifier
peersId = "peers"

argsId :: Identifier
argsId = "args"

myAddr :: K3 Expression
myAddr = EC.variable myId

chrName :: Identifier -> Identifier
chrName n = n++"HasRead"

crName :: Identifier -> Identifier
crName n = n++"Read"

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

{- -- Unused
registerFileDataTriggerFn :: K3 Expression
registerFileDataTriggerFn = EC.variable "registerFileDataTrigger"

registerFileCloseTriggerFn :: K3 Expression
registerFileCloseTriggerFn = EC.variable "registerFileCloseTrigger"

registerSocketAcceptTriggerFn :: K3 Expression
registerSocketAcceptTriggerFn = EC.variable "registerSocketAcceptTrigger"

registerSocketCloseTriggerFn :: K3 Expression
registerSocketCloseTriggerFn = EC.variable "registerSocketCloseTrigger"
-}

registerSocketDataTriggerFn :: K3 Expression
registerSocketDataTriggerFn = EC.variable "registerSocketDataTrigger"

resolveFn :: K3 Expression
resolveFn = EC.variable "resolve"

{- Top-level functions -}
roleId :: Identifier
roleId = "role"

roleVar :: K3 Expression
roleVar = EC.variable roleId

roleFnId :: Identifier
roleFnId = "processRole"

roleFn :: K3 Expression
roleFn = EC.variable roleFnId


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

processInitsAndRoles :: K3 Declaration -> [(Identifier, EndpointInfo)] -> [(Identifier, Identifier)]
                     -> K3 Declaration
processInitsAndRoles (Node t c) endpointBQGs roleDefaults = Node t $ c ++ initializerFns
  where
        (sinkEndpoints, sourceEndpoints) = partition matchSink endpointBQGs
        matchSink (_,(_, Nothing, _, _)) = True
        matchSink _ = False

        initializerFns =
            [ builtinGlobal roleFnId (qualifyT unitFnT)
                $ Just . qualifyE $ mkRoleBody sourceEndpoints sinkEndpoints roleDefaults ]

        sinkInitE acc (_,(_, Nothing, _, Just e)) = acc ++ [e]
        sinkInitE acc _ = acc

        mkRoleBody sources sinks defaults =
          EC.lambda "_" $ EC.block $
            (trace ("Sinks " ++ show sinks) $ foldl sinkInitE [] sinks) ++
            [uncurry (foldl dispatchId) $ defaultAndRestIds sources defaults]

        defaultAndRestIds sources defaults = (defaultE sources $ lookup "" defaults, sources)
        dispatchId elseE (n,(_,_,y,goE))  = EC.ifThenElse (eqRole y) (runE n goE) elseE

        eqRole n = EC.binop OEqu roleVar (EC.constant $ CString n)

        runE _ (Just goE) = goE
        runE n Nothing    = EC.applyMany (EC.variable $ cpName n) [EC.unit]

        defaultE s (Just x) = case find ((x ==) . third . snd) s of
                                Just (n,(_,_,_,goE)) -> runE n goE
                                Nothing              -> EC.unit
        defaultE _ Nothing   = EC.unit

        third (_,_,x,_) = x

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

    mkInit  = ("Init",  TC.unit, TC.unit, Just $ EC.lambda "_" $ initE)
    mkStart = ("Start", TC.unit, TC.unit, Just $ EC.lambda "_" $ startE)
    mkFinal = ("Final", TC.unit, TC.unit, Just $ EC.lambda "_" $ closeE)

    -- Endpoint-specific declarations
    sourceReadDecls = case eSpec of
                        FileMultiplexerEP _ nchans _ ->
                          concatMap (\i -> [sourceHasRead $ muxchanprefix i, sourceRead $ muxchanprefix i]) [0..(nchans-1)]
                        _ -> [sourceHasRead "", sourceRead ""]

    sourceExtraDecls = case eSpec of
      FileSeqEP _ _ -> [ builtinGlobal (cfiName n) (TC.int    @+ TMutable) Nothing
                       , builtinGlobal (cfpName n) (TC.string @+ TMutable) Nothing ]
      FileMultiplexerEP _ _ _ -> [ builtinGlobal (cfmpName n) muxPosMap Nothing
                                 , builtinGlobal (cfmbName n) muxBuffer Nothing
                                 , builtinGlobal (cfmdName n) t Nothing ]
      _ -> []

    mkMethod (m, argT, retT, eOpt) =
      builtinGlobal (n++m) (qualifyT $ TC.function argT retT)
        $ maybe Nothing (Just . qualifyE) eOpt

    mkCollection fields ctype = (TC.collection $ TC.record $ map (qualifyT <$>) fields) @+ TAnnotation ctype

    muxSeqLabel  = "order"
    muxDataLabel = "value"

    muxPosMap = mkCollection [("key", TC.int), ("value", TC.int)] "OrderedMap"
    muxBuffer = mkCollection [("key", TC.int), ("value", t)] "Map"
    muxFullT  = TC.record [("order", TC.int), ("value", t)]

    sourceController = case eSpec of
      FileSeqEP _ _ -> seqSrcController
      FileMultiplexerEP _ nchans _ -> muxSrcController nchans
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

    seqSrcController = builtinTrigger (ccName n) TC.unit $
      EC.lambda "_" $
        EC.ifThenElse
          (EC.applyMany (EC.variable $ chrName n) [EC.unit])
          (controlE $ EC.applyMany (EC.variable $ cpName n) [EC.unit])
          $ EC.block
              [ nextFileIndexE
              , EC.ifThenElse notLastFileIndexE
                  (EC.block [openSeqNextFileE False True openFileFn, controlRcrE])
                  EC.unit ]

    muxSrcController nchans = builtinTrigger (ccName n) TC.unit $
      EC.lambda "_" $
        EC.ifThenElse
          (EC.binop OLth (EC.constant $ CInt 0) $
             EC.applyMany (EC.project "size" $ EC.variable $ cfmpName n) [EC.unit])
          (controlE $ EC.applyMany (EC.project "min_with" $ EC.variable $ cfmpName n)
                        [EC.lambda muxid $ foldl doMuxNext EC.unit [0..(nchans-1)]])
          EC.unit

    muxchanprefix i = "_" ++ show i
    muxchanid  n' i = n' ++ muxchanprefix i
    muxid           = "muxnext"
    muxvar          = EC.variable muxid
    muxidx        i = EC.constant $ CInt i
    muxPred       i = EC.binop OEqu (EC.project "value" muxvar) $ muxidx i

    doMuxNext elseE i = flip (EC.ifThenElse $ muxPred i) elseE $
      EC.block
        [ EC.applyMany (EC.project "lookup_with" $ EC.variable $ cfmbName n)
            [ EC.record [("key", muxidx i), ("value", EC.variable $ cfmdName n)]
            , EC.lambda "x" $ EC.applyMany (EC.variable $ cfName n) [EC.project "value" $ EC.variable "x"] ]
        , EC.ifThenElse (EC.applyMany (EC.variable $ chrName $ muxchanid n i) [EC.unit])
            (EC.applyMany
              (EC.lambda "next" $ EC.block
                [ EC.applyMany (EC.project "insert" $ EC.variable $ cfmbName n)
                    [EC.record [("key", muxidx i), ("value", EC.project muxDataLabel $ EC.variable "next")]]
                , EC.applyMany (EC.project "insert" $ EC.variable $ cfmpName n)
                    [EC.record [("key", EC.project muxSeqLabel $ EC.variable "next"), ("value", muxidx i)]]
                , EC.applyMany (EC.project "erase" $ EC.variable $ cfmpName n) [muxvar]])
              [EC.applyMany (EC.variable $ crName $ muxchanid n i) [EC.unit]])
            (EC.block
              [ EC.applyMany (EC.project "erase" $ EC.variable $ cfmbName n)
                  [EC.record [("key", muxidx i), ("value", EC.variable $ cfmdName n)]]
              , EC.applyMany (EC.project "erase" $ EC.variable $ cfmpName n) [muxvar]]) ]

    nextFileIndexE =
      EC.assign (cfiName n) $ EC.binop OAdd (EC.variable $ cfiName n) (EC.constant $ CInt 1)

    notLastFileIndexE =
      EC.binop OLth (EC.variable $ cfiName n) (EC.applyMany (EC.project "size" argE) [EC.unit])

    openSeqNextFileE withTest withClose openFn = openSeqWithTest withTest $
      EC.block $
        [ EC.applyMany (EC.project "at_with" argE) [EC.variable $ cfiName n, assignSeqPathE] ]
        ++ (if withClose then [closeE] else [])
        ++ [ EC.applyMany openFn [sourceId n, EC.variable (cfpName n), formatE, modeE] ]

    assignSeqPathE = EC.lambda "r" $ EC.assign (cfpName n) (EC.project "path" $ EC.variable "r")

    openSeqWithTest withTest openE =
      if withTest then EC.ifThenElse notLastFileIndexE openE EC.unit else openE

    -- External functions
    cleanT = stripTUIDSpan $ case eSpec of
               FileMultiplexerEP _ _ _ -> muxFullT
               _ -> t

    sourceHasRead n' = (n'++"HasRead", TC.unit, TC.bool, Nothing)
    sourceRead    n' = (n'++"Read",    TC.unit, cleanT,  Nothing)

    sinkHasWrite = ("HasWrite",   TC.unit, TC.bool, Nothing)
    sinkWrite    = ("Write",      cleanT,  TC.unit, Nothing)

    initE = case eSpec of
      BuiltinEP                _ _ -> EC.applyMany openBuiltinFn [sourceId n, argE, formatE]
      FileEP                   _ _ -> openFnE openFileFn
      FileSeqEP                _ _ -> openFileSeqFnE openFileFn
      FileMultiplexerEP _ nchans _ -> openFileMuxFnE openFileFn nchans
      NetworkEP                _ _ -> openFnE openSocketFn
      _                            -> error "Invalid endpoint argument"

    openFnE openFn = EC.applyMany openFn [sourceId n, argE, formatE, modeE]

    openFileSeqFnE openFn =
      EC.block [ EC.assign (cfiName n) (EC.constant $ CInt 0), openSeqNextFileE True False openFn ]

    openFileMuxFnE openFn nchans = EC.block $ map (openFileMuxChanFnE openFn) [0..(nchans-1)]

    openFileMuxChanFnE openFn i =
      EC.applyMany (EC.project "at_with" argE)
        [ muxidx i, EC.lambda "f" $
          EC.block
            [ EC.applyMany openFn [sourceId $ muxchanid n i, EC.project "path" $ EC.variable "f", formatE, modeE]
            , EC.ifThenElse
                (EC.applyMany (EC.variable $ chrName $ muxchanid n i) [EC.unit])
                (EC.applyMany
                  (EC.lambda "x" $ EC.block
                    [ EC.applyMany (EC.project "insert" $ EC.variable $ cfmbName n)
                        [EC.record [("key", muxidx i), ("value", EC.project muxDataLabel $ EC.variable "x")]]
                    , EC.applyMany (EC.project "insert" $ EC.variable $ cfmpName n)
                        [EC.record [("key", EC.project muxSeqLabel $ EC.variable "x"), ("value", muxidx i)]]])
                  [EC.applyMany (EC.variable $ crName $ muxchanid n i) [EC.unit]])
                EC.unit ]]

    modeE = EC.constant . CString $ if isSource then "r" else "w"

    startE = case eSpec of
      BuiltinEP           _ _ -> fileStartE
      FileEP              _ _ -> fileStartE
      FileSeqEP           _ _ -> fileStartE
      FileMultiplexerEP _ _ _ -> fileStartE
      NetworkEP           _ _ -> EC.applyMany registerSocketDataTriggerFn [sourceId n, EC.variable $ ccName n]
      _                       -> error "Invalid endpoint argument"

    fileStartE = EC.send (EC.variable $ ccName n) myAddr EC.unit

    closeE = case eSpec of
      FileMultiplexerEP _ nchans _ -> EC.block $ map (\i -> EC.applyMany closeFn [sourceId $ muxchanid n i]) [0..(nchans-1)]
      _ -> EC.applyMany closeFn [sourceId n]

    controlE processE = case eSpec of
      BuiltinEP           _ _ -> fileControlE processE
      FileEP              _ _ -> fileControlE processE
      FileSeqEP           _ _ -> fileControlE processE
      FileMultiplexerEP _ _ _ -> fileControlE processE
      NetworkEP           _ _ -> processE
      _                       -> error "Invalid endpoint argument"

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
                              Just (FileMultiplexerEP _ _ _) -> mkFeedFn n $ head $ children t
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
  where new_children = runtimeDecls ++ peerDecls ++ (children d)

        runtimeDecls = [
          mkGlobal "registerFileDataTrigger"     (mkCurriedFnT [idT, TC.trigger TC.unit, TC.unit]) Nothing,
          mkGlobal "registerFileCloseTrigger"    (mkCurriedFnT [idT, TC.trigger TC.unit, TC.unit]) Nothing,
          mkGlobal "registerSocketAcceptTrigger" (mkCurriedFnT [idT, TC.trigger TC.unit, TC.unit]) Nothing,
          mkGlobal "registerSocketDataTrigger"   (mkCurriedFnT [idT, TC.trigger TC.unit, TC.unit]) Nothing,
          mkGlobal "registerSocketCloseTrigger"  (mkCurriedFnT [idT, TC.trigger TC.unit, TC.unit]) Nothing ]

        peerDecls = [
          mkGlobal myId    TC.address Nothing,
          mkGlobal peersId peersT     Nothing,
          mkGlobal argsId  progArgT   Nothing,
          mkGlobal roleId  TC.string  Nothing]

        idT      = TC.string
        progArgT = TC.tuple [qualifyT argT, qualifyT paramsT]
        peersT   = mkCollection [("addr", TC.address)]
        argT     = mkCollection [("arg", TC.string)]
        paramsT  = mkCollection [("key", TC.string), ("value", TC.string)]

        mkGlobal n t eOpt = builtinGlobal n (qualifyT t) $ maybe Nothing (Just . qualifyE) eOpt

        mkCurriedFnT tl = foldr1 TC.function tl

        --mkAUnitFnT at = TC.function at TC.unit
        --mkRUnitFnT rt = TC.function TC.unit rt
        --unitFnT       = TC.function TC.unit TC.unit

        mkCollection fields = (TC.collection $ TC.record $ map (qualifyT <$>) fields) @+ TAnnotation "Collection"
