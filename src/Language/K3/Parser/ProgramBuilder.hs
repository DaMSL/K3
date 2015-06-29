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

ccName :: Identifier -> Identifier
ccName n = n++"Controller"

cfiName :: Identifier -> Identifier
cfiName n = n++"FileIndex"

cfpName :: Identifier -> Identifier
cfpName n = n++"FilePath"

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
    sourceDecls = (eSpec, Nothing, (map mkMethod [mkInit, mkStart, mkFinal, sourceHasRead, sourceRead]) ++ [sourceController] ++ sourceExtraDecls)
    sinkDecls = (eSpec, Just sinkImpl, map mkMethod [mkInit, mkFinal, sinkHasWrite, sinkWrite])

    -- Endpoint-specific declarations
    sourceExtraDecls = case eSpec of
      FileSeqEP _ _ _ -> [ builtinGlobal (cfiName n) (TC.int    @+ TMutable) Nothing
                       , builtinGlobal (cfpName n) (TC.string @+ TMutable) Nothing ]
      _ -> []

    mkMethod (m, argT, retT, eOpt) =
      builtinGlobal (n++m) (qualifyT $ TC.function argT retT)
        $ maybe Nothing (Just . qualifyE) eOpt

    mkInit  = ("Init",  TC.unit, TC.unit, Nothing) 
    mkStart = ("Start", TC.unit, TC.unit, Nothing) 
    mkFinal = ("Final", TC.unit, TC.unit, Nothing) 

    sourceController = case eSpec of
      FileSeqEP _ _ _ -> seqSrcController
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
    cleanT = stripTUIDSpan t

    sourceHasRead = ("HasRead",  TC.unit, TC.bool, Nothing)
    sourceRead    = ("Read",     TC.unit, cleanT,  Nothing)
    sinkHasWrite  = ("HasWrite", TC.unit, TC.bool, Nothing)
    sinkWrite     = ("Write",    cleanT,  TC.unit, Nothing)

    --initE = case eSpec of
    --  BuiltinEP _ _   -> EC.applyMany openBuiltinFn [sourceId n, argE, formatE]
    --  FileEP    _ _ _ -> openFnE openFileFn
    --  FileSeqEP _ _ _ -> openFileSeqFnE openFileFn
    --  NetworkEP _ _ _ -> openFnE openSocketFn
    --  _             -> error "Invalid endpoint argument"

    openFnE openFn = EC.applyMany openFn [sourceId n, argE, formatE, modeE]

    openFileSeqFnE openFn =
      EC.block [ EC.assign (cfiName n) (EC.constant $ CInt 0), openSeqNextFileE True False openFn ]

    modeE = EC.constant . CString $ if isSource then "r" else "w"

    startE = case eSpec of
      BuiltinEP _ _   -> fileStartE
      FileEP    _ _ _ -> fileStartE
      FileSeqEP _ _ _ -> fileStartE
      NetworkEP _ _ _ -> EC.applyMany registerSocketDataTriggerFn [sourceId n, EC.variable $ ccName n]
      _             -> error "Invalid endpoint argument"

    fileStartE = EC.send (EC.variable (ccName n)) myAddr EC.unit

    closeE = EC.applyMany closeFn [sourceId n]

    controlE processE = case eSpec of
      BuiltinEP _ _   -> fileControlE processE
      FileEP    _ _ _ -> fileControlE processE
      FileSeqEP _ _ _ -> fileControlE processE
      NetworkEP _ _ _ -> processE
      _             -> error "Invalid endpoint argument"

    fileControlE processE = EC.block [processE, controlRcrE]
    controlRcrE = EC.send (EC.variable $ ccName n) myAddr EC.unit

    sourceId n' = EC.constant $ CString n'

-- | Rewrites a source declaration's process method to access and
--   dispatch the next available event to all its bindings.
bindSource :: [(Identifier, Identifier)] -> K3 Declaration -> (K3 Declaration, [K3 Declaration])
bindSource bindings d
  | DGlobal src t eOpt <- tag d
  , TSource <- tag t
  = (d, [mkProcessFn src eOpt])

  | otherwise = (d, [])

  where
    -- | Constructs a dispatch function declaration for a source.
    mkProcessFn n eOpt =
      builtinGlobal (cpName n) (qualifyT unitFnT) (Just . qualifyE $ body n eOpt)

    body n eOpt = EC.lambda "_" $ EC.applyMany (processFnE n) [nextE n eOpt]

    processFnE n = EC.lambda "next" $ EC.block $
      map (\(_,dest) -> sendNextE dest) $ filter ((n ==) . fst) bindings

    nextE _ (Just e) = stripEUIDSpan e
    nextE n Nothing  = EC.applyMany (EC.variable $ crName n) [EC.unit]
    sendNextE dest   = EC.send (EC.variable dest) myAddr (EC.variable "next")
    unitFnT          = TC.function TC.unit TC.unit

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

        peersT = mkCollection [("addr", TC.address)]
        roleT  = mkCollection [(roleElemLbl, TC.string)]

        mkGlobal n t eOpt = builtinGlobal n (qualifyT t) $ maybe Nothing (Just . qualifyE) eOpt

        mkCollection fields = (TC.collection $ TC.record $ map (qualifyT <$>) fields) @+ TAnnotation "Collection"
