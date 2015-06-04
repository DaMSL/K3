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

{- Runtime functions -}

resolveFn :: K3 Expression
resolveFn = EC.variable "resolve"

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

endpointMethods :: Bool -> EndpointSpec -> Identifier -> K3 Type
                -> (EndpointSpec, Maybe (K3 Expression), [K3 Declaration])
endpointMethods isSource eSpec n t =
  if isSource then sourceDecls else sinkDecls
  where
    sourceDecls = (eSpec, Nothing, (map mkMethod [mkInit, mkStart, mkFinal, sourceHasRead, sourceRead, sourceControllerFn]))
    sinkDecls = (eSpec, Just sinkImpl, map mkMethod [mkInit, mkFinal, sinkHasWrite, sinkWrite])

    mkMethod (m, argT, retT, eOpt) =
      builtinGlobal (n++m) (qualifyT $ TC.function argT retT)
        $ maybe Nothing (Just . qualifyE) eOpt

    sinkImpl =
      EC.lambda "__msg"
        (EC.ifThenElse
          (EC.applyMany (EC.variable $ chwName n) [EC.unit])
          (EC.applyMany (EC.variable $ cwName n) [EC.variable "__msg"])
          (EC.unit))

    -- External functions
    cleanT = stripTUIDSpan t

    mkInit  = ("Init",  TC.unit, TC.unit, Nothing)
    mkStart = ("Start", TC.unit, TC.unit, Nothing)
    mkFinal = ("Final", TC.unit, TC.unit, Nothing)

    --sourceController = builtinGlobal (n++"Controller") (TC.trigger TC.unit) (Just $ EC.lambda "x" (EC.applyMany (EC.variable (n++"ControllerFn")) [EC.unit]))
    sourceControllerFn = ("ControllerFn",  TC.unit, TC.unit, Nothing)
    sourceHasRead = ("HasRead",  TC.unit, TC.bool, Nothing)
    sourceRead    = ("Read",     TC.unit, cleanT,  Nothing)
    sinkHasWrite  = ("HasWrite", TC.unit, TC.bool, Nothing)
    sinkWrite     = ("Write",    cleanT,  TC.unit, Nothing)


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
