{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}

-- | K3 Program constructor
module Language.K3.Parser.ProgramBuilder (
  EndpointType(..),
  defaultRoleName,
  desugarRoleEntries,
  endpointMethods,
  rewriteSource,
  mkRunSourceE,
  mkRunSinkE,
  declareBuiltins
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

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

data EndpointType = Builtin | File | Network 

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
cfName n = n++"Final"

ccName :: Identifier -> Identifier
ccName n = n++"Controller"

{- Runtime functions -}
openBuiltinFn :: K3 Expression
openBuiltinFn = EC.variable "openBuiltin"

openFileFn :: K3 Expression
openFileFn = EC.variable "openFile"

openSocketFn :: K3 Expression
openSocketFn = EC.variable "openSocket"

closeFn :: K3 Expression
closeFn = EC.variable "close"

registerFileDataTriggerFn :: K3 Expression
registerFileDataTriggerFn = EC.variable "registerFileDataTrigger"

registerFileCloseTriggerFn :: K3 Expression
registerFileCloseTriggerFn = EC.variable "registerFileCloseTrigger"

registerSocketAcceptTriggerFn :: K3 Expression
registerSocketAcceptTriggerFn = EC.variable "registerSocketAcceptTrigger"

registerSocketDataTriggerFn :: K3 Expression
registerSocketDataTriggerFn = EC.variable "registerSocketDataTrigger"

registerSocketCloseTriggerFn :: K3 Expression
registerSocketCloseTriggerFn = EC.variable "registerSocketCloseTrigger"

{- Top-level functions -}
initId :: Identifier
initId = "atInit"

exitId :: Identifier
exitId = "atExit"

initDeclId :: Identifier
initDeclId = "initDecls"

initDeclFn :: K3 Expression
initDeclFn = EC.variable initDeclId

roleId :: Identifier
roleId = "role"

roleVar :: K3 Expression
roleVar = EC.variable roleId

roleFnId :: Identifier
roleFnId = "processRole"

roleFn :: K3 Expression
roleFn = EC.variable roleFnId

parseArgsId :: Identifier
parseArgsId = "parseArgs"

parseArgsFn :: K3 Expression
parseArgsFn   = EC.variable parseArgsId

replace_children :: K3 a -> [K3 a] -> K3 a
replace_children (Node n _) nc = Node n nc


{- Desugaring methods -}
-- TODO: replace with Template Haskell

desugarRoleEntries :: K3 Declaration
                      -> [(Identifier, (Maybe [Identifier], Identifier, Maybe (K3 Expression)))]
                      -> [(Identifier, Identifier)]
                      -> K3 Declaration
desugarRoleEntries (Node t c) endpointBQGs roleDefaults  = Node t $ c ++ initializerFns
  where 
        (sinkEndpoints, sourceEndpoints) = partition matchSink endpointBQGs
        matchSink (_,(Nothing, _, _)) = True
        matchSink _ = False

        initializerFns =
            [DC.global initDeclId unitFnT (Just $ mkInitDeclBody sinkEndpoints),
             DC.global roleFnId unitFnT (Just $ mkRoleBody sourceEndpoints roleDefaults)]

        mkInitDeclBody sinks = EC.lambda "_" $ EC.block $ foldl sinkInitE [] sinks

        sinkInitE acc (_,(Nothing, _, Just e)) = acc ++ [e]
        sinkInitE acc _ = acc
        
        mkRoleBody sources defaults =
          EC.lambda "_" $ uncurry (foldl dispatchId) $ defaultAndRestIds sources defaults
        
        defaultAndRestIds sources defaults = (defaultE sources $ lookup "" defaults, sources)
        dispatchId elseE (n,(_,y,goE))  = EC.ifThenElse (eqRole y) (runE n goE) elseE
        
        eqRole n = EC.binop OEqu roleVar (EC.constant $ CString n)

        runE _ (Just goE) = goE
        runE n Nothing    = EC.applyMany (EC.variable $ cpName n) [EC.unit]

        defaultE s (Just x) = case find ((x ==) . second . snd) s of
                                Just (n,(_,_,goE)) -> runE n goE
                                Nothing -> EC.unit
        defaultE _ Nothing   = EC.unit

        second (_,x,_) = x
        
        unitFnT = TC.function TC.unit TC.unit


{- Code generation methods-}
-- TODO: replace with Template Haskell

endpointMethods :: Bool -> EndpointType -> K3 Expression -> K3 Expression ->
                   Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration])
endpointMethods isSource eType argE formatE n t =
  if isSource then sourceDecls else sinkDecls
  where 
    sourceDecls = (Nothing,) $
         (map mkMethod [mkInit, mkStart, mkFinal, sourceHasRead, sourceRead])
      ++ [sourceController]

    sinkDecls = (Just sinkImpl, map mkMethod [mkInit, mkFinal, sinkHasWrite, sinkWrite])

    mkMethod (m,argT,retT,eOpt) = DC.global (n++m) (TC.function argT retT) eOpt

    mkInit  = ("Init",  TC.unit, TC.unit, Just $ EC.lambda "_" $ openEndpointE)
    mkStart = ("Start", TC.unit, TC.unit, Just $ EC.lambda "_" $ startE)
    mkFinal = ("Final", TC.unit, TC.unit, Just $ EC.lambda "_" $ EC.applyMany closeFn [sourceId n])

    sourceController = DC.trigger (ccName n) TC.unit $
      EC.lambda "_"
        (EC.ifThenElse
          (EC.applyMany (EC.variable $ chrName n) [EC.unit])
          (controlE $ EC.applyMany (EC.variable $ cpName n) [EC.unit])
          EC.unit)

    sinkImpl =
      EC.lambda "__msg"
        (EC.ifThenElse
          (EC.applyMany (EC.variable $ chwName n) [EC.unit])
          (EC.applyMany (EC.variable $ cwName n) [EC.variable "__msg"])
          (EC.unit))

    -- External functions
    sourceHasRead = ("HasRead",  TC.unit, TC.bool, Nothing)
    sourceRead    = ("Read",     TC.unit, t,       Nothing)
    sinkHasWrite  = ("HasWrite", TC.unit, TC.bool, Nothing)
    sinkWrite     = ("Write",    t,       TC.unit, Nothing)

    openEndpointE = case eType of
      Builtin -> EC.applyMany openBuiltinFn [sourceId n, argE, formatE]
      File    -> openFnE openFileFn
      Network -> openFnE openSocketFn

    openFnE openFn = EC.applyMany openFn [sourceId n, argE, formatE, modeE]

    modeE = EC.constant . CString $ if isSource then "r" else "w"

    startE = case eType of
      Builtin -> fileStartE
      File    -> fileStartE
      Network -> EC.applyMany registerSocketDataTriggerFn [sourceId n, EC.variable $ ccName n]

    fileStartE = EC.send (EC.variable (ccName n)) myAddr EC.unit

    controlE processE = case eType of
      Builtin -> fileControlE processE
      File    -> fileControlE processE
      Network -> processE

    fileControlE processE =
      EC.block [processE, (EC.send (EC.variable $ ccName n) myAddr EC.unit)]

    sourceId n = EC.constant $ CString n

-- | Rewrites a source declaration's process method to access and dispatch the next available event to all its bindings
rewriteSource :: [(Identifier, Identifier)] -> K3 Declaration -> K3 Declaration
rewriteSource bindings d
  | DGlobal src t eOpt <- tag d
  , TSource <- tag t
  = replace_children d $ (children d) ++ [mkProcessFn bindings src eOpt]

  | otherwise = d

-- | Constructs a dispatch function declaration for a source.
mkProcessFn :: [(Identifier, Identifier)] -> Identifier -> Maybe (K3 Expression) -> K3 Declaration
mkProcessFn bindings n eOpt = 
    DC.global (cpName n) unitFnT (Just $ body bindings n eOpt)
  
  where body bindings n eOpt =
          EC.lambda "_" $ EC.applyMany (processFnE bindings n) [nextE n eOpt]
        
        processFnE bindings n = EC.lambda "next" $ EC.block $
          map (\(_,dest) -> sendNextE dest) $ filter ((n ==) . fst) bindings
        
        nextE _ (Just e) = e
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
-- TODO: stdin, stdout, stderr
declareBuiltins :: K3 Declaration -> K3 Declaration
declareBuiltins d
  | DRole defaultRoleName <- tag d = replace_children d new_children
  | otherwise = d
  where new_children = runtimeDecls ++ peerDecls ++ (children d) ++ topLevelDecls

        runtimeDecls = [
          DC.global parseArgsId (mkUnitFnT argT) Nothing,
          DC.global "openBuiltin" (flip TC.function TC.unit $ TC.tuple [idT, TC.string,  TC.string]) Nothing,
          DC.global "openFile"    (flip TC.function TC.unit $ TC.tuple [idT, TC.string,  TC.string, TC.string]) Nothing,
          DC.global "openSocket"  (flip TC.function TC.unit $ TC.tuple [idT, TC.address, TC.string, TC.string]) Nothing,
          DC.global "close"       (TC.function idT TC.unit) Nothing,
          DC.global "registerFileDataTrigger"     (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerFileCloseTrigger"    (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerSocketAcceptTrigger" (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerSocketDataTrigger"   (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerSocketCloseTrigger"  (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing ]

        peerDecls = [
          DC.global myId TC.address Nothing,
          DC.global peersId (TC.collection TC.address) Nothing,
          DC.global argsId argT Nothing,
          DC.global roleId TC.string Nothing]

        topLevelDecls = [
          DC.global initId unitFnT $ Just atInitE,
          DC.global exitId unitFnT $ Just atExitE ]

        atInitE = EC.lambda "_" $
          EC.block [--EC.assign argsId (EC.applyMany parseArgsFn [EC.unit]),
                    EC.applyMany initDeclFn [EC.unit],
                    EC.applyMany roleFn [EC.unit]]

        atExitE = EC.lambda "_" $ EC.tuple []

        idT = TC.string
        argT = TC.tuple [TC.collection TC.string,
                         TC.collection $ TC.tuple [TC.string, TC.string]]

        mkUnitFnT rt = TC.function TC.unit rt
        unitFnT = TC.function TC.unit TC.unit
