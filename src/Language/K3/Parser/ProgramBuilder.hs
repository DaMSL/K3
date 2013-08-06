{-# LANGUAGE PatternGuards #-}

-- | K3 Program constructor
module Language.K3.Parser.ProgramBuilder (
  defaultRoleName,
  desugarRoleEntries,
  channelMethods,
  rewriteSource,
  mkRunSourceE,
  declareBuiltins
) where

import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC


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

ccName :: Identifier -> Identifier
ccName n = n++"Controller"

chName :: Identifier -> Identifier
chName n = n++"HasNext"

ciName :: Identifier -> Identifier
ciName n = n++"Init"

cfName :: Identifier -> Identifier
cfName n = n++"Final"

cpName :: Identifier -> Identifier
cpName n = n++"Process"

cnName :: Identifier -> Identifier
cnName n = n++"Next"

csName :: Identifier -> Identifier
csName n = n++"Start"

{- Runtime functions -}
openFileFn :: K3 Expression
openFileFn    = EC.variable "openFile"

openSocketFn :: K3 Expression
openSocketFn  = EC.variable "openSocket"

closeFileFn :: K3 Expression
closeFileFn   = EC.variable "closeFile"

closeSocketFn :: K3 Expression
closeSocketFn = EC.variable "closeSocket"

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

goId :: Identifier
goId = "processRole"

goFn :: K3 Expression
goFn = EC.variable goId

parseArgsId :: Identifier
parseArgsId = "parseArgs"

parseArgsFn :: K3 Expression
parseArgsFn   = EC.variable parseArgsId


{- Helpers -}
children :: K3 a -> [K3 a]
children (Node _ x) = x

replace_children :: K3 a -> [K3 a] -> K3 a
replace_children (Node n _) nc = Node n nc


{- Desugaring methods -}
-- TODO: replace with Template Haskell

desugarRoleEntries :: K3 Declaration
                      -> [(Identifier, ([Identifier], Identifier, Maybe (K3 Expression)))]
                      -> [(Identifier, Identifier)]
                      -> K3 Declaration
desugarRoleEntries (Node t c) sf df  = Node t $ c ++ (startFn sf df)
  where startFn sf df = [DC.global goId unitFnT (Just $ mkBody sf df)]
        mkBody sf df  = EC.lambda "_" $ uncurry (foldl dispatchId) $ defaultAndRestIds sf df
        
        defaultAndRestIds sf df = (defaultE sf $ lookup "" df, sf)
        dispatchId elseE (n,(_,y,goE))  = EC.ifThenElse (eqRole y) (runE n goE) elseE
        
        eqRole n = EC.binop OEqu (EC.variable "role") (EC.constant $ CString n)

        runE _ (Just goE) = goE
        runE n Nothing    = EC.applyMany (EC.variable $ cpName n) [EC.unit]

        defaultE sf (Just x) = case find ((x ==) . second . snd) sf of
                                Just (n,(_,_,goE)) -> runE n goE
                                Nothing -> EC.unit
        defaultE _ Nothing   = EC.unit

        second (_,x,_) = x
        
        unitFnT = TC.function TC.unit TC.unit


{- Code generation methods-}
-- TODO: replace with Template Haskell

channelMethods :: Bool -> K3 Expression -> K3 Expression -> Identifier -> K3 Type
                  -> [K3 Declaration]
channelMethods isFile argE formatE n t  =
    (map (mkMethod n)
      [mkInit argE formatE n, mkStart n, mkFinal n,
       sourceHasNext, sourceNext t])
    ++ [sourceController n]

  where mkMethod n (m,t,eOpt) = DC.global (n++m) (TC.function TC.unit t) eOpt

        mkInit argE formatE n = 
          ("Init", TC.unit, Just $ 
            EC.applyMany openFn [sourceId n, argE, formatE])

        mkStart n = ("Start", TC.unit, Just $ startE n)
        mkFinal n = ("Final", TC.unit, Just $ EC.applyMany closeFn [sourceId n])

        sourceController n = DC.trigger (ccName n) TC.unit $
          EC.lambda "_"
            (EC.ifThenElse
              (EC.applyMany (EC.variable $ chName n) [EC.unit])
              (controlE $ EC.applyMany (EC.variable $ cpName n) [EC.unit])
              EC.unit)

        -- External functions
        sourceHasNext = ("HasNext", TC.bool, Nothing)
        sourceNext t  = ("Next", t, Nothing)

        (openFn, closeFn) = if isFile then (openFileFn, closeFileFn)
                                      else (openSocketFn, closeSocketFn)

        startE n =
          if isFile
          then EC.send (EC.variable (ccName n)) myAddr EC.unit
          else EC.applyMany registerSocketDataTriggerFn [sourceId n, EC.variable $ ccName n]

        controlE processE =
          if not isFile
          then processE
          else EC.block [processE, (EC.send (EC.variable $ ccName n) myAddr EC.unit)]  

        sourceId n = string n
        string n = EC.constant $ CString n

-- | Rewrites a source declaration's process method to access and dispatch the next available event to all its bindings
rewriteSource :: [(Identifier, Identifier)] -> K3 Declaration -> K3 Declaration
rewriteSource bindings d
  | DGlobal src t eOpt <- tag d
  , Just _ <- lookup src bindings
  , TSource <- tag t
  = replace_children d $ (children d) ++ [mkProcessFn bindings src eOpt]

  | otherwise = d

-- | Constructs a top-level dispatch function for a source.
mkProcessFn :: [(Identifier, Identifier)] -> Identifier -> Maybe (K3 Expression) -> K3 Declaration
mkProcessFn bindings n eOpt = 
    DC.global (cpName n) unitFnT (Just $ body bindings n eOpt)
  
  where body bindings n eOpt =
          EC.lambda "_" $ EC.applyMany (processFnE bindings n) [nextE n eOpt]
        
        processFnE bindings n = EC.lambda "next" $ EC.block $
          map (\(_,dest) -> sendNextE dest) $ filter ((n ==) . fst) bindings
        
        nextE _ (Just e) = e
        nextE n Nothing  = EC.applyMany (EC.variable $ cnName n) [EC.unit]
        sendNextE dest   = EC.send (EC.variable dest) myAddr (EC.variable "next")
        unitFnT          = TC.function TC.unit TC.unit

-- | Constructs an top-level expression for running sources.
mkRunSourceE :: Identifier -> K3 Expression
mkRunSourceE n = EC.block [EC.applyMany (EC.variable $ ciName n) [EC.unit],
                           EC.applyMany (EC.variable $ csName n) [EC.unit]]


-- TODO: at_exit function body
-- TODO: stdin, stdout, stderr
declareBuiltins :: K3 Declaration -> K3 Declaration
declareBuiltins d
  | DRole defaultRoleName <- tag d = replace_children d new_children
  | otherwise = d
  where new_children = runtimeDecls ++ peerDecls ++ (children d) ++ topLevelDecls

        runtimeDecls = [
          DC.global parseArgsId (mkUnitFnT argT) Nothing,
          DC.global "openFile" (flip TC.function TC.unit $ TC.tuple [idT, TC.string, TC.string]) Nothing,
          DC.global "openSocket" (flip TC.function TC.unit $ TC.tuple [idT, TC.address]) Nothing,
          DC.global "closeFile" (TC.function idT TC.unit) Nothing,
          DC.global "closeSocket" (TC.function idT TC.unit) Nothing,
          DC.global "hasNext" (TC.function idT TC.bool) Nothing,
          DC.global "registerFileDataTrigger" (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerFileCloseTrigger" (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerSocketAcceptTrigger" (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerSocketDataTrigger" (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing,
          DC.global "registerSocketCloseTrigger" (flip TC.function TC.unit $ TC.tuple [idT, TC.trigger TC.unit]) Nothing ]

        peerDecls = [
          DC.global myId TC.address Nothing,
          DC.global peersId (TC.collection TC.address) Nothing,
          DC.global argsId argT Nothing,
          DC.global "role" TC.string (Just $ EC.constant $ CString "s1")]

        topLevelDecls = [
          DC.global initId unitFnT $ Just atInitE,
          DC.global exitId unitFnT $ Just atExitE ]

        atInitE = EC.lambda "_" $
          EC.block [--EC.assign argsId (EC.applyMany parseArgsFn [EC.unit]),
                    EC.applyMany goFn [EC.unit]]

        atExitE = EC.lambda "_" $ EC.tuple []

        idT = TC.string
        argT = TC.tuple [TC.collection TC.string,
                         TC.collection $ TC.tuple [TC.string, TC.string]]

        mkUnitFnT rt = TC.function TC.unit rt
        unitFnT = TC.function TC.unit TC.unit
