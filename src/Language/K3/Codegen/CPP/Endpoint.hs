{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Endpoint where

import Debug.Trace

import Control.Arrow ((&&&))
import Control.Applicative
import Control.Monad.State

import Data.Maybe

import qualified Data.List as L
import qualified Data.Map as M

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Codegen.CPP.Expression
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import qualified Language.K3.Codegen.CPP.Representation as R

storage :: R.Statement
storage = R.Forward $ R.ScalarDecl (R.Name "__storage_") R.Inferred 
                    (Just $ R.Call (R.Project (R.Variable $ R.Name "__engine_") (R.Name "getStorageManager")) [])
unit_t :: R.Type
unit_t = R.Named $ R.Name "unit_t"

send_unit :: Identifier -> R.Expression
send_unit tid = 
  let mknative typ val = R.Call 
                           (R.Variable $ R.Qualified (R.Name "std") (R.Specialized [R.Named $ R.Specialized [typ] (R.Name "TNativeValue")] (R.Name "make_shared")))
                           [val]
  in let v = mknative (unit_t) (R.Initialization (unit_t) [])
  in let header = R.Call (R.Variable $ R.Name "MessageHeader") [R.Variable $ R.Name "me", R.Variable $ R.Name "me", R.Variable $ R.Name tid]
  in let codec = R.Call (R.Variable $ R.Qualified (R.Name "Codec") (R.Specialized [unit_t] (R.Name "getCodec"))) [R.Variable $ R.Name "__internal_format_"]
  in  R.Call (R.Project (R.Variable $ R.Name "__engine_") (R.Name "send")) [header, v, codec]

initEndpoint :: String -> Bool -> EndpointSpec -> CPPGenM [R.Definition]
initEndpoint _ _ ValueEP = return []
initEndpoint _ _ (BuiltinEP _ _) = return []
initEndpoint name isSource (FileEP path isText format) = do
  let args = [ R.Variable $ R.Name "me"
             , R.Literal $ R.LString name
             , R.Variable $ R.Name $ path -- Not a literal string, because path already contains quotes
             , R.Variable $ R.Qualified (R.Name "StorageFormat") (R.Name $ if isText then "Text" else "Binary")
             , R.Call (R.Variable $ R.Qualified (R.Name "Codec") (R.Name "getFormat")) [R.Literal $ R.LString format]
             , R.Variable $ R.Qualified (R.Name "IOMode") (R.Name $ if isSource then "Read" else "Write") 
             ] 
  let open = R.Ignore $ R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "__storage_") (R.Name "openFile")) args  
  let ret = R.Return $ R.Initialization (unit_t) []
  return $ [R.FunctionDefn (R.Name $ name ++ "Init") [("_", unit_t)] (Just $ unit_t) [] False [storage, open, ret]]
initEndpoint _ _ (NetworkEP _ _ _) = return []

startSource :: String -> CPPGenM [R.Definition]
startSource name = do
  let controllerTid = "__" ++ name ++ "Controller_tid"
  let send = R.Ignore $ send_unit controllerTid
  let ret = R.Return $ R.Initialization (unit_t) []
  return $ [R.FunctionDefn (R.Name $ name ++ "Start") [("_", unit_t)] (Just $ unit_t) [] False [send, ret]]

-- TODO remove the original versions, move sink/source generators to new module?
hasRead :: String -> CPPGenM [R.Definition]
hasRead source  = do
    let s_has_r = R.Project (R.Dereference $ R.Variable $ R.Name "__storage_") (R.Name "hasRead")
    let ret = R.Return $ R.Call s_has_r [R.Variable $ R.Name "me", R.Literal $ R.LString source]
    return $ [R.FunctionDefn (R.Name $ source ++ "HasRead") [("_", unit_t)]
      (Just $ R.Primitive R.PBool) [] False [storage, ret]]

doRead :: String -> K3 Type -> CPPGenM [R.Definition]
doRead source typ = do
    ret_type    <- genCType $ typ
    let packed_dec = R.Forward $ R.ScalarDecl (R.Name "packed") R.Inferred $ Just $
                       (R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "__storage_") (R.Name "doRead"))
                               [R.Variable $ R.Name "me", R.Literal $ R.LString source])
    let codec = R.Forward $ R.ScalarDecl (R.Name "__codec_") R.Inferred $ Just $
                  R.Call (R.Variable $ R.Specialized [ret_type] (R.Qualified (R.Name "Codec") (R.Name "getCodec")))
                    [R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "packed") (R.Name "format")) []]
    let unpacked = R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "__codec_") (R.Name "unpack")) [R.Dereference $ R.Variable $ R.Name "packed"]
    let return_stmt = R.IfThenElse (R.Variable $ R.Name "packed")
                        [R.Return $ R.Dereference $ 
                          R.Call (R.Project (R.Dereference unpacked) (R.Specialized [ret_type] (R.Name "as"))) []
                        ]
                        [R.Ignore $ R.ThrowRuntimeErr $ R.Literal $ R.LString $ "Invalid doRead for " ++ source]
    return $ [R.FunctionDefn (R.Name $ source ++ "Read") [("_", unit_t)]
      (Just ret_type) [] False ([storage, packed_dec, codec, return_stmt])]

doWrite :: R.Expression -> String -> K3 Type -> CPPGenM [R.Definition]
doWrite fmt sink typ = do
    elem_type <- genCType $ typ
    let codec = R.Forward $ R.ScalarDecl (R.Name "__codec_") R.Inferred $ Just $
                  R.Call (R.Variable $ R.Specialized [elem_type] (R.Qualified (R.Name "Codec") (R.Name "getCodec")))
                    [fmt]
    let native = R.Call (R.Variable $ R.Specialized [elem_type] (R.Name "NativeValue")) [R.Call (R.Variable $ R.Qualified (R.Name "std") (R.Name "move")) [R.Variable $ R.Name "arg"]]
    let packed = R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "__codec_") (R.Name "pack")) [native]
    let write = R.Ignore $ R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "__storage_") (R.Name "doWrite"))
                               [R.Variable $ R.Name "me", packed]
    let return_stmt = R.Return $ R.Initialization unit_t []
    return $ [R.FunctionDefn (R.Name $ sink ++ "Write") [("arg", elem_type)]
      (Just unit_t) [] False ([storage, write, return_stmt])]

controller :: String -> CPPGenM [R.Definition]
controller name = do
  let cName = name ++ "Controller"
  let hr = R.Call (R.Variable $ R.Name $ name ++ "HasRead") [R.Initialization unit_t []]
  let process = R.Ignore $ R.Call (R.Variable $ R.Name $ name ++ "Process") [R.Initialization unit_t []]
  let send = R.Ignore $ send_unit $ "__" ++ cName ++ "_tid"
  let ret = R.Return $ R.Initialization unit_t []
  let body = R.IfThenElse hr [process, send, ret] [ret]
  let func = R.FunctionDefn (R.Name (name ++ "ControllerFn")) [("_", unit_t)] (Just unit_t) [] False [body]
  return [func]

epDetails :: [Annotation Declaration] -> EndpointSpec
epDetails as =
  let syntax = filter isDSyntax as in
  getSpec syntax
  where
    getSpec [(DSyntax (EndpointDeclaration spec _))] = spec
    getSpec _ = error "Source/Sink missing EndpointSpec"

endpoint :: String -> K3 Type -> Bool -> EndpointSpec -> CPPGenM [R.Definition]
endpoint _ _ _ ValueEP = return []
endpoint _ _ _ (BuiltinEP _ _) = error "Builtin sources/sinks (stdin, stdout, stderr) not yet supported"
endpoint n t isSrc spec@(FileEP _ _ _) = do
  -- Common
  init <- trace "FILE EP" initEndpoint n isSrc spec 
  -- Sources
  start <- if isSrc then startSource n else return []
  cont <- if isSrc then controller n else return []
  hasR <- if isSrc then hasRead n else return []
  doR <- if isSrc then doRead n t else return []
  -- Sinks
  let fmt = case spec of 
              (FileEP _ _ x) -> x
              _ -> error "Sink missing FileEP specification"
  write <- if not isSrc then doWrite (R.Call (R.Variable $ R.Qualified (R.Name "Codec") (R.Name "getFormat")) [R.Literal $ R.LString fmt]) n t else return []
  -- Return
  return $ init ++ start ++ hasR ++ doR ++ cont ++ write
endpoint _ _ _ (NetworkEP _ _ _) = error "Network sources/sinks not yet supported"

