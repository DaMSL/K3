{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Endpoint where

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

import Language.K3.Codegen.CPP.Preprocessing
import Language.K3.Codegen.CPP.Expression
import Language.K3.Codegen.CPP.Primitives
import Language.K3.Codegen.CPP.Types

import qualified Language.K3.Codegen.CPP.Representation as R

storage :: R.Statement
storage = R.Forward $ R.ScalarDecl (R.Name "__storage_") (R.Reference R.Inferred)
                    (Just $ R.Call (R.Project (R.Variable $ R.Name "__engine_") (R.Name "getStorageManager")) [])
unit_t :: R.Type
unit_t = R.Named $ R.Name "unit_t"

send_unit :: Identifier -> R.Expression
send_unit tid =
  let mknative typ val = R.Call
                           (R.Variable $ R.Qualified (R.Name "std") (R.Specialized [R.Named $ R.Specialized [typ] (R.Name "TNativeValue")] (R.Name "make_unique")))
                           [val]
  in let v = mknative (unit_t) (R.Initialization (unit_t) [])
  in let header = R.Call (R.Variable $ R.Name "MessageHeader") [R.Variable $ R.Name "me", R.Variable $ R.Name "me", R.Variable $ R.Name tid]
  in let codec = R.Call (R.Variable $ R.Qualified (R.Name "Codec") (R.Specialized [unit_t] (R.Name "getCodec"))) [R.Variable $ R.Name "__internal_format_"]
  in  R.Call (R.Project (R.Variable $ R.Name "__engine_") (R.Name "send")) [header, v, codec]

initEndpoint :: String -> Bool -> EndpointSpec -> CPPGenM [R.Definition]
initEndpoint _ _ ValueEP = return []
initEndpoint _ _ (BuiltinEP _ _) = return []
initEndpoint name' isSource (FileEP path isText format) = do
  let name = unmangleReservedId name'
  let args = [ R.Variable $ R.Name "me"
             , R.Literal $ R.LString name
             , R.Variable $ R.Name $ path -- Not a literal string, because path already contains quotes
             , R.Variable $ R.Qualified (R.Name "StorageFormat") (R.Name $ if isText then "Text" else "Binary")
             , R.Call (R.Variable $ R.Qualified (R.Name "Codec") (R.Name "getFormat")) [R.Literal $ R.LString format]
             , R.Variable $ R.Qualified (R.Name "IOMode") (R.Name $ if isSource then "Read" else "Write")
             ]
  let open = R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "__storage_") (R.Name "openFile")) args
  let ret = R.Return $ R.Initialization (unit_t) []
  return $ [R.FunctionDefn (R.Name $ name ++ "Init") [("_", unit_t)] (Just $ unit_t) [] False [storage, open, ret]]
initEndpoint _ _ (NetworkEP _ _ _) = return []

startSource :: String -> CPPGenM [R.Definition]
startSource name' = do
  let name = unmangleReservedId name'
  let controllerTid = "__" ++ name ++ "Controller_tid"
  let send = R.Ignore $ send_unit controllerTid
  let ret = R.Return $ R.Initialization (unit_t) []
  return $ [R.FunctionDefn (R.Name $ name ++ "Start") [("_", unit_t)] (Just $ unit_t) [] False [send, ret]]

hasRead :: String -> CPPGenM [R.Definition]
hasRead source'  = do
    return []
    --let source = unmangleReservedId source'
    --let s_has_r = R.Project (R.Variable $ R.Name "__storage_") (R.Name "hasRead")
    --let ret = R.Return $ R.Call s_has_r [R.Variable $ R.Name "me", R.Literal $ R.LString source]
    --return $ [R.FunctionDefn (R.Name $ source ++ "HasRead") [("_", unit_t)]
    --  (Just $ R.Primitive R.PBool) [] False [storage, ret]]

doRead :: String -> K3 Type -> CPPGenM [R.Definition]
doRead source' typ = do
    return []
    --ret_type    <- genCType $ typ
    --let source = unmangleReservedId source'
    --let packed_dec = R.Forward $ R.ScalarDecl (R.Name "packed") R.Inferred $ Just $
    --                   (R.Call (R.Project (R.Variable $ R.Name "__storage_") (R.Name "doRead"))
    --                           [R.Variable $ R.Name "me", R.Literal $ R.LString source])
    --let codec = R.Forward $ R.ScalarDecl (R.Name "__codec_") R.Inferred $ Just $
    --              R.Call (R.Variable $ R.Specialized [ret_type] (R.Qualified (R.Name "Codec") (R.Name "getCodec")))
    --                [R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "packed") (R.Name "format")) []]
    --let unpacked = R.Call (R.Project (R.Dereference $ R.Variable $ R.Name "__codec_") (R.Name "unpack")) [R.Dereference $ R.Variable $ R.Name "packed"]
    --let return_stmt = R.IfThenElse (R.Variable $ R.Name "packed")
    --                    [R.Return $ R.Dereference $
    --                      R.Call (R.Project (R.Dereference unpacked) (R.Specialized [ret_type] (R.Name "as"))) []
    --                    ]
    --                    [R.Ignore $ R.ThrowRuntimeErr $ R.Literal $ R.LString $ "Invalid doRead for " ++ source]
    --return $ [R.FunctionDefn (R.Name $ source ++ "Read") [("_", unit_t)]
    --  (Just ret_type) [] False ([storage, packed_dec, codec, return_stmt])]

hasWrite :: String -> CPPGenM [R.Definition]
hasWrite sink'  = do
    return []
    --let sink = unmangleReservedId sink'
    --let s_has_r = R.Project (R.Variable $ R.Name "__storage_") (R.Name "hasWrite")
    --let ret = R.Return $ R.Call s_has_r [R.Variable $ R.Name "me", R.Literal $ R.LString sink]
    --return $ [R.FunctionDefn (R.Name $ sink ++ "HasWrite") [("_", unit_t)]
    --  (Just $ R.Primitive R.PBool) [] False [storage, ret]]

doWrite :: R.Expression -> String -> K3 Type -> CPPGenM [R.Definition]
doWrite fmt sink' typ = do
    return []
    --let sink = unmangleReservedId sink'
    --elem_type <- genCType $ typ
    --let codec = R.Forward $ R.ScalarDecl (R.Name "__codec_") R.Inferred $ Just $
    --              R.Call (R.Variable $ R.Specialized [elem_type] (R.Qualified (R.Name "Codec") (R.Name "getCodec")))
    --                [fmt]
    --let write = R.Ignore $ R.Call (R.Project (R.Variable $ R.Name "__storage_") (R.Name "doWrite"))
    --                           [R.Variable $ R.Name "me", R.Literal $ R.LString sink, R.Variable $ R.Name "arg", fmt]
    --let return_stmt = R.Return $ R.Initialization unit_t []
    --return $ [R.FunctionDefn (R.Name $ sink ++ "Write") [("arg", elem_type)]
    --  (Just unit_t) [] False ([storage, codec, write, return_stmt])]

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
endpoint n' t isSrc spec@(FileEP _ _ _) = do
  let n = unmangleReservedId n'
  -- Common
  init <- initEndpoint n isSrc spec
  -- Sources
  start <- if isSrc then startSource n else return []
  hasR <- if isSrc then hasRead n else return []
  doR <- if isSrc then doRead n t else return []
  -- Sinks
  let fmt = case spec of
              (FileEP _ _ x) -> x
              _ -> error "Sink missing FileEP specification"
  hasW  <- if not isSrc then hasWrite n else return []
  write <- if not isSrc then doWrite (R.Call (R.Variable $ R.Qualified (R.Name "Codec") (R.Name "getFormat")) [R.Literal $ R.LString fmt]) n t else return []
  -- Return
  return $ init ++ start ++ hasR ++ doR ++ hasW ++ write
endpoint _ _ _ (NetworkEP _ _ _) = error "Network sources/sinks not yet supported"

