-- | Constructors for K3 Types. This module should almost certainly be imported qualified.
module Language.K3.Core.Constructor.Type (
    bool,
    byte,
    int,
    real,
    string,
    unit,
    option,
    indirection,
    tuple,
    record,
    collection,
    function,
    address,
    source,
    sink,
    trigger,
    builtIn,
    forAll,
    declaredVar,
    top,
    bottom,
    recordExtension,
    declaredVarOp
) where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

leaf :: Type -> K3 Type
leaf typeTag = Node (typeTag :@: []) []

-- | Boolean type.
bool :: K3 Type
bool = leaf TBool

-- | Byte type.
byte :: K3 Type
byte = leaf TByte

-- | Integer type.
int :: K3 Type
int = leaf TInt

-- | Decimal type.
real :: K3 Type
real = leaf TReal

-- | String type.
string :: K3 Type
string = leaf TString

-- | Unit type.
unit :: K3 Type
unit = tuple []

-- | Option type, based on the underlying type.
option :: K3 Type -> K3 Type
option t = Node (TOption :@: []) [t]

-- | Indirection type, based on the underlying type.
indirection :: K3 Type -> K3 Type
indirection t = Node (TIndirection :@: []) [t]

-- | Tuple type, based on the element types.
tuple :: [K3 Type] -> K3 Type
tuple = Node (TTuple :@: [])

-- | Record type, based on field labels and types.
record :: [(Identifier, K3 Type)] -> K3 Type
record idts = Node (TRecord ids :@: []) ts where (ids, ts) = unzip idts

-- | Collection type, based on the element record type.
collection :: K3 Type -> K3 Type
collection rt = Node (TCollection :@: []) [rt]

-- | Function type, based on the argument type and return type.
function :: K3 Type -> K3 Type -> K3 Type
function a r = Node (TFunction :@: []) [a, r]

address :: K3 Type
address = leaf TAddress

source :: K3 Type -> K3 Type
source i = Node (TSource :@: []) [i]

sink :: K3 Type -> K3 Type
sink at = Node (TSink :@: []) [at]

trigger :: K3 Type -> K3 Type
trigger at = Node (TTrigger :@: []) [at]

builtIn :: TypeBuiltIn -> K3 Type
builtIn bi = Node (TBuiltIn bi :@: []) []

forAll :: [TypeVarDecl] -> K3 Type -> K3 Type
forAll vdecls t = Node (TForall vdecls :@: []) [t]

declaredVar :: Identifier -> K3 Type
declaredVar i = Node (TDeclaredVar i :@: []) []

top :: K3 Type
top = leaf TTop

bottom :: K3 Type
bottom = leaf TBottom

recordExtension :: [(Identifier, K3 Type)] -> [Identifier] -> K3 Type
recordExtension idts ids' = Node (TRecordExtension ids ids' :@: []) ts
  where (ids, ts) = unzip idts

declaredVarOp :: [Identifier] -> TypeVariableOperator -> K3 Type
declaredVarOp ids op = Node (TDeclaredVarOp ids op :@: []) []
