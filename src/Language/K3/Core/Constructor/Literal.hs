module Language.K3.Core.Constructor.Literal where

import Data.Tree
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Literal
import Language.K3.Core.Type

bool :: Bool -> K3 Literal
bool b = Node (LBool b :@: []) []

byte :: Word8 -> K3 Literal
byte b = Node (LByte b :@: []) []

int :: Int -> K3 Literal
int i = Node (LInt i :@: []) []

real :: Double -> K3 Literal
real r = Node (LReal r :@: []) []

string :: String -> K3 Literal
string s = Node (LString s :@: []) []

some :: K3 Literal -> K3 Literal
some l = Node (LSome :@: []) [l]

none :: NoneMutability -> K3 Literal
none m = Node (LNone m :@: []) []

indirect :: K3 Literal -> K3 Literal
indirect l = Node (LIndirect :@: []) [l]

tuple :: [K3 Literal] -> K3 Literal
tuple fields = Node (LTuple :@: []) fields

record :: [(Identifier, K3 Literal)] -> K3 Literal
record ls = Node (LRecord ids :@: []) elems where (ids, elems) = unzip ls

empty :: K3 Type -> K3 Literal
empty t = Node (LEmpty t :@: []) []

collection :: K3 Type -> [K3 Literal] -> K3 Literal
collection t ls = Node (LCollection t :@: []) ls

address :: K3 Literal -> K3 Literal -> K3 Literal
address h p = Node (LAddress :@: []) [h, p]
