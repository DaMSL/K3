module Language.K3.Codegen.CPP.Preprocessing where

import Data.Functor
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

mangleReservedNames :: K3 Declaration -> K3 Declaration
mangleReservedNames (Node (DGlobal i t me :@: as) cs)
    | i `elem` cppReservedNames = Node (DGlobal (mangleName i) t mme :@: as) mcs
    | otherwise = Node (DGlobal i t mme :@: as) (map mangleReservedNames cs)
  where
    mme = mangleReservedNames' <$> me
    mcs = mangleReservedNames <$> cs

mangleReservedNames (Node (DTrigger i t e :@: as) cs)
    | i `elem` cppReservedNames = Node (DTrigger (mangleName i) t mme :@: as) mcs
    | otherwise = Node (DTrigger i t mme :@: as) mcs
  where
    mme = mangleReservedNames' e
    mcs = mangleReservedNames <$> cs

mangleReservedNames (Node t cs) = Node t (mangleReservedNames <$> cs)

mangleReservedNames' :: K3 Expression -> K3 Expression
mangleReservedNames' (Node (EVariable i :@: as) cs)
    | i `elem` cppReservedNames = Node (EVariable (mangleName i) :@: as) mcs
    | otherwise = Node (EVariable i :@: as) mcs
  where
    mcs = mangleReservedNames' <$> cs
mangleReservedNames' (Node (ERecord is :@: as) cs)
    = Node (ERecord [if i `elem` cppReservedNames then mangleName i else i | i <- is] :@: as) mcs
  where
    mcs = mangleReservedNames' <$> cs
mangleReservedNames' (Node (ELambda i :@: as) cs)
    | i `elem` cppReservedNames = Node (ELambda (mangleName i) :@: as) mcs
    | otherwise = Node (ELambda i :@: as) mcs
  where
    mcs = mangleReservedNames' <$> cs
mangleReservedNames' (Node (ELetIn i :@: as) cs)
    | i `elem` cppReservedNames = Node (ELetIn (mangleName i) :@: as) mcs
    | otherwise = Node (ELetIn i :@: as) mcs
  where
    mcs = mangleReservedNames' <$> cs
mangleReservedNames' (Node (ECaseOf i :@: as) cs)
    | i `elem` cppReservedNames = Node (ECaseOf (mangleName i) :@: as) mcs
    | otherwise = Node (ECaseOf i :@: as) mcs
  where
    mcs = mangleReservedNames' <$> cs
mangleReservedNames' (Node (EBindAs b :@: as) cs)
    = Node (EBindAs (mangleReservedNames'' b) :@: as) mcs
  where
    mcs = mangleReservedNames' <$> cs
mangleReservedNames' (Node t cs) = Node t mcs
  where
    mcs = mangleReservedNames' <$> cs

mangleReservedNames'' :: Binder -> Binder
mangleReservedNames'' (BIndirection i)
    = BIndirection $ if i `elem` cppReservedNames then mangleName i else i
mangleReservedNames'' (BTuple is)
    = BTuple [if i `elem` cppReservedNames then mangleName i else i | i <- is]
mangleReservedNames'' (BRecord iis)
    = BRecord [(f, if i `elem` cppReservedNames then mangleName i else i) | (f, i) <- iis]

mangleName :: Identifier -> Identifier
mangleName s = s ++ "_"

cppReservedNames :: [Identifier]
cppReservedNames =
    [ "auto"
    , "asm"
    , "bool"
    , "break"
    , "case"
    , "catch"
    , "char"
    , "class"
    , "const"
    , "const_cast"
    , "continue"
    , "default"
    , "delete"
    , "do"
    , "double"
    , "dynamic_cast"
    , "else"
    , "enum"
    , "explicit"
    , "extern"
    , "false"
    , "float"
    , "for"
    , "friend"
    , "goto"
    , "if"
    , "inline"
    , "int"
    , "long"
    , "mutable"
    , "namespace"
    , "new"
    , "operator"
    , "private"
    , "protected"
    , "public"
    , "register"
    , "reinterpret_cast"
    , "return"
    , "short"
    , "shutdown"
    , "signed"
    , "sizeof"
    , "static"
    , "static_cast"
    , "struct"
    , "switch"
    , "template"
    , "this"
    , "throw"
    , "true"
    , "try"
    , "typedef"
    , "typeid"
    , "typename"
    , "union"
    , "unsigned"
    , "using"
    , "virtual"
    , "void"
    , "volatile"
    , "wchar_t"
    , "while"
    ]
