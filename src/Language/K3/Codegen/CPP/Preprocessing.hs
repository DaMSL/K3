{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Preprocessing where

import Control.Arrow ((&&&))

import Data.Functor
import Data.Maybe
import Data.Tree

import Language.K3.Analysis.CArgs

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

mangleReservedId :: Identifier -> Identifier
mangleReservedId i | i `elem` cppReservedNames = mangleName i
mangleReservedId i = i

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

flattenApplicationD :: K3 Declaration -> K3 Declaration
flattenApplicationD (Node (DGlobal i t me :@: as) cs)
    = Node (DGlobal i t (flattenApplicationE <$> me) :@: as) (map flattenApplicationD cs)
flattenApplicationD (Node (DTrigger i t e :@: as) cs)
    = Node (DTrigger i t (flattenApplicationE e) :@: as) (map flattenApplicationD cs)
flattenApplicationD (Node t cs) = Node t (map flattenApplicationD cs)

flattenApplicationE :: K3 Expression -> K3 Expression
flattenApplicationE e@(tag &&& annotations &&& children -> (EOperate OApp, (as, [f, a])))
    = case flattened of
        (tag &&& children -> (EOperate OApp, cs)) ->
            if (isJust $ flattened @~ isECArgs)
              then Node (EOperate OApp :@: as) (cs ++ [a])
              else Node (EOperate OApp :@: as) [flattened, a]
        _ -> Node (EOperate OApp :@: as) [flattened, a]
  where flattened = flattenApplicationE f
flattenApplicationE e = e

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
