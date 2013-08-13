{-# LANGUAGE MultiParamTypeClasses, TemplateHaskell, FlexibleInstances, FunctionalDependencies, TypeFamilies, UndecidableInstances #-}

{-|
  This module defines a generic @Transform@ typeclass which allows general
  transformations over a data type.  It also provides Template Haskell routines
  to generate naturally homomorphic boilerplate instances for data types.
  
  An example of use is as follows:
  
  @
    -- Data structure describing the transformation.
    data MyTransformation = MyTransformation Int
    -- Homomorphic instances
    $(concat <$> mapM (defineHomInstance ''MyTransformation)
                  [''Foo, ''Bar])
    -- Common instances
    $(defineCommonHomInstances ''MyTransformation)
    -- Special case
    $(defineHomFunc ''MyTransformation ''Baz $ mkName "homBaz")
    instance Transform Baz MyTransformation where
      transform (MyTransformation n) baz = case baz of
        Baz5 m -> Baz5 $ n + m
        _ -> homBaz baz
  @
  
  In this example, we assume that Foo, Bar, and Baz are data structures which
  contain each other in some fashion.  Each may have any number of constructors.
  The above code defines transformation through Foo and Bar to be naturally
  homomorphic; the data are disassembled, the arguments are transformed, and
  the data are reassembled.  The common instances allow for cases such as Int
  and Set to be defined with minimal boilerplate.  The Baz case is special: if
  the constructor Baz5 is used, it is reconstructed with a value @n@ higher than
  its previous value; all other Baz constructors are naturally homomorphic.
  Note that this special case applies to all instances of Baz5 throughout the
  transformed structure.
-}
module Language.K3.TemplateHaskell.Transform
( Transform(..)
, defineHomInstance
, defineHomFunc
, defineCommonHomInstances
, defineTransformIdentityInstance
, defineTransformFunctorInstance
, defineCommonTupleTransformInstance
, defineCommonPrimitiveTransformIdentityInstances
) where

import Control.Applicative
import Data.Set (Set)
import qualified Data.Set as Set
import Language.Haskell.TH

import Language.K3.TemplateHaskell.Utils

class Transform t d where
  transform :: t -> d -> d
  
-- |A Template Haskell mechanism for defining a transformation instance for
--  the @Transform@ typeclass at a given type declaration.
defineHomInstance :: Name -> Name -> Q [Dec]
defineHomInstance tname dname = do
  -- We're going to defer to the defineHomFunc function by picking a fresh
  -- function name and using it in the type declaration.
  fname <- newName $ "transformFn_" ++ nameBase tname ++ "_" ++
              nameBase dname
  (dtyp,_) <- canonicalType dname
  (ttyp,_) <- canonicalType tname
  (funcDecls,funcCxt) <- defineHomFuncX fname tname dname
  transformBodyDecls <- sequence [
      funD 'transform
        [clause [] (normalB $ varE fname) []]
    ]
  transformDecl <- instanceD
                      (return funcCxt)
                      (return $ applyTypeCon
                        (ConT ''Transform)
                        [ttyp, dtyp]) $
                      map return transformBodyDecls
  return $ funcDecls ++ [transformDecl]

-- |A Template Haskell mechanism for defining a function which performs a
--  purely structural transformation on a given data type.
defineHomFunc :: Name -> Name -> Name -> Q [Dec]
defineHomFunc fname tname dname = do
  (decs,_) <- defineHomFuncX fname tname dname
  return decs

-- |The actual implementation of @defineHomFunc@.  This implementation exposes
--  some specific information about the declarations which were constructed
--  as well as the declarations themselves.
defineHomFuncX :: Name -> Name -> Name -> Q ([Dec],Cxt)
defineHomFuncX fname tname dname = do
  info <- reify dname
  fnDef <- makeFnDef info
  (fnSig,fcxt) <- makeFnSig info
  return (fnDef ++ fnSig, fcxt)
  where
    -- |Creates the type signature for the transformation function.
    makeFnSig :: Info -> Q ([Dec],Cxt)
    makeFnSig info = do
      (dtyp,dbndrs) <- canonicalType dname
      (ttyp,tbndrs) <- canonicalType tname
      let mkPred typ = classP ''Transform [return ttyp, typ]
      argTyps <- getDataArgTypes info
      let preds = map (mkPred . return) argTyps
      fcxt <- cxt preds
      let typ = forallT (dbndrs ++ tbndrs) (return fcxt) $
                  return $
                    mkFnType [ ttyp
                             , dtyp
                             , dtyp ]
      signature <- sigD fname typ
      return ([signature], fcxt)
    -- |Creates the definition for the transformation function.
    makeFnDef :: Info -> Q [Dec]
    makeFnDef info = do
      tagName <- newName "_t"
      valName <- newName "x"
      let branches = case info of
            TyConI dec -> case dec of
              DataD _ _ _ cons _ ->
                map (makeBranch tagName) cons
              NewtypeD _ _ _ con _ ->
                [makeBranch tagName con]
              _ -> error $ "defineHomFunc: " ++ show dname
                      ++ " is not a data decl"
            _ -> error $ "defineHomFunc: " ++ show dname
                  ++ " is not a type constructor"
      let body = normalB $ caseE (varE valName) branches
      sequence [funD fname [clause [varP tagName, varP valName]
                  body []]]
    makeBranch :: Name -> Con -> Q Match
    makeBranch tagName con =
      case con of
        NormalC cname typs -> do
          -- get some names for the argument variables
          argNames <- mapM (newName . ("x" ++) . show) [1..(length typs)]
          -- the pattern is now pretty straightforward
          let pattern = conP cname $ map varP argNames
          -- the body is slightly more involved
          let trans nm = [|transform $(varE tagName) $(varE nm)|]
          let e = foldl appE (conE cname) $ map trans argNames
          match pattern (normalB e) []
        _ -> error $ "defineHomFunc: " ++ show con
              ++ " is not a normal constructor"

-- |Creates a common identity instance for @Transform@.
defineTransformIdentityInstance :: Name -> Name -> Q [Dec]
defineTransformIdentityInstance tname dname = do
  (ttyp,_) <- canonicalType tname
  (dtyp,_) <- canonicalType dname
  [d| instance Transform $(return ttyp) $(return dtyp) where
        transform _ = id |]

-- |Creates a common instance for @Transform@ for @Functor@ data.
defineTransformFunctorInstance :: Name -> Q Type -> Q [Dec]
defineTransformFunctorInstance tname funcType = do
  (ttyp,_) <- canonicalType tname
  [d| instance (Transform $(return ttyp) a)
            => Transform $(return ttyp) ($(funcType) a) where
        transform t d = fmap (transform t) d |]
        
-- |Creates identity @Transform@ instances for common primitives.
defineCommonPrimitiveTransformIdentityInstances :: Name -> Q [Dec]
defineCommonPrimitiveTransformIdentityInstances tname =
  concat <$> mapM (defineTransformIdentityInstance tname)
    [ ''Int
    , ''Integer
    , ''Char
    , ''Bool
    ]
    
defineCommonTupleTransformInstance :: Name -> Int -> Q [Dec]
defineCommonTupleTransformInstance tname n = do
  (ttyp,_) <- canonicalType tname
  paramNames <- sequence $ take n $ mkPrefixNames "tupleParam"
  let tupleType = applyTypeCon (TupleT n) $ map VarT paramNames
  argNames <- sequence $ take n $ mkPrefixNames "tupleArg"
  let transName = mkName "_t"
  let tupleName = mkName "tuple"
  let body =  tupE $ map (\nm -> [|transform $(varE transName) $(varE nm)|])
                      argNames
  let expr = caseE (varE tupleName) $ [match (tupP $ map varP argNames)
                (normalB body) []]
  let context = cxt $ map (\v -> classP (''Transform) [return ttyp, varT v])
                        paramNames
  let instD = 
        instanceD context (applyTypeCon (ConT ''Transform) <$>
                              sequence [return ttyp, return tupleType])
          [funD 'transform [clause [varP transName, varP tupleName]
                          (normalB expr) []]]
  sequence [instD]

{- |Creates instances for homorphic transforms over common cases:
      * n-tuples for n==0 or 2 <= n <= 8
      * primitives (Int, Integer, Char, Bool)
      * @Either@
      * @Maybe@
      * @[]@
      * @Set@
-}    
defineCommonHomInstances :: Name -> Q [Dec]
defineCommonHomInstances tname = do
  (ttyp,_) <- canonicalType tname
  let litDecls =
        [ [d|
            instance (Ord a, Transform $(return ttyp) a)
                  => Transform $(return ttyp) (Set a) where
              transform t s = Set.map (transform t) s
          |]
        ]
  let insts = [ defineCommonPrimitiveTransformIdentityInstances tname
              ] ++ map (defineCommonTupleTransformInstance tname) (0:[2..8])
                ++ map (defineHomInstance tname) [''Either,''Maybe]
                ++ map (defineTransformFunctorInstance tname)
                        [ [t| [] |] ]
                ++ litDecls
  concat <$> sequence insts
