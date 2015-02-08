{-# LANGUAGE MultiParamTypeClasses, TemplateHaskell, FlexibleInstances, FunctionalDependencies, TypeFamilies, UndecidableInstances #-}

{-|
  This module defines a generic @TransformM@ typeclass which allows general
  monadic transformations over a data type.  It also provides Template Haskell
  routines to generate naturally homomorphic boilerplate instances for data
  types.  This typeclass is distinct from @Transform@ for reasons of performance
  and to simplify the underlying generation code.

  An example of use is as follows:

  @
    -- Data structure describing the transformation.
    data MyTransformation = MyTransformation Int
    -- Homomorphic instances
    $(concat <$> mapM (defineHomInstanceM ''MyTransformation)
                  [''Foo, ''Bar])
    -- Common instances
    $(defineCommonHomInstancesM ''MyTransformation)
    -- Special case
    $(defineHomFuncM ''MyTransformationM ''Baz $ mkName "homBaz")
    instance TransformM (Reader Int) Baz MyTransformation where
      transformM (MyTransformation n) baz = case baz of
        Baz5 m -> do
          k <- ask
          return $ Baz5 $ if m > k then m + n else m
        _ -> homBaz baz
  @

  In this example, we assume that Foo, Bar, and Baz are data structures which
  contain each other in some fashion.  Each may have any number of constructors.
  The above code defines transformation through Foo and Bar to be naturally
  homomorphic; the data are disassembled, the arguments are transformed, and
  the data are reassembled.  The common instances allow for cases such as Int
  and Set to be defined with minimal boilerplate.  The Baz case is special: if
  the constructor Baz5 is used, it may be reconstructed with a value @n@ higher
  than its previous value (depending on the value provided in the @Reader@
  environment); all other Baz constructors are naturally homomorphic.  Note that
  this special case applies to all instances of Baz5 throughout the transformed
  structure.
-}
module Language.K3.Utils.TemplateHaskell.TransformM
( TransformM(..)
, defineHomInstanceM
, defineHomFuncM
, defineCommonHomInstancesM
, defineTransformIdentityInstanceM
, defineTransformTraversableInstanceM
, defineCommonTupleTransformInstanceM
, defineCommonPrimitiveTransformIdentityInstancesM
) where

import Control.Applicative
import Control.Monad
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Traversable as Trav
import Language.Haskell.TH

import Language.K3.Utils.TemplateHaskell.Utils

-- |The typeclass which defines monadic transformations.
class (Monad m) => TransformM m t d where
  transformM :: t -> d -> m d

-- |A Template Haskell mechanism for defining a transformation instance for
--  the @TransformM@ typeclass at a given type declaration.
defineHomInstanceM :: Name -> Name -> Q [Dec]
defineHomInstanceM tname dname = do
  -- We're going to defer to the defineHomFunc function by picking a fresh
  -- function name and using it in the type declaration.
  fname <- newName $ "transformMFn_" ++ nameBase tname ++ "_" ++
              nameBase dname
  (dtyp,_) <- canonicalType dname
  (ttyp,_) <- canonicalType tname
  mname <- newName "m"
  (funcDecls,funcCxt) <- defineHomFuncXM mname fname tname dname
  transformBodyDecls <- sequence [
      funD 'transformM
        [clause [] (normalB $ varE fname) []]
    ]
  transformDecl <- instanceD
                      (return funcCxt)
                      (return $ applyTypeCon
                        (ConT ''TransformM)
                        [VarT mname, ttyp, dtyp]) $
                      map return transformBodyDecls
  return $ funcDecls ++ [transformDecl]

-- |A Template Haskell mechanism for defining a function which performs a
--  purely structural transformation on a given data type.
defineHomFuncM :: Name -> Name -> Name -> Q [Dec]
defineHomFuncM fname tname dname = do
  mname <- newName "m"
  (decs,_) <- defineHomFuncXM mname fname tname dname
  return decs

-- |The actual implementation of @defineHomFuncM@.  This implementation exposes
--  some specific information about the declarations which were constructed
--  as well as the declarations themselves.
defineHomFuncXM :: Name -> Name -> Name -> Name -> Q ([Dec],Cxt)
defineHomFuncXM mname fname tname dname = do
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
      let mkPred typ = classP ''TransformM [varT mname, return ttyp, typ]
      argTyps <- getDataArgTypes info
      let preds = (classP ''Monad [varT mname]) :
                  (map (mkPred . return) argTyps)
      fcxt <- cxt preds
      let typ = forallT (dbndrs ++ tbndrs ++ [PlainTV mname]) (return fcxt) $
                  return $
                    mkFnType [ ttyp, dtyp, AppT (VarT mname) dtyp ]
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
              _ -> error $ "defineHomFunc: " ++ show dname
                      ++ " is not a data decl"
            _ -> error $ "defineHomFunc: " ++ show dname
                  ++ " is not a type constructor"
      let caseExpr = caseE (varE valName) branches
      sequence [funD fname [clause [varP tagName, varP valName]
                  (normalB caseExpr) []]]
    makeBranch :: Name -> Con -> Q Match
    makeBranch tagName con =
      case con of
        NormalC cname typs -> do
          -- get some names for the argument variables
          argNames <- sequence $ take (length typs) $ mkPrefixNames "x"
          resNames <- sequence $ take (length typs) $ mkPrefixNames "rM"
          -- the pattern is now pretty straightforward
          let pattern = conP cname $ map varP argNames
          -- the body is slightly more involved
          let trans nm = [|transformM $(varE tagName) $(varE nm)|]
          let stmts = (map (\(a,r) -> bindS (varP r) (trans a)) $
                        zip argNames resNames)
                      ++ [noBindS $ appE [|return|] $
                            foldl appE (conE cname) $ map varE resNames]
          let e = doE stmts
          match pattern (normalB e) []
        _ -> error $ "defineHomFunc: " ++ show con
              ++ " is not a normal constructor"

-- |Creates a common identity instance for @TransformM@.
defineTransformIdentityInstanceM :: Name -> Name -> Q [Dec]
defineTransformIdentityInstanceM tname dname = do
  (ttyp,_) <- canonicalType tname
  (dtyp,_) <- canonicalType dname
  [d| instance (Monad m) => TransformM m $(return ttyp) $(return dtyp) where
        transformM _ = return . id |]

-- |Creates a common instance for @TransformM@ for @Traversable@ data.
defineTransformTraversableInstanceM :: Name -> Q Type -> Q [Dec]
defineTransformTraversableInstanceM tname funcType = do
  (ttyp,_) <- canonicalType tname
  [d| instance (Monad m, TransformM m $(return ttyp) a)
            => TransformM m $(return ttyp) ($(funcType) a) where
        transformM t d = Trav.sequence $ fmap (transformM t) d |]

-- |Creates identity @TransformM@ instances for common primitives.
defineCommonPrimitiveTransformIdentityInstancesM :: Name -> Q [Dec]
defineCommonPrimitiveTransformIdentityInstancesM tname =
  concat <$> mapM (defineTransformIdentityInstanceM tname)
    [ ''Int
    , ''Integer
    , ''Char
    , ''Bool
    ]

defineCommonTupleTransformInstanceM :: Name -> Int -> Q [Dec]
defineCommonTupleTransformInstanceM tname n = do
  (ttyp,_) <- canonicalType tname
  typeParamNames <- sequence $ take n $ mkPrefixNames "tupleParam"
  let tupleType = applyTypeCon (TupleT n) $ map VarT typeParamNames
  argNames <- sequence $ take n $ mkPrefixNames "tupleArg"
  resNames <- sequence $ take n $ mkPrefixNames "tupleRes"
  let transName = mkName "_t"
  let tupleName = mkName "tuple"
  let mname = mkName "m"
  let body = doE $ (map (\(anm,rnm) -> bindS (varP rnm) $
                            [| transformM $(varE transName) $(varE anm) |]) $
                      zip argNames resNames)
                   ++ [noBindS $ appE [|return|] $ tupE $ map varE resNames]
  let expr = caseE (varE tupleName) $ [match (tupP $ map varP argNames)
                (normalB body) []]
  let context = cxt $ map (\v -> classP (''TransformM)
                              [varT mname, return ttyp, varT v]) typeParamNames
                      ++ [classP ''Monad [varT mname]]
  let instD =
        instanceD context (applyTypeCon (ConT ''TransformM) <$>
                              sequence [varT mname, return ttyp,
                                        return tupleType])
          [funD 'transformM [clause [varP transName, varP tupleName]
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
defineCommonHomInstancesM :: Name -> Q [Dec]
defineCommonHomInstancesM tname = do
  (ttyp,_) <- canonicalType tname
  let litDecls =
        [ [d|
            instance (Monad m, Ord a, TransformM m $(return ttyp) a)
                  => TransformM m $(return ttyp) (Set a) where
              transformM t s =
                Set.fromList `liftM` mapM (transformM t) (Set.toList s)
          |]
        ]
  let insts = [ defineCommonPrimitiveTransformIdentityInstancesM tname
              ] ++ map (defineCommonTupleTransformInstanceM tname) (0:[2..8])
                ++ map (defineHomInstanceM tname) [''Either,''Maybe]
                ++ map (defineTransformTraversableInstanceM tname)
                        [ [t| [] |] ]
                ++ litDecls
  concat <$> sequence insts
