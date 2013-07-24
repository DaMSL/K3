{-# LANGUAGE MultiParamTypeClasses, TemplateHaskell, FlexibleInstances, FunctionalDependencies, TypeFamilies, UndecidableInstances #-}

{-|
  This module defines a generic @ReduceM@ typeclass which allows general
  reductions over a data type.  It also provides Template Haskell routines
  to generate naturally catamorphic boilerplate instances for data types.
  This typeclass is distinct from @Reduce@ for reasons of performance and to
  simplify the underlying generation code.
  
  An example of use is as follows:
  
  @
    -- Data structure describing the reduction.
    data MyReduction = MyReduction Int
    -- Catamorphic instances
    $(concat <$> mapM (defineCatInstanceM ''MyReduction) [''Foo, ''Bar])
    -- Common instances
    $(defineCommonCatInstancesM ''MyReduction)
    -- Special case
    $(defineCatFuncM ''MyReduction ''Baz $ mkName "catBaz")
    instance ReduceM (State Int) Baz MyReduction (Set Int) where
      reduceM (MyReduction n) baz = case baz of
        Baz5 m ->
          if n > m
            then mempty
            else do
              modify (+1)
              return $ Set.singleton m
        _ -> homBaz baz
  @
  
  In this example, we assume that Foo, Bar, and Baz are data structures which
  contain each other in some fashion.  Each may have any number of constructors.
  The above code defines reduction through Foo and Bar to be naturally
  catamorphic; the data are disassembled, the arguments are reduced, and the
  results are monoidally concatenated.  The common instances allow for cases
  such as Int and Set to be defined as @mempty@ with minimal boilerplate.  The
  Baz case is special: if the constructor Baz5 is used, it may be reduced to a
  non-empty value; all other Baz constructors are naturally catamorphic.
  Additionally, each event of such a reconstruction increments a counter managed
  by the monad.  Note that this special case applies to all instances of Baz5
  throughout all of the reduced structure.
-}
module Language.K3.TemplateHaskell.ReduceM
( ReduceM(..)
, defineCatInstanceM
, defineCatFuncM
, defineCommonCatInstancesM
, defineReduceEmptyInstanceM
, defineReduceTraversableInstanceM
, defineCommonTupleReduceInstanceM
, defineCommonPrimitiveReduceEmptyInstancesM
) where

import Control.Applicative
import Control.Monad
import Data.List
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Traversable as Trav
import Language.Haskell.TH

import Language.K3.TemplateHaskell.Utils

class (Monad m, Monoid r) => ReduceM m t d r | t d -> r where
  reduceM :: t -> d -> m r
  
-- |A Template Haskell mechanism for defining a reduction instance for
--  the @ReduceM@ typeclass at a given type declaration.
defineCatInstanceM :: Q Type -> Name -> Name -> Q [Dec]
defineCatInstanceM rtype tname dname = do
  -- We're going to defer to the defineCatFunc function by picking a fresh
  -- function name and using it in the type declaration.
  fname <- newName $ "reduceFn_" ++ nameBase tname ++ "_" ++
              nameBase dname
  (dtyp,_) <- canonicalType dname
  (ttyp,_) <- canonicalType tname
  mname <- newName "m"
  (funcDecls,funcCxt) <- defineCatFuncXM rtype mname fname tname dname
  reduceBodyDecls <- sequence [
      funD 'reduceM
        [clause [] (normalB $ varE fname) []]
    ]
  reduceDecl <- instanceD
                      (return funcCxt)
                      (applyTypeCon
                        (ConT ''ReduceM) <$> sequence
                        [varT mname, return ttyp, return dtyp, rtype]) $
                      map return reduceBodyDecls
  return $ funcDecls ++ [reduceDecl]

-- |A Template Haskell mechanism for defining a function which performs a
--  purely structural reduction on a given data type.
defineCatFuncM :: Q Type -> Name -> Name -> Name -> Q [Dec]
defineCatFuncM rtype fname tname dname = do
  mname <- newName "m"
  (decs,_) <- defineCatFuncXM rtype mname fname tname dname
  return decs

-- |The actual implementation of @defineCatFuncM@.  This implementation exposes
--  some specific information about the declarations which were constructed
--  as well as the declarations themselves.
defineCatFuncXM :: Q Type -> Name -> Name -> Name -> Name -> Q ([Dec],Cxt)
defineCatFuncXM rtype mname fname tname dname = do
  info <- reify dname
  fnDef <- makeFnDef info
  (fnSig,fcxt) <- makeFnSig info
  return (fnDef ++ fnSig, fcxt)
  where
    -- |Creates the type signature for the reduction function.
    makeFnSig :: Info -> Q ([Dec],Cxt)
    makeFnSig info = do
      (dtyp,dbndrs) <- canonicalType dname
      (ttyp,tbndrs) <- canonicalType tname
      let mkPred typ = classP ''ReduceM [varT mname, return ttyp, typ, rtype]
      argTyps <- getDataArgTypes info
      let preds = [classP ''Monad [varT mname]]
                    ++ map (mkPred . return) argTyps
      fcxt <- cxt preds
      let typ = forallT (dbndrs ++ tbndrs ++ [PlainTV mname]) (return fcxt) $
                  mkFnType <$> sequence
                    [ return ttyp, return dtyp, appT (varT mname) rtype ]
      signature <- sigD fname typ
      return ([signature], fcxt)
    -- |Creates the definition for the reduction function.
    makeFnDef :: Info -> Q [Dec]
    makeFnDef info = do
      tagName <- newName "_t"
      valName <- newName "v"
      let branches = case info of
            TyConI dec -> case dec of
              DataD _ _ _ cons _ ->
                map (makeBranch tagName) cons
              _ -> error $ "defineCatFunc: " ++ show dname
                      ++ " is not a data decl"
            _ -> error $ "defineCatFunc: " ++ show dname
                  ++ " is not a type constructor"
      let body = normalB $ caseE (varE valName) branches
      sequence [funD fname [clause [varP tagName, varP valName] body []]]
    makeBranch :: Name -> Con -> Q Match
    makeBranch tagName con =
      case con of
        NormalC cname typs -> do
          -- get some names for the argument variables
          argNames <- sequence $ take (length typs) $ mkPrefixNames "x"
          resNames <- sequence $ take (length typs) $ mkPrefixNames "r"
          -- the pattern is now pretty straightforward
          let pattern = conP cname $ map varP argNames
          -- the body is slightly more involved
          let redc nm = [|reduceM $(varE tagName) $(varE nm)|]
          let e = doE $ (map (\(anm,rnm) -> bindS (varP rnm) (redc anm)) $
                            zip argNames resNames)
                        ++ [noBindS $ appE (varE 'return) $
                              appE (varE 'mconcat) $ listE $ map varE resNames]
          match pattern (normalB e) []
        _ -> error $ "defineCatFunc: " ++ show con
              ++ " is not a normal constructor"

-- |Creates a common identity instance for @Reduce@.
defineReduceEmptyInstanceM :: Q Type -> Name -> Name -> Q [Dec]
defineReduceEmptyInstanceM rtype tname dname = do
  (ttyp,_) <- canonicalType tname
  (dtyp,_) <- canonicalType dname
  [d| instance (Monad m)
            => ReduceM m $(return ttyp) $(return dtyp) $(rtype) where
        reduceM _ = return . const mempty |]
        
-- |Creates a @ReduceM@ instance for a concrete data type which is known to be
--  @Traversable@.
defineReduceTraversableInstanceM :: Q Type -> Name -> Q Type -> Q [Dec]
defineReduceTraversableInstanceM rtype tname tcon = do
  (ttyp,_) <- canonicalType tname
  [d| instance (Applicative m, Monad m, ReduceM m $(return ttyp) a $(rtype))
            => ReduceM m $(return ttyp) ($(tcon) a) $(rtype) where
        reduceM t d = fst $ Trav.mapAccumL (\acc dat -> (liftM2 mappend acc (reduceM t dat),())) (return mempty) d |]

-- |Creates identity @ReduceM@ instances for common primitives.
defineCommonPrimitiveReduceEmptyInstancesM :: Q Type -> Name -> Q [Dec]
defineCommonPrimitiveReduceEmptyInstancesM rtype tname =
  Data.List.concat <$> mapM (defineReduceEmptyInstanceM rtype tname)
    [ ''Int
    , ''Integer
    , ''Char
    , ''Bool
    ]
    
defineCommonTupleReduceInstanceM :: Q Type -> Name -> Int -> Q [Dec]
defineCommonTupleReduceInstanceM rtype tname n = do
  (ttyp,_) <- canonicalType tname
  typeParamNames <- sequence $ take n $ mkPrefixNames "tupleParam"
  let tupleType = applyTypeCon (TupleT n) $ map VarT typeParamNames
  argNames <- sequence $ take n $ mkPrefixNames "tupleArg"
  resNames <- sequence $ take n $ mkPrefixNames "tupleRes"
  let redName = mkName "_t"
  let tupleName = mkName "tuple"
  mname <- newName "m" 
  let body = doE $ (map (\(anm,rnm) -> bindS (varP rnm)
                                          [| reduceM $(varE redName)
                                                     $(varE anm) |]) $
                      zip argNames resNames)
                    ++ [noBindS $ appE (varE 'return) $ appE (varE 'mconcat) $
                          listE $ map varE resNames]
  let expr = caseE (varE tupleName) $ [match (tupP $ map varP argNames)
                (normalB body) []]
  let context = cxt $ map (\v -> classP (''ReduceM)
                              [varT mname, return ttyp, varT v, rtype])
                        typeParamNames
                      ++ [classP ''Monad [varT mname]]
  let instD = 
        instanceD context (applyTypeCon (ConT ''ReduceM) <$>
                              sequence [ varT mname, return ttyp
                                       , return tupleType, rtype])
          [funD 'reduceM [clause [varP redName, varP tupleName]
                          (normalB expr) []]]
  sequence [instD]

{- |Creates instances for catamorphic reductions over common cases:
      * n-tuples for 2 <= n <= 8
      * primitives (Int, Integer, Char, Bool, ())
      * @Either@
      * @Maybe@
      * @[]@
      * @Set@
-}    
defineCommonCatInstancesM :: Q Type -> Name -> Q [Dec]
defineCommonCatInstancesM rtype tname = do
  (ttyp,_) <- canonicalType tname
  let litDecls =
        [ [d|
            instance (Monad m, Monoid r, Ord a, ReduceM m $(return ttyp) a r)
                  => ReduceM m $(return ttyp) (Set a) r where
              reduceM t s =
                mconcat `liftM` mapM (reduceM t) (Set.toList s)
              
              
              --  Set.fromList `liftM` mapM (transformM t) (Set.toList s)
          |]
        ]
  let insts = [ defineCommonPrimitiveReduceEmptyInstancesM rtype tname
              ] ++ map (defineCommonTupleReduceInstanceM rtype tname) (0:[2..8])
                ++ map (defineCatInstanceM rtype tname) [''Either,''Maybe]
                ++ map (defineReduceTraversableInstanceM rtype tname)
                    [ [t| [] |] ]
                ++ litDecls
  Data.List.concat <$> sequence insts
