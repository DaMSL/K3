{-# LANGUAGE MultiParamTypeClasses, TemplateHaskell, FlexibleInstances, FunctionalDependencies, TypeFamilies, UndecidableInstances #-}

{-|
  This module defines a generic @Reduce@ typeclass which allows general
  reductions over a data type.  It also provides Template Haskell routines
  to generate naturally catamorphic boilerplate instances for data types.
  
  An example of use is as follows:
  
  @
    -- Data structure describing the reduction.
    data MyReduction = MyReduction Int
    -- Catamorphic instances
    $(concat <$> mapM (defineCatInstance ''MyReduction) [''Foo, ''Bar])
    -- Common instances
    $(defineCommonCatInstances ''MyReduction)
    -- Special case
    $(defineCatFunc ''MyReduction ''Baz $ mkName "catBaz")
    instance Reduce Baz MyReduction (Set Int) where
      reduce (MyReduction n) baz = case baz of
        Baz5 m -> if n > m then mempty else Set.singleton m
        _ -> homBaz baz
  @
  
  In this example, we assume that Foo, Bar, and Baz are data structures which
  contain each other in some fashion.  Each may have any number of constructors.
  The above code defines reduction through Foo and Bar to be naturally
  catamorphic; the data are disassembled, the arguments are reduced, and the
  results are monoidally concatenated.  The common instances allow for cases
  such as Int and Set to be defined as @mempty@ with minimal boilerplate.  The
  Baz case is special: if the constructor Baz5 is used, it may be reduced to a
  non-empty value; all other Baz constructors are naturally catamorphic.  Note
  that this special case applies to all instances of Baz5 throughout all of the
  reduced structure.
-}
module Language.K3.TemplateHaskell.Reduce
( Reduce(..)
, defineCatInstance
, defineCatFunc
, defineCommonCatInstances
, defineReduceEmptyInstance
, defineReduceFoldInstance
, defineCommonTupleReduceInstance
, defineCommonPrimitiveReduceEmptyInstances
) where

import Control.Applicative
import Data.Foldable
import Data.List
import Data.Monoid
import Data.Set (Set)
import Language.Haskell.TH

import Language.K3.TemplateHaskell.Utils

class (Monoid r) => Reduce t d r | t d -> r where
  reduce :: t -> d -> r
  
-- |A Template Haskell mechanism for defining a reduction instance for
--  the @Reduce@ typeclass at a given type declaration.
defineCatInstance :: Q Type -> Name -> Name -> Q [Dec]
defineCatInstance rtype tname dname = do
  -- We're going to defer to the defineCatFunc function by picking a fresh
  -- function name and using it in the type declaration.
  fname <- newName $ "reduceFn_" ++ nameBase tname ++ "_" ++
              nameBase dname
  (dtyp,_) <- canonicalType dname
  (ttyp,_) <- canonicalType tname
  (funcDecls,funcCxt) <- defineCatFuncX rtype fname tname dname
  reduceBodyDecls <- sequence [
      funD 'reduce
        [clause [] (normalB $ varE fname) []]
    ]
  reduceDecl <- instanceD
                      (return funcCxt)
                      (applyTypeCon
                        (ConT ''Reduce) <$> sequence
                        [return ttyp, return dtyp, rtype]) $
                      map return reduceBodyDecls
  return $ funcDecls ++ [reduceDecl]

-- |A Template Haskell mechanism for defining a function which performs a
--  purely structural reduction on a given data type.
defineCatFunc :: Q Type -> Name -> Name -> Name -> Q [Dec]
defineCatFunc rtype fname tname dname = do
  (decs,_) <- defineCatFuncX rtype fname tname dname
  return decs

-- |The actual implementation of @defineCatFunc@.  This implementation exposes
--  some specific information about the declarations which were constructed
--  as well as the declarations themselves.
defineCatFuncX :: Q Type -> Name -> Name -> Name -> Q ([Dec],Cxt)
defineCatFuncX rtype fname tname dname = do
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
      let mkPred typ = classP ''Reduce [return ttyp, typ, rtype]
      let preds = map mkPred $ getDataArgTypes info
      fcxt <- cxt preds
      let typ = forallT (dbndrs ++ tbndrs) (return fcxt) $
                  mkFnType <$> sequence
                    [ return ttyp, return dtyp, rtype ]
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
          let redc nm = [|reduce $(varE tagName) $(varE nm)|]
          let e = [| mconcat $(listE $ map redc argNames) |]
          match pattern (normalB e) []
        _ -> error $ "defineCatFunc: " ++ show con
              ++ " is not a normal constructor"

-- |Creates a common identity instance for @Reduce@.
defineReduceEmptyInstance :: Q Type -> Name -> Name -> Q [Dec]
defineReduceEmptyInstance rtype tname dname = do
  (ttyp,_) <- canonicalType tname
  (dtyp,_) <- canonicalType dname
  [d| instance Reduce $(return ttyp) $(return dtyp) $(rtype) where
        reduce _ = const mempty |]
        
-- |Creates a @Reduce@ instance for a concrete data type which is known to be
--  @Foldable@.
defineReduceFoldInstance :: Q Type -> Name -> Q Type -> Q [Dec]
defineReduceFoldInstance rtype tname tcon = do
  (ttyp,_) <- canonicalType tname
  [d| instance (Reduce $(return ttyp) a $(rtype))
            => Reduce $(return ttyp) ($(tcon) a) $(rtype) where
        reduce t d = foldMap (reduce t) d |]

-- |Creates identity @Reduce@ instances for common primitives.
defineCommonPrimitiveReduceEmptyInstances :: Q Type -> Name -> Q [Dec]
defineCommonPrimitiveReduceEmptyInstances rtype tname =
  Data.List.concat <$> mapM (defineReduceEmptyInstance rtype tname)
    [ ''Int
    , ''Integer
    , ''Char
    , ''Bool
    ]
    
defineCommonTupleReduceInstance :: Q Type -> Name -> Int -> Q [Dec]
defineCommonTupleReduceInstance rtype tname n = do
  (ttyp,_) <- canonicalType tname
  paramNames <- sequence $ take n $ mkPrefixNames "tupleParam"
  let tupleType = applyTypeCon (TupleT n) $ map VarT paramNames
  argNames <- sequence $ take n $ mkPrefixNames "tupleArg"
  let redName = mkName "_t"
  let tupleName = mkName "tuple"
  let list =  listE $ map (\nm -> [| reduce $(varE redName) $(varE nm) |])
                        argNames
  let body = [| mconcat $(list) |]
  let expr = caseE (varE tupleName) $ [match (tupP $ map varP argNames)
                (normalB body) []]
  let context = cxt $ map (\v -> classP (''Reduce) [return ttyp, varT v, rtype])
                        paramNames
  let instD = 
        instanceD context (applyTypeCon (ConT ''Reduce) <$>
                              sequence [return ttyp, return tupleType, rtype])
          [funD 'reduce [clause [varP redName, varP tupleName]
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
defineCommonCatInstances :: Q Type -> Name -> Q [Dec]
defineCommonCatInstances rtype tname =
  let insts = [ defineCommonPrimitiveReduceEmptyInstances rtype tname
              ] ++ map (defineCommonTupleReduceInstance rtype tname) (0:[2..8])
                ++ map (defineCatInstance rtype tname) [''Either,''Maybe]
                ++ map (defineReduceFoldInstance rtype tname)
                    [ [t| [] |], [t|Set|] ]
  in
  Data.List.concat <$> sequence insts
