{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}

module Language.K3.Runtime.Dataspace (
  Dataspace,
  emptyDS,
  initialDS,
  copyDS,
  peekDS,
  insertDS,
  deleteDS,
  updateDS,
  foldDS,
  mapDS,
  mapDS_,
  filterDS,
  combineDS,
  splitDS,
  sortDS,
  
  EmbeddedKV,
  extractKey,
  embedKey,

  AssociativeDataspace,
  lookupKV,
  removeKV,
  insertKV,
  replaceKV,

  dsChainInstanceGenerator
) where

import Control.Applicative

import Language.Haskell.TH
import Language.Haskell.TH.Syntax ( returnQ )

class (Monad m) => OrdM m v where
  compareV :: v -> v -> m Ordering

-- (move the instances to Interpreter/IDataspace.hs)
class (Monad m) => Dataspace m ds v | ds -> v where
  -- The Maybe ds argument to constructors is a hint about which kind of
  -- dataspace to construct
  emptyDS       :: Maybe ds -> m ds
  initialDS     :: [v] -> Maybe ds -> m ds
  copyDS        :: ds -> m ds
  peekDS        :: ds -> m (Maybe v)
  insertDS      :: ds -> v -> m ds
  deleteDS      :: v -> ds -> m ds
  updateDS      :: v -> v -> ds -> m ds
  foldDS        :: ( a -> v -> m a ) -> a -> ds -> m a
  mapDS         :: ( v -> m v ) -> ds -> m ds
  mapDS_        :: ( v -> m v ) -> ds -> m ()
  filterDS      :: ( v -> m Bool ) -> ds -> m ds
  combineDS     :: ds -> ds -> m ds
  splitDS       :: ds -> m (ds, ds)
  sortDS        :: ( v -> v -> m Ordering ) -> ds -> m ds
{- casting? -}

class (Monad m, Dataspace m ds v) => SequentialDataspace m ds v where
  sort2DS :: ( v -> v -> m Ordering ) -> ds -> m ds

class (Monad m, OrdM m v, Dataspace m ds v) => SetDataspace m ds v where
  memberDS       :: v  -> ds -> m Bool
  isSubsetOfDS   :: ds -> ds -> m Bool
  unionDS        :: ds -> ds -> m ds
  intersectionDS :: ds -> ds -> m ds
  differenceDS   :: ds -> ds -> m ds

class (Monad m, OrdM m v, Dataspace m ds v) => SortedDataspace m ds v where
  minDS        :: ds -> m (Maybe v)
  maxDS        :: ds -> m (Maybe v)
  lowerBoundDS :: v -> ds -> m (Maybe v)
  upperBoundDS :: v -> ds -> m (Maybe v)
  sliceDS      :: v -> v -> ds -> m ds

class (Monad m) => EmbeddedKV m v k where
  extractKey :: v -> m k
  embedKey   :: k -> v -> m v

class (Monad m, Dataspace m ds v) => AssociativeDataspace m ds k v where
  lookupKV       :: ds -> k -> m (Maybe v)
  removeKV       :: ds -> k -> v -> m ds
  insertKV       :: ds -> k -> v -> m ds
  replaceKV      :: ds -> k -> v -> m ds


-- | Dataspace instance generation via Template Haskell
dsChainInstanceGenerator :: TypeQ -> TypeQ -> TypeQ -> ExpQ -> [(String, String)] -> String -> Q [Dec]
dsChainInstanceGenerator mT dsT vT mErrorE dsInstances defaultInstance = do
    let methodDecls = [ genMethod method | method <- methods ]
    (:[]) <$> instanceD (cxt []) [t|Dataspace $(mT) $(dsT) $(vT)|] methodDecls

  where 
    methods = 
      [ chainConOM  "emptyDS" 
          [clause [[p|Nothing|]] (normalB [| (emptyDS Nothing) >>= return . $(conE $ mkName defaultInstance) |]) []]
          []
  
      , chainCon1OM "initialDS" "vals"
          [clause [(varP $ mkName "vals"),[p|Nothing|]]
            (normalB [| (initialDS $(varE $ mkName "vals") Nothing) >>= return . $(conE $ mkName defaultInstance) |]) []]
          []

      , chainConM   "copyDS" [] []
      , chainM      "peekDS" [] []
      , chainCon1PM "insertDS" "val" [] []
      , chainCon1M  "deleteDS" "val" [] []
      , chainCon2M  "updateDS" "v" "v'" [] []
      
      , chain2M     "foldDS"   "acc" "acc_init" [] []
      , chainCon1M  "mapDS"    "func" [] []
      , chain1M     "mapDS_"   "func" [] []
      , chainCon1M  "filterDS" "func" [] []
      , chainCon1M  "sortDS"   "sortF" [] []

      , ( "combineDS", (\(cn, vn) -> [ (iConPat cn ("l_"++vn))
                                     , (iConPat cn ("r_"++vn)) ] )
                     , (\(mn, cn, vn) ->
                          [| $(varE $ mkName mn) $(varE (mkName $ "l_"++vn))
                                                 $(varE (mkName $ "r_"++vn))
                                >>= return . $(conE $ mkName cn) |])
                     , []
                     , [clause [[p|_|], [p|_|]] (normalB errorE) []] )

      , ( "splitDS"  , (\(cn, vn) -> [ (iConPat cn vn) ] )
                     , (\(mn, cn, vn) ->
                          [| $(varE $ mkName mn) $(varE $ mkName vn)
                                >>= \(l,r) -> return ($(conE $ mkName cn) l, $(conE $ mkName cn) r) |])
                     , [], [] )
      ]

    genMethod (mn, argPatF, mExprF, preDecl, postDecl) = do
      let instClauses = concatMap (\(cn,vn) -> [clause (argPatF (cn, vn)) (normalB $ mExprF (mn, cn, vn)) []]) dsInstances
          clauses     = preDecl ++ instClauses ++ postDecl
      funD (mkName mn) clauses

    iConPat cn vn    = conP (mkName cn) [varP $ mkName vn]
    iConPatOpt cn vn = conP (mkName "Just") [conP (mkName cn) [varP $ mkName vn]]

    app0E        = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName vn) |])
    app1E an    = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName an) $(varE $ mkName vn) |])
    app2E an bn = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName an) $(varE $ mkName bn) $(varE $ mkName vn) |])

    appConE        = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName vn) >>= return . $(conE $ mkName cn) |])
    appConOE       = (\(mn, cn, vn) -> [| $(varE $ mkName mn) (Just $(varE $ mkName vn)) >>= return . $(conE $ mkName cn) |])
    appCon1E an    = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName an) $(varE $ mkName vn) >>= return . $(conE $ mkName cn) |])
    appCon1OE an   = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName an) (Just $(varE $ mkName vn)) >>= return . $(conE $ mkName cn) |])
    appCon1PE an   = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName vn) $(varE $ mkName an) >>= return . $(conE $ mkName cn) |])
    appCon2E an bn = (\(mn, cn, vn) -> [| $(varE $ mkName mn) $(varE $ mkName an) $(varE $ mkName bn) $(varE $ mkName vn) >>= return . $(conE $ mkName cn) |])

    errorE = [| $(mErrorE) "Mismatched collection types in combine" |]

    -- A method taking a dataspace (and 1-2 extra) argument, and chaining its function
    chainM n        pre post = ( n, (\(cn, vn) -> [ (iConPat cn vn) ] ), app0E , pre, post )
    chain1M n an    pre post = ( n, (\(cn, vn) -> [ (varP $ mkName an), (iConPat cn vn) ] ), app1E an, pre, post )
    chain2M n an bn pre post = ( n, (\(cn, vn) -> [ (varP $ mkName an), (varP $ mkName bn), (iConPat cn vn) ] ), app2E an bn, pre, post )

    -- A method taking a dataspace (and 1-2 extra) argument, chaining its function, and reconstructing its input type.
    chainConM n        pre post = ( n, (\(cn, vn) -> [ (iConPat cn vn) ] ), appConE, pre, post )
    chainConOM n       pre post = ( n, (\(cn, vn) -> [ (iConPatOpt cn vn) ] ), appConOE, pre, post )
    chainCon1M n an    pre post = ( n, (\(cn, vn) -> [ (varP $ mkName an), (iConPat cn vn) ] ), appCon1E an, pre, post )
    chainCon1OM n an   pre post = ( n, (\(cn, vn) -> [ (varP $ mkName an), (iConPatOpt cn vn) ] ), appCon1OE an, pre, post )
    chainCon1PM n an   pre post = ( n, (\(cn, vn) -> [ (iConPat cn vn), (varP $ mkName an) ] ), appCon1PE an, pre, post )
    chainCon2M n an bn pre post = ( n, (\(cn, vn) -> [ (varP $ mkName an), (varP $ mkName bn), (iConPat cn vn) ] ), appCon2E an bn, pre, post )

