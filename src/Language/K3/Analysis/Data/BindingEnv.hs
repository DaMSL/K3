module Language.K3.Analysis.Data.BindingEnv where

import Language.K3.Core.Common
import Prelude hiding ( lookup )
import Data.HashMap.Lazy ( HashMap )
import qualified Data.HashMap.Lazy as Map

import Data.Text ( Text )
import qualified Data.Text as T

{- Binding environment data structures -}
type BindingEnv      a = HashMap Identifier a
type BindingStackEnv a = BindingEnv [a]


{- Data.Text helpers -}
mkErr :: String -> Either Text a
mkErr msg = Left $ T.pack msg

{- BindingEnv helpers -}
empty :: BindingEnv a
empty = Map.empty

member :: BindingEnv a -> Identifier -> Either Text Bool
member env x = return $ Map.member x env

lookup :: BindingEnv a -> Identifier -> Either Text a
lookup env x = maybe err Right $ Map.lookup x env
  where err = mkErr $ "Unbound variable in binding environment: " ++ x

pushWith :: BindingEnv a -> (a -> a -> a) -> Identifier -> a -> BindingEnv a
pushWith env f x v = Map.insertWith f x v env

popWith :: BindingEnv a -> (a -> Maybe a) -> Identifier -> BindingEnv a
popWith env f x = maybe env (maybe (Map.delete x env)
                                   (\nv -> Map.adjust (const nv) x env)
                             . f)
                    $ Map.lookup x env

set :: BindingEnv a -> Identifier -> a -> BindingEnv a
set env x v = Map.insert x v env

delete :: BindingEnv a -> Identifier -> BindingEnv a
delete env x = Map.delete x env

union :: BindingEnv a -> BindingEnv a -> BindingEnv a
union = Map.union

unions :: [BindingEnv a] -> BindingEnv a
unions = Map.unions

foldl :: (b -> Identifier -> a -> b) -> b -> BindingEnv a -> b
foldl f z env = Map.foldlWithKey' f z env

fromList :: [(Identifier, a)] -> BindingEnv a
fromList = Map.fromList

{- BindingStackEnv helpers -}

slookup :: BindingStackEnv a -> Identifier -> Either Text a
slookup env x = lookup env x >>= safeHead
  where
    safeHead l = if null l then err else Right $ head l
    err = mkErr $ "Unbound variable in binding environment: " ++ x

push :: BindingStackEnv a -> Identifier -> a -> BindingStackEnv a
push env x v = pushWith env (++) x [v]

pop :: BindingStackEnv a -> Identifier -> BindingStackEnv a
pop env x = popWith env safeTail x
  where safeTail []  = Nothing
        safeTail [_] = Nothing
        safeTail l   = Just $ tail l
