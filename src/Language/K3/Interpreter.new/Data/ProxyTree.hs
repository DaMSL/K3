module Language.K3.Interpreter.Data.ProxyTree where

import Control.Concurrent.MVar
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Map  ( Map   )
import Data.Trie ( Trie  )
import Data.Word ( Word8 )
import qualified Data.Map          as Map
import qualified Data.Hashtable.IO as HT 
import qualified Data.Trie         as Trie

import System.Mem.StableName
import System.Mem.Weak

import Language.K3.Interpreter.Data.Types

-- | Proxy trees, for efficient synchronization of aliased values.
class (Monad m) => ProxyTree m vk v t where
  lookupPT       :: vk -> t -> m (Maybe v)
  ancestorsPT    :: vk -> t -> m [vk]
  descendantsPT  :: vk -> t -> m [vk]
  rebuildPT      :: vk -> v -> t -> m t

instance ProxyTree Interpretation (ProxyLocation Value) (IProxyEntry Value) (IEnvProxies Value) where
  lookupPT (s,p) t = liftIO (HT.lookup t s) >>= \case
    Nothing -> return Nothing
    Just tr -> return $ Trie.lookup (encode p) tr
  
  ancestorsPT (s,p) t = liftIO (HT.lookup t s) >>= \case
    Nothing -> return []
    Just tr -> return . catMaybes =<< mapM (\path -> tryPathLookup tr s path) $ inits p

  descendantsPT (s,p) t = liftIO (HT.lookup t s) >>= \case
    Nothing -> return []
    Just tr -> foldrM (\wv acc -> maybe acc (\x -> acc++[x]) $ proxyLocation wv) [] $ submap (encode p) tr
  
  rebuildPT (s,p) v t = liftIO (HT.lookup t s) >>= \case
    Nothing -> return t
    Just tr -> do
      mapM_ (\path -> maybe (return ()) (\deltaPath -> updateProxyValue tr path deltaPath v) $ stripPrefix p path) $ inits p

  proxyLocation :: IProxyEntry Value -> Interpretation (Maybe (ProxyLocation Value))
  proxyLocation wv = liftIO (deRefWeak wv) >>= \case
    Nothing       -> return Nothing
    Just (pid, v) -> liftIO (readMVar pid) >>= return . Just

  tryPathLookup :: Trie (IProxyEntry Value) -> StableName Value -> ProxyPath
                -> Interpretation (Maybe (ProxyLocation Value))
  tryPathLookup tr s path = do
    let wvOpt = Trie.lookup (encode path) tr
    case wvOpt of 
      Nothing  -> return Nothing
      Just wv  -> liftIO (deRefWeak wv) >>= return . maybe Nothing (const $ Just (s,path))

  updateProxyValue tr p dp newV = do
    case Trie.lookup tr (encode p) of 
      Nothing -> return ()
      Just wv -> liftIO (deRefWeak wv) >>= \case
                    Nothing          -> return ()
                    Just (pid, oldV) -> modifyMVar_ oldV (\v -> rebuildValue dp v newV)

  rebuildValue (Named n) v nv = undefined
  rebuildValue (Temporary n) v nv = undefined
  rebuildValue (Dataspace (n,t)) v nv = undefined
  rebuildValue Dereference (VIndirection v) nv = undefined
  rebuildValue (TupleField x) (VTuple fields) nv = undefined
  rebuildValue (RecordField x) (VRecord fields) nv = undefined
  rebuildValue (CollectionMember x) (VCollection (self, c)) nv = undefined
  rebuildValue _ _ _ = undefined

  encode :: ProxyPath -> ByteString
  encode p = undefined
  
  decode :: ByteString -> ProxyPath
  decode p = undefined