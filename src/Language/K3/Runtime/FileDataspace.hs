{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE Rank2Types #-}

module Language.K3.Runtime.FileDataspace (
  FileDataspace(..),
  getFile, -- Should this really be exported?  Used to pack dataspace
  initDataDir,
  generateCollectionFilename,
  emptyFile,
  initialFile,
  copyFile,
  peekFile,
  foldFile,
  mapFile,
  mapFile_,
  filterFile,
  insertFile,
  deleteFile,
  updateFile,
  combineFile,
  splitFile
) where

import Control.Concurrent.MVar
import Control.Monad.Reader

import qualified System.IO as IO
import qualified System.Directory as DIR
import qualified System.FilePath as FP

import Language.K3.Core.Common
import Language.K3.Runtime.Engine

newtype FileDataspace v = FileDataspace String
getFile :: FileDataspace v -> String
getFile (FileDataspace name) = name

rootDataPath :: IO.FilePath
rootDataPath = "__DATA"

engineDataPath :: Engine a -> IO.FilePath
engineDataPath engine =
  FP.joinPath [ rootDataPath, validAddr ]
  where
    rawAddr = show $ address $ config engine
    noColonAddr = map ( \ch -> if ch == ':' then '_' else ch ) rawAddr
    validAddr = FP.makeValid noColonAddr

createDataDir :: IO.FilePath -> EngineM b ()
createDataDir path = do
  dirExists <- liftIO $ DIR.doesDirectoryExist path
  unless dirExists $ do
    isFile <- liftIO $ DIR.doesFileExist path
    if isFile then
      throwEngineError $ EngineError $ path ++ " exists but is not a directory, so it cannot be used to store external collections."
    else
      liftIO $ DIR.createDirectory path

initDataDir :: EngineM b ()
initDataDir = do
  createDataDir rootDataPath
  engine <- ask
  createDataDir $ engineDataPath engine

-- Put these into __DATA/peerID/collection_name
generateCollectionFilename :: EngineM b Identifier
generateCollectionFilename = do
    engine <- ask
    -- TODO move creating the data dir into atInit
    initDataDir
    let counter = collectionCount engine
    number <- liftIO $ readMVar counter
    let filename = "collection_" ++ (show number)
    liftIO $ modifyMVar_ counter $ \c -> return (c + 1)
    return $ FP.joinPath [engineDataPath engine, filename]

emptyFile :: () -> EngineM a (FileDataspace a)
emptyFile _ = do
  file_id <- generateCollectionFilename
  openCollectionFile file_id "w"
  close file_id
  return $ FileDataspace file_id

openCollectionFile :: String -> String -> EngineM a String
openCollectionFile name mode =
  do
    engine <- ask
    let wd = valueFormat engine
    openFile name name wd Nothing mode
    return name

copyFile :: FileDataspace v -> EngineM v (FileDataspace v)
copyFile old_id = do
  new_id <- generateCollectionFilename
  openCollectionFile new_id "w"
  foldFile id (\_ val -> do
    doWrite new_id val -- TODO hasWrite
    return ()
    ) () old_id
  close new_id
  return $ FileDataspace new_id
  
initialFile :: (Monad m) => (forall c. EngineM b c -> m c) -> [b] -> m (FileDataspace b)
initialFile liftM vals = do
  new_id <- liftM generateCollectionFilename
  liftM $ openCollectionFile new_id "w"
  foldM (\_ val -> do
    liftM $ doWrite new_id val -- TODO hasWrite
    return ()
    ) () vals
  liftM $ close new_id
  return $ FileDataspace new_id

peekFile :: (Monad m) => (forall c. EngineM b c -> m c) -> FileDataspace b -> m (Maybe b)
peekFile liftM (FileDataspace file_id) = do
  liftM $ openCollectionFile file_id "r"
  can_read <- liftM $ hasRead file_id
  result <-
    case can_read of
      Nothing     -> return Nothing
      Just False  -> return Nothing
      Just True   ->
        liftM $ doRead file_id
  liftM $ close file_id
  return result

-- Pass a lift into these functions, so that "inner loop" can be in 
-- some Monad m.  foldDS etc. can know which lift to use, since they
-- are in the instance of the typeclass.
foldFile :: forall (m :: * -> *) a b.
            Monad m =>
            (forall c. EngineM b c -> m c)
            -> (a -> b -> m a) -> a -> FileDataspace b -> m a
foldFile liftM accumulation initial_accumulator file_ds@(FileDataspace file_id) = do
  liftM $ openCollectionFile file_id "r"
  result <- foldOpenFile liftM accumulation initial_accumulator file_ds
  liftM $ close file_id
  return result
foldOpenFile :: forall (m :: * -> *) a b.
            Monad m =>
            (forall c. EngineM b c -> m c)
            -> (a -> b -> m a) -> a -> FileDataspace b -> m a
foldOpenFile liftM accumulation initial_accumulator file_ds@(FileDataspace file_id) =
 do
   file_over <- liftM $ hasRead file_id
   case file_over of
     Nothing -> return initial_accumulator -- Hiding an error...
     Just False -> return initial_accumulator
     Just True ->
       do
         cur_val <- liftM $ doRead file_id
         case cur_val of
           Nothing -> return initial_accumulator
           Just val -> do
             new_acc <- accumulation initial_accumulator val
             foldOpenFile liftM accumulation new_acc file_ds

mapFile :: (Monad m) => (forall c. EngineM b c -> m c) -> (b -> m b) -> FileDataspace b -> m (FileDataspace b)
mapFile liftM function file_ds@(FileDataspace file_id) = do
  new_id <- liftM generateCollectionFilename
  liftM $ openCollectionFile new_id "w"
  foldFile liftM (inner_func new_id) () file_ds
  liftM $ close new_id
  return $ FileDataspace new_id
  where
    --The typechecker didn't think the b here and the b in mapFile
    --were the same b, so it didn't typecheck.  Letting it infer
    --everything lets the typechecker figure it out
    --inner_func :: [Char] -> () -> b -> EngineM b ()
    inner_func new_id _ v = do
      new_val <- function v
      liftM $ doWrite new_id new_val
      return ()
mapFile_ :: (Monad m) => (forall c. EngineM b c -> m c) -> (b -> m a) -> FileDataspace b -> m ()
mapFile_ liftM function file_id =
  foldFile liftM inner_func () file_id
  where
    --inner_func :: () -> b -> EngineM b ()
    inner_func _ v = do
      function v
      return ()

filterFile :: (Monad m) => (forall c. EngineM b c -> m c) -> (b -> m Bool) -> FileDataspace b -> m (FileDataspace b)
filterFile liftM predicate old_id = do
  new_id <- liftM generateCollectionFilename
  liftM $ openCollectionFile new_id "w"
  foldFile liftM (inner_func new_id) () old_id
  liftM $ close new_id
  return $ FileDataspace new_id
  where
    --inner_func :: [Char] -> () -> Value -> EngineM b ()
    inner_func new_id _ v = do
      include <- predicate v
      if include then
        liftM $ doWrite new_id v
      else
        return ()

insertFile :: (Monad m) => (forall c. EngineM b c -> m c) -> FileDataspace b -> b -> m (FileDataspace b) 
insertFile liftM file_ds@(FileDataspace file_id) v = do
  liftM $ openCollectionFile file_id "a"
  -- can_write <- hasWrite ext_id -- TODO handle errors here
  liftM $ doWrite file_id v
  liftM $ close file_id
  return file_ds

deleteFile :: (Monad m, Eq b) => (forall c. EngineM b c -> m c) -> b -> FileDataspace b -> m (FileDataspace b)
deleteFile liftM v file_ds@(FileDataspace file_id) = do
  deleted_id <- liftM generateCollectionFilename
  liftM $ openCollectionFile deleted_id "w"
  foldFile liftM (\truth val -> do
    if truth == False && val == v
      then return True
      else do
        liftM $ doWrite deleted_id val --TODO error check
        return truth
    ) False file_ds
  liftM $ close deleted_id
  liftM $ liftIO $ DIR.renameFile deleted_id file_id -- What's with the double lift?
  return file_ds

updateFile :: (Monad m, Eq b) => (forall c. EngineM b c -> m c) -> b -> b -> FileDataspace b -> m (FileDataspace b)
updateFile liftM v v' file_ds@(FileDataspace file_id) = do
  new_id <- liftM $ generateCollectionFilename >>= flip openCollectionFile "w"
  did_update <-
    foldFile liftM (\truth val -> do
      if truth == False && val == v
        then do
          liftM $ doWrite new_id v'
          return True
        else do
          liftM $ doWrite new_id val --TODO error check
          return truth
      ) False file_ds
  unless did_update $ liftM $ doWrite new_id v'
  liftM $ close new_id
  liftM $ liftIO $ DIR.renameFile new_id file_id -- Double lift?
  return file_ds

combineFile :: (Monad m) => (forall c. EngineM b c -> m c) -> (FileDataspace b) -> (FileDataspace b) ->  m (FileDataspace b)
combineFile liftM self values = do
  (FileDataspace new_id) <- liftM $ Language.K3.Runtime.FileDataspace.copyFile self
  liftM $ openCollectionFile new_id "a"
  foldFile liftM (\_ v -> do
      liftM $ doWrite new_id v
      return ()
    ) () values
  liftM $ close new_id
  return $ FileDataspace new_id

splitFile :: (Monad m) => (forall c. EngineM b c -> m c) -> FileDataspace b -> m (FileDataspace b, FileDataspace b)
splitFile liftM self = do
  left  <- liftM $ generateCollectionFilename
  right <- liftM $ generateCollectionFilename
  liftM $ openCollectionFile left "w"
  liftM $ openCollectionFile right "w"
  foldFile liftM (\file cur_val -> do
    liftM $ doWrite file cur_val
    return ( if file == left then right else left )
    ) left self
  liftM $ close left
  liftM $ close right
  return (FileDataspace left, FileDataspace right)
