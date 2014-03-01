{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
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
  splitFile,
  sortFile
) where

import Control.Concurrent.MVar
import qualified Control.Monad.Reader as M

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
  dirExists <- M.liftIO $ DIR.doesDirectoryExist path
  M.unless dirExists $ do
    isFile <- M.liftIO $ DIR.doesFileExist path
    if isFile then
      throwEngineError $ EngineError $ path ++ " exists but is not a directory, so it cannot be used to store external collections."
    else
      M.liftIO $ DIR.createDirectory path

initDataDir :: EngineM b ()
initDataDir = do
  createDataDir rootDataPath
  engine <- M.ask
  createDataDir $ engineDataPath engine

-- Put these into __DATA/peerID/collection_name
generateCollectionFilename :: EngineM b Identifier
generateCollectionFilename = do
    engine <- M.ask
    -- TODO move creating the data dir into atInit
    initDataDir
    let counter = collectionCount engine
    number <- M.liftIO $ readMVar counter
    let filename = "collection_" ++ (show number)
    M.liftIO $ modifyMVar_ counter $ \c -> return (c + 1)
    return $ FP.joinPath [engineDataPath engine, filename]

emptyFile :: () -> EngineM a (FileDataspace a)
emptyFile _ = do
  file_id <- generateCollectionFilename
  _ <- openCollectionFile file_id "w"
  close file_id
  return $ FileDataspace file_id

openCollectionFile :: String -> String -> EngineM a String
openCollectionFile name mode =
  do
    engine <- M.ask
    let wd = valueFormat engine
    openFile name name wd Nothing mode
    return name

copyFile :: FileDataspace v -> EngineM v (FileDataspace v)
copyFile old_id = do
  new_id <- generateCollectionFilename
  _ <- openCollectionFile new_id "w"
  foldFile id (\_ val -> do
    doWrite new_id val -- TODO hasWrite
    return ()
    ) () old_id
  close new_id
  return $ FileDataspace new_id
  
initialFile :: (Monad m) => (forall c. EngineM b c -> m c) -> [b] -> m (FileDataspace b)
initialFile liftM vals = do
  new_id <- liftM generateCollectionFilename
  _ <- liftM $ openCollectionFile new_id "w"
  M.foldM (\_ val -> do
    liftM $ doWrite new_id val -- TODO hasWrite
    return ()
    ) () vals
  liftM $ close new_id
  return $ FileDataspace new_id

peekFile :: (Monad m) => (forall c. EngineM b c -> m c) -> FileDataspace b -> m (Maybe b)
peekFile liftM (FileDataspace file_id) = do
  _ <- liftM $ openCollectionFile file_id "r"
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
  _ <- liftM $ openCollectionFile file_id "r"
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
mapFile liftM function file_ds = do
  -- Map over a copy of the dataspace, instead of the original
  -- This way, any modifications to the dataspace that occur inside the
  -- body of the map do not affect the length of iteration, but
  -- are in effect upon exiting the map
  tmp_ds <- liftM $ copyFile file_ds
  new_id <- liftM generateCollectionFilename
  _ <- liftM $ openCollectionFile new_id "w"
  foldFile liftM (inner_func new_id) () tmp_ds
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
      _ <- function v
      return ()

filterFile :: (Monad m) => (forall c. EngineM b c -> m c) -> (b -> m Bool) -> FileDataspace b -> m (FileDataspace b)
filterFile liftM predicate old_id = do
  new_id <- liftM generateCollectionFilename
  _ <- liftM $ openCollectionFile new_id "w"
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
  _ <- liftM $ openCollectionFile file_id "a"
  -- can_write <- hasWrite ext_id -- TODO handle errors here
  liftM $ doWrite file_id v
  liftM $ close file_id
  return file_ds

deleteFile :: (Monad m, Eq b) => (forall c. EngineM b c -> m c) -> b -> FileDataspace b -> m (FileDataspace b)
deleteFile liftM v file_ds@(FileDataspace file_id) = do
  deleted_id <- liftM generateCollectionFilename
  _ <- liftM $ openCollectionFile deleted_id "w"
  _ <- foldFile liftM (\truth val -> do
    if truth == False && val == v
      then return True
      else do
        liftM $ doWrite deleted_id val --TODO error check
        return truth
    ) False file_ds
  liftM $ close deleted_id
  liftM $ M.liftIO $ DIR.renameFile deleted_id file_id -- What's with the double lift?
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
  M.unless did_update $ liftM $ doWrite new_id v'
  liftM $ close new_id
  liftM $ M.liftIO $ DIR.renameFile new_id file_id -- Double lift?
  return file_ds

combineFile :: (Monad m) => (forall c. EngineM b c -> m c) -> (FileDataspace b) -> (FileDataspace b) ->  m (FileDataspace b)
combineFile liftM self values = do
  (FileDataspace new_id) <- liftM $ Language.K3.Runtime.FileDataspace.copyFile self
  _ <- liftM $ openCollectionFile new_id "a"
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
  _ <- liftM $ openCollectionFile left "w"
  _ <- liftM $ openCollectionFile right "w"
  _ <- foldFile liftM (\file cur_val -> do
    liftM $ doWrite file cur_val
    return ( if file == left then right else left )
    ) left self
  liftM $ close left
  liftM $ close right
  return (FileDataspace left, FileDataspace right)

sortFile :: (Monad m) => (forall c. EngineM b c -> m c) -> (b -> b -> m Ordering) -> FileDataspace b -> m (FileDataspace b)
sortFile liftM sortF old_id = do
  -- First phase: partition into sorted runs
  (_, remainder_v, some_runs) <- foldFile liftM partition_runs (0,[],[]) old_id
  remainder_id <- sorted_run remainder_v
  let runs = remainder_id:some_runs
  case runs of
    []  -> liftM $ throwEngineError $ EngineError $ "Invalid sortFile did not create any runs."
    [x] -> return $ FileDataspace x
    _   -> do
            merged_opt <- merge_runs runs -- Second phase: merge runs.
            case merged_opt of
              Just merged_id -> return $ FileDataspace merged_id
              Nothing        -> liftM $ throwEngineError $ EngineError $ "Invalid sortFile did not merge any runs."
  
  where
    block_size = 1000000 :: Int
    partition_runs (cnt, v_acc, id_acc) v 
      | cnt < block_size = return (cnt+1, v:v_acc, id_acc)
      | otherwise = do
          -- Write out a sorted run.
          new_id <- sorted_run v_acc
          return (0, [v], new_id:id_acc)

    sorted_run l = do
      -- Write out a sorted run.
      new_id <- liftM generateCollectionFilename
      _ <- liftM $ openCollectionFile new_id "w"
      sorted_l <- sort_in_mem sortF l
      mapM_ (\v -> liftM $ doWrite new_id v) sorted_l
      liftM $ close new_id
      return new_id

    -- Balanced binary tree of merge operations.
    merge_runs []  = return Nothing
    merge_runs [x] = return $ Just x
    merge_runs ls  = do
      let (a,b) = splitAt (length ls `quot` 2) ls
      a_opt <- merge_runs a
      b_opt <- merge_runs b
      merge_pair a_opt b_opt

    -- For now, just read in the two runs and sort in memory.
    -- TODO: ideally we want to step through the two runs and merge.
    merge_pair Nothing     Nothing     = return Nothing
    merge_pair (Just a_id) Nothing     = return $ Just a_id
    merge_pair Nothing     (Just b_id) = return $ Just b_id
    merge_pair (Just a_id) (Just b_id) = 
      do
        a_vals <- foldFile liftM (\acc v -> return $ acc++[v]) [] $ FileDataspace a_id
        b_vals <- foldFile liftM (\acc v -> return $ acc++[v]) [] $ FileDataspace b_id
        sorted_vals <- sort_in_mem sortF $ a_vals ++ b_vals
        return . Just =<< sorted_run sorted_vals

    -- | Sort an in memory list of values. For now, we use the same implementation as the InMemory dataspace.
    --   Since the comparator can have side effects, we must write our own sorting algorithm.
    --   TODO: improve performance.
    sort_in_mem _ []     = return []
    sort_in_mem _ [x]    = return [x]
    sort_in_mem cmpF ls  = do
      let (a, b) = splitAt (length ls `quot` 2) ls
      a' <- sort_in_mem cmpF a
      b' <- sort_in_mem cmpF b
      merge a' b'
      where merge [] bs = return bs
            merge as [] = return as
            merge (a:as) (b:bs) = cmpF a b >>= \case
                LT -> return . (a:) =<< merge as (b:bs)
                _  -> return . (b:) =<< merge (a:as) bs
