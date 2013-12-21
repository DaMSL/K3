{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE Rank2Types #-}

module Language.K3.Runtime.FileDataspace (
  FileDataspace(..),
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

import Control.Monad.Reader

import Language.K3.Runtime.Engine

newtype FileDataspace v = FileDataspace String
getFile :: FileDataspace v -> String
getFile (FileDataspace name) = name

emptyFile :: () -> EngineM a (FileDataspace a)
emptyFile _ = do
  file_id <- generateCollectionFilename
  openCollectionFile file_id "w"
  close file_id
  return $ FileDataspace file_id

openCollectionFile :: [Char] -> String -> EngineM a ()
openCollectionFile name mode =
  do
    engine <- ask
    let wd = valueFormat engine
    openFile name name wd Nothing mode
    return ()

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
      Just True   -> do
        opt_read <- liftM $ doRead file_id
        return opt_read
  liftM $ close file_id
  return result

-- Pass a lift into these functions, so that "inner loop" can be in 
-- some Monad m.  foldDS etc. can know which lift to use, since they
-- are in the instance of the typeclass.
foldFile :: forall (m :: * -> *) a b.
            Monad m =>
            (forall c. EngineM b c -> m c)
            -> (a -> b -> m a) -> a -> FileDataspace b -> m a
foldFile liftM accumulation initial_accumulator file_id =
 do
   file_over <- liftM $ hasRead $ getFile file_id
   case file_over of
     Nothing -> return initial_accumulator -- Hiding an error...
     Just False -> return initial_accumulator
     Just True ->
       do
         cur_val <- liftM $ doRead $ getFile file_id
         case cur_val of
           Nothing -> return initial_accumulator
           Just val -> do
             new_acc <- accumulation initial_accumulator val
             foldFile liftM accumulation new_acc file_id

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
    --everything lets it figure it out
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
--filterFile :: (b -> EngineM b Bool) -> [Char] -> EngineM b [Char]
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
  return $ file_ds

deleteFile :: (Monad m, Eq b) => (forall c. EngineM b c -> m c) -> b -> FileDataspace b -> m (FileDataspace b)
deleteFile liftM v file_ds@(FileDataspace file_id) = do -- broken because reading / writing from the same file
  liftM $ openCollectionFile file_id "w"
  foldFile liftM (\truth val -> do
    if truth == False && val == v
      then return True
      else do
        liftM $ doWrite file_id val --TODO error check
        return False
    ) False file_ds
  liftM $ close file_id
  return $ file_ds

updateFile :: (Monad m, Eq b) => (forall c. EngineM b c -> m c) -> b -> b -> FileDataspace b -> m (FileDataspace b)
updateFile liftM v v' file_ds@(FileDataspace file_id) = do
  liftM $ openCollectionFile file_id "w"
  foldFile liftM (\truth val -> do
    if truth == False && val == v
      then do
        liftM $ doWrite file_id v'
        return True
      else do
        liftM $ doWrite file_id val --TODO error check
        return False
    ) False file_ds
  liftM $ close file_id
  return $ file_ds

combineFile :: (Monad m) => (forall c. EngineM b c -> m c) -> (FileDataspace b) -> (FileDataspace b) ->  m (FileDataspace b)
combineFile liftM self values = do
  liftM $ openCollectionFile (getFile self) "a"
  foldFile liftM (\_ v -> do
      liftM $ doWrite (getFile self) v
      return ()
    ) () values
  liftM $ close (getFile self)
  return self

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
