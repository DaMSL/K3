{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Annotated collection interpretation
module Language.K3.Interpreter.Collection where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.State

import Data.List
import Data.List.Split ( splitOn )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Dataspace()
import Language.K3.Interpreter.Utils

import Language.K3.Runtime.Dataspace
import Language.K3.Utils.Pretty


{- Identifiers -}
collectionAnnotationId :: Identifier
collectionAnnotationId = "Collection"

sequentialAnnotationId :: Identifier
sequentialAnnotationId = "Seq"

setAnnotationId :: Identifier
setAnnotationId = "Set"

sortedAnnotationId :: Identifier
sortedAnnotationId = "Sorted"

externalAnnotationId :: Identifier
externalAnnotationId = "External"

dataspaceAnnotationIds :: [Identifier]
dataspaceAnnotationIds =
  [collectionAnnotationId, sequentialAnnotationId,
   setAnnotationId, sortedAnnotationId, externalAnnotationId]

annotationSelfId :: Identifier
annotationSelfId = "self"
  -- This is a keyword in the language, thus no binding to it can exist beforehand.

annotationDataId :: Identifier
annotationDataId = "data"
  -- TODO: make this a keyword in the syntax.

annotationComboId :: [Identifier] -> Identifier
annotationComboId annIds = intercalate ";" annIds

annotationComboIdT :: [Annotation Type] -> Maybe Identifier
annotationComboIdT (namedTAnnotations -> [])  = Nothing
annotationComboIdT (namedTAnnotations -> ids) = Just $ annotationComboId ids

annotationComboIdE :: [Annotation Expression] -> Maybe Identifier
annotationComboIdE (namedEAnnotations -> [])  = Nothing
annotationComboIdE (namedEAnnotations -> ids) = Just $ annotationComboId ids

annotationComboIdL :: [Annotation Literal] -> Maybe Identifier
annotationComboIdL (namedLAnnotations -> [])  = Nothing
annotationComboIdL (namedLAnnotations -> ids) = Just $ annotationComboId ids

annotationIdsOfCombo :: Identifier -> [Identifier]
annotationIdsOfCombo s = filter (not . null) $ splitOn ";" s 


{- Collection operations -}

emptyAnnotationNamespace :: [(Identifier, NamedMembers Value)]
emptyAnnotationNamespace = []

emptyCollectionNamespace :: CollectionNamespace Value
emptyCollectionNamespace = CollectionNamespace emptyMembers emptyAnnotationNamespace

emptyDataspace :: [Identifier] -> Interpretation (CollectionDataspace Value)
emptyDataspace annIds = case annIds `intersect` dataspaceAnnotationIds of
  []                                -> emptyDS Nothing >>= return . InMemDS . MemDS
  [x] | x == collectionAnnotationId -> emptyDS Nothing >>= return . InMemDS . MemDS
  [x] | x == sequentialAnnotationId -> emptyDS Nothing >>= return . InMemDS . SeqDS
  [x] | x == setAnnotationId        -> emptyDS Nothing >>= return . InMemDS . SetDS
  [x] | x == sortedAnnotationId     -> emptyDS Nothing >>= return . InMemDS . SortedDS
  [x] | x == externalAnnotationId   -> emptyDS Nothing >>= return . ExternalDS
  _   -> throwE $ RunTimeInterpretationError "Ambiguous collection type based on annotations."

initialDataspace :: [Identifier] -> [Value] -> Interpretation (CollectionDataspace Value)
initialDataspace annIds vals = case annIds `intersect` dataspaceAnnotationIds of
  []                                -> initialDS vals Nothing >>= return . InMemDS . MemDS
  [x] | x == collectionAnnotationId -> initialDS vals Nothing >>= return . InMemDS . MemDS
  [x] | x == sequentialAnnotationId -> initialDS vals Nothing >>= return . InMemDS . SeqDS
  [x] | x == setAnnotationId        -> initialDS vals Nothing >>= return . InMemDS . SetDS
  [x] | x == sortedAnnotationId     -> initialDS vals Nothing >>= return . InMemDS . SortedDS
  [x] | x == externalAnnotationId   -> initialDS vals Nothing >>= return . ExternalDS
  _   -> throwE $ RunTimeInterpretationError "Ambiguous collection type based on annotations."

-- | Create collections with empty namespaces.
--   These are internal methods that should not be used directly, since they 
--   produce partially consistent collections (i.e. w/ inconsistencies between the comboId and namespace).
emptyCollectionBody :: [Identifier] -> Interpretation (Collection Value)
emptyCollectionBody annIds =
  emptyDataspace annIds >>= \ds -> return $ Collection emptyCollectionNamespace ds $ annotationComboId annIds

initialCollectionBody :: [Identifier] -> [Value] -> Interpretation (Collection Value)
initialCollectionBody annIds vals =
  initialDataspace annIds vals >>= initialCollectionBodyDS annIds

initialCollectionBodyDS :: [Identifier] -> CollectionDataspace Value -> Interpretation (Collection Value)
initialCollectionBodyDS annIds ds =
  return $ Collection emptyCollectionNamespace ds $ annotationComboId annIds

emptyCollection :: [Identifier] -> Interpretation Value
emptyCollection annIds = initialCollection annIds []

initialCollection :: [Identifier] -> [Value] -> Interpretation Value
initialCollection annIds vals = do
  c <- initialCollectionBody annIds vals
  freshCollection c

initialCollectionDS :: [Identifier] -> CollectionDataspace Value -> Interpretation Value
initialCollectionDS annIds ds = do
  c <- initialCollectionBodyDS annIds ds
  freshCollection c

-- | Create annotated collections using constructors.
--   These methods should be used to create consistent collections in the interpreter code.
emptyAnnotatedCollection :: Identifier -> Interpretation Value
emptyAnnotatedCollection comboId = lookupACombo comboId >>= \cstrs -> emptyCtor cstrs $ ()

initialAnnotatedCollection :: Identifier -> [Value] -> Interpretation Value
initialAnnotatedCollection comboId vals = lookupACombo comboId >>= \cstrs -> initialCtor cstrs $ vals

copyCollection :: Collection Value -> Interpretation Value
copyCollection newC = lookupACombo (realizationId newC) >>= \cstrs -> (copyCtor cstrs) newC


{- Collection tying and value construction helpers. -}

-- | Tie a self reference and a collection value, assuming the collection member methods
--   have already been contextualized to the self reference. 
tieContextualizedCollection :: MVar Value -> Collection Value -> Interpretation Value
tieContextualizedCollection selfMV c = do
  result <- return $ VCollection (selfMV, c)
  success <- liftIO $ tryPutMVar selfMV result
  if success then return result
             else throwE $ RunTimeInterpretationError "Failed to tie a collection with tryPutMVar"

-- | Tie a self reference and a collection value, contextualizing the collection members as necessary.
tieCollection :: MVar Value -> Collection Value -> Interpretation Value
tieCollection selfMV (Collection ns ds cId) = do
  let annIds = annotationIdsOfCombo cId
  cAnnDefs <- mapM (\x -> lookupADef x >>= return . (x,)) annIds
  nns      <- recontextualizeAnnDefs selfMV ns cAnnDefs
  result   <- return $ VCollection (selfMV, Collection nns ds cId)
  success  <- liftIO $ tryPutMVar selfMV result
  if success then return result
             else throwE $ RunTimeInterpretationError "Failed to tie a collection with tryPutMVar"

freshCollection :: Collection Value -> Interpretation Value
freshCollection c = do
  selfMV <- liftIO $ newEmptyMVar
  tieCollection selfMV c

-- | Return a fresh, unshared collection value after synchronization.
freshenValue :: Value -> Interpretation Value
freshenValue v@(VCollection _) = syncCollection v >>= \case
  VCollection (_,c2) -> freshCollection c2
  _                  -> throwE $ RunTimeInterpretationError "Invalid collection value"
freshenValue v = return v


{- Annotation realization and constructor function utilities -}

-- | Annotation composition retrieval and registration.
getComposedAnnotationT :: [Annotation Type] -> Interpretation (Maybe Identifier)
getComposedAnnotationT anns = getComposedAnnotation $ namedTAnnotations anns
  --(annotationComboIdT anns, namedTAnnotations anns)

getComposedAnnotationE :: [Annotation Expression] -> Interpretation (Maybe Identifier)
getComposedAnnotationE anns = getComposedAnnotation $ namedEAnnotations anns
  --(annotationComboIdE anns, namedEAnnotations anns) 

getComposedAnnotationL :: [Annotation Literal] -> Interpretation (Maybe Identifier)
getComposedAnnotationL anns = getComposedAnnotation $ namedLAnnotations anns
  --(annotationComboIdL anns, namedLAnnotations anns) 

getComposedAnnotation :: [Identifier] -> Interpretation (Maybe Identifier)
getComposedAnnotation annIds = case annIds of
  [] -> return Nothing
  _  -> do 
          let comboId = annotationComboId annIds
          realizationOpt <- tryLookupACombo comboId
          void $ initializeComposition comboId realizationOpt
          return $ Just comboId
  where
    initializeComposition comboId = \case
      Nothing -> do
                  cAnnDefs <- mapM (\x -> lookupADef x >>= return . (x,)) annIds
                  modifyACombos . (:) . (comboId,) $ mkCConstructors cAnnDefs
      Just _  -> return ()

    injectNamespace :: Collection Value -> CollectionNamespace Value -> Collection Value
    injectNamespace (Collection _ ds cId) ns = Collection ns ds cId

    injectDataspace :: Collection Value -> CollectionDataspace Value -> Collection Value
    injectDataspace (Collection ns _ cId) ds = Collection ns ds cId

    mkContextualizedCollection :: [(Identifier, NamedMembers Value)] -> Collection Value
                               -> Interpretation Value
    mkContextualizedCollection cAnnDefs cSkeleton = do
      selfMV      <- liftIO $ newEmptyMVar
      nns         <- contextualizeAnnDefs selfMV cAnnDefs
      completeCol <- return $ injectNamespace cSkeleton nns
      tieContextualizedCollection selfMV completeCol

    mkCConstructors :: [(Identifier, NamedMembers Value)] -> CollectionConstructors Value
    mkCConstructors cAnnDefs =
      CollectionConstructors (mkEmptyConstructor   cAnnDefs)
                             (mkInitialConstructor cAnnDefs)
                             (mkCopyConstructor    cAnnDefs)
                             (mkEmplaceConstructor cAnnDefs)

    mkEmptyConstructor :: [(Identifier, NamedMembers Value)] -> CEmptyConstructor Value
    mkEmptyConstructor cAnnDefs = const $
      emptyCollectionBody (map fst cAnnDefs) >>= mkContextualizedCollection cAnnDefs

    mkInitialConstructor :: [(Identifier, NamedMembers Value)] -> CInitialConstructor Value
    mkInitialConstructor cAnnDefs = \vals -> 
      initialCollectionBody (map fst cAnnDefs) vals >>= mkContextualizedCollection cAnnDefs

    mkEmplaceConstructor :: [(Identifier, NamedMembers Value)] -> CEmplaceConstructor Value
    mkEmplaceConstructor cAnnDefs = \ds ->
      initialCollectionBodyDS (map fst cAnnDefs) ds >>= mkContextualizedCollection cAnnDefs

    mkCopyConstructor :: [(Identifier, NamedMembers Value)] -> CCopyConstructor Value
    mkCopyConstructor cAnnDefs = \coll -> do
      nds         <- copyDS (dataspace coll)
      partialCol  <- return $ injectDataspace coll nds
      selfMV      <- liftIO $ newEmptyMVar
      nns         <- recontextualizeAnnDefs selfMV (namespace partialCol) cAnnDefs
      completeCol <- return $ injectNamespace partialCol nns
      tieContextualizedCollection selfMV completeCol


contextualizeAnnDefs :: MVar Value -> [(Identifier, NamedMembers Value)]
                     -> Interpretation (CollectionNamespace Value)
contextualizeAnnDefs selfMV cAnnDefs = do
  (cns, ans) <- foldM (contextualizeAnnDef False selfMV)
                      (emptyMembers, emptyAnnotationNamespace) cAnnDefs
  return $ CollectionNamespace cns ans

recontextualizeAnnDefs :: MVar Value -> CollectionNamespace Value -> [(Identifier, NamedMembers Value)]
                       -> Interpretation (CollectionNamespace Value)
recontextualizeAnnDefs selfMV ns cAnnDefs = do
  (cns, ans) <- foldM (contextualizeAnnDef True selfMV)
                      (collectionNS ns, annotationNS ns) cAnnDefs
  return $ CollectionNamespace cns ans

contextualizeAnnDef :: Bool -> MVar Value
                    -> (NamedMembers Value, [(Identifier, NamedMembers Value)]) 
                    -> (Identifier, NamedMembers Value)
                    -> Interpretation (NamedMembers Value, [(Identifier, NamedMembers Value)])
contextualizeAnnDef True selfMV (cns, ans) (annId, annDef) = do
    annForC <- foldMembers (insertFunctionMems selfMV) emptyMembers annDef
    ncns    <- foldMembers replaceDup cns annForC 
    nans    <- return $ replaceAssoc ans annId annForC
    return (ncns, nans)

  where insertFunctionMems mv mems i (v@(VFunction _), q) =
          contextualizeFunction mv (v,q) >>= \vq -> return $ insertMember i vq mems
        insertFunctionMems _ mems _ _ = return mems

        replaceDup mems n vq = return $ insertMember n vq mems

contextualizeAnnDef False selfMV (cns, ans) (annId, annDef) = do
    annForC <- mapMembers (const $ contextualizeFunction selfMV) annDef
    ncns    <- foldMembers insertNonDup cns annForC 
    nans    <- return $ (annId, annForC):ans
    return (ncns, nans)

  where insertNonDup mems n vq =
          maybe (return $ insertMember n vq mems) (duplicateError n) $ lookupMember n mems

        duplicateError n _ =
          throwE $ RunTimeInterpretationError $ "Duplicate annotation member detected for: " ++ n


-- | Creates a contextualized collection member function. That is, the member function
--   will add all collection members to the interpretation environment prior to its
--   evaluation. This way, the member function's body can directly refer to other members
--   by name, rather than using the 'self' keyword.
--
-- TODO:
-- i. lift/lower data segment.
-- ii. lift/lower annotation namespaces
contextualizeFunction :: MVar Value -> (Value, VQualifier)
                      -> Interpretation (Value, VQualifier)
contextualizeFunction selfMV (VFunction (f,cl,tg), mq) =
  return . (, mq) . VFunction . (, cl, tg) $ \x -> do
    bindings <- liftCollection
    result <- f x >>= contextualizeFunction selfMV . (, MemImmut) >>= return . fst
    lowerCollection bindings
    return result

  where
    liftCollection = liftIO (readMVar selfMV) >>= \case
        selfV@(VCollection (s, Collection (CollectionNamespace cns _) _ _)) -> do
          if s == selfMV
            then do
              insertE annotationSelfId $ IVal selfV
              bindMembers cns
            else throwE $ RunTimeInterpretationError "Invalid cyclic self value"

        _ -> throwE $ RunTimeInterpretationError "Invalid self value for a collection"

    lowerCollection bindings = do
      void $ removeE annotationSelfId ()
      cns <- unbindMembers bindings
      void $ liftIO $ modifyMVar_ selfMV
        $ \(VCollection (_,c)) -> return $ VCollection (selfMV, rebuildNamespace c cns)

    rebuildNamespace (Collection (CollectionNamespace _ ans) ds cId) ncns =
      Collection (CollectionNamespace ncns ans) ds cId

contextualizeFunction _ vq = return vq
