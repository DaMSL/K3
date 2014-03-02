{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Annotated collection interpretation
module Language.K3.Interpreter.Collection where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.IO.Class

import Data.List

import System.Mem.StableName

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Dataspace()

import Language.K3.Runtime.Dataspace


{- Identifiers -}
collectionAnnotationId :: Identifier
collectionAnnotationId = "Collection"

externalAnnotationId :: Identifier
externalAnnotationId = "External"

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


{- Collection operations -}
emptyCollectionNamespace :: CollectionNamespace Value
emptyCollectionNamespace = CollectionNamespace [] []

emptyDataspace :: [Identifier] -> Interpretation (CollectionDataspace Value)
emptyDataspace annIds = 
  if externalAnnotationId `elem` annIds
    then emptyDS Nothing >>= return . ExternalDS
    else emptyDS Nothing >>= return . InMemoryDS

initialDataspace :: [Identifier] -> [Value] -> Interpretation (CollectionDataspace Value)
initialDataspace annIds vals =
  if externalAnnotationId `elem` annIds
    then initialDS vals Nothing >>= return . ExternalDS
    else initialDS vals Nothing >>= return . InMemoryDS

-- | Create collections with empty namespaces
emptyCollectionBody :: [Identifier] -> Interpretation (Collection Value)
emptyCollectionBody annIds =
  emptyDataspace annIds >>= \ds -> return $ Collection emptyCollectionNamespace ds $ annotationComboId annIds

initialCollectionBody :: [Identifier] -> [Value] -> Interpretation (Collection Value)
initialCollectionBody annIds vals =
  initialDataspace annIds vals >>= \ds -> return $ Collection emptyCollectionNamespace ds $ annotationComboId annIds

emptyCollection :: [Identifier] -> Interpretation Value
emptyCollection annIds = initialCollection annIds []

initialCollection :: [Identifier] -> [Value] -> Interpretation Value
initialCollection annIds vals = initialCollectionBody annIds vals >>= liftIO . newMVar >>= return . VCollection

-- | Create collections with namespaces populated based on the annotation realization.
emptyAnnotatedCollectionBody :: Identifier -> Interpretation (MVar (Collection Value))
emptyAnnotatedCollectionBody comboId = lookupACombo comboId >>= \cstrs -> emptyCtor cstrs $ ()

initialAnnotatedCollectionBody :: Identifier -> [Value] -> Interpretation (MVar (Collection Value))
initialAnnotatedCollectionBody comboId vals = lookupACombo comboId >>= \cstrs -> initialCtor cstrs $ vals

emptyAnnotatedCollection :: Identifier -> Interpretation Value
emptyAnnotatedCollection comboId = emptyAnnotatedCollectionBody comboId >>= return . VCollection

initialAnnotatedCollection :: Identifier -> [Value] -> Interpretation Value
initialAnnotatedCollection comboId vals =
  initialAnnotatedCollectionBody comboId vals >>= return . VCollection

copyCollection :: Collection Value -> Interpretation (Value)
copyCollection newC = do
  binders <- lookupACombo $ extensionId newC
  let copyCstr = copyCtor binders
  c' <- copyCstr newC
  return $ VCollection c'


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
                  namedAnnDefs <- mapM (\x -> lookupADef x >>= return . (x,)) annIds
                  modifyACombos . (:) . (comboId,) $ mkCConstructors namedAnnDefs
      Just _  -> return ()

    mkCConstructors :: [(Identifier, IEnvironment Value)] -> CollectionConstructors Value
    mkCConstructors namedAnnDefs =
      CollectionConstructors (mkEmptyConstructor   namedAnnDefs)
                             (mkInitialConstructor namedAnnDefs)
                             (mkCopyConstructor    namedAnnDefs)
                             (mkEmplaceConstructor namedAnnDefs)

    mkEmptyConstructor :: [(Identifier, IEnvironment Value)] -> CEmptyConstructor Value
    mkEmptyConstructor namedAnnDefs = const $ do
      collection <- emptyCollectionBody (map fst namedAnnDefs)
      newCMV     <- liftIO $ newMVar collection
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      void $ mapM_ (bindAnnotationDef newCMV) namedAnnDefs
      return $ newCMV
    
    mkInitialConstructor :: [(Identifier, IEnvironment Value)] -> CInitialConstructor Value
    mkInitialConstructor namedAnnDefs = \vals -> do
      collection <- initialCollectionBody (map fst namedAnnDefs) vals
      newCMV     <- liftIO $ newMVar collection
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      void $ mapM_ (bindAnnotationDef newCMV) namedAnnDefs
      return $ newCMV
    
    mkCopyConstructor :: [(Identifier, IEnvironment Value)] -> CCopyConstructor Value
    mkCopyConstructor namedAnnDefs = \coll -> do
      newDataSpace <- copyDS (dataspace coll)
      let newcol = Collection (namespace coll) (newDataSpace) (extensionId coll)
      newCMV <- liftIO (newMVar newcol)
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      return newCMV
   
    mkEmplaceConstructor :: [(Identifier, IEnvironment Value)] -> CEmplaceConstructor Value
    mkEmplaceConstructor namedAnnDefs = \dataspace -> do
      let newcol = Collection emptyCollectionNamespace dataspace $ annotationComboId $ map fst namedAnnDefs
      newCMV <- liftIO (newMVar newcol)
      void $ mapM_ (rebindFunctionsInEnv newCMV) namedAnnDefs
      void $ mapM_ (bindAnnotationDef newCMV) namedAnnDefs
      return newCMV

    bindAnnotationDef :: MVar (Collection Value) -> (Identifier, IEnvironment Value) -> Interpretation ()
    bindAnnotationDef cmv (n, env) = mapM_ (bindMember cmv n) env

    bindMember _ _ (_, VFunction _) = return ()
    bindMember cmv annId (n, v) =
      liftIO $ modifyMVar_ cmv (\(Collection (CollectionNamespace cns ans) ds extId) ->
        let (cns', ans') = extendNamespace cns ans annId n v
        in return $ Collection (CollectionNamespace cns' ans') ds extId)

    rebindFunctionsInEnv :: MVar (Collection Value) -> (Identifier, IEnvironment Value) -> Interpretation ()
    rebindFunctionsInEnv cmv (n, env) = mapM_ (rebindFunction cmv n) env
    
    rebindFunction cmv annId (n, VFunction f) =
      liftIO $ modifyMVar_ cmv (\(Collection (CollectionNamespace cns ans) ds extId) ->
        let newF         = contextualizeFunction cmv f
            (cns', ans') = extendNamespace cns ans annId n newF
        in return $ Collection (CollectionNamespace cns' ans') ds extId)
    
    rebindFunction _ _ _ = return ()

    extendNamespace cns ans annId n v =
      let cns'   = replaceAssoc cns n v
          annEnv = maybe Nothing (\env -> Just $ replaceAssoc env n v) $ lookup annId ans
          ans'   = maybe ans (replaceAssoc ans annId) annEnv
      in (cns', ans')

-- | Creates a contextualized collection member function. That is, the member function
--   will add all collection members to the interpretation environment prior to its
--   evaluation. This way, the member function's body can directly refer to other members
--   by name, rather than using the 'self' keyword.
contextualizeFunction :: MVar (Collection Value)
                      -> (IFunction, Closure Value, StableName IFunction)
                      -> Value
contextualizeFunction cmv (f, cl, n) = VFunction . (, cl, n) $ \x -> do
      bindings <- liftCollection
      result   <- f x >>= return . contextualizeResult
      lowerCollection bindings result

  where
    contextualizeResult (VFunction f') = contextualizeFunction cmv f'
    contextualizeResult r = r

    -- TODO:
    -- i. lift/lower data segment.
    -- ii. handle aliases, e.g., 'self.x' and 'x'. 
    --     Also, this needs to be done more generally for bind as expressions.
    -- iii. handle lowering of annotation-specific namespaces
    liftCollection = do
      Collection (CollectionNamespace cns _) _ _ <- liftIO $ readMVar cmv
      bindings <- return $ cns ++ [(annotationSelfId, VCollection cmv)]
      void $ modifyE (bindings ++)
      return bindings

    lowerCollection bindings result = do
        newNsInfo <- lowerBindings bindings
        void $ liftIO $ modifyMVar_ cmv $ \(Collection ns ds extId) -> 
          return (Collection (rebuildNamespace ns newNsInfo) ds extId)
        foldM (flip removeE) result bindings

    lowerBindings env =
      mapM (\(n,_) -> lookupE n >>= return . (n,)) env 
        >>= return . partition ((annotationSelfId /= ) . fst)

    -- TODO: rebind annotation-specific namespace
    rebuildNamespace ns (newGlobalNS, _) =
      CollectionNamespace newGlobalNS $ annotationNS ns
