{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter.Utils where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad.State

import Data.Function
import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Values
import Language.K3.Interpreter.Dataspace()

import Language.K3.Runtime.Engine
import Language.K3.Runtime.Dataspace

import Language.K3.Utils.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["Dispatch", "BindPath"])

{- Misc. helpers-}
details :: K3 a -> (a, [K3 a], [Annotation a])
details (Node (tg :@: anns) ch) = (tg, ch, anns)

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
contextualizeFunction :: MVar (Collection Value) -> (Value -> Interpretation Value, Closure Value) -> Value
contextualizeFunction cmv (f, cl) = VFunction . (, cl) $ \x -> do
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


{- Pretty printing -}
prettyValue :: Value -> IO String
prettyValue = packValueSyntax False

prettyResultValue :: Either InterpretationError Value -> IO [String]
prettyResultValue (Left err)  = return ["Error: " ++ show err]
prettyResultValue (Right val) = prettyValue val >>= return . (:[]) . ("Value: " ++)

prettyEnv :: IEnvironment Value -> IO [String]
prettyEnv env = do
    nWidth   <- return . maximum $ map (length . fst) env
    bindings <- mapM (prettyEnvEntry nWidth) $ sortBy (on compare fst) env
    return $ ["Environment:"] ++ concat bindings 
  where 
    prettyEnvEntry w (n,v) =
      prettyValue v >>= return . shift (prettyName w n) (prefixPadTo (w+4) " .. ") . wrap 70

    prettyName w n    = (suffixPadTo w n) ++ " => "
    suffixPadTo len n = n ++ replicate (max (len - length n) 0) ' '
    prefixPadTo len n = replicate (max (len - length n) 0) ' ' ++ n

prettyIResult :: IResult Value -> EngineM Value [String]
prettyIResult ((res, st), _) =
  liftIO ((++) <$> prettyResultValue res <*> prettyEnv (getEnv st))

showIResult :: IResult Value -> EngineM Value String
showIResult r = prettyIResult r >>= return . boxToString


{- Debugging helpers -}

debugDecl :: (Show a, Pretty b) => a -> b -> c -> c
debugDecl n t = _debugI . boxToString $
  [concat ["Declaring ", show n, " : "]] ++ (indent 2 $ prettyLines t)

showState :: String -> IState -> EngineM Value [String]
showState str st = return $ [str ++ " { "] ++ (indent 2 $ prettyLines st) ++ ["}"]

showResult :: String -> IResult Value -> EngineM Value [String]
showResult str r = 
  prettyIResult r >>= return . ([str ++ " { "] ++) . (++ ["}"]) . indent 2

showDispatch :: Address -> Identifier -> Value -> IResult Value -> EngineM Value [String]
showDispatch addr name args r =
    wrap' <$> liftIO (prettyValue args)
         <*> (showResult "BEFORE" r >>= return . indent 2)
  where
    wrap' arg res =  ["", "TRIGGER " ++ name ++ " " ++ show addr ++ " { "]
                  ++ ["  Args: " ++ arg] 
                  ++ res ++ ["}"]

logState :: String -> Maybe Address -> IState -> EngineM Value ()
logState tag' addr st = do
    msg <- showState (tag' ++ (maybe "" show $ addr)) st
    void $ _notice_Dispatch $ boxToString msg

logResult :: String -> Maybe Address -> IResult Value -> EngineM Value ()
logResult tag' addr r = do
    msg <- showResult (tag' ++ (maybe "" show $ addr)) r 
    void $ _notice_Dispatch $ boxToString msg

logTrigger :: Address -> Identifier -> Value -> IResult Value -> EngineM Value ()
logTrigger addr n args r = do
    msg <- showDispatch addr n args r
    void $ _notice_Dispatch $ boxToString msg

logIState :: Interpretation ()
logIState = get >>= liftIO . putStrLn . pretty

logBindPath :: Interpretation ()
logBindPath = getBindPath >>= void . _notice_BindPath . ("BIND PATH: "++) . show

{- Instances -}
instance Pretty IState where
  prettyLines istate =
         ["Environment:"] ++ (indent 2 $ map prettyEnvEntry $ sortBy (on compare fst) (getEnv istate))
      ++ ["Annotations:"] ++ (indent 2 $ lines $ show $ getAnnotEnv istate)
      ++ ["Static:"]      ++ (indent 2 $ lines $ show $ getStaticEnv istate)
      ++ ["Aliases:"]     ++ (indent 2 $ lines $ show $ getBindStack istate)
    where
      prettyEnvEntry (n,v) = n ++ replicate (maxNameLength - length n) ' ' ++ " => " ++ show v
      maxNameLength        = maximum $ map (length . fst) (getEnv istate)

instance (Pretty a) => Pretty (IResult a) where
    prettyLines ((r, st), _) = ["Status: "] ++ either ((:[]) . show) prettyLines r ++ prettyLines st

instance (Pretty a) => Pretty [(Address, IResult a)] where
    prettyLines l = concatMap (\(x,y) -> [""] ++ prettyLines x ++ (indent 2 $ prettyLines y)) l

instance (Show v) => Show (AEnvironment v) where
  show (AEnvironment defs _) = show defs
