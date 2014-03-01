{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter.Utils where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Function
import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Interpreter.DataTypes
import Language.K3.Interpreter.Values

import Language.K3.Runtime.Engine

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

initialCollectionBody :: Identifier -> [Value] -> Collection Value
initialCollectionBody n vals = Collection emptyCollectionNamespace (InMemoryDS vals) n

-- | These methods create a valid namespace by performing a lookup based on the annotation combo id.
-- TODO any dataspace
initialAnnotatedCollectionBody :: Identifier -> [Value] -> Interpretation (MVar (Collection Value))
initialAnnotatedCollectionBody comboId vals = do
    binders <- lookupACombo comboId
    let initF = initialCtor binders
    cmv        <- initF vals
    -- TODO does this line even do anything?
    void $ liftIO (modifyMVar_ cmv (\(Collection ns ds cid) -> return $ Collection ns ds cid))
    return cmv

-- TODO any dataspace
-- Used to interpret literals, so InMemory?
initialCollection :: [Value] -> Interpretation Value
initialCollection vals = liftIO (newMVar $ initialCollectionBody "" vals) >>= return . VCollection

-- This gets used as the default collection, so it's an InMemory store
emptyCollection :: Interpretation Value
emptyCollection = initialCollection []

initialAnnotatedCollection :: Identifier -> [Value] -> Interpretation Value
initialAnnotatedCollection comboId vals =
  initialAnnotatedCollectionBody comboId vals >>= return . VCollection

emptyAnnotatedCollection :: Identifier -> Interpretation Value
emptyAnnotatedCollection comboId = initialAnnotatedCollection comboId []

copyCollection :: Collection Value -> Interpretation (Value)
copyCollection newC = do
  binders <- lookupACombo $ extensionId newC
  let copyCstr = copyCtor binders
  c'            <- copyCstr newC
  return $ VCollection c'


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
