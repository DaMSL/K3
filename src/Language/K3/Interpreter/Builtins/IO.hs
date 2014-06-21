{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

module Language.K3.Interpreter.Builtins.IO where

import Control.Monad.State

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors

import Language.K3.Utils.Pretty

genIOBuiltin :: Identifier -> K3 Type -> Maybe (Interpretation Value)

genIOBuiltin "error" _ = Just $ vfun $ \_ -> throwE $ RunTimeTypeError "Error encountered in program"

-- Show values
genIOBuiltin "show" _ = Just $ vfun $ \x -> do
    st <- get
    return $ VString $ showPC (getPrintConfig st) x

-- Log to the screen
genIOBuiltin "print" _ = Just $ vfun logString
  where logString (VString s) = do
              -- liftIO $ putStrLn s
              _notice_Function s
              return $ VTuple []
        logString x           = throwE $ RunTimeTypeError ("In 'print': Expected a string but received " ++ show x)

genIOBuiltin _ _ = Nothing
