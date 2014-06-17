{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

module Language.K3.Interpreter.Builtins.Strings where

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors

import Data.String
import Data.Char

genStringBuiltin :: Identifier -> K3 Type -> Maybe (Interpretation Value)

-- substring :: int -> string -> int
genStringBuiltin "substring" _ = 
    Just $ vfun $ \(VInt n) ->
        vfun $ \(VString s) ->
            return $ (VString (take n s))

-- Constants --

genStringBuiltin "getAsciiLetters" _ = Just $ vfun $ \_ -> return $ VString "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

genStringBuiltin "getAsciiLowerCase" _ = Just $ vfun $ \_ -> return $ VString "abcdefghijklmnopqrstuvwxyz"

genStringBuiltin "getAsciiUpperCase" _ = Just $ vfun $ \_ -> return $ VString "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

genStringBuiltin "getDigits" _ = Just $ vfun $ \_ -> return $ VString "0123456789"

genStringBuiltin "getHexDigits" _ = Just $ vfun $ \_ -> return $ VString "0123456789abcdefABCDEF"

genStringBuiltin "getOctalDigits" _ = Just $ vfun $ \_ -> return $ VString "01234567"

-- Functions --

genStringBuiltin "substring" _ = 
  Just $ vfun $ \(VInt n) ->
    vfun $ \(VString s) ->
      return $ \(VString (take n s))

genStringBuiltin "getOrd" _ = Just $ vfun $ (\VString char) -> do
  if length char == 1
      return $ (VInt $ ord char)
  else
      throwE $ RunTimeTypeError ("In 'getOrd' : Expected a character but received" ++ show char)

genStringBuiltin "getChar" _ = Just $ vfun $ \(VInt num) -> do
    

genStringBuiltin "reverseStringByCharacters" _ = Just $ vfun $

genStringBuiltin "reverseStringByWords" _ = Just $ vfun $

genStringBuiltin "convertToUpper" _ = Just $ vfun $

genStringBuiltin "convertToLower" _ = Just $ vfun $


genStringBuiltin _ _ = Nothing
