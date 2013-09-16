{-# LANGUAGE FlexibleInstances, ScopedTypeVariables, TupleSections #-}

{-|
  This module handles command-line options for the K3 unit test system.
-}

module Language.K3.Test.Options
( parseOptions
, K3TestOptions(..)
) where

import Control.Applicative
import Control.Arrow
import Data.Either
import Data.List
import Data.Maybe
import Data.Monoid
import System.Console.GetOpt
import System.Environment
import System.Exit
import System.Log
import Test.Framework
import Test.Framework.Runners.Console

import Language.K3.Logger

{-
  TO ADD A NEW OPTION, the following changes must be made:
    1. Add an entry to the K3TestOptions record.
    2. Add a default Nothing value to the mempty implementation.
    3. Correctly implement mappend.
    4. Add an entry to the defaultOptions value.
    5. Add an entry to k3testOptions to allow it to be used on the command line.
-}

-- |Parses command line arguments for the K3 test system.
parseOptions :: [String]
             -> IO (Either (String,ExitCode) (RunnerOptions, K3TestOptions))
parseOptions args = do
  -- Set some simple defaults
  progName <- getProgName
  let usageHeader = "Usage: " ++ progName ++ " [OPTIONS]"
  -- Build a list of option descriptions for getOpt and run it
  let optDescrs = map (liftOptDescr $ Right . (,mempty)) optionsDescription
               ++ map (liftOptDescr ((Just mempty,) <$>)) k3testOptions
  let (results,nonOpts,errs) = getOpt Permute optDescrs args
  let extrasErr =
        if null nonOpts then [] else
              ["Unrecognized arguments: " ++ unwords nonOpts]
  -- Smash out the result as errors
  let (errs',parsedOptions) = partitionEithers results
  let errs'' = concat $ errs ++ map (++"\n") (errs' ++ extrasErr)
  -- Inline the defaults
  let (mtfOpts, k3tOpts) = mconcat $ (Just mempty,defaultOptions):parsedOptions
  -- Now figure out what to do with them
  let usageStr = usageInfo usageHeader optDescrs
  case (errs'',mtfOpts) of
    (_,Nothing) ->
      return $ Left (usageStr, ExitSuccess)
    (_:_,_) ->
      return $ Left (errs'' ++ usageStr, ExitFailure 1)
    ([],Just tfOpts) ->
      -- We finally know that the option parsing worked out!  Now just attach
      -- the defaults and send them home.
      return $ Right (tfOpts, defaultOptions `mappend` k3tOpts)

-- |A wrapper for logger instructions.
type LoggerInstruction = (String,Priority)

-- |The record representing additional options parsed by this testing library.
--  Each value is a @Maybe@ to allow each record to describe part of the options
--  which have been set; @mappend@ then combines these options correctly.
data K3TestOptions =
  K3TestOptions
  { loggerInstructions :: Maybe [LoggerInstruction]
  , typeSystemOnlyByName :: Maybe (Maybe String)
  }
  
instance Monoid K3TestOptions where
  mempty =
    -- Empty entries here
    K3TestOptions
    { loggerInstructions = Nothing
    , typeSystemOnlyByName = Nothing
    }
  mappend x y =
    -- Operations here to join two defined sets of behavior
    K3TestOptions
    { loggerInstructions = mappendBy (++) loggerInstructions
    , typeSystemOnlyByName = mappendBy preferJustRight typeSystemOnlyByName
    }
    where
      mappendBy :: (a -> a -> a) -> (K3TestOptions -> Maybe a) -> Maybe a
      mappendBy op prj =
        case (prj x, prj y) of
          (Just x', Just y') -> Just $ x' `op` y'
          (Nothing, Just y') -> Just y'
          (Just x', Nothing) -> Just x'
          (Nothing, Nothing) -> Nothing
      preferJustRight :: Maybe a -> Maybe a -> Maybe a
      preferJustRight a b =
        case (a,b) of
          (_, Just _) -> b
          _ -> a

-- |The default options for the K3 test system.  This record must contain no
--  @Nothing@ values; all top-level terms must be @Just@.
defaultOptions :: K3TestOptions
defaultOptions =
  -- Defaults (when options are unspecified) go here
  K3TestOptions
  { loggerInstructions = Just []
  , typeSystemOnlyByName = Just Nothing
  }

-- |The getOpt descriptions for this program.
k3testOptions :: [OptDescr (Either String K3TestOptions)]
k3testOptions =
  -- Each K3 test CLI option appears here as a getOpt description
  [ let parse s =
          case parseInstruction s of
            Left err -> Left err
            Right instr -> Right $ mempty { loggerInstructions = Just [instr] }
    in
    Option "L" ["log"] (ReqArg parse "log_cmd")
      "a logging instruction of the form PRIO or PRIO:MODULE (e.g. debug:Foo)"
  , let parse s = Right $ mempty {typeSystemOnlyByName = Just $ Just s } in 
    Option [] ["ts-only"] (ReqArg parse "filename")
      "execute only type system tests matching the provided filename"
  ]
  
-- |Lifts an existing @OptDescr@ to a new space.
liftOptDescr :: (a -> b) -> OptDescr a -> OptDescr b
liftOptDescr f (Option short long argDescr descr) =
    let argDescr' = case argDescr of
                      NoArg x -> NoArg $ f x
                      ReqArg g name -> ReqArg (f . g) name
                      OptArg g name -> OptArg (f . g) name
    in Option short long argDescr' descr
