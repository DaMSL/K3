{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE ViewPatterns #-}

-- | Generate a compile-time graph of all message sends in a K3 program.
module Language.K3.Analysis.SendGraph where

import Control.Arrow
import System.IO
import Text.Printf

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Control.Monad.State.Strict

import qualified Data.Map as M

type SendGraphS = M.Map Identifier [Identifier]

type SendGraphM = State SendGraphS

getSendMap :: SendGraphM a -> SendGraphS -> SendGraphS
getSendMap = execState

generateSendGraph :: K3 Declaration -> IO ()
generateSendGraph d = do
  let sendMap = getSendMap (declSendGraph d) []
  outFile <- openFile "graph.dot" WriteMode
  hPrintf outFile "digraph {\n"
  void $ M.traverseWithKey (\k -> mapM_ (\i -> hPrintf outFile "  \"%s\" -> \"%s\";\n" k i)) sendMap
  hPrintf outFile "}\n"
  hClose outFile

accumulateSendGraph :: K3 Declaration -> SendGraphM ()
accumulateSendGraph d = declSendGraph d

declSendGraph :: K3 Declaration -> SendGraphM ()
declSendGraph (tag -> DTrigger i _ e) = do
  sendTargets <- exprSendGraph e
  modify $ \s -> M.insertWith (++) i sendTargets s
declSendGraph (tag &&& children -> (DRole _, ds)) = mapM_ declSendGraph ds
declSendGraph _ = return ()

exprSendGraph :: K3 Expression -> SendGraphM [Identifier]
exprSendGraph e = case e of
  (tag &&& children -> (EOperate OSnd, [tag &&& children -> (ETuple, [tag -> EVariable i, _]), _])) -> return [i]
  _ -> concat <$> mapM exprSendGraph (children e)

