{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Language.K3.Runtime.Dataspace.Test (tests) where

import Control.Monad
import Control.Monad.Trans.Either
import Data.List
import Data.Maybe

import Test.HUnit hiding (Test)
import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit

import Language.K3.Interpreter
import Language.K3.Runtime.Dataspace
import Language.K3.Runtime.Engine

-- Duplicated from Interpreter.hs
vunit = VTuple []
throwE :: InterpretationError -> Interpretation a
throwE = Control.Monad.Trans.Either.left

compareDataspaceToList :: (Monad m, Dataspace m ds v, Eq v) => ds -> [v] -> m Bool
compareDataspaceToList ds l = do
  result <- foldDS innerFold (Just l) ds
  return $ case result of
    Nothing -> False
    Just _ -> True
  where
    innerFold :: (Monad m, Eq v) => Maybe [v] -> v -> m (Maybe [v])
    innerFold state cur_val = return $
      case state of
        Just lst ->
          if cur_val == head lst
            then
              Just (tail lst)
            else
              Nothing
        Nothing -> Nothing

emptyPeek :: () -> Interpretation Bool
emptyPeek _ = do
  d <- newDS ([] :: [Value]) vunit
  result <- peekDS d vunit
  --unless (isNothing result) (throwE $ RunTimeInterpretationError "peek on empty dataspace did not return Nothing")
  return (isNothing result)

testEmptyFold :: () -> Interpretation Bool
testEmptyFold _ = do
  d <- newDS ([]::[Value]) vunit
  counter <- foldDS innerFold 0 d
  return (counter == 0 ) -- (assertFailure "Fold on emtpy dataspace didn't work")
  where
    innerFold :: Int -> Value -> Interpretation Int
    innerFold cnt _ = return $ cnt + 1

test_lst = [VInt 1, VInt 2, VInt 3, VInt 4, VInt 4, VInt 100]

testInsert :: () -> Interpretation Bool
testInsert _ = do
  test_ds <- newDS ([]::[Value]) vunit
  test_ds <- foldM (\ds val -> insertDS ds val) test_ds test_lst
  compareDataspaceToList test_ds test_lst
  --return $ unless (result) (assertFailure "InsertDS test failed")
testDelete :: () -> Interpretation Bool
testDelete _ = do
  --test_ds <- newDS ([]::[Value]) vunit
  test_ds <- (initialDS test_lst :: Interpretation [Value])
  deleted <- deleteDS (VInt 3) test_ds
  deleted <- deleteDS (VInt 4) deleted
  compareDataspaceToList deleted [VInt 1, VInt 2, VInt 4, VInt 100]

testMissingDelete :: () -> Interpretation Bool
testMissingDelete _ = do
  test_ds <- (initialDS test_lst :: Interpretation [Value])
  deleted <- deleteDS (VInt 5) test_ds
  compareDataspaceToList deleted test_lst

testUpdate :: () -> Interpretation Bool
testUpdate _ = do
  test_ds <- (initialDS test_lst :: Interpretation [Value])
  updated <- updateDS (VInt 1) (VInt 4) test_ds
  compareDataspaceToList updated [VInt 4, VInt 2, VInt 3, VInt 4, VInt 4, VInt 100]

testUpdateMultiple :: () -> Interpretation Bool
testUpdateMultiple _ = do
  test_ds <- (initialDS test_lst :: Interpretation [Value])
  updated <- updateDS (VInt 4) (VInt 5) test_ds
  compareDataspaceToList updated [VInt 1, VInt 2, VInt 3, VInt 5, VInt 4, VInt 100]

testUpdateMissing :: () -> Interpretation Bool
testUpdateMissing _ = do
  test_ds <- (initialDS test_lst :: Interpretation [Value])
  updated <- updateDS (VInt 40) (VInt 5) test_ds
  compareDataspaceToList updated ( test_lst ++ [VInt 5] )

testFold :: () -> Interpretation Bool
testFold _ = do
  test_ds <- (initialDS test_lst :: Interpretation [Value])
  test_sum <- foldDS innerFold 0 test_ds
  return $ test_sum == 114
  where
    innerFold :: Int -> Value -> Interpretation Int
    innerFold acc value =
      return $ case value of
        VInt v -> acc + v
        otherwise -> -1 -- TODO throw real error

vintAdd :: Int -> Value -> Interpretation Value
vintAdd c val =
  case val of
    VInt v -> return $ VInt (v + c)
    otherwise -> return $ VInt (-1) -- TODO throw real error

testMap :: () -> Interpretation Bool
testMap _ = do
  test_ds <- (initialDS test_lst :: Interpretation [Value])
  mapped_ds <- mapDS (vintAdd 5) test_ds
  compareDataspaceToList mapped_ds [VInt 6, VInt 7, VInt 8, VInt 9, VInt 9, VInt 100]

testCombine :: () -> Interpretation Bool
testCombine _ = do
  left' <- (initialDS test_lst :: Interpretation [Value])
  right' <- (initialDS test_lst :: Interpretation [Value])
  combined <- combineDS left' right' vunit
  compareDataspaceToList combined (test_lst ++ test_lst)

sizeDS :: [Value] -> Interpretation Int
sizeDS ds = do
  foldDS innerFold 0 ds
  where
    innerFold :: Int -> Value -> Interpretation Int
    innerFold cnt _ = return $ cnt + 1
-- depends on combine working
testSplit :: () -> Interpretation Bool
testSplit _ = do
  -- split doesn't do anything if one of the collections contains less than 10 elements
  let long_lst = test_lst ++ test_lst
  first_ds <- (initialDS long_lst :: Interpretation [Value])
  (left', right') <- splitDS first_ds vunit
  leftLen <- sizeDS left'
  rightLen <- sizeDS right'
  if leftLen >= length long_lst || rightLen >= length long_lst || leftLen + rightLen > length long_lst
    then
      return False
    else do
      remainders <- foldM findAndRemoveElement (Just (left', right')) long_lst
      case remainders of
        Nothing -> return False
        Just (l, r) -> do
          lLen <- sizeDS l
          rLen <- sizeDS r
          if lLen == 0 && rLen == 0
            then
              return True
            else
              return False
  where
    findAndRemoveElement :: Maybe ([Value], [Value]) -> Value -> Interpretation (Maybe ([Value], [Value]))
    findAndRemoveElement maybeTuple cur_val =
      case maybeTuple of
        Nothing -> return Nothing
        Just (left, right) -> do
          leftContains <- containsDS left cur_val
          if leftContains
            then do
              removed_left <- deleteDS cur_val left
              return $ Just (removed_left, right)
          else do
            rightContains <- containsDS right cur_val
            if rightContains
              then do
                removed_right <- deleteDS cur_val right
                return $ Just (left, removed_right)
              else
                return Nothing
    containsDS :: [Value] -> Value -> Interpretation Bool
    containsDS ds val =
      foldDS (\fnd cur -> if cur == val then return True else return fnd) False ds
      
callTest testFunc = do
  engine <- simulationEngine [] syntaxValueWD
  interpResult <- runInterpretation engine emptyState (testFunc ())
  success <- either (const $ return False) (either (const $ return False) (return . id) . getResultVal) interpResult
  unless success (assertFailure "Dataspace test failed")
    
tests :: [Test]
tests = [
  testGroup "List Dataspace" [
        testCase "EmptyPeek" $ callTest emptyPeek
    ]
  ]
