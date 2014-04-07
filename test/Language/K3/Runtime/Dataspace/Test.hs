{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Language.K3.Runtime.Dataspace.Test (tests) where

import Control.Monad
import Control.Monad.Trans.Either
import Data.List
import Data.Maybe

import Test.HUnit hiding (Test)
import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit

import Language.K3.Runtime.Common
import Language.K3.Interpreter
import Language.K3.Runtime.Dataspace
import Language.K3.Runtime.Engine
import Language.K3.Runtime.FileDataspace
import Language.K3.Interpreter.Values
import Language.K3.Interpreter.Data.Accessors

compareDataspaceToList :: (Dataspace Interpretation ds Value) => ds -> [Value] -> Interpretation Bool
compareDataspaceToList dataspace l = do
  ds <- foldM findAndRemoveElement dataspace l
  s <- sizeDS ds
  if s == 0 then return True else throwE $ RunTimeInterpretationError $ "Dataspace had " ++ (show s) ++ " extra elements"
  where
    findAndRemoveElement :: (Dataspace Interpretation ds Value) => ds -> Value -> Interpretation (ds)
    findAndRemoveElement ds cur_val = do
      contains <- containsDS ds cur_val
      if contains
        then do
          removed <- deleteDS cur_val ds
          return $ removed
        else
          throwE $ RunTimeInterpretationError $ "Could not find element " ++ (show cur_val)

emptyPeek :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
emptyPeek dataspace _ = do
  d <- emptyDS (Just dataspace)
  result <- peekDS d
  return (isNothing result)

testEmptyFold :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testEmptyFold dataspace _ = do
  d <- emptyDS (Just dataspace)
  counter <- foldDS innerFold 0 d
  return (counter == 0 )
  where
    innerFold :: Int -> Value -> Interpretation Int
    innerFold cnt _ = return $ cnt + 1

test_lst = [VInt 1, VInt 2, VInt 3, VInt 4, VInt 4, VInt 100]

testPeek :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testPeek dataspace _ = do
  test_ds <- initialDS test_lst (Just dataspace)
  peekResult <- peekDS test_ds
  case peekResult of
    Nothing -> throwE $ RunTimeInterpretationError "Peek returned nothing!"
    Just v -> containsDS test_ds v

testInsert :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testInsert dataspace _ = do
  test_ds <- emptyDS (Just dataspace)
  built_ds <- foldM (\ds val -> insertDS ds val) test_ds test_lst
  compareDataspaceToList built_ds test_lst

testDelete :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testDelete dataspace _ = do
  test_ds <- initialDS test_lst (Just dataspace)
  test_ds <- deleteDS (VInt 3) test_ds
  test_ds <- deleteDS (VInt 4) test_ds
  compareDataspaceToList test_ds [VInt 1, VInt 2, VInt 4, VInt 100]

testMissingDelete :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testMissingDelete dataspace _ = do
  test_ds <- initialDS test_lst (Just dataspace)
  deleted <- deleteDS (VInt 5) test_ds
  compareDataspaceToList deleted test_lst

testUpdate :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testUpdate dataspace _ = do
  updated <- initialDS test_lst (Just dataspace) >>= updateDS (VInt 1) (VInt 4)
  compareDataspaceToList updated [VInt 4, VInt 2, VInt 3, VInt 4, VInt 4, VInt 100]

testUpdateMultiple :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testUpdateMultiple dataspace _ = do
  test_ds <- initialDS test_lst (Just dataspace)
  updated <- updateDS (VInt 4) (VInt 5) test_ds
  compareDataspaceToList updated [VInt 1, VInt 2, VInt 3, VInt 5, VInt 4, VInt 100]

testUpdateMissing :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testUpdateMissing dataspace _ = do
  test_ds <- initialDS test_lst (Just dataspace)
  updated <- updateDS (VInt 40) (VInt 5) test_ds
  compareDataspaceToList updated ( test_lst ++ [VInt 5] )

testFold :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testFold dataspace _ = do
  test_ds <- initialDS test_lst (Just dataspace)
  test_sum <- foldDS innerFold 0 test_ds
  return $ test_sum == 114
  where
    innerFold :: Int -> Value -> Interpretation Int
    innerFold acc value =
      case value of
        VInt v -> return $ acc + v
        otherwise -> throwE $ RunTimeTypeError "Exepected Int in folding test"

vintAdd :: Int -> Value -> Value
vintAdd c val =
  case val of
    VInt v -> VInt (v + c)
    otherwise -> VInt (-1) -- TODO throw real error

testMap :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testMap dataspace _ = do
  test_ds <- initialDS test_lst (Just dataspace)
  mapped_ds <- mapDS (return . (vintAdd 5)) test_ds
  compareDataspaceToList mapped_ds [VInt 6, VInt 7, VInt 8, VInt 9, VInt 9, VInt 105]

testFilter :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testFilter dataspace _ = do
  filtered_ds <- initialDS test_lst (Just dataspace) >>= filterDS (\(VInt v) -> return $ v > 50)
  compareDataspaceToList filtered_ds [VInt 100]

testCombine :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testCombine dataspace _ = do
  left' <- initialDS test_lst (Just dataspace)
  right' <- initialDS test_lst (Just dataspace)
  combined <- combineDS left' right'
  compareDataspaceToList combined (test_lst ++ test_lst)

testCombineSelf :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testCombineSelf dataspace _ = do
  self <- initialDS test_lst (Just dataspace)
  combined <- combineDS self self
  compareDataspaceToList combined (test_lst ++ test_lst)

sizeDS :: (Monad m, Dataspace m ds Value) => ds -> m Int
sizeDS ds = do
  foldDS innerFold 0 ds
  where
    innerFold :: (Monad m) => Int -> Value -> m Int
    innerFold cnt _ = return $ cnt + 1
-- depends on combine working
testSplit :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
testSplit dataspace _ = do
  -- split doesn't do anything if one of the collections contains less than 10 elements
  let long_lst = test_lst ++ test_lst
  first_ds <- initialDS long_lst (Just dataspace)
  (left', right') <- splitDS first_ds
  leftLen <- sizeDS left'
  rightLen <- sizeDS right'
  --TODO should the >= be just > ?
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
    findAndRemoveElement :: (Dataspace Interpretation ds Value) => Maybe (ds, ds) -> Value -> Interpretation (Maybe (ds, ds))
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

-- TODO These just makes sure that nothing crashes, but should probably check for correctness also
insertInsideMap :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
insertInsideMap dataspace _ = do
  outer_ds <- initialDS test_lst (Just dataspace)
  result_ds <- mapDS (\cur_val -> do
    insertDS outer_ds (VInt 256);
    return $ VInt 4
    ) outer_ds
  return True
insertInsideMap_ :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
insertInsideMap_ dataspace _ = do
  outer_ds <- initialDS test_lst (Just dataspace)
  mapDS_ (\cur_val -> do
    insertDS outer_ds (VInt 256)
    return $ VInt 4
    ) outer_ds
  return True
insertInsideFilter :: (Dataspace Interpretation ds Value) => ds -> () -> Interpretation Bool
insertInsideFilter dataspace _ = do
  outer_ds <- initialDS test_lst (Just dataspace)
  result_ds <- filterDS (\cur_val -> do
    insertDS outer_ds (VInt 256);
    return True
    ) outer_ds
  return True

containsDS :: (Monad m, Dataspace m ds Value) => ds -> Value -> m Bool
containsDS ds val =
  foldDS (\fnd cur -> return $ fnd || cur == val) False ds

callTest :: (() -> Interpretation Bool) -> IO ()
callTest testFunc = do
  emptyEnv <- emptyStaticEnvIO
  engine <- simulationEngine [] False defaultSystem (syntaxValueWD emptyEnv)
  eState <- emptyStateIO
  interpResult <- runInterpretation engine eState (testFunc ())
  success <- either (return . Just . show) (either (return . Just . show) (\good -> if good then return Nothing else return $ Just "Dataspace test failed") . getResultVal) interpResult
  case success of
    Nothing -> return ()
    Just msg -> assertFailure msg


makeTestGroup :: (Dataspace Interpretation dataspace Value) => String -> dataspace -> Test
makeTestGroup name ds =
  testGroup name [
        testCase "EmptyPeek" $ callTest $ emptyPeek ds,
        testCase "Fold on Empty List Test" $ callTest $ testEmptyFold ds,
        testCase "Peek Test" $ callTest $ testPeek ds,
        testCase "Insert Test" $ callTest $ testInsert ds,
        testCase "Delete Test" $ callTest $ testDelete ds,
        testCase "Delete of missing element Test" $ callTest $ testMissingDelete ds,
        testCase "Update Test" $ callTest $ testUpdate ds,
        testCase "Update Multiple Test" $ callTest $ testUpdateMultiple ds,
        testCase "Update missing element Test" $ callTest $ testUpdateMissing ds,
        testCase "Fold Test" $ callTest $ testFold ds,
        testCase "Map Test" $ callTest $ testMap ds,
        testCase "Filter Test" $ callTest $ testFilter ds,
        testCase "Combine Test" $ callTest $ testCombine ds,
        testCase "Combine with Self Test" $ callTest $ testCombineSelf ds,
        testCase "Split Test" $ callTest $ testSplit ds,
        testCase "Insert inside map" $ callTest $ insertInsideMap ds,
        testCase "Insert inside map_" $ callTest $ insertInsideMap_ ds,
        testCase "Insert inside filter" $ callTest $ insertInsideFilter ds
    ]

tests :: [Test]
tests = [
    makeTestGroup "List Dataspace" ([] :: (Dataspace Interpretation [Value] Value) => [Value]),
    makeTestGroup "File Dataspace" (FileDataspace "tmp" :: FileDataspace Value)
  ]
