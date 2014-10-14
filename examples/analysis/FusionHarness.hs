{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

import Language.K3.Core.Annotation (K3, tag, (@~))
import Language.K3.Core.Declaration (Declaration(..))
import Language.K3.Core.Expression

import Language.K3.Analysis.HMTypes.Inference
import Language.K3.Analysis.Properties
import Language.K3.Analysis.Effects.InsertEffects
import Language.K3.Analysis.Effects.Purity
import Language.K3.Transform.Simplification

import Language.K3.Metaprogram.Evaluation
import Language.K3.Parser.DataTypes
import Language.K3.Parser

import Language.K3.Stages

import Language.K3.Utils.Pretty

testProgram :: String
testProgram = unlines $ [
    "include \"Annotation/Collection.k3\""
  , simpleProg
  ]

streamableProg :: String
streamableProg = "\
  \ declare c : collection {a:int} @Collection                     \
  \ trigger t : () = \\_ -> (                                      \
  \   let x = ((c.map (\\r -> r.a + 1))                            \
  \              .map (\\r -> r.elem + 2))                         \
  \              .fold (\\z -> \\r ->                              \
  \                       if true then (z.insert {a:r.elem}; z)    \
  \                       else if true then z                      \
  \                       else (z.insert {a:r.elem}; z))           \
  \                 (empty {a:int} @Collection)                    \
  \   in                                                           \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                        \
  \ )                                                              \
  \ "

nonStreamableProg :: String
nonStreamableProg = "\
  \ typedef MyC = collection {a:int} @Collection                   \
  \ declare c : MyC                                                \
  \ declare f : MyC -> bool = \\_ -> true                          \
  \ trigger t : () = \\_ -> (                                      \
  \   let x = ((c.map (\\r -> r.a + 1))                            \
  \              .map (\\r -> r.elem + 1))                         \
  \              .fold (\\z -> \\r ->                              \
  \                       if f z then (z.insert {a:r.elem}; z)     \
  \                       else if true then z                      \
  \                       else (z.insert {a:r.elem}; z))           \
  \                 (empty {a:int} @Collection)                    \
  \   in                                                           \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                        \
  \ )                                                              \
  \ "

complexProg :: String
complexProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   let x = (((c.fold (\\z -> \\r ->                              \
  \                       if true                                   \
  \                       then (z.insert {a:r.a+5}; z)              \
  \                       else z)                                   \
  \                     (empty {a:int} @Collection))                \
  \             .map (\\r -> r.a + 1))                              \
  \             .map (\\r -> r.elem + 1))                           \
  \             .fold (\\z -> \\r ->                                \
  \                       if true then (z.insert {a:r.elem+5}; z)   \
  \                       else if true then z                       \
  \                       else (z.insert {a:r.elem}; z))            \
  \                 (empty {a:int} @Collection)                     \
  \   in                                                            \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                         \
  \ )                                                               \
  \ "

mapMapProg :: String
mapMapProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   let x = ((c.map (\\r -> r.a + 1))                             \
  \              .map (\\r -> r.elem + 2))                          \
  \   in                                                            \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                         \
  \ )                                                               \
  \ "

mapFilterProg :: String
mapFilterProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   let x = ((c.map    (\\r -> r.a + 1))                          \
  \              .filter (\\r -> r.elem > 5))                       \
  \   in                                                            \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                         \
  \ )                                                               \
  \ "

mapIterateProg :: String
mapIterateProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   c.insert {a:5};                                               \
  \   ((c.map     (\\r -> r.a + 1))                                 \
  \      .iterate (\\r -> if r.elem > 5 then () else ()))           \
  \ )                                                               \
  \ "

mapFoldProg :: String
mapFoldProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   c.insert {a:5};                                               \
  \   ((c.map  (\\r -> r.a + 1))                                    \
  \      .fold (\\acc -> \\r -> if r.elem > 5 then () else ()) ())  \
  \ )                                                               \
  \ "

mapMapMapProg :: String
mapMapMapProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   let x = (((c.map (\\r -> r.a + 1))                            \
  \               .map (\\r -> r.elem + 2))                         \
  \               .map (\\r -> r.elem + 3))                         \
  \   in                                                            \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                         \
  \ )                                                               \
  \ "

foldMapProg :: String
foldMapProg = "\
  \ declare c : collection {a:int} @Collection                     \
  \ trigger t : () = \\_ -> (                                      \
  \   let x = ((c.fold (\\acc -> \\r ->                            \
  \                      if true then (acc.insert {a:r.a+1}; acc)  \
  \                      else acc)                                 \
  \                    (empty {a:int} @Collection))                \
  \               .map (\\r -> r.a+2))                             \
  \   in                                                           \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                        \
  \ )                                                              \
  \ "

foldFilterProg :: String
foldFilterProg = "\
  \ declare c : collection {a:int} @Collection                     \
  \ trigger t : () = \\_ -> (                                      \
  \   let x = ((c.fold (\\acc -> \\r ->                            \
  \                      if true then (acc.insert {a:r.a+1}; acc)  \
  \                      else acc)                                 \
  \                    (empty {a:int} @Collection))                \
  \               .filter (\\r -> r.a > 2))                        \
  \   in                                                           \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                        \
  \ )                                                              \
  \ "

foldIterateProg :: String
foldIterateProg = "\
  \ declare c : collection {a:int} @Collection                     \
  \ declare d : mut int = 0                                        \
  \ trigger t : () = \\_ -> (                                      \
  \   c.insert {a:5};                                              \
  \   ((c.fold (\\acc -> \\r ->                                    \
  \              if true then (acc.insert {a:r.a+1}; acc)          \
  \              else acc)                                         \
  \            (empty {a:int} @Collection))                        \
  \      .iterate (\\r -> d = r.a))                                \
  \ )                                                              \
  \ "

foldMapMapProg :: String
foldMapMapProg = "\
  \ declare c : collection {a:int} @Collection                     \
  \ trigger t : () = \\_ -> (                                      \
  \   let x = (((c.fold (\\acc -> \\r ->                           \
  \                       if true then (acc.insert {a:r.a+1}; acc) \
  \                       else acc)                                \
  \                     (empty {a:int} @Collection))               \
  \                .map (\\r -> r.a+2))                            \
  \                .map (\\r -> r.elem+3))                         \
  \   in                                                           \
  \   c.insert {a:5}; c.iterate (\\_ -> ())                        \
  \ )                                                              \
  \ "

mapGroupByProg :: String
mapGroupByProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   let x = ((c.map     (\\r -> r.a + 1))                         \
  \              .groupBy (\\r -> r.elem + 2)                       \
  \                       (\\acc -> \\r -> acc + 1)                 \
  \                       0)                                        \
  \   in                                                            \
  \   c.insert {a:5}                                                \
  \ )                                                               \
  \ "

mapMapGroupByProg :: String
mapMapGroupByProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   let x = (((c.map     (\\r -> r.a + 1))                        \
  \               .map     (\\r -> r.elem + 2))                     \
  \               .groupBy (\\r -> r.elem + 3)                      \
  \                        (\\acc -> \\r -> acc + 1)                \
  \                        0)                                       \
  \   in                                                            \
  \   c.insert {a:5}                                                \
  \ )                                                               \
  \ "

groupByMapMapProg :: String
groupByMapMapProg = "\
  \ typedef MyC = collection {a:int} @Collection                    \
  \ declare c : MyC                                                 \
  \ declare d : MyC                                                 \
  \ trigger t : () = \\_ -> (                                       \
  \   let x = (((c.groupBy (\\r -> r.a + 3)                         \
  \                        (\\acc -> \\r -> acc + 1)                \
  \                        0)                                       \
  \               .map     (\\r -> r.value + 1))                    \
  \               .map     (\\r -> r.elem + 2))                     \
  \   in                                                            \
  \   c.insert {a:5}                                                \
  \ )                                                               \
  \ "

foldGroupByProg :: String
foldGroupByProg = "\
  \ typedef MyC = collection {a:int} @Collection                        \
  \ declare c : MyC                                                     \
  \ declare d : MyC                                                     \
  \ trigger t : () = \\_ -> (                                           \
  \   let x = ((c.fold    (\\acc -> \\r -> (acc.insert {a:r.a+1}; acc)) \
  \                       (empty {a:int} @Collection))                  \
  \              .groupBy (\\r -> r.a + 2)                              \
  \                       (\\acc -> \\r -> acc + 1)                     \
  \                       0)                                            \
  \   in                                                                \
  \   c.insert {a:5}                                                    \
  \ )                                                                   \
  \ "

groupByFoldProg :: String
groupByFoldProg = "\
  \ typedef MyC = collection {a:int} @Collection                            \
  \ declare c : MyC                                                         \
  \ declare d : MyC                                                         \
  \ trigger t : () = \\_ -> (                                               \
  \   let x = ((c.groupBy (\\r -> r.a + 2)                                  \
  \                       (\\acc -> \\r -> acc + 1)                         \
  \                       0)                                                \
  \              .fold    (\\acc -> \\r -> (acc.insert {a:r.value+1}; acc)) \
  \                       (empty {a:int} @Collection))                      \
  \   in                                                                    \
  \   c.insert {a:5}                                                        \
  \ )                                                                       \
  \ "

groupByFoldMapProg :: String
groupByFoldMapProg = "\
  \ typedef MyC = collection {a:int} @Collection                            \
  \ declare c : MyC                                                         \
  \ declare d : MyC                                                         \
  \ trigger t : () = \\_ -> (                                               \
  \   let x = ((c.groupBy (\\r -> r.a + 2)                                  \
  \                       (\\acc -> \\r -> acc + 1)                         \
  \                       0)                                                \
  \              .fold    (\\acc -> \\r -> (acc.insert {a:r.value+1}; acc)) \
  \                       (empty {a:int} @Collection))                      \
  \              .map     (\\r -> r.a + 4)                                  \
  \   in                                                                    \
  \   c.insert {a:5}                                                        \
  \ )                                                                       \
  \ "

groupByFoldMapMapProg :: String
groupByFoldMapMapProg = "\
  \ typedef MyC = collection {a:int} @Collection                     \
  \ declare c : MyC                                                  \
  \ declare d : MyC                                                  \
  \ trigger t : () = \\_ -> (                                        \
  \ (((((c.groupBy (\\r -> r.a + 2)                                  \
  \                (\\acc -> \\r -> acc + 1)                         \
  \                0)                                                \
  \       .fold    (\\acc -> \\r -> (acc.insert {a:r.value+1}; acc)) \
  \                (empty {a:int} @Collection))                      \
  \       .map     (\\r -> r.a + 4))                                 \
  \       .map     (\\r -> r.elem + 7))                              \
  \       .iterate (\\r -> ()))                                      \
  \ )                                                                \
  \ "

simpleProg :: String
simpleProg = "\
  \ declare c : mut int = 0                                          \
  \ trigger t : () = \\_ -> (                                        \
  \    c = ((\\x -> x + 1) 5)                                        \
  \ )                                                                \
  \ "

doPurity :: K3 Declaration -> K3 Declaration
doPurity p = runPurity $ runConsolidatedAnalysis p

doFusionInference :: K3 Declaration -> Either String (K3 Declaration)
doFusionInference p = do
  pWithProp <- inferProgramUsageProperties (doPurity p)
  inferFusableProgramApplies pWithProp
  --pWithFuse <- inferFusableProgramApplies pWithProp
  --fuseProgramTransformers pWithFuse

doOptimization :: K3 Declaration -> Either String (K3 Declaration)
doOptimization p = return {-. stripAllProperties-} . stripAllTypeAndEffectAnns =<< runOptPasses p

main :: IO ()
main = do
  -- Parse, splice, and dispatch based on command mode.
  parseResult  <- parseK3 True ["lib/k3"] testProgram
  case parseResult of
    Left err      -> putStrLn $ err
    Right parsedP -> do
      progE <- evalMetaprogram Nothing Nothing Nothing parsedP
      (flip $ either $ putStrLn . show) progE $ \p ->
        (flip $ either putStrLn) (doFusionInference p)  $ \fp ->
          putStrLn $ pretty fp
