module Language.K3.Metaprogram.Primitives.Distributed where

import Language.K3.Core.Metaprogram

-- Returns a function that computes a task assignment for a bipartite
-- computation when given a set of K3 peer ids.
-- Arguments: single site expr -> peers expr -> scheduling function
bipartiteSchedule :: SpliceValue -> SpliceValue -> SpliceValue
bipartiteSchedule = undefined

-- Returns a function to be applied as the LHS of a bipartite computation
-- Arguments: lhs mode -> lhs function -> lhs argument id -> specialized lhs function
bipartiteLHS :: SpliceValue -> SpliceValue -> String -> SpliceValue
bipartiteLHS = undefined
-- ("Apply", None)
-- ("Expression", Some e)
-- TODO: Variable?
-- TODO: Symbol?

-- Returns a function to be applied to a message from the LHS of a bipartite computation
-- Arguments: rhs mode -> rhs function -> specialized rhs function
bipartiteRHS :: SpliceValue -> SpliceValue -> SpliceValue
bipartiteRHS = undefined
-- ("Apply", None)
-- TODO: Pipeline
-- TODO: Variable?
-- TODO: Symbol?

-- Returns a function to be applied to the result of a bipartite computation
-- when the computation's barrier completes.
-- Arguments: bipartite computation result -> continuation function
bipartiteDone :: SpliceValue -> SpliceValue
bipartiteDone = undefined
-- ("Apply", None)
-- TODO: Forward
-- TODO: Gather
