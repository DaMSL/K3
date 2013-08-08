{-|
  This module contains the routines necessary to check type environments.  It
  represents the implementation of the Type Checking section in the K3
  specification.  Note that this module does not include any type inference
  logic.  That is, it does not generate the environment against which to check;
  it merely checks that environment against the AST.
-}
module Language.K3.TypeSystem.TypeChecking ( module X ) where

import Language.K3.TypeSystem.TypeChecking.Declarations as X
import Language.K3.TypeSystem.TypeChecking.Expressions as X
import Language.K3.TypeSystem.TypeChecking.Monad as X
import Language.K3.TypeSystem.TypeChecking.TypeExpressions as X
