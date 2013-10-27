module Language.K3.TypeSystem.Data
( module X
) where

import Language.K3.TypeSystem.Data.ConstraintSet as X
import Language.K3.TypeSystem.Data.Convenience as X
import Language.K3.TypeSystem.Data.Coproduct as X
import Language.K3.TypeSystem.Data.PrimitiveSubtyping as X
-- Hide the constructors of ConstraintSet; the caller should be using the
-- operations defined in ConstraintSet.
import Language.K3.TypeSystem.Data.TypesAndConstraints as X
  hiding (ConstraintSet)
-- But do re-export the type name itself.
import Language.K3.TypeSystem.Data.TypesAndConstraints as X
  (ConstraintSet)
