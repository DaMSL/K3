-- | K3 code generation interface.
module Language.K3.Codegen where

import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

import qualified Language.K3.Codegen.Haskell as HG

class Compilable a where
  cType       :: K3 Type -> a
  expression  :: K3 Expression -> a
  declaration :: K3 Declaration -> a
  
  generate    :: K3 Declaration -> a
  compile     :: a -> String

-- | Generates K3 code, essentially a pretty printer.
data IdentityEmbedding
  = IdDecl       (K3 Declaration)
  | IdExpression (K3 Expression)
  | IdType       (K3 Type)

instance Compilable IdentityEmbedding where
  cType t         = IdType t
  expression e    = IdExpression e
  declaration d   = IdDec d

  generate    = undefined
  compile     = undefined


-- | Haskell code generation.
instance Compilable (CodeGeneration HaskellEmbedding) where
  cType       = HG.cType
  expression  = HG.expression
  declaration = HG.declaration

  generate    = HG.generate
  compile     = HG.stringify

