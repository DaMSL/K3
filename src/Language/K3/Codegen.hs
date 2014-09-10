{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}

-- | K3 code generation interface.
module Language.K3.Codegen where

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

{- Disabled while HaskellCG is deprecated
--import qualified Language.K3.Codegen.Haskell as HG
-}

class Compilable a where
  typ         :: K3 Type -> a
  expression  :: K3 Expression -> a
  declaration :: K3 Declaration -> a

  generate    :: Identifier -> K3 Declaration -> a
  compile     :: a -> Either String String

-- | Generates K3 code, essentially a pretty printer.
data IdentityEmbedding
  = IdDecl       (K3 Declaration)
  | IdExpression (K3 Expression)
  | IdType       (K3 Type)

instance Compilable IdentityEmbedding where
  typ t           = IdType t
  expression e    = IdExpression e
  declaration d   = IdDecl d

  generate        = undefined
  compile         = undefined

-- | DEPRECATED: Haskell code generation.
{-
instance Compilable (HG.CodeGeneration HG.HaskellEmbedding) where
  typ         = HG.typ
  expression  = HG.expression
  declaration = HG.declaration

  generate    = HG.generate
  compile     = HG.compile
-}
