{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Top-Level Declarations in K3.
module Language.K3.Core.Declaration (
    Declaration(..),
    Annotation(..),

    -- * User defined Annotations
    Polarity(..),
    AnnMemDecl(..),

    isDSpan,
    isDUID,
    isDSyntax
) where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Utils.Pretty

-- | Top-Level Declarations
data Declaration
    = DGlobal       Identifier (K3 Type) (Maybe (K3 Expression))
    -- | Trigger declaration.  Type is argument type of trigger.  Expression
    --  must be a function taking that argument type and returning unit.
    | DTrigger      Identifier (K3 Type) (K3 Expression)
    | DRole         Identifier
    | DAnnotation   Identifier [TypeVarDecl] [AnnMemDecl]

    -- | Type synonym declaration.
    | DTypeDef      Identifier (K3 Type)
  deriving (Eq, Read, Show)

-- | Annotation declaration members
data AnnMemDecl
    = Lifted      Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  UID
    
    | Attribute   Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  UID
    
    | MAnnotation Polarity Identifier UID
  deriving (Eq, Read, Show)  

-- | Annotation member polarities
data Polarity = Provides | Requires deriving (Eq, Read, Show)

-- | Annotations on Declarations.
data instance Annotation Declaration
    = DSpan   Span
    | DUID    UID
    | DSyntax SyntaxAnnotation
  deriving (Eq, Read, Show)


instance Pretty (K3 Declaration) where
    prettyLines (Node (DGlobal i t me :@: as) ds) =
        ["DGlobal " ++ i ++ drawAnnotations as, "|"]
        ++ case (me, ds) of
            (Nothing, []) -> terminalShift t
            (Just e, [])  -> nonTerminalShift t ++ ["|"] ++ terminalShift e
            (Nothing, _)  -> nonTerminalShift t ++ ["|"] ++ drawSubTrees ds
            (Just e, _)   -> nonTerminalShift t ++ ["|"] ++ nonTerminalShift e ++ ["|"] ++ drawSubTrees ds
    
    prettyLines (Node (DTrigger i t e :@: as) ds) =
        ["DTrigger " ++ i ++ drawAnnotations as, "|"]
        ++ nonTerminalShift t ++ ["|"]
        ++ case ds of
            [] -> terminalShift e
            _  -> nonTerminalShift e ++ ["|"] ++ drawSubTrees ds
    
    prettyLines (Node (DRole i :@: as) ds) = ["DRole " ++ i ++ " :@: " ++ show as, "|"] ++ drawSubTrees ds
    
    prettyLines (Node (DAnnotation i vdecls members :@: as) ds) =
        ["DAnnotation " ++ i
            ++ if null vdecls
                then ""
                else ("[" ++ (removeTrailingWhitespace . boxToString $
                        foldl1 (\a b -> a %+ [", "] %+ b) $ map prettyLines vdecls)
                    ++ "]"
                ) ++ drawAnnotations as, "|"]
        ++ drawAnnotationMembers members
        ++ drawSubTrees ds
      where
        drawAnnotationMembers []  = []
        drawAnnotationMembers [x] = terminalShift x
        drawAnnotationMembers x   =
            concatMap (\y -> nonTerminalShift y ++ ["|"]) (init x)
            ++ terminalShift (last x)

    prettyLines (Node (DTypeDef i t :@: _) _) = ["DTypeDef " ++ i ++ " "] `hconcatTop` prettyLines t

instance Pretty AnnMemDecl where
  prettyLines (Lifted      pol n t eOpt uid) =
    ["Lifted " ++ unwords [show pol, n, show uid], "|"]
    ++ case eOpt of
        Nothing -> terminalShift t
        Just e  -> nonTerminalShift t ++ ["|"] ++ terminalShift e
  
  prettyLines (Attribute   pol n t eOpt uid) =
    ["Attribute " ++ unwords [show pol, n, show uid], "|"]
    ++ case eOpt of
        Nothing -> terminalShift t
        Just e  -> nonTerminalShift t ++ ["|"] ++ terminalShift e
  
  prettyLines (MAnnotation pol n uid) =
    ["MAnnotation " ++ unwords [show pol, n, show uid]]


{- Declaration annotation predicates -}

isDSpan :: Annotation Declaration -> Bool
isDSpan (DSpan _) = True
isDSpan _         = False

isDUID :: Annotation Declaration -> Bool
isDUID (DUID _) = True
isDUID _        = False

isDSyntax :: Annotation Declaration -> Bool
isDSyntax (DSyntax _) = True
isDSyntax _           = False
