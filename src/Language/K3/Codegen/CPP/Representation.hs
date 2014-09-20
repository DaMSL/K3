{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

module Language.K3.Codegen.CPP.Representation (
    Stringifiable(..),

    Name(..),

    Primitive(..),
    Type(..),

    pattern Address,
    pattern Collection,
    pattern Byte,
    pattern SharedPointer,
    pattern Unit,
    pattern Tuple,
    pattern Void,

    Literal(..),
    Capture(..),
    Expression(..),

    Declaration(..),
    Statement(..),

    Definition(..),
) where

import Data.Maybe
import Data.String

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Common hiding (Address)

class Stringifiable a where
    stringify :: a -> Doc

-- Pretty-Printing Utility Functions

commaSep :: [Doc] -> Doc
commaSep = fillSep . punctuate comma

hangBrace :: Doc -> Doc
hangBrace d = "{" <$$> indent 2 d <$$> text "}"

binaryParens :: Identifier -> Expression -> (Doc -> Doc)
binaryParens _ (Call _ _) = id
binaryParens _ (Variable _) = id
binaryParens op (Binary op' _ _) = if precedence op < precedence op' then parens else id
  where
    precedence :: Identifier -> Int
    precedence x = fromJust $ lookup x precedences

    precedences :: [(Identifier, Int)]
    precedences
        = [ ("!", 3)
          , ("*", 5), ("/", 5), ("%", 5)
          , ("+", 6), ("-", 6)
          , (">>", 7)
          , ("<<", 7)
          , ("==", 9)
          , ("|", 12)
          , ("&&", 13)
          , ("||", 14)
          ]

binaryParens _ _ = parens

data Name
    = Name Identifier
    | Qualified Name Name
    | Specialized [Type] Name
  deriving (Eq, Ord, Read, Show)

instance Stringifiable Name where
    stringify (Name i) = text i
    stringify (Qualified i n) = stringify i <> "::" <> stringify n
    stringify (Specialized ts n) = stringify n <> angles (commaSep $ map stringify ts)

data Primitive
    = PBool
    | PInt
    | PDouble
    | PString
  deriving (Eq, Ord, Read, Show)

instance Stringifiable Primitive where
    stringify PBool = "bool"
    stringify PInt = "int"
    stringify PDouble = "double"
    stringify PString = stringify (Qualified (Name "std") $ Name "string")

data Type
    = Const Type
    | Function [Type] Type
    | Inferred
    | Named Name
    | Parameter Identifier
    | Pointer Type
    | Primitive Primitive
    | Reference Type
    | RValueReference Type
  deriving (Eq, Ord, Read, Show)

pattern Address = Named (Name "Address")
pattern Collection c t = Named (Specialized [t] (Name c))
pattern Byte = Named (Name "unsigned char")
pattern SharedPointer t = Named (Specialized [t] (Name "shared_ptr"))
pattern Unit = Named (Name "unit_t")
pattern Tuple ts = Named (Specialized ts (Qualified (Name "std") (Name "tuple")))
pattern Void = Named (Name "void")

instance Stringifiable Type where
    stringify Inferred = "auto"
    stringify (Function ats rt) = stringify (Qualified (Name "std") (Name "function"))
                                  <> angles (stringify rt <> parens (commaSep (map stringify ats)))
    stringify (Const t) = "const" <+> stringify t
    stringify (Named n) = stringify n
    stringify (Parameter i) = fromString i
    stringify (Pointer t) = stringify t <> "*"
    stringify (Primitive p) = stringify p
    stringify (Reference t) = stringify t <> "&"
    stringify (RValueReference t) = stringify t <> "&&"

data Literal
    = LBool Bool
    | LChar Char
    | LInt Int
    | LDouble Double
    | LString String
    | LNullptr
  deriving (Eq, Read, Show)

instance Stringifiable Literal where
    stringify (LBool b) = if b then "true" else "false"
    stringify (LChar c) = squotes (char c)
    stringify (LInt i) = int i
    stringify (LDouble d) = double d
    stringify (LString s) = dquotes $ string s
    stringify (LNullptr) = "nullptr"

data Capture
    = ValueCapture (Maybe (Identifier, Maybe Expression))
    | RefCapture (Maybe (Identifier, Maybe Expression))
  deriving (Eq, Read, Show)

instance Stringifiable Capture where
    stringify (ValueCapture Nothing) = "="
    stringify (ValueCapture (Just (i, Nothing))) = fromString i
    stringify (ValueCapture (Just (i, Just e))) = fromString i <+> equals <+> stringify e
    stringify (RefCapture Nothing) = "&"
    stringify (RefCapture (Just (i, Nothing))) = "&" <> fromString i
    stringify (RefCapture (Just (i, Just e))) = "&" <> fromString i <+> equals <+> stringify e

data Expression
    = Binary Identifier Expression Expression
    | Call Expression [Expression]
    | Dereference Expression
    | Initialization Type [Expression]
    | Lambda [Capture] [(Identifier, Type)] (Maybe Type) [Statement]
    | Literal Literal
    | Project Expression Name
    | Subscript Expression Expression
    | Unary Identifier Expression
    | Variable Name
  deriving (Eq, Read, Show)

instance Stringifiable Expression where
    stringify (Binary op a b)
        = binaryParens op a (stringify a) <+> fromString op <+> binaryParens op b (stringify b)
    stringify (Call e as) = stringify e <> parens (commaSep $ map stringify as)
    stringify (Dereference e) = fromString "*" <> parens (stringify e)
    stringify (Initialization t es) = stringify t <+> braces (commaSep $ map stringify es)
    stringify (Lambda cs as rt bd) = cs' <+> as' <+> rt' <+> bd'
      where
        cs' = brackets $ commaSep (map stringify cs)
        as' = parens $ commaSep [stringify t <+> fromString i | (i, t) <- as]
        rt' = maybe empty (\rt'' -> "->" <+> stringify rt'') rt
        bd' = hangBrace $ vsep $ map stringify bd
    stringify (Literal lt) = stringify lt
    stringify (Project pt i) = parenthesize pt <> dot <> stringify i
      where
        parenthesize pt'@(Variable _) = stringify pt'
        parenthesize pt' = parens $ stringify pt'
    stringify (Subscript a b)
        = case b of
            (Lambda _ _ _ _) -> parenthesize a <> brackets (parens $ stringify b)
            _ -> parenthesize a <> brackets (stringify b)
      where
        parenthesize a'@(Variable _) = stringify a'
        parenthesize a' = parens $ stringify a'
    stringify (Unary op e) = fromString op <> parens (stringify e)
    stringify (Variable n) = stringify n

data Declaration
    = ClassDecl Name
    | FunctionDecl Name [Type] Type
    | ScalarDecl Name Type (Maybe Expression)
    | TemplateDecl [(Identifier, Maybe Type)] Declaration
    | UsingDecl (Either Name Name) (Maybe Name)
  deriving (Eq, Read, Show)

instance Stringifiable Declaration where
    stringify (ClassDecl n) = "class" <+> stringify n
    stringify (FunctionDecl n ats rt) = stringify rt <+> stringify n <> parens (commaSep $ map stringify ats)
    stringify (ScalarDecl n t mi) =
        stringify t <+> stringify n <> maybe empty (\i -> space <> equals <+> stringify i) mi
    stringify (TemplateDecl ts d) = "template" <+> angles (commaSep $ map parameterize ts) <+> stringify d
      where
        parameterize (i, Nothing) = "class" <+> fromString i
        parameterize (i, Just t) = stringify t <+> fromString i
    stringify (UsingDecl en mn) =
        "using" <+> leftAlias <> rightAlias
      where
        leftAlias = either (\n -> "namespace" <+> stringify n) stringify en
        rightAlias = maybe empty (\i -> space <> equals <+> stringify i) mn

data Statement
    = Assignment Expression Expression
    | Block [Statement]
    | Forward Declaration
    | IfThenElse Expression [Statement] [Statement]
    | Ignore Expression
    | Return Expression
  deriving (Eq, Read, Show)

instance Stringifiable Statement where
    stringify (Assignment a e) = stringify a <+> equals <+> stringify e <> semi
    stringify (Block ss) = hangBrace (vsep [stringify s | s <- ss])
    stringify (Forward d) = stringify d <> semi
    stringify (IfThenElse p ts es) =
        "if" <+> parens (stringify p) <+> hangBrace (vsep $ map stringify ts)
              <> (if (null es) then empty else
                      (space <> "else" <+> hangBrace (vsep $ map stringify es)))
    stringify (Ignore e) = stringify e <> semi
    stringify (Return e) = "return" <+> stringify e <> semi

data Definition
    = ClassDefn Name [Type] [Type] [Definition] [Definition] [Definition]
    | FunctionDefn Name [(Identifier, Type)] (Maybe Type) [Expression] [Statement]
    | GlobalDefn Statement
    | GuardedDefn Identifier Definition
    | IncludeDefn Identifier
    | NamespaceDefn Identifier [Definition]
    | TemplateDefn [(Identifier, Maybe Type)] Definition
  deriving (Eq, Read, Show)

instance Stringifiable Definition where
    stringify (ClassDefn cn ts ps publics privates protecteds) =
        "class" <+> stringify cn <> (if null ts then empty else angles (commaSep $ map stringify ts))
                    <> stringifyParents ps
                    <+> hangBrace (vsep $ concat [publics', privates', protecteds']) <> semi
      where
        guardNull xs ys = if null xs then [] else ys
        stringifyParents parents
            = if null ps then empty else colon <+> commaSep ["public" <+> stringify t | t <- parents]
        publics' =  guardNull publics ["public" <> colon, indent 4 (vsep $ map stringify publics)]
        privates' = guardNull protecteds ["protected" <> colon, indent 4 (vsep $ map stringify protecteds)]
        protecteds' = guardNull privates ["private" <> colon, indent 4 (vsep $ map stringify privates)]
    stringify (FunctionDefn fn as mrt is bd) = rt' <> fn' <> as' <> is' <+> bd'
      where
        rt' = maybe empty (\rt'' -> stringify rt'' <> space) mrt
        fn' = stringify fn
        as' = parens (commaSep [stringify t <+> fromString i | (i, t) <- as])
        is' = if null is then empty else colon <+> commaSep (map stringify is)
        bd' = if null bd then braces empty else hangBrace (vsep $ map stringify bd)
    stringify (GlobalDefn s) = stringify s
    stringify (GuardedDefn i d)
        = "#ifndef" <+> fromString i <$$> "#define" <+> fromString i <$$> stringify d <$$> "#endif"
    stringify (IncludeDefn i) = "#include" <+> dquotes (fromString i)
    stringify (NamespaceDefn n ss) = "namespace" <+> fromString n <+> hangBrace (vsep $ map stringify ss)
    stringify (TemplateDefn ts d) = "template" <+> angles (commaSep $ map parameterize ts) <$$> stringify d
      where
        parameterize (i, Nothing) = "class" <+> fromString i
        parameterize (i, Just t) = stringify t <+> fromString i