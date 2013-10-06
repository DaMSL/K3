{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Pretty.Syntax (
    program,
    decl,
    expr,
    typ,

    programS,
    declS,
    exprS,
    typeS,
    symbolS
) where

import Control.Applicative ( (<*>) )
import qualified Control.Applicative as C ( (<$>) )
import Control.Monad

import Data.List 

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Type
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

import Text.PrettyPrint.ANSI.Leijen

data SyntaxError = SyntaxError String deriving (Eq, Show)

-- | A simple syntax printer monad.
type Printer a     = Either SyntaxError a
type SyntaxPrinter = Printer Doc

runSyntaxPrinter :: SyntaxPrinter -> Either String Doc
runSyntaxPrinter p = either (\(SyntaxError s) -> Left s) Right p

throwSP :: String -> Printer a
throwSP = Left . SyntaxError

programS :: K3 Declaration -> Either String String
programS p = program p >>= (\doc -> return $ displayS (renderPretty 0.8 100 doc) $ "")

declS :: K3 Declaration -> Either String String
declS d = show C.<$> runSyntaxPrinter (decl d)

exprS :: K3 Expression -> Either String String
exprS e = show C.<$> runSyntaxPrinter (expr e)

typeS :: K3 Type -> Either String String
typeS t = show C.<$> runSyntaxPrinter (typ t)

symbolS :: K3 Expression -> Either String String
symbolS e = show C.<$> runSyntaxPrinter (symbol e)

program :: K3 Declaration -> Either String Doc
program = runSyntaxPrinter . decl

-- | Declaration syntax printing.
decl :: K3 Declaration -> SyntaxPrinter
decl (details -> (DGlobal n t eOpt, cs, anns)) =
    case tag t of 
      TSource -> withSubDecls $ endpoint' "source"
      TSink   -> withSubDecls $ endpoint' "sink"
      _       -> withSubDecls $ decl'
  where
    withSubDecls d = vsep C.<$> ((:) C.<$> d <*> subDecls cs)

    decl' = globalDecl C.<$> qualifierAndType t
                         <*> optionalPrinter qualifierAndExpr eOpt

    globalDecl (qualT, t') eqeOpt = 
      hang 2 $ text "declare" <+> text n <+> colon <+> qualT <+> (align t') <+> initializer eqeOpt <> line
    
    endpoint' kw = endpoint kw n C.<$> endpointSpec anns <*> typ t <*> optionalPrinter expr eOpt

    initializer opt = maybe empty (\(qualE, e) -> equals <+> qualE <$> e) opt


decl (details -> (DTrigger n t e, cs, _)) =
    vsep C.<$> ((:) C.<$> decl' <*> subDecls cs)
  
  where
    decl' = triggerDecl C.<$> typ t <*> expr e
    triggerDecl t' e' =
      hang 2 $ text "trigger" <+> text n <+> colon <+> (align t') <+> equals <$> e' <> line


decl (details -> (DRole n, cs, _)) = 
  if n == "__global" then vsep C.<$> ((++) C.<$> subDecls cs <*> bindingDecls cs)
                     else roleDecl C.<$> ((++) C.<$> subDecls cs <*> bindingDecls cs)
  where roleDecl subDecls' =
          text "role" <+> text n
                      <+> lbrace </> (align . indent 2 $ vsep subDecls') <$> rbrace <> line


decl (details -> (DAnnotation n tvars mems, cs, _)) = do
  tsps <- mapM typeVarDecl tvars
  msps <- mapM memberDecl mems
  csps <- mapM decl cs
  return $ vsep . (: csps) $
    text "annotation" <+> text n <+> text "given" <+> text "type"
      <+> cat (punctuate comma tsps)
      <+> lbrace <$> (indent 2 $ vsep msps) <$> rbrace <> line
  where
    memberDecl (Lifted pol i t eOpt _) =
      attrDecl pol "lifted" i C.<$> qualifierAndType t
                                <*> optionalPrinter qualifierAndExpr eOpt
    
    memberDecl (Attribute pol i t eOpt _) =
      attrDecl pol "" i C.<$> qualifierAndType t
                          <*> optionalPrinter qualifierAndExpr eOpt
    
    memberDecl (MAnnotation pol i _) =
      return $ polarity pol <+> text "annotation" <+> text i

    attrDecl pol kw j (qualT, t') eqeOpt = 
      hang 2 $ polarity pol <+> (if null kw then text j else text kw <+> text j)
                            <+> colon <+> qualT <+> (align t') <+> initializer eqeOpt

    polarity Provides = text "provides"
    polarity Requires = text "requires"

    initializer opt = maybe empty (\(qualE, e') -> equals <+> qualE <$> e') opt 

decl _ = throwSP "Invalid declaration"


typeVarDecl :: TypeVarDecl -> SyntaxPrinter
typeVarDecl (TypeVarDecl i mtExpr) =
  case mtExpr of
    Nothing -> return $ text i
    Just tExpr -> do
      t <- typ tExpr
      return $ text i <+> text "<=" <+> t


subDecls :: [K3 Declaration] -> Printer [Doc]
subDecls d = mapM decl $ filter (not . generatedDecl) d
  where generatedDecl (details -> (DGlobal _ _ _, _, anns)) = any isGenerated $ filter isDSpan anns
        generatedDecl _ = False
        isGenerated (DSpan (GeneratedSpan _)) = True
        isGenerated _ = False


bindingDecls :: [K3 Declaration] -> Printer [Doc]
bindingDecls d = mapM feed d >>= foldM (\acc x -> maybe (return acc) (return . (acc++) . (:[])) x) []
  where
    feed (details -> (DGlobal n _ _, _, anns))
      | any isDEndpointDecl anns = endpointBindings anns >>= return . maybe Nothing (doFeed n)
      | otherwise                = return Nothing
    feed _ = return Nothing

    doFeed src dests = Just $ vsep $ flip map dests $ ((text "feed" <+> text src <+> text "|>") <+>) . text


isDEndpointDecl :: Annotation Declaration -> Bool
isDEndpointDecl (DSyntax (EndpointDeclaration _ _)) = True
isDEndpointDecl _ = False


endpointSpec :: [Annotation Declaration] -> Printer (Maybe EndpointSpec)
endpointSpec = matchAnnotation isDEndpointDecl spec
  where spec (DSyntax (EndpointDeclaration s _ )) = return $ Just s
        spec _ = return $ Nothing

endpointBindings :: [Annotation Declaration] -> Printer (Maybe EndpointBindings)
endpointBindings = matchAnnotation isDEndpointDecl bindings
  where bindings (DSyntax (EndpointDeclaration _ b)) = return $ Just b
        bindings _ = return $ Nothing

endpoint :: String -> Identifier -> Maybe EndpointSpec -> Doc -> Maybe Doc -> Doc
endpoint kw n specOpt t' eOpt' = case specOpt of 
  Nothing                   -> common Nothing
  Just ValueEP              -> common . Just $ maybe empty (text "value" <+>) eOpt'
  Just (BuiltinEP kind fmt) -> common . Just $ text kind <+> text fmt
  Just (FileEP path fmt)    -> common . Just $ text "file" <+> text path <+> text fmt
  Just (NetworkEP addr fmt) -> common . Just $ text "network" <+> text addr <+> text fmt

  where
    common initializer =
      hang 2 $ text kw <+> text n
                       <+> colon <+> (align t')
                       <+> maybe empty (equals <$>) initializer <> line


-- | Expression syntax printing.
expr :: K3 Expression -> SyntaxPrinter
expr (details -> (EConstant c, _, anns)) =
  case c of 
    CBool b   -> return . text $ if b then "true" else "false"
    CInt i    -> return $ int i
    CByte w   -> return . integer $ toInteger w
    CReal r   -> return $ double r
    CString s -> return . dquotes $ text s
    CNone q   -> return $ text "None" <+> nQualifier q
    CEmpty t  -> typ t >>= return . emptyCollection (namedEAnnotations anns)

  where
    emptyCollection annIds t =
      text "empty" <+> t <+> text "@" <+> commaBrace (map text annIds)
        
    nQualifier NoneMut   = text "mut"
    nQualifier NoneImmut = text "immut"

expr (tag -> EVariable i) = return $ text i

expr (details -> (ESome, [x], _)) = qualifierAndExpr x >>= return . uncurry someExpr
expr (tag -> ESome)               = exprError "some"

expr (details -> (EIndirect, [x], _)) = qualifierAndExpr x >>= return . uncurry indirectionExpr
expr (tag -> EIndirect)               = exprError "indirection"

expr (details -> (ETuple, cs, _)) = mapM qualifierAndExpr cs >>= return . tupleExpr
expr (tag -> ETuple)              = exprError "tuple"

expr (details -> (ERecord is, cs, _)) = mapM qualifierAndExpr cs >>= return . recordExpr is
expr (tag -> ERecord _)               = exprError "record"

expr (details -> (ELambda i, [b], _)) = expr b >>= return . lambdaExpr i
expr (tag -> ELambda _)               = exprError "lambda"

expr (details -> (EOperate otag, cs, _))
    | otag `elem` [ONeg, ONot], [a] <- cs = expr a >>= unary otag
    | otherwise, [a, b] <- cs             = uncurry (binary otag) =<< ((,) C.<$> expr a <*> expr b)
    | otherwise                           = exprError "operator"

expr (details -> (EProject i, [r], _)) = expr r >>= return . projectExpr i
expr (tag -> EProject _)               = exprError "project"

expr (details -> (ELetIn i, [e, b], _)) = letExpr i C.<$> qualifierAndExpr e <*> expr b
expr (tag -> ELetIn _)                  = exprError "let"

expr (details -> (EAssign i, [e], _)) = expr e >>= return . assignExpr i
expr (tag -> EAssign _)               = exprError "assign"

expr (details -> (ECaseOf i, [e, s, n], _)) = caseExpr i C.<$> expr e <*> expr s <*> expr n
expr (tag -> ECaseOf _)                     = exprError "case-of"

expr (details -> (EBindAs b, [e, f], _)) = bindExpr b C.<$> expr e <*> expr f
expr (tag -> EBindAs _)                  = exprError "bind-as"

expr (details -> (EIfThenElse, [p, t, e], _)) = branchExpr C.<$> expr p <*> expr t <*> expr e
expr (tag -> EIfThenElse)                     = exprError "if-then-else"

expr (details -> (EAddress, [h, p], _)) = address h p
expr (tag -> EAddress)                  = exprError "address"

expr (tag -> ESelf) = return $ keyword "self"

expr _ = exprError "unknown"

unary :: Operator -> Doc -> SyntaxPrinter
unary ONeg e = return $ text "-" <//> e
unary ONot e = return $ text "not" <+> e
unary op _   = throwSP $ "Invalid unary operator '" ++ show op ++ "'"

binary :: Operator -> Doc -> Doc -> SyntaxPrinter
binary op e e' = 
  case op of 
    OAdd -> infixOp "+" 
    OSub -> infixOp "-" 
    OMul -> infixOp "*" 
    ODiv -> infixOp "/" 
    OAnd -> infixOp "&&"
    OOr  -> infixOp "||"
    OEqu -> infixOp "=="
    ONeq -> infixOp "/="
    OLth -> infixOp "<" 
    OLeq -> infixOp "<="
    OGth -> infixOp ">" 
    OGeq -> infixOp ">="
    OSeq -> return . align $ e <> semi <$> e'
    OApp -> return $ e <+> e'
    OSnd -> infixOp "<-"
    _    -> throwSP $ "Invalid binary operator '" ++ show op ++ "'"

  where infixOp opStr = return $ e <+> text opStr <+> e'

address :: K3 Expression -> K3 Expression -> SyntaxPrinter
address h p
  | EConstant (CString s) <- tag h = addrExpr C.<$> (return $ text s) <*> expr p
  | otherwise                      = addrExpr C.<$> expr h <*> expr p

symbol :: K3 Expression -> SyntaxPrinter
symbol (tag -> EConstant (CString s)) = return $ text s
symbol _ = throwSP "Invalid symbol expression"

qualifierAndExpr :: K3 Expression -> Printer (Doc, Doc)
qualifierAndExpr e@(annotations -> anns) = (,) C.<$> eQualifier anns <*> expr e

eQualifier :: [Annotation Expression] -> SyntaxPrinter
eQualifier anns = qualifier isEQualified eqSyntax anns
  where 
    eqSyntax EImmutable = return $ text "immut"
    eqSyntax EMutable   = return $ text "mut"
    eqSyntax _          = throwSP "Invalid expression qualifier"

exprError :: String -> SyntaxPrinter
exprError msg = throwSP $ "Invalid " ++ msg ++ " expression"

-- | Type expression syntax printing.
typ :: K3 Type -> SyntaxPrinter
typ (tag -> TBool)       = return $ text "bool"
typ (tag -> TByte)       = return $ text "byte"
typ (tag -> TInt)        = return $ text "int"
typ (tag -> TReal)       = return $ text "real"
typ (tag -> TString)     = return $ text "string"
typ (tag -> TAddress)    = return $ text "address"

typ (details -> (TOption, [x], _)) = qualifierAndType x >>= return . uncurry optionType
typ (tag -> TOption)               = throwSP "Invalid option type"

typ (details -> (TIndirection, [x], _)) = qualifierAndType x >>= return . uncurry indirectionType
typ (tag -> TIndirection)               = throwSP "Invalid indirection type"

typ (details -> (TTuple, ch, _)) = mapM qualifierAndType ch >>= return . tupleType

typ (details -> (TRecord ids, ch, _))
  | length ids == length ch  = mapM qualifierAndType ch >>= return . recordType ids
  | otherwise                = throwSP "Invalid record type"

typ (details -> (TFunction, [a,r], _)) = mapM typ [a,r] >>= \chT -> return $ funType (head chT) (last chT)
typ (tag -> TFunction)                 = throwSP "Invalid function type"

typ (details -> (TSource, [x], _)) = typ x
typ (tag -> TSource)               = throwSP "Invalid sink type"

typ (details -> (TSink, [x], _)) = typ x
typ (tag -> TSink)               = throwSP "Invalid sink type"

typ (details -> (TTrigger, [x], _)) = typ x >>= return . triggerType
typ (tag -> TTrigger)               = throwSP "Invalid trigger type"

typ (details -> (TCollection, [x], anns)) =
  typ x >>= return . collectionType (map text $ namedTAnnotations anns)

typ (tag -> TCollection) = throwSP "Invalid collection type"

typ (tag -> TBuiltIn TSelf)      = return $ keyword "self"
typ (tag -> TBuiltIn TContent)   = return $ keyword "content"
typ (tag -> TBuiltIn THorizon)   = return $ keyword "horizon"
typ (tag -> TBuiltIn TStructure) = return $ keyword "structure"

typ (tag -> TForall _)      = throwSP "TForall syntax not supported"
typ (tag -> TDeclaredVar _) = throwSP "TDeclaredVar syntax not supported"

typ _ = throwSP "Cannot generate type syntax"

qualifierAndType :: K3 Type -> Printer (Doc, Doc)
qualifierAndType t@(annotations -> anns) = (,) C.<$> tQualifier anns <*> typ t

tQualifier :: [Annotation Type] -> SyntaxPrinter
tQualifier anns = qualifier isTQualified tqSyntax anns
  where 
    tqSyntax TImmutable = return $ text "immut"
    tqSyntax TMutable   = return $ text "mut"
    tqSyntax _          = throwSP "Invalid type qualifier"


{- Syntax constructors -}

optionType :: Doc -> Doc -> Doc
optionType qual t = text "option" <+> qual <+> t

indirectionType :: Doc -> Doc -> Doc
indirectionType qual t = text "ind" <+> qual <+> t

tupleType :: [(Doc, Doc)] -> Doc
tupleType qualC = tupled $ map (uncurry (<+>)) qualC

recordType :: [Identifier] -> [(Doc, Doc)] -> Doc
recordType ids qualC =
  commaBrace $ map (\(a,b) -> text a <+> colon <+> b)
             $ zip ids $ map (uncurry (<+>)) qualC

collectionType :: [Doc] -> Doc -> Doc
collectionType namedAnns t =
  text "collection" <+> t <+> text "@" <+> commaBrace namedAnns

funType :: Doc -> Doc -> Doc
funType arg ret = parens $ arg </> text "->" <+> ret

triggerType :: Doc -> Doc
triggerType t = text "trigger" <+> t


someExpr :: Doc -> Doc -> Doc
someExpr qual e = text "Some" <+> qual <+> e

indirectionExpr :: Doc -> Doc -> Doc
indirectionExpr qual e = text "ind" <+> qual <+> e

tupleExpr :: [(Doc, Doc)] -> Doc
tupleExpr qualC = tupled $ map (uncurry (<+>)) qualC

recordExpr :: [Identifier] -> [(Doc, Doc)] -> Doc
recordExpr ids qualC = 
  commaBrace $ map (\(a,b) -> text a <+> colon <+> b)
             $ zip ids $ map (uncurry (<+>)) qualC

lambdaExpr :: Identifier -> Doc -> Doc
lambdaExpr n b = parens $ backslash <> text n <+> text "->" <+> b

projectExpr :: Identifier -> Doc -> Doc
projectExpr n r = r <> dot <> text n

letExpr :: Identifier -> (Doc, Doc) -> Doc -> Doc
letExpr n (qual,e) b =
  text "let" <+> align (text n <+> equals <+> qual </> e) <$> text "in" </> b

assignExpr :: Identifier -> Doc -> Doc
assignExpr n e = text n <+> equals <+> e

caseExpr :: Identifier -> Doc -> Doc -> Doc -> Doc
caseExpr i e s n = text "case" </> hang 2 e </> text "of"
                               </> indent 2 sCase </> indent 2 nCase
  where sCase = braces $ text "Some" <+> text i <+> text "->" </> s
        nCase = braces $ text "None" <+> text "->" </> n

bindExpr :: Binder -> Doc -> Doc -> Doc
bindExpr b e e' = text "bind" </> hang 2 e </> text "as"
                              </> (hang 2 $ binder b)
                              </> text "in" <$> indent 2 e'
  where
    binder (BIndirection i) = text "ind" <+> text i
    binder (BTuple ids)     = tupled $ map text ids
    binder (BRecord idMap)  = commaBrace $ map (\(s,t) -> text s <+> colon <+> text t) idMap

branchExpr :: Doc -> Doc -> Doc -> Doc
branchExpr p t e = text "if" <+> (align $ p <$> text "then" </> t <$> text "else" </> e)

addrExpr :: Doc -> Doc -> Doc
addrExpr h p = h <> colon <> p


{- Helpers -}

keyword :: String -> Doc
keyword s = text s

commaBrace :: [Doc] -> Doc
commaBrace = encloseSep lbrace rbrace comma

details :: K3 a -> (a, [K3 a], [Annotation a])
details n = (tag n, children n, annotations n)

matchAnnotation :: (Eq (Annotation a))
                => (Annotation a -> Bool) -> (Annotation a -> Printer b) -> [Annotation a]
                -> Printer b
matchAnnotation matchF mapF anns = case filter matchF anns of
    []            -> throwSP "No matching annotation found"
    [q]           -> mapF q
    l | same l    -> mapF $ head l
      | otherwise -> throwSP "Multiple matching annotations found"
  where same l = 1 == (length $ nub l)

qualifier :: (Eq (Annotation a))
          => (Annotation a -> Bool) -> (Annotation a -> SyntaxPrinter) -> [Annotation a]
          -> SyntaxPrinter
qualifier = matchAnnotation

optionalPrinter :: (a -> Printer b) -> Maybe a -> Printer (Maybe b)
optionalPrinter f opt = maybe (return Nothing) (\x -> f x >>= return . Just) opt
