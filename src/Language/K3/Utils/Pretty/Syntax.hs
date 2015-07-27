{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Utils.Pretty.Syntax (
    program,
    decl,
    expr,
    typ,
    literal,

    programS,
    declS,
    exprS,
    typeS,
    literalS,
    symbolS
) where

import Prelude hiding ((<$>))

import Control.Applicative ( (<*>) )
import qualified Control.Applicative as C ( (<$>) )
import Control.Monad

import Data.Either
import qualified Data.Map as Map
import Data.Maybe
import Data.List hiding ( group )

import Debug.Trace

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Core.Metaprogram hiding ( mpAnnMemDecl )

import qualified Language.K3.Utils.Pretty as P

import Text.PrettyPrint.ANSI.Leijen

data SyntaxError = SyntaxError String deriving (Eq, Show)

-- | A simple syntax printer monad.
type Printer a     = Either SyntaxError a
type SyntaxPrinter = Printer Doc

-- | Hard-coded printer configuration
showImmut :: Bool
showImmut = False

runSyntaxPrinter :: SyntaxPrinter -> Either String Doc
runSyntaxPrinter = either (\(SyntaxError s) -> Left s) Right

reasonM :: String -> Printer a -> Printer a
reasonM msg m = either (\(SyntaxError s) -> Left . SyntaxError $ s ++ msg) return m

throwSP :: String -> Printer a
throwSP = Left . SyntaxError

programS :: K3 Declaration -> Either String String
programS p = program p >>= \doc -> return $ displayS (renderSmart 0.6 100 doc) ""

declS :: K3 Declaration -> Either String String
declS d = show C.<$> runSyntaxPrinter (decl d)

exprS :: K3 Expression -> Either String String
exprS e = show C.<$> runSyntaxPrinter (expr e)

typeS :: K3 Type -> Either String String
typeS t = show C.<$> runSyntaxPrinter (typ t)

literalS :: K3 Literal -> Either String String
literalS l = show C.<$> runSyntaxPrinter (literal l)

symbolS :: K3 Expression -> Either String String
symbolS e = show C.<$> runSyntaxPrinter (symbol e)

program :: K3 Declaration -> Either String Doc
program = runSyntaxPrinter . decl

-- | Declaration syntax printing.
decl :: K3 Declaration -> SyntaxPrinter
decl d@(details -> (_,_,anns)) = attachComments (commentD anns) $ decl' d

decl' :: K3 Declaration -> SyntaxPrinter
decl' (details -> (DGlobal n t eOpt, cs, anns)) =
    case tag t of
      TSource -> withSubDecls $ endpoint' "source"
      TSink   -> withSubDecls $ endpoint' "sink"
      _       -> withSubDecls declG
  where
    withSubDecls d = vsep C.<$> ((:) C.<$> d <*> subDecls cs)

    declG = globalDecl C.<$> declGType
                         <*> optionalPrinter qualifierAndExpr eOpt

    declGType = case tag t of
      TForall _ -> (Nothing,) C.<$> typ t
      _         -> qualifierAndType t

    globalDecl (qualTOpt, t') eqeOpt =
      hang 2 $ text "declare" <+> text n </> colon <+> maybe (align t') (<+> (align t')) qualTOpt
                                         </> declInitializer eqeOpt <> line

    endpoint' kw = endpoint kw n C.<$> endpointSpec anns <*> typ t <*> optionalPrinter expr eOpt


decl' (details -> (DTrigger n t e, cs, _)) =
    vsep C.<$> ((:) C.<$> declT <*> subDecls cs)
  where
    declT = triggerDecl C.<$> typ t <*> expr e
    triggerDecl t' e' =
      hang 2 $ text "trigger" <+> text n <+> colon <+> align t' <+> equals <$> e' <> line


decl' (details -> (DRole n, cs, _)) =
  if n == "__global" then vsep     C.<$> ((++) C.<$> subDecls cs <*> bindingDecls cs)
                     else roleDecl C.<$> ((++) C.<$> subDecls cs <*> bindingDecls cs)
  where roleDecl roleCh =
          text "role" <+> text n
                      <+> lbrace </> (align . indent 2 $ vsep roleCh) <$> rbrace <> line


decl' (details -> (DDataAnnotation n tvars mems, cs, _)) = do
  pfxsp <- dAnnPrefix n [] tvars
  msps  <- mapM annMemDecl mems
  csps  <- mapM decl cs
  return $ vsep . (: csps) $ pfxsp <+> lbrace <$> (indent 2 $ vsep msps) <$> rbrace <> line

decl' (details -> (DGenerator mp, cs, _)) = do
  msp    <- mpDeclaration mp
  csps   <- mapM decl cs
  return . vsep $ msp : csps

decl' (details -> (DTypeDef tn t, cs, _)) = do
  tsp  <- typ t
  csps <- mapM decl cs
  return $ vsep . (: csps) $ text "typedef" <+> text tn <+> equals <+> tsp

decl' _ = throwSP "Invalid declaration"

declInitializer :: Maybe (Maybe Doc, Doc) -> Doc
declInitializer = maybe empty (\(qualEOpt, e') -> equals </> (maybe e' (<+> e') qualEOpt))

-- TODO: generate syntax for member property annotations.
annMemDecl :: AnnMemDecl -> SyntaxPrinter
annMemDecl (Lifted pol i t eOpt _) =
  attrDecl pol "lifted" i C.<$> qualifierAndType t
                            <*> optionalPrinter qualifierAndExpr eOpt

annMemDecl (Attribute pol i t eOpt _) =
  attrDecl pol "" i C.<$> qualifierAndType t
                      <*> optionalPrinter qualifierAndExpr eOpt

annMemDecl (MAnnotation pol i _) =
  return $ polarity pol <+> text "annotation" <+> text i

attrDecl :: Polarity -> String -> Identifier -> (Maybe Doc, Doc) -> Maybe (Maybe Doc, Doc) -> Doc
attrDecl pol kw j (qualTOpt, t') eqeOpt =
  hang 2 $ polarity pol <+> (if null kw then text j else text kw <+> text j)
                        <+> maybe colon (colon <+>) qualTOpt <+> (align t') <+> declInitializer eqeOpt

polarity :: Polarity -> Doc
polarity Provides = text "provides"
polarity Requires = text "requires"

mpDeclaration :: MPDeclaration -> SyntaxPrinter
mpDeclaration (MPDataAnnotation i svars tvars (partitionEithers -> (mpAnnMems, annMems))) = do
  pfxsp <- dAnnPrefix i svars tvars
  mpsps <- mapM mpAnnMemDecl mpAnnMems
  msps  <- mapM annMemDecl annMems
  return $ pfxsp <+> lbrace <$> (indent 2 $ vsep $ mpsps ++ msps) <$> rbrace

mpDeclaration (MPCtrlAnnotation i svars rewriteRules extensions) = do
  svsps  <- mapM typedSpliceVar svars
  rsps   <- return . indent 2 . vsep =<< mapM rewriteRule rewriteRules
  cdsps  <- return . indent 2 . vsep =<< mapM ctrlExtension extensions
  headsp <- return $ if null svsps then text "control" <+> text i
                     else text "control" <+> text i <+> (brackets $ cat (punctuate comma svsps))
  return $ headsp <$> rsps <$> cdsps

mpAnnMemDecl :: MPAnnMemDecl -> SyntaxPrinter
mpAnnMemDecl (MPAnnMemDecl i c mems) = do
  csp  <- spliceValue c
  msps <- mapM annMemDecl mems
  return $ text "for" <+> text i <+> text "in" <+> csp <$> (indent 2 $ vsep msps)

dAnnPrefix :: Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> SyntaxPrinter
dAnnPrefix i svars tvars = do
    svsps  <- mapM typedSpliceVar svars
    tsps   <- mapM typeVarDecl tvars
    return $ case (svsps, tsps) of
      ([], []) -> name i
      (_,  []) -> name i <+> sparams svsps
      ([], _)  -> name i <+> tparams tsps
      (_, _)   -> name i <+> sparams svsps <+> tparams tsps
  where
    name n        = text "annotation" <+> text n
    sparams svsps = brackets $ cat (punctuate comma svsps)
    tparams tsps  = text "given" <+> text "type" <+> cat (punctuate comma tsps)

rewriteRule :: PatternRewriteRule -> SyntaxPrinter
rewriteRule (pat, rewrite, extensions) =
    printRule C.<$> expr pat <*> expr rewrite <*> mapM ctrlExtension extensions
  where
    printRule patd rewrited extds =
      patd <$> (indent 2 $ text "=>") <$> (indent 4 rewrited) <$> (indent 2 $ vsep extds)

ctrlExtension :: K3 Declaration -> SyntaxPrinter
ctrlExtension d = (text "+>" <+>) C.<$> decl d

spliceType :: SpliceType -> SyntaxPrinter
spliceType = \case
    STLabel    -> return $ text "label"
    STType     -> return $ text "type"
    STExpr     -> return $ text "expr"
    STDecl     -> return $ text "decl"
    STLiteral  -> return $ text "literal"
    STRecord r -> mapM field (Map.toList r) >>= return . braces . cat . punctuate comma
    STList   t -> spliceType t >>= return . brackets

  where field (i,t) = spliceType t >>= return . ((text i <+> colon) <+>)

spliceValue :: SpliceValue -> SyntaxPrinter
spliceValue = \case
    SVar     i    -> return $ text i
    SLabel   i    -> return $ text i
    SType    t    -> typ  t
    SExpr    e    -> expr e
    SDecl    d    -> decl d
    SLiteral l    -> literal l
    SRecord  nsvs -> mapM spliceField (recordElemsAsList nsvs) >>= return . braces . cat . punctuate comma
    SList    svs  -> mapM spliceValue svs >>= return . brackets . cat . punctuate comma

  where spliceField (i,v) = spliceValue v >>= return . ((text i <+> colon) <+>)

typedSpliceVar :: TypedSpliceVar -> SyntaxPrinter
typedSpliceVar (t, i) = spliceType t >>= return . ((text i <+> colon) <+>)

subDecls :: [K3 Declaration] -> Printer [Doc]
subDecls d = mapM decl {-$ filter (not . generatedDecl)-} d
  where generatedDecl (details -> (DGlobal _ _ _, _, anns))  = any isGenerated $ filter isDSpan anns
        generatedDecl (details -> (DTrigger _ _ _, _, anns)) = any isGenerated $ filter isDSpan anns
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
endpointSpec = matchAnnotation "endpoint" isDEndpointDecl spec
  where spec (DSyntax (EndpointDeclaration s _ )) = return $ Just s
        spec _ = return $ Nothing

endpointBindings :: [Annotation Declaration] -> Printer (Maybe EndpointBindings)
endpointBindings = matchAnnotation "endpoint" isDEndpointDecl bindings
  where bindings (DSyntax (EndpointDeclaration _ b)) = return $ Just b
        bindings _ = return $ Nothing

endpoint :: String -> Identifier -> Maybe EndpointSpec -> Doc -> Maybe Doc -> Doc
endpoint kw n specOpt t' eOpt' = case specOpt of
  Nothing                   -> common Nothing
  Just ValueEP              -> common $ maybe Nothing (Just . (text "value" <+>)) eOpt'
  Just (BuiltinEP kind fmt) -> common . Just $ text kind <+> text fmt
  Just (FileEP path txt fmt)    -> common . Just $ text "file" <+> text path  <+> text (txtOrBin txt) <+> text fmt
  Just (NetworkEP addr txt fmt) -> common . Just $ text "network" <+> text addr <+> text (txtOrBin txt) <+> text fmt
  Just (FileSeqEP pathcol txt fmt) -> common . Just $ text "fileseq" <+> text pathcol <+> text (txtOrBin txt) <+> text fmt

  Just (FileMuxEP pathcol txt fmt) ->
    common . Just $ text "filemux" <+> text pathcol <+> text (txtOrBin txt) <+> text fmt

  Just (FileMuxseqEP seqcol txt fmt) ->
    common . Just $ text "filemxsq" <+> text seqcol <+> text (txtOrBin txt) <+> text fmt

  where
    common initializer =
      hang 2 $ text kw <+> text n
                       <+> colon <+> (align t')
                       <+> maybe empty (equals <$>) initializer <> line

    txtOrBin t = if t then "text" else "binary"


-- | Expression syntax printing.
expr :: K3 Expression -> SyntaxPrinter
expr e@(details -> (_,_,anns)) = attachComments (commentE anns) $ (expr' e >>= return . exdoc)

exrs :: Doc -> Either ([Either Doc Doc], Operator) Doc
exrs = Right

exdoc :: Either ([Either Doc Doc], Operator) Doc -> Doc
exdoc (Left (l, op)) = align $ fillCatArgs $ punctuateArgs lsep l
  where lsep = case op of
                 OApp -> space
                 OSeq -> semi <> space
                 _ -> error "Invalid exdoc operator"

        fillCatArgs :: [Either Doc Doc] -> Doc
        fillCatArgs [] = empty
        fillCatArgs [h] = either id (\d -> hardline <> d) h
        fillCatArgs (h:t) = (either (\d -> (d <//>)) (\d -> ((hardline <> d) <//>)) h) $ fillCatArgs t

        punctuateArgs :: Doc -> [Either Doc Doc] -> [Either Doc Doc]
        punctuateArgs _ []      = []
        punctuateArgs _ [dE]    = [dE]
        punctuateArgs p (dE:ds) = (either (\d -> Left $ d <> p) (\d -> Right $ d <> p) dE) : punctuateArgs p ds


exdoc (Right d) = d

expr' :: K3 Expression -> Printer (Either ([Either Doc Doc], Operator) Doc)
expr' (details -> (EConstant c, _, anns)) =
  case c of
    CBool b   -> return . exrs . text $ if b then "true" else "false"
    CInt i    -> return . exrs $ int i
    CByte w   -> return . exrs $ integer $ toInteger w
    CReal r   -> return . exrs $ double r
    CString s -> return . exrs $ dquotes $ text s
    CNone q   -> return . exrs $ text "None" <+> nQualifier q
    CEmpty t  -> typ t >>= return . exrs . emptyCollection (namedEAnnotations anns)

  where
    emptyCollection annIds t = text "empty" </> t <+> annotated (map text annIds)

    nQualifier NoneMut   = text "mut"
    nQualifier NoneImmut = text "immut"

expr' (tag -> EVariable i) = return . exrs $ text i

expr' (details -> (ESome, [x], _)) = qualifierAndExpr x >>= return . exrs . uncurry someExpr
expr' (tag -> ESome)               = exprError "some"

expr' (details -> (EIndirect, [x], _)) = qualifierAndExpr x >>= return . exrs . uncurry indirectionExpr
expr' (tag -> EIndirect)               = exprError "indirection"

expr' (details -> (ETuple, cs, _)) = mapM qualifierAndExpr cs >>= return . exrs . tupleExpr
expr' (tag -> ETuple)              = exprError "tuple"

expr' (details -> (ERecord is, cs, _)) = mapM qualifierAndExpr cs >>= return . exrs . recordExpr is
expr' (tag -> ERecord _)               = exprError "record"

expr' (details -> (ELambda i, [b], _)) = expr b >>= return . exrs . lambdaExpr i
expr' (tag -> ELambda _)               = exprError "lambda"

expr' (details -> (EOperate otag, cs, _))
    | otag `elem` [ONeg, ONot], [a] <- cs    = expr a >>= unary otag
    | otag `elem` [OApp, OSeq], [a, b] <- cs = uncurry (binarychain a b otag) =<< ((,) C.<$> expr' a <*> expr' b)
    | otherwise, [a, b] <- cs                = uncurry (binary otag $ isELambda b) =<< ((,) C.<$> expr a <*> expr b)
    | otherwise                              = exprError "operator"

expr' (details -> (EProject i, [r], _)) = expr r >>= return . exrs . projectExpr i
expr' (tag -> EProject _)               = exprError "project"

expr' (details -> (ELetIn i, [e, b], _)) = (letExpr i C.<$> qualifierAndExpr e <*> expr b) >>= return . exrs
expr' (tag -> ELetIn _)                  = exprError "let"

expr' (details -> (EAssign i, [e], _)) = expr e >>= return . exrs . assignExpr i
expr' (tag -> EAssign _)               = exprError "assign"

expr' (details -> (ECaseOf i, [e, s, n], _)) = (caseExpr i C.<$> expr e <*> expr s <*> expr n) >>= return . exrs
expr' (tag -> ECaseOf _)                     = exprError "case-of"

expr' (details -> (EBindAs b, [e, f], _)) = (bindExpr b C.<$> expr e <*> expr f) >>= return . exrs
expr' (tag -> EBindAs _)                  = exprError "bind-as"

expr' (details -> (EIfThenElse, [p, t, e], _)) = (branchExpr C.<$> expr p <*> expr t <*> expr e) >>= return . exrs
expr' (tag -> EIfThenElse)                     = exprError "if-then-else"

expr' (details -> (EAddress, [h, p], _)) = address h p >>= return . exrs
expr' (tag -> EAddress)                  = exprError "address"

expr' (tag -> ESelf) = return . exrs $ keyword "self"

expr' _ = exprError "unknown"

isELambda :: K3 Expression -> Bool
isELambda (tag -> ELambda _) = True
isELambda _ = False

unary :: Operator -> Doc -> Printer (Either ([Either Doc Doc], Operator) Doc)
unary ONeg e = return . exrs $ text "-" <> e
unary ONot e = return . exrs $ text "not" <+> e
unary op _   = throwSP $ "Invalid unary operator '" ++ show op ++ "'"

bccomments :: K3 Expression -> Either ([Either Doc Doc], Operator) Doc -> Doc
bccomments e d = attachCommentsD (commentE $ annotations e) $ exdoc d

bccommentsL :: K3 Expression -> [Either Doc Doc] -> [Either Doc Doc]
bccommentsL e dl = attachCommentsL (commentE $ annotations e) dl

binarychain :: K3 Expression -> K3 Expression -> Operator
            -> Either ([Either Doc Doc], Operator) Doc
            -> Either ([Either Doc Doc], Operator) Doc
            -> Printer (Either ([Either Doc Doc], Operator) Doc)
binarychain e1 e2 OSeq (Left (l1, OSeq)) (Left (l2, OSeq)) =
  return $ Left ((bccommentsL e1 l1) ++ (bccommentsL e2 l2), OSeq)

binarychain e1 e2@(tag -> ELambda _) op (Left (l1, op1)) r | op == op1 =
  return $ Left (bccommentsL e1 l1 ++ [Right $ bccomments e2 r], op)

binarychain e1 e2 op (Left (l1, op1)) r | op == op1 =
  return $ Left (bccommentsL e1 l1 ++ [Left $ bccomments e2 r], op)

binarychain le re op l r = binary op (isELambda re) (bccomments le l) (bccomments re r)

binary :: Operator -> Bool -> Doc -> Doc -> Printer (Either ([Either Doc Doc], Operator) Doc)
binary op rLambda e e' =
  case op of
    OAdd    -> infixOp "+"
    OSub    -> infixOp "-"
    OMul    -> infixOp "*"
    ODiv    -> infixOp "/"
    OMod    -> infixOp "%"
    OAnd    -> infixOp "&&"
    OOr     -> infixOp "||"
    OEqu    -> infixOp "=="
    ONeq    -> infixOp "/="
    OLth    -> infixOp "<"
    OLeq    -> infixOp "<="
    OGth    -> infixOp ">"
    OGeq    -> infixOp ">="
    OConcat -> infixOp "++"
    OSeq    -> return $ Left ([Left e, Left e'], op)
    OApp    -> return $ Left ([Left e, if rLambda then Right e' else Left e'], op)
    OSnd    -> infixOpSL "<-"
    _       -> throwSP $ "Invalid binary operator '" ++ show op ++ "'"

  where infixOp   opStr = return . exrs $ e <+> text opStr <+> e'
        infixOpSL opStr = return . exrs $ e <+> text opStr </> e'

address :: K3 Expression -> K3 Expression -> SyntaxPrinter
address h p
  | EConstant (CString s) <- tag h = addrExpr C.<$> (return $ text s) <*> expr p
  | otherwise                      = addrExpr C.<$> expr h <*> expr p

symbol :: K3 Expression -> SyntaxPrinter
symbol (tag -> EConstant (CString s)) = return $ text s
symbol _ = throwSP "Invalid symbol expression"

qualifierAndExpr :: K3 Expression -> Printer (Maybe Doc, Doc)
qualifierAndExpr e@(annotations -> anns) =
  reasonM (P.boxToString $ ["", "on expr:"] ++ P.prettyLines e)
    $ (,) C.<$> eQualifier anns <*> expr e

eQualifier :: [Annotation Expression] -> Printer (Maybe Doc)
eQualifier = qualifier "expr mutability" isEQualified eqSyntax
  where
    eqSyntax EImmutable = return $ if showImmut then Just $ text "immut" else Nothing
    eqSyntax EMutable   = return $ Just $ text "mut"
    eqSyntax _          = throwSP "Invalid expression qualifier"

exprError :: String -> Printer (Either ([Either Doc Doc], Operator) Doc)
exprError msg = throwSP $ "Invalid " ++ msg ++ " expression"

-- | Type expression syntax printing.
typ :: K3 Type -> SyntaxPrinter
typ t@(details -> (_,_,anns)) = attachComments (commentT anns) $ typ' t

typ' :: K3 Type -> SyntaxPrinter
typ' (tag -> TBool)       = return $ text "bool"
typ' (tag -> TByte)       = return $ text "byte"
typ' (tag -> TInt)        = return $ text "int"
typ' (tag -> TReal)       = return $ text "real"
typ' (tag -> TString)     = return $ text "string"
typ' (tag -> TAddress)    = return $ text "address"
typ' (tag -> TTop)        = return $ text "top"

typ' (details -> (TOption, [x], _)) = qualifierAndType x >>= return . uncurry optionType
typ' (tag -> TOption)               = throwSP "Invalid option type"

typ' (details -> (TIndirection, [x], _)) = qualifierAndType x >>= return . uncurry indirectionType
typ' (tag -> TIndirection)               = throwSP "Invalid indirection type"

typ' (details -> (TTuple, ch, _)) = mapM qualifierAndType ch >>= return . tupleType

typ' (details -> (TRecord ids, ch, _))
  | length ids == length ch  = mapM qualifierAndType ch >>= return . recordType ids
  | otherwise                = throwSP "Invalid record type"

typ' (details -> (TFunction, [a,r], _)) =
  mapM typ [a,r] >>= \chT -> return $ funType (onArg $ head chT) (last chT)
  where onArg = if isTFunction a then parens else id

typ' (tag -> TFunction)                 = throwSP "Invalid function type"

typ' (details -> (TSource, [x], _)) = typ x
typ' (tag -> TSource)               = throwSP "Invalid sink type"

typ' (details -> (TSink, [x], _)) = typ x
typ' (tag -> TSink)               = throwSP "Invalid sink type"

typ' (details -> (TTrigger, [x], _)) = typ x >>= return . triggerType
typ' (tag -> TTrigger)               = throwSP "Invalid trigger type"

typ' (details -> (TCollection, [x], anns)) =
  typ x >>= return . collectionType (map text $ namedTAnnotations anns)

typ' (tag -> TCollection) = throwSP "Invalid collection type"

typ' (tag -> TBuiltIn TSelf)      = return $ keyword "self"
typ' (tag -> TBuiltIn TContent)   = return $ keyword "content"
typ' (tag -> TBuiltIn THorizon)   = return $ keyword "horizon"
typ' (tag -> TBuiltIn TStructure) = return $ keyword "structure"

typ' (details -> (TForall tvd, [x], _)) = polymorphicType C.<$> mapM typeVarDecl tvd <*> typ x
typ' (tag -> TForall _) = throwSP "Invalid forall type"

typ' (tag -> TDeclaredVar n) = return $ keyword n

typ' _ = throwSP "Invalid type syntax"

qualifierAndType :: K3 Type -> Printer (Maybe Doc, Doc)
qualifierAndType t@(annotations -> anns) =
  reasonM (P.boxToString $ ["", "on type:"] ++ P.prettyLines t)
    $ (,) C.<$> tQualifier anns <*> typ t

tQualifier :: [Annotation Type] -> Printer (Maybe Doc)
tQualifier anns = qualifier "type mutability" isTQualified tqSyntax anns
  where
    tqSyntax TImmutable = return $ if showImmut then Just $ text "immut" else Nothing
    tqSyntax TMutable   = return $ Just $ text "mut"
    tqSyntax _          = throwSP "Invalid type qualifier"

typeVarDecl :: TypeVarDecl -> SyntaxPrinter
typeVarDecl (TypeVarDecl i mlbtExpr mubtExpr) = do
  lb <- if isNothing mlbtExpr then return empty else
          liftM (<+> text "=<") $ typ (fromJust mlbtExpr)
  ub <- if isNothing mubtExpr then return empty else
          liftM (text "<=" <+>) $ typ (fromJust mubtExpr)
  return $ lb <+> text i <+> ub


-- | Literals pretty printing
literal :: K3 Literal -> SyntaxPrinter
literal l@(details -> (_,_,anns)) = attachComments (commentL anns) $ literal' l

literal' :: K3 Literal -> SyntaxPrinter
literal' (tag -> LBool b)   = return . text $ if b then "true" else "false"
literal' (tag -> LByte b)   = return . integer $ toInteger b
literal' (tag -> LInt i)    = return $ int i
literal' (tag -> LReal r)   = return $ double r
literal' (tag -> LString s) = return . dquotes $ text s
literal' (tag -> LNone nm)  = return $ text "None" <+> nQualifier nm
  where nQualifier NoneMut   = text "mut"
        nQualifier NoneImmut = text "immut"

literal' (details -> (LSome, [x], _))      = qualifierAndLiteral x >>= return . uncurry someLiteral
literal' (details -> (LIndirect, [x], _))  = qualifierAndLiteral x >>= return . uncurry indirectionLiteral
literal' (details -> (LTuple, ch, _))      = mapM qualifierAndLiteral ch >>= return . tupleLiteral
literal' (details -> (LRecord ids, ch, _)) = mapM qualifierAndLiteral ch >>= return . recordLiteral ids
literal' (details -> (LEmpty t, [], anns)) = typ t >>= return . emptyLiteral (namedLAnnotations anns)

literal' (details -> (LCollection t, elems, anns)) =
  collectionLiteral (namedLAnnotations anns) C.<$> elemType t <*> mapM (elemVal t) elems
  where elemType (details -> (TRecord [x], [y], _)) = qualifierAndType y >>= return . tSingleElem x
        elemType (details -> (TRecord ids, ch, _))  = mapM qualifierAndType ch >>= return . tMultiElem ids
        elemType _ = throwSP "Invalid collection literal element type"

        elemVal (tag -> TRecord [_]) (details -> (LRecord [_], [v], _)) = literal' v
        elemVal (tag -> TRecord _) v = literal' v
        elemVal _ _ = throwSP "Invalid collection literal element value"

        tSingleElem n (qualTOpt,t') = text n <+> maybe colon (colon <+>) qualTOpt <+> t'
        tMultiElem ids qualC = cat $ punctuate comma $ map (\(a,b) -> text a <+> colon <+> b)
                                   $ zip ids $ map (\(qOpt,t') -> maybe t' (<+> t') qOpt) qualC

literal' (details -> (LAddress, [h,p], _))
  | LString s <- tag h = literal' p >>= return . addressLiteral (text s)
  | otherwise = throwSP "Invalid address literal"

literal' _ = throwSP "Invalid literal during syntax printing"

qualifierAndLiteral :: K3 Literal -> Printer(Doc, Doc)
qualifierAndLiteral l@(annotations -> anns) = (,) C.<$> lQualifier anns <*> literal l

lQualifier :: [Annotation Literal] -> SyntaxPrinter
lQualifier anns = qualifier "lit mutability" isLQualified lqSyntax anns
  where
    lqSyntax LImmutable = return $ if showImmut then text "immut" else empty
    lqSyntax LMutable   = return $ text "mut"
    lqSyntax _          = throwSP "Invalid literal qualifier"


{- Syntax constructors -}

optionType :: Maybe Doc -> Doc -> Doc
optionType qual t = text "option" <+> maybe t (<+> t) qual

indirectionType :: Maybe Doc -> Doc -> Doc
indirectionType qual t = text "ind" <+> maybe t (<+> t) qual

tupleType :: [(Maybe Doc, Doc)] -> Doc
tupleType qualC = stupled $ map (\(q,t) -> maybe t (<+> t) q) qualC

recordType :: [Identifier] -> [(Maybe Doc, Doc)] -> Doc
recordType ids qualC =
  commaBrace $ map (\(a,b) -> text a <+> colon <+> b)
             $ zip ids $ map (\(q,t) -> maybe t (<+> t) q) qualC

collectionType :: [Doc] -> Doc -> Doc
collectionType namedAnns t = text "collection" <+> t <+> annotated namedAnns

funType :: Doc -> Doc -> Doc
funType arg ret = arg </> text "->" <+> ret

triggerType :: Doc -> Doc
triggerType t = text "trigger" <+> t

polymorphicType :: [Doc] -> Doc -> Doc
polymorphicType [] t    = t
polymorphicType tvars t = text "forall" <+> (foldl1 (<+>) tvars) <+> dot <+> t

someExpr :: Maybe Doc -> Doc -> Doc
someExpr qualEOpt e = text "Some" <+> (maybe e (<+> e) qualEOpt)

indirectionExpr :: Maybe Doc -> Doc -> Doc
indirectionExpr qualEOpt e = text "ind" <+> (maybe e (<+> e) qualEOpt)

tupleExpr :: [(Maybe Doc, Doc)] -> Doc
tupleExpr qualC = stupled $ map (\(qOpt, e) -> maybe e (<+> e) qOpt) qualC

recordExpr :: [Identifier] -> [(Maybe Doc, Doc)] -> Doc
recordExpr ids qualC =
  commaBrace $ map (\(a,b) -> text a <+> colon <+> b)
             $ zip ids $ map (\(qOpt, e) -> maybe e (<+> e) qOpt) qualC

lambdaExpr :: Identifier -> Doc -> Doc
lambdaExpr n b = parens $ (backslash <> text n </> text "->") </> b

projectExpr :: Identifier -> Doc -> Doc
projectExpr n r = r <//> dot <> text n

letExpr :: Identifier -> (Maybe Doc, Doc) -> Doc -> Doc
letExpr n (qOpt, e) b =
  (text "let" <+> align (text n </> (maybe equals (equals <+>) qOpt) </> e) </> text "in") <$> b

assignExpr :: Identifier -> Doc -> Doc
assignExpr n e = hang 2 $ (text n <+> equals) </> e

caseExpr :: Identifier -> Doc -> Doc -> Doc -> Doc
caseExpr i e s n = (text "case" </> hang 2 e </> text "of")
                                <$> indent 2 sCase <$> indent 2 nCase
  where sCase = braces $ text "Some" <+> text i <+> text "->" </> s
        nCase = braces $ text "None" <+> text "->" </> n

bindExpr :: Binder -> Doc -> Doc -> Doc
bindExpr b e e' = (text "bind" </> hang 2 e </> text "as"
                               </> (hang 2 $ binder b)
                               </> text "in") <$> indent 2 e'
  where
    binder (BIndirection i) = text "ind" <+> text i
    binder (BTuple ids)     = stupled $ map text ids
    binder (BRecord idMap)  = commaBrace $ map (\(s,t) -> text s <+> colon <+> text t) idMap

branchExpr :: Doc -> Doc -> Doc -> Doc
branchExpr p t e = text "if" <+> (align $ p <$> text "then" </> t <$> text "else" </> e)

addrExpr :: Doc -> Doc -> Doc
addrExpr h p = h <> colon <> p



someLiteral :: Doc -> Doc -> Doc
someLiteral qual l = text "Some" <+> qual <+> l

indirectionLiteral :: Doc -> Doc -> Doc
indirectionLiteral qual l = text "ind" <+> qual <+> l

tupleLiteral :: [(Doc, Doc)] -> Doc
tupleLiteral qualC = stupled $ map (uncurry (<+>)) qualC

recordLiteral :: [Identifier] -> [(Doc, Doc)] -> Doc
recordLiteral ids qualC =
  commaBrace $ map (\(a,b) -> text a <+> colon <+> b)
             $ zip ids $ map (uncurry (<+>)) qualC

emptyLiteral :: [Identifier] -> Doc -> Doc
emptyLiteral annIds t = text "empty" <+> t <+> annotated (map text annIds)

collectionLiteral :: [Identifier] -> Doc -> [Doc] -> Doc
collectionLiteral annIds t elems =
  text "collection" <+> text "{|" <+> t <+> text "|" <+> (cat $ punctuate comma elems) <+> text "|}"
                    <+> annotated (map text annIds)

addressLiteral :: Doc -> Doc -> Doc
addressLiteral h p = h <> colon <> p


{- Source comments -}
comment :: SyntaxAnnotation -> Maybe (Doc, Bool)
comment (SourceComment post multi _ contents) = Just (string $ prefix ++ contents ++ suffix, post)
  where prefix = if multi then "/* " else "//"
        suffix = if multi then "*/" else "\n"

comment _ = Nothing

annotatedComments :: [Annotation a] -> (Annotation a -> Maybe SyntaxAnnotation)
                  -> (Maybe Doc, Maybe Doc)
annotatedComments anns extractF =
  let (post, pre) = partition snd docs in (mk pre, mk post)
  where mk d = if null d then Nothing else Just $ (vcat $ map (group . fst) d) <> line
        docs = mapMaybe (\x -> extractF x >>= comment) anns

commentD :: [Annotation Declaration] -> (Maybe Doc, Maybe Doc)
commentD anns = annotatedComments anns extractSyntax
  where extractSyntax (DSyntax x) = Just x
        extractSyntax _ = Nothing

commentE :: [Annotation Expression] -> (Maybe Doc, Maybe Doc)
commentE anns = annotatedComments anns extractSyntax
  where extractSyntax (ESyntax x) = Just x
        extractSyntax _ = Nothing

commentL :: [Annotation Literal] -> (Maybe Doc, Maybe Doc)
commentL anns = annotatedComments anns extractSyntax
  where extractSyntax (LSyntax x) = Just x
        extractSyntax _ = Nothing

commentT :: [Annotation Type] -> (Maybe Doc, Maybe Doc)
commentT anns = annotatedComments anns extractSyntax
  where extractSyntax (TSyntax x) = Just x
        extractSyntax _ = Nothing

attachComments :: (Maybe Doc, Maybe Doc) -> SyntaxPrinter -> SyntaxPrinter
attachComments prePostOpt body = attachCommentsD prePostOpt C.<$> body

attachCommentsD :: (Maybe Doc, Maybe Doc) -> Doc -> Doc
attachCommentsD (Nothing, Nothing)    body = body
attachCommentsD (Just pre, Nothing)   body = pre <> body
attachCommentsD (Nothing, Just post)  body = body <+> post
attachCommentsD (Just pre, Just post) body = pre <> body <+> post

attachCommentsL :: (Maybe Doc, Maybe Doc) -> [Either Doc Doc] -> [Either Doc Doc]
attachCommentsL _ [] = []
attachCommentsL (Nothing, Nothing)    dl = dl
attachCommentsL (Just pre, Nothing)   (h:t) = (either (Left . (pre <>)) (Right . (pre <>)) h):t
attachCommentsL (Nothing, Just post)  dl    = init dl ++ [either (Left . (<+> post)) (Right . (<+> post)) $ last dl]
attachCommentsL (Just pre, Just post) (h:t) = [either (Left . (pre <>)) (Right . (pre <>)) h]
                                                ++ init t
                                                ++ [either (Left . (<+> post)) (Right . (<+> post)) $ last t]

{- Helpers -}

keyword :: String -> Doc
keyword = text

sencloseSep :: Doc -> Doc -> Doc -> [Doc] -> Doc
sencloseSep left right sp ds
    = case ds of
        []  -> left <> right
        [d] -> left <> d <> right
        _   -> align (fillCat (zipWith (<>) (left : repeat sp) ds) <> right)

commaBrace :: [Doc] -> Doc
commaBrace = sencloseSep lbrace rbrace (comma <> text " ")

stupled :: [Doc] -> Doc
stupled = sencloseSep lparen rparen (comma <> text " ")

annotated :: [Doc] -> Doc
annotated []   = empty
annotated [x]  = text "@" <> x
annotated anns = text "@" <> commaBrace anns

matchAnnotation :: (Eq (Annotation a), HasUID (Annotation a))
                => String -> (Annotation a -> Bool) -> (Annotation a -> Printer b)
                -> [Annotation a] -> Printer b
matchAnnotation desc matchF mapF anns =
  let uidStr = maybe "" show (getUID =<< find (isJust . getUID) anns) in
  case filter matchF anns of
    []            -> throwSP $ unwords ["No matching", desc, "annotation found at", uidStr]
    [q]           -> mapF q
    l | same l    -> mapF $ head l
      | otherwise -> throwSP $ unwords ["Multiple matching", desc, "annotations found at", uidStr]
  where same l = 1 == length (nub l)

qualifier :: (Eq (Annotation a), HasUID (Annotation a))
          => String -> (Annotation a -> Bool) -> (Annotation a -> Printer b)
          -> [Annotation a] -> Printer b
qualifier = matchAnnotation

optionalPrinter :: (a -> Printer b) -> Maybe a -> Printer (Maybe b)
optionalPrinter f = maybe (return Nothing) (\x -> f x >>= return . Just)
