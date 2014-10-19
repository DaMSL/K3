{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

{-| K3-Haskell Metaprogramming Bridge.
    This allows Haskell expressions to be used in K3 metaprogramming expressions
    to generate the relevant K3 AST for splicing.

    All quoted variables in the Haskell expression are substituted from the K3
    metaprogramming splice context. Thus we recommend avoiding using
    TemplateHaskell features inside the Haskell expression.

    The bridge parses the Haskell expression with haskell-src-exts and substitutes
    any VarQuote expressions from the splice context.
    Next we pretty-print the resulting expression, and then use the hint package
    to interpret the pretty-printing.
-}

module Language.K3.Metaprogram.MetaHK3 where

import Language.Haskell.Interpreter hiding ( TemplateHaskell )
import Language.Haskell.Interpreter.Unsafe

import Language.Haskell.Exts.Extension
import Language.Haskell.Exts.Parser
import Language.Haskell.Exts.Pretty
import Language.Haskell.Exts.Syntax

import Language.K3.Core.Common
import Language.K3.Core.Metaprogram
import Language.K3.Metaprogram.DataTypes

metaHK3TraceLogging :: Bool
metaHK3TraceLogging = False

localLog :: (Functor m, Monad m) => String -> m ()
localLog = logVoid metaHK3TraceLogging

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
localLogAction = logAction metaHK3TraceLogging

hk3Msg :: [String] -> String
hk3Msg sl = unwords $ ["HK3"] ++ sl

{- K3-Haskell AST splicing -}

evalHaskellProg :: SpliceContext -> String -> GeneratorM SpliceValue
evalHaskellProg sctxt expr = case parseExpWithMode pm expr of
    ParseOk exprAST   -> evalHaskell $ spliceQuotesExp sctxt exprAST
    ParseFailed _ msg -> throwG $ hk3Msg ["splice parse failed:", msg]
  where
    pm = defaultParseMode {extensions = [EnableExtension TemplateHaskell]}

    evalHaskell (prettyPrint -> astStr) = localLogAction (evalLoggerF astStr) $
      generatorWithEvalOptions $ \evalOpts -> generatorWithInterpreter $ \itptr -> do
        r <- liftIO $ unsafeRunInterpreterWithArgs (mpInterpArgs evalOpts) $ interpretWithOptions itptr astStr
        either interpFail return r

    evalLoggerF astStr = maybe (Just $ hk3Msg ["input:", astStr]) (\r -> Just $ hk3Msg ["result:", show r])

    interpretWithOptions itptr str = itptr >> do
      resStr <- eval str
      return $ ((read resStr) :: SpliceValue)

    interpFail err = throwG $ hk3Msg ["splice eval failed:", show err]

injectSpliceValue :: SpliceValue -> Maybe Exp
injectSpliceValue v = case parseExp $ show v of
  ParseOk e ->  Just e
  _ -> Nothing

{- Declarations -}
spliceQuotesDecl :: SpliceContext -> Decl -> Decl
spliceQuotesDecl sctxt decl = case decl of
    FunBind matchL -> FunBind $ map rcrm matchL
    PatBind l p tOpt rhs binds -> PatBind l (rcrp p) tOpt (rcrr rhs) $ rcrb binds
    _ -> decl
  where rcrm = spliceQuotesMatch sctxt
        rcrp = spliceQuotesPat   sctxt
        rcrr = spliceQuotesRhs   sctxt
        rcrb = spliceQuotesBinds sctxt

{- Unhandled cases
  TypeDecl SrcLoc Name [TyVarBind] Type
  TypeFamDecl SrcLoc Name [TyVarBind] (Maybe Kind)
  DataDecl SrcLoc DataOrNew Context Name [TyVarBind] [QualConDecl] [Deriving]
  GDataDecl SrcLoc DataOrNew Context Name [TyVarBind] (Maybe Kind) [GadtDecl] [Deriving]
  DataFamDecl SrcLoc Context Name [TyVarBind] (Maybe Kind)
  TypeInsDecl SrcLoc Type Type
  DataInsDecl SrcLoc DataOrNew Type [QualConDecl] [Deriving]
  GDataInsDecl SrcLoc DataOrNew Type (Maybe Kind) [GadtDecl] [Deriving]
  ClassDecl SrcLoc Context Name [TyVarBind] [FunDep] [ClassDecl]
  InstDecl SrcLoc Context QName [Type] [InstDecl]
  DerivDecl SrcLoc Context QName [Type]
  InfixDecl SrcLoc Assoc Int [Op]
  DefaultDecl SrcLoc [Type]
  SpliceDecl SrcLoc Exp
  TypeSig SrcLoc [Name] Type
  ForImp SrcLoc CallConv Safety String Name Type
  ForExp SrcLoc CallConv String Name Type
  RulePragmaDecl SrcLoc [Rule]
  DeprPragmaDecl SrcLoc [([Name], String)]
  WarnPragmaDecl SrcLoc [([Name], String)]
  InlineSig SrcLoc Bool Activation QName
  InlineConlikeSig SrcLoc Activation QName
  SpecSig SrcLoc Activation QName [Type]
  SpecInlineSig SrcLoc Bool Activation QName [Type]
  InstSig SrcLoc Context QName [Type]
  AnnPragma SrcLoc Annotation
-}

spliceQuotesMatch :: SpliceContext -> Match -> Match
spliceQuotesMatch sctxt (Match l n patL tOpt rhs binds) =
    Match l n (map rcrp patL) tOpt (rcrr rhs) $ rcrb binds
  where rcrp = spliceQuotesPat   sctxt
        rcrr = spliceQuotesRhs   sctxt
        rcrb = spliceQuotesBinds sctxt

spliceQuotesRhs :: SpliceContext -> Rhs -> Rhs
spliceQuotesRhs sctxt = \case
    UnGuardedRhs e    -> UnGuardedRhs $ rcre e
    GuardedRhss grhsL -> GuardedRhss $ map rcrg grhsL
  where rcre = spliceQuotesExp sctxt
        rcrg = spliceQuotesGuardedRhs sctxt

spliceQuotesGuardedRhs :: SpliceContext -> GuardedRhs -> GuardedRhs
spliceQuotesGuardedRhs sctxt (GuardedRhs l stmtL e) = GuardedRhs l (map rcrs stmtL) $ rcre e
  where rcre = spliceQuotesExp  sctxt
        rcrs = spliceQuotesStmt sctxt


{- Expressions -}
spliceQuotesExp :: SpliceContext -> Exp -> Exp
spliceQuotesExp sctxt e = case e of
  VarQuote (UnQual (Ident i)) -> maybe e id $ do
    v <- lookupSCtxt i sctxt
    injectSpliceValue v

  InfixApp e1 qop e2      -> InfixApp (rcr e1) qop $ rcr e2
  App e1 e2               -> App (rcr e1) $ rcr e2
  NegApp e1               -> NegApp $ rcr e1
  Lambda l patL e1        -> Lambda l (map rcrp patL) $ rcr e1
  Let binds e1            -> Let (rcrb binds) $ rcr e1
  If pe te ee             -> If (rcr pe) (rcr te) $ rcr ee
  MultiIf ifAltL          -> MultiIf $ map rcrif ifAltL
  Case e1 altL            -> Case (rcr e1) $ map rcra altL
  Do stmtL                -> Do $ map rcrs stmtL
  MDo stmtL               -> MDo $ map rcrs stmtL
  Tuple b eL              -> Tuple b $ map rcr eL
  TupleSection b eOptL    -> TupleSection b $ map (maybe Nothing (Just . rcr)) eOptL
  List eL                 -> List $ map rcr eL
  Paren e1                -> Paren $ rcr e1
  LeftSection e1 qop      -> LeftSection (rcr e1) qop
  RightSection qop e1     -> RightSection qop (rcr e1)
  RecConstr qn fuL        -> RecConstr qn $ map rcrf fuL
  RecUpdate e1 fuL        -> RecUpdate (rcr e1) $ map rcrf fuL
  EnumFrom e1             -> EnumFrom $ rcr e1
  EnumFromTo e1 e2        -> EnumFromTo (rcr e1) $ rcr e2
  EnumFromThen e1 e2      -> EnumFromThen (rcr e1) $ rcr e2
  EnumFromThenTo e1 e2 e3 -> EnumFromThenTo (rcr e1) (rcr e2) $ rcr e3
  ListComp e1 qsL         -> ListComp (rcr e1) $ map rcrq qsL
  ParComp e1 qsLL         -> ParComp  (rcr e1) $ map (map rcrq) qsLL
  ExpTypeSig l e1 ty      -> ExpTypeSig l (rcr e1) ty
  CorePragma s e1         -> CorePragma s $ rcr e1
  SCCPragma s e1          -> SCCPragma s $ rcr e1
  GenPragma s ip jp e1    -> GenPragma s ip jp $ rcr e1
  Proc l p e1             -> Proc l p $ rcr e1
  LeftArrApp e1 e2        -> LeftArrApp (rcr e1) $ rcr e2
  RightArrApp e1 e2       -> RightArrApp (rcr e1) $ rcr e2
  LeftArrHighApp e1 e2    -> LeftArrHighApp (rcr e1) $ rcr e2
  RightArrHighApp e1 e2   -> RightArrHighApp (rcr e1) $ rcr e2
  LCase altL              -> LCase $ map rcra altL

  _ -> e

  where rcr   = spliceQuotesExp   sctxt
        rcrp  = spliceQuotesPat   sctxt
        rcra  = spliceQuotesAlt   sctxt
        rcrif = spliceQuotesIfAlt sctxt
        rcrs  = spliceQuotesStmt  sctxt
        rcrb  = spliceQuotesBinds sctxt
        rcrf  = spliceQuotesFieldUpdate sctxt
        rcrq  = spliceQuotesQualStmt sctxt

  {- Unhandled cases
  Var QName
  IPVar IPName
  Con QName
  Lit Literal

  TypQuote QName
  BracketExp Bracket
  SpliceExp Splice
  QuasiQuote String String

  XTag SrcLoc XName [XAttr] (Maybe Exp) [Exp]
  XETag SrcLoc XName [XAttr] (Maybe Exp)
  XPcdata String
  XExpTag Exp
  XChildTag SrcLoc [Exp]
  -}

spliceQuotesBinds :: SpliceContext -> Binds -> Binds
spliceQuotesBinds sctxt = \case
    BDecls declL -> BDecls $ map rcrd declL
    IPBinds ipbL -> IPBinds $ map rcrb ipbL
  where rcrd = spliceQuotesDecl sctxt
        rcrb = spliceQuotesIPBind sctxt

spliceQuotesIPBind :: SpliceContext -> IPBind -> IPBind
spliceQuotesIPBind sctxt (IPBind l ipn e) = IPBind l ipn $ rcre e
  where rcre = spliceQuotesExp sctxt

spliceQuotesAlt :: SpliceContext -> Alt -> Alt
spliceQuotesAlt sctxt (Alt l pat gAlts binds) = Alt l (rcrp pat) (rcrg gAlts) $ rcrb binds
  where rcrp = spliceQuotesPat sctxt
        rcrg = spliceQuotesGuardedAlts sctxt
        rcrb = spliceQuotesBinds sctxt

spliceQuotesIfAlt :: SpliceContext -> IfAlt -> IfAlt
spliceQuotesIfAlt sctxt (IfAlt e1 e2) = IfAlt (rcr e1) $ rcr e2
  where rcr = spliceQuotesExp sctxt

spliceQuotesGuardedAlts :: SpliceContext -> GuardedAlts -> GuardedAlts
spliceQuotesGuardedAlts sctxt = \case
    UnGuardedAlt e -> UnGuardedAlt $ rcr e
    GuardedAlts gL -> GuardedAlts $ map rcrg gL
  where rcr  = spliceQuotesExp sctxt
        rcrg = spliceQuotesGuardedAlt sctxt

spliceQuotesGuardedAlt :: SpliceContext -> GuardedAlt -> GuardedAlt
spliceQuotesGuardedAlt sctxt (GuardedAlt l stmtL e) = GuardedAlt l (map rcrs stmtL) $ rcr e
  where rcr  = spliceQuotesExp sctxt
        rcrs = spliceQuotesStmt sctxt

spliceQuotesFieldUpdate :: SpliceContext -> FieldUpdate -> FieldUpdate
spliceQuotesFieldUpdate sctxt fu = case fu of
  FieldUpdate qn e -> FieldUpdate qn $ spliceQuotesExp sctxt e
  _ -> fu

spliceQuotesQualStmt :: SpliceContext -> QualStmt -> QualStmt
spliceQuotesQualStmt sctxt = \case
    QualStmt s         -> QualStmt $ rcrs s
    ThenTrans e        -> ThenTrans $ rcr e
    ThenBy e1 e2       -> ThenBy (rcr e1) $ rcr e2
    GroupBy e          -> GroupBy $ rcr e
    GroupUsing e       -> GroupUsing $ rcr e
    GroupByUsing e1 e2 -> GroupByUsing (rcr e1) $ rcr e2
  where rcr  = spliceQuotesExp  sctxt
        rcrs = spliceQuotesStmt sctxt

{- Statements -}
spliceQuotesStmt :: SpliceContext -> Stmt -> Stmt
spliceQuotesStmt sctxt = \case
    Generator l p e -> Generator l (rcrp p) $ rcre e
    Qualifier e     -> Qualifier $ rcre e
    LetStmt binds   -> LetStmt $ rcrb binds
    RecStmt stmtL   -> RecStmt $ map rcr stmtL
  where rcr  = spliceQuotesStmt  sctxt
        rcre = spliceQuotesExp   sctxt
        rcrp = spliceQuotesPat   sctxt
        rcrb = spliceQuotesBinds sctxt


{- Patterns -}
spliceQuotesPat :: SpliceContext -> Pat -> Pat
spliceQuotesPat sctxt pat = case pat of
    PNeg p              -> PNeg $ rcr p
    PInfixApp p1 qn p2  -> PInfixApp (rcr p1) qn (rcr p2)
    PApp qn patL        -> PApp qn $ map rcr patL
    PTuple boxed patL   -> PTuple boxed $ map rcr patL
    PList patL          -> PList $ map rcr patL
    PParen p            -> PParen $ rcr p
    PRec qn patFieldL   -> PRec qn $ map rcrpf patFieldL
    PAsPat n p          -> PAsPat n $ rcr p
    PIrrPat p           -> PIrrPat $ rcr p
    PatTypeSig l p ty   -> PatTypeSig l (rcr p) ty
    PViewPat e p        -> PViewPat (rcre e) $ rcr p
    _ -> pat

  where rcr   = spliceQuotesPat sctxt
        rcre  = spliceQuotesExp sctxt
        rcrpf = spliceQuotesPatField sctxt

  {- Unhandled cases
  PVar Name
  PLit Literal
  PNPlusK Name Integer
  PWildCard

  PRPat [RPat]
  PXTag SrcLoc XName [PXAttr] (Maybe Pat) [Pat]
  PXETag SrcLoc XName [PXAttr] (Maybe Pat)
  PXPcdata String
  PXPatTag Pat
  PXRPats [RPat]
  PQuasiQuote String String
  PBangPat Pat
  -}

spliceQuotesPatField :: SpliceContext -> PatField -> PatField
spliceQuotesPatField sctxt pf = case pf of
  PFieldPat qn p -> PFieldPat qn $ spliceQuotesPat sctxt p
  _ -> pf

