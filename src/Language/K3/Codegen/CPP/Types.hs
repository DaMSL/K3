module Language.K3.Codegen.CPP.Types where

import Data.Functor

import Control.Monad.State
import Control.Monad.Trans.Either

import qualified Data.Map as M
import qualified Data.Set as S

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.Core.Declaration

import qualified Language.K3.Codegen.Imperative as I

-- | The C++ code generation monad. Provides access to various configuration values and error
-- reporting.
type CPPGenM a = EitherT CPPGenE (State CPPGenS) a

-- | Run C++ code generation action using a given initial state.
runCPPGenM :: CPPGenS -> CPPGenM a -> (Either CPPGenE a, CPPGenS)
runCPPGenM s = flip runState s . runEitherT

-- | Error messages thrown by C++ code generation.
data CPPGenE = CPPGenE String deriving (Eq, Read, Show)

-- | Throw a code generation error.
throwE :: CPPGenE -> CPPGenM a
throwE = left

-- | All generated code is produced in the form of pretty-printed blocks.
type CPPGenR = Doc

-- | State carried around during C++ code generation.
data CPPGenS = CPPGenS {
        -- | UUID counter for generating identifiers.
        uuid :: Int,

        -- | Code necessary to initialize global declarations.
        initializations :: CPPGenR,

        -- | Forward declarations for constructs as a result of cyclic scope.
        forwards :: CPPGenR,

        -- | The global variables declared, for use in exclusion during λ-capture. Needs to be
        -- supplied ahead-of-time, due to cyclic scoping.
        globals  :: [Identifier],

        refreshables :: [Identifier],

        -- | Mapping of record signatures to corresponding record structure, for generation of
        -- record classes.
        recordMap :: M.Map Identifier [(Identifier, K3 Type)],

        -- | Mapping of annotation class names to list of member declarations, for eventual
        -- declaration of composite classes.
        annotationMap :: M.Map Identifier [AnnMemDecl],

        -- | Set of annotation combinations actually encountered during the program.
        composites :: S.Set (S.Set Identifier),

        -- | The set of triggers declared in a program, used to populate the dispatch table.
        triggers :: S.Set Identifier,

        -- | The serialization method to use.
        serializationMethod :: SerializationMethod

    } deriving Show

-- | The default code generation state.
defaultCPPGenS :: CPPGenS
defaultCPPGenS = CPPGenS 0 empty empty [] [] M.empty M.empty S.empty S.empty BoostSerialization

refreshCPPGenS :: CPPGenM ()
refreshCPPGenS = do
    gs <- globals <$> get
    rs <- refreshables <$> get
    put defaultCPPGenS { globals = gs, refreshables = rs }

-- | Copy state elements from the imperative transformation to CPP code generation.
transitionCPPGenS :: I.ImperativeS -> CPPGenS
transitionCPPGenS is = defaultCPPGenS { globals = I.globals is, refreshables = I.refreshables is}

-- | Generate a new unique symbol, required for temporary reification.
genSym :: CPPGenM Identifier
genSym = do
    current <- uuid <$> get
    modify (\s -> s { uuid = succ (uuid s) })
    return $ '_':  show current

-- | Add an annotation to the code generation state.
addAnnotation :: Identifier -> [AnnMemDecl] -> CPPGenM ()
addAnnotation i amds = modify (\s -> s { annotationMap = M.insert i amds (annotationMap s) })

-- | Add a new composite specification to the code generation state.
addComposite :: [Identifier] -> CPPGenM ()
addComposite is = modify (\s -> s { composites = S.insert (S.fromList is) (composites s) })

-- | Add a new record specification to the code generation state.
addRecord :: Identifier -> [(Identifier, K3 Type)] -> CPPGenM ()
addRecord i its = modify (\s -> s { recordMap = M.insert i its (recordMap s) })

-- | Add a new trigger specification to the code generation state.
addTrigger :: Identifier -> CPPGenM ()
addTrigger i = modify (\s -> s { triggers = S.insert i (triggers s) })

data SerializationMethod
    = BoostSerialization
  deriving (Eq, Read, Show)
