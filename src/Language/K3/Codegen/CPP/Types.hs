module Language.K3.Codegen.CPP.Types where

import Control.Monad.State
import Control.Monad.Trans.Except

import Data.List
import qualified Data.Map as M
import qualified Data.Set as S

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Annotation
import Language.K3.Core.Common hiding ( current )
import Language.K3.Core.Type
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import qualified Language.K3.Codegen.CPP.Representation as R

-- | The C++ code generation monad. Provides access to various configuration values and error
-- reporting.
type CPPGenM a = ExceptT CPPGenE (State CPPGenS) a

-- | Run C++ code generation action using a given initial state.
runCPPGenM :: CPPGenS -> CPPGenM a -> (Either CPPGenE a, CPPGenS)
runCPPGenM s = flip runState s . runExceptT

-- | Error messages thrown by C++ code generation.
data CPPGenE = CPPGenE String deriving (Eq, Read, Show)

-- -- | Throw a code generation error.
throwE :: CPPGenE -> CPPGenM a
throwE = Control.Monad.Trans.Except.throwE

-- | All generated code is produced in the form of pretty-printed blocks.
type CPPGenR = Doc

-- | State carried around during C++ code generation.
data CPPGenS = CPPGenS {
        -- | UUID counter for generating identifiers.
        uuid :: Int,

        -- | Code necessary to initialize global declarations.
        initializations :: [R.Statement],

        staticDeclarations    :: [R.Statement],
        staticInitializations :: [R.Statement],

        -- | User-defined global initializations
        globalInitializations :: [R.Statement],

        -- | Forward declarations for constructs as a result of cyclic scope.
        forwards :: [R.Declaration],

        -- | The global variables declared, for use in exclusion during λ-capture. Needs to be
        -- supplied ahead-of-time, due to cyclic scoping.
        globals  :: [(Identifier, (K3 Type, Bool))], -- Whether it's a builtin

        patchables :: [(Identifier, (K3 Type, Bool))], -- Whether we need to set

        showables :: [(Identifier, K3 Type)],

        -- | Mapping of record signatures to corresponding record structure, for generation of
        -- record classes.
        recordMap :: M.Map Identifier [(Identifier, K3 Type)],

        -- | Mapping of annotation class names to list of member declarations, for eventual
        -- declaration of composite classes.
        annotationMap :: M.Map Identifier [AnnMemDecl],

        -- | Map form annotation combinations actually encountered during the program, to
        --   the content types used in the combinations.
        composites :: M.Map (S.Set Identifier) [K3 Type],

        -- | List of triggers declared in a program, used to populate the dispatch table.
        triggers :: [(Identifier, K3 Type)],

        -- | The serialization method to use.
        serializationMethod :: SerializationMethod,

        -- | Used to know if a global is fully applied
        applyLevel :: Int,

        -- | Whether to optimize const refs
        optRefs :: Bool,

        -- | Whether to optimize moves
        optMoves :: Bool


    } deriving Show

-- | The default code generation state.
defaultCPPGenS :: CPPGenS
defaultCPPGenS = CPPGenS 0 [] [] [] [] [] [] [] [] M.empty M.empty M.empty [] BoostSerialization 0 False False

refreshCPPGenS :: CPPGenM ()
refreshCPPGenS = do
    gs <- globals <$> get
    rs <- patchables <$> get
    put defaultCPPGenS { globals = gs, patchables = rs }

-- | Generate a new unique symbol, required for temporary reification.
genSym :: CPPGenM Identifier
genSym = do
    current <- uuid <$> get
    modify (\s -> s { uuid = succ (uuid s) })
    return $ "__" ++ show current

addForward :: R.Declaration -> CPPGenM ()
addForward r = modify (\s -> s { forwards = r : forwards s })

addInitialization :: [R.Statement] -> CPPGenM ()
addInitialization ss = modify (\s -> s { initializations = initializations s ++ ss })

addGlobalInitialization :: [R.Statement] -> CPPGenM ()
addGlobalInitialization ss = modify (\s -> s { globalInitializations = globalInitializations s ++ ss })

addStaticInitialization :: [R.Statement] -> CPPGenM ()
addStaticInitialization ss = modify (\s -> s { staticInitializations = staticInitializations s ++ ss })

addStaticDeclaration :: [R.Statement] -> CPPGenM ()
addStaticDeclaration ss = modify (\s -> s { staticDeclarations = staticDeclarations s ++ ss })

-- | Add an annotation to the code generation state.
addAnnotation :: Identifier -> [AnnMemDecl] -> CPPGenM ()
addAnnotation i amds = modify (\s -> s { annotationMap = M.insert i amds (annotationMap s) })

-- | Add a new composite specification to the code generation state.
addComposite :: [Identifier] -> K3 Type -> CPPGenM ()
addComposite is t = modify (\s -> s { composites = addC (composites s) })
  where addC c = M.insertWith (\a b -> nub $ a ++ b) (S.fromList is) [stripTUIDSpan t] c

-- | Add a new record specification to the code generation state.
addRecord :: Identifier -> [(Identifier, K3 Type)] -> CPPGenM ()
addRecord i its = modify (\s -> s { recordMap = M.insert i its (recordMap s) })

data SerializationMethod
    = BoostSerialization
  deriving (Eq, Read, Show)

incApplyLevel :: CPPGenM ()
incApplyLevel = modify (\env -> env {applyLevel = applyLevel env + 1})

resetApplyLevel :: CPPGenM ()
resetApplyLevel = modify (\env -> env {applyLevel = 0})
