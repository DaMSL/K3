{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

-- | Data types for the K3 Interpreter

module Language.K3.Interpreter.Data.Types where

import Control.Concurrent.MVar
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Map  ( Map   )
import Data.Word ( Word8 )
import qualified Data.HashTable.IO as HT

import GHC.Generics
import System.Mem.StableName

import Language.K3.Core.Common
import Language.K3.Runtime.Engine
import Language.K3.Runtime.FileDataspace

import Language.K3.Utils.Pretty(PrintConfig)

-- | K3 Values
--   Values are pure, allowing their definition as Eq and Ord instances.
--
--   Certain K3 values use the concept of an entity tag, which introduces instantiation
--   and lifetimes for values. We define physical equality as equivalent entities, and
--   logical equality as structurally equal values.
--   K3 indirections, functions and triggers support phyiscal equality only, since we cannot
--   inspect their structures in pure fashion, while all other kind of values employ structural
--   equality.
--
--   Collections include a self pointer, as well as a pure representation of their fields.
--   This ensures collection values are referentially transparent, while also allowing
--   aliased access to fields through the self keyword.
--   The contents of the self pointer and explicit fields must be kept synchronized by
--   method contextualization primitives for mixed self and non-self accesses.
--
--   Note, due to the dependency on the Interpretation monad, values
--   cannot be separated from the monad definition.
--
data Value
    = VBool        Bool
    | VByte        Word8
    | VInt         Int
    | VReal        Double
    | VString      String
    | VAddress     Address
    | VOption      (Maybe Value, VQualifier)
    | VTuple       [(Value, VQualifier)]
    | VRecord      (NamedMembers Value)
    | VIndirection (IIndirection, VQualifier, EntityTag)
    | VCollection  (IIndirection, Collection Value)
      -- Collections have both a mutable indirection version which is used where structural
      -- references are essential (e.g. self), and a cached version that is refreshed
      -- periodically. The cached version works well with Haskell's built-in typeclasses.
    | VFunction    (IFunction, Closure Value, EntityTag)
    | VTrigger     (Identifier, Maybe IFunction, EntityTag)

-- | Runtime qualifier values. Used to construct environment entries.
data VQualifier = MemImmut | MemMut deriving (Eq, Generic, Ord, Read, Show)

-- | Entity tags for values.
--   In-memory values use stable names, while external values use an integer
--   corresponding to their file offset.
data EntityTag
  = forall a . MemEntTag (StableName a)
  | ExtEntTag Int

-- | A datastructure for named bindings
type NamedBindings v = Map Identifier v

-- | Type synonym for interepreter indirections
type IIndirection = MVar Value

-- | Type synonym for interpreted lambdas.
type IFunction = Value -> Interpretation Value

-- | Function closures that capture free variable bindings.
type Closure v = IEnvironment v


{- Interpreter dataspaces -}

newtype ListMDS         v = ListMDS         [v]
newtype SetAsOrdListMDS v = SetAsOrdListMDS [v]
newtype BagAsOrdListMDS v = BagAsOrdListMDS [v]

data PrimitiveMDS v
    = MemDS    (ListMDS v)
    | SeqDS    (ListMDS v)
    | SetDS    (SetAsOrdListMDS v)
    | SortedDS (BagAsOrdListMDS v)

data CollectionDataspace v
    = InMemoryDS [v]
    | InMemDS    (PrimitiveMDS v)
    | ExternalDS (FileDataspace v)


{- Collections and annotations -}

-- | Collection members, which must be pure values to support Eq and Ord
--   instances for collection values (e.g., for comparison on mutable members).
type NamedMembers v  = NamedBindings (v, VQualifier)

-- | Collection implementation.
--   The namespace contains lifted members, the dataspace contains final
--   records, and the realization identifier is the instance's annotation combination name.
data Collection v = Collection { namespace     :: CollectionNamespace v
                               , dataspace     :: CollectionDataspace v
                               , realizationId :: Identifier }

-- | Two-level namespacing of collection constituents. Collections have two levels of named values:
--   i. global names, comprised of unambiguous annotation member names.
--   ii. annotation-specific names, comprised of overlapping named annotation members.
--
-- The annotation namespace is kept as an association list, where the list order
-- indicates the resolution order of member names.
--
-- For now, we assume names are unambiguous and keep everything as a global name.
-- TODO: extend type system to use two-level namespaces.
--
-- Namespaces are implemented as named bindings of values and member qualifiers.
-- Thus, namespaces are referentially transparent and provide copy semantics when
-- used in interpretation environment modification. However, as a result, we need
-- to create proxy values for mutable fields during method contextualization.
--
data CollectionNamespace v =
        CollectionNamespace { collectionNS :: NamedMembers v
                            , annotationNS :: [(Identifier, NamedMembers v)] }

data CollectionConstructors v =
  CollectionConstructors { emptyCtor   :: CEmptyConstructor v
                         , initialCtor :: CInitialConstructor v
                         , copyCtor    :: CCopyConstructor v
                         , emplaceCtor :: CEmplaceConstructor v }

-- | A collection initializer that populates default lifted attributes.
type CEmptyConstructor v = () -> Interpretation Value

-- | A collection initializer that takes a list of values and builds
--   a collection populated with those values.
type CInitialConstructor v = [v] -> Interpretation Value

-- | An emplacing constructor that takes a dataspace, and injects it into a new collection.
type CEmplaceConstructor v = CollectionDataspace v -> Interpretation Value

-- | A copy constructor that takes a collection, copies its mutable fields,
--   and rebinds its member functions to lift/lower bindings to/from the new collection.
type CCopyConstructor v = Collection v -> Interpretation Value

-- | Annotation environment, for lifted attributes. This contains two mappings:
--  i. annotation ids => lifted attribute ids, lifted attribute value
--  ii. combined annotation ids => combination constructors
--
-- There are four types of annotation combination constructors (see above), each of
-- which returns a collection value (with the self pointer initialized), for varying
-- arguments that define the collection's contents.
--
data AEnvironment v =
  AEnvironment { definitions  :: AnnotationDefinitions v
               , realizations :: AnnotationCombinations v }

type AnnotationDefinitions v  = [(Identifier, NamedMembers v)]
type AnnotationCombinations v = [(Identifier, CollectionConstructors v)]

-- | An environment for rebuilding static values after their serialization.
--   The static value can either be a value (e.g., a global function) or a
--   collection namespace associated with an annotation combination id.
type SEnvironment v = (IEnvironment v, AEnvironment v)


-- | Environment entries.
--   These distinguish immutable and mutable values, enabling sharing of the latter kind
--   at the environment level, rather than at the value level.
data IEnvEntry v
  = IVal v
  | MVal (MVar v)

-- | Interpretation Environment.
--   This is a hashtable of names to environment entries.
type IEnvironment v = HT.CuckooHashTable Identifier [IEnvEntry v]

-- | Proxy values and paths, for alias synchronization.
data ProxyStep
    = Named             Identifier
    | Temporary         Identifier
    | Dataspace         (Identifier, EntityTag)
    | ProxySelf
    | Dereference
    | MatchOption
    | TupleField        Int
    | RecordField       Identifier
    | CollectionMember  Identifier

type ProxyPath       = [ProxyStep]
type ProxyPathStack  = [Maybe ProxyPath]


-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IState (WriterT ILog IEngineM))

-- | Errors encountered during interpretation.
data InterpretationError
    = RunTimeInterpretationError String (Maybe (Span, UID))
    | RunTimeTypeError String (Maybe (Span, UID))
  deriving (Eq, Read, Show)

-- | Type synonym for interpreter engine and engine monad
type IEngine  = Engine  Value
type IEngineM = EngineM Value

-- | Interpretation event log.
type ILog = [String]

-- | Identifiers for global declarations
type Globals = [Identifier]

-- | Interpreter tracing and debugging
data ITracer = ITracer { stackTrace   :: [(Span, UID)]
                       , watchedExprs :: [UID]
                       , watchedVars  :: [(UID, [Identifier])] }
                deriving (Eq, Read, Show)

-- | Type declaration for an Interpretation's state.
data IState = IState { getGlobals     :: Globals
                     , getEnv         :: IEnvironment Value
                     , getAnnotEnv    :: AEnvironment Value
                     , getStaticEnv   :: SEnvironment Value
                     , getProxyStack  :: ProxyPathStack
                     , getTracer      :: ITracer
                     , getPrintConfig :: PrintConfig}

-- | An evaluated value type, produced from running an interpretation.
type IResult a = ((Either InterpretationError a, IState), ILog)

-- | Pairing of errors and environments for debugging output.
type EnvOnError = (InterpretationError, IEnvironment Value)

