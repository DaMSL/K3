-- | Data types for the K3 Interpreter

module Language.K3.Interpreter.Data.Types where

import Control.Concurrent.MVar
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Data.Map  ( Map   )
import Data.Trie ( Trie  )
import Data.Word ( Word8 )
import qualified Data.Map          as Map
import qualified Data.Hashtable.IO as HT 
import qualified Data.Trie         as Trie

import System.Mem.StableName
import System.Mem.Weak

import Language.K3.Core.Common
import Language.K3.Runtime.Engine
import Language.K3.Runtime.FileDataspace

-- | K3 Values
--   Note, due to the dependency on the Interpretation monad, values
--   cannot be separated from the monad definition.
data Value
    = VBool        Bool
    | VByte        Word8
    | VInt         Int
    | VReal        Double
    | VString      String
    | VAddress     Address
    | VOption      (Maybe Value)
    | VTuple       [Value]
    | VRecord      (NamedBindings Value)
    | VCollection  (IIndirection, Collection Value)
    | VIndirection (IIndirection, EntityTag)
    | VFunction    (IFunction, Closure Value, EntityTag)
    | VTrigger     (Identifier, Maybe IFunction, EntityTag)

-- | Entity tags for values. 
--   In-memory values use stable names, while external values use an integer
--   corresponding to their file offset.
data EntityTag 
  = forall a . MemEntTag (StableName a)
  | ExtEntTag Int

-- | A datastructure for named bindings
type NamedBindings v = Map Identifier v

-- | Type synonym for interpreted lambdas.
type IFunction = Value -> Interpretation Value

-- | Type synonym for interepreter indirections
type IIndirection = MVar Value

-- | Function closures that capture free variable bindings.
type Closure v = IEnvBindings v


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

data MemberQualifier = MemImmut | MemMut deriving (Eq, Read, Show)
type NamedMembers v  = NamedBindings (v, MemberQualifier)

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
     deriving (Read, Show)

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

-- | A copy constructor that takes a collection, copies its mutable fields,
--   and rebinds its member functions to lift/lower bindings to/from the new collection.
type CCopyConstructor v = Collection v -> Interpretation Value

-- | An emplacing constructor that takes a dataspace, and injects it into a new collection.
type CEmplaceConstructor v = CollectionDataspace v -> Interpretation Value

-- | Annotation environment, for lifted attributes. This contains two mappings:
--  i. annotation ids => lifted attribute ids, lifted attribute value
--  ii. combined annotation ids => combination namespace
-- 
--  The second mapping is used to store concrete annotation combinations used at
--  collection instances (once for all instances), and defines namespaces containing
--  bindings that are introduced to the interpretation environment when invoking members.
data AEnvironment v = 
  AEnvironment { definitions  :: AnnotationDefinitions v
               , realizations :: AnnotationCombinations v }

type AnnotationDefinitions v  = [(Identifier, NamedMembers v)]
type AnnotationCombinations v = [(Identifier, CollectionConstructors v)]

-- | An environment for rebuilding static values after their serialization.
--   The static value can either be a value (e.g., a global function) or a 
--   collection namespace associated with an annotation combination id.
type SEnvironment v = (IEnvironment v, AEnvironment v)


-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IState (WriterT ILog IEngineM))

-- | Errors encountered during interpretation.
data InterpretationError
    = RunTimeInterpretationError String
    | RunTimeTypeError String
  deriving (Eq, Read, Show)

-- | Type synonym for interpreter engine and engine monad
type IEngine  = Engine  Value
type IEngineM = EngineM Value

-- | Interpretation event log.
type ILog = [String]

-- | Identifiers for global declarations
type Globals = [Identifier]

-- | Environment entries.
--   These encapsulate sharing and proxying of mutable values (MVals) and aliases (PVals)
--   at the environment level, rather than at the value level.
data IEnvEntry v
  = IVal (        v, StableName v)
  | MVal (MVar    v, StableName v)
  | PVal (ProxyId v)

type IProxyEntry  v = Weak (ProxyId v, MVar v)
type ProxyId      v = MVar (ProxyLocation v)

{- Proxy value synchronization -}
data ProxyStep
    = Named             Identifier
    | Temporary         Identifier
    | Dataspace         (Identifier, EntityTag)
    | Dereference           
    | TupleField        Int
    | RecordField       Identifier
    | CollectionMember  Identifier
  deriving (Eq, Show, Read)

type ProxyPath       = [ProxyStep]
type ProxyPathStack  = [Maybe ProxyPath]
type ProxyLocation v = (StableName v, ProxyPath)

-- | Interpretation Environment.
--   This is a hashtable of names to environment entries, as well as a proxy value environment.
--
--   Proxy values are bindings captured in closures, and indexed by a proxy identifier.
--   Our proxy environment maps proxy identifiers to weak values, so that they are
--   garbage-collected whenever the last closure that points to the value is destroyed.
--   To achieve this, the proxy identifier acts as the key for Haskell's weak pointers.
--
--   The proxy value environment is a two-level nested map data structure, with the outer tier
--   indexed by the stable name of the proxy's root value, and the inner tier indexed by a
--   binary representation of bind paths. The outer tier data structure is a hashtable, and the
--   inner tier a trie, to support efficient prefix matching from a proxy's root value.
--
type IEnvBindings v = HT.CuckooHashTable Identifier (IEnvEntry v)
type IEnvProxies  v = HT.CuckooHashTable (StableName v) (Trie (IProxyEntry v))

data IEnvironment v = IEnvironment { envBindings :: IEnvBindings v
                                   , envProxies  :: IEnvProxies v }

-- | Type declaration for an Interpretation's state.
data IState = IState { getGlobals    :: Globals
                     , getEnv        :: IEnvironment Value
                     , getAnnotEnv   :: AEnvironment Value
                     , getStaticEnv  :: SEnvironment Value
                     , getBindStack  :: ProxyPathStack }

-- | An evaluated value type, produced from running an interpretation.
type IResult a = ((Either InterpretationError a, IState), ILog)

-- | Pairing of errors and environments for debugging output.
type EnvOnError = (InterpretationError, IEnvironment Value)


-- | Proxy trees, for efficient signalling of aliased values.
class ProxyTree vk v t where
  lookup       :: vk -> t -> v
  pre          :: vk -> t -> [vk]
  succ         :: vk -> t -> [vk]
  ancestors    :: vk -> t -> [vk]
  descendants  :: vk -> t -> [vk]
  rebuild      :: vk -> v -> t -> t

instance ProxyTree (ProxyLocation Value) (IProxyEntry Value) (IEnvProxies Value) where
  pre (s,p) t  = undefined
  succ (s,p) t = undefined

  ancestors k t   = undefined
  descendants k t = undefined
  
  rebuild k v t = undefined

