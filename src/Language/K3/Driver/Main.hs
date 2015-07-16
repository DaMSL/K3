import Control.Monad
import Control.Monad.IO.Class

import qualified Options.Applicative as Options
import Options.Applicative( (<>) )

import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Parser.SQL
import Language.K3.Driver.Driver
import Language.K3.Driver.Options
import Language.K3.Driver.Service

run :: Options -> DriverM ()
run opts = do
  void $ initialize opts
  case mode opts of
    Service (RunMaster sOpts smOpts)   -> liftIO $ runServiceMaster sOpts smOpts opts
    Service (RunWorker sOpts)          -> liftIO $ runServiceWorker sOpts
    Service (SubmitJob sOpts rjOpts)   -> liftIO $ submitJob        sOpts rjOpts opts
    Service (QueryService sOpts qOpts) -> liftIO $ queryService     sOpts qOpts
    Service (Shutdown  sOpts)          -> liftIO $ shutdownService  sOpts
    Compile copts | ccStage copts == Stage2 -> compile opts copts (DC.role "__global" [])
    SQL -> liftIO $ k3ofsql $ inputProgram $ input opts
    _ -> k3in opts >>= metaprogram opts >>= dispatch opts

-- | Top-Level.
main :: IO ()
main = Options.execParser options >>= runDriverM_ . run
  where
    options = Options.info (Options.helper <*> programOptions) $ Options.fullDesc
                <> Options.progDesc "The K3 Compiler."
                <> Options.header "The K3 Compiler."
