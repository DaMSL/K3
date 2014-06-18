-- This module provides the machinery necessary to generate C++ code from K3 programs. The resulting
-- code can be compiled using a C++ compiler and linked against the K3 runtime library to produce a
-- binary.
module Language.K3.Codegen.CPP (
    module Language.K3.Codegen.CPP.Types,

    program
) where

import Language.K3.Codegen.CPP.Types
import Language.K3.Codegen.CPP.Program (program)
