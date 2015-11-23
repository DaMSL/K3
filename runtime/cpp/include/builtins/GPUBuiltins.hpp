#ifndef K3_GPUBUILTINS
#define K3_GPUBUILTINS

#include "cuda.h"
#include "nvrtc.h"

#include "types/BaseString.hpp"
#include "Common.hpp"

namespace K3 {

class GPUBuiltins {

public:
  GPUBuiltins();

  /* CUDA Driver API wrapper */
  int          get_device_count(unit_t);
  unit_t       device_info(unit_t);

  /* Runtime compilation */
  string_impl  compile_to_ptx_str(const string_impl& code, const string_impl& ptxname);
  int          compile_to_ptx_file(const string_impl& code, const string_impl& fname);
  
  /* Run kernels */
  
  /* Builtins    */
private:
  class pImpl;
  std::shared_ptr<pImpl> _impl;
};
} // namespace K3

#endif
