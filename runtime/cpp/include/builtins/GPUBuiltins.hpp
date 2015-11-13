#ifndef K3_GPUBUILTINS
#define K3_GPUBUILTINS

#include "cuda.h"
#include "nvrtc.h"

#include "types/BaseString.hpp"
#include "Common.hpp"

namespace K3 {

class GPUBuiltins {

public:
  GPUBuiltins(){
    /* initialize CUDA driver API here */
    if(CUDA_SUCCESS != cuInit(0))
      throw "CUDA Driver cannot be initialized.";
  }

  /* CUDA Driver API wrapper */
  int get_device_count(unit_t) { 
    int *count;
    if (cuDeviceGetCount(count) == CUDA_SUCCESS)
      return *count;
    else
      return -1;
  }

  unit_t compile_to_ptx(const string_impl& code, const string_impl& ptxname) {
    
    nvrtcProgram prog;              /* a program handle */
    nvrtcResult  res;               /* an enumeration of result */

    /* ... Runtime Compilation using NVRTC library .... */
    return unit_t{};
  }

};
} // namespace K3
 
#endif
