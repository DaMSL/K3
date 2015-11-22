#ifndef K3_GPUBUILTINS
#define K3_GPUBUILTINS

#include "cuda.h"
#include "nvrtc.h"

#include "types/BaseString.hpp"
#include "Common.hpp"

#define NUM_THREADS 128
#define NUM_BLOCKS 32
#define NVRTC_SAFE_CALL(x)                                        \
  do {                                                            \
    nvrtcResult result = x;                                       \
    if (result != NVRTC_SUCCESS) {                                \
      std::cerr << "\nerror: " #x " failed with error "           \
                << nvrtcGetErrorString(result) << '\n';           \
      exit(1);                                                    \
    }                                                             \
  } while(0)
#define CUDA_SAFE_CALL(x)                                         \
  do {                                                            \
    CUresult result = x;                                          \
    if (result != CUDA_SUCCESS) {                                 \
      const char *msg;                                            \
      cuGetErrorName(result, &msg);                               \
      std::cerr << "\nerror: " #x " failed with error "           \
                << msg << '\n';                                   \
      exit(1);                                                    \
    }                                                             \
  } while(0)

namespace K3 {

class GPUBuiltins {

public:
  GPUBuiltins();

  /* CUDA Driver API wrapper */
  int          get_device_count(unit_t);
  unit_t       device_info(unit_t);

  string_impl  compile_to_ptx(const string_impl& code, const string_impl& ptxname) ;
  unit_t       run_ptx(const string_impl ptx, const string_impl func_name);

private:
  class pImpl;
  std::shared_ptr<pImpl> _impl;
};
} // namespace K3

#endif
