#ifndef K3_GPUBUILTINS
#define K3_GPUBUILTINS

#include "cuda.h"
#include "nvrtc.h"

#include "types/BaseString.hpp"
#include "Common.hpp"

#include "collections/STLDataspace.hpp"
#include "collections/Collection.hpp"
#include "collections/Vector.hpp"

//TODO: replace this C Macro to c++ func.
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

struct device_info {
  int    max_thread_per_block;
  int    max_block_dim_x;
  int    max_block_dim_y;
  int    max_block_dim_z;
  int    max_grid_dim_x;
  int    max_grid_dim_y;
  int    max_grid_dim_z;
  int    max_shm_per_block;
  int    warp_size;
  int    cc_major;
  int    cc_minor;
  size_t tot_mem;
};

// Alias
using dev_info      = struct device_info;
using attribute_vec = std::vector<dev_info>;

class GPUimpl {
public:
  GPUimpl();
  ~GPUimpl();
  int          get_dev_count();
  void         dev_info();
  void         compile_ptx(const char* src,
                           const char* name,
                           std::string& ptxstr,
                           int nh = 0,
                           const char** headers = NULL,
                           const char** includeNames = NULL,
                           int dev = 0,
                           bool materialize = 0
                          );
  
  /*! \brief  Load a kernel function from a precompiled ptx file or 
   *          runtime-compile ptx string
   *  \param  modname The name of the module file/string
   *  \param  funname The name of the function to load
   *  \param  is_file Boolean value, from a precompiled file or rt-compiled string
   *  \param  kfunc   The CUfunction that loads.
   */
  void          load_kernel(const std::string& modname,
                            const std::string& funname,
                            CUfunction* kfunc,
                            bool  is_file = 0
                           );
  
  /*! \brief  run a kernel func with args. 
   *  \param  dev  The device id user gave.
   *  \param  fun  The cufunction object
   *  \param  args The arg array
   *  \param  sync The boolean that tells whether we synchronize here.
   */ 
  int           run_kernel(CUfunction*  fun,
                           CUcontext*   context,
                           CUdevice*    device,
                           void**       args,
                           bool         sync = true,
                           uint         gridx = 1,
                           uint         gridy = 1,
                           uint         gridz = 1,
                           uint         blockx = 1,
                           uint         blocky = 1,
                           uint         blockz = 1
                          ) 

  void          cleanup(CUcontext* context);

  // Template functions 
  template <typename T>
  int transfer_to_device(T* hdata, CUdeviceptr ddata, size_t buffersize)
  {
    if(cuMemAlloc(&ddata, buffersize) != CUDA_SUCCESS)
      return -1;
    if (cuMemcpyHtoD(ddata, hdata, buffersize) != CUDA_SUCCESS) {
      CUDA_SAFE_CALL(cuMemFree(ddata));
      return -1;
    }
    return 0;
  }

  template <typename T>
  int transfer_to_host(T* hdata, CUdeviceptr ddata, size_t buffersize)
  {
    if (!hdata || cuMemcpyDtoH(hdata, ddata, buffersize) != CUDA_SUCCESS)
      return -1;
    CUDA_SAFE_CALL(cuMemFree(ddata));
    return 0;
  }

private:
  /* device parameters */
  int              _dcount;
  attribute_vec    _dattributes;
  int              _version;  
};


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
  template <class T>
  T            transformer_gpu(T in, const string_impl& modname, 
                                     const string_impl& funname,
                                     const string_impl& ptx,
                                     int   dev) {
    if(dev >= _impl->_dcount)
    throw "invalid dev id";

    CUdevice    cdev;
    CUdeviceptr cptr;
    CUcontext   cont;

    CUDA_SAFE_CALL(cuDeviceGet(&cdev, dev));
    CUDA_SAFE_CALL(cuCtxCreate(&cont, dev, cdev));
    std::vector<int> result;

    // We throw exceptions at temp.
    if(_impl->transfer_to_device(&in.getConstContainer()[0], cptr, sizeof(int) * in.size()) == -1)
      throw "transfer data to device failed.";
    // Launching kernel in between.
    if(_impl->transfer_to_host(&result[0], cptr, sizeof(int) * in.size()) == -1)
      throw "transfer data back to host failed.";
    return T(result);
  }
  
  /* Builtins    */
private:
  std::shared_ptr<GPUimpl> _impl;
};
} // namespace K3

#endif
