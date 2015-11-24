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
  void          load_kernel(const string_impl& modname,
                            const string_impl& funname,
                            CUfunction kfunc,
                            bool  is_file = 0
                           );
  
  /*! \brief  run a kernel func with args. 
   *  \param  dev  The device id user gave.
   *  \param  fun  The cufunction object
   *  \param  args The arg array
   *  \param  sync The boolean that tells whether we synchronize here.
   */ 
  int           run_kernel(CUfunction   fun,
                           CUdevice*    device,
                           void**       args,
                           bool         sync = true,
                           uint         gridx = 1,
                           uint         gridy = 1,
                           uint         gridz = 1,
                           uint         blockx = 256,
                           uint         blocky = 1,
                           uint         blockz = 1
                          ); 

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
    std::cout << "transfer to device succeed" << std::endl;
    return 0;
  }

  template <typename T>
  int transfer_to_host(T* hdata, CUdeviceptr ddata, size_t buffersize)
  {
    CUDA_SAFE_CALL(cuMemcpyDtoH(hdata, ddata, buffersize));
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
  template <template<class> class R, typename I, typename O>
  Vector<R<I>>       transformer_gpu(Vector<R<I>>& in, 
                                     const string_impl& modname, 
                                     const string_impl& funname,
                                     const string_impl& ptx,
                                     int   dev) {
    if(dev >= _impl->get_dev_count())
      throw "invalid dev id";

    CUdevice    cdev;
    CUdeviceptr cptr;
    CUdeviceptr dres;

    CUcontext   cont;
    CUmodule    module;

    CUDA_SAFE_CALL(cuDeviceGet(&cdev, dev));
    CUDA_SAFE_CALL(cuCtxCreate(&cont, dev, cdev));
    
    size_t size = in.getConstContainer().size();
    O* res = new O[size];
    CUDA_SAFE_CALL(cuMemAlloc(&dres, size*sizeof(O)));
 
    cuMemAlloc(&cptr, size * sizeof(I));
    cuMemcpyHtoD(cptr, &in.getConstContainer()[0], size * sizeof(I));    
    
    void* param[] = { &cptr, &dres, &size };
    CUfunction fun;
    CUDA_SAFE_CALL(cuModuleLoadDataEx(&module, ptx.c_str(), 0, 0, 0));
    CUDA_SAFE_CALL(cuModuleGetFunction(&fun, module, funname.c_str()));
    _impl->run_kernel(fun, &cdev, param, true, 256, 1, 1, 256, 1, 1);    
    
    if(_impl->transfer_to_host(res, dres, sizeof(O) * size) == -1) {
      CUDA_SAFE_CALL(cuMemFree(cptr));    
      throw "transfer data back to host failed.";
    }
    
    CUDA_SAFE_CALL(cuMemFree(cptr));
    CUDA_SAFE_CALL(cuModuleUnload(module)); 
    CUDA_SAFE_CALL(cuCtxDestroy(cont));  
    Vector<R<O>> result;
    for(int i = 0; i < size; i++)
      result.insert(res[i]);

    delete[] res;
    return result;
  }

  template <template<class> class R>
  Vector<R<int>>     transformer_gpu_int(Vector<R<int>>& in,
                                         const string_impl& modname,
                                         const string_impl& funname,
                                         const string_impl& ptx,
                                         int   dev) {
    return this->transformer_gpu<R, int, int>(in, modname, funname, ptx, dev);
  }

  template <template<class> class R>
  Vector<R<float>>   transformer_gpu_real(Vector<R<double>>& in,
                                          const string_impl& modname,
                                          const string_impl& funname,
                                          const string_impl& ptx,
                                          int   dev) {
    return this->transformer_gpu<R, double, double>(in, modname, funname, ptx, dev);
  }
  
  /* Builtins    */
private:
  std::shared_ptr<GPUimpl> _impl;
};
} // namespace K3

#endif
