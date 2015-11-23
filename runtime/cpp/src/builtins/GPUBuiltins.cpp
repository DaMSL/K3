#include "cuda.h"
#include "nvrtc.h"
#include <iostream>

#include "types/BaseString.hpp"
#include "Common.hpp"
#include "builtins/GPUBuiltins.hpp"

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

#define FETCH(loc, attr) \
  CUDA_SAFE_CALL(cuDeviceGetAttribute((loc), (attr), device));
#define NUMOPTIONS 5

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

using dev_info      = struct device_info;
using attribute_vec = std::vector<dev_info>;

class GPUBuiltins::pImpl {
public:
  pImpl() {
    CUDA_SAFE_CALL(cuInit(0));
    CUDA_SAFE_CALL(cuDeviceGetCount(&_dcount));
    CUDA_SAFE_CALL(cuDriverGetVersion(&_version));
    _dattributes.resize(_dcount);    

    CUdevice device;
    for(int i = 0; i < _dcount; i++) {
      CUDA_SAFE_CALL(cuDeviceGet(&device, i));
      FETCH(&(_dattributes[i].max_thread_per_block), CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK)
      FETCH(&(_dattributes[i].max_block_dim_x),      CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_X)
      FETCH(&(_dattributes[i].max_block_dim_y),      CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Y)
      FETCH(&(_dattributes[i].max_block_dim_z),      CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Z)
      FETCH(&(_dattributes[i].max_grid_dim_x),       CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_X)
      FETCH(&(_dattributes[i].max_grid_dim_y),       CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Y)
      FETCH(&(_dattributes[i].max_grid_dim_z),       CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Z)
      FETCH(&(_dattributes[i].max_shm_per_block),    CU_DEVICE_ATTRIBUTE_MAX_SHARED_MEMORY_PER_BLOCK)
      FETCH(&(_dattributes[i].warp_size),            CU_DEVICE_ATTRIBUTE_WARP_SIZE)
      FETCH(&(_dattributes[i].cc_major),             CU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MAJOR)
      FETCH(&(_dattributes[i].cc_minor),             CU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MINOR)
      CUDA_SAFE_CALL(cuDeviceTotalMem(&(_dattributes[i].tot_mem), device));
    }
  }
 
  ~pImpl() 
  {
    _dattributes.clear();
  }

  int  get_dev_count() 
  {
    return _dcount;
  }

  /* summary */
  void dev_info() 
  {
    // TODO: all prints
    std::cout << "=============================================" << "\n";
    std::cout << "CUDA Driver Version: " << _version << "\n";
    std::cout << "Total Device Number: " << _dcount   << "\n";
    std::cout << "Dev Id    " << "CC Major    " << "CC Minor    " << "\n";
    for(int i = 0; i < _dcount; i++) {
      std::cout << i << "    " << _dattributes[i].cc_major
                     << "    " << _dattributes[i].cc_minor << "\n";
    }
    std::cout << "=============================================" << std::endl;
  }
 
  void compile_ptx(const char* src, 
                   const char* name,
                   std::string& ptxstr,
                   int nh = 0, 
                   const char** headers = NULL,
                   const char** includeNames = NULL,
                   int dev = 0,
                   bool materialize = 0
                   ) 
  {
   
    nvrtcProgram prog;

    // Build CC option
    std::string cc("--gpu-architecture=compute_");
    switch (_dattributes[dev].cc_major) {
      case 2:
        cc.append("20"); break;
      case 3:
        if(_dattributes[dev].cc_minor < 5)
          cc.append("30");
        else
          cc.append("35");
        break;
      case 5:
        cc.append("50"); break;
      default:
        cc.append("20"); break;
    }
 
    const char* opts[NUMOPTIONS]; 
    opts[0] = cc.c_str();
    opts[1] = "--std=c++11";
    opts[2] = "--builtin-move-forward=true";
    opts[3] = "--builtin-initializer-list=true";
    opts[4] = "--device-c";
      
    NVRTC_SAFE_CALL(nvrtcCreateProgram(&prog, src, name, nh, headers, includeNames));
    NVRTC_SAFE_CALL(nvrtcCompileProgram(prog, 5, opts));
    
    size_t ptxsize;
    NVRTC_SAFE_CALL(nvrtcGetPTXSize(prog, &ptxsize));
    char* ptx = new char[ptxsize]; 
    NVRTC_SAFE_CALL(nvrtcGetPTX(prog, ptx));
    NVRTC_SAFE_CALL(nvrtcDestroyProgram(&prog));

    // return value
    if(materialize) {
    } else {
      if (ptxstr.empty())
        ptxstr.append(ptx);
      else
        throw "Invalid String Reference: Not Empty";
    }
  }
  
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

  /*! \brief  Load a kernel function from a precompiled ptx file or 
   *          runtime-compile ptx string
   *  \param  modname The name of the module file/string
   *  \param  funname The name of the function to load
   *  \param  is_file Boolean value, from a precompiled file or rt-compiled string
   *  \param  kfunc   The CUfunction that loads.
   */
  void load_kernel(const std::string& modname,
                   const std::string& funname,
                   CUfunction* kfunc,
                   bool  is_file = 0
                   ) 
  {
    if (modname.empty() || funname.empty())
      return;

    CUmodule   module;
    CUfunction kernel;

    if (is_file)
      CUDA_SAFE_CALL(cuModuleLoad(&module, modname.c_str()));
    else
      // TODO: We set jit options to 0 temporarily
      CUDA_SAFE_CALL(cuModuleLoadDataEx(&module, modname.c_str(), 0, 0, 0));
    CUDA_SAFE_CALL(cuModuleGetFunction(&kernel, module, funname.c_str()));
    CUDA_SAFE_CALL(cuModuleUnload(module));
  }
  
  /*! \brief  run a kernel func with args. 
   *  \param  dev  The device id user gave.
   *  \param  fun  The cufunction object
   *  \param  args The arg array
   *  \param  sync The boolean that tells whether we synchronize here.
   */
  int run_kernel(CUfunction*  fun,
                 CUcontext*   context,
                 void**       args,
                 bool         sync = true,
                 int          dev = 0,
                 uint         gridx = 1,
                 uint         gridy = 1,
                 uint         gridz = 1,
                 uint         blockx = 1,
                 uint         blocky = 1,
                 uint         blockz = 1
                 ) 
  {
    if (dev >= _dcount || !fun)
      return -1;

    //TODO: Sanity checks on grid block dimensions

    CUdevice    cuDevice;
    CUDA_SAFE_CALL(cuDeviceGet(&cuDevice, dev));
    CUDA_SAFE_CALL(cuCtxCreate(context, dev, cuDevice));
    
    CUDA_SAFE_CALL(cuLaunchKernel(*fun,
                                  gridx, gridy, gridz,
                                  blockx, blocky, blockz, 
                                  0, NULL, 
                                  args, 0));
    if(sync)
      CUDA_SAFE_CALL(cuCtxSynchronize());
    return 0;
  }

  void cleanup(CUcontext* context)
  {
    if(context)
      CUDA_SAFE_CALL(cuCtxDestroy(*context));
  }
  
  /* device parameters */
  int              _dcount;
  attribute_vec    _dattributes;
  int              _version;
};

GPUBuiltins::GPUBuiltins()
: _impl(new pImpl()) {}

int 
GPUBuiltins::get_device_count(unit_t) {
  return _impl->get_dev_count();
}

unit_t 
GPUBuiltins::device_info(unit_t) {
  _impl->dev_info();
  return unit_t{};
}

string_impl  
GPUBuiltins::compile_to_ptx_str(const string_impl& code, 
                                const string_impl& ptxname){
  std::string ptx;
  _impl->compile_ptx(code.c_str(), code.c_str(), ptx);
  return string_impl(ptx);
}
  
int
GPUBuiltins::compile_to_ptx_file(const string_impl& code, 
                                 const string_impl& fname){
  return 0;
}

/*
unit_t 
GPUBuiltins::run_ptx(const string_impl ptx, const string_impl func_name){
		// Load the generated PTX and get a handle to the SAXPY kernel.
		  CUdevice cuDevice;
		  CUcontext context;
		  CUmodule module;
		  CUfunction kernel;
		  CUDA_SAFE_CALL(cuInit(0));
		  CUDA_SAFE_CALL(cuDeviceGet(&cuDevice, 0));
		  CUDA_SAFE_CALL(cuCtxCreate(&context, 0, cuDevice));
		  CUDA_SAFE_CALL(cuModuleLoadDataEx(&module, ptx.c_str(), 0, 0, 0));
		  CUDA_SAFE_CALL(cuModuleGetFunction(&kernel, module, func_name.c_str()));
		  // Generate input for execution, and create output buffers.
		  size_t n = NUM_THREADS * NUM_BLOCKS;
		  size_t bufferSize = n * sizeof(float);
		  float a = 5.1f;
		  float *hX = new float[n], *hY = new float[n], *hOut = new float[n];
		  for (size_t i = 0; i < n; ++i) {
		    hX[i] = static_cast<float>(i);
		    hY[i] = static_cast<float>(i * 2);
		  }
		  CUdeviceptr dX, dY, dOut;
		  CUDA_SAFE_CALL(cuMemAlloc(&dX, bufferSize));
		  CUDA_SAFE_CALL(cuMemAlloc(&dY, bufferSize));
		  CUDA_SAFE_CALL(cuMemAlloc(&dOut, bufferSize));
		  CUDA_SAFE_CALL(cuMemcpyHtoD(dX, hX, bufferSize));
		  CUDA_SAFE_CALL(cuMemcpyHtoD(dY, hY, bufferSize));
		  // Execute SAXPY.
		  void *args[] = { &a, &dX, &dY, &dOut, &n };
		  CUDA_SAFE_CALL(
		    cuLaunchKernel(kernel,
		                   NUM_THREADS, 1, 1,   // grid dim
		                   NUM_BLOCKS, 1, 1,    // block dim
		                   0, NULL,             // shared mem and stream
		                   args, 0));           // arguments
		  CUDA_SAFE_CALL(cuCtxSynchronize());
		  // Retrieve and print output.
		  CUDA_SAFE_CALL(cuMemcpyDtoH(hOut, dOut, bufferSize));
		  for (size_t i = 0; i < n; ++i) {
		    std::cout << a << " * " << hX[i] << " + " << hY[i]
		              << " = " << hOut[i] << '\n';
		  }
		  // Release resources.
		  CUDA_SAFE_CALL(cuMemFree(dX));
		  CUDA_SAFE_CALL(cuMemFree(dY));
		  CUDA_SAFE_CALL(cuMemFree(dOut));
		  CUDA_SAFE_CALL(cuModuleUnload(module));
		  CUDA_SAFE_CALL(cuCtxDestroy(context));
		  delete[] hX;
		  delete[] hY;
		  delete[] hOut;

		return unit_t{};
	}
*/
} // namespace K3
