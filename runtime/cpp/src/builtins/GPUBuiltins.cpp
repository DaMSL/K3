#include "cuda.h"
#include "nvrtc.h"
#include <iostream>

#include "types/BaseString.hpp"
#include "Common.hpp"
#include "builtins/GPUBuiltins.hpp"

#define FETCH(loc, attr) \
  CUDA_SAFE_CALL(cuDeviceGetAttribute((loc), (attr), device));

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
 
  ~pImpl() {
    _dattributes.clear();
  }

  int  get_dev_count() {
    return _dcount;
  }

  /* summary */
  void dev_info() {
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
  
  // Return compiler options for compute capability
  String& get_compile_cc_opts(int dev) {
    std::string cc("--gpu-architecture=compute_");
    switch (_dattributes[dev].cc_major) {
      case 2:
        return cc.append("20");
      case 3:
        if(_dattributes[dev].cc_minor < 5)
          return cc.append("30");
        else
          return cc.append("35");
      case 5:
        return cc.append("50");
      default:
        return cc.append("20");
    }
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
GPUBuiltins::compile_to_ptx(const string_impl& code, const string_impl& ptxname) {

		nvrtcProgram prog;              /* a program handle */
		nvrtcResult  result;               /* an enumeration of result */

		/* ... Runtime Compilation using NVRTC library .... */
		//const char *kernel = code.c_str();
		//ptxname(""kernel.cu");

		const char *kernel = "                                                  \n\
				extern \"C\" __global__                                         \n\
				void saxpy(float a, float *x, float *y, float *out, size_t n)   \n\
				{                                                               \n\
				  size_t tid = blockIdx.x * blockDim.x + threadIdx.x;           \n\
				  if (tid < n) {                                                \n\
					out[tid] = a * x[tid] + y[tid];                             \n\
				  }                                                             \n\
				}                                                               \n";

		//create program:
		result =
		   nvrtcCreateProgram(&prog,         // prog
				   (string_impl("extern \"C\" ") + code).c_str() ,         // buffer
				   ptxname.c_str(),    // name         // buffer
					0,             // numHeaders
					NULL,          // headers
					NULL);        // includeNames
		if (result !=  NVRTC_SUCCESS){
			throw "nvtrcCreateProgram failed";
		}

		//compile program:
		const char *opts[] = {"--gpu-architecture=compute_20",
							  "--fmad=false"};
		result = nvrtcCompileProgram(prog,  // prog
										2,     // numOptions
									 opts); // options
		if (result != NVRTC_SUCCESS) {
			throw "nvrtcCompileProgram failed";
		}

		// Obtain PTX from the program
		size_t ptxSize;
		result = nvrtcGetPTXSize(prog, &ptxSize);
		if (result != NVRTC_SUCCESS) {
			throw "fail to get PTX size";
		}
		char *ptx = new char[ptxSize];
		if (nvrtcGetPTX(prog, ptx) !=  NVRTC_SUCCESS){
			throw "fail to get PTX";
		}

		if (nvrtcDestroyProgram(&prog) != NVRTC_SUCCESS){
			throw "fail to destroy program ";
		}

		return string_impl(ptx);
		//return unit_t{};
	  }

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

}
