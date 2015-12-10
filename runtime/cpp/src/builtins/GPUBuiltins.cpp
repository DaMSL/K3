#include "cuda.h"
#include "nvrtc.h"
#include <iostream>
#include <regex>
#include <fstream>
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

#define FETCH(loc, attr) \
  CUDA_SAFE_CALL(cuDeviceGetAttribute((loc), (attr), device));
#define NUMOPTIONS 5

namespace K3 {

const string_impl extern_C("struct R_elem2 {int elem;}; \n  extern \"C\" ");
//const string_impl extern_C("template <typename R_elem> \n extern \"C\" ");

GPUimpl::GPUimpl() {
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
 
GPUimpl::~GPUimpl() 
{
  _dattributes.clear();
}

int
GPUimpl::get_dev_count() 
{
  return _dcount;
}

/* summary */
void
GPUimpl::dev_info() 
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

void 
GPUimpl::compile_ptx(const char* src, 
                     std::string& ptxstr,
                     int nh, 
                     const char** headers,
                     const char** includeNames,
                     int dev,
                     bool materialize
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
      
  NVRTC_SAFE_CALL(nvrtcCreateProgram(&prog, src, "kernel", nh, headers, includeNames));
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
  
void 
GPUimpl::load_kernel(const string_impl& modname,
                     const string_impl& funname,
                     CUfunction kfunc,
                     bool  is_file
                    ) 
{
  if (modname.length() == 0 || funname.length() == 0)
     return;

  CUmodule module;
  if (is_file)
    CUDA_SAFE_CALL(cuModuleLoad(&module, modname.c_str()));
  else
    // TODO: We set jit options to 0 temporarily
    CUDA_SAFE_CALL(cuModuleLoadDataEx(&module, modname.c_str(), 0, 0, 0));
  CUDA_SAFE_CALL(cuModuleGetFunction(&kfunc, module, funname.c_str()));
  CUDA_SAFE_CALL(cuModuleUnload(module));
}
  
int 
GPUimpl::run_kernel(CUfunction   fun,
                    CUdevice*    device,
                    void**       args,
                    bool         sync,
                    uint         gridx,
                    uint         gridy,
                    uint         gridz,
                    uint         blockx,
                    uint         blocky,
                    uint         blockz
                   ) 
{
  if (!device)
    return -1;
  std::cout << "launching" << std::endl;
  //TODO: Sanity checks on grid block dimensions
  CUDA_SAFE_CALL(cuLaunchKernel(fun,
                                gridx, gridy, gridz,
                                blockx, blocky, blockz, 
                                0, NULL, 
                                args, 0));
  if(sync)
    CUDA_SAFE_CALL(cuCtxSynchronize());
  return 0;
}

void 
GPUimpl::cleanup(CUcontext* context)
{
  if(context)
    CUDA_SAFE_CALL(cuCtxDestroy(*context));
}

GPUBuiltins::GPUBuiltins()
: _impl(new GPUimpl()) {}

int 
GPUBuiltins::get_device_count(unit_t) {
  return _impl->get_dev_count();
}

unit_t 
GPUBuiltins::device_info(unit_t) {
  _impl->dev_info();
  return unit_t{};
}
/*
string_impl  
GPUBuiltins::compile_to_ptx_str(const string_impl& code, 
                                const string_impl& ptxname){
  std::string ptx;
  _impl->compile_ptx((extern_C + code).c_str(), code.c_str(), ptx);
  return string_impl(ptx);
}
*/

string_impl  
GPUBuiltins::compile_to_ptx_str(const string_impl& code){
  std::string ptx;
  std::regex e ("__(device|global|host)__");
  std::regex e0 ("extern(\\s)+\\\"C\\\"(\\s)+__(device|global)__");
  std::string kernel =  static_cast<std::string>(code);
  if (!std::regex_match(kernel, e0) ){
    kernel = static_cast<std::string> (std::regex_replace (kernel, e, " extern \"C\" $0"));
  }
  _impl->compile_ptx(kernel.c_str(), ptx);
  return string_impl(ptx);
}
  
int
GPUBuiltins::compile_to_ptx_file(const string_impl& code, 
                                 const string_impl& fname){
  std::ofstream file(fname.c_str());
  file << compile_to_ptx_str(code);
  file.close();
  return 0;
}

string_impl
GPUBuiltins::load_ptx_from_file(const string_impl& filename){
  std::ifstream file(filename.c_str());
  auto s = [&file]{
    std::ostringstream ss{};
    ss << file.rdbuf();
    return ss.str();
  }();
  file.close();
  return string_impl(s);
}
} // namespace K3
