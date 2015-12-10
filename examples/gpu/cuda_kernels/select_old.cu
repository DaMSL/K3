struct R {                                          
  int elem;                                            
};                                                   
extern "C" __global__                                          
void select(const R *input, R *out_idx, size_t n)      
{
  __shared__ int temp[512];
  size_t thid = threadIdx.x;
  size_t start = 2 * blockIdx.x * blockDim.x;
  temp[2 * thid] = ((start + 2 * thid < n) && (input[start + 2 * thid].elem < 5) ? 1: 0);
  temp[2 * thid + 1] = ((start + 2 * thid + 1 < n) && (input[start + 2 * thid + 1].elem < 5) ? 1: 0);
  for (int stride = 1; stride <= blockDim.x; stride <<= 1){
    __syncthreads();
    int index = (thid + 1) * stride * 2 - 1;
    if (index < 2 * blockDim.x){
      temp[index] += temp[index - stride];
    }
  }
  if (thid == 0){
    temp[2 * blockDim.x - 1] = 0;
  }
  for (int stride = blockDim.x ; stride >= 1; stride >>= 1){
    __syncthreads();
    int index = (thid + 1) * stride * 2 + 1;
    if (index  < 2 * blockDim.x){
      int t = temp[index - stride];
      temp[index - stride] = temp[index];
      temp[index] += t;
    }
  }
  __syncthreads();
  out_idx[2 * thid + start].elem = temp[2 * thid + 1] ;
  out_idx[2 * thid + start + 1].elem = (2 * thid + 2 < 2 * blockDim.x ? temp[2 * thid + 2] : temp[2 * thid + 1] + input[2 * thid + 1 + start].elem) ;
}



 
