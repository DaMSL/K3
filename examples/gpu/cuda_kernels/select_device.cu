struct R {                                          
  int elem;                                            
};                                                   

__device__
int predicate(const R* input, int index){
  if (input[index].elem < 5)
    return 1;
  else
    return 0;
}

extern "C" __global__                                          
void select(const R *input, R *out_idx, size_t n)      
{
  __shared__ int temp[512];
  size_t thid = threadIdx.x;
  size_t start = 2 * blockIdx.x * blockDim.x;
  // SELECTION
  temp[2 * thid] = ((start + 2 * thid < n) && predicate(input, start + 2 * thid));
  temp[2 * thid + 1] = ((start + 2 * thid + 1 < n) && predicate(input, start + 2 * thid + 1));
  
  // perform inclusive scan on temp (3 steps)
  // step 1. reduction 
  for (int stride = 1; stride <= blockDim.x; stride <<= 1){
    __syncthreads();
    int index = (thid + 1) * stride * 2 - 1;
    if (index < 2 * blockDim.x){
      temp[index] += temp[index - stride];
    }
  }
// step 2. clear last element
  if (thid == 0){
    temp[2 * blockDim.x - 1] = 0;
  }
  // step 3: traverse down the tree
  for (int stride = blockDim.x ; stride >= 2; stride >>= 1){
    __syncthreads();
    int index = (thid + 1) * stride * 2 - 1;
    if (index  < 2 * blockDim.x){
      int t = temp[index - stride];
      temp[index - stride] = temp[index];
      temp[index] += t;
    }
  }  
 __syncthreads();
//  // inclusive scan -> exclusive scan
//  temp[2 * thid] = temp[2 * thid + 1] ;
//  temp[2 * thid + 1] = (2 * thid + 2 < 2 * blockDim.x ?
//     temp[2 * thid + 2] : temp[2 * thid + 1] + predicate(input, 2 * thid + 1 + start) ) ;
//  __syncthreads();
//  // set all location to -1 (meaning not selected)
//  out_idx[start + 2*thid].elem = -1;
//  out_idx[start + 2*thid + 1].elem = -1;
//  __syncthreads();
//  // write index to the right location (overwrite -1)
//  if (thid != 0 && temp[2 * thid] > temp[2 * thid - 1] ){
//    out_idx[start + temp[2 * thid] - 1].elem = 2 * thid;
//  } 
//  if (thid == 0 && temp[2 * thid] > 0){
//   out_idx[start + temp[2 * thid] - 1].elem = 2 * thid;
//  } 
//  if (temp[2 * thid + 1] > temp[2 * thid]){
//    out_idx[start + temp[2 * thid + 1] - 1].elem = 2 * thid + 1;
//  }
//  //
// choice 1. output exclusive scan
 out_idx[2 * thid + start].elem = temp[2 * thid ] ;
 out_idx[2 * thid + start + 1].elem = temp[2 * thid + 1] ; 
} 

