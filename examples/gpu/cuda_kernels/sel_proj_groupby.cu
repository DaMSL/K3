struct I {
  int s_id;
  int s_age;
  float s_wage;
};

struct O {
  float s_wage;
};

extern "C" __global__ void
queryplan(I *g_idata, O *g_odata, unsigned int n)
{
    __shared__ float sdata[256];

    // perform first level of reduction,
    // reading from global memory, writing to shared memory
    unsigned int tid = threadIdx.x;
    unsigned int i = blockIdx.x*(256*2) + threadIdx.x;

    float mySum = (i < n) ? g_idata[i].s_wage : 0;

    if (i + 256 < n)
        mySum += g_idata[i+256].s_wage;

    sdata[tid] = mySum;
    __syncthreads();

    // do reduction in shared mem

    __syncthreads();

    if ((tid < 128))
    {
        sdata[tid] = mySum = mySum + sdata[tid + 128];
    }

     __syncthreads();

    if ((tid <  64))
    {
       sdata[tid] = mySum = mySum + sdata[tid +  64];
    }

    __syncthreads();

#if (__CUDA_ARCH__ >= 300 )
    if ( tid < 32 )
    {
        // Fetch final intermediate sum from 2nd warp
        mySum += sdata[tid + 32];
        // Reduce final warp using shuffle
        for (int offset = warpSize/2; offset > 0; offset /= 2)
        {
            mySum += __shfl_down(mySum, offset);
        }
    }
#else
    // fully unroll reduction within a single warp
    if ((tid < 32))
    {
        sdata[tid] = mySum = mySum + sdata[tid + 32];
    }

    __syncthreads();

    if ((tid < 16))
    {
        sdata[tid] = mySum = mySum + sdata[tid + 16];
    }

    __syncthreads();

    if ((tid <  8))
    {
        sdata[tid] = mySum = mySum + sdata[tid +  8];
    }

    __syncthreads();

    if ((tid <  4))
    {
        sdata[tid] = mySum = mySum + sdata[tid +  4];
    }

    __syncthreads();

    if (( tid <  1))
    {
        sdata[tid] = mySum = mySum + sdata[tid +  1];
    }

    __syncthreads();
#endif

    // write result for this block to global mem
    if (tid == 0) 
      g_odata[blockIdx.x].s_wage = mySum;
    else {
      int i = blockDim.x * blockIdx.x + tid;
      if (i < n)
      	g_odata[i].s_wage = 0;
    }
}
