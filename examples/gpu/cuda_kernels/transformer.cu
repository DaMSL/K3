struct R_elem 
{ 
  int elem;
}; 
__global__ void 
transformer(const R_elem *A, R_elem *C, size_t numElements) 
{                                                      
  size_t i = blockDim.x * blockIdx.x + threadIdx.x;    
  if (i < numElements)                                 
    C[i].elem = A[i].elem + i;                         
}                                                      
