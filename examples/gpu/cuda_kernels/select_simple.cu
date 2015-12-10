extern "C" {
struct I {
  int s_id;
  int s_age;
  float s_wage;
};

struct O {
  int s_id;
  float s_wage;
};

__global__ 
void select(const I *A, O *C, size_t n)      
{
  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < n){                                     
    C[i].s_id = A[i].s_id;                       
    C[i].s_wage = A[i].s_wage ;                   
  }                                           
} 
}
