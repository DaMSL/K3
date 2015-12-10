struct I {
  int s_id;
  int s_age;
  double s_wage;
};

struct O {
  int s_id;
  double s_wage;
};

extern "C" __global__ 
void select_proj(const I *A, O *C, size_t n)      
{
  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < n) { 
    if (A[i].s_wage > 1400000.0) {                                     
      C[i].s_id = A[i].s_id;                       
      C[i].s_wage = A[i].s_wage;
    }
    else {
      C[i].s_id = 0;
      C[i].s_wage = 0.0;                   
    }
  }                                           
}
