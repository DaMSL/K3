#include "builtins/VectorBuiltins.hpp"

namespace K3 {

VectorBuiltins::VectorBuiltins() { srand(time(NULL)); }

Vector<R_elem<double>> VectorBuiltins::zeroVector(int i) {
  Vector<R_elem<double>> result;
  auto& c = result.getContainer();
  c.resize(i);
  for (int j = 0; j < i; j++) {
    c[j] = R_elem<double>{0.0};
  }
  return result;
}

// Elements must have random values in [0,1)
Vector<R_elem<double>> VectorBuiltins::randomVector(int i) {
  Vector<R_elem<double>> result;
  auto& c = result.getContainer();
  c.resize(i);
  for (int j = 0; j < i; j++) {
    c[j] = R_elem<double>{(rand() / (RAND_MAX + 1.))};
  }
  return result;
}

}  // namespace K3
