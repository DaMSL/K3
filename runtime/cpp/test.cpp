#include "BaseTypes.hpp"

unit_t foo() { return unit_t {}; }

int main(int argc, char* argv[]) {
  unit_t r = foo();
  return 0;
}
