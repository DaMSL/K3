#include <iostream>
#include <string>

#include "Value.hpp"
#include "ProgramContext.hpp"

void DummyContext::processNative(TriggerID t, NativeValue& v) {
  if (t == 1) {
    int i = *v.as<int>();
    intTrigger(i);
  } else if (t == 2) {
    std::string s = std::move(*v.as<std::string>());
    stringTrigger(s);
  } else {
    std::cout << "INVALID TRIGGER ID" << std::endl;
  }
  return;
}

void DummyContext::intTrigger(int i) {
  my_int_ = i;
  return;
}

void DummyContext::stringTrigger(std::string s) {
  my_string_ = s;
  return;
}
