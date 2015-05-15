#include <iostream>
#include <string>

#include "Value.hpp"
#include "ProgramContext.hpp"

void ProgramContext::dispatch(unique_ptr<Message> m) {
  dispatch(*m);
}

DummyContext::DummyContext(shared_ptr<DummyState> s) {
  state_ = s;
}

void DummyContext::dispatch(const Message& m) {
  TriggerID t = m.trigger();
  auto v = m.value();
  if (t == 1) {
    int i = *v->as<int>();
    intTrigger(i);
  } else if (t == 2) {
    std::string s = std::move(*v->as<std::string>());
    stringTrigger(s);
  } else {
    std::cout << "INVALID TRIGGER ID" << std::endl;
  }
  return;
}

void DummyContext::intTrigger(int i) {
  state_->my_int_ = i;
  return;
}

void DummyContext::stringTrigger(std::string s) {
  state_->my_string_ = s;
  return;
}
