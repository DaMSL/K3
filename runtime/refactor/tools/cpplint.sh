#!/bin/bash
./tools/external/cpplint.py \
  --filter=-legal/copyright,-runtime/int,-runtime/references,-runtime/explicit,-build/c++11 --extensions=hpp,cpp --linelength=100 \
  include/* include/core/* include/builtins/* include/collections/* include/network/* \
  src/* src/core/* src/builtins/* src/network/* \
  test/Engine.cpp
