#!/bin/bash
./tools/external/cpplint.py \
  --filter=-build/include_order,-build/namespaces,-legal/copyright,-runtime/printf,-runtime/invalid_increment,-runtime/int,-runtime/references,-runtime/explicit,-build/c++11 --extensions=hpp,cpp --linelength=100 \
  include/* include/core/* include/builtins/* include/builtins/loaders/* include/collections/* include/network/* include/serialization/* include/types* \
  src/* src/core/* src/builtins/* src/network/* src/serialization/* src/types/* \
  test/Engine.cpp
