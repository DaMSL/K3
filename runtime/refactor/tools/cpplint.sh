#!/bin/bash
./tools/external/cpplint.py --filter=-legal/copyright,-runtime/int,-runtime/references,-build/c++11 --extensions=hpp,cpp --linelength=100 include/* src/* test/*
