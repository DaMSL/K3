cmake_minimum_required (VERSION 3.0)
project                (k3rt)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++1y" COMPILER_SUPPORTS_CXX1Y)
if(COMPILER_SUPPORTS_CXX1Y)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")
else()
  message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++1y support. Please use a different C++ compiler.")
endif()

set (srcs BaseString.cpp
          Builtins.cpp
          Context.cpp
          EndpointBuffer.cpp
          Endpoint.cpp
          Engine.cpp
          Codec.cpp
          IOHandle.cpp
          Literals.cpp
          Options.cpp
    )

include_directories    (. external)
add_library            (k3rt ${srcs})
