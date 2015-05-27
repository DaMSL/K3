cmake_minimum_required (VERSION 3.0)
project (k3)
set (CMAKE_ARCHIVE_OUTPUT_DIRECTORY ./lib)
set (CMAKE_LIBRARY_OUTPUT_DIRECTORY ./lib)
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ./bin)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++1y" COMPILER_SUPPORTS_CXX1Y)
if(COMPILER_SUPPORTS_CXX1Y)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++1y")
else()
  message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++1y support. Please use a different C++ compiler.")
endif()

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 

# TODO error handling on not found
find_package(Boost 1.55.0 COMPONENTS serialization system thread program_options)
find_library(YAMLCPP_LIBRARY
             NAMES libyaml-cpp.a yaml-cpp)

add_subdirectory(include/external/gtest-1.7.0)
enable_testing()

set (srcs src/core/Engine.cpp 
          src/core/Peer.cpp
          src/core/ProgramContext.cpp
          src/types/BaseString.cpp
          src/types/Message.cpp
          src/types/Value.cpp
          src/network/NetworkManager.cpp
          src/network/Listener.cpp
          src/network/IncomingConnection.cpp
          src/network/OutgoingConnection.cpp
          src/builtins/StandardBuiltins.cpp
          src/builtins/TimeBuiltins.cpp
          src/builtins/StringBuiltins.cpp
          src/Options.cpp
    )

# TODO add k3 back
set (link_libs k3 ${Boost_LIBRARIES} ${YAMLCPP_LIBRARY} profiler)
set (test_link_libs ${link_libs} gtest gtest_main)

include_directories    (include include/external ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR} include/external/)
add_library            (k3 ${srcs})

set (tests test/Engine.cpp
    )

add_executable(Tests ${tests} test/Main.cpp)
target_link_libraries(Tests ${test_link_libs})
