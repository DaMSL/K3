cmake_minimum_required (VERSION 2.8)
project (k3)


set (CMAKE_ARCHIVE_OUTPUT_DIRECTORY ./lib)
set (CMAKE_LIBRARY_OUTPUT_DIRECTORY ./lib)
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ./bin)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG(-std=c++14 COMPILER_SUPPORTS_CXX14)
if(COMPILER_SUPPORTS_CXX14)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -g -std=c++14 -DYAS_SERIALIZE_BOOST_TYPES=1")
else()
  message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++14 support. Please use a different C++ compiler.")
endif()

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 

# TODO error handling on not found
find_package(Boost 1.55.0 COMPONENTS chrono serialization system thread program_options)
find_library(YAMLCPP_LIBRARY
             NAMES libyaml-cpp.a yaml-cpp)
find_library(CSVPP_LIBRARY
             NAMES libcsvpp.dylib csvpp)

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
          src/storage/StorageManager.cpp
          src/storage/FileHandle.cpp
          src/network/OutgoingConnection.cpp
          src/builtins/StandardBuiltins.cpp
          src/builtins/TimeBuiltins.cpp
          src/builtins/StringBuiltins.cpp
          src/builtins/ProfilingBuiltins.cpp
          src/builtins/VectorBuiltins.cpp
          src/serialization/Codec.cpp
          src/Options.cpp
          src/Prettify.cpp
    )

set (link_libs k3 ${Boost_LIBRARIES} ${YAMLCPP_LIBRARY} ${CSVPP_LIBRARY} dynamic)
set (test_link_libs ${link_libs} gtest gtest_main)

include_directories    (include include/external ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
add_library            (k3 ${srcs})

set (tests test/Engine.cpp
	   test/StorageManager.cpp
           test/Serialization.cpp
    )

add_executable(Tests ${tests} test/Main.cpp)
target_link_libraries(Tests ${test_link_libs})
