cmake_minimum_required(VERSION 2.8)

project(k3-mesos)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

include(FindPkgConfig)
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)
include(CheckFunctionExists)
include(CheckVariableExists)
include(CheckTypeSize)
include(CheckLibraryExists)
include(CheckCSourceCompiles)
include(CMakeDetermineCCompiler)

set(CMAKE_CXX_FLAGS "-g -std=c++11")

FIND_PACKAGE(Boost 1.54.0 COMPONENTS program_options system thread REQUIRED )

set(MESOS_INCLUDE_DIR /usr/local/include CACHE PATH "Mesos include dir")
set(MESOS_LIBRARY_DIR /usr/local/lib CACHE PATH "Mesos library dir")
set(MESOS_LIBRARIES mesos CACHE STRING "Mesos library (ldflags)")
set(MESOS_CPPFLAGS CACHE STRING "Mesos C preprocessor flags")
set(YAMLCPP_STATIC libyaml-cpp.a)

link_directories(${MESOS_LIBRARY_DIR})
include_directories(${MESOS_INCLUDE_DIR})
include_directories(${MESOS_INCLUDE_DIR}/mesos)

link_directories(${Boost_LIBRARY_DIRS})

add_definitions(-Wall -Wno-variadic-macros)


add_executable(k3executor  k3_executor.cpp)
target_link_libraries(k3executor mesos ${Boost_LIBRARIES} ${YAMLCPP_STATIC})

add_executable(k3scheduler  k3_scheduler.cpp)
target_link_libraries(k3scheduler ${MESOS_LIBRARIES} ${Boost_LIBRARIES} ${YAMLCPP_STATIC})

#add_executable(k3run  k3run.cpp)
#target_link_libraries(k3run ${MESOS_LIBRARIES} ${BOOST_LIBRARIES} ${YAMLCPP_STATIC})

#add_executable(ycpp ycpp.cpp)
#target_link_libraries(ycpp ${YAMLCPP_STATIC})

# we need the following definitions in order to get some special
# OS-level features like posix_fadvise() or readahead().
# add_definitions(-DXOPEN_SOURCE=600)
add_definitions(-D_GNU_SOURCE)

# ISO C99: explicitely request format specifiers
add_definitions(-D__STDC_FORMAT_MACROS)


