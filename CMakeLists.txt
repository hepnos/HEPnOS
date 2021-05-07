cmake_minimum_required (VERSION 3.12)
project (hepnos C CXX)

if(NOT "${CMAKE_CXX_STANDARD}")
  set (CMAKE_CXX_STANDARD 14)
endif()

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "OFF" CACHE BOOL "Build a shared library")

#include_directories (${CMAKE_CURRENT_SOURCE_DIR}/include)

find_package (spdlog REQUIRED)
find_package (PkgConfig REQUIRED)
find_package (MPI REQUIRED)
find_package (thallium CONFIG REQUIRED)
find_package (Boost REQUIRED COMPONENTS serialization)
find_package (bedrock REQUIRED)
#include_directories(${Boost_INCLUDE_DIRS})

pkg_check_modules (uuid REQUIRED IMPORTED_TARGET uuid)
pkg_check_modules (sdskv-client REQUIRED IMPORTED_TARGET sdskv-client)
pkg_check_modules (ch-placement REQUIRED IMPORTED_TARGET ch-placement)

find_package (CppUnit)
if (CPPUNIT_FOUND)
    message(STATUS "CppUnit found, unit tests will be compiled")
    include_directories(${CPPUNIT_INCLUDE_DIR})
    enable_testing()
else (CPPUNIT_FOUND)
    message(STATUS "CppUnit not found, unit tests will not be compiled")
endif (CPPUNIT_FOUND)

add_subdirectory (src)
add_subdirectory (bin)
if(ENABLE_EXAMPLES)
    message(STATUS "Examples will be built")
    add_subdirectory (examples)
endif(ENABLE_EXAMPLES)

if(CPPUNIT_FOUND AND ENABLE_TESTS)
    message(STATUS "Unit tests are enabled, will be built")
    add_subdirectory (test)
endif(CPPUNIT_FOUND AND ENABLE_TESTS)
