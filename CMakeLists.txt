#  general cmake flags:
#    -DCMAKE_INSTALL_PREFIX=/usr/local     -- the prefix for installing
#    -DCMAKE_BUILD_TYPE=type               -- type can be Debug, Release, ...
#    -DCMAKE_PREFIX_PATH=/dir              -- external packages
#
#     note that CMAKE_PREFIX_PATH can be a list of directories:
#      -DCMAKE_PREFIX_PATH='/dir1;/dir2;/dir3'
#

cmake_minimum_required (VERSION 3.14)
project (hepnos CXX)

add_definitions (-g)
set (CMAKE_CXX_STANDARD 14)

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

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# packages we depend on
include (xpkg-import)
find_package (mercury CONFIG REQUIRED)
find_package (Boost REQUIRED COMPONENTS serialization)
include_directories(${Boost_INCLUDE_DIRS})
xpkg_import_module (margo REQUIRED margo)
xpkg_import_module (sdskv-client REQUIRED sdskv-client)
xpkg_import_module (bake-client  REQUIRED bake-client)
xpkg_import_module (sdskv-server REQUIRED sdskv-server)
xpkg_import_module (bake-server  REQUIRED bake-server)
xpkg_import_module (ch-placement REQUIRED ch-placement)
find_package (yaml-cpp REQUIRED)

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

if(CPPUNIT_FOUND AND ENABLE_TESTS)
    message(STATUS "Unit tests are enabled, will be built")
    add_subdirectory (test)
endif(CPPUNIT_FOUND AND ENABLE_TESTS)
