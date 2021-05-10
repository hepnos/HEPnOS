# list of source files
set(hepnos-src DataStore.cpp
	       DataSet.cpp
	       RunSet.cpp
	       Run.cpp
	       SubRun.cpp
	       Event.cpp
	       UUID.cpp
	       WriteBatch.cpp
	       ParallelEventProcessor.cpp
	       Prefetcher.cpp
	       ProductCache.cpp
	       ProductID.cpp
	       AsyncEngine.cpp
	       EventSet.cpp)

set(hepnos-service-src
	UUID.cpp
       service/ComparisonFunction.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

#
# library version set here (e.g. for shared libs).
#
set (HEPNOS_VERSION_MAJOR 0)
set (HEPNOS_VERSION_MINOR 5)
set (HEPNOS_VERSION_PATCH 0)
set (hepnos-vers "${HEPNOS_VERSION_MAJOR}.${HEPNOS_VERSION_MINOR}")
set (HEPNOS_VERSION "${hepnos-vers}.${HEPNOS_VERSION_PATCH}")

# where to install files for "find_package"
set (hepnos-pkg "share/cmake/hepnos")

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (PRIVATE_LIBS "-lhepnos")
configure_file ("hepnos.pc.in" "hepnos.pc" @ONLY)

#
# Client library
#

# add hepnos library
add_library(hepnos ${hepnos-src})
target_link_libraries (hepnos
    nlohmann_json
    PkgConfig::uuid
    PkgConfig::sdskv-client
    PkgConfig::ch-placement)
target_include_directories (hepnos BEFORE PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/private)
target_include_directories (hepnos PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (hepnos BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)

# for shared libs, establish the lib version
set_target_properties (hepnos
    PROPERTIES VERSION ${HEPNOS_VERSION}
    SOVERSION ${HEPNOS_VERSION_MAJOR})

#
# Service library
#

# add hepnos-service library
add_library(hepnos-service SHARED ${hepnos-service-src})
target_link_libraries (hepnos-service
    PkgConfig::uuid
    mercury)
target_include_directories (hepnos-service BEFORE PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (hepnos-service BEFORE PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/service>)

# for shared libs, establish the lib version
set_target_properties (hepnos-service
    PROPERTIES VERSION ${HEPNOS_VERSION}
    SOVERSION ${HEPNOS_VERSION_MAJOR})

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "hepnos-config-version.cmake"
    VERSION ${HEPNOS_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (hepnos-config.cmake.in hepnos-config.cmake @ONLY)

#
# "make install" rules
#
install (TARGETS hepnos EXPORT hepnos-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (TARGETS hepnos-service ARCHIVE DESTINATION lib LIBRARY DESTINATION lib)
install (EXPORT hepnos-targets
         DESTINATION ${hepnos-pkg}
         FILE "hepnos-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/hepnos-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/hepnos-config-version.cmake"
         DESTINATION ${hepnos-pkg} )
install (DIRECTORY ../include/hepnos
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
install (DIRECTORY ../include/hepnos
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (FILES ../include/hepnos.hpp
         DESTINATION include)
install (FILES ../include/hepnos-service.h
         DESTINATION include)
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/hepnos.pc"
         DESTINATION "lib/pkgconfig/")
