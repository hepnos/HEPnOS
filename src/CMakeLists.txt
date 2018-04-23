# list of source files
set(hepnos-src DataStore.cpp DataSet.cpp RunSet.cpp Run.cpp) 

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (hepnos-pkg "share/cmake/hepnos")

#
# library version set here (e.g. for shared libs).
#
set (HEPNOS_VERSION_MAJOR 1)
set (HEPNOS_VERSION_MINOR 0)
set (HEPNOS_VERSION_PATCH 0)
set (hepnos-vers "${HEPNOS_VERSION_MAJOR}.${HEPNOS_VERSION_MINOR}")
set (HEPNOS_VERSION "${hepnos-vers}.${HEPNOS_VERSION_PATCH}")

add_library(hepnos ${hepnos-src})
target_link_libraries (hepnos mercury margo yaml-cpp sdskv-client bake-client ch-placement)
target_include_directories (hepnos PUBLIC $<INSTALL_INTERFACE:include>)

# local include's BEFORE, in case old incompatable .h files in prefix/include
include_directories (hepnos ${CMAKE_CURRENT_SOURCE_DIR}/private)
target_include_directories (hepnos BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)

# for shared libs, establish the lib version
set_target_properties (hepnos
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
#install (TARGETS hepnos EXPORT hepnos-targets
#         ARCHIVE DESTINATION lib
#         LIBRARY DESTINATION lib)
#install (EXPORT hepnos-targets
#         DESTINATION ${hepnos-pkg}
#         FILE "hepnos-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/hepnos-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/hepnos-config-version.cmake"
               "../cmake/xpkg-import.cmake"
         DESTINATION ${hepnos-pkg} )
install (DIRECTORY ../include/hepnos
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
