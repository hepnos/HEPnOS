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
	       EventSet.cpp
	       Statistics.cpp)

set(hepnos-service-src
       UUID.cpp
       service/ComparisonFunction.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

#
# library version set here (e.g. for shared libs).
#
set (HEPNOS_VERSION_MAJOR 0)
set (HEPNOS_VERSION_MINOR 6)
set (HEPNOS_VERSION_PATCH 4)
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
    MPI::MPI_C
    nlohmann_json
    yokan-client
    spdlog::spdlog
    PkgConfig::uuid
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

configure_file (hepnos-list-databases.jx9
                ${CMAKE_BINARY_DIR}/share/hepnos-list-databases.jx9
                COPYONLY)

install (FILES ${CMAKE_BINARY_DIR}/share/hepnos-list-databases.jx9
         DESTINATION share)

if ("${BEDROCK_QUERY_EXE}" STREQUAL "")
    message (WARNING "Could not find a bedrock-query executable, hepnos-list-databases will not be created")
else ()
    configure_file (hepnos-list-databases.in hepnos-list-databases @ONLY)
    install (FILES ${CMAKE_CURRENT_BINARY_DIR}/hepnos-list-databases
             DESTINATION bin
             PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ)
endif ()

if ("${BEDROCK_EXE}" STREQUAL "")
    message (WARNING "Could not find a bedrock executable, hepnos-daemon symlink will not be created")
else ()
    install (CODE "execute_process( \
               COMMAND ${CMAKE_COMMAND} -E create_symlink \
               ${BEDROCK_EXE} \
               ${CMAKE_INSTALL_PREFIX}/bin/hepnos-daemon \
               )"
            )
endif ()
