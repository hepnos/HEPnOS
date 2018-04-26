add_executable(hepnos-daemon hepnos-daemon.cpp)
target_link_libraries(hepnos-daemon hepnos-service yaml-cpp margo bake-server sdskv-server)

install(TARGETS hepnos-daemon DESTINATION bin)
