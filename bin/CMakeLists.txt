add_executable(hepnos-daemon hepnos-daemon.cpp)
target_link_libraries(hepnos-daemon hepnos-service yaml-cpp margo sdskv-server)

add_executable(hepnos-shutdown hepnos-shutdown.cpp)
target_link_libraries(hepnos-shutdown hepnos yaml-cpp margo)

add_executable(hepnos-ls hepnos-ls.cpp)
target_link_libraries(hepnos-ls hepnos yaml-cpp margo)

install(TARGETS hepnos-shutdown DESTINATION bin)
install(TARGETS hepnos-daemon DESTINATION bin)
install(TARGETS hepnos-ls DESTINATION bin)
