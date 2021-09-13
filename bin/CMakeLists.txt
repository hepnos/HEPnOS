add_executable(hepnos-shutdown hepnos-shutdown.cpp)
target_link_libraries(hepnos-shutdown hepnos)

add_executable(hepnos-ls hepnos-ls.cpp)
target_link_libraries(hepnos-ls hepnos)

install(TARGETS hepnos-shutdown DESTINATION bin)
install(TARGETS hepnos-ls DESTINATION bin)
