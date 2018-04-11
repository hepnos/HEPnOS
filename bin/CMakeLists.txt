add_executable(hepnos-service hepnos-service.cpp hepnos-service-util.cpp)
target_link_libraries(hepnos-service sdskv-server bake-server yaml-cpp)
