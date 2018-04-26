# Move the configuration file used for the configuration test
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${CMAKE_CURRENT_BINARY_DIR}/run-test.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test-util.sh ${CMAKE_CURRENT_BINARY_DIR}/test-util.sh COPYONLY)

add_executable(example example.cpp)
target_link_libraries(example hepnos ${Boost_LIBRARIES})

set(CTEST_ENVIRONMENT "MKTEMP=mktemp" "TIMEOUT=timeout")

add_executable(DataStoreTest DataStoreTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(DataStoreTest cppunit hepnos hepnos-service)

add_executable(DataSetTest DataSetTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(DataSetTest cppunit hepnos hepnos-service)

add_executable(RunSetTest RunSetTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(RunSetTest cppunit hepnos hepnos-service)

add_executable(RunTest RunTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(RunTest cppunit hepnos hepnos-service)

add_test(NAME DataStoreTest COMMAND run-test.sh ./DataStoreTest)
add_test(NAME DataSetTest   COMMAND run-test.sh ./DataSetTest)
add_test(NAME RunSetTest    COMMAND run-test.sh ./RunSetTest)
add_test(NAME RunTest       COMMAND run-test.sh ./RunTest)
