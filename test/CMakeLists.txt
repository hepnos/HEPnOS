# Move the configuration file used for the configuration test
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${CMAKE_CURRENT_BINARY_DIR}/run-test.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test-util.sh ${CMAKE_CURRENT_BINARY_DIR}/test-util.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.yaml ${CMAKE_CURRENT_BINARY_DIR}/config.yaml COPYONLY)

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

add_executable(SubRunTest SubRunTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(SubRunTest cppunit hepnos hepnos-service)

add_executable(EventTest EventTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(EventTest cppunit hepnos hepnos-service)

add_executable(LoadStoreTest LoadStoreTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(LoadStoreTest cppunit hepnos hepnos-service boost_serialization)

add_test(NAME DataStoreTest COMMAND run-test.sh ./DataStoreTest)
add_test(NAME DataSetTest   COMMAND run-test.sh ./DataSetTest)
add_test(NAME RunSetTest    COMMAND run-test.sh ./RunSetTest)
add_test(NAME RunTest       COMMAND run-test.sh ./RunTest)
add_test(NAME SubRunTest    COMMAND run-test.sh ./SubRunTest)
add_test(NAME EventTest     COMMAND run-test.sh ./EventTest)
add_test(NAME LoadStoreTest COMMAND run-test.sh ./LoadStoreTest)
