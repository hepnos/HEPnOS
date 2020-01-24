# Move the configuration file used for the configuration test
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${CMAKE_CURRENT_BINARY_DIR}/run-test.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run-two-tests.sh ${CMAKE_CURRENT_BINARY_DIR}/run-two-tests.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test-util.sh ${CMAKE_CURRENT_BINARY_DIR}/test-util.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test-util.sh ${CMAKE_CURRENT_BINARY_DIR}/test-util.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.yaml ${CMAKE_CURRENT_BINARY_DIR}/config.yaml COPYONLY)

set (BOOST_DEPS Boost::serialization)

add_executable(example example.cpp)
target_link_libraries(example hepnos ${BOOST_DEPS})

set(CTEST_ENVIRONMENT "MKTEMP=mktemp" "TIMEOUT=timeout")

add_executable(DataStoreTest DataStoreTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(DataStoreTest ${CPPUNIT_LIBRARIES} hepnos)

add_executable(DataSetTest DataSetTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(DataSetTest ${CPPUNIT_LIBRARIES} hepnos)

add_executable(RunSetTest RunSetTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(RunSetTest ${CPPUNIT_LIBRARIES} hepnos)

add_executable(RunTest RunTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(RunTest ${CPPUNIT_LIBRARIES} hepnos)

add_executable(SubRunTest SubRunTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(SubRunTest ${CPPUNIT_LIBRARIES} hepnos)

add_executable(EventTest EventTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(EventTest ${CPPUNIT_LIBRARIES} hepnos)

add_executable(LoadStoreTest LoadStoreTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(LoadStoreTest ${CPPUNIT_LIBRARIES} hepnos ${BOOST_DEPS})

add_executable(PtrTest PtrTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(PtrTest ${CPPUNIT_LIBRARIES} hepnos ${BOOST_DEPS})

add_executable(WriteAndRestartTest WriteAndRestartTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(WriteAndRestartTest ${CPPUNIT_LIBRARIES} hepnos ${BOOST_DEPS})

add_executable(RestartAndReadTest RestartAndReadTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(RestartAndReadTest ${CPPUNIT_LIBRARIES} hepnos ${BOOST_DEPS})

add_executable(WriteBatchTest WriteBatchTest.cpp HEPnOSTestMain.cpp)
target_link_libraries(WriteBatchTest ${CPPUNIT_LIBRARIES} hepnos ${BOOST_DEPS})

#add_executable(ParallelMPITest ParallelMPITest.cpp HEPnOSTestMain.cpp)
#target_link_libraries(ParallelMPITest ${CPPUNIT_LIBRARIES} hepnos ${BOOST_DEPS})

add_test(NAME DataStoreTest COMMAND run-test.sh ./DataStoreTest)
add_test(NAME DataSetTest   COMMAND run-test.sh ./DataSetTest)
add_test(NAME RunSetTest    COMMAND run-test.sh ./RunSetTest)
add_test(NAME RunTest       COMMAND run-test.sh ./RunTest)
add_test(NAME SubRunTest    COMMAND run-test.sh ./SubRunTest)
add_test(NAME EventTest     COMMAND run-test.sh ./EventTest)
add_test(NAME LoadStoreTest COMMAND run-test.sh ./LoadStoreTest)
add_test(NAME WriteBatchTest COMMAND run-test.sh ./WriteBatchTest)
add_test(NAME PtrTest       COMMAND run-test.sh ./PtrTest)
add_test(NAME RestartTest   COMMAND run-two-tests.sh ./WriteAndRestartTest ./RestartAndReadTest)
#add_test(NAME ParallelMPITest COMMAND run-test.sh "mpirun -np 4 ./ParallelMPITest" 30)
