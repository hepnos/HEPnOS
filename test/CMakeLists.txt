# Move the configuration file used for the configuration test
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ${CMAKE_CURRENT_BINARY_DIR}/run-test.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run-two-tests.sh ${CMAKE_CURRENT_BINARY_DIR}/run-two-tests.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test-util.sh ${CMAKE_CURRENT_BINARY_DIR}/test-util.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test-util.sh ${CMAKE_CURRENT_BINARY_DIR}/test-util.sh COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.yaml ${CMAKE_CURRENT_BINARY_DIR}/config.yaml COPYONLY)

set (BOOST_DEPS Boost::serialization)

set(CTEST_ENVIRONMENT "MKTEMP=mktemp" "TIMEOUT=timeout")

add_library(HEPnOSTest HEPnOSTestMain.cpp)
target_link_libraries(HEPnOSTest ${CPPUNIT_LIBRARIES} hepnos)

add_executable(DataStoreTest DataStoreTest.cpp)
target_link_libraries(DataStoreTest HEPnOSTest)

add_executable(DataSetTest DataSetTest.cpp)
target_link_libraries(DataSetTest HEPnOSTest)

add_executable(RunSetTest RunSetTest.cpp)
target_link_libraries(RunSetTest HEPnOSTest)

add_executable(EventSetTest EventSetTest.cpp)
target_link_libraries(EventSetTest HEPnOSTest)

add_executable(RunTest RunTest.cpp)
target_link_libraries(RunTest HEPnOSTest)

add_executable(SubRunTest SubRunTest.cpp)
target_link_libraries(SubRunTest HEPnOSTest)

add_executable(EventTest EventTest.cpp)
target_link_libraries(EventTest HEPnOSTest)

add_executable(LoadStoreTest LoadStoreTest.cpp)
target_link_libraries(LoadStoreTest HEPnOSTest ${BOOST_DEPS})

add_executable(LoadStoreVectorsTest LoadStoreVectorsTest.cpp)
target_link_libraries(LoadStoreVectorsTest HEPnOSTest ${BOOST_DEPS})

add_executable(PtrTest PtrTest.cpp)
target_link_libraries(PtrTest HEPnOSTest ${BOOST_DEPS})

add_executable(WriteAndRestartTest WriteAndRestartTest.cpp)
target_link_libraries(WriteAndRestartTest HEPnOSTest ${BOOST_DEPS})

add_executable(RestartAndReadTest RestartAndReadTest.cpp)
target_link_libraries(RestartAndReadTest HEPnOSTest ${BOOST_DEPS})

add_executable(WriteBatchTest WriteBatchTest.cpp)
target_link_libraries(WriteBatchTest HEPnOSTest ${BOOST_DEPS})

add_executable(AsyncWriteBatchTest AsyncWriteBatchTest.cpp)
target_link_libraries(AsyncWriteBatchTest HEPnOSTest ${BOOST_DEPS})

add_executable(ParallelMPITest ParallelMPITest.cpp HEPnOSTestMain.cpp)
target_link_libraries(ParallelMPITest ${CPPUNIT_LIBRARIES} hepnos ${BOOST_DEPS})

add_test(NAME DataStoreTest COMMAND run-test.sh ./DataStoreTest)
add_test(NAME DataSetTest   COMMAND run-test.sh ./DataSetTest)
add_test(NAME RunSetTest    COMMAND run-test.sh ./RunSetTest)
add_test(NAME EventSetTest  COMMAND run-test.sh ./EventSetTest)
add_test(NAME RunTest       COMMAND run-test.sh ./RunTest)
add_test(NAME SubRunTest    COMMAND run-test.sh ./SubRunTest)
add_test(NAME EventTest     COMMAND run-test.sh ./EventTest)
add_test(NAME LoadStoreTest COMMAND run-test.sh ./LoadStoreTest)
add_test(NAME LoadStoreVectorsTest COMMAND run-test.sh ./LoadStoreVectorsTest)
add_test(NAME WriteBatchTest COMMAND run-test.sh ./WriteBatchTest)
add_test(NAME AsyncWriteBatchTest COMMAND run-test.sh ./AsyncWriteBatchTest)
add_test(NAME PtrTest       COMMAND run-test.sh ./PtrTest)
add_test(NAME RestartTest   COMMAND run-two-tests.sh ./WriteAndRestartTest ./RestartAndReadTest)
add_test(NAME ParallelMPITest COMMAND run-test.sh "mpirun -np 4 ./ParallelMPITest" 120)
