#ifndef __HEPNOS_TEST_WRITEANDRESTART_H
#define __HEPNOS_TEST_WRITEANDRESTART_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class WriteAndRestartTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( WriteAndRestartTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testStoreDataSet );
    CPPUNIT_TEST( testStoreRun );
    CPPUNIT_TEST( testStoreSubRun );
    CPPUNIT_TEST( testStoreEvent );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testStoreDataSet();
    void testStoreRun();
    void testStoreSubRun();
    void testStoreEvent();
};

#endif
