#ifndef __HEPNOS_TEST_LOADSTORE_H
#define __HEPNOS_TEST_LOADSTORE_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class LoadStoreTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( LoadStoreTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testLoadStoreDataSet );
    CPPUNIT_TEST( testLoadStoreRun );
    CPPUNIT_TEST( testLoadStoreSubRun );
    CPPUNIT_TEST( testLoadStoreEvent );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testLoadStoreDataSet();
    void testLoadStoreRun();
    void testLoadStoreSubRun();
    void testLoadStoreEvent();
};

#endif
