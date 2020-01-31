#ifndef __HEPNOS_TEST_LOADSTORE_VECTOR_H
#define __HEPNOS_TEST_LOADSTORE_VECTOR_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class LoadStoreVectorsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( LoadStoreVectorsTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testLoadStoreDataSet );
    CPPUNIT_TEST( testLoadStoreRun );
    CPPUNIT_TEST( testLoadStoreSubRun );
    CPPUNIT_TEST( testLoadStoreEvent );
    CPPUNIT_TEST( testLoadStoreDataSetSubVector );
    CPPUNIT_TEST( testLoadStoreRunSubVector );
    CPPUNIT_TEST( testLoadStoreSubRunSubVector );
    CPPUNIT_TEST( testLoadStoreEventSubVector );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testLoadStoreDataSet();
    void testLoadStoreRun();
    void testLoadStoreSubRun();
    void testLoadStoreEvent();
    void testLoadStoreDataSetSubVector();
    void testLoadStoreRunSubVector();
    void testLoadStoreSubRunSubVector();
    void testLoadStoreEventSubVector();
};

#endif
