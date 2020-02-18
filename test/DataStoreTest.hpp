#ifndef __HEPNOS_TEST_DATASTORE_H
#define __HEPNOS_TEST_DATASTORE_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class DataStoreTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( DataStoreTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
};

#endif
