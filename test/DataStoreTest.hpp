#ifndef __HEPNOS_TEST_DATASTORE_H
#define __HEPNOS_TEST_DATASTORE_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class DataStoreTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( DataStoreTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testBraketOperator );
    CPPUNIT_TEST( testFind );
    CPPUNIT_TEST( testBeginEnd );
    CPPUNIT_TEST( testLowerUpperBounds );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testBraketOperator();
    void testFind();
    void testBeginEnd();
    void testLowerUpperBounds();
};

#endif
