#ifndef __HEPNOS_TEST_DATASET_H
#define __HEPNOS_TEST_DATASET_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class DataSetTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( DataSetTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testBraketOperator );
    CPPUNIT_TEST( testFind );
    CPPUNIT_TEST( testBeginEnd );
    CPPUNIT_TEST( testCreateRuns );
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
    void testCreateRuns();
};

#endif
