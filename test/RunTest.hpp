#ifndef __HEPNOS_TEST_RUN_H
#define __HEPNOS_TEST_RUN_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class RunTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RunTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testDescriptor );
    CPPUNIT_TEST( testCreateSubRuns );
    CPPUNIT_TEST( testBraketOperator );
    CPPUNIT_TEST( testFind );
    CPPUNIT_TEST( testBeginEnd );
    CPPUNIT_TEST( testLowerUpperBounds );
    CPPUNIT_TEST( testAsync );
    CPPUNIT_TEST( testPrefetcher );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testDescriptor();
    void testCreateSubRuns();
    void testBraketOperator();
    void testFind();
    void testBeginEnd();
    void testLowerUpperBounds();
    void testAsync();
    void testPrefetcher();
};

#endif
