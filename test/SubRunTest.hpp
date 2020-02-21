#ifndef __HEPNOS_TEST_SUBRUN_H
#define __HEPNOS_TEST_SUBRUN_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class SubRunTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( SubRunTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testCreateEvents );
    CPPUNIT_TEST( testBraketOperator );
    CPPUNIT_TEST( testFind );
    CPPUNIT_TEST( testBeginEnd );
    CPPUNIT_TEST( testLowerUpperBounds );
    CPPUNIT_TEST( testAsync );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testCreateEvents();
    void testBraketOperator();
    void testFind();
    void testBeginEnd();
    void testLowerUpperBounds();
    void testAsync();
};

#endif
