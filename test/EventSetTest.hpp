#ifndef __HEPNOS_TEST_EVENTSET_H
#define __HEPNOS_TEST_EVENTSET_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class EventSetTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( EventSetTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testBeginEnd );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testBeginEnd();
};

#endif
