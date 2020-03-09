#ifndef __HEPNOS_TEST_EVENT_H
#define __HEPNOS_TEST_EVENT_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class EventTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( EventTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testDescriptor );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testDescriptor();
};

#endif
