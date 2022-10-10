#ifndef __HEPNOS_TEST_QUEUE_H
#define __HEPNOS_TEST_QUEUE_H

#include <mpi.h>
#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class QueueTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( QueueTest );
    CPPUNIT_TEST( testQueue );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testQueue();
};

#endif
