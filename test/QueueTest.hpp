#ifndef __HEPNOS_TEST_QUEUE_H
#define __HEPNOS_TEST_QUEUE_H

#include <mpi.h>
#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class QueueTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( QueueTest );
    CPPUNIT_TEST( testQueueCreate );
    CPPUNIT_TEST( testQueueOpen );
    CPPUNIT_TEST( testQueueEmpty );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testQueueCreate();
    void testQueueOpen();
    void testQueueEmpty();
};

#endif
