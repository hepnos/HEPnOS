#ifndef __HEPNOS_TEST_PARALLEL_H
#define __HEPNOS_TEST_PARALLEL_H

#include <mpi.h>
#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class ParallelMPITest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( ParallelMPITest );
    CPPUNIT_TEST( testParallelEventProcessor );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testParallelEventProcessor();
};

#endif
