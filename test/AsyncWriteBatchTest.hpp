#ifndef __HEPNOS_TEST_ASYNCWRITEBATCH_H
#define __HEPNOS_TEST_ASYNCWRITEBATCH_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class AsyncWriteBatchTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( AsyncWriteBatchTest );
    CPPUNIT_TEST( testAsyncWriteBatchRun );
    CPPUNIT_TEST( testAsyncWriteBatchSubRun );
    CPPUNIT_TEST( testAsyncWriteBatchEvent );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testAsyncWriteBatchRun();
    void testAsyncWriteBatchSubRun();
    void testAsyncWriteBatchEvent();
};

#endif
