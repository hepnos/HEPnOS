#ifndef __HEPNOS_TEST_WRITEBATCH_H
#define __HEPNOS_TEST_WRITEBATCH_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class WriteBatchTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( WriteBatchTest );
    CPPUNIT_TEST( testWriteBatchRun );
    CPPUNIT_TEST( testWriteBatchSubRun );
    CPPUNIT_TEST( testWriteBatchEvent );
    CPPUNIT_TEST( testWriteBatchEmpty );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testWriteBatchRun();
    void testWriteBatchSubRun();
    void testWriteBatchEvent();
    void testWriteBatchEmpty();
};

#endif
