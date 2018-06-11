#ifndef __HEPNOS_TEST_RESTARTANDREAD_H
#define __HEPNOS_TEST_RESTARTANDREAD_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class RestartAndReadTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RestartAndReadTest );
    CPPUNIT_TEST( testLoadDataSet );
    CPPUNIT_TEST( testLoadRun );
    CPPUNIT_TEST( testLoadSubRun );
    CPPUNIT_TEST( testLoadEvent );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testLoadDataSet();
    void testLoadRun();
    void testLoadSubRun();
    void testLoadEvent();
};

#endif
