#ifndef __HEPNOS_TEST_PTR_H
#define __HEPNOS_TEST_PTR_H

#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

extern hepnos::DataStore* datastore;

class PtrTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( PtrTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testMakePtr );
    CPPUNIT_TEST( testPtrLoad );
    CPPUNIT_TEST( testPtrLoadFromArray );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testMakePtr();
    void testPtrLoad();
    void testPtrLoadFromArray();
};

#endif
