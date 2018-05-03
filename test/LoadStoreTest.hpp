#ifndef __HEPNOS_TEST_LOADSTORE_H
#define __HEPNOS_TEST_LOADSTORE_H

#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>
#include <cppunit/extensions/HelperMacros.h>
#include <hepnos.hpp>

class TestObjectA {

    friend class boost::serialization::access;

    public:

    int& x() { return _x; }
    double& y() { return _y; }

    bool operator==(const TestObjectA& other) const {
        return _x == other._x && _y == other._y;
    }

    private:

    int    _x;
    double _y;

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & _x;
        ar & _y;
    }
};

class TestObjectB {

    friend class boost::serialization::access;

    public:

    int& a() { return _a; }
    std::string& b() { return _b; }

    bool operator==(const TestObjectB& other) const {
        return _a == other._a && _b == other._b;
    }

    private:

    int         _a;
    std::string _b;

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & _a;
        ar & _b;
    }
};

extern hepnos::DataStore* datastore;

class LoadStoreTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( LoadStoreTest );
    CPPUNIT_TEST( testFillDataStore );
    CPPUNIT_TEST( testLoadStoreDataSet );
    CPPUNIT_TEST( testLoadStoreRun );
    CPPUNIT_TEST( testLoadStoreSubRun );
    CPPUNIT_TEST( testLoadStoreEvent );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp();
    void tearDown();

    void testFillDataStore();
    void testLoadStoreDataSet();
    void testLoadStoreRun();
    void testLoadStoreSubRun();
    void testLoadStoreEvent();
};

#endif
