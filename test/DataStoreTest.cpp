#include "DataStoreTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( DataStoreTest );

using namespace hepnos;

void DataStoreTest::setUp() {}

void DataStoreTest::tearDown() {}

void DataStoreTest::testFillDataStore() {
    // erroneous dataset creations
    // "/" is forbidden in the name, will throw an exception
    CPPUNIT_ASSERT_THROW(
            datastore->root().createDataSet("ds0/AAA"),
            hepnos::Exception);
    // "%" is forbidden in the name, will throw an exception
    CPPUNIT_ASSERT_THROW(
            datastore->root().createDataSet("ds0%RRS"),
            hepnos::Exception);
    // correct dataset creation
    DataSet ds1 = datastore->root().createDataSet("ds1");
    // assert the characteristics of the created dataset
    CPPUNIT_ASSERT(ds1.valid());
    CPPUNIT_ASSERT_EQUAL_STR("ds1", ds1.name());
    CPPUNIT_ASSERT_EQUAL_STR("", ds1.container());
    CPPUNIT_ASSERT_EQUAL_STR("/ds1", ds1.fullname());
    // assert invalid dataset when it does not exist
    CPPUNIT_ASSERT_THROW(datastore->root()["invalid"], hepnos::Exception);
    // assert comparison with a default-constructed dataset
    DataSet ds0;
    CPPUNIT_ASSERT(ds0 != ds1);
    CPPUNIT_ASSERT(!(ds0 == ds1));
    // assert that ds1.next() is not valid
    DataSet ds2 = ds1.next();
    CPPUNIT_ASSERT(!ds2.valid());
    // create more datasets
    DataSet ds3 = datastore->root().createDataSet("ds3");
    ds2 = datastore->root().createDataSet("ds2");
    // assert that these are valid
    CPPUNIT_ASSERT(ds2.valid());
    CPPUNIT_ASSERT(ds3.valid());
    // assert that ds1.next() == ds2 and ds2.next() == ds3
    CPPUNIT_ASSERT(ds2 == ds1.next());
    CPPUNIT_ASSERT(ds3 == ds2.next());
    // create more datasets for future tests
    DataSet ds4 = datastore->root().createDataSet("dsB");
    DataSet ds5 = datastore->root().createDataSet("dsD");
    CPPUNIT_ASSERT(ds4.valid());
    CPPUNIT_ASSERT(ds5.valid());
}

