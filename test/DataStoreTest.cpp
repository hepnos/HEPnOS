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
            datastore->createDataSet("ds0/AAA"),
            hepnos::Exception);
    // "%" is forbidden in the name, will throw an exception
    CPPUNIT_ASSERT_THROW(
            datastore->createDataSet("ds0%RRS"),
            hepnos::Exception);
    // correct dataset creation
    DataSet ds1 = datastore->createDataSet("ds1");
    // assert the characteristics of the created dataset
    CPPUNIT_ASSERT(ds1.valid());
    CPPUNIT_ASSERT_EQUAL_STR("ds1", ds1.name());
    CPPUNIT_ASSERT_EQUAL_STR("", ds1.container());
    CPPUNIT_ASSERT_EQUAL_STR("ds1", ds1.fullname());
    // assert invalid dataset when it does not exist
    DataSet ds_invalid = (*datastore)["invalid"];
    DataSet ds_invalid2 = (*datastore)["invalid2"];
    CPPUNIT_ASSERT(!ds_invalid.valid());
    CPPUNIT_ASSERT(!ds_invalid2.valid());
    // assert comparison with a default-constructed dataset
    DataSet ds0;
    CPPUNIT_ASSERT(ds0 != ds1);
    CPPUNIT_ASSERT(!(ds0 == ds1));
    // assert that ds1.next() is not valid
    DataSet ds2 = ds1.next();
    CPPUNIT_ASSERT(!ds2.valid());
    // create more datasets
    DataSet ds3 = datastore->createDataSet("ds3");
    ds2 = datastore->createDataSet("ds2");
    // assert that these are valid
    CPPUNIT_ASSERT(ds2.valid());
    CPPUNIT_ASSERT(ds3.valid());
    // assert that ds1.next() == ds2 and ds2.next() == ds3
    CPPUNIT_ASSERT(ds2 == ds1.next());
    CPPUNIT_ASSERT(ds3 == ds2.next());
    // create more datasets for future tests
    DataSet ds4 = datastore->createDataSet("dsB");
    DataSet ds5 = datastore->createDataSet("dsD");
    CPPUNIT_ASSERT(ds4.valid());
    CPPUNIT_ASSERT(ds5.valid());
}

void DataStoreTest::testBraketOperator() {
    // check that accessing a dataset that does not exist
    // yields a non-valid DataSet
    DataSet ds6 = (*datastore)["ds6"];
    CPPUNIT_ASSERT(!ds6.valid());

    // check that accessing a dataset that exists yields
    // a valid DataSet instance with correct information
    DataSet ds2 = (*datastore)["ds2"];
    CPPUNIT_ASSERT(ds2.valid());
    CPPUNIT_ASSERT_EQUAL_STR("ds2", ds2.name());
    CPPUNIT_ASSERT_EQUAL_STR("", ds2.container());
    CPPUNIT_ASSERT_EQUAL_STR("ds2", ds2.fullname());
}

void DataStoreTest::testFind() {
    // test calling find for a DataSet that does not exist
    {
        auto it = datastore->find("ds6");
        CPPUNIT_ASSERT(it == datastore->end());
        CPPUNIT_ASSERT(!(it->valid()));
    }
    // test calling find for a DataSet that exists
    {
        auto it = datastore->find("ds2");
        CPPUNIT_ASSERT(it != datastore->end());
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT_EQUAL_STR("ds2",it->name());
        // test iteration
        ++it;
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT_EQUAL_STR("ds3",it->name());
    }
}

void DataStoreTest::testBeginEnd() {
    std::vector<std::string> names = {
        "ds1", "ds2", "ds3", "dsB", "dsD"};
    auto it = datastore->begin();
    for(int i=0; i < names.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(names[i], it->name());
    }
    CPPUNIT_ASSERT(it == datastore->end());
}

void DataStoreTest::testLowerUpperBounds() {
    {
        auto it = datastore->lower_bound("dsB");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsB");
    }
    {
        auto it = datastore->lower_bound("dsC");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsD");
    }
    {
        auto it = datastore->lower_bound("dsA");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsB");
    }
    {
        auto it = datastore->lower_bound("dsE");
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == datastore->end());
    }
    {
        auto it = datastore->upper_bound("dsB");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsD");
    }
    {
        auto it = datastore->upper_bound("dsC");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsD");
    }
    {
        auto it = datastore->upper_bound("dsA");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsB");
    }
    {
        auto it = datastore->upper_bound("dsD");
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == datastore->end());
    }
}
