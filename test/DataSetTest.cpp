#include "DataSetTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( DataSetTest );

using namespace hepnos;

void DataSetTest::setUp() {}

void DataSetTest::tearDown() {}

void DataSetTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    datastore->createDataSet("shane");
    datastore->createDataSet("phil");
    datastore->createDataSet("rob");

    // erroneous dataset creations
    // "/" is forbidden in the name, will throw an exception
    CPPUNIT_ASSERT_THROW(
            mds.createDataSet("ds0/AAA"),
            hepnos::Exception);
    // "%" is forbidden in the name, will throw an exception
    CPPUNIT_ASSERT_THROW(
            mds.createDataSet("ds0%RRS"),
            hepnos::Exception);
    // correct dataset creation
    DataSet ds1 = mds.createDataSet("ds1");
    // assert the characteristics of the created dataset
    CPPUNIT_ASSERT(ds1.valid());
    CPPUNIT_ASSERT_EQUAL_STR("ds1", ds1.name());
    CPPUNIT_ASSERT_EQUAL_STR("matthieu", ds1.container());
    CPPUNIT_ASSERT_EQUAL_STR("matthieu/ds1", ds1.fullname());
    // assert access from DataStore using full path
    DataSet matthieu_ds1 = (*datastore)["matthieu/ds1"];
    CPPUNIT_ASSERT(matthieu_ds1.valid());
    CPPUNIT_ASSERT(matthieu_ds1 == ds1);
    // create a dataset inside ds1
    DataSet ds11 = ds1.createDataSet("ds11");
    CPPUNIT_ASSERT(ds11.valid());
    // access ds11 using path from "matthieu"
    DataSet ds1_ds11 = mds["ds1/ds11"];
    CPPUNIT_ASSERT(ds1_ds11.valid());
    CPPUNIT_ASSERT(ds1_ds11 == ds11);
    // assert comparison with a default-constructed dataset
    DataSet ds0;
    CPPUNIT_ASSERT(ds0 != ds1);
    CPPUNIT_ASSERT(!(ds0 == ds1));
    // assert that ds1.next() is not valid
    DataSet ds2 = ds1.next();
    CPPUNIT_ASSERT(!ds2.valid());
    // create more datasets
    DataSet ds3 = mds.createDataSet("ds3");
    ds2 = mds.createDataSet("ds2");
    // assert that these are valid
    CPPUNIT_ASSERT(ds2.valid());
    CPPUNIT_ASSERT(ds3.valid());
    // assert that ds1.next() == ds2 and ds2.next() == ds3
    CPPUNIT_ASSERT(ds2 == ds1.next());
    CPPUNIT_ASSERT(ds3 == ds2.next());
    // create more datasets for future tests
    DataSet ds4 = mds.createDataSet("dsB");
    DataSet ds5 = mds.createDataSet("dsD");
    CPPUNIT_ASSERT(ds4.valid());
    CPPUNIT_ASSERT(ds5.valid());
}

void DataSetTest::testBraketOperator() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    // check that accessing a dataset that does not exist
    // yields a non-valid DataSet
    DataSet ds6 = mds["ds6"];
    CPPUNIT_ASSERT(!ds6.valid());

    // check that accessing a dataset that exists yields
    // a valid DataSet instance with correct information
    DataSet ds2 = mds["ds2"];
    CPPUNIT_ASSERT(ds2.valid());
    CPPUNIT_ASSERT_EQUAL_STR("ds2", ds2.name());
    CPPUNIT_ASSERT_EQUAL_STR("matthieu", ds2.container());
    CPPUNIT_ASSERT_EQUAL_STR("matthieu/ds2", ds2.fullname());
}

void DataSetTest::testFind() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    // test calling find for a DataSet that does not exist
    {
        auto it = mds.find("ds6");
        CPPUNIT_ASSERT(it == mds.end());
        CPPUNIT_ASSERT(!(it->valid()));
    }
    // test calling find for a DataSet that exists
    {
        auto it = mds.find("ds2");
        CPPUNIT_ASSERT(it != mds.end());
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT_EQUAL_STR("ds2",it->name());
        // test iteration
        ++it;
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT_EQUAL_STR("ds3",it->name());
    }
}

void DataSetTest::testBeginEnd() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    std::vector<std::string> names = {
        "ds1", "ds2", "ds3", "dsB", "dsD"};
    auto it = mds.begin();
    for(int i=0; i < names.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(names[i], it->name());
    }
    CPPUNIT_ASSERT(it == mds.end());
}

void DataSetTest::testLowerUpperBounds() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    {
        auto it = mds.lower_bound("dsB");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsB");
    }
    {
        auto it = mds.lower_bound("dsC");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsD");
    }
    {
        auto it = mds.lower_bound("dsA");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsB");
    }
    {
        auto it = mds.lower_bound("dsE");
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == mds.end());
    }
    {
        auto it = mds.upper_bound("dsB");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsD");
    }
    {
        auto it = mds.upper_bound("dsC");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsD");
    }
    {
        auto it = mds.upper_bound("dsA");
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->name() == "dsB");
    }
    {
        auto it = mds.upper_bound("dsD");
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == mds.end());
    }
}

void DataSetTest::testCreateRuns() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    {
        Run r = mds[45];
        CPPUNIT_ASSERT(!r.valid());
        CPPUNIT_ASSERT_THROW(r.number(), hepnos::Exception);
    }

    {
        Run r = mds.createRun(45);
        CPPUNIT_ASSERT(r.valid());
        CPPUNIT_ASSERT(45 == r.number());
        CPPUNIT_ASSERT_EQUAL_STR("matthieu", r.container());
    }
    
}
