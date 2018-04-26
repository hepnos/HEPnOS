#include "RunSetTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( RunSetTest );

using namespace hepnos;

void RunSetTest::setUp() {}

void RunSetTest::tearDown() {}

void RunSetTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    // erroneous run creation
    CPPUNIT_ASSERT_THROW(
            mds.createRun(InvalidRunNumber),
            hepnos::Exception);
    // default-constructed run has InvalidRunNumber number
    Run r0;
    CPPUNIT_ASSERT(!r0.valid());
    CPPUNIT_ASSERT(InvalidRunNumber == r0.number());
    // correct run creation
    Run r1 = mds.createRun(42);
    // assert the characteristics of the created dataset
    CPPUNIT_ASSERT(r1.valid());
    CPPUNIT_ASSERT(42 == r1.number());
    CPPUNIT_ASSERT_EQUAL_STR("matthieu", r1.container());
    // assert comparison with a default-constructed run
    CPPUNIT_ASSERT(r0 != r1);
    CPPUNIT_ASSERT(!(r0 == r1));
    // assert that r1.next() is not valid
    Run r2 = r1.next();
    CPPUNIT_ASSERT(!r2.valid());
    // create more runs
    Run r3 = mds.createRun(47);
    r2 = mds.createRun(45);
    // assert that these are valid
    CPPUNIT_ASSERT(r2.valid());
    CPPUNIT_ASSERT(r3.valid());
    // assert that r1.next() == r2 and r2.next() == r3
    CPPUNIT_ASSERT(r2 == r1.next());
    CPPUNIT_ASSERT(r3 == r2.next());
    // create more datasets for future tests
    Run r4 = mds.createRun(53);
    Run r5 = mds.createRun(59);
    CPPUNIT_ASSERT(r4.valid());
    CPPUNIT_ASSERT(r5.valid());
}

void RunSetTest::testBraketOperator() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    // check that accessing a Run that does not exist
    // yields a non-valid Run instance
    Run r6 = mds[43];
    CPPUNIT_ASSERT(!r6.valid());

    // check that accessing a run that exists yields
    // a valid Run instance with correct information
    Run r2 = mds[45];
    CPPUNIT_ASSERT(r2.valid());
    CPPUNIT_ASSERT(45 == r2.number());
    CPPUNIT_ASSERT_EQUAL_STR("matthieu", r2.container());

    // check that we access the same Run using the runs() function
    // to go through the RunSet
    Run r22 = mds.runs()[45];
    CPPUNIT_ASSERT(r2 == r22);
}

void RunSetTest::testFind() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    // test calling find for a Run that does not exist
    {
        auto it = mds.runs().find(43);
        CPPUNIT_ASSERT(it == mds.runs().end());
        CPPUNIT_ASSERT(!(it->valid()));
    }
    // test calling find for a Run that exists
    {
        auto it = mds.runs().find(45);
        CPPUNIT_ASSERT(it != mds.runs().end());
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(45 == it->number());
        // test iteration
        ++it;
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(47 == it->number());
    }
}

void RunSetTest::testBeginEnd() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    std::vector<RunNumber> numbers = {42, 45, 47, 53, 59};
    auto it = mds.runs().begin();
    for(int i=0; i < numbers.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(numbers[i], it->number());
    }
    CPPUNIT_ASSERT(it == mds.runs().end());
}

void RunSetTest::testLowerUpperBounds() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    {
        auto it = mds.runs().lower_bound(47);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 47);
    }
    {
        auto it = mds.runs().lower_bound(48);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 53);
    }
    {
        auto it = mds.runs().lower_bound(46);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 47);
    }
    {
        auto it = mds.runs().lower_bound(60);
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == mds.runs().end());
    }
    {
        auto it = mds.runs().upper_bound(47);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 53);
    }
    {
        auto it = mds.runs().upper_bound(48);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 53);
    }
    {
        auto it = mds.runs().upper_bound(46);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 47);
    }
    {
        auto it = mds.runs().upper_bound(59);
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == mds.runs().end());
    }
}

void RunSetTest::testCreateSubRuns() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r2 = mds[45];

    {
        SubRun sr = r2[73];
        CPPUNIT_ASSERT(!sr.valid());
        CPPUNIT_ASSERT(sr.number() == InvalidSubRunNumber);
    }

    {
        SubRun sr = r2.createSubRun(73);
        CPPUNIT_ASSERT(sr.valid());
        CPPUNIT_ASSERT(73 == sr.number());
    }
}