#include "RunSetTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( RunSetTest );

using namespace hepnos;

void RunSetTest::setUp() {}

void RunSetTest::tearDown() {}

void RunSetTest::testFillDataStore() {
    auto root = datastore->root();
    auto mds = root.createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    // erroneous run creation
    CPPUNIT_ASSERT_THROW(
            mds.createRun(InvalidRunNumber),
            hepnos::Exception);
    // default-constructed run has InvalidRunNumber number
    Run r0;
    CPPUNIT_ASSERT(!r0.valid());
    CPPUNIT_ASSERT_THROW(r0.number(), hepnos::Exception);
    // correct run creation
    Run r1 = mds.createRun(42);
    // assert the characteristics of the created dataset
    CPPUNIT_ASSERT(r1.valid());
    CPPUNIT_ASSERT(42 == r1.number());
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
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    // check that accessing a Run that does not exist
    // yields a non-valid Run instance
    CPPUNIT_ASSERT_THROW(mds[43], hepnos::Exception);

    // check that accessing a run that exists yields
    // a valid Run instance with correct information
    Run r2 = mds[45];
    CPPUNIT_ASSERT(r2.valid());
    CPPUNIT_ASSERT(45 == r2.number());

    // check that we access the same Run using the runs() function
    // to go through the RunSet
    Run r22 = mds.runs()[45];
    CPPUNIT_ASSERT(r2 == r22);
}

void RunSetTest::testFind() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
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
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    std::vector<RunNumber> numbers = {42, 45, 47, 53, 59};
    auto it = mds.runs().begin();
    for(int i=0; i < numbers.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(numbers[i], it->number());
    }
    CPPUNIT_ASSERT(it == mds.runs().end());
}

void RunSetTest::testLowerUpperBounds() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
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
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r2 = mds[45];

    {
        CPPUNIT_ASSERT_THROW(r2[73], hepnos::Exception);
    }

    {
        SubRun sr = r2.createSubRun(73);
        CPPUNIT_ASSERT(sr.valid());
        CPPUNIT_ASSERT(73 == sr.number());
    }
}

void RunSetTest::testAsync() {
    auto root = datastore->root();
    DataSet mds = root.createDataSet("matthieu_async");
    CPPUNIT_ASSERT(mds.valid());
    hepnos::AsyncEngine async(*datastore, 1);
    
    for(unsigned i=0; i < 10; i++) {
        Run r = mds.createRun(async, i);
        CPPUNIT_ASSERT(r.valid());
    }

    async.wait();

    for(unsigned i=0; i < 10; i++) {
        CPPUNIT_ASSERT(mds[i].valid());
    }
}

void RunSetTest::testPrefetcher() {
    auto root = datastore->root();
    DataSet mds = root.createDataSet("matthieu_prefetch");
    CPPUNIT_ASSERT(mds.valid());
    
    for(unsigned i=0; i < 20; i++) {
        Run r = mds.createRun(i);
        CPPUNIT_ASSERT(r.valid());
    }

    // test begin/end
    {
        Prefetcher prefetcher(*datastore);
        unsigned i=0;
        for(auto it = mds.runs().begin(prefetcher); it != mds.runs().end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
    // test begin/end using a Prefetchable object
    {
        Prefetcher prefetch(*datastore);
        unsigned i=0;
        for(auto& r : prefetch(mds.runs())) {
            CPPUNIT_ASSERT(r.valid());
            CPPUNIT_ASSERT(r.number() == i);
            i += 1;
        }
    }
    // test lower_bound
    {
        Prefetcher prefetcher(*datastore);
        unsigned i=5;
        auto it = mds.runs().lower_bound(5, prefetcher);
        for(; it != mds.runs().end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
    // test upper_bound
    {
        Prefetcher prefetcher(*datastore);
        unsigned i=6;
        auto it = mds.runs().upper_bound(5, prefetcher);
        for(; it != mds.runs().end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
}

void RunSetTest::testAsyncPrefetcher() {
    auto root = datastore->root();
    DataSet mds = root.createDataSet("matthieu_prefetch");
    CPPUNIT_ASSERT(mds.valid());
    
    for(unsigned i=0; i < 20; i++) {
        Run r = mds.createRun(i);
        CPPUNIT_ASSERT(r.valid());
    }

    // test begin/end
    {
        AsyncEngine async(*datastore, 1);
        Prefetcher prefetcher(*datastore, async);
        unsigned i=0;
        for(auto it = mds.runs().begin(prefetcher); it != mds.runs().end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
    // test begin/end using a Prefetchable object
    {
        AsyncEngine async(*datastore, 1);
        Prefetcher prefetch(*datastore, async);
        unsigned i=0;
        for(auto& r : prefetch(mds.runs())) {
            CPPUNIT_ASSERT(r.valid());
            CPPUNIT_ASSERT(r.number() == i);
            i += 1;
        }
    }
    // test lower_bound
    {
        AsyncEngine async(*datastore, 1);
        Prefetcher prefetcher(*datastore, async);
        unsigned i=5;
        auto it = mds.runs().lower_bound(5, prefetcher);
        for(; it != mds.runs().end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
    // test upper_bound
    {
        AsyncEngine async(*datastore, 1);
        Prefetcher prefetcher(*datastore, async);
        unsigned i=6;
        auto it = mds.runs().upper_bound(5, prefetcher);
        for(; it != mds.runs().end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
}
