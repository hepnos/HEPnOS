#include "RunTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( RunTest );

using namespace hepnos;

void RunTest::setUp() {}

void RunTest::tearDown() {}

void RunTest::testFillDataStore() {
    auto root = datastore->root();
    auto mds = root.createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
}

void RunTest::testDescriptor() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());
    RunDescriptor r1_desc;
    r1.toDescriptor(r1_desc);

    Run r2 = Run::fromDescriptor(*datastore, r1_desc);
    CPPUNIT_ASSERT(r2.valid());

    RunDescriptor invalid_desc;
    Run r3 = Run::fromDescriptor(*datastore, invalid_desc);
    CPPUNIT_ASSERT(!r3.valid());

    Run r4 = Run::fromDescriptor(*datastore, invalid_desc, false);
    CPPUNIT_ASSERT(r4.valid());
}

void RunTest::testCreateSubRuns() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];

    SubRun sr0 = r1.createSubRun(0);
    CPPUNIT_ASSERT(sr0.valid());
    CPPUNIT_ASSERT(0 == sr0.number());

    SubRun sr10 = r1.createSubRun(10);
    CPPUNIT_ASSERT(sr10.valid());
    CPPUNIT_ASSERT(10 == sr10.number());

    SubRun sr23 = r1.createSubRun(23);
    CPPUNIT_ASSERT(sr23.valid());
    CPPUNIT_ASSERT(23 == sr23.number());

    SubRun sr13 = r1.createSubRun(13);
    CPPUNIT_ASSERT(sr13.valid());
    CPPUNIT_ASSERT(13 == sr13.number());

    SubRun sr38 = r1.createSubRun(38);
    CPPUNIT_ASSERT(sr38.valid());
    CPPUNIT_ASSERT(38 == sr38.number());
}

void RunTest::testBraketOperator() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());

    // check access to non-existing SubRun
    CPPUNIT_ASSERT_THROW(r1[12], hepnos::Exception);

    // check access to existing SubRun
    SubRun sr13 = r1[13];
    CPPUNIT_ASSERT(sr13.valid());
    CPPUNIT_ASSERT(13 == sr13.number());
}

void RunTest::testFind() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());
    // test calling find for a SubRun that does not exist
    {
        auto it = r1.find(12);
        CPPUNIT_ASSERT(it == r1.end());
        CPPUNIT_ASSERT(!(it->valid()));
    }
    // test calling find for a SubRun that exists
    {
        auto it = r1.find(13);
        CPPUNIT_ASSERT(it != r1.end());
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(13 == it->number());
        // test iteration
        ++it;
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(23 == it->number());
    }
}

void RunTest::testBeginEnd() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());

    std::vector<SubRunNumber> numbers = {0, 10, 13, 23, 38};
    auto it = r1.begin();
    for(int i=0; i < numbers.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(numbers[i], it->number());
    }
    CPPUNIT_ASSERT(it == r1.end());
}

void RunTest::testLowerUpperBounds() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());

    {
        auto it = r1.lower_bound(0);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 0);
    }
    {
        auto it = r1.lower_bound(13);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 13);
    }
    {
        auto it = r1.lower_bound(14);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 23);
    }
    {
        auto it = r1.lower_bound(12);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 13);
    }
    {
        auto it = r1.lower_bound(40);
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == r1.end());
    }
    {
        auto it = r1.upper_bound(0);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 10);
    }
    {
        auto it = r1.upper_bound(13);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 23);
    }
    {
        auto it = r1.upper_bound(14);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 23);
    }
    {
        auto it = r1.upper_bound(12);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 13);
    }
    {
        auto it = r1.upper_bound(38);
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == r1.end());
    }
}

void RunTest::testAsync() {
    auto root = datastore->root();
    DataSet mds = root.createDataSet("matthieu_async");
    Run r = mds.createRun(1);
    hepnos::AsyncEngine async(*datastore, 1);

    for(unsigned i=0; i < 10; i++) {
        SubRun sr = r.createSubRun(async, i);
        CPPUNIT_ASSERT(sr.valid());
    }

    async.wait();

    for(unsigned i=0; i < 10; i++) {
        SubRun sr = r[i];
        CPPUNIT_ASSERT(sr.valid());
    }
}

void RunTest::testPrefetcher() {
    auto root = datastore->root();
    DataSet mds = root.createDataSet("matthieu_prefetch");
    CPPUNIT_ASSERT(mds.valid());
    Run r = mds.createRun(42);
    CPPUNIT_ASSERT(r.valid());

    for(unsigned i=0; i < 20; i++) {
        SubRun sr = r.createSubRun(i);
        CPPUNIT_ASSERT(sr.valid());
    }

    // test begin/end
    {
        Prefetcher prefetcher(*datastore);
        unsigned i=0;
        for(auto it = r.begin(prefetcher); it != r.end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
    // test lower_bound
    {
        Prefetcher prefetcher(*datastore);
        unsigned i=5;
        auto it = r.lower_bound(5, prefetcher);
        for(; it != r.end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
    // test upper_bound
    {
        Prefetcher prefetcher(*datastore);
        unsigned i=6;
        auto it = r.upper_bound(5, prefetcher);
        for(; it != r.end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
    }
}

void RunTest::testAsyncPrefetcher() {
    auto root = datastore->root();
    DataSet mds = root.createDataSet("matthieu_prefetch");
    CPPUNIT_ASSERT(mds.valid());
    Run r = mds.createRun(42);
    CPPUNIT_ASSERT(r.valid());

    for(unsigned i=0; i < 20; i++) {
        SubRun sr = r.createSubRun(i);
        CPPUNIT_ASSERT(sr.valid());
    }
    // test begin/end
    {
        AsyncEngine async(*datastore, 1);
        Prefetcher prefetcher(async);
        unsigned i=0;
        for(auto it = r.begin(prefetcher); it != r.end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
        CPPUNIT_ASSERT_EQUAL(20, (int)i);
    }
    // test lower_bound
    {
        AsyncEngine async(*datastore, 1);
        Prefetcher prefetcher(async);
        unsigned i=5;
        auto it = r.lower_bound(5, prefetcher);
        for(; it != r.end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
        CPPUNIT_ASSERT(i == 20);
    }
    // test upper_bound
    {
        AsyncEngine async(*datastore, 1);
        Prefetcher prefetcher(async);
        unsigned i=6;
        auto it = r.upper_bound(5, prefetcher);
        for(; it != r.end(); it++) {
            CPPUNIT_ASSERT(it->valid());
            CPPUNIT_ASSERT(it->number() == i);
            i += 1;
        }
        CPPUNIT_ASSERT(i == 20);
    }
}
