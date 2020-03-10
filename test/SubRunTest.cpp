#include "SubRunTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( SubRunTest );

using namespace hepnos;

void SubRunTest::setUp() {}

void SubRunTest::tearDown() {}

void SubRunTest::testFillDataStore() {
    auto root = datastore->root();
    auto mds = root.createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
}

void SubRunTest::testDescriptor() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto r1 = mds[42];
    auto sr1 = r1[3];
    CPPUNIT_ASSERT(sr1.valid());
    SubRunDescriptor sr1_desc;
    sr1.toDescriptor(sr1_desc);

    SubRun sr2 = SubRun::fromDescriptor(*datastore, sr1_desc);
    CPPUNIT_ASSERT(sr2.valid());

    SubRunDescriptor invalid_desc;
    SubRun sr3 = SubRun::fromDescriptor(*datastore, invalid_desc);
    CPPUNIT_ASSERT(!sr3.valid());

    SubRun sr4 = SubRun::fromDescriptor(*datastore, invalid_desc, false);
    CPPUNIT_ASSERT(sr4.valid());
}

void SubRunTest::testCreateEvents() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1[3];
    CPPUNIT_ASSERT(sr1.valid());

    Event e10 = sr1.createEvent(10);
    CPPUNIT_ASSERT(e10.valid());
    CPPUNIT_ASSERT(10 == e10.number());

    Event e23 = sr1.createEvent(23);
    CPPUNIT_ASSERT(e23.valid());
    CPPUNIT_ASSERT(23 == e23.number());

    Event e13 = sr1.createEvent(13);
    CPPUNIT_ASSERT(e13.valid());
    CPPUNIT_ASSERT(13 == e13.number());

    Event e38 = sr1.createEvent(38);
    CPPUNIT_ASSERT(e38.valid());
    CPPUNIT_ASSERT(38 == e38.number());
}

void SubRunTest::testBraketOperator() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1[3];
    CPPUNIT_ASSERT(sr1.valid());

    // check access to non-existing SubRun
    CPPUNIT_ASSERT_THROW(sr1[12], hepnos::Exception);

    // check access to existing SubRun
    Event e13 = sr1[13];
    CPPUNIT_ASSERT(e13.valid());
    CPPUNIT_ASSERT(13 == e13.number());
}

void SubRunTest::testFind() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1[3];
    CPPUNIT_ASSERT(sr1.valid());
    // test calling find for a SubRun that does not exist
    {
        auto it = sr1.find(12);
        CPPUNIT_ASSERT(it == sr1.end());
        CPPUNIT_ASSERT(!(it->valid()));
    }
    // test calling find for a SubRun that exists
    {
        auto it = sr1.find(13);
        CPPUNIT_ASSERT(it != sr1.end());
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(13 == it->number());
        // test iteration
        ++it;
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(23 == it->number());
    }
}

void SubRunTest::testBeginEnd() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1[3];
    CPPUNIT_ASSERT(sr1.valid());

    std::vector<EventNumber> numbers = {10, 13, 23, 38};
    auto it = sr1.begin();
    for(int i=0; i < numbers.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(numbers[i], it->number());
    }
    CPPUNIT_ASSERT(it == sr1.end());
}

void SubRunTest::testLowerUpperBounds() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1[3];
    CPPUNIT_ASSERT(sr1.valid());

    {
        auto it = sr1.lower_bound(13);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 13);
    }
    {
        auto it = sr1.lower_bound(14);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 23);
    }
    {
        auto it = sr1.lower_bound(12);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 13);
    }
    {
        auto it = sr1.lower_bound(40);
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == sr1.end());
    }
    {
        auto it = sr1.upper_bound(13);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 23);
    }
    {
        auto it = sr1.upper_bound(14);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 23);
    }
    {
        auto it = sr1.upper_bound(12);
        CPPUNIT_ASSERT(it->valid());
        CPPUNIT_ASSERT(it->number() == 13);
    }
    {
        auto it = sr1.upper_bound(38);
        CPPUNIT_ASSERT(!(it->valid()));
        CPPUNIT_ASSERT(it == sr1.end());
    }
}

void SubRunTest::testAsync() {
    auto root = datastore->root();
    DataSet mds = root.createDataSet("matthieu_async");
    Run r = mds.createRun(1);
    SubRun sr = r.createSubRun(3);
    hepnos::AsyncEngine async(*datastore, 1);

    for(unsigned i=0; i < 10; i++) {
        Event e = sr.createEvent(async, i);
        CPPUNIT_ASSERT(e.valid());
    }

    async.wait();

    for(unsigned i=0; i < 10; i++) {
        Event e = sr[i];
        CPPUNIT_ASSERT(e.valid());
    }
}

void SubRunTest::testPrefetcher() {
    // TODO
}
