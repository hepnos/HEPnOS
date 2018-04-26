#include "RunTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( RunTest );

using namespace hepnos;

void RunTest::setUp() {}

void RunTest::tearDown() {}

void RunTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
}

void RunTest::testCreateSubRuns() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];

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
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());

    // check access to non-existing SubRun
    SubRun sr0 = r1[12];
    CPPUNIT_ASSERT(!sr0.valid());
    CPPUNIT_ASSERT(sr0.number() == InvalidSubRunNumber);

    // check access to existing SubRun
    SubRun sr13 = r1[13];
    CPPUNIT_ASSERT(sr13.valid());
    CPPUNIT_ASSERT(13 == sr13.number());
}

void RunTest::testFind() {
    DataSet mds = (*datastore)["matthieu"];
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
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());

    std::vector<SubRunNumber> numbers = {10, 13, 23, 38};
    auto it = r1.begin();
    for(int i=0; i < numbers.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(numbers[i], it->number());
    }
    CPPUNIT_ASSERT(it == r1.end());
}

void RunTest::testLowerUpperBounds() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds[42];
    CPPUNIT_ASSERT(r1.valid());

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

