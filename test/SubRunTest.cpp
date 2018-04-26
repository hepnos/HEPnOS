#include "SubRunTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( SubRunTest );

using namespace hepnos;

void SubRunTest::setUp() {}

void SubRunTest::tearDown() {}

void SubRunTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
}

void SubRunTest::testCreateEvents() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1(3);
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

void SubRunTest::testParenthesisOperator() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1(3);
    CPPUNIT_ASSERT(sr1.valid());

    // check access to non-existing SubRun
    Event e0 = sr1(12);
    CPPUNIT_ASSERT(!e0.valid());
    CPPUNIT_ASSERT(e0.number() == InvalidEventNumber);

    // check access to existing SubRun
    Event e13 = sr1(13);
    CPPUNIT_ASSERT(e13.valid());
    CPPUNIT_ASSERT(13 == e13.number());
}

void SubRunTest::testFind() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1(3);
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
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1(3);
    CPPUNIT_ASSERT(sr1.valid());

    std::vector<EventNumber> numbers = {10, 13, 23, 38};
    auto it = sr1.begin();
    for(int i=0; i < numbers.size(); i++, it++) {
        CPPUNIT_ASSERT_EQUAL(numbers[i], it->number());
    }
    CPPUNIT_ASSERT(it == sr1.end());
}

void SubRunTest::testLowerUpperBounds() {
    DataSet mds = (*datastore)["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1(3);
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

