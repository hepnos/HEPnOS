#include "EventTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( EventTest );

using namespace hepnos;

void EventTest::setUp() {}

void EventTest::tearDown() {}

void EventTest::testFillDataStore() {
    auto root = datastore->root();
    auto mds = root.createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
    CPPUNIT_ASSERT(sr1.valid());
    Event ev1 = sr1.createEvent(22);
    CPPUNIT_ASSERT(ev1.valid());
}

void EventTest::testDescriptor() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto r1 = mds[42];
    auto sr1 = r1[3];
    auto ev1 = sr1[22];

    CPPUNIT_ASSERT(ev1.valid());
    EventDescriptor ev1_desc;
    ev1.toDescriptor(ev1_desc);

    Event ev2 = Event::fromDescriptor(*datastore, ev1_desc);
    CPPUNIT_ASSERT(ev2.valid());

    EventDescriptor invalid_desc;
    Event ev3 = Event::fromDescriptor(*datastore, invalid_desc);
    CPPUNIT_ASSERT(!ev3.valid());

    Event ev4 = Event::fromDescriptor(*datastore, invalid_desc, false);
    CPPUNIT_ASSERT(ev4.valid());
}
