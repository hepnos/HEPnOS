#include "EventTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( EventTest );

using namespace hepnos;

void EventTest::setUp() {}

void EventTest::tearDown() {}

void EventTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
    CPPUNIT_ASSERT(sr1.valid());
    Event ev1 = sr1.createEvent(22);
    CPPUNIT_ASSERT(ev1.valid());
}

