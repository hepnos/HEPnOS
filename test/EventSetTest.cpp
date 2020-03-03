#include "EventSetTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( EventSetTest );

using namespace hepnos;

void EventSetTest::setUp() {}

void EventSetTest::tearDown() {}

void EventSetTest::testFillDataStore() {
    auto root = datastore->root();
    auto mds = root.createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    for(unsigned i=3; i < 5; i++) {
        auto run = mds.createRun(i);
        CPPUNIT_ASSERT(run.valid());
        for(unsigned j=6; j < 9; j++) {
            auto subrun = run.createSubRun(j);
            CPPUNIT_ASSERT(subrun.valid());
            for(unsigned k=1; k < 5; k++) {
                auto event = subrun.createEvent(k);
                CPPUNIT_ASSERT(event.valid());
            }
        }
    }
    // dataset in which (0,0,0) exists
    auto zero = root.createDataSet("zero");
    CPPUNIT_ASSERT(mds.valid());
    for(unsigned i=0; i < 2; i++) {
        auto run = zero.createRun(i);
        CPPUNIT_ASSERT(run.valid());
        for(unsigned j=0; j < 3; j++) {
            auto subrun = run.createSubRun(j);
            CPPUNIT_ASSERT(subrun.valid());
            for(unsigned k=0; k < 4; k++) {
                auto event = subrun.createEvent(k);
                CPPUNIT_ASSERT(event.valid());
            }
        }
    }
    // empty dataset
    root.createDataSet("empty");
    // only runs and subruns
    auto mds2 = root.createDataSet("runsandsubruns");
    CPPUNIT_ASSERT(mds.valid());
    for(unsigned i=3; i < 5; i++) {
        auto run = mds.createRun(i);
        CPPUNIT_ASSERT(run.valid());
        for(unsigned j=6; j < 9; j++) {
            auto subrun = run.createSubRun(j);
            CPPUNIT_ASSERT(subrun.valid());
        }
    }
    // one single item
    auto onevent = root.createDataSet("oneevent");
    onevent.createRun(1).createSubRun(3).createEvent(76);
}

void EventSetTest::testBeginEnd() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());
    DataSet zero = root["zero"];
    CPPUNIT_ASSERT(zero.valid());
    DataSet empty = root["empty"];
    CPPUNIT_ASSERT(empty.valid());
    DataSet runsandsubruns = root["runsandsubruns"];
    CPPUNIT_ASSERT(runsandsubruns.valid());
    DataSet oneevent = root["oneevent"];
    CPPUNIT_ASSERT(oneevent.valid());
    
    // empty dataset doesn't have events
    auto eventset = empty.events();
    for(auto& ev : eventset) {
        CPPUNIT_ASSERT_MESSAGE("This statement shouldn't be reached", false);
    }
    // runsandsubruns dataset doesn't have events
    eventset = runsandsubruns.events();
    for(auto& ev : eventset) {
        CPPUNIT_ASSERT_MESSAGE("This statement shouldn't be reached", false);
    }

    std::vector<std::tuple<RunNumber, SubRunNumber, EventNumber>> events;
    // iteration target by target
    unsigned i = 0;
    for(int target = 0; target < datastore->numTargets(hepnos::ItemType::EVENT); target++) {
        eventset = mds.events(target);
        for(auto& ev : eventset) {
            CPPUNIT_ASSERT(ev.valid());
            i += 1;
        }
    }
    CPPUNIT_ASSERT_EQUAL(2*3*4, (int)i);
    // iteration all targets at once
    i = 0;
    eventset = mds.events();
    for(auto& ev : eventset) {
        CPPUNIT_ASSERT(ev.valid());
        i += 1;
    }
    CPPUNIT_ASSERT_EQUAL(2*3*4, (int)i);
    // iteration target by target when first event is (0,0,0)
    i = 0;
    for(int target = 0; target < datastore->numTargets(hepnos::ItemType::EVENT); target++) {
        eventset = zero.events(target);
        for(auto& ev : eventset) {
            CPPUNIT_ASSERT(ev.valid());
            i += 1;
        }
    }
    CPPUNIT_ASSERT_EQUAL(2*3*4, (int)i);
    // iteration all targets at once
    i = 0;
    eventset = zero.events();
    for(auto& ev : eventset) {
        CPPUNIT_ASSERT(ev.valid());
        i += 1;
    }
    CPPUNIT_ASSERT_EQUAL(2*3*4, (int)i);
    // iteration on single-event dataset
    i = 0;
    eventset = oneevent.events();
    for(auto& ev : eventset) {
        CPPUNIT_ASSERT_EQUAL(76, (int)ev.number());
        i += 1;
    }
    CPPUNIT_ASSERT_EQUAL(1,(int)i);
}

