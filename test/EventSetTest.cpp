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

void EventSetTest::testInvalid() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT_THROW(mds.events(datastore->numTargets(ItemType::EVENT)+1), Exception);
    CPPUNIT_ASSERT_THROW(mds.events(datastore->numTargets(ItemType::EVENT)+1), Exception);
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
            auto ev_number = ev.number();
            auto sr = ev.subrun();
            auto sr_number = sr.number();
            auto r_number = sr.run().number();
            events.emplace_back(r_number, sr_number, ev_number);
        }
    }
    CPPUNIT_ASSERT_EQUAL(2*3*4, (int)i);
    std::sort(events.begin(), events.end(), [](const auto& t1, const auto& t2) {
                auto run1    = std::get<0>(t1);
                auto subrun1 = std::get<1>(t1);
                auto event1  = std::get<2>(t1);
                auto run2    = std::get<0>(t2);
                auto subrun2 = std::get<1>(t2);
                auto event2  = std::get<2>(t2);
                if(run1 < run2) return true;
                if(run1 > run2) return false;
                if(subrun1 < subrun2) return true;
                if(subrun1 > subrun2) return false;
                if(event1 < event2) return true;
                return false;
            });
    unsigned e = 0;
    for(unsigned i=3; i < 5; i++) {
        for(unsigned j=6; j < 9; j++) {
            for(unsigned k=1; k < 5; k++, e++) {
                CPPUNIT_ASSERT_EQUAL(i, (unsigned)std::get<0>(events[e]));
                CPPUNIT_ASSERT_EQUAL(j, (unsigned)std::get<1>(events[e]));
                CPPUNIT_ASSERT_EQUAL(k, (unsigned)std::get<2>(events[e]));
            }
        }
    }
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

void EventSetTest::testPrefetcher() {
    auto root = datastore->root();
    DataSet mds = root["matthieu"];
    CPPUNIT_ASSERT(mds.valid());

    {
        Prefetcher prefetcher(*datastore);
        std::vector<std::tuple<RunNumber, SubRunNumber, EventNumber>> events;
        // iteration target by target
        unsigned i = 0;
        for(int target = 0; target < datastore->numTargets(hepnos::ItemType::EVENT); target++) {
            auto eventset = mds.events(target);
            for(auto it = eventset.begin(prefetcher); it != eventset.end(); it++) {
                auto& ev = *it;
                CPPUNIT_ASSERT(ev.valid());
                i += 1;
                auto ev_number = ev.number();
                auto sr = ev.subrun();
                auto sr_number = sr.number();
                auto r_number = sr.run().number();
                events.emplace_back(r_number, sr_number, ev_number);
            }
        }
        CPPUNIT_ASSERT_EQUAL(2*3*4, (int)i);
        std::sort(events.begin(), events.end(), [](const auto& t1, const auto& t2) {
                auto run1    = std::get<0>(t1);
                auto subrun1 = std::get<1>(t1);
                auto event1  = std::get<2>(t1);
                auto run2    = std::get<0>(t2);
                auto subrun2 = std::get<1>(t2);
                auto event2  = std::get<2>(t2);
                if(run1 < run2) return true;
                if(run1 > run2) return false;
                if(subrun1 < subrun2) return true;
                if(subrun1 > subrun2) return false;
                if(event1 < event2) return true;
                return false;
                });
        unsigned e = 0;
        for(unsigned i=3; i < 5; i++) {
            for(unsigned j=6; j < 9; j++) {
                for(unsigned k=1; k < 5; k++, e++) {
                    CPPUNIT_ASSERT_EQUAL(i, (unsigned)std::get<0>(events[e]));
                    CPPUNIT_ASSERT_EQUAL(j, (unsigned)std::get<1>(events[e]));
                    CPPUNIT_ASSERT_EQUAL(k, (unsigned)std::get<2>(events[e]));
                }
            }
        }
    }
    {
        Prefetcher prefetcher(*datastore);
        // iteration all targets at once
        unsigned i = 0;
        auto eventset = mds.events();
        for(auto it = eventset.begin(prefetcher); it != eventset.end(); it++) {
            auto& ev = *it;
            CPPUNIT_ASSERT(ev.valid());
            i += 1;
        }
        CPPUNIT_ASSERT_EQUAL(2*3*4, (int)i);
    }
}

