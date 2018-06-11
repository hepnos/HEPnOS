#include "WriteAndRestartTest.hpp"
#include "CppUnitAdditionalMacros.hpp"
#include "TestObjects.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( WriteAndRestartTest );

using namespace hepnos;

void WriteAndRestartTest::setUp() {}

void WriteAndRestartTest::tearDown() {}

void WriteAndRestartTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
    CPPUNIT_ASSERT(sr1.valid());
    Event ev1 = sr1.createEvent(22);
    CPPUNIT_ASSERT(ev1.valid());
}

void WriteAndRestartTest::testStoreDataSet() {

    auto mds = (*datastore)["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(mds.store(key1, out_obj_a));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(mds.store(key1, out_obj_b));
}

void WriteAndRestartTest::testStoreRun() {

    auto mds = (*datastore)["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(run.store(key1, out_obj_a));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(run.store(key1, out_obj_b));
}

void WriteAndRestartTest::testStoreSubRun() {

    auto mds = (*datastore)["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(subrun.store(key1, out_obj_a));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(subrun.store(key1, out_obj_b));
}

void WriteAndRestartTest::testStoreEvent() {

    auto mds = (*datastore)["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(event.store(key1, out_obj_a));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(event.store(key1, out_obj_b));
}

