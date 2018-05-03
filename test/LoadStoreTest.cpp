#include "LoadStoreTest.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( LoadStoreTest );

using namespace hepnos;

void LoadStoreTest::setUp() {}

void LoadStoreTest::tearDown() {}

void LoadStoreTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
    CPPUNIT_ASSERT(sr1.valid());
    Event ev1 = sr1.createEvent(22);
    CPPUNIT_ASSERT(ev1.valid());
}

void LoadStoreTest::testLoadStoreDataSet() {

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
    // we cannot store at that key again something of the same type
    TestObjectA tmpa;
    CPPUNIT_ASSERT(!mds.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(mds.store(key1, out_obj_b));

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!mds.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(mds.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(mds.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

void LoadStoreTest::testLoadStoreRun() {

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
    // we cannot store at that key again something of the same type
    TestObjectA tmpa;
    CPPUNIT_ASSERT(!run.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(run.store(key1, out_obj_b));

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!run.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(run.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(run.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

void LoadStoreTest::testLoadStoreSubRun() {

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
    // we cannot store at that key again something of the same type
    TestObjectA tmpa;
    CPPUNIT_ASSERT(!subrun.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(subrun.store(key1, out_obj_b));

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!subrun.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

void LoadStoreTest::testLoadStoreEvent() {

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
    // we cannot store at that key again something of the same type
    TestObjectA tmpa;
    CPPUNIT_ASSERT(!event.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(event.store(key1, out_obj_b));

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!event.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(event.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(event.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

