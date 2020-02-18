#include "RestartAndReadTest.hpp"
#include "CppUnitAdditionalMacros.hpp"
#include "TestObjects.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( RestartAndReadTest );

using namespace hepnos;

void RestartAndReadTest::setUp() {}

void RestartAndReadTest::tearDown() {}

void RestartAndReadTest::testLoadDataSet() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    // initialized for comparison
    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    // we can reload obj_a from key1
    CPPUNIT_ASSERT(mds.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(mds.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

void RestartAndReadTest::testLoadRun() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    // For comparison
    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    // we can reload obj_a from key1
    CPPUNIT_ASSERT(run.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(run.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

void RestartAndReadTest::testLoadSubRun() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    // For comparison
    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    // we can reload obj_a from key1
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

void RestartAndReadTest::testLoadEvent() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    // For comparison
    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    // we can reload obj_a from key1
    CPPUNIT_ASSERT(event.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a == out_obj_a);
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(event.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b == out_obj_b);
}

