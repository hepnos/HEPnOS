#include "LoadStoreVectorsTest.hpp"
#include "CppUnitAdditionalMacros.hpp"
#include "TestObjects.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( LoadStoreVectorsTest );

using namespace hepnos;

void LoadStoreVectorsTest::setUp() {}

void LoadStoreVectorsTest::tearDown() {}

void LoadStoreVectorsTest::testFillDataStore() {
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

void LoadStoreVectorsTest::testLoadStoreDataSet() {
    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(mds.store(key1, out_obj_a));
    // we cannot store at that key again something of the same type
    std::vector<TestObjectA> tmpa(4);
    CPPUNIT_ASSERT(!mds.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(mds.store(key1, out_obj_b));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!mds.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(mds.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == out_obj_a.size());
    for(unsigned i=0; i < std::min(in_obj_a.size(), out_obj_a.size()); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i]);
    }
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(mds.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == out_obj_b.size());
    for(unsigned i=0; i < std::min(in_obj_b.size(), out_obj_b.size()); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i]);
    }
}

void LoadStoreVectorsTest::testLoadStoreRun() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(run.store(key1, out_obj_a));
    // we cannot store at that key again something of the same type
    std::vector<TestObjectA> tmpa(4);
    CPPUNIT_ASSERT(!run.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(run.store(key1, out_obj_b));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!run.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(run.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == out_obj_a.size());
    for(unsigned i=0; i < std::min(in_obj_a.size(), out_obj_a.size()); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i]);
    }
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(run.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == out_obj_b.size());
    for(unsigned i=0; i < std::min(in_obj_b.size(), out_obj_b.size()); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i]);
    }
}

void LoadStoreVectorsTest::testLoadStoreSubRun() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());
    
    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(subrun.store(key1, out_obj_a));
    // we cannot store at that key again something of the same type
    std::vector<TestObjectA> tmpa(4);
    CPPUNIT_ASSERT(!subrun.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(subrun.store(key1, out_obj_b));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!subrun.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == out_obj_a.size());
    for(unsigned i=0; i < std::min(in_obj_a.size(), out_obj_a.size()); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i]);
    }
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == out_obj_b.size());
    for(unsigned i=0; i < std::min(in_obj_b.size(), out_obj_b.size()); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i]);
    }
}

void LoadStoreVectorsTest::testLoadStoreEvent() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykey";

    // we can store obj_a
    CPPUNIT_ASSERT(event.store(key1, out_obj_a));
    // we cannot store at that key again something of the same type
    std::vector<TestObjectA> tmpa(4);
    CPPUNIT_ASSERT(!event.store(key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(event.store(key1, out_obj_b));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    std::string key2 = "otherkey";
    // we can't load something at a key that does not exist
    CPPUNIT_ASSERT(!event.load(key2, in_obj_a));
    // we can reload obj_a from key1
    CPPUNIT_ASSERT(event.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == out_obj_a.size());
    for(unsigned i=0; i < std::min(in_obj_a.size(), out_obj_a.size()); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i]);
    }
    // we can reload obj_b from key1
    CPPUNIT_ASSERT(event.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == out_obj_b.size());
    for(unsigned i=0; i < std::min(in_obj_b.size(), out_obj_b.size()); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i]);
    }
}

void LoadStoreVectorsTest::testLoadStoreDataSetSubVector() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykeyvec";

    // we can store obj_a from index 1 (included) to 5 (excluded)
    CPPUNIT_ASSERT(mds.store(key1, out_obj_a, 1, 5));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(mds.store(key1, out_obj_b, 1, 3));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    // we can reload obj_a from key1 (only the elements we stored)
    CPPUNIT_ASSERT(mds.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == 4);
    for(unsigned i=0; i < in_obj_a.size(); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i+1]);
    }
    // we can reload obj_b from key1 (only the elements we stored)
    CPPUNIT_ASSERT(mds.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == 2);
    for(unsigned i=0; i < in_obj_b.size(); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i+1]);
    }
}

void LoadStoreVectorsTest::testLoadStoreRunSubVector() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykeyvec";

    // we can store obj_a from index 1 (included) to 5 (excluded)
    CPPUNIT_ASSERT(run.store(key1, out_obj_a, 1, 5));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(run.store(key1, out_obj_b, 1, 3));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    // we can reload obj_a from key1 (only the elements we stored)
    CPPUNIT_ASSERT(run.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == 4);
    for(unsigned i=0; i < in_obj_a.size(); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i+1]);
    }
    // we can reload obj_b from key1 (only the elements we stored)
    CPPUNIT_ASSERT(run.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == 2);
    for(unsigned i=0; i < in_obj_b.size(); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i+1]);
    }
}
void LoadStoreVectorsTest::testLoadStoreSubRunSubVector() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykeyvec";

    // we can store obj_a from index 1 (included) to 5 (excluded)
    CPPUNIT_ASSERT(subrun.store(key1, out_obj_a, 1, 5));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(subrun.store(key1, out_obj_b, 1, 3));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    // we can reload obj_a from key1 (only the elements we stored)
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == 4);
    for(unsigned i=0; i < in_obj_a.size(); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i+1]);
    }
    // we can reload obj_b from key1 (only the elements we stored)
    CPPUNIT_ASSERT(subrun.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == 2);
    for(unsigned i=0; i < in_obj_b.size(); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i+1]);
    }
}
void LoadStoreVectorsTest::testLoadStoreEventSubVector() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> out_obj_a(10);
    for(unsigned i=0; i < 10; i++) {
        out_obj_a[i].x() = 44+i;
        out_obj_a[i].y() = 1.2+i;
    }
    std::vector<TestObjectB> out_obj_b(5);
    for(unsigned i=0; i < 5; i++) {
        out_obj_b[i].a() = 33+i;
        out_obj_b[i].b() = "you" + std::to_string(i);
    }
    std::string key1 = "mykeyvec";

    // we can store obj_a from index 1 (included) to 5 (excluded)
    CPPUNIT_ASSERT(event.store(key1, out_obj_a, 1, 5));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(event.store(key1, out_obj_b, 1, 3));

    std::vector<TestObjectA> in_obj_a;
    std::vector<TestObjectB> in_obj_b;

    // we can reload obj_a from key1 (only the elements we stored)
    CPPUNIT_ASSERT(event.load(key1, in_obj_a));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_a.size() == 4);
    for(unsigned i=0; i < in_obj_a.size(); i++) {
        CPPUNIT_ASSERT(in_obj_a[i] == out_obj_a[i+1]);
    }
    // we can reload obj_b from key1 (only the elements we stored)
    CPPUNIT_ASSERT(event.load(key1, in_obj_b));
    // and they are the same
    CPPUNIT_ASSERT(in_obj_b.size() == 2);
    for(unsigned i=0; i < in_obj_b.size(); i++) {
        CPPUNIT_ASSERT(in_obj_b[i] == out_obj_b[i+1]);
    }
}
