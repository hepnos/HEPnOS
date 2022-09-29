#include "LoadStoreTest.hpp"
#include "CppUnitAdditionalMacros.hpp"
#include "TestObjects.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( LoadStoreTest );

using namespace hepnos;

void LoadStoreTest::setUp() {}

void LoadStoreTest::tearDown() {}

void LoadStoreTest::testFillDataStore() {

    auto root = datastore->root();
    auto ds1 = root.createDataSet("matthieu");
    CPPUNIT_ASSERT(ds1.valid());
    Run r1 = ds1.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
    CPPUNIT_ASSERT(sr1.valid());
    Event ev1 = sr1.createEvent(22);
    CPPUNIT_ASSERT(ev1.valid());
    DataSet ds2 = root.createDataSet("matthieu_async");
    CPPUNIT_ASSERT(ds2.valid());
    Run r2 = ds2.createRun(42);
    CPPUNIT_ASSERT(r2.valid());
    SubRun sr2 = r2.createSubRun(3);
    CPPUNIT_ASSERT(sr2.valid());
    Event ev2 = sr2.createEvent(22);
    CPPUNIT_ASSERT(ev2.valid());
}

void LoadStoreTest::testLoadStoreDataSet() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
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
    // TestObjectA tmpa;
    // CPPUNIT_ASSERT(!mds.store(key1, tmpa));
    // NOTE: HEPnOS now allows overwritting
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

    auto root = datastore->root();
    auto mds = root["matthieu"];
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
    // TestObjectA tmpa;
    // CPPUNIT_ASSERT(!run.store(key1, tmpa));
    // NOTE: HEPnOS now allows overwritting
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

    auto root = datastore->root();
    auto mds = root["matthieu"];
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
    // TestObjectA tmpa;
    // CPPUNIT_ASSERT(!subrun.store(key1, tmpa));
    // NOTE: HEPnOS now allows overwritting
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

    auto root = datastore->root();
    auto mds = root["matthieu"];
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
    // TestObjectA tmpa;
    // CPPUNIT_ASSERT(!event.store(key1, tmpa));
    // NOTE: HEPnOS now allows overwritting
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

void LoadStoreTest::testListProducts() {

    auto root = datastore->root();
    auto mds = root["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun.createEvent(23);
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
    std::string key1 = "testkeyA";
    std::string key2 = "testkeyB";

    ProductID id_A_key1 = event.store(key1, out_obj_a);
    ProductID id_A_key2 = event.store(key2, out_obj_a);
    ProductID id_B_key1 = event.store(key1, out_obj_b);

    std::vector<ProductID> all_products
        = event.listProducts();
    CPPUNIT_ASSERT(all_products.size() == 3);
    CPPUNIT_ASSERT(std::find(all_products.begin(), all_products.end(), id_A_key1) != all_products.end());
    CPPUNIT_ASSERT(std::find(all_products.begin(), all_products.end(), id_A_key2) != all_products.end());
    CPPUNIT_ASSERT(std::find(all_products.begin(), all_products.end(), id_B_key1) != all_products.end());

    std::vector<ProductID> products_key1
        = event.listProducts(key1);
    CPPUNIT_ASSERT(products_key1.size() == 2);
    CPPUNIT_ASSERT(std::find(products_key1.begin(), products_key1.end(), id_A_key1) != products_key1.end());
    CPPUNIT_ASSERT(std::find(products_key1.begin(), products_key1.end(), id_B_key1) != products_key1.end());

    std::vector<ProductID> no_product
        = event.listProducts("bla");
    CPPUNIT_ASSERT(no_product.empty());
}

// Async Tests

void LoadStoreTest::testAsyncLoadStoreDataSet() {

    auto root = datastore->root();
    auto mds = root["matthieu_async"];
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

    hepnos::AsyncEngine async(*datastore, 1);

    // we can store obj_a
    CPPUNIT_ASSERT(mds.store(async, key1, out_obj_a));
    // we cannot store at that key again something of the same type,
    // but we will know that only when checking the async engine
    // for errors
    // TestObjectA tmpa;
    // CPPUNIT_ASSERT(mds.store(async, key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(mds.store(async, key1, out_obj_b));

    async.wait();
    // there should be one error logged
    // CPPUNIT_ASSERT(async.errors().size() == 1);
    CPPUNIT_ASSERT(async.errors().size() == 0);

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

void LoadStoreTest::testAsyncLoadStoreRun() {

    auto root = datastore->root();
    auto mds = root["matthieu_async"];
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

    hepnos::AsyncEngine async(*datastore, 1);
    // we can store obj_a
    CPPUNIT_ASSERT(run.store(async, key1, out_obj_a));
    // we cannot store at that key again something of the same type
    // but we will know about it only later when checking for errors
    //TestObjectA tmpa;
    //CPPUNIT_ASSERT(run.store(async, key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(run.store(async, key1, out_obj_b));

    async.wait();
    // there should be one error logged
    //CPPUNIT_ASSERT(async.errors().size() == 1);
    CPPUNIT_ASSERT(async.errors().size() == 0);

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

void LoadStoreTest::testAsyncLoadStoreSubRun() {

    auto root = datastore->root();
    auto mds = root["matthieu_async"];
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

    hepnos::AsyncEngine async(*datastore, 1);
    // we can store obj_a
    CPPUNIT_ASSERT(subrun.store(async, key1, out_obj_a));
    // we cannot store at that key again something of the same type
    // but we will know about it only when checking for errors
    //TestObjectA tmpa;
    //CPPUNIT_ASSERT(subrun.store(async, key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(subrun.store(async, key1, out_obj_b));

    async.wait();
    // there should be one error logged
    //CPPUNIT_ASSERT(async.errors().size() == 1);
    CPPUNIT_ASSERT(async.errors().size() == 0);

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

void LoadStoreTest::testAsyncLoadStoreEvent() {

    auto root = datastore->root();
    auto mds = root["matthieu_async"];
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

    hepnos::AsyncEngine async(*datastore, 1);
    // we can store obj_a
    CPPUNIT_ASSERT(event.store(async, key1, out_obj_a));
    // we cannot store at that key again something of the same type
    // but we will know about it later when checking for errors
    //TestObjectA tmpa;
    //CPPUNIT_ASSERT(event.store(async, key1, tmpa));
    // we can store obj_b at the same key because it's not the same type
    CPPUNIT_ASSERT(event.store(async, key1, out_obj_b));

    async.wait();
    //CPPUNIT_ASSERT(async.errors().size() == 1);
    CPPUNIT_ASSERT(async.errors().size() == 0);

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

void LoadStoreTest::testPrefetchLoadStore() {
    auto root = datastore->root();
    auto mds = root.createDataSet("prefetch_run");
    std::string label = "key";
    {
        TestObjectA obj_a;
        TestObjectB obj_b;
        for(unsigned i = 0; i < 10; i++) {
            obj_a.x() = i;
            obj_a.y() = 2*i;
            obj_b.a() = 3*i;
            obj_b.b() = "matthieu";
            auto r = mds.createRun(i);
            CPPUNIT_ASSERT(r.valid());
            r.store(label, obj_a);
            r.store(label, obj_b);
        }
    }
    {
        // iterate through the dataset with a prefetcher
        Prefetcher p(*datastore);
        p.fetchProduct<TestObjectA>(label);
        p.fetchProduct<TestObjectB>(label);
        unsigned i = 0;
        for(auto& run : p(mds.runs())) {
            TestObjectA obj_a;
            TestObjectB obj_b;
            run.load(p, label, obj_a);
            run.load(p, label, obj_b);
            CPPUNIT_ASSERT(obj_a.x() == i);
            CPPUNIT_ASSERT(obj_a.y() == 2*i);
            CPPUNIT_ASSERT(obj_b.a() == 3*i);
            CPPUNIT_ASSERT(obj_b.b() == "matthieu");
            i += 1;
        }
    }
}

void LoadStoreTest::testAsyncPrefetchLoadStore() {
    auto root = datastore->root();
    auto mds = root.createDataSet("prefetch_run");
    std::string label = "key";
    {
        TestObjectA obj_a;
        TestObjectB obj_b;
        for(unsigned i = 0; i < 20; i++) {
            obj_a.x() = i;
            obj_a.y() = 2*i;
            obj_b.a() = 3*i;
            obj_b.b() = "matthieu";
            auto r = mds.createRun(i);
            CPPUNIT_ASSERT(r.valid());
            r.store(label, obj_a);
            r.store(label, obj_b);
        }
    }
    {
        // iterate through the dataset with a prefetcher
        AsyncEngine async(*datastore, 1);
        Prefetcher p(async);
        p.fetchProduct<TestObjectA>(label);
        p.fetchProduct<TestObjectB>(label);
        unsigned i = 0;
        for(auto& run : p(mds.runs())) {
            TestObjectA obj_a;
            TestObjectB obj_b;
            run.load(p, label, obj_a);
            run.load(p, label, obj_b);
            CPPUNIT_ASSERT(obj_a.x() == i);
            CPPUNIT_ASSERT(obj_a.y() == 2*i);
            CPPUNIT_ASSERT(obj_b.a() == 3*i);
            CPPUNIT_ASSERT(obj_b.b() == "matthieu");
            i += 1;
        }
    }
}
