#include "AsyncWriteBatchTest.hpp"
#include "CppUnitAdditionalMacros.hpp"
#include "TestObjects.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( AsyncWriteBatchTest );

using namespace hepnos;

void AsyncWriteBatchTest::setUp() {}

void AsyncWriteBatchTest::tearDown() {}

void AsyncWriteBatchTest::testAsyncWriteBatchRun() {
    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    auto dataset = datastore->root().createDataSet("testAsyncWriteBatchRun");
    hepnos::AsyncEngine async_engine(*datastore, 2);

    {
        hepnos::WriteBatch batch(*datastore, async_engine);

        for(auto i = 0; i < 5; i++) {
            auto run = dataset.createRun(batch, i);
            CPPUNIT_ASSERT(run.store(batch, key1, out_obj_a));
            CPPUNIT_ASSERT(run.store(batch, key1, out_obj_b));
        }
    }
    async_engine.wait(); // useless, but just to test
    {
        hepnos::WriteBatch batch(*datastore, async_engine);

        for(auto i = 5; i < 10; i++) {
            auto run = dataset.createRun(batch, i);
            CPPUNIT_ASSERT(run.store(batch, key1, out_obj_a));
            CPPUNIT_ASSERT(run.store(batch, key1, out_obj_b));
        }
    }

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    for(auto i = 0; i < 10; i++) {
        auto run = dataset.runs()[i];
        CPPUNIT_ASSERT(run.valid());
        CPPUNIT_ASSERT(run.load(key1, in_obj_a));
        CPPUNIT_ASSERT(run.load(key1, in_obj_b));
        CPPUNIT_ASSERT(out_obj_a == in_obj_a);
        CPPUNIT_ASSERT(out_obj_b == in_obj_b);
    }
}

void AsyncWriteBatchTest::testAsyncWriteBatchSubRun() {
    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    auto dataset = datastore->root().createDataSet("testAsyncWriteBatchSubRun");
    hepnos::AsyncEngine async_engine(*datastore, 2);
    auto run = dataset.createRun(42);

    {
        hepnos::WriteBatch batch(*datastore, async_engine);

        for(auto i = 0; i < 10; i++) {
            auto sr = run.createSubRun(batch, i);
            CPPUNIT_ASSERT(sr.store(batch, key1, out_obj_a));
            CPPUNIT_ASSERT(sr.store(batch, key1, out_obj_b));
        }
    }

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    for(auto i = 0; i < 10; i++) {
        auto sr = run[i];
        CPPUNIT_ASSERT(sr.valid());
        CPPUNIT_ASSERT(sr.load(key1, in_obj_a));
        CPPUNIT_ASSERT(sr.load(key1, in_obj_b));
        CPPUNIT_ASSERT(out_obj_a == in_obj_a);
        CPPUNIT_ASSERT(out_obj_b == in_obj_b);
    }
}

void AsyncWriteBatchTest::testAsyncWriteBatchEvent() {
    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    auto dataset = datastore->root().createDataSet("testAsyncWriteBatchEvent");
    hepnos::AsyncEngine async_engine(*datastore, 2);
    auto run = dataset.createRun(42);
    auto subrun = run.createSubRun(2);

    {
        hepnos::WriteBatch batch(*datastore, async_engine);

        for(auto i = 0; i < 10; i++) {
            auto e = subrun.createEvent(batch, i);
            CPPUNIT_ASSERT(e.store(batch, key1, out_obj_a));
            CPPUNIT_ASSERT(e.store(batch, key1, out_obj_b));
        }
    }

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    for(auto i = 0; i < 10; i++) {
        auto e = subrun[i];
        CPPUNIT_ASSERT(e.valid());
        CPPUNIT_ASSERT(e.load(key1, in_obj_a));
        CPPUNIT_ASSERT(e.load(key1, in_obj_b));
        CPPUNIT_ASSERT(out_obj_a == in_obj_a);
        CPPUNIT_ASSERT(out_obj_b == in_obj_b);
    }
}
