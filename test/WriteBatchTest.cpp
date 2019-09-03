#include "WriteBatchTest.hpp"
#include "CppUnitAdditionalMacros.hpp"
#include "TestObjects.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( WriteBatchTest );

using namespace hepnos;

void WriteBatchTest::setUp() {}

void WriteBatchTest::tearDown() {}

void WriteBatchTest::testFillDataStore() {
    /*
    for(auto i = 10; i < 30; i++)
        datastore->createDataSet("matthieu" + std::to_string(i));
    auto ds = (*datastore)["matthieu15"];
    CPPUNIT_ASSERT(ds.valid());
    for(auto i = 0; i < 20; i++)
        ds.createRun(i);
    auto r = ds[12];
    CPPUNIT_ASSERT(r.valid());
    for(auto i = 0; i < 20; i++)
        r.createSubRun(i);
    auto sr = r[5];
    CPPUNIT_ASSERT(sr.valid());
    for(auto i = 0; i < 20; i++)
        sr.createEvent(i);
    auto ev = sr[2];
    CPPUNIT_ASSERT(ev.valid());
    */
}

void WriteBatchTest::testWriteBatchDataSet() {

    TestObjectA out_obj_a;
    out_obj_a.x() = 44;
    out_obj_a.y() = 1.2;
    TestObjectB out_obj_b;
    out_obj_b.a() = 33;
    out_obj_b.b() = "you";
    std::string key1 = "mykey";

    {
        hepnos::WriteBatch batch(*datastore);

        for(auto i = 10; i < 30; i++) {
            std::string dsname = "testWriteBatchDataSet"+std::to_string(i);
            auto ds = datastore->createDataSet(batch, dsname);
            CPPUNIT_ASSERT(ds.store(batch, key1, out_obj_a));
            CPPUNIT_ASSERT(ds.store(batch, key1, out_obj_b));
        }
    }

    TestObjectA in_obj_a;
    TestObjectB in_obj_b;

    for(auto i = 10; i < 30; i++) {
        std::string dsname = "testWriteBatchDataSet"+std::to_string(i);
        auto ds = (*datastore)[dsname];
        CPPUNIT_ASSERT(ds.valid());
        CPPUNIT_ASSERT(ds.load(key1, in_obj_a));
        CPPUNIT_ASSERT(ds.load(key1, in_obj_b));
        CPPUNIT_ASSERT(out_obj_a == in_obj_a);
        CPPUNIT_ASSERT(out_obj_b == in_obj_b);
    }
}

void WriteBatchTest::testWriteBatchRun() {

}

void WriteBatchTest::testWriteBatchSubRun() {

}

void WriteBatchTest::testWriteBatchEvent() {

}
