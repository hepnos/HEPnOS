#include <map>
#include <boost/serialization/map.hpp>
#include "PtrTest.hpp"
#include "CppUnitAdditionalMacros.hpp"
#include "TestObjects.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( PtrTest );

using namespace hepnos;

void PtrTest::setUp() {}

void PtrTest::tearDown() {}

void PtrTest::testFillDataStore() {

    auto mds = datastore->createDataSet("matthieu");
    CPPUNIT_ASSERT(mds.valid());
    Run r1 = mds.createRun(42);
    CPPUNIT_ASSERT(r1.valid());
    SubRun sr1 = r1.createSubRun(3);
    CPPUNIT_ASSERT(sr1.valid());
    Event ev1 = sr1.createEvent(22);
    CPPUNIT_ASSERT(ev1.valid());
}

void PtrTest::testMakePtr() {

    auto mds = (*datastore)["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    TestObjectA objA;
    objA.x() = 44;
    objA.y() = 1.2;
    TestObjectB objB;
    objB.a() = 33;
    objB.b() = "you";
    std::string key1 = "mykey";

    // we store obj_a
    auto objA_product_id = event.store(key1, objA);
    CPPUNIT_ASSERT(objA_product_id.valid());
    // we store obj_b
    auto objB_product_id = subrun.store(key1, objB);
    CPPUNIT_ASSERT(objB_product_id.valid());

    // we make a pointer to object A
    auto ptrA = datastore->makePtr<TestObjectA>(objA_product_id);
    CPPUNIT_ASSERT(ptrA.valid());

    // we make a pointer to object B
    auto ptrB = datastore->makePtr<TestObjectB>(objB_product_id);
    CPPUNIT_ASSERT(ptrB.valid());

    // store pointer
    std::string keyPtrA = "mypointerA";
    std::string keyPtrB = "mypointerB";
    CPPUNIT_ASSERT(run.store(keyPtrA, ptrA));
    CPPUNIT_ASSERT(run.store(keyPtrB, ptrB));
}

void PtrTest::testPtrLoad() {
    auto mds = (*datastore)["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    TestObjectA objA;
    objA.x() = 44;
    objA.y() = 1.2;
    TestObjectB objB;
    objB.a() = 33;
    objB.b() = "you";

    Ptr<TestObjectA> ptrA;
    Ptr<TestObjectB> ptrB;
    std::string keyPtrA = "mypointerA";
    std::string keyPtrB = "mypointerB";

    CPPUNIT_ASSERT(run.load(keyPtrA, ptrA));
    CPPUNIT_ASSERT(run.load(keyPtrB, ptrB));

    CPPUNIT_ASSERT(objA == *ptrA);
    CPPUNIT_ASSERT(objB == *ptrB);
}

void PtrTest::testPtrLoadFromArray() {

    auto mds = (*datastore)["matthieu"];
    auto run = mds[42];
    auto subrun = run[3];
    auto event = subrun[22];
    CPPUNIT_ASSERT(mds.valid());
    CPPUNIT_ASSERT(run.valid());
    CPPUNIT_ASSERT(subrun.valid());
    CPPUNIT_ASSERT(event.valid());

    std::vector<TestObjectA> vecA(3);
    vecA[0].x() = 43;
    vecA[0].y() = 4.3;
    vecA[1].x() = 44;
    vecA[1].y() = 4.4;
    vecA[2].x() = 45;
    vecA[2].y() = 4.5;

    auto prodIDvec = event.store("somekey", vecA);

    auto ptrToVecA = datastore->makePtr<TestObjectA>(prodIDvec,1);
    
    CPPUNIT_ASSERT(vecA[1] == *ptrToVecA);
   
    std::map<unsigned,TestObjectA> mapA;
    mapA[0].x() = 43;
    mapA[0].y() = 4.3;
    mapA[1].x() = 44;
    mapA[1].y() = 4.4;
    mapA[2].x() = 45;
    mapA[2].y() = 4.5;

    auto prodIDmap = event.store("somekey", mapA);

    auto ptrToMapA = datastore->makePtr<TestObjectA,std::map<unsigned,TestObjectA>>(prodIDmap,1);

    CPPUNIT_ASSERT(mapA[1] == *ptrToMapA);
}
