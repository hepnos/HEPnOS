#include <algorithm>
#include <thallium.hpp>
#include "QueueTest.hpp"
#include "TestObjects.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( QueueTest );

namespace tl = thallium;
using namespace hepnos;

void QueueTest::setUp() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0) {
        datastore->createQueue<TestObjectB>("queue_b");
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

void QueueTest::tearDown() {
    MPI_Barrier(MPI_COMM_WORLD);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0) {
        datastore->destroyQueue<TestObjectB>("queue_b");
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

void QueueTest::testQueueCreate() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0) {
        // different types and name than the one already created
        CPPUNIT_ASSERT_NO_THROW(
            datastore->createQueue<TestObjectA>("queue_a"));
        // same type as above, but different name
        CPPUNIT_ASSERT_NO_THROW(
            datastore->createQueue<TestObjectA>("other_queue_a"));
        // same type and name as the one already created in setUp
        CPPUNIT_ASSERT_THROW(
            datastore->createQueue<TestObjectB>("queue_b"),
            hepnos::Exception);
        // destroy queue_a
        CPPUNIT_ASSERT_NO_THROW(
            datastore->destroyQueue<TestObjectA>("queue_a"));
        // destroy other_queue_a
        CPPUNIT_ASSERT_NO_THROW(
            datastore->destroyQueue<TestObjectA>("other_queue_a"));
    }
}

void QueueTest::testQueueOpen() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    auto mode = rank < 2 ?
        hepnos::QueueAccessMode::PRODUCER : hepnos::QueueAccessMode::CONSUMER;
    hepnos::Queue queue;
    // open a queue that exists
    CPPUNIT_ASSERT_NO_THROW(
        queue = datastore->openQueue<TestObjectB>("queue_b", mode));
    // close it
    CPPUNIT_ASSERT_NO_THROW(queue.close());
    // wrong type
    CPPUNIT_ASSERT_THROW(
        queue = datastore->openQueue<TestObjectA>("queue_b", mode),
        hepnos::Exception);
    // wrong name
    CPPUNIT_ASSERT_THROW(
        queue = datastore->openQueue<TestObjectA>("queue_b", mode),
        hepnos::Exception);
}

void QueueTest::testQueueEmpty() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank != 0) return;
    hepnos::Queue queue;
    // open a queue that exists
    CPPUNIT_ASSERT_NO_THROW(
        queue = datastore->openQueue<TestObjectB>(
            "queue_b", hepnos::QueueAccessMode::PRODUCER));
    // check that it's empty
    CPPUNIT_ASSERT(queue.empty());
    // push an object in it
    TestObjectB obj;
    queue.push(obj);
    // check that the queue isn't empty
    CPPUNIT_ASSERT(!queue.empty());
    // consume the object
    queue.pop(obj);
    // check that the queue is empty
    CPPUNIT_ASSERT(queue.empty());
}
