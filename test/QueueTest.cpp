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

void QueueTest::testQueuePushPop() {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    hepnos::Queue queue;
    int num_reads = 0;
    if(rank < 2) { // producers
        // open a queue
        CPPUNIT_ASSERT_NO_THROW(
            queue = datastore->openQueue<TestObjectB>(
                "queue_b", hepnos::QueueAccessMode::PRODUCER));
        MPI_Barrier(MPI_COMM_WORLD);
        // initialize data
        std::vector<TestObjectB> vec(10);
        for(int i = 0; i < vec.size(); ++i) {
            vec[i].a() = i;
        }
        // push data
        for(int i = 0; i < vec.size(); ++i) {
            queue.push(vec[i]);
        }
    } else { // consumers
        // open a queue
        CPPUNIT_ASSERT_NO_THROW(
            queue = datastore->openQueue<TestObjectB>(
                "queue_b", hepnos::QueueAccessMode::CONSUMER));
        MPI_Barrier(MPI_COMM_WORLD);
        // consume
        std::vector<TestObjectB> vec;
        TestObjectB b;
        while(queue.pop(b)) {
            vec.push_back(b);
        }
        num_reads = vec.size();
    }
    // close the queue
    CPPUNIT_ASSERT_NO_THROW(queue.close());
    int total_reads = 0;
    MPI_Allreduce(&num_reads, &total_reads, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    CPPUNIT_ASSERT(total_reads == 20);
    MPI_Barrier(MPI_COMM_WORLD);
}
