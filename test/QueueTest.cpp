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
        datastore->createQueue<TestObjectA>("queue_a");
        datastore->createQueue<TestObjectB>("queue_b");
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

void QueueTest::tearDown() {
    MPI_Barrier(MPI_COMM_WORLD);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0) {
        datastore->destroyQueue<TestObjectA>("queue_a");
        datastore->destroyQueue<TestObjectB>("queue_b");
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

void QueueTest::testQueue() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    auto mode = rank < 2 ?
        hepnos::QueueAccessMode::PRODUCER : hepnos::QueueAccessMode::CONSUMER;
    auto queue = datastore->openQueue<TestObjectA>("queue_a", mode);
}

