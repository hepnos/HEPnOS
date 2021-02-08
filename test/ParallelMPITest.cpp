#include <algorithm>
#include <thallium.hpp>
#include "ParallelMPITest.hpp"
#include "TestObjects.hpp"
#include "CppUnitAdditionalMacros.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION( ParallelMPITest );

namespace tl = thallium;
using namespace hepnos;

void ParallelMPITest::setUp() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    auto mds = datastore->root().createDataSet("matthieu");
    auto run = mds.createRun(rank);
    for(unsigned j = 0; j < 8; j++) {
        auto subrun = run.createSubRun(j);
        for(unsigned k = 0; k < 8; k++) {
            TestObjectA a;
            a.x() = k;
            a.y() = k*2.0;
            TestObjectB b;
            b.a() = k;
            auto ev = subrun.createEvent(k);
            ev.store("abc", a);
            ev.store("abc", b);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

void ParallelMPITest::tearDown() {}

struct item {

    uint64_t run    = 0;
    uint64_t subrun = 0;
    uint64_t event  = 0;

    item(uint64_t r, uint64_t sr, uint64_t e)
    : run(r), subrun(sr), event(e) {}

    item() = default;

    bool operator<(const item& other) const {
        if(run < other.run) return true;
        if(run > other.run) return false;
        if(subrun < other.subrun) return true;
        if(subrun > other.subrun) return false;
        return event < other.event;
    }

    bool operator==(const item& other) const {
        return run == other.run
            && subrun == other.subrun
            && event == other.event;
    }
};

void ParallelMPITest::testParallelEventProcessor() {
    auto mds = datastore->root()["matthieu"];

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    ParallelEventProcessorStatistics stats;
    ParallelEventProcessor parallel_processor(*datastore, MPI_COMM_WORLD);
    std::vector<item> items;
    parallel_processor.process(mds,
        [&items, rank](const Event& ev) {
            SubRun sr = ev.subrun();
            Run r = sr.run();
            std::cout << "Rank " << rank << " invoking lambda for item " <<
                r.number() << " " << sr.number() << " " << ev.number() << std::endl;
            items.emplace_back(r.number(), sr.number(), ev.number());
            double t = tl::timer::wtime();
            while(tl::timer::wtime() - t < 0.1) {}
        },
        &stats
    );

    std::cout << "Rank " << rank << " statistics:\n"
        << "  total_events_processed = " << stats.total_events_processed << "\n"
        << "  local_events_processed = " << stats.local_events_processed << "\n"
        << "  total_time = " << stats.total_time << "\n"
        << "  acc_event_processing_time = " << stats.acc_event_processing_time << "\n"
        << "  acc_product_loading_time = " << stats.acc_product_loading_time << "\n"
        << "  processing_time_stats = " << stats.processing_time_stats << "\n"
        << "  waiting_time_stats = " << stats.waiting_time_stats << std::endl;

    if(rank != 0) {
        int num_local_items = items.size();
        MPI_Send(&num_local_items, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        if(num_local_items) {
            MPI_Send(items.data(), items.size()*sizeof(item), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
        }
    } else {
        for(unsigned j=1; j < size; j++) {
            int num_items = 0;
            MPI_Recv(&num_items, 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            items.resize(items.size() + num_items);
            if(num_items) {
                MPI_Recv(&items[items.size() - num_items], sizeof(item)*num_items,
                    MPI_BYTE, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
        std::sort(items.begin(), items.end());
        CPPUNIT_ASSERT(items.size() == size*8*8);
        unsigned x = 0;
        for(unsigned i = 0; i < (unsigned)size; i++) {
            for(unsigned j = 0; j < 8; j++) {
                for(unsigned k = 0; k < 8; k++) {
                    auto& e = items[x];
                    CPPUNIT_ASSERT(e.run == i && e.subrun == j && e.event == k);
                    x += 1;
                }
            }
        }
    }
}

void ParallelMPITest::testParallelEventProcessorAsync() {
    auto mds = datastore->root()["matthieu"];

    AsyncEngine async(*datastore, 2);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    ParallelEventProcessorStatistics stats;
    ParallelEventProcessor parallel_processor(async, MPI_COMM_WORLD);

    std::vector<item> items;
    tl::mutex         item_mtx;

    parallel_processor.process(mds,
        [&items, &item_mtx, rank](const Event& ev) {
            SubRun sr = ev.subrun();
            Run r = sr.run();
            std::cout << "Rank " << rank << " ES " << tl::xstream::self().get_rank() << " invoking lambda for item " <<
                r.number() << " " << sr.number() << " " << ev.number() << std::endl;
            {
                std::lock_guard<tl::mutex> lock(item_mtx);
                items.emplace_back(r.number(), sr.number(), ev.number());
            }
            double t = tl::timer::wtime();
            while(tl::timer::wtime() - t < 0.1) {
                tl::thread::yield();
            }
        },
        &stats
    );

    std::cout << "Rank " << rank << " statistics:\n"
        << "  total_events_processed = " << stats.total_events_processed << "\n"
        << "  local_events_processed = " << stats.local_events_processed << "\n"
        << "  total_time = " << stats.total_time << "\n"
        << "  acc_event_processing_time = " << stats.acc_event_processing_time << "\n"
        << "  acc_product_loading_time = " << stats.acc_product_loading_time << "\n"
        << "  processing_time_stats = " << stats.processing_time_stats << "\n"
        << "  waiting_time_stats = " << stats.waiting_time_stats << std::endl;

    if(rank != 0) {
        int num_local_items = items.size();
        MPI_Send(&num_local_items, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        if(num_local_items) {
            MPI_Send(items.data(), items.size()*sizeof(item), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
        }
    } else {
        for(unsigned j=1; j < size; j++) {
            int num_items = 0;
            MPI_Recv(&num_items, 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            items.resize(items.size() + num_items);
            if(num_items) {
                MPI_Recv(&items[items.size() - num_items], sizeof(item)*num_items,
                    MPI_BYTE, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
        std::sort(items.begin(), items.end());
        CPPUNIT_ASSERT(items.size() == size*8*8);
        unsigned x = 0;
        for(unsigned i = 0; i < (unsigned)size; i++) {
            for(unsigned j = 0; j < 8; j++) {
                for(unsigned k = 0; k < 8; k++) {
                    auto& e = items[x];
                    CPPUNIT_ASSERT(e.run == i && e.subrun == j && e.event == k);
                    x += 1;
                }
            }
        }
    }
}

void ParallelMPITest::testParallelEventProcessorWithProducts() {
    auto mds = datastore->root()["matthieu"];

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    ParallelEventProcessorStatistics stats;
    ParallelEventProcessor parallel_processor(*datastore, MPI_COMM_WORLD);
    parallel_processor.preload<TestObjectA>("abc");

    std::vector<item> items;
    parallel_processor.process(mds,
        [&items, rank](const Event& ev, const ProductCache& cache) {
            SubRun sr = ev.subrun();
            Run r = sr.run();
            std::cout << "Rank " << rank << " invoking lambda for item " <<
                r.number() << " " << sr.number() << " " << ev.number() << std::endl;
            items.emplace_back(r.number(), sr.number(), ev.number());
            TestObjectA a;
            TestObjectB b;
            CPPUNIT_ASSERT(ev.load(cache, "abc", a));
            CPPUNIT_ASSERT(!ev.load(cache, "abc", b));
            CPPUNIT_ASSERT(a.x() == ev.number());
            double t = tl::timer::wtime();
            while(tl::timer::wtime() - t < 0.1) {}
        },
        &stats
    );

    std::cout << "Rank " << rank << " statistics:\n"
        << "  total_events_processed = " << stats.total_events_processed << "\n"
        << "  local_events_processed = " << stats.local_events_processed << "\n"
        << "  total_time = " << stats.total_time << "\n"
        << "  acc_event_processing_time = " << stats.acc_event_processing_time << "\n"
        << "  acc_product_loading_time = " << stats.acc_product_loading_time << "\n"
        << "  processing_time_stats = " << stats.processing_time_stats << "\n"
        << "  waiting_time_stats = " << stats.waiting_time_stats << std::endl;

    if(rank != 0) {
        int num_local_items = items.size();
        MPI_Send(&num_local_items, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        if(num_local_items) {
            MPI_Send(items.data(), items.size()*sizeof(item), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
        }
    } else {
        for(unsigned j=1; j < size; j++) {
            int num_items = 0;
            MPI_Recv(&num_items, 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            items.resize(items.size() + num_items);
            if(num_items) {
                MPI_Recv(&items[items.size() - num_items], sizeof(item)*num_items,
                    MPI_BYTE, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
        std::sort(items.begin(), items.end());
        CPPUNIT_ASSERT(items.size() == size*8*8);
        unsigned x = 0;
        for(unsigned i = 0; i < (unsigned)size; i++) {
            for(unsigned j = 0; j < 8; j++) {
                for(unsigned k = 0; k < 8; k++) {
                    auto& e = items[x];
                    CPPUNIT_ASSERT(e.run == i && e.subrun == j && e.event == k);
                    x += 1;
                }
            }
        }
    }
}
