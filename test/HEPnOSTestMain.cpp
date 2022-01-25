#include <mpi.h>
#include <fstream>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/XmlOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <hepnos.hpp>

hepnos::DataStore* datastore = nullptr;

int main(int argc, char* argv[])
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(argc < 2) return 1;

    sleep(1);
    // Create the datastore
    hepnos::DataStore ds = hepnos::DataStore::connect("na+sm", argv[1]);
    datastore = &ds;

    // Get the top level suite from the registry
    CppUnit::Test *suite = CppUnit::TestFactoryRegistry::getRegistry().makeTest();

    // Adds the test to the list of test to run
    CppUnit::TextUi::TestRunner runner;
    runner.addTest( suite );

    std::ofstream xmlOutFile;
    if(argc >= 3) {
        if(rank == 0) {
            const char* xmlOutFileName = argv[2];
            xmlOutFile.open(xmlOutFileName);
        } else {
            xmlOutFile.open("/dev/null");
        }
        // Change the default outputter to a compiler error format outputter
        runner.setOutputter(new CppUnit::XmlOutputter(&runner.result(), xmlOutFile));
    } else {
        // Change the default outputter to a compiler error format outputter
        runner.setOutputter(new CppUnit::XmlOutputter(&runner.result(), std::cerr));
    }

    // Run the tests.
    bool wasSucessful = runner.run();

    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 0) {
        ds.shutdown();
    }
    if(argc >= 3)
       xmlOutFile.close();

    MPI_Finalize();
    // Return error code 1 if the one of test failed.
    return wasSucessful ? 0 : 1;
}
