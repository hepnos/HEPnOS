#include <mpi.h>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/XmlOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <hepnos.hpp>

hepnos::DataStore* datastore = nullptr;

int main(int argc, char* argv[])
{
    if(argc != 2) return 1;

    MPI_Init(&argc, &argv);

    sleep(1);
    // Create the datastore
    hepnos::DataStore ds = hepnos::DataStore::connect(argv[1]);
    datastore = &ds;

    // Get the top level suite from the registry
    CppUnit::Test *suite = CppUnit::TestFactoryRegistry::getRegistry().makeTest();

    // Adds the test to the list of test to run
    CppUnit::TextUi::TestRunner runner;
    runner.addTest( suite );

    // Change the default outputter to a compiler error format outputter
    runner.setOutputter( new CppUnit::XmlOutputter( &runner.result(),
                std::cerr ) );
    // Run the tests.
    bool wasSucessful = runner.run();

    MPI_Barrier(MPI_COMM_WORLD);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if(rank == 0) ds.shutdown();

    MPI_Finalize();

    // Return error code 1 if the one of test failed.
    return wasSucessful ? 0 : 1;
}
