Client connection and service shutdown
======================================

The following code sample showcases how to initialize a DataStore
object in a client program. This object is the main entry point to
the HEPnOS storage system. Its :code:`connect()` function takes
the name of a YAML file as a parameter, which should be the file
created by HEPnOS when starting up.

.. literalinclude:: ../../examples/01_init_shutdown/main.cpp
       :language: cpp

The :code:`DataStore::shutdown()` method can be used to tell
HEPnOS to shutdown. This method should be called by only one client
and will terminate all the HEPnOS processes. If HEPnOS is setup to
use in-memory databases, you will loose all the data store in HEPnOS.
