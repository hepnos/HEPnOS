Client connection and service shutdown
======================================

The following code sample showcases how to initialize a DataStore
object in a client program. This object is the main entry point to
the HEPnOS storage system. Its :code:`connect()` function takes
the name of a YAML file as a parameter, which should be the file
created by HEPnOS when starting up.

.. literalinclude:: ../../examples/01_init_shutdown/main.cpp
       :language: cpp

The :code:`DataStore::connect()` function may also take an additional
boolean parameter indicating whether to use a background thread for
network progress. Setting this value to :code:`true` can be useful
if the application relies on asynchronous operations (:code:`AsyncEngine`).

The :code:`DataStore::shutdown()` method can be used to tell the
HEPnOS service to shutdown.

.. important::
   The :code:`DataStore::shutdown()` method should be called by only one
   client and will terminate all the HEPnOS processes. If HEPnOS is setup to
   use in-memory databases, you will loose all the data store in HEPnOS.
   If multiple clients call this method, they will either block or fail,
   depending on the network protocol used by HEPnOS.
