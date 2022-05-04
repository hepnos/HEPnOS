Client connection and service shutdown
======================================

The following code sample showcases how to initialize a *DataStore*
object in a client program. This object is the main entry point to
the HEPnOS storage system. Its :code:`connect()` function takes
the name of a JSON file as a parameter, which should be the file
created by the *hepnos-list-databases* command.

.. literalinclude:: ../../examples/01_init_shutdown/main.cpp
       :language: cpp

The :code:`DataStore::connect()` function may also take an additional
parameter to supply a JSON configuration file for the underlying Margo layer
(see `the Margo documentation <https://mochi.readthedocs.io/en/latest/margo.html>`_
for more information on the format of this configuration file).

An useful example of Margo JSON file could be one that sets up a dedicated
progress thread:

.. code-block:: json

   {
       "use_progress_thread": true
   }

Setting this value to :code:`true` can be useful
if the application relies on asynchronous operations (:code:`AsyncEngine`).

The :code:`DataStore::shutdown()` method can be used to tell the
HEPnOS service to shutdown.

.. important::
   The :code:`DataStore::shutdown()` method should be called *by only one*
   client and will terminate *all* the HEPnOS server processes. If HEPnOS
   is setup to use in-memory databases, you will loose all the data stored
   in HEPnOS. If multiple clients call this method, they will either block
   or fail, depending on the network protocol used by HEPnOS.
