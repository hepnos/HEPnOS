Deployment
==========

Creating a configuration file
-----------------------------

The first step before deploying HEPnOS is to create a configuration file.
This configuration file should look like the following.

.. code-block:: yaml

   ---
   address: ofi+gni://
   threads: 63
   databases:
     datasets:
       name: hepnos-datasets
       path: /dev/shm
       type: map
       targets: 1
       providers: 1
     runs:
       name: hepnos-runs
       path: /dev/shm
       type: map
       targets: 1
       providers: 1
     subruns:
       name: hepnos-subruns
       path: /dev/shm
       type: map
       targets: 1
       providers: 1
     events:
       name: hepnos-events
       path: /dev/shm
       type: map
       targets: 1
       providers: 1
     products:
       name: hepnos-products
       path: /dev/shm
       type: map
       targets: 1
       providers: 1

The first field of the configuration is an address, or more precisely,
a protocol to use. Here *ofi+gni* indicates that libfabric should be
used with the Cray GNI backend. On a laptop or single node, the *na+sm*
(shared memory) protocol may be used. *ofi+tcp* may be used on Linux
clusters with traditional TCP networks.

The *threads* field indicates how many threads should be used by
HEPnOS on each node. Typically this value should be set to the number
of cores available, minus one core that is used to run the network
progress loop.

Then come five databases entries, respectively for DataSets, Runs,
SubRuns, Events, and Products. Each of these entries must have a name,
a path, a type, a number of targets per provider and a number of providers.
The name should be distinct for each database. The type of database can
be *map* (in memory database), *ldb* (LevelDB), or *bdb* (BerkeleyDB).
If *map* is used, the *path* is ignored since the database is stored in
memory. Otherwise, the path should point to a directory in a local
file system.

There is no real reason for changing the value of the *providers* entry
to anything else than 1 at the moment. However, changing the number of *targets*
may be useful to improve performance under heavy concurrency. "Target" is
another term for "database instance." Setting *targets* to a value greater
than 1 will make each node handle multiple databases.

.. important::
   If the *providers* or *targets* fields are set to a value greater than 1,
   the name of the database should include the $PROVIDER or $TARGET keys
   respectively. These keys will be replaced with the provider number and
   the target number.

.. important::
   If multiple HEPnOS daemons are started on the same node, the $RANK
   key should be used either in the database name or in the database
   path. This $RANK key will be replaced with the MPI rank of the
   process.

Deploying HEPnOS on a single node
---------------------------------

Simply ssh into the node where you want to run the HEPnOS service and type:


.. code-block:: console

   hepnos-daemon config.yaml client.yaml

This tells HEPnOS to start and configure itself using the *config.yaml* file (written before).
HEPnOS will generate a *client.yaml* file that can be used for clients to connect to it.
The command will block. To run it as a daemon, put it in the background, use nohup, or 
another other mechanism available on your platform.

Deploying HEPnOS on multiple nodes
----------------------------------

The hepnos-daemon program is actually an MPI program that can be deployed on multiple nodes:

.. code-block:: console

   mpirun -np N -f hostfile hepnos-daemon config.yaml client.yaml

Replacing N with the number of nodes and hostfile with the name of a file containing the list
of hosts on which to deploy HEPnOS.
