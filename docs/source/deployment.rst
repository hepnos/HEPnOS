Deployment
==========

Creating a configuration file
-----------------------------

HEPnOS relies on the `Bedrock <https://mochi.readthedocs.io/en/latest/bedrock.html>`_
Mochi microservice for bootstrapping and configuration.

The first step before deploying HEPnOS is to create a configuration file.
This configuration file should be in JSON format and at least contain the following.

.. code-block:: json

   {
      "ssg" : [
        {
            "name" : "hepnos",
            "bootstrap" : "mpi",
            "group_file" : "hepnos.ssg",
            "swim" : { "disabled" : true }
        }
      ],
      "libraries" : {
        "yokan" : "libyokan-bedrock-module.so"
      },
      "providers" : [
        {
            "name" : "hepnos",
            "type" : "yokan",
            "config" : {
                "databases" : [
                    {
                        "name" : "hepnos-datasets",
                        "type" : "map",
                        "config": {}
                    },
                    {
                        "name" : "hepnos-runs",
                        "type" : "map",
                        "config": {}
                    },
                    {
                        "name" : "hepnos-subruns",
                        "type" : "map",
                        "config": {}
                    },
                    {
                        "name" : "hepnos-events",
                        "type" : "map",
                        "config": {}
                    },
                    {
                        "name" : "hepnos-products",
                        "type" : "map",
                        "config": {}
                    }
                ]
            }
        }
      ]
   }


This example configuration file only provides the bare minimum to get
started. The *"ssg"* section sets up the group management component.
The only important field here is the name of the group file, which we
will use later.

The *"providers"* section should contain at least one Yokan provider
with a number of databases. These databases must have a name that
starts with *"hepnos-datasets"*, *"hepnos-runs"*, *"hepnos-subruns"*,
*"hepnos-events"*, or *"hepnos-products"*. At least one database for each
type of data should be provided, but you are free to use more than
one database for some types of data, as long as their name starts
with the above prefixes. For example, you can have two databases
to store events, named *"hepnos-events-1"* and *"hepnos-events-2"*.

Configuring with the HEPnOS Wizard
----------------------------------

An easy way of creating a HEPnOS configuration for Bedrock is to use
the HEPnOS Wizard, which can be installed as follows.

.. code-block:: console

   $ spack install py-hepnos-wizard

Once installed and loaded, you can use it as follows.

.. code-block:: console

   $ hepnos-gen-config --address na+sm --output=myconfig.json

The only required parameter is the *address*, which should be a valid
protocol accepted by the underlying Mercury library (e.g. *na+sm*, *ofi+tcp*,
and so on).

Passing *--help* to hepnos-gen-config will provide information on
all the available arguments and their meaning.


Deploying HEPnOS on a single node
---------------------------------

To deploy HEPnOS on a single node, simply ssh into the node and type the following.

.. code-block:: console

   bedrock na+sm -c config.json

Change *na+sm* into the protocol that you want to use for communication.
This tells Bedrock to start and initialize itself with the provided configuration.
The command will block. To run it as a daemon, put it in the background, use nohup, or
another other mechanism available on your platform.

Deploying HEPnOS on multiple nodes
----------------------------------

The bedrock program can just as simply be deployed on multiple nodes, using
your favorite MPI laucher (mpirun, aprun, etc.), for instance:

.. code-block:: console

   mpirun -np 4 -f hostfile bedrock na+sm -c config.json

Getting connection information
------------------------------

Once deployed, run the following command to obtain connection information readable
by the client.

.. code-block:: console

   hepnos-list-databases na+sm -s ssg-file > connection.json

Where *ssg-file* is the name of the SSG file as specified in your HEPnOS
configuration file.

This command will query the service and print a JSON representation of
the information required for a client to connect to HEPnOS (addresses, database ids, etc.).
Hence we pipe its output to a *connection.json* file that the clients will use later.

.. important::
   On some platforms, you will need to launch this command as an MPI application
   running on a single process/node (typically if your login node does not connect
   to the compute nodes via the same type of network as across compute nodes).

