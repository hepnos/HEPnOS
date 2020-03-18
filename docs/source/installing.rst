Installing
==========

The recommended way to install the HEPnOS and its dependencies 
is to use `Spack <https://spack.readthedocs.io/en/latest/>`_.
Spack is a package management tool designed to support multiple
versions and configurations of software on a wide variety of
platforms and environments.

Installing Spack and the Mochi repository
-----------------------------------------

First, you will need to install Spack as explained
`here <https://spack.readthedocs.io/en/latest/getting_started.html>`_.
Once Spack is installed and available in your path, clone the following
git reporitory and add it as a Spack namespace.

.. code-block:: console

   git clone https://xgitlab.cels.anl.gov/sds/sds-repo.git
   spack repo add sds-repo

You can then check that Spack can find HEPnOS by typping:

.. code-block:: console

   spack info hepnos

You should see something like the following.

.. code-block:: console

   CMakePackage:   hepnos
   
   Description:
       Object store for High Energy Physics, build around Mochi components
   
   Homepage: https://xgitlab.cels.anl.gov/sds/HEPnOS
   ... (more lines follow) ...

Installing HEPnOS
---------------------

Installing HEPnOS is then as simple as typping the following.

.. code-block:: console

   spack install hepnos

Loading and using HEPnOS
------------------------

Once installed, you can load HEPnOS using the following command.

.. code-block:: console

   spack load -r hepnos

This will load HEPnOS and its dependencies (Mercury, Thallium, Argobots, etc.).
You are now ready to use HEPnOS!


Using the HEPnOS client library with cmake
------------------------------------------

Within a cmake project, HEPnOS can be found using:

.. code-block:: console
   
   find_package(hepnos REQUIRED)

You can now link targets as follows.

.. code-block:: console
   
   add_executable(my_hepnos_client source.c)
   target_link_libraries(my_hepnos_client hepnos)

Using the HEPnOS client libraries with pkg-config
-------------------------------------------------

Support for pkg-config is still preliminary and not completely supported.
Once loaded into your working environment, :code:`pkg-config --libs --cflags hepnos`
will give you the CFLAGS and LDFLAGS you need to link your code against
HEPnOS. However this command will not give you CFLAGS or LDFLAGS for the
Boost serialization library, which does not itself support pkg-config.
You will have to add these manually.
