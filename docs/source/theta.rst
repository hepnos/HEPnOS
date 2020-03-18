Deploying and running on Theta
==============================

This section specifically describes how to install
and run HEPnOS and HEPnOS programs on Theta.

Setting up spack
----------------

If you don't have spack already, you need to set it up.

.. code-block:: console
   
   git clone https://github.com/spack/spack.git
   . spack/share/spack/setup-env.sh

Note that the second line should be run everytime you log in
to bring spack into your PATH.

Now edit *~/.spack/cray/packages.yaml* with the following.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          packages.yaml (show/hide)

    .. literalinclude:: ../../examples/theta/packages.yaml
       :language: yaml

Those are system packages that Spack won't need to install.

Setting up Mochi
----------------

Once spack is installed, you need to add the mochi repository.

.. code-block:: console
   
   git clone https://xgitlab.cels.anl.gov/sds/sds-repo

Setting up a Spack environment
------------------------------

We will then create an environment called *my-hepnos-env* and
add HEPnOS to it.

.. code-block:: console
   
   spack env create hepnos-env
   spack env activate hepnos-env

Edit the *spack/var/spack/environments/hepnos-env/spack.yaml* file
so it looks like the following.

.. code-block:: yaml

  spack:
    specs:
    - hepnos
    view: true
    repos:
    - /gpfs/mira-home/<usernam>/sds-repo
    packages:
      libfabric:
        variants: fabrics=gni
      mercury:
        variants: +udreg ~boostsys

Replace <username> by your username (or the full path to the cloned
sds-repo folder if you have cloned it somewhere else than in you home).
You may add more specs in this file if you need.

Now type :code:`spack install` to install HEPnOS.

Running HEPnOS
--------------

To run HEPnOS on Theta, the HEPnOS's YAML configuration file should
use *ofi+gni* in the address field.

Theta by default restricts communication between processes of distinct
MPI applications. To enable such connections, you need to create a
protection domain. This can be done as follows from the MOM nodes
(i.e. either inside the script you give to qsub, or when you are in
an interactive session).

.. code-block:: console

   apmgr pdomain -c -u mydomain

Replace *mydomain* with a suitable name. This name should not conflict
with a name setup by someone else. Also the protection domain is persistent
across job submissions, so keep it available only if you need it later,
and if not, use the following command to destroy it once your job has completed.

.. code-block:: console

   apmgr pdomain -r -u mydomain

HEPnOS can be started on a set of nodes (either from a qsub script or
in an interactive sessions) using the following command.

.. code-block:: console

   . spack/share/spack/setup-env.sh
   spack env activate hepnos-env
   aprun -n X -N 1 -p mydomain hepnos-daemon <config.yaml> <client.yaml> &

Replace <config.yaml> by the HEPnOS configuration file and <client.yaml>
by the name of the file you want to create for clients to connect.
