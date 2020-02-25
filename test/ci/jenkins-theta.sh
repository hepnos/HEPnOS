#!/bin/bash
set -e

HERE=`dirname "$0"`

module swap PrgEnv-intel PrgEnv gnu
module load cce

echo "Setting up Spack"
git clone https://github.com/spack/spack.git
. spack/share/spack/setup-env.sh

echo "Setting up sds-repo"
git clone https://xgitlab.cels.anl.gov/sds/sds-repo.git
spack repo add sds-repo

echo "Copying packages.yaml file"
cp $HERE/theta-packages.yaml $HOME/.spack/cray/packages.yaml

echo "Installing depencencies"
spack install ch-placement
spack install thallium
spack install sdskeyval
spack install yaml-cpp
spack install boost+serialization
spack install cmake
spack install libuuid
spack install cppunit

echo "List of packages"
spack find

echo "Loading packages"
spack load -r ch-placement
spack load -r thallium
spack load -r sdskeyval
spack load -r yaml-cpp
spack load -r boost
spack load -r cmake
spack load -r libuuid
spack load -r cppunit

echo "Building HEPnOS"
mkdir build
cd build
cmake .. -DCMAKE_CXX_COMPILER=CC -DENABLE_TESTS=ON -DBUILD_SHARED_LIBS=ON
make
