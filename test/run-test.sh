#!/bin/bash -x

if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi

timeout_sec=${2:-10}

source test-util.sh

TEST_DIR=`$MKTEMP -d /tmp/hepnos-XXXXXX`
CON_FILE=$TEST_DIR/connection.yaml
cp config.yaml $TEST_DIR/config.yaml
CFG_FILE=$TEST_DIR/config.yaml
sed -i -e "s|XXX|${TEST_DIR}/database|g" $CFG_FILE

hepnos_test_start_servers 2 2 20 $CFG_FILE $CON_FILE

export HEPNOS_CONFIG_FILE=$CON_FILE

# run a connect test client
run_to ${timeout_sec} $1 $CON_FILE
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

##############

wait

# cleanup
rm -rf $TEST_DIR
rm -rf /dev/shm/hepnos.*.dat
exit 0
