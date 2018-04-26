#!/bin/bash -x

if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi

source test-util.sh

TEST_DIR=`$MKTEMP -d /tmp/hepnos-XXXXXX`
CFG_FILE=$TEST_DIR/config.yml

hepnos_test_start_servers 2 1 20 $CFG_FILE

export HEPNOS_CONFIG_FILE=$CFG_FILE

# run a connect test client
run_to 10 $1 $CFG_FILE
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

##############

wait

# cleanup
rm -rf $TEST_DIR

exit 0
