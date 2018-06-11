#!/bin/bash -x

if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi

source test-util.sh

TEST_DIR=`$MKTEMP -d /tmp/hepnos-XXXXXX`
CON_FILE=$TEST_DIR/connection.yaml
cp config.yaml $TEST_DIR/config.yaml
CFG_FILE=$TEST_DIR/config.yaml
sed -i -e "s|XXX|${TEST_DIR}/database|g" $CFG_FILE
export HEPNOS_CONFIG_FILE=$CON_FILE

# Run HEPnOS
hepnos_test_start_servers 2 1 20 $CFG_FILE $CON_FILE

# Run a test client
run_to 10 $1 $CON_FILE
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

# Wait on both HEPnOS and test client to close
wait

# Run HEPnOS again
hepnos_test_start_servers 2 1 20 $CFG_FILE $CON_FILE

# Run the second test client
run_to 10 $2 $CON_FILE
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

# Wait on both HEPnOS and test client to close
wait

##############
# cleanup
rm -rf $TEST_DIR
rm -rf /dev/shm/hepnos.dat
exit 0
