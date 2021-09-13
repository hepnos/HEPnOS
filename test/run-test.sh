#!/bin/bash -x

if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi

timeout_sec=${2:-60}

source test-util.sh

TEST_DIR=`$MKTEMP -d ./hepnos-XXXXXX`
SSG_FILE=$TEST_DIR/hepnos-test.ssg
CON_FILE=$TEST_DIR/connection.json
CFG_FILE=$TEST_DIR/config.json

cp config.json $CFG_FILE
sed -i -e "s|XXX|${SSG_FILE}|g" $CFG_FILE

hepnos_test_start_servers 2 2 ${timeout_sec} $CFG_FILE $CON_FILE $SSG_FILE

sleep 3
run_to ${timeout_sec} $1 $CON_FILE $1.xml
if [ $? -ne 0 ]; then
    wait
    exit 1
fi


wait

# cleanup
rm -rf $TEST_DIR
exit 0
