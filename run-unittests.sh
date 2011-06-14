#!/bin/bash

MAX_TRIES=${2:-100}
LOG_FILENAME=unittest.log
TEST_NAME=${1:-lock}

function notify()
{
    echo "$1"
    if [ "$MAX_TRIES" != "1" ]; then
        ssh -p 8888 localhost growlnotify --sticky -t "Lock unittests" -m "'$1'" 2> /dev/null
    fi
}

for ITER in `seq $MAX_TRIES`
do
    rm -f $LOG_FILENAME

    env/bin/trial $TEST_NAME

    CODE=$?
    echo "EXIT CODE: $CODE"

    if [ $CODE != 0 ]; then
        notify "Done with exit code=$CODE, iter=$ITER"
        exit 1
    fi

    echo 'Retrying'
done

notify "No more tries left."
