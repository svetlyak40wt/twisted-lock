#!/bin/bash

MAX_TRIES=${1:-20}
LOG_FILENAME=stress_tests/log/client.log
#export PYTHONPATH=`pwd`/stress_tests:$PYTHONPATH

function notify()
{
    echo "$1"
    ssh -p 8888 localhost growlnotify --sticky -t "Lock unittests" -m "'$1'" 2> /dev/null
}

for ITER in `seq $MAX_TRIES`
do
    rm -f stress_tests/log/*.log

    ./stress_tests/consistency.py

    CODE=$?
    echo "EXIT CODE: $CODE"

    if [ $CODE != 0 ]; then
        notify "Done with exit code=$CODE, iter=$ITER"
        exit 1
    fi

    sleep 5
    echo 'Retrying'
done

notify "No errors"
