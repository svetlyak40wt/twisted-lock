#!/bin/bash

MAX_TRIES=20
LOG_FILENAME=stress_tests/log/client.log

for ITER in `seq $MAX_TRIES`
do
    rm -f stress_tests/log/*.log

    ./stress_tests/consistency.py

    CODE=$?
    echo "EXIT CODE: $CODE"

    if [ $CODE != 0 ]; then
        echo "Done with exit code=$CODE, iter=$ITER"
        break
    fi

    sleep 5
    echo 'Retrying'
done

