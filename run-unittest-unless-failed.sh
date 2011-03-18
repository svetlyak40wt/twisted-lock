#!/bin/bash

MAX_TRIES=100
LOG_FILENAME=unittest.log
TEST_NAME=$1


for ITER in `seq $MAX_TRIES`
do
    rm -f $LOG_FILENAME

    env/bin/trial $TEST_NAME

    CODE=$?
    echo "EXIT CODE: $CODE"

    if [ $CODE != 0 ]; then
        echo "Done with exit code=$CODE, iter=$ITER"
        break
    fi

    echo 'Retrying'
done

