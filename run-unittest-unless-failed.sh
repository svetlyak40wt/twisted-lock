#!/bin/bash

MAX_TRIES=100
LOG_FILENAME=unittest.log
TEST_NAME=$1

function growl()
{
    ssh -p 8888 localhost ~/usr/bin/growl "$1"
}

for ITER in `seq $MAX_TRIES`
do
    rm -f $LOG_FILENAME

    env/bin/trial $TEST_NAME

    CODE=$?
    echo "EXIT CODE: $CODE"

    if [ $CODE != 0 ]; then
        MSG="Done with exit code=$CODE, iter=$ITER"
        echo $MSG
        growl "$MSG"
        break
    fi

    echo 'Retrying'
done

if [ $ITER == $MAX_TRIES ]; then
    MSG="No more tries left."
    echo $MSG
    growl "$MSG"
fi

