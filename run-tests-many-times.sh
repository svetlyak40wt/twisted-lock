#!/bin/bash

NUM=${1:-2}
let ERRORS=0

date > tests.log

for i in `seq $NUM`
do
    echo "$i iteration"
    if ! env/bin/trial lock.test.test_paxos >> tests.log 2>&1
    then
        let ERRORS=$ERRORS+1
    fi
done

echo "Num errors: $ERRORS"

