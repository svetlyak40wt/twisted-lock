Twisted lock
============

Fault-tolerant distributed lock with REST interface. Internaly,
it uses PAXOS protocol to reach consensus on propagated values.

How to install
--------------

    virtualenv --no-site-packages env
    env/bin/pip install Twisted Logbook
    # if you want to run unittests, do
    env/bin/pip install mock

How to run
----------

Run servers

    ./server.py configs.example_cluster.server1
    ./server.py configs.example_cluster.server2
    ./server.py configs.example_cluster.server3

Now you can:

* lock:

        curl -X POST -q http://127.0.0.1:9001/blah

* unlock

        curl -X POST -q http://127.0.0.1:9001/blah

* get server info:

        # status
        curl -q http://127.0.0.1:9001/info/status
        # or keys
        curl -q http://127.0.0.1:9001/info/keys


How to run tests
----------------

There are two types of tests: unittests and stresstests. Errors could be stochastic,
that is because all tests need to be run more then once, to be sure that there
is no errors.

    # run whole testsuite ten times
    ./run-unittests.sh lock 10
    # and now run consistency tests
    ./run-consistency-tests.sh 10

Credits
-------

Alexander Artemenko (<svetlyak.40wt@gmail.com>) — initial author.

Feel free to fork the project and send me pull requests.
