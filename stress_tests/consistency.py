#!/usr/bin/env python
from __future__ import with_statement

import subprocess
import threading
import time
import random
import urllib2
import socket

from collections import defaultdict
from itertools import izip
from logbook import Logger, StderrHandler


NUM_SERVERS = 5
NUM_WORKERS = 4
NUM_DATA_SLOTS = 2
NUM_ITERATIONS = 10
RANDOM_SERVER = True

logger = Logger()
socket.setdefaulttimeout(5)

class PostRequest(urllib2.Request):
    def get_method(self):
        return 'POST'


class DeleteRequest(urllib2.Request):
    def get_method(self):
        return 'DELETE'


_server_distribution = dict(
    (worker_id, worker_id+1)#random.randint(1, NUM_SERVERS))
    for worker_id in range(NUM_SERVERS)
)

def choose_server(worker_id):
    if RANDOM_SERVER:
        return _server_distribution[worker_id]
    return 1


def run_servers():
    results = []
    for i in range(1, NUM_SERVERS + 1):
        results.append(
            subprocess.Popen(['./server.py', 'stress_tests/configs/server%d.cfg' % i])
        )
    return results


def stop(servers):
    for server in servers:
        server.terminate()
    for server in servers:
        server.wait()


def compare_logs():
    data = defaultdict(list)
    for x in range(1, NUM_SERVERS+1):
        log = urllib2.urlopen('http://127.0.0.1:900%d/info/log' % x).read()
        log = '\n'.join(
            '%3d %s' % (lineno+1, line)
            for lineno, line in enumerate(log.split('\n'))
        )
        data[log].append(x)
        with open('/tmp/%s' % x, 'w') as f:
            f.write(log)

    if len(data) == 1:
        logger.info('Logs are equal')
    else:
        for log, servers in data.iteritems():
            logger.error('Log from servers %s:\n%s' % (
                ', '.join(map(str, servers)),
                log
            ))

class Worker(threading.Thread):
    # reference lock state
    _keys = {}
    _lock = threading.Lock()

    def __init__(self, id_, data):
        super(Worker, self).__init__()
        self.id = id_
        self.d = data
        self.iter = 0

    def run(self):
        with StderrHandler(format_string='[{record.time}] {record.level_name} [%s] {record.message}' % self.id).threadbound():
            iterations_left = NUM_ITERATIONS
            num_fails_in_sequence = 0

            while iterations_left > 0:
                # acquire the locks
                locked = []
                for data_id in range(NUM_DATA_SLOTS):
                    key = 'key-%d' % data_id
                    if self.lock(key):
                        locked.append(key)
                    else:
                        break

                if len(locked) == NUM_DATA_SLOTS:
                    num_fails_in_sequence = 0
                    for data_id in range(NUM_DATA_SLOTS):
                        self.d[data_id].append(self.id)
                    iterations_left -= 1
                else:
                    num_fails_in_sequence += 1
                    if num_fails_in_sequence > NUM_ITERATIONS * NUM_DATA_SLOTS * NUM_WORKERS:
                        # seems that algorithm stuck somewhere,
                        # exit from worker
                        logger.warning('seems that algorithm stuck somewhere')
                        break

                # release the locks
                for key in reversed(locked):
                    self.unlock(key)

                time.sleep(random.random())

            logger.debug('exit from the worker, locals=%r' % (locals(),))

    def lock(self, key):
        num_retries = 10

        while num_retries > 0:
            try:
                self.iter += 1
                logger.debug('locking %r (iter=%s)' % (key, self.iter))
                request = PostRequest(
                    'http://127.0.0.1:900%d/%s?w=%s&i=%s' % (choose_server(self.id), key, self.id, self.iter)
                )
                result = urllib2.urlopen(request)
            except urllib2.HTTPError, e:
                logger.error('lock HTTPError: %s, key %r, retries left %s' % (e, key, num_retries))
                if e.code in (409, 417): # key already exists or paxos failed, retry
                    num_retries -= 1
                    time.sleep(random.random())
                else:
                    break
            except urllib2.URLError, e:
                logger.error('lock URLError: %s, key %r, retries left %s' % (e, key, num_retries))
                num_retries -= 1
                time.sleep(random.random())
            else:
                # check if we lock right
                with self._lock:
                    if key in self._keys:
                        if self._keys[key] == self.id:
                            logger.critical('################## key %s already locked by me' % (key, ))
                        else:
                            logger.critical('------------------ key %s already locked by worker %s' % (key, self._keys[key]))
                    else:
                        self._keys[key] = self.id

                logger.debug('locked %r' % (key,))

                return True
        return False

    def unlock(self, key):
        num_retries = 10

        while num_retries > 0:
            try:
                self.iter += 1
                logger.debug('unlocking %r (iter=%s)' % (key, self.iter))
                request = DeleteRequest(
                    'http://127.0.0.1:900%d/%s?w=%s&i=%s' % (choose_server(self.id), key, self.id, self.iter)
                )
                result = urllib2.urlopen(request)

                with self._lock:
                    if key in self._keys:
                        if self._keys[key] != self.id:
                            logger.critical('++++++++++++++++++ key %s was locked by worker %s' % (key, self._keys[key]))
                        del self._keys[key]
                    else:
                        logger.critical('================== key %s was unlocked by another worker' % (key, ))

                logger.debug('unlocked %r' % (key,))
                return
            except urllib2.HTTPError, e:
                logger.error('unlock HTTPError: %s, key %r, retries left %s' % (e, key, num_retries))
                if e.code == 417: # paxos failed, retry
                    num_retries -= 1
                    time.sleep(random.random())
                else:
                    logger.critical('unlock failed because exception')
                    raise
            except urllib2.URLError, e:
                logger.error('lock URLError: %s, key %r, retries left %s' % (e, key, num_retries))
                num_retries -= 1
                time.sleep(random.random())
        logger.critical('unlock failed after retries')
        raise


def test():
    d = defaultdict(list)
    seed = int(time.time())
    print 'RANDOM SEED: %s' % seed
    random.seed(seed)

    threads = [Worker(worker_id, d) for worker_id in range(1, NUM_WORKERS+1)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    logger.info('Checking the data.')
    if not d:
        logger.error('Data dict is empty.')

    for key, value in d.iteritems():
        if len(value) != NUM_ITERATIONS * NUM_WORKERS:
            logger.error('Wrong data count for key "%s": %s, should be %s' % (
                key,
                len(value),
                NUM_ITERATIONS * NUM_WORKERS,
            ))

    data = izip(*[d[i] for i in range(NUM_DATA_SLOTS)])
    # All rows should have the only on worker's id
    for row in data:
        if len(set(row)) != 1:
            logger.error('Bad data raw: %r' % (row,))

    compare_logs()


def main():
    logger.info('Starting')
    servers = run_servers()

    time.sleep(10)
    try:
        test()
    except Exception, e:
        logger.exception('Test failed: %s' % e)

    logger.info('Stopping')
    stop(servers)

if __name__ == '__main__':
    with StderrHandler(format_string='[{record.time}] {record.level_name} {record.message}').applicationbound():
        main()
