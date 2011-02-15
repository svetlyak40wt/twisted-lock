#!/usr/bin/env python

import subprocess
import threading
import time
import random
import urllib2

from collections import defaultdict
from itertools import izip
from logbook import Logger, StderrHandler


NUM_SERVERS = 5
NUM_WORKERS = 4
NUM_DATA_SLOTS = 2
NUM_ITERATIONS = 10
RANDOM_SERVER = True

logger = Logger()

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


def lock(key, worker):
    logger.debug('locking %r' % (key,))
    url = 'http://127.0.0.1:900%d/%s?w=%s' % (choose_server(worker), key, worker)
    request = PostRequest(url)
    num_retries = 10
    sleep_between = 1

    while num_retries > 0:
        try:
            result = urllib2.urlopen(request)
        except urllib2.HTTPError, e:
            logger.error('lock HTTPError: %s, key %r' % (e, key))
            if e.code in (409, 417): # key already exists or paxos failed, retry
                num_retries -= 1
                time.sleep(sleep_between)
                sleep_between *= 2
            else:
                break
        else:
            return True
    return False


def unlock(key, worker):
    logger.debug('unlocking %r' % (key,))
    url = 'http://127.0.0.1:900%d/%s?w=%s' % (choose_server(worker), key, worker)
    request = DeleteRequest(url)
    num_retries = 10
    sleep_between = 1

    while num_retries > 0:
        try:
            result = urllib2.urlopen(request)
            return
        except urllib2.HTTPError, e:
            logger.error('unlock HTTPError: %s, key %r' % (e, key))
            if e.code == 417: # paxos failed, retry
                num_retries -= 1
                time.sleep(sleep_between)
                sleep_between *= 2
            else:
                logger.critical('unlock failed because exception')
                raise
    logger.critical('unlock failed after retries')


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


def test():
    d = defaultdict(list)

    def worker(worker_id):
        with StderrHandler(format_string='[{record.time}] {record.level_name} [%s] {record.message}' % worker_id).threadbound():
            iterations_left = NUM_ITERATIONS
            num_fails_in_sequence = 0

            while iterations_left > 0:
                # acquire the locks
                locked = []
                for data_id in range(NUM_DATA_SLOTS):
                    key = 'key-%d' % data_id
                    if lock(key, worker_id):
                        locked.append(key)
                    else:
                        break

                if len(locked) == NUM_DATA_SLOTS:
                    num_fails_in_sequence = 0
                    for data_id in range(NUM_DATA_SLOTS):
                        d[data_id].append(worker_id)
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
                    unlock(key, worker_id)

                time.sleep(random.random() * 0.1)

            logger.debug('exit from the worker, locals=%r' % (locals(),))


    threads = [
        threading.Thread(target = worker, args = (worker_id,))
        for worker_id in range(NUM_WORKERS)
    ]
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
