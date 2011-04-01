#!/usr/bin/env python
from __future__ import with_statement

import random
import socket
import subprocess
import sys
import threading
import time
import urllib2
import os
import logbook

from collections import defaultdict
from itertools import izip
from logbook import Logger, StderrHandler, FileHandler, NestedSetup, NullHandler, Processor


NUM_SERVERS = 5
NUM_WORKERS = 4
NUM_DATA_SLOTS = 2
NUM_ITERATIONS = 10
RANDOM_SERVER = True
KILL_PROBABLITITY = 0.0
STUCK_TIME_LIMIT = 60

logger = Logger()
socket.setdefaulttimeout(5)

class PostRequest(urllib2.Request):
    def get_method(self):
        return 'POST'


class DeleteRequest(urllib2.Request):
    def get_method(self):
        return 'DELETE'


_server_distribution = dict(
    (worker_id, random.randint(1, NUM_SERVERS))
    for worker_id in range(NUM_SERVERS)
)

def choose_server(worker_id):
    if RANDOM_SERVER:
        return _server_distribution[worker_id]
    return 1


class Server(object):
    def __init__(self, id_):
        self.id = id_
        self.config = 'stress_tests.configs.server%d' % id_
        self.pid = None

    def start(self):
        if self.pid is None:
            logger.info('Starting server %s' % self.id)
            self.pid = subprocess.Popen(['./server.py', self.config])

    def stop(self):
        if self.pid is not None:
            logger.info('Stopping server %s' % self.id)
            self.pid.send_signal(9)
            self.pid.wait()
            self.pid = None


def start_servers(servers):
    for server in servers:
        server.start()


def stop(servers):
    for server in servers:
        server.stop()

def compare_logs():
    data = defaultdict(list)
    for x in range(1, NUM_SERVERS+1):
        log = urllib2.urlopen('http://127.0.0.1:900%d/info/log' % x).read()
        log = '\n'.join(
            '%3d %s' % (lineno+1, line)
            for lineno, line in filter(None, enumerate(log.split('\n')))
        )
        data[log].append(x)
        with open('/tmp/%s' % x, 'w') as f:
            f.write(log)

    if len(data) == 1:
        logger.info('Logs are equal')
        return 0
    else:
        for log, servers in data.iteritems():
            logger.error('Log from servers %s:\n%s' % (
                ', '.join(map(str, servers)),
                log
            ))
        return 8


def print_server_status():
    for x in range(1, NUM_SERVERS+1):
        status = urllib2.urlopen('http://127.0.0.1:900%d/info/status' % x).read()
        keys = urllib2.urlopen('http://127.0.0.1:900%d/info/keys' % x).read()
        logger.info('Server%s:\n%s\n%s' % (x, status, keys))


class Worker(threading.Thread):
    # reference lock state
    _keys = {}
    _lock = threading.Lock()

    def __init__(self, id_, data, exit_event):
        super(Worker, self).__init__()
        self.id = id_
        self.d = data
        self.iter = 0
        self.exit_event = exit_event

    def run(self):
        def inject_worker_id(record):
            record.extra['worker_id'] = self.id

        with Processor(inject_worker_id).threadbound():
            iterations_left = NUM_ITERATIONS
            num_fails_in_sequence = 0

            while iterations_left > 0 and not self.exit_event.is_set():
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
                    logger.info('decrement: locked=%r' % (locked,))
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
                    'http://127.0.0.1:900%d/%s?data=%s-%s' % (choose_server(self.id), key, self.id, self.iter)
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
                            logger.warning('################## key %s already locked by me' % (key, ))
                        else:
                            logger.warning('------------------ key %s already locked by worker %s' % (key, self._keys[key]))
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
                    'http://127.0.0.1:900%d/%s?data=%s-%s' % (choose_server(self.id), key, self.id, self.iter)
                )
                result = urllib2.urlopen(request)

                with self._lock:
                    if key in self._keys:
                        if self._keys[key] != self.id:
                            logger.warning('++++++++++++++++++ key %s was locked by worker %s' % (key, self._keys[key]))
                        del self._keys[key]
                    else:
                        logger.warning('================== key %s was unlocked by another worker' % (key, ))

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


def test(servers):
    d = defaultdict(list)
    seed = int(time.time())
    print 'RANDOM SEED: %s' % seed
    random.seed(seed)

    def watch_on_progress(exit_event):
        done = len(d[0])
        total = NUM_ITERATIONS * NUM_WORKERS
        bar_length = 40
        previous_progress = 0
        time_since_progress = time.time()

        while done < total:
            done = len(d[0])
            progress = float(done) / total
            if progress > previous_progress:
                logger.info(
                    'Progress: %5.1f%% [%s%s]' % (
                        progress * 100,
                        '#' * int(bar_length * progress),
                        ' ' * (bar_length - int(bar_length * progress)),
                    )
                )
                previous_progress = progress
                time_since_progress = time.time()
            else:
                if time.time() - time_since_progress > STUCK_TIME_LIMIT:
                    logger.critical('Progress stuck on %s%%' % (progress * 100))
                    break
            time.sleep(1)
        exit_event.set()


    def killer():
        """This thread kills random server at random time."""
        while True:
            time.sleep(random.randint(1, 5))

            # restore previously killed servers
            for server in servers:
                server.start()

            time_to_kill = random.random() < KILL_PROBABLITITY
            if time_to_kill:
                # kill someone
                server = random.choice(servers)
                server.stop()

    exit_event = threading.Event()
    threads = [Worker(worker_id, d, exit_event) for worker_id in range(1, NUM_WORKERS+1)]

    server_killer = threading.Thread(target=killer)
    server_killer.daemon = True
    server_killer.start()

    for thread in threads:
        thread.daemon = True
        thread.start()

    watch_on_progress(exit_event)

    # Give servers a chance to finish processes
    time.sleep(5)

    logger.info('Checking the data.')

    if not d:
        logger.error('Data dict is empty.')

    result = 0
    for key, value in d.iteritems():
        if len(value) != NUM_ITERATIONS * NUM_WORKERS:
            result |= 2
            logger.error('Wrong data count for key "%s": %s, should be %s' % (
                key,
                len(value),
                NUM_ITERATIONS * NUM_WORKERS,
            ))

    data = izip(*[d[i] for i in range(NUM_DATA_SLOTS)])
    # All rows should have the only on worker's id
    for row in data:
        if len(set(row)) != 1:
            logger.error('Bad data row: %r' % (row,))
            result |= 4

    result |= compare_logs()

    if result != 0:
        print_server_status()

    return result


def main():
    logger.info('Starting')

    servers = [Server(i) for i in range(1, NUM_SERVERS + 1)]

    start_servers(servers)

    time.sleep(10)
    try:
        return test(servers)
    except Exception, e:
        logger.exception('Test failed: %s' % e)
        return 1
    finally:
        logger.info('Stopping')
        stop(servers)

if __name__ == '__main__':
    format = '[{record.time}] {record.level_name:>5} [{record.extra[worker_id]}] {record.message}'

    logging_setup = NestedSetup([
        NullHandler(),
        FileHandler(
            filename=os.path.join(os.path.dirname(__file__), 'log/client.log'),
            format_string=format,
            bubble=True,
        ),
        StderrHandler(level=logbook.INFO, format_string=format, bubble=True),
    ])

    with logging_setup.applicationbound():
        sys.exit(main())
