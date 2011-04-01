# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import operator

from twisted.web.client import Agent, getPage
from twisted.internet import reactor
from twisted.internet.task import deferLater
from twisted.internet.defer import inlineCallbacks, gatherResults
from twisted.web.http import EXPECTATION_FAILED, OK

from ..lock import LockFactory, LockProtocol, Syncronizer
from ..config import Config
from ..utils import wait_calls
from .import TestCase as BaseTestCase, seed, get_body

class TestCase(BaseTestCase):
    num_nodes = 3
    timeout = 15

    def __init__(self, *args, **kwargs):
        self.client = Agent(reactor)

        nodes = [('127.0.0.1', 4001 + x) for x in range(self.num_nodes)]

        self.configs = [
            Config(
                NODES=nodes,
                LOCK_PORT=4001 + x,
                HTTP_PORT=9001 + x
            )
            for x in range(self.num_nodes)
        ]

        super(TestCase, self).__init__(*args, **kwargs)

    def setUp(self):
        LockProtocol.send_line_hooks = []
        self.servers = []

        for x, cfg in enumerate(self.configs):
            s = LockFactory(cfg)
            setattr(self, 's%s' % (x+1), s)
            self.servers.append(s)
        self.addCleanup(self._close_servers)

    def _close_servers(self):
        for server in self.servers:
            if server is not None:
                server.close()

    def stop_server(self, number):
        s = getattr(self, 's%s' % number)
        setattr(self, 's%s' % number, None)
        s.close()
        self.servers[number - 1] = None

    def start_server(self, number):
        s = LockFactory(self.configs[number - 1])
        setattr(self, 's%s' % number, s)
        self.servers[number - 1] = s

    @inlineCallbacks
    def wait_when_connection_establied(self):
        yield gatherResults([s.when_connected() for s in self.servers])

    @inlineCallbacks
    def _add_key(self, key):
        result = yield self.client.request(
            'POST',
            'http://%s:%s/%s' % (self.s1.http_interface, self.s1.http_port, key)
        )
        self.assertEqual(OK, result.code)

    def _assert_servers_consistency(self, *args):
        for x, s in enumerate(self.servers):
            if s is not None:
                try:
                    self.assertEqual(
                        dict((item, '') for item in args),
                        s._keys
                    )
                except Exception, e:
                    e.args = ('In %s server: ' % (x + 1) + e.args[0],)
                    raise


class Simple(TestCase):
    num_nodes = 1
    @inlineCallbacks
    def test_start_server(self):
        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual(EXPECTATION_FAILED, result.code)


class MasterLeases(TestCase):
    @inlineCallbacks
    def test_status(self):
        yield self.wait_when_connection_establied()
        server = self.s1
        self.assertEqual(None, server.master)
        result = yield self.client.request(
            'GET',
            'http://%s:%s/info/status' % (server.http_interface, server.http_port)
        )
        self.assertEqual(OK, result.code)

    @inlineCallbacks
    def test_node_become_a_master(self):
        yield self.wait_when_connection_establied()

        self.assertEqual(None, self.s2.master)
        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah?data=some-data' % (self.s1.http_interface, self.s1.http_port)
        )
        expected = (self.s1.http_interface, self.s1.http_port)
        self.assertEqual(expected, self.s1.master.http)
        self.assertEqual(expected, self.s2.master.http)
        self.assertEqual(expected, self.s3.master.http)

    @inlineCallbacks
    def test_requests_are_proxied_to_master(self):
        yield self.wait_when_connection_establied()

        proposers = set()

        def save_proposer(conn, line):
            proposers.add(conn.factory.port)

        LockProtocol.send_line_hooks = [(
            re.compile(r'^paxos_prepare.*'), save_proposer)]

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah?data=some-data' % (self.s1.http_interface, self.s1.http_port)
        )

        result = yield self.client.request(
            'POST',
            'http://%s:%s/again?data=another-data' % (self.s2.http_interface, self.s2.http_port)
        )

        result = yield getPage(
            'http://%s:%s/info/keys' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual("{'blah': 'some-data', 'again': 'another-data'}\n", result)
        self.assertEqual([4001], list(proposers))


class Paxos(TestCase):
    @inlineCallbacks
    def test_data_replicated_to_all_nodes(self):
        yield self.wait_when_connection_establied()

        for s in self.servers:
            self.assertEqual({}, s._keys)

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        for s in self.servers:
            self.assertEqual({'blah': ''}, s._keys)


    @inlineCallbacks
    def test_data_deletion_replicated_too(self):
        yield self.wait_when_connection_establied()

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        for s in self.servers:
            self.assertEqual({'blah': ''}, s._keys)

        result = yield self.client.request(
            'DELETE',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        for s in self.servers:
            self.assertEqual({}, s._keys)
            self.assertEqual(
                ['set-key blah ""', 'del-key blah'],
                s._log
            )


class Disconnections(TestCase):
    num_nodes = 5

    @inlineCallbacks
    def _run_disconnection_test(
        self,
        s1_expected_log=None,
        s2_expected_log=None,
        s3_expected_log=None,
    ):
        """Adds three keys into the cluster:
            - blah
            - minor
            - again
        """
        default_expected_log = [
            'set-key blah ""',
            'set-key minor ""',
            'set-key again ""',
        ]
        if s1_expected_log is None:
            s1_expected_log = default_expected_log
        if s2_expected_log is None:
            s2_expected_log = default_expected_log
        if s3_expected_log is None:
            s3_expected_log = default_expected_log

        yield self.wait_when_connection_establied()

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual(OK, result.code)

        result = yield self.wait_when_connection_establied()

        result = yield self.client.request(
            'POST',
            'http://%s:%s/minor' % (self.s1.http_interface, self.s1.http_port)
        )

        add_again = deferLater(reactor, 0.5, lambda:
            self.client.request(
                'POST',
                'http://%s:%s/again' % (self.s1.http_interface, self.s1.http_port)
            )
        )
        result = yield self.s3.when_sync_completed()

        result = yield add_again

        self._assert_servers_consistency('blah', 'minor', 'again')

        self.assertEqual(s1_expected_log, self.s1._log)
        self.assertEqual(s2_expected_log, self.s2._log)
        self.assertEqual(s3_expected_log, self.s3._log)

    @inlineCallbacks
    def test_no_connection_errors(self):
        yield self._run_disconnection_test()

    @inlineCallbacks
    def test_connection_lost_during_accept(self):
        """ Отключение узла когда он принимает accept."""
        def drop_connection(conn, line):
            if conn.other_side[1] == self.s3.port:
                LockProtocol.send_line_hooks = []
                self.s3.disconnect()

        LockProtocol.send_line_hooks = [(
            re.compile(r'^paxos_accept 1 .*'), drop_connection)]
        yield self._run_disconnection_test(
            s3_expected_log=[
                'set-key minor ""',
                'set-key again ""',
            ]
        )

    @inlineCallbacks
    def test_connection_lost_during_prepare(self):
        """ Отключение узла когда он принимает prepare."""
        def drop_connection(conn, line):
            if conn.other_side[1] == self.s3.port:
                LockProtocol.send_line_hooks = []
                self.s3.disconnect()


        LockProtocol.send_line_hooks = [(
            re.compile('^paxos_prepare 1$'), drop_connection)]
        yield self._run_disconnection_test()

    @inlineCallbacks
    def test_connect_node_to_working_system(self):
        """ Подключение узла (чистого) к системе в которой уже есть несколько операций. """
        result = yield self.stop_server(3)

        yield self._add_key('blah')
        yield self._add_key('minor')
        yield self._add_key('again')

        self.start_server(3)

        yield self.wait_when_connection_establied()
        yield self._add_key('and-again')

        numerated = []
        for i in xrange(10):
            numerated.append('and-%s' % i)
            yield self._add_key(numerated[-1])

        self._assert_servers_consistency(*(['blah', 'minor', 'again', 'and-again'] + numerated))
        self.assertEqual(False, self.s3.get_stale())


from mock import patch, Mock
class Sync(TestCase):
    num_nodes = 5

    def setUp(self):
        self.patch = patch('lock.lock.Syncronizer')
        self.Syncronizer = self.patch.start()
        self.Syncronizer.side_effect = lambda *args, **kwargs: Mock(wraps=Syncronizer(*args, **kwargs))
        super(Sync, self).setUp()

    def tearDown(self):
        self.patch.stop()
        super(Sync, self).tearDown()

    @inlineCallbacks
    def test_stale_not_does_not_reply_on_sync_requests(self):
        result = yield self.wait_when_connection_establied()

        ns = self.servers[0] # not stale server

        for s in self.servers[1:]:
            s._stale = True

        ns.set_stale(True) # now change state to stale

        result = yield self.client.request(
            'GET',
            'http://%s:%s/info/status' % (ns.http_interface, ns.http_port)
        )
        calls = lambda s: [item[0] for item in s.syncronizer.method_calls]

        other_calls = reduce(operator.add, (calls(s) for s in self.servers[1:]))
        self.assertEqual(['on_sync_subscribe'], other_calls)
        # No on_sync_snapshot', 'unsubscribe' should be called because two
        # other nodes are stale
        self.assertEqual(['subscribe'], calls(ns))

    @inlineCallbacks
    def test_choose_another_master(self):
        """Выбор другого мастера"""
        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual(200, result.code)
        self.assertEqual(9001, self.s2.master.http[1])

        result = yield self.client.request(
            'POST',
            'http://%s:%s/minor' % (self.s5.http_interface, self.s5.http_port)
        )
        self.assertEqual(200, result.code)
        self.assertEqual(9001, self.s2.master.http[1])

        result = yield self.stop_server(1)

        result = yield self.client.request(
            'POST',
            'http://%s:%s/again' % (self.s5.http_interface, self.s5.http_port)
        )
        body = yield get_body(result)
        self.assertEqual(200, result.code)

        # another node was choosen
        self.assertEqual(9005, self.s2.master.http[1])

        self.start_server(1)
        yield self.wait_when_connection_establied()

        # first node does not knows any master after connection was established
        self.assertEqual(None, self.s1.master)

        result = yield self.client.request(
            'POST',
            'http://%s:%s/and-again' % (self.s5.http_interface, self.s5.http_port)
        )

        # old master still does not know about the new master
        # because it received it's state as a snapshot
        yield wait_calls(lambda: self.s1.master is not None)
        self.assertEqual(9005, self.s1.master.http[1])

        result = yield self.client.request(
            'POST',
            'http://%s:%s/and-one-more' % (self.s5.http_interface, self.s5.http_port)
        )

        # now first node discovered another master
        self.assertEqual(9005, self.s1.master.http[1])
        # and it is not stale
        self.assertEqual(False, self.s1.get_stale())

        self._assert_servers_consistency(*['blah', 'minor', 'again', 'and-again', 'and-one-more'])

# какие могут быть тесты
# 7. Если большой кластер развалился на два, меньший, должен отдавать какой-нибудь подходящий HTTP код ошибки.

# Потеря мастера приводит к выбору нового, а старый мастер после переподключения, понмает, что стал slave.
