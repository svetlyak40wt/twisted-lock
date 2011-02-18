import shlex

from twisted.internet.defer import Deferred
from twisted.internet import base
from twisted.internet import reactor
from collections import deque

base.DelayedCall.debug = True

class PaxosError(RuntimeError):
    pass

class PrepareTimeout(PaxosError):
    pass

class AcceptTimeout(PaxosError):
    pass


def _stop_waiting(timeout):
    if timeout is not None and not (timeout.called or timeout.cancelled):
        timeout.cancel()


class Paxos(object):
    def __init__(self, transport, quorum_timeout=2):
        self.transport = transport
        self.quorum_timeout = quorum_timeout

        self.id = 0
        self.max_seen_id = 0

        self.proposed_value = None
        self.deferred = None
        self.queue = deque() # queue of (value, deferred) to propose

        # delayed calls for timeouts
        self._accepted_timeout = None
        self._acks_timeout = None

    def recv(self, message, client):
        message = shlex.split(message)
        command = getattr(self, message[0])
        command(client=client, *message[1:])

    def propose(self, value):
        deferred = Deferred()
        if self.proposed_value is None:
            self._start_paxos(value, deferred)
            return deferred
        else:
            self.queue.append((value, deferred))

    def _start_paxos(self, value, deferred):
        """Starts paxos iteration proposing given value."""
        self.id += 1
        self.proposed_value = value
        self.deferred = deferred

        self._num_acks_to_wait = self.transport.quorum_size

        def _timeout_callback():
            print '+++ prepare timeout'
            self.deferred.errback(PrepareTimeout)

        self._acks_timeout = reactor.callLater(self.quorum_timeout, _timeout_callback)
        self.transport.broadcast('paxos_prepare %s' % self.id)

    def paxos_prepare(self, num, client):
        num = int(num)
        if num > self.max_seen_id:
            self.max_seen_id = num
            self._send_to(client, 'paxos_ack %s' % num)

    def paxos_ack(self, num, client):
        num = int(num)
        if num == self.id:
            self._num_acks_to_wait -= 1
            if self._num_acks_to_wait == 0:
                _stop_waiting(self._acks_timeout)

                self._num_accepts_to_wait = self.transport.quorum_size

                def _timeout_callback():
                    print '+++ accept timeout'
                    self.deferred.errback(AcceptTimeout)

                self._accepted_timeout = reactor.callLater(
                    self.quorum_timeout,
                    _timeout_callback
                )
                self.transport.broadcast('paxos_accept %s "%s"' % (self.id, self.proposed_value))

    def paxos_accept(self, num, value, client):
        num = int(num)
        if num == self.max_seen_id:
            self._send_to(client, 'paxos_accepted %s' % num)

    def paxos_accepted(self, num, client):
        num = int(num)
        if num == self.id:
            self._num_accepts_to_wait -= 1
            if self._num_accepts_to_wait == 0:
                _stop_waiting(self._accepted_timeout)
                self.transport.broadcast('paxos_learn %s "%s"' % (self.id, self.proposed_value))

    def paxos_learn(self, num, value, client):
        num = int(num)
        self.id = num
        self.transport.learn(num, value)
        if self.deferred is not None and \
                self.proposed_value == value:

            self.deferred.callback((num, value))

            if self.queue:
                # start new Paxos instance
                # for next value from the queue
                next_value, deferred = self.queue.pop()
                self._start_paxos(next_value, deferred)
            else:
                self.proposed_value = None
                self.deferred = None

    def _send_to(self, client, message):
        client.send(message, self.transport)

    def _get_timeouts(self):
        return [
            self._accepted_timeout,
            self._acks_timeout,
        ]

    def _cancel_timeouts(self):
        for timeout in self._get_timeouts():
            _stop_waiting(timeout)

