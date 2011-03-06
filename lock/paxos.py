import shlex

from twisted.internet.defer import Deferred
from twisted.internet import base
from twisted.internet import reactor
from collections import deque
from logbook import Logger

from .utils import escape

base.DelayedCall.debug = True
logger = Logger('paxos')

class PaxosError(RuntimeError):
    pass

class PrepareTimeout(PaxosError):
    def __init__(self):
        super(PrepareTimeout, self).__init__('Prepare timeout')

class AcceptTimeout(PaxosError):
    def __init__(self):
        super(AcceptTimeout, self).__init__('Accept timeout')

class LearnError(PaxosError):
    def __init__(self):
        super(LearnError, self).__init__('Learned value from old iteration')


def _stop_waiting(timeout):
    if timeout is not None and not (timeout.called or timeout.cancelled):
        timeout.cancel()


class Paxos(object):
    def __init__(self, transport, on_learn, on_prepare = None, quorum_timeout=2):
        self.transport = transport
        self.on_learn = on_learn
        self.on_prepare = on_prepare
        self.quorum_timeout = quorum_timeout

        self.id = 0
        self.max_seen_id = 0

        self.proposed_value = None
        self.deferred = None
        self.queue = deque() # queue of (value, deferred) to propose

        # delayed calls for timeouts
        self._accepted_timeout = None
        self._acks_timeout = None
        self._waiting_for_result = None

    def recv(self, message, client):
        message = shlex.split(message)
        command = getattr(self, message[0])
        command(client=client, *message[1:])

    def propose(self, value):
        deferred = Deferred()
        if self.proposed_value is None:
            self._start_paxos(value, deferred)
        else:
            self.queue.append((value, deferred))
            logger.debug('Request for %s was queued, queue size is %s (because we are proposing %s now)' % (
                    value,
                    len(self.queue),
                    self.proposed_value,
                )
            )
        return deferred

    def _start_paxos(self, value, deferred):
        """Starts paxos iteration proposing given value."""
        self.id = self.max_seen_id + 1
        self.proposed_value = value
        self.deferred = deferred

        self._num_acks_to_wait = self.transport.quorum_size

        def _timeout_callback():
            logger.info('+++ prepare timeout')
            # TODO sometimes self.deferred is None when this callbach is called
            self.deferred.errback(PrepareTimeout())
            self.deferred = None
            self.proposed_value = None

        self._acks_timeout = reactor.callLater(self.quorum_timeout, _timeout_callback)
        self.transport.broadcast('paxos_prepare %s' % self.id)

    def paxos_prepare(self, num, client):
        num = int(num)
        if num > self.max_seen_id:
            if self.on_prepare is not None:
                self.on_prepare(num, client)

            self.max_seen_id = num
            self._send_to(client, 'paxos_ack %s' % num)

    def paxos_ack(self, num, client):
        num = int(num)
        if self.proposed_value is not None and num == self.id:
            self._num_acks_to_wait -= 1
            if self._num_acks_to_wait == 0:
                _stop_waiting(self._acks_timeout)

                self._num_accepts_to_wait = self.transport.quorum_size

                def _timeout_callback():
                    logger.info('+++ accept timeout')
                    self.deferred.errback(AcceptTimeout())
                    self.deferred = None
                    self.proposed_value = None

                self._accepted_timeout = reactor.callLater(
                    self.quorum_timeout,
                    _timeout_callback
                )
                self.transport.broadcast('paxos_accept %s "%s"' % (self.id, escape(self.proposed_value)))

    def paxos_accept(self, num, value, client):
        num = int(num)
        if num == self.max_seen_id:
            self._send_to(client, 'paxos_accepted %s' % num)

    def paxos_accepted(self, num, client):
        num = int(num)
        if self.proposed_value is not None and num == self.id:
            self._num_accepts_to_wait -= 1
            if self._num_accepts_to_wait == 0:
                _stop_waiting(self._accepted_timeout)
                self.transport.broadcast('paxos_learn %s "%s"' % (self.id, escape(self.proposed_value)))
                self._waiting_for_result = num

    def paxos_learn(self, num, value, client):
        logger.info('paxos.learn %s' % value)

        num = int(num)
        if num == self.max_seen_id:
            try:
                result = self.on_learn(num, value)
            except Exception, e:
                logger.exception('paxos.learn %s' % value)
                result = e

            if self._waiting_for_result == num:
                self._waiting_for_result = None
                if isinstance(result, Exception):
                    logger.warning('returning error from paxos.learn %s, %s' % (value, result))
                    self.deferred.errback(result)
                else:
                    logger.warning('returning success from paxos.learn %s' % value)
                    self.deferred.callback(result)

                logger.debug('queue size: %s' % len(self.queue))
                if self.queue:
                    # start new Paxos instance
                    # for next value from the queue
                    next_value, deferred = self.queue.pop()
                    logger.debug('next value from the queue: %s' % value)
                    self._start_paxos(next_value, deferred)
                else:
                    self.proposed_value = None
                    self.deferred = None
        else:
            logger.debug('learned value from old iteration')
            if self.deferred is not None:
                self.deferred.errback(LearnError())

            if self.queue:
                # start new Paxos instance
                # for next value from the queue
                next_value, deferred = self.queue.pop()
                logger.debug('next value from the queue: %s' % value)
                self._start_paxos(next_value, deferred)
            else:
                self.proposed_value = None
                self.deferred = None


    def get_state(self):
        """Used to serialize paxos state for node sync."""
        return dict(
            id=self.id,
            max_seen_id=self.max_seen_id,
        )

    def set_state(self, data):
        """Used to restore paxos state during node sync."""
        self.id = data['id']
        self.max_seen_id = data['max_seen_id']

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
