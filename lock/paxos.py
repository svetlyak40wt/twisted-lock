# -*- coding: utf-8 -*-
import shlex

from twisted.internet.defer import Deferred
from twisted.internet import base
from twisted.internet import reactor
from collections import deque
from logbook import Logger
from bisect import insort

from .utils import escape

base.DelayedCall.debug = True

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
    def __init__(self, transport, on_learn, on_prepare=None,
            on_stale=None, quorum_timeout=3,
            logger_group=None,
        ):
        self._logger = Logger('paxos')
        if logger_group is not None:
            logger_group.add_logger(self._logger)

        self.transport = transport
        self.on_learn = on_learn
        self.on_prepare = on_prepare
        self.on_stale = on_stale
        self.quorum_timeout = quorum_timeout

        self.id = 0
        self.max_seen_id = 0
        self.last_accepted_id = 0
        self._logger.debug('2 last_accepted_id=%(last_accepted_id)s' % self.__dict__)

        self.proposed_value = None
        self.deferred = None
        self.queue = deque() # queue of (value, deferred) to propose
        self._learn_queue = [] # sorted list with learn requests which come out of order

        # delayed calls for timeouts
        self._accepted_timeout = None
        self._acks_timeout = None
        self._waiting_to_learn_id = deque()

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
            self._logger.debug('Request for %s was queued, queue size is %s (because we are proposing %s now)' % (
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
            self._logger.info('+++ prepare timeout')
            # TODO sometimes self.deferred is None when this callbach is called
            self.deferred.errback(PrepareTimeout())
            self.deferred = None
            self.proposed_value = None

        self._acks_timeout = reactor.callLater(self.quorum_timeout, _timeout_callback)
        self.transport.broadcast('paxos_prepare %s %s' % (self.id, self.last_accepted_id))

    def paxos_prepare(self, num, last_accepted_id, client):
        num = int(num)
        last_accepted_id = int(last_accepted_id)

        if last_accepted_id > self.last_accepted_id:
            # Move to the "stale" state
            self._logger.debug('stale last_accepted_id(%s) < self.last_accepted_id(%s)' % (
                last_accepted_id, self.last_accepted_id
            ))
            self.on_stale(last_accepted_id)
        else:
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
                    self._logger.info('+++ accept timeout')
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
            if self.id == num:
                # we have a deferred to return result in this round
                self._waiting_to_learn_id.append((num, self.deferred))
            else:
                # may be we have deferred but it is for another Paxos round
                self._waiting_to_learn_id.append((num, None))
            self._send_to(client, 'paxos_accepted %s' % num)

    def paxos_accepted(self, num, client):
        num = int(num)
        if self.proposed_value is not None and num == self.id:
            self._num_accepts_to_wait -= 1
            if self._num_accepts_to_wait == 0:
                _stop_waiting(self._accepted_timeout)
                self.transport.broadcast('paxos_learn %s "%s"' % (self.id, escape(self.proposed_value)))

    def paxos_learn(self, num, value, client):
        self._logger.info('paxos.learn %s' % value)

        num = int(num)
        if self._waiting_to_learn_id and num == self._waiting_to_learn_id[0][0]:
            num, deferred = self._waiting_to_learn_id.popleft()

            try:
                result = self.on_learn(num, value, client)
            except Exception, e:
                self._logger.exception('paxos.learn %s' % value)
                result = e

            self.last_accepted_id = num
            self._logger.debug('1 last_accepted_id=%(last_accepted_id)s' % self.__dict__)

            if deferred is not None and value == self.proposed_value:
                # this works for current round coordinator only
                # because it must return result to the client
                # and to start a new round for next request

                if isinstance(result, Exception):
                    self._logger.warning('returning error from paxos.learn %s, %s' % (value, result))
                    deferred.errback(result)
                else:
                    self._logger.warning('returning success from paxos.learn %s' % value)
                    deferred.callback(result)

                self._logger.debug('queue size: %s' % len(self.queue))
                if self.queue:
                    # start new Paxos instance
                    # for next value from the queue
                    next_value, deferred = self.queue.pop()
                    self._logger.debug('next value from the queue: %s' % next_value)
                    self._start_paxos(next_value, deferred)
                else:
                    self.proposed_value = None
                    self.deferred = None

            if self._learn_queue:
                self._logger.debug('relearning remembered values')
                # clear queue because it will be filled again if needed
                queue, self._learn_queue = self._learn_queue, []
                for args in queue:
                    self.paxos_learn(*args)

        else:
            self._logger.debug('learned value from another iteration (num=%s, waiting_to_learn=%s)' % (num, self._waiting_to_learn_id))
            insort(self._learn_queue, (num, value, client))
            # TODO add timeout for learn_queue emptiness


    def get_state(self):
        """Used to serialize paxos state for node sync."""
        return dict(
            id=self.id,
            max_seen_id=self.max_seen_id,
            last_accepted_id=self.last_accepted_id,
        )

    def set_state(self, data):
        """Used to restore paxos state during node sync."""
        self.id = data['id']
        self.max_seen_id = data['max_seen_id']
        self.last_accepted_id = data['last_accepted_id']
        self._logger.debug('3 last_accepted_id=%(last_accepted_id)s' % self.__dict__)

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
