import shlex

class Paxos(object):
    def __init__(self, transport):
        self.transport = transport
        self.id = 0
        self.max_seen_id = 0

        self.proposed_value = None

    def recv(self, message, client):
        message = shlex.split(message)
        command = getattr(self, message[0])
        command(client=client, *message[1:])

    def propose(self, value):
        self.id += 1
        self.proposed_value = value

        self._num_acks_to_wait = self.transport.quorum_size
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
                self._num_accepts_to_wait = self.transport.quorum_size
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
                self.transport.broadcast('paxos_learn %s "%s"' % (self.id, self.proposed_value))

    def paxos_learn(self, num, value, client):
        num = int(num)
        self.transport.learn(num, value)

    def _send_to(self, client, message):
        client.send(message, self.transport)

