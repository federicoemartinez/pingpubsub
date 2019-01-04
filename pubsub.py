__author__ = 'Fede M'

import io

from twisted.logger import Logger, textFileLogObserver


from twisted.python import log
from twisted.python.logfile import DailyLogFile
logfile = DailyLogFile.fromFullPath("pubsub.log")
log.startLogging(logfile)

log = Logger(observer=textFileLogObserver(logfile))


# Code for PyInstaller
import sys
import uuid
if 'twisted.internet.reactor' in sys.modules:
    del sys.modules['twisted.internet.reactor']

# Try to use the best reactor available

try:
    from twisted.internet import epollreactor

    epollreactor.install()
except:
    try:
        from twisted.internet import iocpreactor

        iocpreactor.install()
    except:
        from twisted.internet import selectreactor

        selectreactor.install()

from twisted.internet import reactor, protocol, endpoints
from twisted.protocols import basic
import json


class PubProtocol(basic.LineReceiver):

    def __init__(self, factory, serializer):
        self.factory = factory
        self.uids = set()
        self.serializer = serializer


    def connectionLost(self, reason):
        self._clean_uids()

    def _clean_uids(self):
        if self.uids:
            for each in self.uids:
                if each in self.factory.clients and self in self.factory.clients[each]:
                    self.factory.clients[each].remove(self)
                    if len(self.factory.clients[each]) == 0:
                        del self.factory.clients[each]

    @classmethod
    def no_ack_timeout(cls, factory, channel, uid_conversation, uid_to):
        if uid_conversation in factory.waiting_acks:
            log.warn('No ack for %s to %s' % (uid_conversation,uid_to))
            data = {'ack':0, 'error': 'time out, unable to contact'}
            channel.sendLine(factory.serializer.dumps(data))
            clients = factory.waiting_acks[uid_conversation]["clients"]
            for client in clients:
                log.warn('UIDS for this client: %s' % client.uids)
                if len(client.uids) == 1:
                    try:
                        log.warn('Aborting connection because the only uid it had is not responding %s' % (uid_conversation,))
                        client.transport.abortConnection()
                    except Exception, e:
                        log.failure("{message!r}", message=e.message)
                client.uids.remove(uid_to)
            del factory.clients[uid_to]
        else:
            log.warn('Callback called but should have not been for conversation %s' % (uid_conversation,))

    def process_ack(self, uid_conversation, ack = 1):
        if uid_conversation in self.factory.waiting_acks:
            log.debug('Processing ack for conversation %s' % (uid_conversation,))
            data = {'ack':ack, 'uid_conversation':uid_conversation}
            channel = self.factory.waiting_acks[uid_conversation]["channel"]
            channel.sendLine(self.serializer.dumps(data))
            del self.factory.waiting_acks[uid_conversation]
            if uid_conversation in self.factory.conversation_callbacks:
                self.factory.conversation_callbacks[uid_conversation].cancel()
                del self.factory.conversation_callbacks[uid_conversation]
        else:
            log.warn('Tried to process an ack that is not present %s' % (uid_conversation,))

    def send_uid_no_registered(self):
        data = {'ack': 0, 'error': 'uid not registered'}
        self.sendLine(self.serializer.dumps(data))

    def process_new_subscription(self):
        pass

    def process_line_data(self,data, raw_line):
        if "ack" in data:
                if data["ack"] == 1:
                    self.process_ack(data["uid_conversation"])
                else:
                    log.info("Un-ack received: {message!r}", message=raw_line.rstrip() )
        if "to" in data:
            uid_to = data["to"]
            clients = self.factory.clients.get(uid_to)
            if clients:
                    uid_conversation = str(uuid.uuid4())
                    self.factory.waiting_acks[uid_conversation] = {"clients":clients, "channel":self}
                    data = self.serializer.dumps({'uid_to': uid_to, 'uid_conversation':uid_conversation})
                    for client in clients:
                        try:
                            client.sendLine(data)
                        except Exception, e:
                            log.failure("{message!r}", message=e.message)
                            client.transport.loseConnection()
                    callback = reactor.callLater(1.7, PubProtocol.no_ack_timeout, self.factory, self, uid_conversation, uid_to)
                    self.factory.conversation_callbacks[uid_conversation] = callback
            else:
                self.send_uid_no_registered()

        elif "uids" in data:
                self._clean_uids()
                self.uids = set(data["uids"])
                for each in self.uids:
                    self.factory.clients[each].add(self)
                self.process_new_subscription()
        elif "command" in data:
            if data["command"] == "list":
                self.sendLine(self.serializer.dumps(self.factory.clients.keys()))
            if data["command"] == "subscribed":
                self.sendLine(self.serializer.dumps(data["args"]['uid'] in self.factory.clients))
        else:
            log.warn("{line!r}", line=raw_line)


    def lineReceived(self, line):
        log.debug("{line!r}", line=line)
        try:
            data = self.serializer.loads(line.rstrip())
            self.process_line_data(data, line)

        except Exception, e:
            log.failure("{message!r}", message=e.message)
            log.error("{line!r}", line=line)


from collections import defaultdict


class PubFactory(protocol.Factory):
    def __init__(self, serializer=json):
        self.clients = defaultdict(lambda: set())
        self.waiting_acks = {}
        self.conversation_callbacks = {}
        self.serializer = serializer

    def buildProtocol(self, addr):
        return PubProtocol(self, self.serializer)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Starts a new pubsub hub for the ping protocol')
    parser.add_argument('--port', metavar='P', type=int,
                        help='Port to listen', default=1025)
    parser.add_argument('--ip', dest='ip',
                        help='ip to listen', default="127.0.0.1")

    args = parser.parse_args()

    endpoints.serverFromString(reactor, "tcp:interface=%s:port=%s" % (args.ip, args.port)).listen(PubFactory())
    reactor.run()