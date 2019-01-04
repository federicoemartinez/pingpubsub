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

    def __init__(self, factory):
        self.factory = factory
        self.uids = []
        self.ident = str(uuid.uuid4())




    def connectionLost(self, reason):
        self._clean_uids()

    def _clean_uids(self):
        if self.uids:
            for each in self.uids:
                if each in self.factory.clients and self in self.factory.clients[each]:
                    self.factory.clients[each].remove(self)
                    if len(self.factory.clients[each]) == 0:
                        del self.factory.clients[each]




    def no_ack_timeout(self, uid_conversation, uid_to):
        if uid_conversation in self.factory.waiting_acks:
            log.warn('No ack for %s' % (uid_conversation,))
            data = {'ack':0, 'error': 'time out, unable to contact'}
            self.sendLine(json.dumps(data))
            clients = self.factory.waiting_acks[uid_conversation]["clients"]
            for client in clients:
                if len(client.uids) == 1:
                    try:
                        log.warn('Aborting connection because the only uid it had is not responding %s' % (uid_conversation,))
                        client.transport.abortConnection()
                    except Exception, e:
                        log.failure("{message!r}", message=e.message)
                client.uids.remove(uid_to)
            self.factory.uids.remove(uid_to)
            del self.factory.clients[uid_to]
        else:
            log.warn('Callback called but should have not been for conversation %s' % (uid_conversation,))

    def process_ack(self, uid_conversation, ack = 1):
        if uid_conversation in self.factory.waiting_acks:
            log.debug('Processing ack for conversation %s' % (uid_conversation,))
            data = {'ack':ack, 'uid_conversation':uid_conversation}
            channel = self.factory.waiting_acks[uid_conversation]["channel"]
            channel.sendLine(json.dumps(data))
            del self.factory.waiting_acks[uid_conversation]
            if uid_conversation in self.factory.conversation_callbacks:
                self.factory.conversation_callbacks[uid_conversation].cancel()
                del self.factory.conversation_callbacks[uid_conversation]
        else:
            log.warn('Tried to process an ack that is not present %s' % (uid_conversation,))

    def send_uid_no_registered(self):
        data = {'ack': 0, 'error': 'uid not registered'}
        self.sendLine(json.dumps(data))

    def lineReceived(self, line):
        log.debug("{line!r}", line=line)
        try:
            data = json.loads(line.rstrip())
            if "ack" in data:
                if data["ack"] == 1:
                    self.process_ack(data["uid_conversation"])
            if "to" in data:
                uid_to = data["to"]
                clients = self.factory.clients.get(uid_to)
                if clients:
                        uid_conversation = str(uuid.uuid4())
                        self.factory.waiting_acks[uid_conversation] = {"clients":clients, "channel":self}
                        data = json.dumps({'uid_to': uid_to, 'uid_conversation':uid_conversation})
                        for client in clients:
                            try:
                                client.sendLine(data)
                            except Exception, e:
                                log.failure("{message!r}", message=e.message)
                                client.transport.loseConnection()
                        callback = reactor.callLater(1.7, self.no_ack_timeout, uid_conversation, uid_to)
                        self.factory.conversation_callbacks[uid_conversation] = callback
                else:
                    self.send_uid_no_registered()

            elif "uids" in data:
                    self._clean_uids()
                    self.uids = data["uids"]
                    for each in self.uids:
                        self.factory.clients[each].add(self)
            elif "command" in data:
                if data["command"] == "list":
                    self.sendLine(json.dumps(self.factory.clients.keys()))
                if data["command"] == "subscribed":
                    self.sendLine(json.dumps(data["args"]['uid'] in self.factory.clients))
            else:
                log.warn("{line!r}", line=line)

        except Exception, e:
            log.failure("{message!r}", message=e.message)
            log.error("{line!r}", line=line)


from collections import defaultdict


class PubFactory(protocol.Factory):
    def __init__(self):
        self.clients = defaultdict(lambda: set())
        self.waiting_acks = {}
        self.conversation_callbacks = {}

    def buildProtocol(self, addr):
        return PubProtocol(self)


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