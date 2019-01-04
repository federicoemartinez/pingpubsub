__author__ = 'Fede M'
import json

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

from pubsub import PubProtocol
from twisted.internet import reactor, protocol, endpoints
from twisted.protocols import basic
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.python import log
from twisted.python.logfile import DailyLogFile
from twisted.logger import Logger, textFileLogObserver

logfile = DailyLogFile.fromFullPath("broker.log")
log.startLogging(logfile)

log = Logger(observer=textFileLogObserver(logfile))


class BrokerPubProtocol(PubProtocol):
    def __init__(self, factory, serializer=json):
        self.factory = factory
        self.uids =set()
        self.serializer = json

    def connectionLost(self, reason):
        self._clean_uids()
        self.factory.refresh_uids()

    def _unsupported_messages(self):
        return ("to", "command", )

    def process_new_subscription(self):
        self.factory.refresh_uids()

    def lineReceived(self, line):
        try:
            data = self.serializer.loads(line.rstrip())
            for each in self._unsupported_messages():
                if each in data:
                    log.error("Unsupported message {message!r}", message = line.rstrip())
                    self.transport.abortConnection()
            self.process_line_data(data, line)
        except Exception, e:
            log.failure(e.message)
            self.transport.abortConnection()

from collections import defaultdict
from twisted.internet.endpoints import TCP4ClientEndpoint


class BrokerPubFactory(protocol.Factory):
    def __init__(self, ip, port, serializer=json):
        self.clients = defaultdict(lambda: set())
        self.uids = set()
        self.subscriber = None
        self.thread = None
        self.ip = ip
        self.port = port
        self.defer = None
        self.subfactory = SubscriberClientFactory(self, serializer)
        self.secs_to_wait = 0.5
        self.suscribe()
        self.waiting_acks = {}
        self.conversation_callbacks = {}
        self.serializer = serializer

    def refresh_uids(self):
        self.uids = set(self.clients.keys())
        if self.subscriber is None:
            self.suscribe()
        else:
            self.subscriber.set_uids(self.uids)

    def suscribe(self):
        if self.defer is None:
            point = TCP4ClientEndpoint(reactor, self.ip, self.port)
            self.defer = point.connect(self.subfactory)
            self.defer.addCallback(self.set_suscriber)
            self.defer.addErrback(self.connection_failed)

    def connection_lost(self):
        log.warn("Connection lost")
        self.subscriber = None
        reactor.callLater(self.secs_to_wait, self.suscribe)

    def connection_failed(self, reason):
        log.error("Connection failed: %s" % (reason,))
        self.defer = None
        self.secs_to_wait = 10 if self.secs_to_wait >= 10 else self.secs_to_wait * 2
        self.connection_lost()

    def set_suscriber(self, p):
        self.subscriber = p
        self.defer = None
        self.subscriber.set_uids(self.uids)

    def send_uid_no_registered(self):
        data = {'ack': 0, 'error': 'uid not registered'}
        self.subscriber.sendLine(self.serializer.dumps(data))

    def new_message(self, data):
        if "uid_to" in data:
            uid_to = data["uid_to"]
            clients = self.clients.get(uid_to)
            uid_conversation = data['uid_conversation']
            if clients:
                self.waiting_acks[uid_conversation] = {"clients": clients, "channel": self.subscriber}
                data = self.serializer.dumps({'uid_to': uid_to, 'uid_conversation': uid_conversation})
                for client in clients:
                    try:
                        log.debug('Going to send message for uid %s in conversation %s' % (uid_to, uid_conversation,))
                        client.sendLine(data)
                    except Exception, e:
                        log.failure("ERROR in new_message: {message!r}", message=e.message)
                        client.transport.loseConnection()
                callback = reactor.callLater(1.7, BrokerPubProtocol.no_ack_timeout, self, self.subscriber, uid_conversation, uid_to)
                self.conversation_callbacks[uid_conversation] = callback
            else:
                self.send_uid_no_registered()


    def close_all(self):
        for each in self.clients.values():
            for client in each:
                client.transport.loseConnection()
        self.subscriber = None

    def buildProtocol(self, addr):
        return BrokerPubProtocol(self)


class Subscriber(basic.LineReceiver):
    def __init__(self, broker, serializer):
        self.broker = broker
        self.uids = []
        self.registered = False
        self.serializer = serializer

    def lineReceived(self, line):
        data = self.serializer.loads(line.rstrip())
        self.broker.new_message(data)

    def set_uids(self, uids):
        self.uids = uids
        self.registered = False
        self.register()

    def register(self):
        d = self.serializer.dumps({"uids": list(self.uids)})
        x = self.sendLine(d)
        if x:
            self.registered = True

    def connectionLost(self, reason):
        self.broker.connection_lost()


class SubscriberClientFactory(ReconnectingClientFactory):
    def __init__(self, broker, serializer=json):
        self.broker = broker
        self.serializer = serializer

    def buildProtocol(self, addr):
        s = Subscriber(self.broker, self.serializer)
        s.factory = self
        return s


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Starts a new pubsub broker hub for the ping protocol')
    parser.add_argument('--port', metavar='P', type=int,
                        help='Port to listen', default=1026)
    parser.add_argument('--ip', dest='ip',
                        help='ip to listen', default="127.0.0.1")
    parser.add_argument('--dest_port', type=int,
                        help='Port to connect', default=1025)
    parser.add_argument('--dest_ip', dest='dest_ip',
                        help='ip to connect', default="127.0.0.1")
    args = parser.parse_args()

    endpoints.serverFromString(reactor, "tcp:interface=%s:port=%s" % (args.ip, args.port)).listen(BrokerPubFactory(args.dest_ip, args.dest_port))
    reactor.run()
