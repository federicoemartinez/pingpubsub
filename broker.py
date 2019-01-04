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
from suscriber_client import Subscriber
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
    def __init__(self, factory):
        self.factory = factory
        self.uids = []

    def connectionLost(self, reason):
        self._clean_uids()
        self.factory.refresh_uids()

    def lineReceived(self, line):
        try:
            data = json.loads(line.rstrip())
            if 'ack' in data:
                if data["ack"] == 1:
                    self.process_ack(data["uid_conversation"])
            elif "uids" in data:
                self._clean_uids()
                self.uids = data["uids"]
                for each in self.uids:
                    self.factory.clients[each].add(self)
                self.factory.refresh_uids()

        except Exception, e:
            log.error(e.message)


from collections import defaultdict
from twisted.internet.endpoints import TCP4ClientEndpoint


class BrokerPubFactory(protocol.Factory):
    def __init__(self, ip, port):
        self.clients = defaultdict(lambda: set())
        self.uids = []
        self.subscriber = None
        self.thread = None
        self.ip = ip
        self.port = port
        self.defer = None
        self.subfactory = SubscriberClientFactory(self)
        self.secs_to_wait = 0.5
        self.suscribe()
        self.waiting_acks = {}

    def refresh_uids(self):
        uids = set()
        for uid in self.clients.keys():
            uids.add(uid)
        self.uids = list(uids)
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
        self.subscriber.sendLine(json.dumps(data))

    def process_ack(self, uid_conversation, ack = 1):
        if uid_conversation in self.factory.waiting_acks:
            data = {'ack':ack, 'uid_conversation':uid_conversation}
            channel = self.factory.waiting_acks[uid_conversation]["channel"]
            channel.sendLine(json.dumps(data))
            del self.factory.waiting_acks[uid_conversation]

    def no_ack_timeout(self, uid_conversation):
        if uid_conversation in self.waiting_acks:
            data = {'ack': 0, 'error': 'time out, unable to contact'}
            self.subscriber.sendLine(json.dumps(data))
            clients = self.factory.waiting_acks[uid_conversation]
            for client in clients:
                try:
                    client.transport.abortConnection()
                except Exception, e:
                    log.error("{message!r}", message=e.message)

    def new_message(self, data):
        if "uid_to" in data:
            uid_to = data["uid_to"]
            clients = self.clients.get(uid_to)
            uid_conversation = data['uid_conversation']
            if clients:
                self.waiting_acks[uid_conversation] = {"clients": clients, "channel": self.subscriber}
                data = json.dumps({'uid_to': uid_to, 'uid_conversation': uid_conversation})
                for client in clients:
                    try:
                        client.sendLine(data)
                    except Exception, e:
                        log.error("{message!r}", message=e.message)
                        client.transport.loseConnection()
                reactor.callLater(1.7, self.no_ack_timeout, uid_conversation)
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
    def __init__(self, broker):
        self.broker = broker
        self.uids = []
        self.registered = False

    def lineReceived(self, line):
        data = json.loads(line.rstrip())
        self.broker.new_message(data)

    def set_uids(self, uids):
        self.uids = uids
        self.registered = False
        self.register()

    def register(self):
        d = json.dumps({"uids": self.uids})
        x = self.sendLine(d)
        if x:
            self.registered = True

    def connectionLost(self, reason):
        self.broker.connection_lost()


class SubscriberClientFactory(ReconnectingClientFactory):
    def __init__(self, broker):
        self.broker = broker

    def buildProtocol(self, addr):
        s = Subscriber(self.broker)
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
