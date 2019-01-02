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
            if "uids" in data:
                self._clean_uids()
                self.uids = data["uids"]
                for each in self.uids:
                    self.factory.clients[each].add(self)
                self.factory.refresh_uids()
        except Exception, e:
            print e


from collections import defaultdict
import asyncore

from threading import Thread

#FIXME: Use only twisted
class suscriberThread(Thread):
    def run(self):
        try:
            asyncore.loop()
        except asyncore.ExitNow, e:
            print e


class BrokerPubFactory(protocol.Factory):
    def __init__(self, ip, port):
        self.clients = defaultdict(lambda: set())
        self.uids = []
        self.subscriber = None
        self.thread = None
        self.ip = ip
        self.port = port

    def refresh_uids(self):
        uids = set()
        for uid in self.clients.keys():
            uids.add(uid)
        self.uids = list(uids)
        if self.subscriber is None:
            self.subscriber = Subscriber(self.ip, self.port, self.uids, self.new_message, self.close_all)
            self.thread = suscriberThread()
            self.thread.start()
        self.subscriber.set_uids(self.uids)

    def new_message(self, uid):
        uid = uid.rstrip()
        if uid in self.clients:
            for client in self.clients[uid]:
                client.sendLine(str(uid))

    def close_all(self):
        for each in self.clients.values():
            for client in each:
                client.transport.loseConnection()
        self.subscriber = None


    def buildProtocol(self, addr):
        return BrokerPubProtocol(self)


if __name__ == '__main__':
    endpoints.serverFromString(reactor, "tcp:1026").listen(BrokerPubFactory("127.0.0.1", 1025))
    reactor.run()