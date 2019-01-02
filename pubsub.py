__author__ = 'Fede M'

import io

from twisted.logger import Logger, textFileLogObserver


log = Logger(observer=textFileLogObserver(io.open("pubsub.log", 'a')))

# Code for PyInstaller
import sys

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


    def connectionLost(self, reason):
        self._clean_uids()

    def _clean_uids(self):
        if self.uids:
            for each in self.uids:
                if each in self.factory.clients and self in self.factory.clients[each]:
                    self.factory.clients[each].remove(self)
                    if len(self.factory.clients[each]) == 0:
                        del self.factory.clients[each]

    def lineReceived(self, line):
        log.debug("{line!r}", line=line)
        #if self.uids == []:
        try:
            data = json.loads(line.rstrip())
            if "to" in data:
                uid_to = data["to"]
                clients = self.factory.clients.get(uid_to)
                if clients:
                    for client in clients:
                        try:
                            client.sendLine(str(uid_to))
                        except Exception, e:
                            log.error("{message!r}", message=e.message)
                            client.transport.loseConnection()
            elif "uids" in data:
                    self._clean_uids()
                    self.uids = data["uids"]
                    for each in self.uids:
                        self.factory.clients[each].add(self)
            else:
                log.warn("{line!r}", line=line)

        except Exception, e:
            log.error("{message!r}", message=e.message)
            log.error("{line!r}", line=line)


from collections import defaultdict


class PubFactory(protocol.Factory):
    def __init__(self):
        self.clients = defaultdict(lambda: set())

    def buildProtocol(self, addr):
        return PubProtocol(self)


if __name__ == '__main__':
    endpoints.serverFromString(reactor, "tcp:1025").listen(PubFactory())
    reactor.run()