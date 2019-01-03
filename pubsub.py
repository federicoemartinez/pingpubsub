__author__ = 'Fede M'

from twisted.logger import Logger, textFileLogObserver
import io
log = Logger(observer=textFileLogObserver(io.open("pubsub.log",'a')))

#Code for PyInstaller
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
        self.uid = None


    def connectionLost(self, reason):
        if self.uid and self.uid in self.factory.clients and self.factory.clients[self.uid] == self:
            log.info("removing " + str(self.uid))
            del self.factory.clients[self.uid]

    def lineReceived(self, line):
        log.debug("{line!r}", line=line)
        if self.uid is None:
            try:
                data = json.loads(line.rstrip())
                if "to" in data:
                    client = self.factory.clients.get(data["to"])
                    if client: client.sendLine("ping")
                else:
                    if "uid" in data:
                        self.uid = data["uid"]
                        if self.uid in self.factory.clients:
                           log.warn("UID {uid} is trying to register but it is already registered", uid=self.uid)
                           other_client = self.factory.clients[self.uid]
                           other_client.transport.loseConnection()
                        self.factory.clients[self.uid] = self
            except Exception, e:
                log.error("{message!r}", message = e.message)
                log.error("{line!r}", line=line)
        else:
            log.warn("{line!r}", line=line)


class PubFactory(protocol.Factory):
    def __init__(self):
        self.clients = {}

    def buildProtocol(self, addr):
        return PubProtocol(self)


if __name__ == '__main__':
    endpoints.serverFromString(reactor, "tcp:1025").listen(PubFactory())
    reactor.run()