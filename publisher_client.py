import json
import socket


class PublisherClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def publish(self, uid):
        try:
            d = {"to": uid}
            data = json.dumps(d)
            s = socket.socket()
            s.connect((self.host, self.port))
            x = s.send(data + "\r\n")
            if x:
                return True
        except Exception, e:
            print(e)
        return False


if __name__ == '__main__':
    import time

    pc = PublisherClient("127.0.0.1", 1025)

    pc.publish("test")
    time.sleep(2)
    pc.publish("test1")
    time.sleep(2)
    pc.publish("4")

