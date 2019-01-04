import json
import socket
import select



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
                s.setblocking(0)
                ready = select.select([s], [], [], 3)
                if ready[0]:
                    data = s.recv(4096)
                    print data
                return True

        except Exception, e:
            print(e)
        return False

    def list(self):
        try:
            d = {"command": "list"}
            data = json.dumps(d)
            s = socket.socket()
            s.connect((self.host, self.port))
            x = s.send(data + "\r\n")
            if x:
                return s.recv(8000)
        except Exception, e:
            print(e)
        return False

    def subscribed(self, uid):
        try:
            d = {"command": "subscribed", "args":{"uid":uid}}
            data = json.dumps(d)
            s = socket.socket()
            a = s.connect((self.host, self.port))
            x = s.send(data + "\r\n")
            if x:
                return s.recv(8000)
        except Exception, e:
            print(e)
        return False

if __name__ == '__main__':
    import time

    pc = PublisherClient("127.0.0.1", 1025)
    print pc.subscribed("test1")
    print pc.subscribed("caca")
    pc.publish("test")
    time.sleep(2)
    pc.publish("test1")
    time.sleep(2)
    pc.publish("4")
    print pc
    print pc.list()

