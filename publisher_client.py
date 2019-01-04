import json
import socket
import select



class PublisherClient(object):

    TIMEOUT_SECONDS = 3

    def __init__(self, host, port, serializer=json):
        self.host = host
        self.port = port
        self.serializer=serializer

    def publish(self, uid):
        d = {"to": uid}
        data = self.serializer.dumps(d)
        s = socket.socket()
        s.connect((self.host, self.port))
        x = s.send(data + "\r\n")
        res = False
        if x:
            s.setblocking(0)
            ready = select.select([s], [], [], PublisherClient.TIMEOUT_SECONDS)
            if ready[0]:
                data = s.recv(4096)
                ret = self.serializer.loads(data.strip())
                res = ret["ack"] == 1
        s.close()
        return res

    def list(self):
            d = {"command": "list"}
            data = self.serializer.dumps(d)
            s = socket.socket()
            s.connect((self.host, self.port))
            x = s.send(data + "\r\n")
            stream_data = []
            if x:
                s.setblocking(0)
                while(True):
                    ready = select.select([s], [], [], PublisherClient.TIMEOUT_SECONDS)
                    if ready[0]:
                        data = s.recv(4096)
                        if data:
                            stream_data.append(data)
                            if "\n" in data: break
                        else:
                            break
                    else:
                        stream_data = []
                        break
                if stream_data:
                    string_data = "".join(stream_data).rstrip()
                    res = self.serializer.loads(string_data)
                else:
                    res = []
                s.close()
                return res
            return []

    def subscribed(self, uid):

        d = {"command": "subscribed", "args":{"uid":uid}}
        data = self.serializer.dumps(d)
        s = socket.socket()
        s.connect((self.host, self.port))
        x = s.send(data + "\r\n")
        res = False
        if x:
            s.setblocking(0)
            ready = select.select([s], [], [], PublisherClient.TIMEOUT_SECONDS)
            if ready[0]:
                data = s.recv(4096)
                ret = self.serializer.loads(data.strip())
                res = uid in ret and ret[uid]
        return res

if __name__ == '__main__':

    pc = PublisherClient("127.0.0.1", 1025)
    print pc.subscribed("test1")
    print pc.subscribed("nope")
    print pc.publish("test")
    print pc.publish("test1")
    print pc.publish("fede")
    print pc.list()

