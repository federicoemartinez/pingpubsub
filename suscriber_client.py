__author__ = 'Fede M'


import asyncore, socket
import json

class Subscriber(asyncore.dispatcher):

    def __init__(self, host, port, uid, callback, close_callback=None):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect( (host, port) )
        self.port = port
        self.host = host
        self.uid = uid
        self.registered = False
        self.callback = callback
        self.close_callback = close_callback

    def handle_connect(self):
        if not self.registered:
            d = json.dumps({"uid":self.uid})
            self.send(d+"\r\n")
            self.registered = True

    def handle_close(self):
        self.close()
        self.registered = False
        if self.close_callback:
            self.close_callback()

    def handle_read(self):
        x = self.recv(8192)
        if x:
            print(x)
            self.callback()

    def writable(self):
        return not self.registered

    def handle_write(self):
        if not self.registered:
            d = json.dumps({"uid":self.uid})
            x = self.send(d+"\r\n")
            if x:
                self.registered = True


if __name__ == '__main__':

    def func():
       print("I was called")

    def func2():
        print("Closing")

    client = Subscriber("127.0.0.1", 1025, "test", func, func2)
    asyncore.loop()