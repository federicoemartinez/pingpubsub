__author__ = 'Fede M'

import asyncore
import socket
import json


class Subscriber(asyncore.dispatcher):
    def __init__(self, host, port, uids, callback, close_callback=None):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))
        self.port = port
        self.host = host
        self.uids = uids
        self.registered = False
        self.callback = callback
        self.close_callback = close_callback

    def handle_connect(self):
        if not self.registered:
            d = json.dumps({"uids": self.uids})
            self.send(d + "\r\n")
            self.registered = True

    def handle_close(self):
        self.close()
        self.registered = False
        if self.close_callback:
            self.close_callback()

    def handle_read(self):
        x = self.recv(8192)
        if x:
            self.callback(x)

    def writable(self):
        return not self.registered

    def set_uids(self, uids):
        self.uids = uids
        self.registered = False

    def handle_write(self):
        d = json.dumps({"uids": self.uids})
        x = self.send(d + "\r\n")
        if x:
            self.registered = True


if __name__ == '__main__':
    def func(x):
        print("I was called")
        print x

    def func2():
        print("Closing")
        raise asyncore.ExitNow("BYE")

    client = Subscriber("127.0.0.1", 1026, ["test1"], func, func2)
    asyncore.loop()
