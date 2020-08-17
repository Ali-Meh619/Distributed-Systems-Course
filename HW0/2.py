import json


class message:
    def __init__(self, type, value):
        self.type = type;
        self.value = value;


m = message('start', 'hi')
ba = json.dumps(m.__dict__).encode('utf-8')
mm = message(**json.loads(ba, encoding='utf-8'));

import socket
import time


def node2(x, ip1, po1, ip2, po2):
    if x == 'r':

        while True:

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((ip1, po1))
            s.listen()
            connection, client = s.accept()

            data = connection.recv(1024);
            mm = message(**json.loads(data, encoding='utf-8'));
            print(str(ip1) + ' on Port ' + str(po1) + ' recieved ' + str(mm.type) + ' and ' + str(mm.value))

            if mm.value == 'hello':
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((ip2, po2))
                m = message('start', 'hi')
                ba = json.dumps(m.__dict__).encode('utf-8')
                s.send(ba)
            # time.sleep(5)

            if mm.value == 'goodbye':
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((ip2, po2))
                m = message('end', 'bye')
                ba = json.dumps(m.__dict__).encode('utf-8')
                s.send(ba)
                break;

    if x == 's':

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # connecting

        s.connect((ip2, po2))
        a = input("type?")
        b = input("value?")

        m = message(a, b)
        ba = json.dumps(m.__dict__).encode('utf-8')

        s.send(ba)
        while True:

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((ip1, po1))
            s.listen()

            connection, client = s.accept()
            data = connection.recv(1024);
            mm = message(**json.loads(data, encoding='utf-8'));
            print(str(ip1) + ' on Port ' + str(po1) + ' recieved ' + str(mm.type) + ' and ' + str(mm.value))
            if mm.value == 'hi':
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((ip2, po2))
                m = message('end', 'goodbye')
                ba = json.dumps(m.__dict__).encode('utf-8')
                s.send(ba)

            if mm.value == 'bye':
                break



node2('s','127.0.0.1',6044,'127.0.0.1',6033)