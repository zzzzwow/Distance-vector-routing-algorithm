# _*_coding: utf-8 _*_
import copy
import json
import struct
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
import socket
from multiprocessing import Pool
from typing import Optional

START_PORT = 20000


def create_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return sock


def create_server(ip, port, listen=20):
    sock = create_socket()
    sock.bind((ip, port))
    sock.listen(listen)
    return sock


def create_client(ip, port):
    """
    create a client to connect to the server
    """
    sock = create_socket()
    sock.connect((ip, port))
    return sock


class SocketHandle:
    def __init__(self, sock, models: 'ItemProcess', lock):
        self.models = models  # store all the information of the node
        self.lock = lock  # use lock to prevent collisions with routing table modifications during concurrency
        self.sock = sock
        self.change = False

    def show_message(self, text):
        """
        print message
        """
        print(f"Node<{self.models.node}>: %s" % text)

    def send_message(self, sock, data: dict):
        """
        send the routing table dictionary
        """
        self.show_message(f"send message: {data}")
        content = json.dumps(data).encode("utf-8")
        sock.send(struct.pack("!I", len(content)) + content)

    @staticmethod
    def _recv(sock, size, chunk=2048):
        """
        receive information block by block
        """
        while size > 0:
            if size < chunk:
                chunk = size
            block = sock.recv(chunk)
            size -= len(block)
            yield block

    def recv_message(self, sock):
        size = struct.unpack("!I", b"".join(v for v in self._recv(sock, 4)))[0]
        data = json.loads(
            b"".join(v for v in self._recv(sock, size))
        )
        return data

    def update(self, messages):
        """
        after receiving the neighbor node information,
        bellman-Ford algorithm is used to determine the shortest path,
        update the routing table, and push the latest routing table to all connected neighbor nodes
        """
        own = messages.pop(self.models.node)  # After receiving the information from the neighbor node,
        # obtain the required information
        distance = own['distance']
        if own['next_hop'] not in self.models.tables:
            self.change = True
            self.models.tables[own['next_hop']] = {"distance": distance, "next_hop": own["next_hop"]}
        elif own["distance"] < self.models.tables[own['next_hop']]['distance']:
            self.change = True
            self.models.tables[own['next_hop']] = {"distance": distance, "next_hop": own["next_hop"]}
        tables = {}
        for k, v in messages.items():
            v['distance'] += distance  # Add the distance from the neighbor to the table sent by the neighbor,
            # Indicates the distance to other nodes when next_hop is the neighbor
            tables[k] = v
        for key, item in tables.items():
            if key not in self.models.tables:  # if the distance to a node is not in the previous routing table, add it
                self.models.tables[key] = item
                self.change = True
            elif item['next_hop'] == self.models.tables[key]['next_hop']:
                # if the next_hop in the received message is the same as the next_hop in the table,
                # the distance is compared,take the shortest distance
                if item['distance'] != self.models.tables[key]['distance']:
                    self.models.tables[key] = item
                    self.change = True
            else:  # judge who has the shortest distance as next_hop, and use it as next_hop
                if item['distance'] < self.models.tables[key]['distance']:
                    self.models.tables[key] = item
                    self.change = True

        if self.change:
            self.save_table()
            for client in self.models.live_socket:  # send the latest routing table to all connected neighbor nodes
                self.show_message(f"result:{self.models.tables}")
                self.change = False
                self.send_message(client, self.get_send_table())
                self.show_message("change send all!")

    def save_table(self):
        """
        print out the local DV table and save it to a json file
        """
        self.show_message(f"save:{self.models.tables}")
        with open(f"{self.models.node}.json", mode='w') as fp:
            fp.write(
                json.dumps(self.models.tables)
            )

    def get_send_table(self):
        """
        form the dictionary to be sent to the neighbor
        """
        data = {}
        for k, v in self.models.tables.items():
            data[k] = {"distance": v["distance"], "next_hop": self.models.node}
        return data


class ServerHandle(SocketHandle):
    def __init__(self, ip, port, models, lock):
        self.ip = ip
        self.port = port
        sock = create_server(ip, port)
        super().__init__(sock, models, lock)
        self.show_message(f"create server: {(ip, port)}")

    def accept(self):
        """
        after the connection between the server and the client is successful,
        the server maintains the connection with the client through threads
        """
        while True:
            client, address = self.sock.accept()
            with self.lock:
                self.models.live_socket.add(client)
            threading.Thread(target=self.run_client, args=(client, address)).start()

    def run_client(self, sock, address):
        """
        perform interaction with the client
        """
        self.show_message(f"{address} connect !")
        self.change = True
        try:
            while True:
                if self.change:
                    self.save_table()
                    self.send_message(sock, self.get_send_table())
                    self.change = False
                message = self.recv_message(sock)
                self.show_message(f"recv:{message}")
                with self.lock:
                    self.update(message)
        except IOError as e:
            self.show_message(str(e))
        finally:
            self.models.live_socket.discard(sock)


class ClientHandle(SocketHandle):
    def __init__(self, ip, port, models, lock):
        self.ip = ip
        self.port = port
        sock = create_client(ip, port)
        super().__init__(sock, models, lock)
        self.change = False
        self.show_message(f"start client connect:{ip, port}")

    def accept(self):
        """
        operations performed by the client after the client connects to the server
        """
        try:
            self.models.live_socket.add(self.sock)
            while True:
                message = self.recv_message(self.sock)
                self.show_message(f"client accept:{message}", )
                with self.lock:
                    self.show_message(f"accept->{self.ip}:{self.port}")
                    self.update(message)
                    self.show_message(f"end->{self.ip}:{self.port}")
        except IOError as e:
            self.show_message(str(e))
        finally:
            self.models.live_socket.discard(self.sock)


def parse_conf(path):
    """
    parse the configuration file
    """
    infos = defaultdict(dict)
    cur_key = None
    with open(path, "r") as fp:
        for line in fp.readlines():
            if not line.strip():
                continue
            if line.startswith("Node"):
                kind, value = line.strip().split(" ")
                cur_key = value
            else:
                node, distance = line.strip().split(" ")
                infos[cur_key].update(
                    {node: {"distance": int(distance), "next_hop": node}}
                )
    return infos


@dataclass
class ItemProcess:
    """
    store the routing table of a node
    """
    node: str
    server: bool
    tables: dict
    sockets: dict = field(default_factory=dict)
    connect: Optional[dict] = field(default_factory=dict)
    live_socket: set = field(default_factory=set)
    port: Optional[int] = None


def main():
    infos = parse_conf("input.txt")  # parse the information in the configuration file "input.txt"
    servers = {}
    for port, (node, item) in enumerate(infos.items(), start=START_PORT):
        servers[node] = ItemProcess(
            node=node,
            server=True,
            port=port,
            tables=copy.deepcopy(item),
        )
    for node, item in infos.items():
        for name, v in item.items():
            if name in servers:
                servers[name].connect[node] = servers[node].port
            else:
                servers[name] = ItemProcess(
                    node=name,
                    server=False,
                    port=None,
                    connect={node: servers[node].port},
                    tables={},
                )
    with Pool(3) as p:
        p.map(run_process, [v for v in servers.values()])
        p.close()
        p.join()


def run_process(models: ItemProcess):
    """
    execute the respective sub-processes
    """
    lock = threading.Lock()
    print(f"Node<{models.node}> init tables:{models.tables}")
    # start thread in the sub-process, asynchronous processing,
    # realize multiple clients at the same time to connect to the server
    if models.server:
        threading.Thread(target=ServerHandle("localhost", models.port, models, lock).accept).start()
    time.sleep(0.1)  # Set the time to avoid the situation where the client starts first and the server starts later
    for node, port in models.connect.items():
        threading.Thread(target=ClientHandle("localhost", port, models, lock).accept).start()


if __name__ == '__main__':
    main()
