import sys, socket

from pprint import pprint
from aerospike_py.connection import SocketConnection
from aerospike_py.client import AerospikeClient

hostname = sys.argv[1]
namespace = sys.argv[2]
key = sys.argv[3]

cli = AerospikeClient(SocketConnection(socket.create_connection((sys.argv[1], 3000))))

pprint(cli.get(namespace, '', key))
