import sys, socket

from aerospike_py.client import AerospikeClient
from aerospike_py.connection import SocketConnection

cli = AerospikeClient(SocketConnection(socket.create_connection((sys.argv[1], 3000))))
infokeys = cli.info([
    'build', 'edition', 'node', 'service', 'services', 'statistics', 'version'
])

for k, v in infokeys.items():
    print("%-15s: %s" % (k, v))
