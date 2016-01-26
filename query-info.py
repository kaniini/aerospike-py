import sys, socket

from aerospike_py.info import request_info_keys
from aerospike_py.connection import SocketConnection

sck = SocketConnection(socket.create_connection((sys.argv[1], 3000)))
header, infokeys = request_info_keys(sck, [
    'build', 'edition', 'node', 'service', 'services', 'statistics', 'version'
])

for k, v in infokeys.items():
    print("%-15s: %s" % (k, v))
