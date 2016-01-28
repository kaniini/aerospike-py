import sys, socket
import hexdump

from pprint import pprint
from aerospike_py.connection import SocketConnection
import aerospike_py.message

hostname = sys.argv[1]
namespace = sys.argv[2]
key = sys.argv[3]

sck = SocketConnection(socket.create_connection((sys.argv[1], 3000)))

envelope = aerospike_py.message.pack_asmsg(
    aerospike_py.message.AS_INFO1_READ | aerospike_py.message.AS_INFO1_GET_ALL,
    0,
    0,
    0,
    0,
    0,
    [
        aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
        aerospike_py.message.pack_asmsg_field(b'', aerospike_py.message.AS_MSG_FIELD_TYPE_SET),
        aerospike_py.message.pack_asmsg_field(b'\x03' + key.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_KEY),
    ],
    [
#        aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_READ, 0, 'name', b'')
    ]
)

pprint(aerospike_py.message.submit_message(sck, envelope))
