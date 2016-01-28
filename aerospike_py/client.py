from aerospike_py.connection import SocketConnection
from aerospike_py.info import request_info_keys
import aerospike_py.message


class AerospikeClient:
    def __init__(self, sck):
        self.sck = sck

    def info(self, keys):
        return request_info_keys(self.sck, keys)[1]

    def get(self, namespace, set='', key='', bins=[]):
        flags = aerospike_py.message.AS_INFO1_READ
        if not bins:
            flags |= aerospike_py.message.AS_INFO1_GET_ALL

        bin_cmds = [aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_READ, 0, bn, b'') for bn in bins]
        envelope = aerospike_py.message.pack_asmsg(flags, 0, 0, 0, 0, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(set.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_SET),
                aerospike_py.message.pack_asmsg_field(b'\x03' + key.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_KEY),
            ],
            bin_cmds
        )

        outer, asmsg_hdr, asmsg_fields, asmsg_ops = aerospike_py.message.submit_message(self.sck, envelope)

        buckets = {}
        for op in asmsg_ops:
            buckets[op[1]] = op[2].decode('UTF-8').strip('\x00')

        return buckets
