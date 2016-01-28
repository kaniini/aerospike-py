import socket

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
            buckets[op[1]] = aerospike_py.message.decode_payload(op[0].bin_data_type, op[2])

        return buckets

    def put(self, namespace, set='', key='', bins={}, create_only=False, bin_create_only=False):
        flags = aerospike_py.message.AS_INFO2_WRITE
        if create_only:
            flags |= aerospike_py.message.AS_INFO2_CREATE_ONLY

        if bin_create_only:
            flags |= aerospike_py.message.AS_INFO2_BIN_CREATE_ONLY

        encoded_bins = [(k, aerospike_py.message.encode_payload(v)) for k, v in bins.items()]
        bin_cmds = [aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_WRITE, i[1][1], i[0], i[1][0]) for i in encoded_bins]
        envelope = aerospike_py.message.pack_asmsg(0, flags, 0, 0, 0, 0,
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
            buckets[op[1]] = aerospike_py.message.decode_payload(op[0].bin_data_type, op[2])

        return buckets

    def delete(self, namespace, set='', key=''):
        envelope = aerospike_py.message.pack_asmsg(0, aerospike_py.message.AS_INFO2_WRITE | aerospike_py.message.AS_INFO2_DELETE, 0, 0, 0, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(set.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_SET),
                aerospike_py.message.pack_asmsg_field(b'\x03' + key.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_KEY),
            ],
            []
        )

        outer, asmsg_hdr, asmsg_fields, asmsg_ops = aerospike_py.message.submit_message(self.sck, envelope)

        buckets = {}
        for op in asmsg_ops:
            buckets[op[1]] = aerospike_py.message.decode_payload(op[0].bin_data_type, op[2])

        return buckets


    def incr(self, namespace, set='', key='', bin='', incr_by=0):
        flags = aerospike_py.message.AS_INFO2_WRITE

        bin_cmds = [aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_INCR, 1, bin, aerospike_py.message.encode_payload(incr_by)[0])]
        envelope = aerospike_py.message.pack_asmsg(0, flags, 0, 0, 0, 0,
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
            buckets[op[1]] = aerospike_py.message.decode_payload(op[0].bin_data_type, op[2])

        return buckets


def connect(host: str, port: int) -> AerospikeClient:
    return AerospikeClient(SocketConnection(socket.create_connection((host, port))))
