import asyncio
import hashlib

from aerospike_py.connection import AsyncConnection
from aerospike_py.info import request_info_keys
from aerospike_py.result_code import ASMSGProtocolException
from aerospike_py.message import ASIOException
import aerospike_py.message


def hash_key(set='', key=''):
    h = hashlib.new('ripemd160')
    h.update(set.encode('UTF-8'))
    h.update(b'\x03' + key.encode('UTF-8'))
    return h.digest()


class AerospikeClient:
    def __init__(self, conn):
        self.conn = conn

    def info(self, keys):
        return request_info_keys(self.conn, keys)[1]

    def _process_bucket(self, asmsg_ops):
        buckets = {}
        for op in asmsg_ops:
            buckets[op[1]] = aerospike_py.message.decode_payload(op[0].bin_data_type, op[2])

        return buckets

    @asyncio.coroutine
    def _submit_message(self, envelope, retry_count=3):
        while retry_count:
            try:
                outer, asmsg_hdr, asmsg_fields, asmsg_ops = yield from aerospike_py.message.submit_message(self.conn, envelope)
                return self._process_bucket(asmsg_ops)
            except ASMSGProtocolException as e:
                if e.result_code not in (14,):
                    raise
                retry_count -= 1
                if not retry_count:
                    raise
            except ASIOException as e:
                return None

        return buckets

    @asyncio.coroutine
    def _submit_batch(self, envelope, retry_count=3):
        while retry_count:
            try:
                messages = yield from aerospike_py.message.submit_multi_message(self.conn, envelope)
                return [self._process_bucket(x[3]) for x in messages]
            except ASMSGProtocolException as e:
                if e.result_code not in (14,):
                    raise
                retry_count -= 1
                if not retry_count:
                    raise
            except ASIOException as e:
                return None

        return buckets

    @asyncio.coroutine
    def get(self, namespace, set='', key='', bins=[], record_ttl=0, retry_count=3):
        flags = aerospike_py.message.AS_INFO1_READ
        if not bins:
            flags |= aerospike_py.message.AS_INFO1_GET_ALL

        bin_cmds = [aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_READ, 0, bn, b'') for bn in bins]
        envelope = aerospike_py.message.pack_asmsg(flags, 0, 0, 0, record_ttl, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(hash_key(set, key), aerospike_py.message.AS_MSG_FIELD_TYPE_DIGEST_RIPE),
            ],
            bin_cmds
        )

        return self._submit_message(envelope, retry_count)

    @asyncio.coroutine
    def mget(self, namespace, groups=[], bins={}, record_ttl=0, retry_count=3):
        flags = aerospike_py.message.AS_INFO1_READ # | aerospike_py.message.AS_INFO1_BATCH
        if not bins:
            flags |= aerospike_py.message.AS_INFO1_GET_ALL

        bin_cmds = [aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_READ, 0, bn, b'') for bn in bins]

        hashes = b''
        for k in groups:
            hashes += hash_key(k[0], k[1])

        envelope = aerospike_py.message.pack_asmsg(flags, 0, 0, 0, record_ttl, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(hashes, aerospike_py.message.AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY),
            ],
            bin_cmds
        )

        return self._submit_batch(envelope, retry_count)

    @asyncio.coroutine
    def put(self, namespace, set='', key='', bins={}, create_only=False, bin_create_only=False, record_ttl=0, retry_count=3):
        flags = aerospike_py.message.AS_INFO2_WRITE
        if create_only:
            flags |= aerospike_py.message.AS_INFO2_CREATE_ONLY

        if bin_create_only:
            flags |= aerospike_py.message.AS_INFO2_BIN_CREATE_ONLY

        encoded_bins = [(k, aerospike_py.message.encode_payload(v)) for k, v in bins.items()]
        bin_cmds = [aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_WRITE, i[1][1], i[0], i[1][0]) for i in encoded_bins]
        envelope = aerospike_py.message.pack_asmsg(0, flags, 0, 0, record_ttl, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(hash_key(set, key), aerospike_py.message.AS_MSG_FIELD_TYPE_DIGEST_RIPE),
            ],
            bin_cmds
        )

        return self._submit_message(envelope, retry_count)

    @asyncio.coroutine
    def delete(self, namespace, set='', key='', record_ttl=0, retry_count=3):
        envelope = aerospike_py.message.pack_asmsg(0, aerospike_py.message.AS_INFO2_WRITE | aerospike_py.message.AS_INFO2_DELETE, 0, 0, record_ttl, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(hash_key(set, key), aerospike_py.message.AS_MSG_FIELD_TYPE_DIGEST_RIPE),
            ],
            []
        )

        return self._submit_message(envelope, retry_count)

    @asyncio.coroutine
    def incr(self, namespace, set='', key='', bin='', incr_by=0, record_ttl=0, retry_count=3):
        flags = aerospike_py.message.AS_INFO2_WRITE

        bin_cmds = [aerospike_py.message.pack_asmsg_operation(aerospike_py.message.AS_MSG_OP_INCR, 1, bin, aerospike_py.message.encode_payload(incr_by)[0])]
        envelope = aerospike_py.message.pack_asmsg(0, flags, 0, 0, record_ttl, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(hash_key(set, key), aerospike_py.message.AS_MSG_FIELD_TYPE_DIGEST_RIPE),
            ],
            bin_cmds
        )

        return self._submit_message(envelope, retry_count)

    @asyncio.coroutine
    def _append_op(self, namespace, set='', key='', bin='', append_blob='', op=aerospike_py.message.AS_MSG_OP_APPEND, record_ttl=0, retry_count=3):
        flags = aerospike_py.message.AS_INFO2_WRITE

        blob = aerospike_py.message.encode_payload(append_blob)
        bin_cmds = [aerospike_py.message.pack_asmsg_operation(op, blob[1], bin, blob[0])]
        envelope = aerospike_py.message.pack_asmsg(0, flags, 0, 0, record_ttl, 0,
            [
                aerospike_py.message.pack_asmsg_field(namespace.encode('UTF-8'), aerospike_py.message.AS_MSG_FIELD_TYPE_NAMESPACE),
                aerospike_py.message.pack_asmsg_field(hash_key(set, key), aerospike_py.message.AS_MSG_FIELD_TYPE_DIGEST_RIPE),
            ],
            bin_cmds
        )

        return self._submit_message(envelope, retry_count)

    @asyncio.coroutine
    def append(self, namespace, set='', key='', bin='', append_blob='', record_ttl=0):
        return self._append_op(namespace, set, key, bin, append_blob, aerospike_py.message.AS_MSG_OP_APPEND, record_ttl)

    @asyncio.coroutine
    def prepend(self, namespace, set='', key='', bin='', append_blob='', record_ttl=0):
        return self._append_op(namespace, set, key, bin, append_blob, aerospike_py.message.AS_MSG_OP_PREPEND, record_ttl)

    @asyncio.coroutine
    def touch(self, namespace, set, key, bin='', record_ttl=0):
        return self._append_op(namespace, set, key, bin, None, aerospike_py.message.AS_MSG_OP_TOUCH, record_ttl)


def connect(host: str, port: int) -> AerospikeClient:
    return AerospikeClient(AsyncConnection(host, port))

