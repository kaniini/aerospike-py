import asyncio
from collections import namedtuple
import struct

from aerospike_py.connection import Connection
from aerospike_py.result_code import ASMSGProtocolException


class ASIOException(Exception):
    pass


class InvalidMessageException(Exception):
    pass


# --- all messages ---

AerospikeOuterHeader = namedtuple('AerospikeOuterHeader', ['version', 'msg_type', 'sz'])
AerospikeOuterHeaderStruct = struct.Struct('>Q')


def pack_outer_header(header: AerospikeOuterHeader) -> bytes:
    size = (header.sz) | ((header.version & 0xFF) << 56) | ((header.msg_type & 0xFF) << 48)
    return AerospikeOuterHeaderStruct.pack(size)


def unpack_outer_header(data: bytes) -> AerospikeOuterHeader:
    header_uint64 = AerospikeOuterHeaderStruct.unpack(data)[0]
    size = header_uint64 & 0xFFFFFFFFFFFF
    msg_type = data[1]
    msg_proto = data[0]
    return AerospikeOuterHeader(msg_proto, msg_type, size)


def pack_message(envelope: bytes, msg_type: int) -> bytes:
    size = len(envelope)
    return pack_outer_header(AerospikeOuterHeader(2, msg_type, size)) + envelope


def unpack_message(envelope: bytes, whole_message: bool = False) -> (AerospikeOuterHeader, bytes):
    if len(envelope) < 8:
        raise InvalidMessageException('message length is too short')

    header = unpack_outer_header(envelope[0:8])
    if header.version != 2:
        raise InvalidMessageException('protocol version %d is not supported' % header.version)

    if header.msg_type not in (1, 3):
        raise InvalidMessageException('message type %d is not supported' % header.msg_type)

    if whole_message and header.sz != len(envelope[8:]):
        raise InvalidMessageException('message payload is less than the specified length (%d < %d).' % (len(envelope[8:]), header.sz))

    return (header, envelope[8:])


# --- AS_MSG (type 3) messages ---

AS_INFO1_READ = (1 << 0)
AS_INFO1_GET_ALL = (1 << 1)
AS_INFO1_BATCH = (1 << 3)
AS_INFO1_NOBINDATA = (1 << 5)
AS_INFO1_CONSISTENCY_ALL = (1 << 6)

AS_INFO2_WRITE = (1 << 0)
AS_INFO2_DELETE = (1 << 1)
AS_INFO2_GENERATION = (1 << 2)
AS_INFO2_GENERATION_GT = (1 << 3)
AS_INFO2_GENERATION_DUP = (1 << 4)
AS_INFO2_CREATE_ONLY = (1 << 5)
AS_INFO2_CREATE_BIN_ONLY = (1 << 6)

AS_INFO3_LAST = (1 << 0)
AS_INFO3_COMMIT_MASTER = (1 << 1)
AS_INFO3_UPDATE_ONLY = (1 << 3)
AS_INFO3_CREATE_OR_REPLACE = (1 << 4)
AS_INFO3_REPLACE_ONLY = (1 << 5)


AerospikeASMSGHeader = namedtuple('AerospikeASMSGHeader', [
    'header_sz', 'info1', 'info2', 'info3', 'result_code', 'generation', 'record_ttl', 'transaction_ttl', 'n_fields', 'n_ops'
])
AerospikeASMSGHeaderStruct = struct.Struct('>BBBBxBIIIHH')


def pack_asmsg_header(info1: int, info2: int, info3: int, generation: int, record_ttl: int, transaction_ttl: int, n_fields: int, n_ops: int) -> bytes:
    header = AerospikeASMSGHeader(22, info1, info2, info3, 0, generation, record_ttl, transaction_ttl, n_fields, n_ops)
    return AerospikeASMSGHeaderStruct.pack(*header)


def unpack_asmsg_header(header: bytes) -> AerospikeASMSGHeader:
    if len(header) < 22:
        raise InvalidMessageException('AS_MSG header is not 22 bytes long')

    return AerospikeASMSGHeader(*AerospikeASMSGHeaderStruct.unpack(header))


# Aerospike fields are actually locators, i.e. they describe how to locate data rows/documents/whatever.
AerospikeASMSGFieldHeader = namedtuple('AerospikeASMSGFieldHeader', ['size', 'field_type'])
AerospikeASMSGFieldHeaderStruct = struct.Struct('>IB')


AS_MSG_FIELD_TYPE_NAMESPACE = 0
AS_MSG_FIELD_TYPE_SET = 1
AS_MSG_FIELD_TYPE_KEY = 2
AS_MSG_FIELD_TYPE_BIN = 3
AS_MSG_FIELD_TYPE_DIGEST_RIPE = 4
AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY = 6
AS_MSG_FIELD_TYPE_TRID = 7
AS_MSG_FIELD_TYPE_SCAN_OPTIONS = 8


AS_MSG_PARTICLE_TYPE_NULL = 0
AS_MSG_PARTICLE_TYPE_INTEGER = 1
AS_MSG_PARTICLE_TYPE_DOUBLE = 2
AS_MSG_PARTICLE_TYPE_STRING = 3
AS_MSG_PARTICLE_TYPE_BLOB = 4
AS_MSG_PARTICLE_TYPE_PYTHON_BLOB = 9
AS_MSG_PARTICLE_TYPE_MAP = 19
AS_MSG_PARTICLE_TYPE_LIST = 20
AS_MSG_PARTICLE_TYPE_GEOJSON = 23


_decoders = {
    AS_MSG_PARTICLE_TYPE_NULL: lambda x: None,
    AS_MSG_PARTICLE_TYPE_INTEGER: lambda x: struct.unpack('>Q', x[0:8])[0],
    AS_MSG_PARTICLE_TYPE_DOUBLE: lambda x: struct.unpack('>d', x[0:8])[0],
    AS_MSG_PARTICLE_TYPE_STRING: lambda x: x.decode('UTF-8').strip('\x00'),
    AS_MSG_PARTICLE_TYPE_BLOB: lambda x: x,
}


NoneType = type(None)

_encoders = {
    NoneType: lambda x: (b'', AS_MSG_PARTICLE_TYPE_NULL),
    int: lambda x: (struct.pack('>Q', x), AS_MSG_PARTICLE_TYPE_INTEGER),
    float: lambda x: (struct.pack('>d', x), AS_MSG_PARTICLE_TYPE_DOUBLE),
    str: lambda x: (x.encode('UTF-8') + b'\x00', AS_MSG_PARTICLE_TYPE_STRING),
    bytes: lambda x: (x, AS_MSG_PARTICLE_TYPE_BLOB),
}


def encode_payload(payload):
    encoder = _encoders.get(type(payload), lambda x: (b'', AS_MSG_PARTICLE_TYPE_NULL))
    return encoder(payload)


def decode_payload(ptype, payload):
    decoder = _decoders.get(ptype, lambda x: x)
    return decoder(payload)


def pack_asmsg_field(data: bytes, field_type: int) -> bytes:
    header = AerospikeASMSGFieldHeader(len(data) + 1, field_type)
    return AerospikeASMSGFieldHeaderStruct.pack(*header) + data


def unpack_asmsg_field(data: bytes) -> (AerospikeASMSGFieldHeader, bytes):
    header = AerospikeASMSGFieldHeader(*AerospikeASMSGFieldHeaderStruct.unpack(data[0:5]))
    return (header, data[5:])


# Aerospike operations describe what to do to the located record(s).
AerospikeASMSGOperationHeader = namedtuple('AerospikeASMSGOperationHeader', [
    'size', 'op', 'bin_data_type', 'bin_version', 'bin_name_length'
])
AerospikeASMSGOperationHeaderStruct = struct.Struct('>IBBBB')


AS_MSG_OP_READ = 1
AS_MSG_OP_WRITE = 2
AS_MSG_OP_INCR = 5
AS_MSG_OP_APPEND = 9
AS_MSG_OP_PREPEND = 10
AS_MSG_OP_TOUCH = 11

AS_MSG_OP_MC_INCR = 129
AS_MSG_OP_MC_APPEND = 130
AS_MSG_OP_MC_PREPEND = 131
AS_MSG_OP_MC_TOUCH = 132


def pack_asmsg_operation(op: int, bin_data_type: int, bin_name: str, bin_data: bytes) -> bytes:
    bin_name_enc = bin_name.encode('UTF-8')
    header = AerospikeASMSGOperationHeader(len(bin_name_enc) + len(bin_data) + 4, op, bin_data_type, 0, len(bin_name_enc))
    return AerospikeASMSGOperationHeaderStruct.pack(*header) + bin_name_enc + bin_data


def unpack_asmsg_operation(data: bytes) -> (AerospikeASMSGOperationHeader, str, bytes):
    header = AerospikeASMSGOperationHeader(*AerospikeASMSGOperationHeaderStruct.unpack(data[0:8]))
    if len(data) == 8:
        return header, None, None

    bin_name = data[8:8 + header.bin_name_length].decode('UTF-8')
    return (header, bin_name, data[8 + header.bin_name_length:])


def pack_asmsg(info1: int, info2: int, info3: int, generation: int, record_ttl: int, transaction_ttl: int, fields: list, ops: list) -> bytes:
    asmsg_hdr = pack_asmsg_header(info1, info2, info3, generation, record_ttl, transaction_ttl, len(fields), len(ops))
    return asmsg_hdr + b''.join(fields) + b''.join(ops)


def unpack_asmsg(data: bytes) -> (AerospikeASMSGHeader, list, list):
    asmsg_hdr = unpack_asmsg_header(data[0:22])

    # next comes fields:
    pos = 22
    fields = []
    for i in range(asmsg_hdr.n_fields):
        f_hdr, _ = unpack_asmsg_field(data[pos:(pos + 5)])
        f_hdr, payload = unpack_asmsg_field(data[pos:(pos + 5 + f_hdr.size)])
        fields += [(f_hdr, payload)]
        pos += (4 + f_hdr.size)

    ops = []
    for i in range(asmsg_hdr.n_ops):
        o_hdr, _, _ = unpack_asmsg_operation(data[pos:(pos + 8)])
        o_hdr, bin_name, bin_payload = unpack_asmsg_operation(data[pos:(pos + 5 + o_hdr.size)])
        ops += [(o_hdr, bin_name, bin_payload)]
        pos += (4 + o_hdr.size)

    return asmsg_hdr, fields, ops, data[pos:]


@asyncio.coroutine
def submit_message(conn: Connection, data: bytes) -> (AerospikeOuterHeader, AerospikeASMSGHeader, list, list):
    ohdr = AerospikeOuterHeader(2, 3, len(data))
    buf = pack_outer_header(ohdr) + data
    yield from conn.write(buf)

    hdr_payload = yield from conn.read(8)
    if not hdr_payload:
        raise ASIOException('read')

    header, _ = unpack_message(hdr_payload)

    data = hdr_payload
    data += yield from conn.read(header.sz)

    header, payload = unpack_message(data)
    asmsg_header, asmsg_fields, asmsg_ops, _ = unpack_asmsg(payload)

    if asmsg_header.result_code != 0:
        raise ASMSGProtocolException(asmsg_header.result_code)

    return header, asmsg_header, asmsg_fields, asmsg_ops


@asyncio.coroutine
def submit_multi_message(conn: Connection, data: bytes) -> list:
    ohdr = AerospikeOuterHeader(2, 3, len(data))
    buf = pack_outer_header(ohdr) + data
    yield from conn.write(buf)

    not_last = True
    messages = []

    while not_last:
        hdr_payload = yield from conn.read(8)
        if not hdr_payload:
            raise ASIOException('read')

        header, _ = unpack_message(hdr_payload)

        data = hdr_payload
        data += yield from conn.read(header.sz)

        if len(data) != 8 + header.sz:
            raise ASIOException('read')

        header, payload = unpack_message(data)
        while payload:
            asmsg_header, asmsg_fields, asmsg_ops, payload = unpack_asmsg(payload)
            messages += [(header, asmsg_header, asmsg_fields, asmsg_ops)]

        if asmsg_header.result_code not in (0, 2):
            raise ASMSGProtocolException(asmsg_header.result_code)

        if (asmsg_header.info3 & AS_INFO3_LAST) == AS_INFO3_LAST:
            not_last = False
            continue

    return messages
