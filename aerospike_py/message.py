from collections import namedtuple
import struct


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
AS_INFO1_NOBINDATA = (1 << 5)
AS_INFO1_CONSISTENCY_ALL = (1 << 6)

AS_INFO2_WRITE = (1 << 0)
AS_INFO2_DELETE = (1 << 1)
AS_INFO2_GENERATION = (1 << 2)
AS_INFO2_GENERATION_GT = (1 << 3)
AS_INFO2_GENERATION_DUP = (1 << 4)
AS_INFO2_CREATE_ONLY = (1 << 5)

AS_INFO3_LAST = (1 << 0)
AS_INFO3_COMMIT_MASTER = (1 << 1)
AS_INFO3_UPDATE_ONLY = (1 << 3)
AS_INFO3_CREATE_OR_REPLACE = (1 << 4)
AS_INFO3_REPLACE_ONLY = (1 << 5)


AerospikeASMSGHeader = namedtuple('AerospikeASMSGHeader', [
    'header_sz', 'info1', 'info2', 'info3', 'result_code', 'generation', 'record_ttl', 'transaction_ttl', 'n_fields', 'n_ops'
])
AerospikeASMSGHeaderStruct = struct.Struct('>BBBxBIIIHH')


def pack_asmsg_header(info1: int, info2: int, info3: int, generation: int, record_ttl: int, transaction_ttl: int, n_fields: int, n_ops: int) -> bytes:
    header = AerospikeASMSGHeader(22, info1, info2, info3, 0, generation, record_ttl, transaction_ttl, n_fields, n_ops)
    return AerospikeASMSGHeaderStruct.pack(*header)


def unpack_asmsg_header(header: bytes) -> AerospikeASMSGHeader:
    if len(header) < 22:
        raise InvalidMessageException('AS_MSG header is not 22 bytes long')

    return AerospikeASMSGHeader(*AerospikeASMSGHeaderStruct.unpack(bytes))
