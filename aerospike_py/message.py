from collections import namedtuple
import struct


class InvalidMessageException(Exception):
    pass


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
