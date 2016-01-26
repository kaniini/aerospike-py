from collections import namedtuple
import struct


AerospikeOuterHeader = namedtuple('AerospikeOuterHeader', ['version', 'msg_type', 'sz'])
AerospikeOuterHeaderStruct = struct.Struct('>Q')

def read_outer_header(data):
    header_uint64 = AerospikeOuterHeaderStruct.unpack(data)[0]
    size = header_uint64 & 0xFFFFFFFFFFFF
    msg_type = data[1]
    msg_proto = data[0]
    return AerospikeOuterHeader(msg_proto, msg_type, size)


def write_outer_header(header):
    size = (header.sz) | ((header.version & 0xFF) << 56) | ((header.msg_type & 0xFF) << 48)
    return AerospikeOuterHeaderStruct.pack(size)
