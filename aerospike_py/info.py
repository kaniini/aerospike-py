from aerospike_py.connection import Connection
from aerospike_py.message import pack_message, unpack_message, AerospikeOuterHeader


def request_info_keys(conn: Connection, commands: list) -> (AerospikeOuterHeader, dict):
    payload = pack_message('\n'.join(commands).encode('UTF-8'), 1)
    conn.write(payload)

    hdr_payload = conn.read(8)
    header, _ = unpack_message(hdr_payload)

    header, payload = unpack_message(hdr_payload + conn.read(header.sz))
    lines = payload.decode('UTF-8')

    infokeys = {}
    for line in lines.split('\n'):
        k, _, v = line.partition('\t')
        if ';' in v:
            infokeys[k] = v.split(';')
        else:
            infokeys[k] = v

    return header, infokeys
