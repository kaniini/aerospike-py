from aerospike_py.connection import Connection
from aerospike_py.message import pack_message, unpack_message, AerospikeOuterHeader


def request_info_keys(conn: Connection, commands: list) -> (AerospikeOuterHeader, dict):
    payload = pack_message('\n'.join(commands))
    conn.write(payload)

    header, payload = unpack_message(conn.read())
    lines = payload.decode('UTF-8')

    infokeys = {}
    for line in lines.split('\n'):
        k, _, v = line.partition('\t')
        infokeys[k] = v

    return header, infokeys
