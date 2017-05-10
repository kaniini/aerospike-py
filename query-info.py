import asyncio, sys, socket

from aerospike_py.client import AerospikeClient

loop = asyncio.get_event_loop()

cli = loop.run_until_complete(loop.create_connection(AerospikeClient, sys.argv[1], 3000))[1]
infokeys = loop.run_until_complete(cli.info([
    'build', 'edition', 'node', 'service', 'services', 'statistics', 'version'
]))

for k, v in infokeys.items():
    print("%-15s: %s" % (k, v))
