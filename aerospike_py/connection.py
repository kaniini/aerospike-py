import asyncio
from logging import getLogger


LOGGER = getLogger(__name__)


class Connection:
    """Connection classes simply provide an interface specification for abstracting I/O.
    They can be used with Twisted, Eventlet, AsyncIO, etc. without problem.
    """
    def read(self, length):
        pass

    def write(self, buf):
        pass


class AsyncConnection(Connection):
    """A Connection subclass which uses AsyncIO."""
    def __init__(self, host: str, port: int):
        conn = asyncio.open_connection(host, port)
        loop = asyncio.get_event_loop()
        try:
            (self.reader, self.writer) = loop.run_until_complete(conn)
        except OSError:
            LOGGER.exception("Can't connect to Aerospike")
            self.reader = self.writer = None

    def read(self, length: int):
        data = yield from self.reader.readexactly(length)
        return data

    def write(self, buf):
        self.writer.write(buf)
        yield from self.writer.drain()
