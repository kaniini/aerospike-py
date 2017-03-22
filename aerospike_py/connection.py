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


class ASConnectionError(Exception):
    pass


class AsyncConnection(Connection):
    """A Connection subclass which uses AsyncIO."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    @asyncio.coroutine
    def open_connection(self):
        try:
            (self.reader, self.writer) = yield from asyncio.open_connection(self.host, self.port)
        except OSError:
            LOGGER.exception("Can't connect to Aerospike")
            self.reader = self.writer = None

    def close_connection(self):
        if self.writer:
            self.writer.close()

        self.reader = self.writer = None

    @asyncio.coroutine
    def cycle_connection(self):
        self.close_connection()
        yield from asyncio.shield(self.open_connection())

    @asyncio.coroutine
    def read(self, length: int):
        try:
            data = yield from self.reader.readexactly(length)
            assert len(data) == length
        except (EnvironmentError, asyncio.IncompleteReadError):
            data = None

        return data

    @asyncio.coroutine
    def write(self, buf):
        try:
            self.writer.write(buf)
            yield from self.writer.drain()
        except EnvironmentError as e:
            raise ASConnectionError('while writing to aerospike, encountered %r' % e)
