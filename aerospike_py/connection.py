import asyncio
from logging import getLogger


LOGGER = getLogger(__name__)


class Connection:
    """Connection classes simply provide an interface specification for abstracting I/O.
    They can be used with Twisted, Eventlet, AsyncIO, etc. without problem.
    """
    def read(self, length, needs_resync):
        pass

    def write(self, buf):
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
    def read(self, length: int, needs_resync: bool):
        orig_length = length

        # if we need to resync, then we use the first 2 bytes to sync
        found_header_sentinel = not needs_resync
        if needs_resync:
            length -= 2

        trailing = b''

        try:
            # theory: aerospike messages begin with either {0x02, 0x01} or {0x02, 0x03}.
            # so we use these as sync bytes to ensure we return the proper header bytes.
            while not found_header_sentinel:
                trailing = (yield from self.reader.readuntil(b'\002'))[-1:]
                next_byte = yield from self.reader.readexactly(1)
                if next_byte in (b'\001', b'\003'):
                    found_header_sentinel = True
                    trailing += next_byte

            if needs_resync:
                assert len(trailing) == 2

            remainder = yield from self.reader.readexactly(length)
            assert len(remainder) == length

            data = trailing + remainder
            assert len(data) == orig_length
        except (EnvironmentError, asyncio.IncompleteReadError):
            data = None

        return data

    @asyncio.coroutine
    def write(self, buf):
        self.writer.write(buf)
        yield from self.writer.drain()
