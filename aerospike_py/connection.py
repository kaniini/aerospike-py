class Connection:
    """Connection classes simply provide an interface specification for abstracting I/O.
    They can be used with Twisted, Eventlet, AsyncIO, etc. without problem.
    """
    def read(self, length):
        pass

    def write(self, buf):
        pass


class SocketConnection(Connection):
    """SocketConnections are Connection instances which assume a functional Sockets API."""
    def __init__(self, fd):
        self._fd = fd

    def read(self, length):
        return self._fd.recv(length)

    def write(self, buf):
        return self._fd.send(buf)
