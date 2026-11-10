"""A small blocking client for the Stratum server.

    from stratum.client import StratumClient

    with StratumClient(port=6380) as client:
        client.set("greeting", "hello")
        client.get("greeting")            # b'hello'
        client.scan("a", "z", limit=10)   # [(b'greeting', b'hello')]

Keys and values may be bytes or str (str is sent as UTF-8). Values always
come back as bytes.
"""

import socket

from . import resp
from .resp import ResponseError  # re-exported for callers

__all__ = ["StratumClient", "ResponseError"]


class StratumClient:
    def __init__(self, host="127.0.0.1", port=6380, timeout=None):
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._stream = self._sock.makefile("rb")

    def execute(self, *args):
        """Send one command and return its decoded reply."""
        self._sock.sendall(resp.encode_command(*args))
        return resp.read_reply(self._stream)

    def pipeline(self, commands):
        """Send many commands in one network write, then read every reply.

        ``commands`` is a list of argument tuples. Error replies are
        returned in place as ResponseError objects instead of being raised.
        """
        self._sock.sendall(b"".join(resp.encode_command(*c) for c in commands))
        replies = []
        for _ in commands:
            try:
                replies.append(resp.read_reply(self._stream))
            except ResponseError as exc:
                replies.append(exc)
        return replies

    def ping(self):
        return self.execute("PING") == "PONG"

    def get(self, key):
        return self.execute("GET", key)

    def set(self, key, value):
        return self.execute("SET", key, value) == "OK"

    def delete(self, *keys):
        """Delete keys; returns how many of them existed."""
        return self.execute("DEL", *keys)

    def exists(self, *keys):
        return self.execute("EXISTS", *keys)

    def scan(self, start="", end="", limit=1000):
        """Up to ``limit`` (key, value) pairs with start <= key < end."""
        flat = self.execute("SCAN", start, end, "LIMIT", limit)
        return list(zip(flat[0::2], flat[1::2]))

    def scan_iter(self, start="", end="", page_size=500):
        """Iterate a whole range, fetching ``page_size`` pairs at a time."""
        start = resp.to_bytes(start)
        while True:
            page = self.scan(start, end, page_size)
            yield from page
            if len(page) < page_size:
                return
            # the smallest key strictly greater than the last one we saw
            start = page[-1][0] + b"\x00"

    def info(self):
        return self.execute("INFO").decode()

    def close(self):
        try:
            self._stream.close()
        finally:
            self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
