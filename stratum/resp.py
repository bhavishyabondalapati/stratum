"""RESP (REdis Serialization Protocol), version 2 - the subset Stratum needs.

Every value starts with a one-byte type marker and ends with CRLF:

    +OK\\r\\n                 simple string
    -ERR something\\r\\n      error
    :42\\r\\n                 integer
    $5\\r\\nhello\\r\\n         bulk string (binary safe; $-1 is "nil")
    *2\\r\\n...               array of two values

A client sends each command as an array of bulk strings, for example
``SET k v`` is ``*3\\r\\n$3\\r\\nSET\\r\\n$1\\r\\nk\\r\\n$1\\r\\nv\\r\\n``. For
convenience (telnet / netcat) the server also accepts "inline" commands: a
plain line of space-separated words.
"""

import asyncio

CRLF = b"\r\n"
MAX_BULK_BYTES = 64 * 1024 * 1024
MAX_ARRAY_ITEMS = 1024 * 1024


class ProtocolError(Exception):
    """The peer sent bytes that are not valid RESP."""


class ResponseError(Exception):
    """The server answered with an error reply (``-ERR ...``)."""


# ---------------------------------------------------------------- encoding

def simple(text):
    return b"+" + text.encode() + CRLF


def error(message):
    return b"-" + message.replace("\r", " ").replace("\n", " ").encode() + CRLF


def integer(number):
    return b":%d\r\n" % number


def bulk(data):
    if data is None:
        return b"$-1\r\n"
    return b"$%d\r\n%s\r\n" % (len(data), data)


def array(items):
    """Encode a list of bulk strings (bytes or None)."""
    return b"*%d\r\n" % len(items) + b"".join(bulk(item) for item in items)


def to_bytes(arg):
    if isinstance(arg, bytes):
        return arg
    if isinstance(arg, str):
        return arg.encode()
    if isinstance(arg, int) and not isinstance(arg, bool):
        return b"%d" % arg
    raise TypeError(f"cannot send {type(arg).__name__} as a command argument")


def encode_command(*args):
    """Encode a client command as an array of bulk strings."""
    return array([to_bytes(arg) for arg in args])


# ---------------------------------------------------------------- decoding

def _parse_int(raw, what):
    try:
        return int(raw)
    except ValueError:
        raise ProtocolError(f"invalid {what}") from None


async def read_command(reader):
    """Read one command from an asyncio StreamReader (server side).

    Returns a list of bytes arguments, an empty list for a blank line, or
    None when the client has disconnected.
    """
    try:
        line = await reader.readline()
    except ValueError:  # line longer than the stream's buffer limit
        raise ProtocolError("line too long") from None
    if not line:
        return None
    if not line.endswith(b"\n"):
        return None  # connection closed in the middle of a line
    line = line.rstrip(b"\r\n")

    if not line.startswith(b"*"):
        return line.split()  # inline command

    count = _parse_int(line[1:], "array length")
    if count < 0 or count > MAX_ARRAY_ITEMS:
        raise ProtocolError("invalid array length")
    args = []
    try:
        for _ in range(count):
            header = await reader.readline()
            if not header.startswith(b"$"):
                if not header:
                    return None
                raise ProtocolError("expected a bulk string")
            length = _parse_int(header[1:].rstrip(b"\r\n"), "bulk length")
            if length < 0 or length > MAX_BULK_BYTES:
                raise ProtocolError("invalid bulk length")
            data = await reader.readexactly(length + 2)
            if data[-2:] != CRLF:
                raise ProtocolError("bulk string not terminated by CRLF")
            args.append(data[:-2])
    except asyncio.IncompleteReadError:
        return None
    except ValueError:
        raise ProtocolError("line too long") from None
    return args


def read_reply(stream):
    """Read one reply from a blocking binary file object (client side).

    Returns str for simple strings, int, bytes or None for bulk strings and
    a list for arrays. Raises ResponseError for error replies.
    """
    line = stream.readline()
    if not line.endswith(CRLF):
        raise ConnectionError("connection closed by server")
    kind, rest = line[:1], line[1:-2]
    if kind == b"+":
        return rest.decode()
    if kind == b"-":
        raise ResponseError(rest.decode(errors="replace"))
    if kind == b":":
        return _parse_int(rest, "integer reply")
    if kind == b"$":
        length = _parse_int(rest, "bulk length")
        if length < 0:
            return None
        data = stream.read(length + 2)
        if len(data) != length + 2:
            raise ConnectionError("connection closed by server")
        return data[:-2]
    if kind == b"*":
        count = _parse_int(rest, "array length")
        if count < 0:
            return None
        return [read_reply(stream) for _ in range(count)]
    raise ProtocolError(f"unknown reply type {kind!r}")
