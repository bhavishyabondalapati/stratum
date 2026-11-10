"""An asyncio TCP server that exposes a Stratum database over RESP.

Run it:   python -m stratum.server --dir ./data --port 6380
Talk to it with stratum.client, the stratum.cli REPL, or redis-cli.

Supported commands:

    PING [message]              -> PONG (or the message)
    ECHO message                -> message
    SET key value               -> OK
    GET key                     -> value or nil
    DEL key [key ...]           -> number of keys that existed
    EXISTS key [key ...]        -> number of keys that exist
    SCAN start end [LIMIT n]    -> flat array [k1, v1, k2, v2, ...] of keys
                                   with start <= key < end, in order. An
                                   empty string means "no bound". n defaults
                                   to 1000. RANGE is an alias.
    INFO                        -> engine statistics as text
    QUIT                        -> OK, then the connection is closed

Note: SCAN here is a *range scan* over sorted keys, not Redis's cursor-based
SCAN - an LSM tree keeps keys sorted, so ranges are its natural query.

Engine calls run in a worker thread (``loop.run_in_executor``) so a slow
operation - an fsync, or a write stalled behind a memtable flush - never
freezes the event loop for the other connections.
"""

import argparse
import asyncio
import signal
import threading

from . import resp
from .engine import Stratum

DEFAULT_PORT = 6380
DEFAULT_SCAN_LIMIT = 1000
MAX_SCAN_LIMIT = 100_000


class CommandError(Exception):
    """A problem with a command; sent back to the client as ``-ERR ...``."""


def _wrong_args(name):
    return CommandError(f"wrong number of arguments for '{name.lower()}' command")


class StratumServer:
    def __init__(self, db, host="127.0.0.1", port=DEFAULT_PORT):
        self.db = db
        self.host = host
        self.port = port
        self._server = None
        self._writers = set()
        self._commands = {
            b"PING": self._ping,
            b"ECHO": self._echo,
            b"GET": self._get,
            b"SET": self._set,
            b"DEL": self._del,
            b"EXISTS": self._exists,
            b"SCAN": self._scan,
            b"RANGE": self._scan,
            b"INFO": self._info,
            b"COMMAND": self._command,
        }

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port, limit=1 << 20
        )
        self.port = self._server.sockets[0].getsockname()[1]  # real port if 0

    async def stop(self):
        if self._server is not None:
            self._server.close()
            for writer in list(self._writers):
                writer.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(self, reader, writer):
        self._writers.add(writer)
        try:
            while True:
                try:
                    args = await resp.read_command(reader)
                except resp.ProtocolError as exc:
                    writer.write(resp.error(f"ERR protocol error: {exc}"))
                    await writer.drain()
                    break
                if args is None:
                    break  # client went away
                if not args:
                    continue  # blank line
                name = args[0].upper()
                if name == b"QUIT":
                    writer.write(resp.simple("OK"))
                    await writer.drain()
                    break
                writer.write(await self._dispatch(name, args[1:]))
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._writers.discard(writer)
            writer.close()

    async def _dispatch(self, name, args):
        handler = self._commands.get(name)
        if handler is None:
            return resp.error(f"ERR unknown command '{name.decode(errors='replace')}'")
        try:
            # run the (blocking) engine call off the event loop
            return await asyncio.get_running_loop().run_in_executor(None, handler, args)
        except CommandError as exc:
            return resp.error(f"ERR {exc}")
        except Exception as exc:
            return resp.error(f"ERR {type(exc).__name__}: {exc}")

    # ----------------------------------------------------- command handlers
    # Each takes the argument list and returns an encoded RESP reply.

    def _ping(self, args):
        if len(args) > 1:
            raise _wrong_args("ping")
        return resp.bulk(args[0]) if args else resp.simple("PONG")

    def _echo(self, args):
        if len(args) != 1:
            raise _wrong_args("echo")
        return resp.bulk(args[0])

    def _get(self, args):
        if len(args) != 1:
            raise _wrong_args("get")
        return resp.bulk(self.db.get(args[0]))

    def _set(self, args):
        if len(args) != 2:
            raise _wrong_args("set")
        self.db.put(args[0], args[1])
        return resp.simple("OK")

    def _del(self, args):
        if not args:
            raise _wrong_args("del")
        removed = 0
        for key in args:
            if key in self.db:
                self.db.delete(key)
                removed += 1
        return resp.integer(removed)

    def _exists(self, args):
        if not args:
            raise _wrong_args("exists")
        return resp.integer(sum(key in self.db for key in args))

    def _scan(self, args):
        if len(args) not in (2, 4):
            raise _wrong_args("scan")
        limit = DEFAULT_SCAN_LIMIT
        if len(args) == 4:
            if args[2].upper() != b"LIMIT":
                raise CommandError("syntax error, expected: SCAN start end [LIMIT n]")
            try:
                limit = int(args[3])
            except ValueError:
                raise CommandError("LIMIT must be an integer") from None
            if not 1 <= limit <= MAX_SCAN_LIMIT:
                raise CommandError(f"LIMIT must be between 1 and {MAX_SCAN_LIMIT}")
        flat = []
        for key, value in self.db.scan(args[0] or None, args[1] or None, limit):
            flat.append(key)
            flat.append(value)
        return resp.array(flat)

    def _info(self, args):
        if args:
            raise _wrong_args("info")
        stats = self.db.stats()
        lines = ["# Stratum"]
        for key, value in stats.items():
            if key != "tables":
                lines.append(f"{key}:{value}")
        for table in stats["tables"]:
            lines.append(
                "table_%06d:bytes=%d,entries=%d"
                % (table["file"], table["bytes"], table["entries"])
            )
        return resp.bulk("\r\n".join(lines).encode())

    def _command(self, args):
        return resp.array([])  # redis-cli sends COMMAND DOCS when it connects


class ServerThread:
    """Runs a StratumServer on its own event loop in a background thread.

    Handy for tests and for embedding a server in another program:

        with ServerThread(db, port=0) as server:
            client = StratumClient(port=server.port)
    """

    def __init__(self, db, host="127.0.0.1", port=0):
        self.server = StratumServer(db, host, port)
        self._thread = threading.Thread(target=self._run, name="stratum-server", daemon=True)
        self._ready = threading.Event()
        self._loop = None
        self._stop = None
        self._error = None

    @property
    def port(self):
        return self.server.port

    def _run(self):
        try:
            asyncio.run(self._main())
        except Exception as exc:
            self._error = exc
        finally:
            self._ready.set()

    async def _main(self):
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        await self.server.start()
        self._ready.set()
        await self._stop.wait()
        await self.server.stop()

    def start(self):
        self._thread.start()
        self._ready.wait()
        if self._error is not None:
            raise self._error
        return self

    def stop(self):
        if self._loop is not None and self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._stop.set)
        self._thread.join()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc_info):
        self.stop()


async def serve(db, host, port):
    """Serve until SIGINT / SIGTERM."""
    server = StratumServer(db, host, port)
    await server.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    print(f"Stratum listening on {server.host}:{server.port} (data in {db.path})", flush=True)
    await stop.wait()
    await server.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Stratum RESP server")
    parser.add_argument("--dir", default="data", help="database directory (default: data)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-sync", action="store_true",
                        help="do not fsync the WAL on every write (faster, less durable)")
    args = parser.parse_args(argv)
    db = Stratum(args.dir, sync=not args.no_sync)
    try:
        asyncio.run(serve(db, args.host, args.port))
    finally:
        db.close()
        print("Stratum stopped cleanly.")


if __name__ == "__main__":
    main()
