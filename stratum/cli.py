"""Interactive shell (REPL) for Stratum.

Connect to a running server:      python -m stratum.cli --port 6380
Or open a directory directly:     python -m stratum.cli --dir ./data

Type ``help`` inside the shell for the list of commands.
"""

import argparse
import shlex
import sys

from .client import StratumClient
from .engine import Stratum
from .resp import ResponseError
from .util import StratumError

HELP = """\
Commands:
  set <key> <value>           store a value
  get <key>                   print a value, or (nil)
  del <key> [key ...]         delete keys
  exists <key>                is the key present?
  scan [start] [end] [limit]  list keys in [start, end); "" = no bound
  ping                        check the connection
  info                        engine statistics
  help                        this text
  quit / exit                 leave
Use quotes for spaces:  set greeting "hello world"\
"""


class _EmbeddedBackend:
    """Makes a local Stratum engine look like a StratumClient."""

    def __init__(self, directory):
        self.db = Stratum(directory)

    def ping(self):
        return True

    def get(self, key):
        return self.db.get(key.encode())

    def set(self, key, value):
        self.db.put(key.encode(), value.encode())
        return True

    def delete(self, *keys):
        removed = 0
        for key in keys:
            if key.encode() in self.db:
                self.db.delete(key.encode())
                removed += 1
        return removed

    def exists(self, *keys):
        return sum(key.encode() in self.db for key in keys)

    def scan(self, start="", end="", limit=1000):
        return list(self.db.scan(start.encode() or None, end.encode() or None, limit))

    def info(self):
        return "\n".join(
            f"{key}:{value}" for key, value in self.db.stats().items() if key != "tables"
        )

    def close(self):
        self.db.close()


def _show(data):
    return data.decode("utf-8", errors="backslashreplace")


class Repl:
    """Parses one line at a time and returns the text to print.

    Kept separate from input()/print() so it can be unit tested.
    """

    def __init__(self, backend):
        self.backend = backend
        self.done = False

    def execute(self, line):
        try:
            words = shlex.split(line)
        except ValueError as exc:
            return f"(error) {exc}"
        if not words:
            return ""
        name, args = words[0].lower(), words[1:]
        try:
            return self._run(name, args)
        except (ResponseError, StratumError, OSError) as exc:
            return f"(error) {exc}"

    def _run(self, name, args):
        backend = self.backend
        if name in ("quit", "exit"):
            self.done = True
            return "bye"
        if name == "help":
            return HELP
        if name == "ping" and not args:
            return "PONG" if backend.ping() else "(error) no PONG"
        if name == "get" and len(args) == 1:
            value = backend.get(args[0])
            return "(nil)" if value is None else _show(value)
        if name == "set" and len(args) == 2:
            backend.set(args[0], args[1])
            return "OK"
        if name == "del" and args:
            return f"(deleted {backend.delete(*args)})"
        if name == "exists" and len(args) == 1:
            return "yes" if backend.exists(args[0]) else "no"
        if name == "scan" and len(args) <= 3:
            start = args[0] if len(args) > 0 else ""
            end = args[1] if len(args) > 1 else ""
            try:
                limit = int(args[2]) if len(args) > 2 else 100
            except ValueError:
                return "(error) limit must be a number"
            pairs = backend.scan(start, end, limit)
            lines = [f"{_show(key)} -> {_show(value)}" for key, value in pairs]
            lines.append(f"({len(pairs)} keys)")
            return "\n".join(lines)
        if name == "info" and not args:
            return backend.info().replace("\r\n", "\n")
        if name in ("ping", "get", "set", "del", "exists", "scan", "info"):
            return f"(error) wrong arguments for '{name}' - type help"
        return f"(error) unknown command '{name}' - type help"


def main(argv=None, stdin=None, stdout=None):
    parser = argparse.ArgumentParser(description="Stratum interactive shell")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--dir", help="open this database directory directly "
                                      "instead of connecting to a server")
    args = parser.parse_args(argv)
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout

    try:
        if args.dir:
            backend = _EmbeddedBackend(args.dir)
            prompt = f"stratum[{args.dir}]> "
        else:
            backend = StratumClient(args.host, args.port)
            prompt = f"stratum[{args.host}:{args.port}]> "
    except (OSError, StratumError) as exc:
        print(f"could not open: {exc}", file=stdout)
        return 1

    repl = Repl(backend)
    interactive = stdin.isatty()
    try:
        while not repl.done:
            if interactive:
                stdout.write(prompt)
                stdout.flush()
            line = stdin.readline()
            if not line:
                break  # end of input (Ctrl-D)
            output = repl.execute(line)
            if output:
                print(output, file=stdout)
    except KeyboardInterrupt:
        print(file=stdout)
    finally:
        backend.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
