import asyncio
import io
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

from stratum import Stratum, resp
from stratum.cli import Repl, _EmbeddedBackend
from stratum.cli import main as cli_main
from stratum.client import ResponseError, StratumClient
from stratum.server import ServerThread

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class RespCodecTest(unittest.TestCase):
    def test_encoders(self):
        self.assertEqual(resp.simple("OK"), b"+OK\r\n")
        self.assertEqual(resp.error("ERR bad\r\nthing"), b"-ERR bad  thing\r\n")
        self.assertEqual(resp.integer(-3), b":-3\r\n")
        self.assertEqual(resp.bulk(b"hi"), b"$2\r\nhi\r\n")
        self.assertEqual(resp.bulk(b""), b"$0\r\n\r\n")
        self.assertEqual(resp.bulk(None), b"$-1\r\n")
        self.assertEqual(resp.array([b"a", None]), b"*2\r\n$1\r\na\r\n$-1\r\n")
        self.assertEqual(
            resp.encode_command("SET", b"k", 5), b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n5\r\n"
        )
        with self.assertRaises(TypeError):
            resp.encode_command("SET", 1.5)
        with self.assertRaises(TypeError):
            resp.encode_command(True)

    def test_read_reply_all_types(self):
        data = b"+OK\r\n:42\r\n$3\r\na\r\n\r\n$-1\r\n*2\r\n$1\r\nx\r\n:7\r\n*-1\r\n"
        stream = io.BytesIO(data)
        self.assertEqual(resp.read_reply(stream), "OK")
        self.assertEqual(resp.read_reply(stream), 42)
        self.assertEqual(resp.read_reply(stream), b"a\r\n")  # binary safe
        self.assertIsNone(resp.read_reply(stream))
        self.assertEqual(resp.read_reply(stream), [b"x", 7])
        self.assertIsNone(resp.read_reply(stream))
        with self.assertRaises(ConnectionError):
            resp.read_reply(stream)  # end of stream

    def test_read_reply_errors(self):
        with self.assertRaises(ResponseError):
            resp.read_reply(io.BytesIO(b"-ERR nope\r\n"))
        with self.assertRaises(resp.ProtocolError):
            resp.read_reply(io.BytesIO(b"?what\r\n"))
        with self.assertRaises(resp.ProtocolError):
            resp.read_reply(io.BytesIO(b":notanumber\r\n"))
        with self.assertRaises(ConnectionError):
            resp.read_reply(io.BytesIO(b"$10\r\nshort\r\n"))

    def read_commands(self, data):
        async def go():
            reader = asyncio.StreamReader()
            reader.feed_data(data)
            reader.feed_eof()
            out = []
            while True:
                command = await resp.read_command(reader)
                if command is None:
                    return out
                out.append(command)

        return asyncio.run(go())

    def test_read_command(self):
        data = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING hello\r\n\r\n*0\r\n"
        self.assertEqual(
            self.read_commands(data), [[b"GET", b"k"], [b"PING", b"hello"], [], []]
        )

    def test_read_command_disconnect_mid_command(self):
        for data in (b"*2\r\n$3\r\nGET\r\n", b"*1\r\n$10\r\nabc", b"*1\r\n", b"PING"):
            self.assertEqual(self.read_commands(data), [], data)

    def test_read_command_protocol_errors(self):
        for data in (
            b"*x\r\n",
            b"*-1\r\n",
            b"*1\r\n:5\r\n",
            b"*1\r\n$x\r\n",
            b"*1\r\n$-5\r\n",
            b"*1\r\n$1\r\nabc\r\n",
            b"*1\r\n$999999999999\r\n",
        ):
            with self.assertRaises(resp.ProtocolError, msg=data):
                self.read_commands(data)


class ServerTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-server-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.db = Stratum(self.dir, sync=False, memtable_bytes=8192)
        self.addCleanup(self.db.close)
        self.server = ServerThread(self.db).start()
        self.addCleanup(self.server.stop)

    def client(self):
        client = StratumClient(port=self.server.port, timeout=10)
        self.addCleanup(client.close)
        return client

    def raw(self, payload, expect_bytes):
        """Send raw bytes, read back exactly ``expect_bytes`` bytes."""
        with socket.create_connection(("127.0.0.1", self.server.port), timeout=10) as sock:
            sock.sendall(payload)
            data = b""
            while len(data) < expect_bytes:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
            return data


class ServerCommandTest(ServerTestCase):
    def test_ping_echo(self):
        client = self.client()
        self.assertTrue(client.ping())
        self.assertEqual(client.execute("PING", "hi"), b"hi")
        self.assertEqual(client.execute("ECHO", "yo"), b"yo")
        self.assertEqual(client.execute("ping"), "PONG")  # case-insensitive

    def test_set_get_del_exists(self):
        client = self.client()
        self.assertIsNone(client.get("k"))
        self.assertTrue(client.set("k", "v"))
        self.assertEqual(client.get("k"), b"v")
        self.assertEqual(self.db.get(b"k"), b"v")  # really in the engine
        self.assertEqual(client.exists("k", "nope"), 1)
        client.set("k2", b"")
        self.assertEqual(client.get("k2"), b"")
        self.assertEqual(client.delete("k", "k2", "missing"), 2)
        self.assertIsNone(client.get("k"))
        self.assertEqual(client.delete("k"), 0)

    def test_binary_safe_keys_and_values(self):
        client = self.client()
        key = bytes(range(256))
        value = b"\r\n$5\r\n\x00" * 1000
        client.set(key, value)
        self.assertEqual(client.get(key), value)

    def test_large_value(self):
        client = self.client()
        value = os.urandom(3 * 1024 * 1024)
        client.set("big", value)
        self.assertEqual(client.get("big"), value)

    def test_scan(self):
        client = self.client()
        for i in range(50):
            client.set("key%02d" % i, "v%d" % i)
        everything = [(b"key%02d" % i, b"v%d" % i) for i in range(50)]
        self.assertEqual(client.scan(), everything)
        self.assertEqual(client.scan("key10", "key13"), everything[10:13])
        self.assertEqual(client.scan("key45"), everything[45:])
        self.assertEqual(client.scan(end="key03"), everything[:3])
        self.assertEqual(client.scan(limit=7), everything[:7])
        self.assertEqual(client.execute("RANGE", "key48", ""), [b"key48", b"v48", b"key49", b"v49"])
        self.assertEqual(list(client.scan_iter(page_size=7)), everything)
        self.assertEqual(list(client.scan_iter("key20", "key30", page_size=10)), everything[20:30])
        self.assertEqual(list(client.scan_iter("zzz")), [])

    def test_errors(self):
        client = self.client()
        cases = [
            (("NOPE",), "unknown command"),
            (("GET",), "wrong number of arguments"),
            (("GET", "a", "b"), "wrong number of arguments"),
            (("SET", "a"), "wrong number of arguments"),
            (("DEL",), "wrong number of arguments"),
            (("EXISTS",), "wrong number of arguments"),
            (("ECHO",), "wrong number of arguments"),
            (("PING", "a", "b"), "wrong number of arguments"),
            (("INFO", "x"), "wrong number of arguments"),
            (("SCAN", "a"), "wrong number of arguments"),
            (("SCAN", "a", "b", "LIMIT", "x"), "LIMIT must be an integer"),
            (("SCAN", "a", "b", "LIMIT", "0"), "LIMIT must be between"),
            (("SCAN", "a", "b", "COUNT", "5"), "syntax error"),
            (("SET", "k" * 70_000, "v"), "key is too large"),
        ]
        for command, message in cases:
            with self.assertRaisesRegex(ResponseError, message, msg=command):
                client.execute(*command)
        self.assertTrue(client.ping())  # the connection is still usable

    def test_info_and_command(self):
        client = self.client()
        client.set("a", "1")
        self.db.flush()
        info = client.info()
        self.assertIn("memtable_entries:0", info)
        self.assertIn("table_count:1", info)
        self.assertIn("table_0", info)
        self.assertEqual(client.execute("COMMAND", "DOCS"), [])

    def test_pipelining(self):
        client = self.client()
        commands = [("SET", "k%d" % i, i) for i in range(200)]
        commands += [("GET", "k%d" % i) for i in range(200)]
        commands.append(("BOGUS",))
        replies = client.pipeline(commands)
        self.assertEqual(replies[:200], ["OK"] * 200)
        self.assertEqual(replies[200:400], [b"%d" % i for i in range(200)])
        self.assertIsInstance(replies[400], ResponseError)

    def test_inline_commands_and_quit(self):
        expected = b"+OK\r\n$1\r\nv\r\n+PONG\r\n+OK\r\n"
        got = self.raw(b"SET k v\r\n\r\nGET k\r\nPING\r\nQUIT\r\nPING\r\n", len(expected) + 10)
        self.assertEqual(got, expected)  # nothing after QUIT

    def test_protocol_error_closes_connection(self):
        got = self.raw(b"*1\r\n:5\r\n", 200)
        self.assertTrue(got.startswith(b"-ERR protocol error"))

    def test_abrupt_disconnect_does_not_hurt_server(self):
        self.raw(b"*2\r\n$3\r\nGET\r\n", 0)  # half a command, then hang up
        self.assertTrue(self.client().ping())

    def test_many_concurrent_clients(self):
        errors = []

        def worker(worker_id):
            try:
                with StratumClient(port=self.server.port, timeout=30) as client:
                    for i in range(100):
                        key = "w%d-k%d" % (worker_id, i)
                        client.set(key, "v%d" % i)
                        if client.get(key) != b"v%d" % i:
                            errors.append(f"{key}: read back the wrong value")
            except Exception as exc:
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.client().scan(limit=5000)), 800)
        self.assertGreater(self.db.stats()["flushes"], 0)

    def test_port_in_use_raises(self):
        with self.assertRaises(OSError):
            ServerThread(self.db, port=self.server.port).start()


class ServerProcessTest(unittest.TestCase):
    """Start the real `python -m stratum.server`, stop it with SIGTERM."""

    def test_server_process_persists_data_across_restart(self):
        directory = tempfile.mkdtemp(prefix="stratum-proc-")
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]

        def start():
            proc = subprocess.Popen(
                [sys.executable, "-m", "stratum.server", "--dir", directory,
                 "--port", str(port), "--no-sync"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            self.addCleanup(proc.stdout.close)
            self.addCleanup(lambda: proc.poll() is None and proc.kill())
            self.assertIn(b"listening", proc.stdout.readline())
            return proc

        proc = start()
        with StratumClient(port=port, timeout=10) as client:
            client.set("durable", "yes")
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=10), 0)
        self.assertIn(b"stopped cleanly", proc.stdout.read())

        proc = start()
        with StratumClient(port=port, timeout=10) as client:
            self.assertEqual(client.get("durable"), b"yes")
        proc.send_signal(signal.SIGINT)
        self.assertEqual(proc.wait(timeout=10), 0)


class ReplTest(ServerTestCase):
    def check_backend(self, repl):
        run = repl.execute
        self.assertEqual(run("ping"), "PONG")
        self.assertEqual(run("get a"), "(nil)")
        self.assertEqual(run('set a "hello world"'), "OK")
        self.assertEqual(run("get a"), "hello world")
        self.assertEqual(run("SET b 2"), "OK")  # commands are case-insensitive
        self.assertEqual(run("exists a"), "yes")
        self.assertEqual(run("exists zzz"), "no")
        self.assertEqual(run("scan"), "a -> hello world\nb -> 2\n(2 keys)")
        self.assertEqual(run("scan b"), "b -> 2\n(1 keys)")
        self.assertEqual(run('scan "" b'), "a -> hello world\n(1 keys)")
        self.assertEqual(run('scan "" "" 1'), "a -> hello world\n(1 keys)")
        self.assertEqual(run("scan a b x"), "(error) limit must be a number")
        self.assertEqual(run("del a b nope"), "(deleted 2)")
        self.assertEqual(run("get a"), "(nil)")
        self.assertEqual(run(""), "")
        self.assertIn("memtable_entries", run("info"))
        self.assertIn("Commands:", run("help"))
        self.assertIn("unknown command", run("frobnicate"))
        self.assertIn("wrong arguments", run("get"))
        self.assertIn("(error)", run('set a "unterminated'))
        self.assertFalse(repl.done)
        self.assertEqual(run("quit"), "bye")
        self.assertTrue(repl.done)

    def test_repl_against_server(self):
        self.check_backend(Repl(self.client()))

    def test_repl_shows_server_errors(self):
        repl = Repl(self.client())
        self.assertIn("(error)", repl.execute("set %s v" % ("k" * 70_000)))

    def test_repl_embedded(self):
        directory = tempfile.mkdtemp(prefix="stratum-cli-")
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        backend = _EmbeddedBackend(directory)
        self.addCleanup(backend.close)
        self.check_backend(Repl(backend))

    def test_main_loop_with_piped_input(self):
        out = io.StringIO()
        code = cli_main(
            ["--port", str(self.server.port)],
            stdin=io.StringIO("set x 1\nget x\nquit\nget never-run\n"),
            stdout=out,
        )
        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue(), "OK\n1\nbye\n")

    def test_main_embedded_until_eof(self):
        directory = tempfile.mkdtemp(prefix="stratum-cli-")
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        out = io.StringIO()
        code = cli_main(["--dir", directory], stdin=io.StringIO("set x 1\n\nget x\n"), stdout=out)
        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue(), "OK\n1\n")

    def test_main_reports_connection_failure(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            free_port = probe.getsockname()[1]
        out = io.StringIO()
        self.assertEqual(cli_main(["--port", str(free_port)], stdin=io.StringIO(""), stdout=out), 1)
        self.assertIn("could not open", out.getvalue())


if __name__ == "__main__":
    unittest.main()
