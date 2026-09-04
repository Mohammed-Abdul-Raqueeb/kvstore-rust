#!/usr/bin/env python3
"""
integration_test.py — drives the real kvstore server over TCP.

Each test function starts its own server on port 0 with a temporary data
directory, so tests cannot interfere.
"""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

BIN = os.environ.get(
    "KVSTORE_BIN",
    os.path.join(os.path.dirname(__file__), "..", "target", "debug", "kvstore"),
)

passed = 0
failed = 0
failures = []


def check(cond, msg):
    global passed, failed
    if cond:
        passed += 1
    else:
        failed += 1
        failures.append(msg)
        print(f"      FAIL: {msg}")


class KV:
    """A test client that speaks the line protocol."""

    def __init__(self, port, timeout=3.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def cmd(self, line):
        """Sends a command and returns the reply line (without the CRLF)."""
        self.sock.sendall((line + "\n").encode())
        return self._read_reply()

    def _read_reply(self):
        """Reads one CRLF-terminated reply."""
        while b"\r\n" not in self.buf:
            try:
                chunk = self.sock.recv(4096)
            except (socket.timeout, TimeoutError):
                return None
            if not chunk:
                return None
            self.buf += chunk

        line, self.buf = self.buf.split(b"\r\n", 1)
        result = line.decode(errors="replace")

        # Multi-bulk replies (*N): read N more lines.
        if result.startswith("*"):
            count = int(result[1:])
            items = []
            for _ in range(count):
                while b"\r\n" not in self.buf:
                    try:
                        chunk = self.sock.recv(4096)
                    except (socket.timeout, TimeoutError):
                        return None
                    if not chunk:
                        return None
                    self.buf += chunk
                item, self.buf = self.buf.split(b"\r\n", 1)
                items.append(item.decode(errors="replace"))
            return items

        return result

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class Server:
    def __init__(self, extra_args=None):
        self.tmpdir = tempfile.mkdtemp(prefix="kvtest-")
        cmd = [BIN, "0", self.tmpdir]
        if extra_args:
            cmd.extend(extra_args)
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        line = self.proc.stdout.readline()
        if "listening on" not in line:
            self.proc.kill()
            raise RuntimeError(f"server failed to start: {line!r}")
        self.port = int(line.strip().split(":")[-1])

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def client(self, **kw):
        return KV(self.port, **kw)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop()


def test(name):
    def wrap(fn):
        global passed, failed
        before = failed
        try:
            fn()
            status = "ok  " if failed == before else "FAIL"
        except Exception as exc:
            failed += 1
            failures.append(f"{name}: {exc!r}")
            print(f"      FAIL: {exc!r}")
            status = "FAIL"
        print(f"  {status} {name}")
        return fn
    return wrap


# ------------------------------------------------------------------- tests

print("basic CRUD")

@test("SET then GET returns the value")
def _():
    with Server() as s:
        c = s.client()
        check(c.cmd("SET color blue") == "+OK", "SET should return +OK")
        check(c.cmd("GET color") == "$blue", "GET should return the value")
        c.close()

@test("GET on a missing key returns null")
def _():
    with Server() as s:
        c = s.client()
        check(c.cmd("GET noexist") == "$-1", "missing key should be null")
        c.close()

@test("SET overwrites the previous value")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET k first")
        c.cmd("SET k second")
        check(c.cmd("GET k") == "$second", "should be overwritten")
        c.close()

@test("DEL removes a key and reports 1/0")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET k v")
        check(c.cmd("DEL k") == ":1", "DEL existing should return :1")
        check(c.cmd("DEL k") == ":0", "DEL missing should return :0")
        check(c.cmd("GET k") == "$-1", "key should be gone")
        c.close()

@test("EXISTS reports 0/1")
def _():
    with Server() as s:
        c = s.client()
        check(c.cmd("EXISTS k") == ":0", "missing key")
        c.cmd("SET k v")
        check(c.cmd("EXISTS k") == ":1", "present key")
        c.close()

@test("DBSIZE tracks the key count")
def _():
    with Server() as s:
        c = s.client()
        check(c.cmd("DBSIZE") == ":0", "empty")
        c.cmd("SET a 1")
        c.cmd("SET b 2")
        check(c.cmd("DBSIZE") == ":2", "two keys")
        c.close()

@test("FLUSHDB clears the store")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET a 1")
        c.cmd("SET b 2")
        check(c.cmd("FLUSHDB") == "+OK", "FLUSHDB should succeed")
        check(c.cmd("DBSIZE") == ":0", "should be empty")
        c.close()

@test("PING returns PONG or echoes back")
def _():
    with Server() as s:
        c = s.client()
        check(c.cmd("PING") == "$PONG", "bare PING")
        check(c.cmd("PING hello") == "$hello", "PING with arg")
        c.close()

@test("SET preserves spaces in values")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET msg hello world how are you")
        check(c.cmd("GET msg") == "$hello world how are you", "spaces preserved")
        c.close()

@test("commands are case-insensitive")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("set k v")
        check(c.cmd("get k") == "$v", "lowercase should work")
        c.close()

@test("unknown command returns an error")
def _():
    with Server() as s:
        c = s.client()
        reply = c.cmd("FROBNICATE")
        check(reply is not None and reply.startswith("-ERR"), f"got: {reply!r}")
        c.close()

@test("missing arguments return an error")
def _():
    with Server() as s:
        c = s.client()
        for bad in ["GET", "SET key", "DEL"]:
            reply = c.cmd(bad)
            check(reply is not None and reply.startswith("-ERR"), f"{bad!r} -> {reply!r}")
        c.close()


print("\nKEYS and patterns")

@test("KEYS * returns all keys")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET apple 1")
        c.cmd("SET banana 2")
        keys = c.cmd("KEYS *")
        check(isinstance(keys, list) and len(keys) == 2, f"got: {keys!r}")
        c.close()

@test("KEYS with a prefix pattern")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET user:1 alice")
        c.cmd("SET user:2 bob")
        c.cmd("SET session:1 tok")
        keys = c.cmd("KEYS user:*")
        check(isinstance(keys, list) and len(keys) == 2, f"got: {keys!r}")
        c.close()


print("\npersistence")

@test("data survives a restart via AOL replay")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET name raqueeb")
        c.cmd("SET city hyderabad")
        c.cmd("DEL city")
        c.close()
        port = s.port
        datadir = s.tmpdir
    # Server stopped. Reopen with the same data dir.
    with Server() as s2:
        # We need to point at the same data dir; Server() makes a new one.
        pass
    # Actually do it properly:
    s_first = Server()
    c = s_first.client()
    c.cmd("SET name raqueeb")
    c.cmd("SET city hyderabad")
    c.cmd("DEL city")
    c.close()
    datadir = s_first.tmpdir
    s_first.stop()
    time.sleep(0.5)

    # Reopen pointing at the same data.
    proc2 = subprocess.Popen(
        [BIN, "0", datadir],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    line = proc2.stdout.readline()
    port2 = int(line.strip().split(":")[-1])
    c2 = KV(port2)
    check(c2.cmd("GET name") == "$raqueeb", "name should survive")
    check(c2.cmd("GET city") == "$-1", "city should be deleted")
    c2.close()
    proc2.send_signal(signal.SIGINT)
    proc2.wait(timeout=5)

@test("SAVE creates a snapshot")
def _():
    with Server() as s:
        c = s.client()
        c.cmd("SET x 42")
        check(c.cmd("SAVE") == "+OK", "SAVE should succeed")
        import pathlib
        check(pathlib.Path(s.tmpdir, "snapshot.kvs").is_file(), "snapshot file should exist")
        c.close()


print("\nmultiple clients")

@test("one client's SET is visible to another client")
def _():
    with Server() as s:
        a = s.client()
        b = s.client()
        a.cmd("SET shared 123")
        check(b.cmd("GET shared") == "$123", "client B should see client A's write")
        a.close()
        b.close()

@test("10 clients writing concurrently")
def _():
    with Server() as s:
        clients = [s.client() for _ in range(10)]
        for i, c in enumerate(clients):
            c.cmd(f"SET key{i} val{i}")
        # Verify with a fresh client.
        checker = s.client()
        for i in range(10):
            check(checker.cmd(f"GET key{i}") == f"$val{i}", f"key{i} missing")
        checker.close()
        for c in clients:
            c.close()

@test("a client disconnecting does not affect others")
def _():
    with Server() as s:
        a = s.client()
        b = s.client()
        a.cmd("SET a 1")
        a.close()  # drop connection
        check(b.cmd("GET a") == "$1", "data should persist across disconnects")
        b.close()


print("\nshutdown")

@test("SIGINT shuts the server down cleanly")
def _():
    s = Server()
    c = s.client()
    c.cmd("SET alive true")
    c.close()
    s.proc.send_signal(signal.SIGINT)
    try:
        code = s.proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        s.proc.kill()
        s.proc.wait()
        check(False, "server did not exit within 5s of SIGINT")
        return
    check(code == 0, f"expected exit 0, got {code}")


# --------------------------------------------------------------- summary

print("\n" + "-" * 50)
if failed == 0:
    print(f"All {passed} checks passed.")
    sys.exit(0)

print(f"{failed} check(s) failed:")
for f in failures:
    print(f"  - {f}")
sys.exit(1)
