# kvstore — a persistent key-value store in Rust

An in-memory hash map exposed over TCP with an append-only log and periodic
snapshots. Pure Rust stdlib — no external crates.

```bash
cargo build --release
./target/release/kvstore          # localhost:6380
./target/release/kvstore 7000     # custom port
./target/release/kvstore 0 /tmp/mydata  # random port, custom data dir
```

```
$ nc localhost 6380
SET name raqueeb
+OK
GET name
$raqueeb
DEL name
:1
GET name
$-1
```

## Commands

| Command | Reply | Description |
|---|---|---|
| `SET key value` | `+OK` | store a key; value may contain spaces |
| `GET key` | `$value` or `$-1` | retrieve or null |
| `DEL key` | `:1` or `:0` | delete; returns whether it existed |
| `EXISTS key` | `:1` or `:0` | check existence |
| `KEYS [pattern]` | `*N` + items | glob match (`*`, `?`); default `*` |
| `DBSIZE` | `:N` | number of keys |
| `FLUSHDB` | `+OK` | delete everything |
| `PING [msg]` | `$PONG` or `$msg` | liveness check |
| `SAVE` | `+OK` | force a snapshot now |
| `INFO` | `$text` | key count and persistence stats |

## Wire protocol

Line-oriented. The client sends one command per `\n`. Replies are CRLF-terminated:

- `+OK` — success
- `$value` — a string (or `$-1` for null)
- `:42` — integer
- `-ERR message` — error
- `*N` followed by N `$item` lines — list

Compatible with `nc`, `telnet`, and any TCP socket library.

## Persistence

Two mechanisms, used together — exactly the AOF + RDB split that Redis uses:

**Append-only log (AOL):** every mutating command (`SET`, `DEL`, `FLUSHDB`) is
appended to `appendonly.log` and flushed immediately. On restart, the log is
replayed to rebuild state. Fast (one seek-to-end + write per command), but the
log grows without bound.

**Snapshot:** after a configurable number of mutations (default 1000), the
entire hash map is serialised to `snapshot.kvs` via a temp file + rename (atomic
on POSIX), and the AOL is truncated. On restart, the snapshot is loaded first,
then only the AOL entries written *after* it are replayed.

The tradeoff: the AOL is what guarantees no data loss between snapshots; the
snapshot is what keeps the AOL from growing forever.

## Project structure

```text
kvstore/
├── Cargo.toml
├── README.md
├── .gitignore
├── src/
│   ├── lib.rs
│   ├── store.rs
│   ├── server.rs
│   └── bin/
│       └── kvstore.rs
└── tests/
    ├── store_tests.rs
    └── integration_test.py
```

## Layout

| File | Responsibility |
|---|---|
| `src/store.rs` | HashMap, commands, AOL, snapshot, glob matcher |
| `src/server.rs` | TCP listener, poll() loop, per-client buffered I/O |
| `src/bin/kvstore.rs` | argument parsing, signal handling, startup |
| `tests/store_tests.rs` | 34 Rust tests: parsing, CRUD, persistence, corruption |
| `tests/integration_test.py` | 32 checks over real TCP: protocol, concurrency, restart |

## Tests

```bash
cargo test                              # 34 Rust unit + persistence tests
python3 tests/integration_test.py       # 32 checks over real TCP
```

The Rust tests cover: command parsing (case insensitivity, missing args, unknown
commands, spaces in values), every CRUD operation, KEYS with glob patterns, reply
encoding, AOL replay, snapshot creation and AOL truncation, snapshot + AOL
rebuild after restart, FLUSHDB replay, manual SAVE, corrupt log lines being
skipped, and the snapshot leaving no temp files behind.

The integration tests cover: every command over real TCP, error handling, KEYS
patterns, AOL-based data survival across a process restart, SAVE, cross-client
visibility, 10 concurrent writers, client disconnection not affecting others,
and a clean SIGINT shutdown.

## Design decisions, and how to defend them

**Why single-threaded + poll(), not thread-per-client.** A key-value store is a
shared mutable data structure. With threads, every read and write needs a mutex,
and a bug there is a data race — nondeterministic and invisible in testing.
With one thread, the borrow checker guarantees that the store is accessed by
exactly one call path at a time. There is nothing to lock because the compiler
has already proven there is nothing to race on. This is not just a convenience;
it is the core thesis of the project.

**The borrow checker lesson that is worth telling in an interview.** The first
version of `handle_readable` held a `&mut` borrow of `self.clients` (to read
bytes into the client's buffer) while also calling `self.store.execute()` (which
needs `&mut self`). This compiled in C and would have been a data race in a
multithreaded version. Rust refused to compile it. The fix was to split the
function into two phases: phase 1 borrows only the client buffer, then drops
the borrow; phase 2 borrows the store, executes, then re-borrows the client
to write the reply. Each phase touches a different part of `self`, which is
what satisfies the borrow checker. This is the kind of structural insight that
the ownership model gives you for free and that a garbage-collected language
would let you discover in production.

**Why the AOL is flushed after every command.** A crash between a write and a
flush loses the buffered commands. Flushing after every command means the worst
case is one lost command (if the process is killed between the write and the
fsync that the OS eventually does). This is the same tradeoff Redis makes with
`appendfsync always` — slower writes, stronger durability.

**Why the snapshot writes to a temp file and then renames.** Writing in place
means a crash mid-write leaves a truncated snapshot and data loss. Rename is
atomic on POSIX: the reader sees either the complete old snapshot or the
complete new one, never a partial one. This is the same pattern as the Go todo
list and the PHP notes API.

**Why the AOL is truncated after a snapshot.** Everything in the AOL is now
covered by the snapshot. Without truncation, the AOL grows forever and replay
at startup gets slower and slower. This is the compaction step that Redis's
`BGREWRITEAOF` does in the background; here it is synchronous and instant
because the snapshot already captured the full state.

**Why corrupt AOL lines are skipped, not fatal.** A power failure can leave a
partial write at the end of the log. Refusing to start would make the data in
the snapshot and the valid log entries inaccessible. Skipping the unrecognised
line and logging a warning is what Redis does with `aof-load-truncated yes`.
There is a test that writes garbage into the middle of a log and asserts that
the lines before and after are both loaded.

**Why SET splits into at most 3 parts.** `SET greeting hello world` should
store the value `"hello world"`, not just `"hello"`. `splitn(3, ...)` keeps
everything after the second space as the value. Without this, a user who stores
a sentence silently loses everything after the first word.

**Why KEYS uses a custom glob matcher instead of regex.** The KEYS pattern
language is `*` (any sequence) and `?` (one char), not a regex. Using
`regex::Regex` would mean pulling in a crate (unavailable here) and would also
accept patterns that are O(2^n) to match (ReDoS). The recursive glob matcher
is simple, correct, and O(n·m) worst case on the short keys a KV store holds.

**Why the poll() wrapper uses repr(C) instead of the libc crate.** The libc
crate is not available (crates.io is blocked), and Rust's stdlib does not
expose `poll()`. The `PollFd` struct is `#[repr(C)]` so its memory layout
matches the kernel's `struct pollfd`, and the `poll` function is declared via
`extern "C"`. The unsafe surface is four lines; everything above it is safe
Rust.

**Trade-offs I would raise.** There is no authentication, no expiry/TTL, no
pipelining, and no replication. Values are strings only — no lists, sets, or
hashes. The snapshot is synchronous: on a million-key store it blocks the event
loop for a visible pause; Redis forks to snapshot in the background (which is
the `BGSAVE` command). The protocol is not Redis-compatible (RESP uses
`$len\r\ndata\r\n` for bulk strings); making it so would be a good next step.
And the glob matcher, while correct, is recursive and would stack-overflow on
a pattern of a thousand `*` characters.
