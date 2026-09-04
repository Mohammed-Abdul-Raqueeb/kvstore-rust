//! tests/store_tests.rs — tests for command parsing, store logic, and persistence.

use std::path::PathBuf;

use kvstore::store::*;

fn mem() -> Store {
    Store::in_memory()
}

fn exec(store: &mut Store, line: &str) -> Reply {
    let cmd = Command::parse(line).unwrap();
    store.execute(&cmd)
}

fn exec_raw(store: &mut Store, line: &str) -> Reply {
    match Command::parse(line) {
        Ok(cmd) => store.execute(&cmd),
        Err(msg) => Reply::Error(msg),
    }
}

// --------------------------------------------------------------- command parsing

#[test]
fn parse_set_preserves_spaces_in_value() {
    let cmd = Command::parse("SET greeting hello world").unwrap();
    assert_eq!(
        cmd,
        Command::Set {
            key: "greeting".into(),
            value: "hello world".into()
        }
    );
}

#[test]
fn parse_get() {
    let cmd = Command::parse("GET mykey").unwrap();
    assert_eq!(cmd, Command::Get { key: "mykey".into() });
}

#[test]
fn parse_is_case_insensitive() {
    assert!(Command::parse("set k v").is_ok());
    assert!(Command::parse("SeT k v").is_ok());
    assert!(Command::parse("GET k").is_ok());
    assert!(Command::parse("get k").is_ok());
}

#[test]
fn parse_missing_arguments() {
    assert!(Command::parse("GET").is_err());
    assert!(Command::parse("SET key").is_err());
    assert!(Command::parse("DEL").is_err());
}

#[test]
fn parse_unknown_command() {
    let err = Command::parse("FROBNICATE").unwrap_err();
    assert!(err.contains("unknown"), "got: {}", err);
}

#[test]
fn parse_empty_line() {
    assert!(Command::parse("").is_err());
    assert!(Command::parse("   ").is_err());
}

// --------------------------------------------------------------- CRUD

#[test]
fn get_missing_key_returns_null() {
    let mut s = mem();
    assert_eq!(exec(&mut s, "GET noexist"), Reply::Null);
}

#[test]
fn set_then_get() {
    let mut s = mem();
    assert_eq!(exec(&mut s, "SET color blue"), Reply::Ok);
    assert_eq!(exec(&mut s, "GET color"), Reply::Value("blue".into()));
}

#[test]
fn set_overwrites() {
    let mut s = mem();
    exec(&mut s, "SET k first");
    exec(&mut s, "SET k second");
    assert_eq!(exec(&mut s, "GET k"), Reply::Value("second".into()));
}

#[test]
fn del_returns_1_for_existing_0_for_missing() {
    let mut s = mem();
    exec(&mut s, "SET k v");
    assert_eq!(exec(&mut s, "DEL k"), Reply::Integer(1));
    assert_eq!(exec(&mut s, "DEL k"), Reply::Integer(0));
    assert_eq!(exec(&mut s, "GET k"), Reply::Null);
}

#[test]
fn exists() {
    let mut s = mem();
    assert_eq!(exec(&mut s, "EXISTS k"), Reply::Integer(0));
    exec(&mut s, "SET k v");
    assert_eq!(exec(&mut s, "EXISTS k"), Reply::Integer(1));
}

#[test]
fn dbsize() {
    let mut s = mem();
    assert_eq!(exec(&mut s, "DBSIZE"), Reply::Integer(0));
    exec(&mut s, "SET a 1");
    exec(&mut s, "SET b 2");
    assert_eq!(exec(&mut s, "DBSIZE"), Reply::Integer(2));
}

#[test]
fn flushdb_clears_everything() {
    let mut s = mem();
    exec(&mut s, "SET a 1");
    exec(&mut s, "SET b 2");
    assert_eq!(exec(&mut s, "FLUSHDB"), Reply::Ok);
    assert_eq!(exec(&mut s, "DBSIZE"), Reply::Integer(0));
}

#[test]
fn ping() {
    let mut s = mem();
    assert_eq!(exec(&mut s, "PING"), Reply::Value("PONG".into()));
    assert_eq!(
        exec(&mut s, "PING hello"),
        Reply::Value("hello".into())
    );
}

#[test]
fn info_contains_keys_count() {
    let mut s = mem();
    exec(&mut s, "SET a 1");
    if let Reply::Value(info) = exec(&mut s, "INFO") {
        assert!(info.contains("keys:1"), "got: {}", info);
    } else {
        panic!("INFO should return a Value");
    }
}

// --------------------------------------------------------------- KEYS and glob

#[test]
fn keys_star_returns_all() {
    let mut s = mem();
    exec(&mut s, "SET apple 1");
    exec(&mut s, "SET banana 2");
    if let Reply::List(mut keys) = exec(&mut s, "KEYS *") {
        keys.sort();
        assert_eq!(keys, vec!["apple", "banana"]);
    } else {
        panic!("KEYS should return a List");
    }
}

#[test]
fn keys_with_pattern() {
    let mut s = mem();
    exec(&mut s, "SET user:1 alice");
    exec(&mut s, "SET user:2 bob");
    exec(&mut s, "SET session:1 tok");
    if let Reply::List(keys) = exec(&mut s, "KEYS user:*") {
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|k| k.starts_with("user:")));
    } else {
        panic!("KEYS should return a List");
    }
}

#[test]
fn glob_question_mark() {
    assert!(glob_match("h?llo", "hello"));
    assert!(glob_match("h?llo", "hallo"));
    assert!(!glob_match("h?llo", "hllo"));
}

#[test]
fn glob_star() {
    assert!(glob_match("h*o", "hello"));
    assert!(glob_match("h*o", "ho"));
    assert!(glob_match("*", "anything"));
    assert!(glob_match("*", ""));
}

#[test]
fn glob_literal() {
    assert!(glob_match("exact", "exact"));
    assert!(!glob_match("exact", "nope"));
}

// --------------------------------------------------------------- reply encoding

#[test]
fn reply_encode_ok() {
    assert_eq!(Reply::Ok.encode(), "+OK\r\n");
}

#[test]
fn reply_encode_value() {
    assert_eq!(Reply::Value("hi".into()).encode(), "$hi\r\n");
}

#[test]
fn reply_encode_null() {
    assert_eq!(Reply::Null.encode(), "$-1\r\n");
}

#[test]
fn reply_encode_integer() {
    assert_eq!(Reply::Integer(42).encode(), ":42\r\n");
}

#[test]
fn reply_encode_error() {
    let encoded = Reply::Error("bad".into()).encode();
    assert!(encoded.starts_with("-ERR"));
    assert!(encoded.contains("bad"));
}

#[test]
fn reply_encode_list() {
    let r = Reply::List(vec!["a".into(), "b".into()]);
    let enc = r.encode();
    assert!(enc.starts_with("*2\r\n"));
    assert!(enc.contains("$a\r\n"));
}

// --------------------------------------------------------------- persistence

fn temp_store(threshold: u64) -> (Store, PathBuf) {
    let dir = std::env::temp_dir().join(format!("kvstore-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let mut config = StoreConfig::new(&dir);
    config.snapshot_threshold = threshold;
    let store = Store::open(config).unwrap();
    (store, dir)
}

fn reopen(dir: &PathBuf) -> Store {
    let config = StoreConfig::new(dir);
    Store::open(config).unwrap()
}

#[test]
fn data_survives_aol_replay() {
    let (mut s, dir) = temp_store(999999);
    exec(&mut s, "SET name raqueeb");
    exec(&mut s, "SET city hyderabad");
    exec(&mut s, "DEL city");
    drop(s);

    let mut s2 = reopen(&dir);
    assert_eq!(exec(&mut s2, "GET name"), Reply::Value("raqueeb".into()));
    assert_eq!(exec(&mut s2, "GET city"), Reply::Null);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_is_written_and_aol_is_truncated() {
    let (mut s, dir) = temp_store(3); // snapshot after 3 mutations
    exec(&mut s, "SET a 1");
    exec(&mut s, "SET b 2");
    exec(&mut s, "SET c 3"); // triggers snapshot

    let aol = std::fs::read_to_string(dir.join("appendonly.log")).unwrap();
    assert!(
        aol.trim().is_empty(),
        "AOL should be empty after snapshot, got: {:?}",
        aol
    );
    assert!(dir.join("snapshot.kvs").is_file());
    drop(s);

    let mut s2 = reopen(&dir);
    assert_eq!(exec(&mut s2, "GET a"), Reply::Value("1".into()));
    assert_eq!(exec(&mut s2, "GET b"), Reply::Value("2".into()));
    assert_eq!(exec(&mut s2, "GET c"), Reply::Value("3".into()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_plus_aol_rebuild() {
    let (mut s, dir) = temp_store(3);
    // 3 SETs trigger a snapshot.
    exec(&mut s, "SET a 1");
    exec(&mut s, "SET b 2");
    exec(&mut s, "SET c 3");
    // Then two more go into the new AOL.
    exec(&mut s, "SET d 4");
    exec(&mut s, "DEL b");
    drop(s);

    let mut s2 = reopen(&dir);
    assert_eq!(exec(&mut s2, "GET a"), Reply::Value("1".into()));
    assert_eq!(exec(&mut s2, "GET b"), Reply::Null);
    assert_eq!(exec(&mut s2, "GET c"), Reply::Value("3".into()));
    assert_eq!(exec(&mut s2, "GET d"), Reply::Value("4".into()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn empty_store_opens_without_error() {
    let (s, dir) = temp_store(1000);
    assert!(s.is_empty());
    drop(s);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flushdb_is_replayed() {
    let (mut s, dir) = temp_store(999999);
    exec(&mut s, "SET a 1");
    exec(&mut s, "SET b 2");
    exec(&mut s, "FLUSHDB");
    exec(&mut s, "SET c 3");
    drop(s);

    let mut s2 = reopen(&dir);
    assert_eq!(exec(&mut s2, "GET a"), Reply::Null);
    assert_eq!(exec(&mut s2, "GET c"), Reply::Value("3".into()));
    assert_eq!(s2.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn manual_save_command() {
    let (mut s, dir) = temp_store(999999);
    exec(&mut s, "SET x 42");
    assert_eq!(exec(&mut s, "SAVE"), Reply::Ok);
    assert!(dir.join("snapshot.kvs").is_file());
    drop(s);

    let mut s2 = reopen(&dir);
    assert_eq!(exec(&mut s2, "GET x"), Reply::Value("42".into()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupt_aol_line_is_skipped_not_fatal() {
    let (_, dir) = temp_store(999999);
    // Write a valid line, then garbage, then another valid line.
    std::fs::write(
        dir.join("appendonly.log"),
        "SET good value\nGARBAGE LINE\nSET also_good ok\n",
    )
    .unwrap();

    let mut s = reopen(&dir);
    assert_eq!(exec(&mut s, "GET good"), Reply::Value("value".into()));
    assert_eq!(exec(&mut s, "GET also_good"), Reply::Value("ok".into()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_is_atomic_no_tmp_left() {
    let (mut s, dir) = temp_store(2);
    exec(&mut s, "SET a 1");
    exec(&mut s, "SET b 2"); // triggers snapshot
    assert!(!dir.join("snapshot.kvs.tmp").exists());
    assert!(dir.join("snapshot.kvs").exists());
    let _ = std::fs::remove_dir_all(&dir);
}
