//! store.rs — the in-memory key-value store with persistence.
//!
//! Two persistence strategies, both used:
//!
//! **Append-only log (AOL):** every mutating command is appended to a log file
//! the moment it succeeds. On restart the log is replayed to rebuild state.
//! Fast writes (one seek-to-end per command), but the log grows without bound.
//!
//! **Snapshot:** a periodic full dump of the hash map. On restart, load the
//! snapshot first, then replay only the log entries written *after* it. This
//! is exactly the AOF + RDB split that Redis uses.
//!
//! The tradeoff: the AOL is what guarantees no data loss between snapshots;
//! the snapshot is what keeps the AOL from growing forever.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// The result of a store command, ready to be sent back to the client.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Ok,
    Value(String),
    Null,
    Integer(i64),
    Error(String),
    List(Vec<String>),
}

impl Reply {
    /// Wire format: a simple line protocol.
    ///
    ///   +OK          → success
    ///   $value       → a string value
    ///   $-1          → null (key not found)
    ///   :42          → integer
    ///   -ERR message → error
    ///   *3\r\n$a\r\n$b\r\n$c  → list
    pub fn encode(&self) -> String {
        match self {
            Reply::Ok => "+OK\r\n".to_string(),
            Reply::Value(v) => format!("${}\r\n", v),
            Reply::Null => "$-1\r\n".to_string(),
            Reply::Integer(n) => format!(":{}\r\n", n),
            Reply::Error(msg) => format!("-ERR {}\r\n", msg),
            Reply::List(items) => {
                let mut out = format!("*{}\r\n", items.len());
                for item in items {
                    out.push_str(&format!("${}\r\n", item));
                }
                out
            }
        }
    }
}

/// Configuration for persistence paths and thresholds.
pub struct StoreConfig {
    pub data_dir: PathBuf,
    /// How many log entries before we trigger a snapshot.
    pub snapshot_threshold: u64,
}

impl StoreConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            snapshot_threshold: 1000,
        }
    }

    fn aol_path(&self) -> PathBuf {
        self.data_dir.join("appendonly.log")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.data_dir.join("snapshot.kvs")
    }

    fn snapshot_tmp_path(&self) -> PathBuf {
        self.data_dir.join("snapshot.kvs.tmp")
    }
}

pub struct Store {
    data: HashMap<String, String>,
    config: StoreConfig,
    aol: Option<BufWriter<File>>,
    /// Commands since the last snapshot; drives the threshold check.
    aol_since_snapshot: u64,
}

impl Store {
    /// Creates a new store, loading any existing data from disk.
    pub fn open(config: StoreConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.data_dir)?;

        let mut data = HashMap::new();

        // 1. Load the snapshot if it exists.
        let snap_path = config.snapshot_path();
        if snap_path.is_file() {
            load_snapshot(&snap_path, &mut data)?;
        }

        // 2. Replay the append-only log on top.
        let aol_path = config.aol_path();
        if aol_path.is_file() {
            replay_aol(&aol_path, &mut data)?;
        }

        // 3. Open the AOL for future appends.
        let aol_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&aol_path)?;

        Ok(Self {
            data,
            config,
            aol: Some(BufWriter::new(aol_file)),
            aol_since_snapshot: 0,
        })
    }

    /// Creates a purely in-memory store (no persistence). Used by tests.
    pub fn in_memory() -> Self {
        Self {
            data: HashMap::new(),
            config: StoreConfig::new("/dev/null"),
            aol: None,
            aol_since_snapshot: 0,
        }
    }

    // ---------------------------------------------------------------- commands

    pub fn execute(&mut self, cmd: &Command) -> Reply {
        match cmd {
            Command::Get { key } => self.get(key),
            Command::Set { key, value } => self.set(key, value),
            Command::Del { key } => self.del(key),
            Command::Exists { key } => self.exists(key),
            Command::Keys { pattern } => self.keys(pattern),
            Command::Dbsize => Reply::Integer(self.data.len() as i64),
            Command::Ping { msg } => match msg {
                Some(m) => Reply::Value(m.clone()),
                None => Reply::Value("PONG".to_string()),
            },
            Command::Flushdb => self.flushdb(),
            Command::Save => self.save_snapshot(),
            Command::Info => self.info(),
        }
    }

    fn get(&self, key: &str) -> Reply {
        match self.data.get(key) {
            Some(v) => Reply::Value(v.clone()),
            None => Reply::Null,
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Reply {
        self.data.insert(key.to_string(), value.to_string());
        self.append_log(&format!("SET {} {}\n", key, value));
        Reply::Ok
    }

    fn del(&mut self, key: &str) -> Reply {
        let removed = self.data.remove(key).is_some();
        if removed {
            self.append_log(&format!("DEL {}\n", key));
        }
        Reply::Integer(if removed { 1 } else { 0 })
    }

    fn exists(&self, key: &str) -> Reply {
        Reply::Integer(if self.data.contains_key(key) { 1 } else { 0 })
    }

    fn keys(&self, pattern: &str) -> Reply {
        let matched: Vec<String> = if pattern == "*" {
            self.data.keys().cloned().collect()
        } else {
            self.data
                .keys()
                .filter(|k| glob_match(pattern, k))
                .cloned()
                .collect()
        };
        Reply::List(matched)
    }

    fn flushdb(&mut self) -> Reply {
        self.data.clear();
        self.append_log("FLUSHDB\n");
        Reply::Ok
    }

    fn save_snapshot(&mut self) -> Reply {
        match self.write_snapshot() {
            Ok(()) => Reply::Ok,
            Err(e) => Reply::Error(format!("snapshot failed: {}", e)),
        }
    }

    fn info(&self) -> Reply {
        let info = format!(
            "keys:{}\naol_since_snapshot:{}\nsnapshot_threshold:{}",
            self.data.len(),
            self.aol_since_snapshot,
            self.config.snapshot_threshold,
        );
        Reply::Value(info)
    }

    // ----------------------------------------------------------- persistence

    fn append_log(&mut self, line: &str) {
        if let Some(ref mut aol) = self.aol {
            // Best-effort: a failed append is logged but does not crash the
            // server. The snapshot is the safety net.
            if let Err(e) = aol.write_all(line.as_bytes()) {
                eprintln!("[aol] write failed: {}", e);
                return;
            }
            // Flush after every command so a crash loses at most one.
            if let Err(e) = aol.flush() {
                eprintln!("[aol] flush failed: {}", e);
            }
        }

        self.aol_since_snapshot += 1;

        if self.aol_since_snapshot >= self.config.snapshot_threshold {
            if let Err(e) = self.write_snapshot() {
                eprintln!("[snapshot] auto-snapshot failed: {}", e);
            }
        }
    }

    /// Writes an atomic snapshot: serialize to a temp file, then rename.
    fn write_snapshot(&mut self) -> io::Result<()> {
        let tmp = self.config.snapshot_tmp_path();
        let dst = self.config.snapshot_path();

        {
            let file = File::create(&tmp)?;
            let mut w = BufWriter::new(file);

            for (k, v) in &self.data {
                writeln!(w, "{} {}", k, v)?;
            }
            w.flush()?;
        }

        fs::rename(&tmp, &dst)?;

        // Truncate the AOL: everything in it is now covered by the snapshot.
        let aol_path = self.config.aol_path();
        if aol_path.is_file() {
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&aol_path)?;
            self.aol = Some(BufWriter::new(file));
        }

        self.aol_since_snapshot = 0;
        Ok(())
    }

    /// Number of keys, for tests.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// ---------------------------------------------------------------- file I/O

/// Loads a snapshot: each line is `key value`.
fn load_snapshot(path: &Path, data: &mut HashMap<String, String>) -> io::Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((key, value)) = trimmed.split_once(' ') {
            data.insert(key.to_string(), value.to_string());
        }
    }
    Ok(())
}

/// Replays the append-only log: each line is a command.
fn replay_aol(path: &Path, data: &mut HashMap<String, String>) -> io::Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
        match parts.first().map(|s| s.to_uppercase()).as_deref() {
            Some("SET") if parts.len() >= 3 => {
                data.insert(parts[1].to_string(), parts[2].to_string());
            }
            Some("DEL") if parts.len() >= 2 => {
                data.remove(parts[1]);
            }
            Some("FLUSHDB") => {
                data.clear();
            }
            _ => {
                // Unknown log entry: skip rather than crash. A hand-edited
                // log or a partial write should not prevent startup.
                eprintln!("[aol] skipping unrecognised line: {:?}", trimmed);
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------ command parsing

/// A parsed client command.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Del { key: String },
    Exists { key: String },
    Keys { pattern: String },
    Dbsize,
    Ping { msg: Option<String> },
    Flushdb,
    Save,
    Info,
}

impl Command {
    /// Parses one line of client input into a command.
    pub fn parse(line: &str) -> Result<Self, String> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Err("empty command".to_string());
        }

        // Split into at most 3 parts so that `SET key hello world` keeps
        // the value as "hello world" rather than discarding "world".
        let parts: Vec<&str> = trimmed.splitn(3, char::is_whitespace).collect();
        let verb = parts[0].to_uppercase();

        match verb.as_str() {
            "GET" => {
                if parts.len() < 2 {
                    return Err("usage: GET <key>".to_string());
                }
                Ok(Command::Get {
                    key: parts[1].to_string(),
                })
            }
            "SET" => {
                if parts.len() < 3 {
                    return Err("usage: SET <key> <value>".to_string());
                }
                Ok(Command::Set {
                    key: parts[1].to_string(),
                    value: parts[2].to_string(),
                })
            }
            "DEL" => {
                if parts.len() < 2 {
                    return Err("usage: DEL <key>".to_string());
                }
                Ok(Command::Del {
                    key: parts[1].to_string(),
                })
            }
            "EXISTS" => {
                if parts.len() < 2 {
                    return Err("usage: EXISTS <key>".to_string());
                }
                Ok(Command::Exists {
                    key: parts[1].to_string(),
                })
            }
            "KEYS" => {
                let pattern = if parts.len() >= 2 {
                    parts[1].to_string()
                } else {
                    "*".to_string()
                };
                Ok(Command::Keys { pattern })
            }
            "DBSIZE" => Ok(Command::Dbsize),
            "PING" => {
                let msg = if parts.len() >= 2 {
                    Some(parts[1..].join(" "))
                } else {
                    None
                };
                Ok(Command::Ping { msg })
            }
            "FLUSHDB" => Ok(Command::Flushdb),
            "SAVE" => Ok(Command::Save),
            "INFO" => Ok(Command::Info),
            _ => Err(format!("unknown command '{}'", verb)),
        }
    }
}

// ------------------------------------------------------------ glob matching

/// A minimal glob matcher: `*` matches any sequence, `?` matches one char,
/// everything else is literal. Used only for the KEYS command.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    glob_match_inner(&pat, &txt)
}

fn glob_match_inner(pat: &[char], txt: &[char]) -> bool {
    if pat.is_empty() {
        return txt.is_empty();
    }

    match pat[0] {
        '*' => {
            // Try matching zero characters, one character, two, ... up to the
            // whole remaining text. This is the simple recursive version;
            // for KEYS on a few thousand entries it is more than fast enough.
            for skip in 0..=txt.len() {
                if glob_match_inner(&pat[1..], &txt[skip..]) {
                    return true;
                }
            }
            false
        }
        '?' => {
            !txt.is_empty() && glob_match_inner(&pat[1..], &txt[1..])
        }
        ch => {
            !txt.is_empty() && txt[0] == ch && glob_match_inner(&pat[1..], &txt[1..])
        }
    }
}
