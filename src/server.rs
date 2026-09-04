//! server.rs — the TCP listener and per-client buffered I/O.
//!
//! Like the C chat server, this is single-threaded and event-driven: one
//! thread, no locks, `poll()` to multiplex all connections. The store is
//! borrowed mutably by whoever is handling the current event, which Rust's
//! borrow checker guarantees is exactly one place at a time — a data-race
//! that would require a mutex in C is simply impossible to write here.

use std::collections::HashMap;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::os::unix::io::AsRawFd;

use crate::store::{Command, Reply, Store};

const MAX_LINE: usize = 65536;  // max bytes before a newline
const MAX_CLIENTS: usize = 512;

/// Per-connection state.
struct Client {
    stream: TcpStream,
    addr: SocketAddr,
    buf: Vec<u8>,
}

impl Client {
    fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
            buf: Vec::with_capacity(256),
        }
    }
}

pub struct Server {
    listener: TcpListener,
    store: Store,
    clients: HashMap<i32, Client>,  // fd -> Client
}

impl Server {
    pub fn new(listener: TcpListener, store: Store) -> Self {
        Self {
            listener,
            store,
            clients: HashMap::new(),
        }
    }

    pub fn port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }

    /// Runs until a shutdown flag (set by a signal handler) fires.
    pub fn run(&mut self, stop: &std::sync::atomic::AtomicBool) {
        self.listener.set_nonblocking(true).unwrap();

        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            // Build the pollfd array: listener + every client.
            let mut fds = Vec::with_capacity(self.clients.len() + 1);

            fds.push(libc_pollfd(self.listener.as_raw_fd(), libc::POLLIN));

            let client_fds: Vec<i32> = self.clients.keys().cloned().collect();
            for &fd in &client_fds {
                fds.push(libc_pollfd(fd, libc::POLLIN));
            }

            // poll with a 500ms timeout so we re-check the stop flag promptly.
            let ready = unsafe {
                libc::poll(
                    fds.as_mut_ptr() as *mut PollFd,
                    fds.len() as libc::NfdsT,
                    500,
                )
            };

            if ready < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == ErrorKind::Interrupted {
                    continue;
                }
                eprintln!("[server] poll error: {}", err);
                break;
            }

            // Check the listener.
            if fds[0].revents & libc::POLLIN != 0 {
                self.accept_all();
            }

            // Check each client.
            for pfd in &fds[1..] {
                if pfd.revents == 0 {
                    continue;
                }
                let fd = pfd.fd;

                if pfd.revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
                    self.disconnect(fd);
                    continue;
                }

                if pfd.revents & (libc::POLLIN | libc::POLLHUP) != 0 {
                    if self.handle_readable(fd).is_err() {
                        self.disconnect(fd);
                    }
                }
            }
        }

        eprintln!(
            "[server] shutting down ({} client(s), {} key(s))",
            self.clients.len(),
            self.store.len()
        );
    }

    fn accept_all(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    if self.clients.len() >= MAX_CLIENTS {
                        let _ = write_line(&stream, "-ERR server full\r\n");
                        drop(stream);
                        continue;
                    }

                    stream.set_nonblocking(true).unwrap_or(());

                    let fd = stream.as_raw_fd();
                    eprintln!("[server] {} connected (fd={})", addr, fd);
                    self.clients.insert(fd, Client::new(stream, addr));
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("[server] accept error: {}", e);
                    break;
                }
            }
        }
    }

    fn handle_readable(&mut self, fd: i32) -> io::Result<()> {
        // Phase 1: read bytes off the socket.
        // We borrow the client mutably here, but release it before touching
        // self.store. This is the pattern Rust forces: you cannot hold a
        // mutable reference into self.clients while also calling &mut self
        // methods. The borrow checker catches the data race at compile time.
        {
            let client = match self.clients.get_mut(&fd) {
                Some(c) => c,
                None => return Err(io::Error::new(ErrorKind::NotFound, "unknown fd")),
            };

            let mut tmp = [0u8; 4096];
            loop {
                match client.stream.read(&mut tmp) {
                    Ok(0) => return Err(io::Error::new(ErrorKind::ConnectionAborted, "eof")),
                    Ok(n) => client.buf.extend_from_slice(&tmp[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }

            if client.buf.len() > MAX_LINE && !client.buf.contains(&b'\n') {
                let _ = write_line(&client.stream, "-ERR line too long\r\n");
                return Err(io::Error::new(ErrorKind::InvalidData, "line too long"));
            }
        }
        // The mutable borrow of `client` ends here.

        // Phase 2: extract lines, execute against the store, reply.
        //
        // We pull complete lines out of the buffer into a Vec first, then
        // drop the borrow of self.clients, then execute each against
        // self.store, and finally write the reply. Each step touches a
        // different part of self, which is what satisfies the borrow checker.
        loop {
            let line = {
                let client = match self.clients.get_mut(&fd) {
                    Some(c) => c,
                    None => return Ok(()),
                };
                match client.buf.iter().position(|&b| b == b'\n') {
                    Some(pos) => {
                        let raw: Vec<u8> = client.buf.drain(..=pos).collect();
                        String::from_utf8_lossy(&raw).trim().to_string()
                    }
                    None => break,
                }
            };

            if line.is_empty() {
                continue;
            }

            let reply = match Command::parse(&line) {
                Ok(cmd) => self.store.execute(&cmd),
                Err(msg) => Reply::Error(msg),
            };

            if let Some(c) = self.clients.get(&fd) {
                let _ = write_line(&c.stream, &reply.encode());
            }
        }

        Ok(())
    }

    fn disconnect(&mut self, fd: i32) {
        if let Some(client) = self.clients.remove(&fd) {
            eprintln!("[server] {} disconnected (fd={})", client.addr, fd);
        }
    }
}

fn write_line(stream: &TcpStream, data: &str) -> io::Result<()> {
    // Use a temporary mutable reference via the Write impl for &TcpStream,
    // which does not require &mut TcpStream — only a shared reference.
    // This is safe because the OS serialises concurrent writes to the same fd.
    (&*stream).write_all(data.as_bytes())?;
    (&*stream).flush()
}

// ------------------------------------------------------------- libc glue

/// We use raw poll() because Rust's stdlib doesn't expose it. The alternative
/// is mio, which we can't use (crates.io is not available). This is a thin
/// wrapper that keeps the unsafe surface minimal.
#[repr(C)]
#[derive(Clone, Copy)]
struct PollFd {
    fd: i32,
    events: i16,
    revents: i16,
}

fn libc_pollfd(fd: i32, events: i16) -> PollFd {
    PollFd { fd, events, revents: 0 }
}

mod libc {
    pub const POLLIN: i16 = 0x0001;
    pub const POLLHUP: i16 = 0x0010;
    pub const POLLERR: i16 = 0x0008;
    pub const POLLNVAL: i16 = 0x0020;

    pub type NfdsT = u64;

    extern "C" {
        pub fn poll(fds: *mut super::PollFd, nfds: NfdsT, timeout: i32) -> i32;
    }
}
