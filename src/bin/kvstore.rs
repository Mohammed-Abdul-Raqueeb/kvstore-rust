use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use kvstore::server::Server;
use kvstore::store::{Store, StoreConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let port: u16 = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(6380);

    let data_dir = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "data".to_string());

    let config = StoreConfig::new(&data_dir);
    let store = Store::open(config).unwrap_or_else(|e| {
        eprintln!("failed to open store at {:?}: {}", data_dir, e);
        std::process::exit(1);
    });

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("failed to bind {}: {}", addr, e);
        std::process::exit(1);
    });

    let actual_port = listener.local_addr().unwrap().port();
    println!("listening on 127.0.0.1:{}", actual_port);

    // Shutdown flag, shared with the signal handler.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);

    // SIGINT / SIGTERM: set the flag. The server checks it every poll timeout.
    unsafe {
        libc_signal(2, move || stop_clone.store(true, Ordering::Relaxed));
    }

    let mut server = Server::new(listener, store);
    server.run(&stop);
}

/// Minimal signal handler registration. We cannot use ctrlc or signal-hook
/// crates, so this is the raw libc approach.
unsafe fn libc_signal(signum: i32, handler: impl Fn() + Send + 'static) {
    // Box the closure and leak it so its pointer is valid for the process
    // lifetime. One allocation per signal, never freed — acceptable.
    let boxed: Box<Box<dyn Fn() + Send>> = Box::new(Box::new(handler));
    let raw = Box::into_raw(boxed);

    // Store the pointer in a global so the C callback can find it.
    match signum {
        2 => SIGINT_HANDLER = raw as *mut _,
        _ => {}
    }

    extern "C" fn trampoline(_: i32) {
        unsafe {
            if !SIGINT_HANDLER.is_null() {
                let handler = &*(SIGINT_HANDLER as *const Box<dyn Fn() + Send>);
                handler();
            }
        }
    }

    // Register with the OS.
    extern "C" {
        fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
    }
    signal(signum, trampoline);
}

static mut SIGINT_HANDLER: *mut () = std::ptr::null_mut();
