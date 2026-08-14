//! The supervisor-to-child channel, which is a different object per platform.
//!
//! A Unix socket on Unix and a named pipe on Windows. Not TCP on either: a
//! loopback port can be connected to by any process on the machine, and this
//! channel carries task payloads and is trusted to say a job is done. The two
//! chosen here are protected by filesystem permissions and by an ACL
//! respectively, which is the property that matters.

#[cfg(unix)]
pub use unix::{connect_path, Listener, Reader, Writer};
#[cfg(windows)]
pub use windows::{connect_path, Listener, Reader, Writer};

#[cfg(unix)]
mod unix {
    use std::io;
    use tokio::net::{unix::OwnedReadHalf, unix::OwnedWriteHalf, UnixListener};

    pub type Reader = OwnedReadHalf;
    pub type Writer = OwnedWriteHalf;

    pub struct Listener(UnixListener);

    impl Listener {
        /// Bind, and make the socket unreachable by other accounts.
        pub fn bind(path: &str) -> io::Result<Self> {
            let listener = UnixListener::bind(path)?;
            // Belt and braces behind the 0700 directory: Linux enforces socket
            // permissions, and the platforms that do not are covered by not
            // being able to traverse to it.
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            Ok(Listener(listener))
        }

        pub async fn accept(&self) -> io::Result<(Reader, Writer)> {
            let (stream, _) = self.0.accept().await?;
            Ok(stream.into_split())
        }
    }

    /// What a child is told to connect to.
    pub fn connect_path(dir: &std::path::Path) -> String {
        dir.join("sock").to_string_lossy().into_owned()
    }
}

#[cfg(windows)]
mod windows {
    use std::io;
    use tokio::io::{ReadHalf, WriteHalf};
    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};

    pub type Reader = ReadHalf<NamedPipeServer>;
    pub type Writer = WriteHalf<NamedPipeServer>;

    /// A named pipe server is one instance per connection, so the listener
    /// holds the next idle instance and creates its successor on each accept —
    /// the pattern the Win32 API expects, and the reason this is not just a
    /// thin wrapper the way the Unix side is.
    pub struct Listener {
        name: String,
        next: std::sync::Mutex<Option<NamedPipeServer>>,
    }

    impl Listener {
        pub fn bind(name: &str) -> io::Result<Self> {
            let first = ServerOptions::new()
                .first_pipe_instance(true)
                .create(name)?;
            Ok(Listener {
                name: name.to_string(),
                next: std::sync::Mutex::new(Some(first)),
            })
        }

        pub async fn accept(&self) -> io::Result<(Reader, Writer)> {
            let server = self
                .next
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| io::Error::other("listener has no idle instance"))?;
            server.connect().await?;
            *self.next.lock().unwrap() = Some(ServerOptions::new().create(&self.name)?);
            Ok(tokio::io::split(server))
        }
    }

    /// Named pipes are not filesystem paths; the directory only supplies a name
    /// unique to this supervisor.
    pub fn connect_path(dir: &std::path::Path) -> String {
        let tag = dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "tarsk".into());
        format!(r"\\.\pipe\{tag}")
    }
}

/// Resident memory of one process, in bytes.
///
/// sysinfo does this everywhere except Windows, where it reported the same
/// 4.1MB for every process regardless of what they held — measured against a
/// child allocating 300MB in fifty-megabyte steps, which it followed exactly
/// nowhere. The ceiling is only as good as this number, so on that platform it
/// is read from the API sysinfo was aiming at, declared here rather than
/// pulled in as a dependency for three functions.
#[cfg(windows)]
pub fn child_rss(pid: u32) -> Option<u64> {
    // PROCESS_MEMORY_COUNTERS: two DWORDs then eight SIZE_Ts, which on x86-64
    // means the pair of u32s share the first eight bytes.
    #[repr(C)]
    #[derive(Default)]
    struct Counters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set: usize,
        working_set: usize,
        quota_peak_paged: usize,
        quota_paged: usize,
        quota_peak_non_paged: usize,
        quota_non_paged: usize,
        pagefile: usize,
        peak_pagefile: usize,
    }

    // K32GetProcessMemoryInfo lives in kernel32, which is linked already;
    // GetProcessMemoryInfo is the same call forwarded through psapi.dll and
    // would need a second library on the link line.
    unsafe extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> isize;
        fn K32GetProcessMemoryInfo(process: isize, counters: *mut Counters, cb: u32) -> i32;
        fn CloseHandle(handle: isize) -> i32;
    }

    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle == 0 {
            return None;
        }
        let mut counters = Counters {
            cb: std::mem::size_of::<Counters>() as u32,
            ..Default::default()
        };
        let ok = K32GetProcessMemoryInfo(
            handle,
            &mut counters,
            std::mem::size_of::<Counters>() as u32,
        );
        CloseHandle(handle);
        (ok != 0).then_some(counters.working_set as u64)
    }
}

/// Everywhere else sysinfo is right, and reusing the caller's `System` keeps
/// the process table warm between polls.
#[cfg(unix)]
pub fn child_rss_with(sys: &mut sysinfo::System, pid: u32) -> Option<u64> {
    let key = sysinfo::Pid::from_u32(pid);
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[key]), false);
    sys.process(key).map(|p| p.memory())
}

#[cfg(windows)]
pub fn child_rss_with(_sys: &mut sysinfo::System, pid: u32) -> Option<u64> {
    child_rss(pid)
}
