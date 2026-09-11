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

/// Resident memory of a worker, in bytes.
///
/// On Unix that is one process. On Windows a venv's `python.exe` is a launcher
/// that starts the real interpreter as a child and waits for it, so the pid the
/// supervisor holds belongs to a four-megabyte stub while every allocation
/// happens in a process it never looks at. Windows therefore sums the worker
/// and its descendants. Double-counting shared pages errs towards recycling
/// early, which is the safe direction for a limit. Unix keeps the single cheap
/// read: nothing there stands between the spawn and the interpreter.
#[cfg(unix)]
pub fn child_rss_with(sys: &mut sysinfo::System, pid: u32) -> Option<u64> {
    let key = sysinfo::Pid::from_u32(pid);
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[key]), false);
    sys.process(key).map(|p| p.memory())
}

/// The working set of one process, read from the API rather than through
/// sysinfo.
///
/// sysinfo opens each process with PROCESS_QUERY_INFORMATION | PROCESS_VM_READ
/// and reports nothing for the ones it is refused. That refusal is what the
/// probe has been printing as `0.0 MB` for a child holding three hundred
/// megabytes: the runner image changed under us between a green run on
/// 20260810.198 and a red one on 20260907.229, with this crate, sysinfo 0.38.4
/// and the workflow all byte-identical across the pair.
///
/// PROCESS_QUERY_LIMITED_INFORMATION is the least privilege that still answers
/// a memory question, and it is granted where the wider pair is denied. This
/// call was already here once — it was measured against Get-Process on Windows
/// and agreed with it — and was dropped on the theory that the wrong pid, not
/// the reading, was the whole story. The pid was half of it.
#[cfg(windows)]
fn process_rss(pid: u32) -> Option<u64> {
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

#[cfg(windows)]
pub fn child_rss_with(sys: &mut sysinfo::System, pid: u32) -> Option<u64> {
    use std::collections::HashSet;

    let root = sysinfo::Pid::from_u32(pid);
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
    let mut family: HashSet<sysinfo::Pid> = HashSet::from([root]);
    // Parents come before children in no particular order, so walk until the
    // set stops growing rather than assuming one generation.
    loop {
        let before = family.len();
        for (child, proc) in sys.processes() {
            if proc.parent().is_some_and(|p| family.contains(&p)) {
                family.insert(*child);
            }
        }
        if family.len() == before {
            break;
        }
    }
    // sysinfo is used to find the family and nothing else; every byte comes
    // from process_rss. The root is read whether or not sysinfo listed it, so
    // a process table we were refused cannot turn a live child into a zero —
    // the old `sys.process(root)?` guard did exactly that.
    let mut total = 0;
    let mut read_any = false;
    for member in &family {
        if let Some(bytes) = process_rss(member.as_u32()) {
            total += bytes;
            read_any = true;
        }
    }
    read_any.then_some(total)
}
