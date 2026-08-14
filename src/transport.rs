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
/// On Unix that is one process. On Windows a venv's `python.exe` can be a
/// launcher that starts the real interpreter as a child, so the pid the
/// supervisor holds belongs to a four-megabyte stub while every allocation
/// happens in a process it never looks at — which is exactly what the ceiling
/// saw there: 4.1MB, unchanged, while the worker held three hundred.
///
/// Windows therefore sums the worker and its descendants. Double-counting
/// shared pages errs towards recycling early, which is the safe direction for
/// a limit. Unix keeps the single cheap read: nothing there stands between the
/// spawn and the interpreter.
#[cfg(unix)]
pub fn child_rss_with(sys: &mut sysinfo::System, pid: u32) -> Option<u64> {
    let key = sysinfo::Pid::from_u32(pid);
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[key]), false);
    sys.process(key).map(|p| p.memory())
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
    sys.process(root)?;
    Some(
        family
            .iter()
            .filter_map(|p| sys.process(*p))
            .map(|p| p.memory())
            .sum(),
    )
}
