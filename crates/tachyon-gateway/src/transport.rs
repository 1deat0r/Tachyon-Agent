//! Local IPC transport: Unix domain sockets on Unix, named pipes on
//! Windows (spec §36). Both present the same framed byte stream, so the
//! gateway, client, and protocol never branch on platform.

use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[cfg(windows)]
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

/// Windows pipe name prefix.
#[cfg(windows)]
const PIPE_PREFIX: &str = r"\\.\pipe\";

/// Derives a stable Windows named-pipe address for a runtime directory,
/// so parallel gateways on different data dirs do not share a pipe.
#[cfg(windows)]
#[must_use]
pub fn pipe_name_for(data_dir: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{PIPE_PREFIX}tachyon-{:016x}",
        fnv1a64(data_dir.as_os_str().as_encoded_bytes())
    ))
}

/// 64-bit FNV-1a: short stable hashes without a new dependency.
#[cfg(windows)]
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// Bound local endpoint. Construct with [`Listener::bind`], serve with
/// [`Listener::accept`].
pub struct Listener {
    #[cfg(unix)]
    inner: UnixListener,
    #[cfg(windows)]
    pipe_name: PathBuf,
    /// Next pipe instance, created eagerly so clients can always connect.
    #[cfg(windows)]
    pending: std::sync::Mutex<Option<NamedPipeServer>>,
}

impl Listener {
    /// Binds the local endpoint for `socket_path` (`data_dir/gateway.sock`
    /// on Unix; the parent data dir selects the pipe name on Windows).
    /// The first pipe instance is created here so connects never race
    /// the accept loop.
    pub fn bind(socket_path: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                inner: UnixListener::bind(socket_path)?,
            })
        }
        #[cfg(windows)]
        {
            let dir = socket_path.parent().unwrap_or(socket_path);
            let pipe_name = pipe_name_for(dir);
            let first = ServerOptions::new()
                .first_pipe_instance(true)
                .create(&pipe_name)?;
            Ok(Self {
                pipe_name,
                pending: std::sync::Mutex::new(Some(first)),
            })
        }
    }

    /// Address clients connect to: the socket path on Unix, the pipe
    /// name on Windows. Recorded in the endpoint file.
    #[must_use]
    pub fn local_address(&self) -> PathBuf {
        #[cfg(unix)]
        {
            self.inner
                .local_addr()
                .ok()
                .and_then(|addr| addr.as_pathname().map(Path::to_owned))
                .unwrap_or_else(|| PathBuf::from("/unknown.sock"))
        }
        #[cfg(windows)]
        {
            self.pipe_name.clone()
        }
    }

    /// Accepts one connection. On Windows each accept creates a fresh
    /// pipe instance and waits for its client.
    pub async fn accept(&self) -> io::Result<Stream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.inner.accept().await?;
            Ok(Stream {
                inner: StreamKind::Unix(stream),
            })
        }
        #[cfg(windows)]
        {
            // Hand off the waiting instance and immediately stage its
            // replacement, so a listener always exists for the next client.
            let server = {
                let mut pending = self
                    .pending
                    .lock()
                    .map_err(|_| io::Error::other("pipe handoff lock poisoned"))?;
                let server = match pending.take() {
                    Some(server) => server,
                    None => ServerOptions::new().create(&self.pipe_name)?,
                };
                *pending = Some(ServerOptions::new().create(&self.pipe_name)?);
                server
            };
            server.connect().await?;
            Ok(Stream {
                inner: StreamKind::Server(server),
            })
        }
    }
}

/// One connected byte stream, either transport.
pub struct Stream {
    inner: StreamKind,
}

enum StreamKind {
    #[cfg(unix)]
    Unix(UnixStream),
    /// Accepted server side of a named pipe.
    #[cfg(windows)]
    Server(NamedPipeServer),
    /// Connected client side of a named pipe.
    #[cfg(windows)]
    Client(NamedPipeClient),
}

/// Connects to `address` (socket path or pipe name).
///
/// On Windows, `Listener::bind` stages exactly one pipe instance and the
/// replacement is created inside `Listener::accept`. Between a client
/// consuming that instance and the accept loop staging its replacement,
/// every other connect fails with `ERROR_PIPE_BUSY` (231) — including the
/// whole gateway recovery window before the accept loop is spawned (#35).
/// The connect waits that window out with a bounded retry (~1 s, 25 ms
/// steps) and only surfaces the last busy error if the window never
/// closes; any other error returns immediately.
pub async fn connect(address: &Path) -> io::Result<Stream> {
    #[cfg(unix)]
    {
        let stream = UnixStream::connect(address).await?;
        Ok(Stream {
            inner: StreamKind::Unix(stream),
        })
    }
    #[cfg(windows)]
    {
        // Win32 ERROR_PIPE_BUSY: all pipe instances are busy.
        const ERROR_PIPE_BUSY: i32 = 231;
        const MAX_ATTEMPTS: usize = 40;
        const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);
        let mut last_busy: Option<io::Error> = None;
        for _ in 0..MAX_ATTEMPTS {
            match ClientOptions::new().open(address) {
                Ok(client) => {
                    return Ok(Stream {
                        inner: StreamKind::Client(client),
                    });
                }
                Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    last_busy = Some(err);
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_busy.expect("at least one busy connect attempt"))
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            #[cfg(unix)]
            StreamKind::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(windows)]
            StreamKind::Server(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(windows)]
            StreamKind::Client(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            #[cfg(unix)]
            StreamKind::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(windows)]
            StreamKind::Server(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(windows)]
            StreamKind::Client(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            #[cfg(unix)]
            StreamKind::Unix(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(windows)]
            StreamKind::Server(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(windows)]
            StreamKind::Client(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            #[cfg(unix)]
            StreamKind::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(windows)]
            StreamKind::Server(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(windows)]
            StreamKind::Client(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
