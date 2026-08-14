//! The admin console, served by the supervisor that is already running.
//!
//! Every queue people take seriously ships one of these — Flower, Sidekiq Web,
//! asynq-mon, Bull Board — and the reason is not the charts. Grafana draws
//! better charts. The reason is that when something is stuck you want to read
//! its traceback and put it back, and a graph cannot do either.
//!
//! It lives here rather than in a Python web app because the pieces were all
//! here already: an HTTP listener for `/metrics`, the broker connection, and
//! every query the console needs, each with tests against Redis and Postgres.
//! Adding routes to that is smaller than introducing a web framework to a
//! project whose only dependency is msgpack.
//!
//! What it is not: `/metrics` serves numbers, and numbers are not worth
//! protecting. This serves task payloads and can cancel and replay work. The
//! defaults are set accordingly — see `guard`.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::Shared;

/// Longest request we will read. A browser sends two to four kilobytes once it
/// has cookies; the old `/metrics` loop read 1024 and discarded it, which was
/// fine when nothing was parsed and would have truncated a header mid-name the
/// moment anything was.
const MAX_REQUEST: usize = 16 * 1024;

/// How many rows any one table shows. A console that renders ten thousand jobs
/// is a console nobody can read and a supervisor doing work for nobody.
const PAGE: usize = 100;

struct Request {
    method: String,
    path: String,
    authorization: String,
    body: String,
}

/// What the environment permits. Read once at startup so a request never pays
/// for it, and so a refusal to start is visible immediately rather than on the
/// first request from someone who should not have reached it.
struct Guard {
    /// `Basic <base64>` as the browser will send it, or empty for no auth.
    expect: String,
    actions: bool,
}

impl Guard {
    fn allows(&self, request: &Request) -> bool {
        if self.expect.is_empty() {
            return true;
        }
        constant_time_eq(
            request.authorization.trim().as_bytes(),
            self.expect.as_bytes(),
        )
    }
}

/// Compare without leaking where two strings first differ.
///
/// A timing oracle on an admin token is not the likeliest way into a queue, but
/// it is the one that costs four lines to close.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn base64(input: &[u8]) -> String {
    const SET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(SET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Decide what this address is allowed to serve, or refuse to serve it.
///
/// Binding an unauthenticated admin console to `0.0.0.0` is a mistake that
/// looks exactly like binding an unauthenticated metrics endpoint there, which
/// is ordinary. So it is refused rather than warned about: the same flag that
/// was harmless yesterday now exposes task payloads and a cancel button.
fn guard(addr: &str) -> Result<Guard, String> {
    let token = std::env::var("TARSK_ADMIN_TOKEN").unwrap_or_default();
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    let local = matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]");
    if !local && token.is_empty() {
        return Err(format!(
            "refusing to serve the console on {addr}: it shows task payloads and can cancel \
             and replay work, and there is no TARSK_ADMIN_TOKEN set. Bind to 127.0.0.1, or \
             set the token."
        ));
    }
    Ok(Guard {
        expect: if token.is_empty() {
            String::new()
        } else {
            format!("Basic {}", base64(format!("tarsk:{token}").as_bytes()))
        },
        // Cancelling and replaying are destructive, so an accidentally reachable
        // console is a viewer until someone says otherwise.
        actions: std::env::var("TARSK_ADMIN_ACTIONS").is_ok_and(|v| v == "1"),
    })
}

pub(crate) async fn serve<F>(addr: String, shared: Arc<Shared>, snapshot: F)
where
    F: Fn() -> String + Send + Sync + 'static,
{
    let guard = match guard(&addr) {
        Ok(guard) => guard,
        Err(why) => {
            eprintln!("tarsk: {why}");
            return;
        }
    };
    let Ok(listener) = TcpListener::bind(&addr).await else {
        eprintln!("tarsk: could not bind {addr}");
        return;
    };
    eprintln!(
        "tarsk: console on http://{addr}/ ({}, {})",
        if guard.expect.is_empty() {
            "no auth"
        } else {
            "token required"
        },
        if guard.actions {
            "actions enabled"
        } else {
            "read only"
        },
    );
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let body = snapshot();
        let shared = shared.clone();
        let expect = guard.expect.clone();
        let actions = guard.actions;
        // One task per connection: a slow reader must not hold up the next
        // person looking at a queue, and neither may touch the supervision loop.
        tokio::spawn(async move {
            let guard = Guard { expect, actions };
            let _ = handle(stream, &shared, &guard, body).await;
        });
    }
}

async fn handle(
    mut stream: TcpStream,
    shared: &Arc<Shared>,
    guard: &Guard,
    metrics_body: String,
) -> std::io::Result<()> {
    let Some(request) = read_request(&mut stream).await else {
        return respond(&mut stream, 400, "text/plain", "bad request").await;
    };

    // Prometheus scrapes are exempt: they were unauthenticated before this
    // existed, they carry no payloads, and breaking every scrape config to add
    // a console would be a poor trade.
    if request.path == "/metrics" {
        return respond(&mut stream, 200, "text/plain; version=0.0.4", &metrics_body).await;
    }

    if !guard.allows(&request) {
        let mut head = String::from("HTTP/1.1 401 Unauthorized\r\n");
        head.push_str("WWW-Authenticate: Basic realm=\"tarsk\"\r\n");
        head.push_str("Content-Length: 0\r\nConnection: close\r\n\r\n");
        stream.write_all(head.as_bytes()).await?;
        return stream.shutdown().await;
    }

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => {
            let page = dashboard(shared, guard).await;
            respond(&mut stream, 200, "text/html; charset=utf-8", &page).await
        }
        ("POST", path) if guard.actions => {
            let id = form_value(&request.body, "id");
            let queue = form_value(&request.body, "queue");
            let queue = if queue.is_empty() {
                "default".to_string()
            } else {
                queue
            };
            match path {
                "/cancel" => {
                    let _ = shared.broker.revoke(&queue, &id, 86_400_000).await;
                }
                "/replay" => {
                    let ids = if id.is_empty() {
                        Vec::new()
                    } else {
                        vec![id.clone()]
                    };
                    let _ = shared.broker.dead_replay(&queue, &ids).await;
                }
                "/purge" => {
                    let ids = if id.is_empty() {
                        Vec::new()
                    } else {
                        vec![id.clone()]
                    };
                    let _ = shared.broker.dead_purge(&queue, &ids).await;
                }
                _ => return respond(&mut stream, 404, "text/plain", "no such action").await,
            }
            // See-other rather than a rendered page, so a refresh does not
            // replay the action the reader already took.
            let head = "HTTP/1.1 303 See Other\r\nLocation: /\r\n\
                        Content-Length: 0\r\nConnection: close\r\n\r\n";
            stream.write_all(head.as_bytes()).await?;
            stream.shutdown().await
        }
        ("POST", _) => {
            respond(
                &mut stream,
                403,
                "text/plain",
                "read only: set TARSK_ADMIN_ACTIONS=1 to allow cancel, replay and purge",
            )
            .await
        }
        _ => respond(&mut stream, 404, "text/plain", "not found").await,
    }
}

/// Read until the headers end, then whatever body the length promises.
async fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut raw = Vec::with_capacity(2048);
    let mut chunk = [0u8; 2048];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if raw.windows(4).any(|w| w == b"\r\n\r\n") || raw.len() >= MAX_REQUEST {
            break;
        }
        let read = tokio::time::timeout_at(deadline, stream.read(&mut chunk))
            .await
            .ok()?
            .ok()?;
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&chunk[..read]);
    }
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, rest) = text.split_once("\r\n\r\n")?;
    let mut lines = head.lines();
    let mut start = lines.next()?.split_whitespace();
    let method = start.next()?.to_string();
    let target = start.next()?.to_string();
    // Query strings are dropped: every action here is a POST with a form
    // body, and a parameter nobody reads is a parameter someone will assume
    // is honoured.
    let path = target.split('?').next().unwrap_or(&target).to_string();
    let authorization = lines
        .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_default();
    // Bodies here are one short form; anything already read is all of it.
    Some(Request {
        method,
        path,
        authorization,
        body: rest.to_string(),
    })
}

fn form_value(body: &str, key: &str) -> String {
    body.split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| percent_decode(v))
        .unwrap_or_default()
}

fn percent_decode(input: &str) -> String {
    let bytes = input.replace('+', " ").into_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Everything rendered into the page goes through this.
///
/// Task names, job ids and tracebacks all come from outside this process, and a
/// console that shows them is a console that will be handed a `<script>` tag
/// eventually.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn span(ms: i64) -> String {
    let seconds = ms.max(0) as f64 / 1000.0;
    if seconds < 90.0 {
        format!("{seconds:.0}s")
    } else if seconds < 5400.0 {
        format!("{:.0}m", seconds / 60.0)
    } else {
        format!("{:.1}h", seconds / 3600.0)
    }
}

async fn dashboard(shared: &Arc<Shared>, guard: &Guard) -> String {
    let queues = shared.broker.queue_names();
    let depth = shared.broker.depth().await.unwrap_or_default();
    let jobs = shared.broker.jobs(&queues, PAGE).await.unwrap_or_default();

    let mut page = String::with_capacity(8192);
    page.push_str(HEAD);
    page.push_str("<h1>tarsk</h1>");
    if !guard.actions {
        page.push_str(
            "<p class=note>Read only. Set <code>TARSK_ADMIN_ACTIONS=1</code> on the worker \
             to allow cancel, replay and purge.</p>",
        );
    }

    page.push_str("<h2>Queues</h2><table><tr><th>queue<th>ready<th>running<th>delayed<th>dead");
    for row in &depth {
        page.push_str(&format!(
            "<tr><td>{}<td class=n>{}<td class=n>{}<td class=n>{}<td class=n>{}",
            escape(&row.queue),
            row.ready,
            row.in_flight,
            row.delayed,
            row.dead
        ));
    }
    page.push_str("</table>");

    page.push_str("<h2>Jobs</h2>");
    if jobs.is_empty() {
        page.push_str("<p class=note>Nothing waiting or running.</p>");
    } else {
        page.push_str("<table><tr><th>id<th>state<th>task<th>age<th>worker<th>");
        for job in &jobs {
            let when = if job.state == "delayed" {
                format!("due in {}", span(-job.age_ms))
            } else {
                format!("{} ago", span(job.age_ms))
            };
            page.push_str(&format!(
                "<tr><td class=id>{}<td><span class=\"s {}\">{}</span><td>{}<td>{}<td>{}<td>{}",
                escape(&job.id),
                job.state,
                job.state,
                escape(&job.name),
                when,
                escape(&job.worker),
                action(guard, "/cancel", &job.queue, &job.id, "cancel"),
            ));
        }
        page.push_str("</table>");
    }

    page.push_str("<h2>Dead letters</h2>");
    let mut any_dead = false;
    for queue in &queues {
        let dead = shared
            .broker
            .dead_list(queue, PAGE)
            .await
            .unwrap_or_default();
        if dead.is_empty() {
            continue;
        }
        any_dead = true;
        page.push_str(&format!(
            "<table><tr><th>id<th>task<th>error<th>{}",
            action(guard, "/purge", queue, "", "purge all")
        ));
        for entry in dead {
            page.push_str(&format!(
                "<tr><td class=id>{}<td>{}<td class=err>{}<td>{}",
                escape(&entry.id),
                escape(&entry.name),
                escape(entry.error.lines().next().unwrap_or("")),
                action(guard, "/replay", queue, &entry.id, "replay"),
            ));
            page.push_str(&format!(
                "<tr class=tb><td colspan=4><pre>{}</pre>",
                escape(entry.traceback.trim())
            ));
        }
        page.push_str("</table>");
    }
    if !any_dead {
        page.push_str("<p class=note>None.</p>");
    }

    page.push_str("</body>");
    page
}

fn action(guard: &Guard, path: &str, queue: &str, id: &str, label: &str) -> String {
    if !guard.actions {
        return String::new();
    }
    format!(
        "<form method=post action={path}>\
         <input type=hidden name=queue value=\"{}\">\
         <input type=hidden name=id value=\"{}\">\
         <button>{label}</button></form>",
        escape(queue),
        escape(id)
    )
}

/// Inline, because a second request for a stylesheet is a second route to
/// serve and a second thing to get wrong.
const HEAD: &str = "<!doctype html><meta charset=utf-8><title>tarsk</title>\
<meta http-equiv=refresh content=5>\
<style>\
:root{color-scheme:light dark}\
body{font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;margin:2rem auto;max-width:70rem;padding:0 1rem}\
h1{font-size:1.1rem;letter-spacing:.08em;text-transform:uppercase;opacity:.6}\
h2{font-size:.95rem;margin-top:2rem}\
table{border-collapse:collapse;width:100%}\
th{text-align:left;font-weight:600;opacity:.55;padding:.3rem .6rem .3rem 0;border-bottom:1px solid}\
td{padding:.3rem .6rem .3rem 0;border-bottom:1px solid;border-color:color-mix(in srgb,currentColor 12%,transparent)}\
td.n{text-align:right;font-variant-numeric:tabular-nums}\
td.id{opacity:.55}\
td.err{color:#b3261e}\
.s{padding:0 .4rem;border-radius:.2rem;font-size:.85em}\
.running{background:#1a73e820;color:#1a73e8}\
.ready{background:#8888881f}\
.delayed{background:#f9ab0020;color:#a06800}\
.note{opacity:.6}\
tr.tb pre{margin:.2rem 0 .6rem;white-space:pre-wrap;font-size:.85em;opacity:.7;overflow-x:auto}\
button{font:inherit;padding:.1rem .5rem;cursor:pointer}\
form{display:inline}\
</style><body>";

async fn respond(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_reference_vectors() {
        // RFC 4648 §10, so the padding cases are not guesswork.
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foob"), "Zm9vYg==");
        assert_eq!(base64(b"tarsk:hunter2"), "dGFyc2s6aHVudGVyMg==");
    }

    #[test]
    fn markup_from_outside_is_escaped() {
        let out = escape("<script>alert(1)</script>");
        assert!(!out.contains('<'), "{out}");
        assert_eq!(escape("a&b"), "a&amp;b");
    }

    #[test]
    fn a_form_body_survives_encoding() {
        assert_eq!(form_value("id=abc%3A1&queue=q", "id"), "abc:1");
        assert_eq!(form_value("id=abc&queue=two+words", "queue"), "two words");
        assert_eq!(form_value("id=abc", "missing"), "");
    }

    #[test]
    fn comparison_is_length_safe() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
