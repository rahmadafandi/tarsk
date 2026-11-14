//! What the supervisor says, one line an event, on stderr.
//!
//! Plain by default: `tarsk: …`, which is what a person at a terminal wants.
//! `TARSK_LOG_FORMAT=json` turns each line into one JSON object instead, for
//! the log pipelines that cannot do anything useful with prose — a level to
//! filter on, a timestamp that does not depend on when the collector read it.
//!
//! No logging crate. There are a dozen call sites and two formats; a facade
//! and a backend would be more code than this file, and one more thing for a
//! supervisor that promises a constant footprint to carry.

use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

fn json() -> bool {
    static JSON: OnceLock<bool> = OnceLock::new();
    *JSON.get_or_init(|| std::env::var("TARSK_LOG_FORMAT").is_ok_and(|v| v == "json"))
}

/// A string as JSON wants it. Tracebacks and broker messages go through here,
/// and both are full of quotes and newlines.
fn quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn render(as_json: bool, level: &str, message: &str, unix_seconds: f64) -> String {
    if as_json {
        format!(
            "{{\"ts\":{unix_seconds:.3},\"level\":{},\"logger\":\"tarsk\",\"message\":{}}}",
            quoted(level),
            quoted(message)
        )
    } else {
        format!("tarsk: {message}")
    }
}

pub fn line(level: &str, message: &str) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    eprintln!("{}", render(json(), level, message, now));
}

/// Something worth knowing that is not a problem.
macro_rules! note {
    ($($arg:tt)*) => { $crate::log::line("info", &format!($($arg)*)) };
}
/// Something that is.
macro_rules! alert {
    ($($arg:tt)*) => { $crate::log::line("warning", &format!($($arg)*)) };
}
pub(crate) use {alert, note};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_lines_read_as_they_always_did() {
        assert_eq!(
            render(false, "warning", "could not bind 0.0.0.0:9090", 1.5),
            "tarsk: could not bind 0.0.0.0:9090"
        );
    }

    #[test]
    fn json_lines_survive_what_brokers_and_tracebacks_put_in_them() {
        let line = render(
            true,
            "warning",
            "db error: \"x\"\n\tat line 1 \\ \u{1}",
            1794000000.25,
        );
        assert_eq!(
            line,
            r#"{"ts":1794000000.250,"level":"warning","logger":"tarsk","message":"db error: \"x\"\n\tat line 1 \\ \u0001"}"#
        );
        // One object, one line: a newline inside it would be two log records.
        assert!(!line.contains('\n'));
    }
}
