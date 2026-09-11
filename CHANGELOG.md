# Changelog

## 0.2.0

### Breaking: one distribution per broker

`pip install tarsk` has shipped Redis, Postgres and AMQP since 0.1. It no longer ships
any of them. There are four distributions now, all the same Rust and the same `tarsk`
package, differing only in which broker is compiled in:

| install | brokers |
|---|---|
| `pip install tarsk` | `memory://` |
| `pip install tarsk-redis` | `memory://`, `redis://`, `rediss://` |
| `pip install tarsk-postgres` | `memory://`, `postgres://`, `postgresql://` |
| `pip install tarsk-amqp` | `memory://`, `amqp://`, `amqps://` |

**If your broker URL is not `memory://`, upgrading to 0.2.0 without changing the package
will break your workers.** They will start and then refuse the URL. The fix is one line:

```bash
pip uninstall tarsk && pip install tarsk-redis     # or -postgres, or -amqp
```

Nothing in your code changes — same `import tarsk`, same API, same CLI, same wire
format. A 0.1.3 worker and a 0.2.0 worker on the same queue understand each other.

Why: a wheel with four broker drivers in it is 3.3 MB of Rust to run one of them, and
nobody runs four brokers at once. This is the `opencv-python` / `opencv-python-headless`
arrangement, with the same consequence — **install exactly one.** All four provide
`tarsk._core`, so pip will happily let the second overwrite the first and leave you
running a binary that is not the one you asked for.

### Added

- `tarsk` refuses to import when it finds more than one `tarsk*` distribution installed,
  naming what it found and what to uninstall. Without it, the wrong binary is silent.
- `tarsk._core.backends()` lists the brokers the installed wheel actually has.
- `TARSK_REDIS_URL` and `TARSK_PG_URL` point the broker tests at a server you already
  run, as `TARSK_AMQP_URL` already did.

### Changed

- A URL for a backend this build lacks now names the package to install
  (`redis:// is not available in this build — install tarsk-redis …`) rather than a cargo
  feature to add, and lists what the build does have. It also prints only the scheme,
  not the URL, which was putting broker passwords in logs.
