# Benchmarks

The harness lives here; the results and the write-up live on the documentation site:

**📊 [rahmadafandi.github.io/tarsk/benchmarks](https://rahmadafandi.github.io/tarsk/benchmarks)**

One copy of the numbers, so a stale table cannot survive in the other.

```bash
python bench/run.py                    # every scenario
python bench/run.py memory oom gap     # a subset
```

Linux only: RSS comes from `/proc`, and the hard-limit scenario uses `systemd-run --user
--scope`.

By default the harness starts its own `redis-server` on a free port and tears it down, so it
never touches anything you are running. To use your own instead:

```bash
BENCH_REDIS=redis://127.0.0.1:6379/10 python bench/run.py
```

Nothing here calls `FLUSHALL` — which empties every database in an instance no matter which one
is selected, measured: a key in db 0 does not survive a `FLUSHALL` issued from db 10. The
harness uses `FLUSHDB`, so a database number in the URL is respected and the rest of your
instance is left alone. A throwaway server is still the better measurement, since it runs
without persistence and without other traffic.

## Before a release

```bash
gh workflow run bench.yml -f refs="<last tag> HEAD" -f scenarios=scale
```

Both are built and drained in turn on one runner, twice over, and the run fails if `HEAD` is
more than 3% slower than the tag in both rounds once each is divided by taskiq's figure from
the same round; the table is in the run's summary. About a quarter of an hour. It is not part
of CI because a shared runner is too noisy to gate every push on, and it is here at all
because 0.4.0 shipped a 4.6% slowdown nothing had measured. `bench/compare.py` reads the
report; `refs` takes any number of tags or commits, oldest first, to find where a change
came in.

The published tables come from a GitHub Actions runner via
[`.github/workflows/bench.yml`](../.github/workflows/bench.yml), which anyone with the
repository can dispatch. Prefer those over anything a laptop produces: a machine nobody else has
is not evidence, and this page has had to retract three conclusions that were properties of a
machine rather than of the software.
