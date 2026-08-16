# Examples

One directory per broker, but only one of them is a full tour — because the
task API does not change between brokers, and pretending otherwise would mean
maintaining the same five files three times.

| directory | what it is |
|---|---|
| [`redis/`](redis/) | The full tour: basics, the memory ceiling, retries and soft timeouts, chains/groups/priority/cron, async handlers and progress |
| [`postgres/`](postgres/) | What actually differs on Postgres — schema, claiming, TLS. Every file in `redis/` also runs here unchanged: `TARSK_BROKER=postgres://… python examples/redis/01_basics.py` (and the same URL on the worker) |
| [`memory/`](memory/) | The in-process batch broker: no server, no separate worker, one command. Different code on purpose — `Supervisor.run` instead of a producer and a worker |

Each file's docstring carries the exact worker command it expects. Every task
declares an explicit `name=`, and each file says why: a name derived from the
defining module differs between the worker's import and a direct run, and the
worker rejects what the producer sent.
