"""Producer-side `meta` reaches a before_send middleware and survives the send."""

import tarsk


class _Tagger:
    """Attaches a fixed key, the way Nevra attaches a trace id."""

    def before_send(self, ctx):
        ctx.meta["tag"] = "from-middleware"


def _enqueue(app, task):
    """Send through the real Enqueue path, capturing what reached the producer."""
    captured = {}

    class _FakeProducer:
        def send(self, task_id, queue, name, payload, timeout_ms, delay,
                 chain, meta, key, ttl_ms, expires_ms):
            captured["meta"] = meta
            return ""

    app._producer = _FakeProducer()
    task.send()
    return captured["meta"]


def test_before_send_sees_the_senders_meta():
    app = tarsk.App(broker="memory://")

    seen = {}

    class _Reader:
        def before_send(self, ctx):
            seen["meta"] = dict(ctx.meta)

    app.middleware(_Reader())

    @app.task()
    async def noop():
        pass

    _enqueue(app, noop.options(meta={"from": "sender"}))
    assert seen["meta"] == {"from": "sender"}


def test_middleware_can_add_to_meta_and_it_is_sent():
    from tarsk import _proto

    app = tarsk.App(broker="memory://")
    app.middleware(_Tagger())

    @app.task()
    async def noop():
        pass

    packed = _enqueue(app, noop.options(meta={"from": "sender"}))
    assert _proto.unpack_result(packed) == {"from": "sender", "tag": "from-middleware"}


def test_middleware_meta_is_sent_when_the_sender_gave_none():
    from tarsk import _proto

    app = tarsk.App(broker="memory://")
    app.middleware(_Tagger())

    @app.task()
    async def noop():
        pass

    packed = _enqueue(app, noop)
    assert packed != b"", "an empty sender meta must not discard what a middleware added"
    assert _proto.unpack_result(packed) == {"tag": "from-middleware"}
