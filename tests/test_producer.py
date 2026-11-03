import json

from pipeline.producer import run


class FakeProducer:
    def __init__(self, fail_first: bool = False):
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.flushed = False
        self._fail_first = fail_first

    def produce(self, topic, key, value, on_delivery):
        if self._fail_first:
            self._fail_first = False
            raise BufferError("queue full")
        self.messages.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, _timeout=0):
        return 0

    def flush(self):
        self.flushed = True


def test_run_publishes_keyed_valid_json_in_time_order(sample_files):
    fake = FakeProducer(fail_first=True)  # also exercises the BufferError retry path
    stats = run(sample_files, "trades", fake, limit=500)
    assert stats.sent == 500 and stats.delivery_errors == 0 and fake.flushed
    times = []
    for topic, key, value in fake.messages:
        msg = json.loads(value)
        assert topic == "trades" and key.decode() == msg["symbol"]
        times.append(msg["trade_time_ms"])
    assert times == sorted(times)
    assert {k.decode() for _, k, _ in fake.messages} == {"BTCUSDT", "ETHUSDT"}


def test_run_counts_delivery_errors(sample_files):
    class Failing(FakeProducer):
        def produce(self, topic, key, value, on_delivery):
            on_delivery("broker down", None)

    stats = run(sample_files, "trades", Failing(), limit=10)
    assert stats.delivery_errors == 10


def test_dry_run_prints_and_needs_no_producer(sample_files, capsys):
    stats = run(sample_files, "trades", None, limit=3, dry_run=True)
    assert stats.sent == 3
    assert len(capsys.readouterr().out.strip().splitlines()) == 3
