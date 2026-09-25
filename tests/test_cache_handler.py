"""Object-list cache helpers: in-memory cache semantics and the CSV persistence under the user's home."""
import time

import pytest

from protocol_proxy.protocol.bacnet import cache_handler as ch

from tests.conftest import error_pdu


class FakeReader:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = 0

    async def read_property(self, address, object_identifier, prop):
        self.calls += 1
        return self.replies.pop(0)


def run(coro):
    import asyncio
    return asyncio.run(coro)


def test_cache_miss_reads_device_then_hits():
    reader = FakeReader(['analog-input,1', 'analog-value,1'])
    cache = {}
    first = run(ch._get_cached_object_list(reader, cache, 300, '10.0.0.5', 'device,7'))
    second = run(ch._get_cached_object_list(reader, cache, 300, '10.0.0.5', 'device,7'))
    assert first == second == ['analog-input,1', 'analog-value,1'] and reader.calls == 1
    assert list(cache) == ['10.0.0.5:device,7']


def test_expired_entry_is_reread():
    reader = FakeReader(['old'], ['new'])
    cache = {'10.0.0.5:device,7': (['stale'], time.time() - 1000)}
    assert run(ch._get_cached_object_list(reader, cache, 300, '10.0.0.5', 'device,7')) == ['old']
    cache['10.0.0.5:device,7'] = (['old'], time.time() - 1000)
    assert run(ch._get_cached_object_list(reader, cache, 300, '10.0.0.5', 'device,7')) == ['new']


@pytest.mark.parametrize('reply', [error_pdu('device', 'unknown-object'), 'not-a-list', None])
def test_errors_and_invalid_replies_return_none_and_are_not_cached(reply):
    cache = {}
    assert run(ch._get_cached_object_list(FakeReader(reply), cache, 300, '10.0.0.5', 'device,7')) is None
    assert cache == {}


def test_exception_while_reading_returns_none():
    class Boom:
        async def read_property(self, *a):
            raise RuntimeError('link down')
    assert run(ch._get_cached_object_list(Boom(), {}, 300, '10.0.0.5', 'device,7')) is None


def test_clear_cache_for_device_and_all_objects_of_device():
    cache = {'a:device,1': ([], 0), 'a:device,2': ([], 0), 'b:device,1': ([], 0)}
    ch._clear_cache_for_device(cache, 'a', 'device,1')
    assert set(cache) == {'a:device,2', 'b:device,1'}
    ch._clear_cache_for_device(cache, 'a')
    assert set(cache) == {'b:device,1'}
    ch._clear_cache_for_device(cache, 'nobody')             # no error


def test_cache_stats():
    now = time.time()
    cache = {'a:device,1': (['x', 'y'], now), 'b:device,1': (None, now - 1000)}
    stats = ch._get_cache_stats(cache, 300)
    assert stats['total_entries'] == 2
    by_device = {e['device']: e for e in stats['entries']}
    assert by_device['a:device,1']['object_count'] == 2 and by_device['a:device,1']['expired'] is False
    assert by_device['b:device,1']['object_count'] == 0 and by_device['b:device,1']['expired'] is True


def test_object_properties_persist_under_home(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))       # Path.home() follows HOME on POSIX
    ch.save_object_properties('10.0.0.5', 'device,7', 'analog-input,1',
                              {'object-name': 'Temp', 'units': 'degreesFahrenheit', 'present-value': 68.25})
    ch.save_object_properties('10.0.0.5', 'device,7', 'analog-input,1',
                              {'object-name': 'Temp2', 'units': 'degreesFahrenheit'})       # update, not duplicate
    loaded = ch.load_cached_object_properties('10.0.0.5', 'device,7')
    assert (tmp_path / '.bacnet_scan_tool' / 'object_properties.csv').exists()
    assert loaded['status'] == 'done' and loaded['_from_cache'] is True
    assert loaded['results']['analog-input,1']['object-name'] == 'Temp2'
    assert loaded['pagination']['total_items'] == 1        # updated in place, not duplicated
