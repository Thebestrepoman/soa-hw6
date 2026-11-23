import hashlib
from copy import deepcopy
from unittest.mock import Mock

import pytest

from warehouse.consumer import handle
from warehouse.domain import EventConflict, InvalidEvent
from warehouse.store import Store, dumps


class MemoryStore(Store):
    """Имитирует сбой после изменения первой проекции, до завершения batch."""
    def __init__(self):
        self.saved = None
        self.quantity = 0
        self.builds = 0
        self.fail_once = True
        self.applied_plans = []

    def processed(self, event_id):
        return deepcopy(self.saved)

    def make_plan(self, event, partition, offset):
        self.builds += 1
        return {'outcome': 'APPLIED', 'value': self.quantity + event['quantity'], 'write_timestamp': 12345}

    def save_intent(self, event, plan, partition, offset):
        self.saved = dict(event_id=event['event_id'], payload_hash=hashlib.sha256(dumps(event).encode()).hexdigest(),
                          plan_json=dumps(plan), source_partition=partition, source_offset=offset, status='PLANNED')
        return self.processed(event['event_id'])

    def apply_plan(self, plan):
        self.applied_plans.append(plan)
        self.quantity = plan['value']
        if self.fail_once:
            self.fail_once = False
            raise TimeoutError('Ответ координатора потерян после первой записи')
        self.saved['status'] = 'APPLIED'


def test_recovery_reuses_durable_plan_after_ambiguous_write():
    store = MemoryStore()
    event = {'event_id': 'same', 'quantity': 50}
    with pytest.raises(TimeoutError):
        store.process(event, 0, 10)
    assert store.quantity == 50
    assert store.process(event, 0, 10) == 'APPLIED'
    assert store.quantity == 50
    assert store.builds == 1
    assert store.applied_plans[0] == store.applied_plans[1]
    assert store.process(event, 0, 11) == 'DUPLICATE'
    assert store.quantity == 50


def test_event_id_collision_is_rejected():
    store = MemoryStore()
    store.fail_once = False
    store.process({'event_id': 'same', 'quantity': 50}, 0, 1)
    with pytest.raises(EventConflict):
        store.process({'event_id': 'same', 'quantity': 70}, 0, 2)
    assert store.quantity == 50


def setup_delivery():
    message = Mock()
    message.value.return_value = b'raw-avro'
    message.key.return_value = b'warehouse-main'
    message.headers.return_value = []
    message.topic.return_value = 'warehouse-events'
    message.partition.return_value = 0
    message.offset.return_value = 7
    message.timestamp.return_value = (1, 1000)
    event = {'event_id': 'e', 'event_type': 'PRODUCT_RECEIVED'}
    codec = Mock()
    codec.decode.return_value = (event, event, 1)
    return message, codec, Mock(), Mock(), Mock()


def test_commit_happens_strictly_after_database_ack():
    message, codec, store, publisher, commit = setup_delivery()
    order = []
    store.process.side_effect = lambda *a, **k: order.append('db') or 'APPLIED'
    commit.side_effect = lambda *a: order.append('commit')
    handle(message, codec, store, publisher, commit)
    assert order == ['db', 'commit']


def test_database_outage_neither_commits_nor_discards_event_to_dlq():
    args = setup_delivery()
    args[2].process.side_effect = TimeoutError('quorum unavailable')
    with pytest.raises(TimeoutError):
        handle(*args)
    args[3].send.assert_not_called()
    args[4].assert_not_called()


def test_dlq_ack_before_commit_and_original_bytes_preserved():
    args = setup_delivery()
    args[2].process.side_effect = InvalidEvent('negative quantity')
    order = []
    args[3].send.side_effect = lambda *a: order.append('dlq')
    args[4].side_effect = lambda *a: order.append('commit')
    handle(*args)
    assert order == ['dlq', 'commit']
    import json
    dlq = json.loads(args[3].send.call_args.args[2])
    assert dlq['original_event']['event_id'] == 'e'
    assert dlq['original_bytes_base64']
    assert dlq['kafka_metadata']['offset'] == 7


def test_failed_dlq_write_does_not_commit():
    args = setup_delivery()
    args[2].process.side_effect = InvalidEvent('negative quantity')
    args[3].send.side_effect = RuntimeError('broker unavailable')
    with pytest.raises(RuntimeError):
        handle(*args)
    args[4].assert_not_called()
