import json

from warehouse.codec import AvroCodec, SCHEMAS
from warehouse.domain import InvalidEvent
import pytest


@pytest.fixture
def codec():
    value = AvroCodec()
    value.request = lambda method, path: {
        'id': 91 + int(path[-1]),
        'schema': (SCHEMAS / f'warehouse-event-v{path[-1]}.avsc').read_text(),
    }
    return value


@pytest.mark.parametrize('version', [1, 2])
def test_avro_wire_roundtrip_and_reader_default(codec, version):
    event = {'event_id': 'e', 'event_type': 'PRODUCT_RECEIVED', 'timestamp': '2026-04-01T12:00:00Z',
             'product_id': 'SKU', 'zone_id': 'ZONE-A', 'quantity': 10}
    if version == 2:
        event['supplier_id'] = 'SUP-001'
    wire = codec.encode(event, version)
    decoded, original, actual_version = codec.decode(wire)
    assert wire[0] == 0
    assert actual_version == version
    assert decoded['supplier_id'] == (None if version == 1 else 'SUP-001')
    assert ('supplier_id' in original) == (version == 2)


@pytest.mark.parametrize('payload', [b'', b'plain json', b'\x00\x00'])
def test_invalid_wire_goes_to_validation_error(codec, payload):
    with pytest.raises(InvalidEvent):
        codec.decode(payload)


def test_truncated_avro_body_is_invalid_event(codec):
    event = {'event_id': 'event-with-long-id', 'event_type': 'PRODUCT_RECEIVED',
             'timestamp': '2026-04-01T12:00:00Z'}
    payload = codec.encode(event, 1)
    with pytest.raises(InvalidEvent, match='Повреждено Avro'):
        codec.decode(payload[:7])
