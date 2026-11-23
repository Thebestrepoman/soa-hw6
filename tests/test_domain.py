from copy import deepcopy

import pytest

from warehouse.domain import InvalidEvent, transition


def event(kind='PRODUCT_RECEIVED', **extra):
    return dict(event_id='e-1', event_type=kind, timestamp='2026-04-01T12:00:00Z',
                product_id='SKU-1', zone_id='ZONE-A', quantity=100, **extra)


def empty():
    return {'inventory': {}, 'totals': {}, 'order': None, 'zones': {
        z: {'zone_id': z, 'capacity': 1000, 'occupied': 0, 'last_event_at': -1}
        for z in ('ZONE-A', 'ZONE-B')}}


def apply(snapshot, ev):
    plan = transition(ev, snapshot)
    for row in plan['inventory']:
        snapshot['inventory'].setdefault(row['product_id'], {})[row['zone_id']] = row
    for row in plan['totals']:
        snapshot['totals'][row['product_id']] = row
    for row in plan['zones']:
        snapshot['zones'][row['zone_id']] = row
    if plan['order']:
        snapshot['order'] = plan['order']
    return plan


def next_event(kind, minute, **kwargs):
    ev = event(kind)
    ev.update(event_id=f'e-{minute}', timestamp=f'2026-04-01T12:{minute:02d}:00Z', **kwargs)
    return ev


def test_full_warehouse_cycle():
    state = empty()
    apply(state, event())
    apply(state, next_event('PRODUCT_RESERVED', 1, quantity=30))
    apply(state, next_event('PRODUCT_MOVED', 2, from_zone_id='ZONE-A', to_zone_id='ZONE-B', quantity=20))
    apply(state, next_event('PRODUCT_SHIPPED', 3, quantity=10))
    apply(state, next_event('ORDER_CREATED', 4, order_id='O-1', items=[
        {'product_id': 'SKU-1', 'zone_id': 'ZONE-A', 'quantity': 15}]))
    row = state['inventory']['SKU-1']['ZONE-A']
    assert (row['available_quantity'], row['reserved_quantity']) == (25, 45)
    apply(state, next_event('ORDER_COMPLETED', 5, order_id='O-1'))
    row = state['inventory']['SKU-1']['ZONE-A']
    assert (row['available_quantity'], row['reserved_quantity']) == (25, 30)
    assert state['totals']['SKU-1']['total_available'] == 45
    assert state['totals']['SKU-1']['total_reserved'] == 30
    assert state['order']['status'] == 'COMPLETED'


def test_release_and_count_are_distinct_from_shipping():
    state = empty()
    apply(state, event())
    apply(state, next_event('PRODUCT_RESERVED', 1, quantity=20))
    apply(state, next_event('PRODUCT_RELEASED', 2, quantity=5))
    apply(state, next_event('INVENTORY_COUNTED', 3, counted_quantity=7))
    row = state['inventory']['SKU-1']['ZONE-A']
    assert (row['available_quantity'], row['reserved_quantity']) == (7, 15)
    assert state['zones']['ZONE-A']['occupied'] == 22


def test_stale_delta_is_ignored():
    state = empty()
    apply(state, event())
    apply(state, next_event('PRODUCT_SHIPPED', 5, quantity=20))
    before = deepcopy(state)
    result = apply(state, next_event('PRODUCT_RECEIVED', 2, quantity=50))
    assert result['outcome'] == 'STALE'
    assert state == before
    assert state['totals']['SKU-1']['total_available'] == 80


def test_equal_timestamp_is_ignored():
    state = empty()
    apply(state, event())
    assert transition(dict(event(), event_id='different'), state)['outcome'] == 'STALE'


@pytest.mark.parametrize('kind,extra', [
    ('PRODUCT_SHIPPED', {'quantity': -5}),
    ('PRODUCT_RESERVED', {'quantity': 0}),
    ('PRODUCT_RECEIVED', {'quantity': True}),
    ('INVENTORY_COUNTED', {'counted_quantity': -1}),
    ('PRODUCT_MOVED', {'from_zone_id': 'ZONE-A', 'to_zone_id': 'ZONE-A'}),
    ('UNKNOWN', {}),
    ('ORDER_CREATED', {'order_id': 'O', 'items': []}),
    ('ORDER_COMPLETED', {'order_id': 'missing'}),
    ('PRODUCT_RECEIVED', {'zone_id': 'missing'}),
    ('PRODUCT_RECEIVED', {'timestamp': '2026-04-01T12:00:00'}),
])
def test_invalid_events_leave_state_unchanged(kind, extra):
    state = empty()
    before = deepcopy(state)
    ev = event(kind)
    ev.update(extra)
    with pytest.raises(InvalidEvent):
        transition(ev, state)
    assert state == before


def test_failed_multi_item_order_is_all_or_nothing():
    state = empty()
    apply(state, event())
    before = deepcopy(state)
    ev = next_event('ORDER_CREATED', 1, order_id='O', items=[
        {'product_id': 'SKU-1', 'zone_id': 'ZONE-A', 'quantity': 10},
        {'product_id': 'SKU-missing', 'zone_id': 'ZONE-A', 'quantity': 10},
    ])
    with pytest.raises(InvalidEvent):
        transition(ev, state)
    assert state == before


def test_capacity_counts_available_and_reserved():
    state = empty()
    state['zones']['ZONE-A']['capacity'] = 100
    apply(state, event())
    apply(state, next_event('PRODUCT_RESERVED', 1, quantity=80))
    assert state['zones']['ZONE-A']['occupied'] == 100
    with pytest.raises(InvalidEvent, match='вместимость'):
        transition(next_event('PRODUCT_RECEIVED', 2, quantity=1), state)


def test_manual_release_cannot_steal_order_reservation():
    state = empty()
    apply(state, event())
    apply(state, next_event('ORDER_CREATED', 1, order_id='O', items=[
        {'product_id': 'SKU-1', 'zone_id': 'ZONE-A', 'quantity': 10}]))
    with pytest.raises(InvalidEvent):
        transition(next_event('PRODUCT_RELEASED', 2, quantity=1), state)


def test_stale_order_does_not_update_any_product():
    state = empty()
    apply(state, event())
    apply(state, next_event('PRODUCT_RECEIVED', 5, product_id='SKU-2'))
    before = deepcopy(state)
    plan = transition(next_event('ORDER_CREATED', 2, order_id='O', items=[
        {'product_id': 'SKU-1', 'zone_id': 'ZONE-A', 'quantity': 10},
        {'product_id': 'SKU-2', 'zone_id': 'ZONE-A', 'quantity': 10},
    ]), state)
    assert plan['outcome'] == 'STALE'
    assert state == before


def test_supplier_v1_default_and_v2_value():
    state = empty()
    apply(state, event())
    assert state['inventory']['SKU-1']['ZONE-A']['supplier_id'] is None
    apply(state, next_event('PRODUCT_RECEIVED', 1, supplier_id='SUP-1'))
    assert state['inventory']['SKU-1']['ZONE-A']['supplier_id'] == 'SUP-1'


def test_transition_is_deterministic_and_does_not_mutate_snapshot():
    state = empty()
    before = deepcopy(state)
    assert transition(event(), state) == transition(event(), state)
    assert state == before
