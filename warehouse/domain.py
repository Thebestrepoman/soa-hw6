"""Правила склада. Здесь нет клиентов Kafka и Cassandra."""
from copy import deepcopy
from datetime import datetime, timezone

EVENT_TYPES = {
    'PRODUCT_RECEIVED', 'PRODUCT_SHIPPED', 'PRODUCT_MOVED', 'PRODUCT_RESERVED',
    'PRODUCT_RELEASED', 'INVENTORY_COUNTED', 'ORDER_CREATED', 'ORDER_COMPLETED',
}


class InvalidEvent(ValueError):
    code = 'VALIDATION_ERROR'


class EventConflict(InvalidEvent):
    code = 'EVENT_ID_CONFLICT'


def required(event, *fields):
    for field in fields:
        value = event.get(field)
        if not isinstance(value, str) or not value.strip() or len(value) > 128:
            raise InvalidEvent(f'{field}: требуется непустая строка длиной до 128 символов')


def positive(value, field='quantity', allow_zero=False):
    minimum = 0 if allow_zero else 1
    if type(value) is not int or not minimum <= value <= 1_000_000:
        raise InvalidEvent(f'{field}: ожидается целое число от {minimum} до 1000000, получено {value}')


def event_time(event):
    try:
        dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        if dt.tzinfo is None:
            raise ValueError('нет часового пояса')
        return int(dt.timestamp() * 1_000_000)
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError) as exc:
        raise InvalidEvent('timestamp: нужна дата ISO 8601 с часовым поясом') from exc


def validate(event):
    required(event, 'event_id', 'event_type')
    event_time(event)
    kind = event['event_type']
    if kind not in EVENT_TYPES:
        raise InvalidEvent(f'Неизвестный event_type: {kind}')
    if kind == 'ORDER_CREATED':
        required(event, 'order_id')
        items = event.get('items')
        if not isinstance(items, list) or not 1 <= len(items) <= 10:
            raise InvalidEvent('Заказ должен содержать от 1 до 10 позиций')
        seen = set()
        for item in items:
            required(item, 'product_id', 'zone_id')
            positive(item.get('quantity'))
            key = (item['product_id'], item['zone_id'])
            if key in seen:
                raise InvalidEvent('Повторная позиция товара в одной зоне')
            seen.add(key)
    elif kind == 'ORDER_COMPLETED':
        required(event, 'order_id')
    else:
        required(event, 'product_id')
        if kind == 'PRODUCT_MOVED':
            required(event, 'from_zone_id', 'to_zone_id')
            if event['from_zone_id'] == event['to_zone_id']:
                raise InvalidEvent('Зоны перемещения должны различаться')
        else:
            required(event, 'zone_id')
        if kind == 'INVENTORY_COUNTED':
            positive(event.get('counted_quantity'), 'counted_quantity', allow_zero=True)
        else:
            positive(event.get('quantity'))


def involved_products(event, order):
    if event['event_type'] == 'ORDER_CREATED':
        return {i['product_id'] for i in event['items']}
    if event['event_type'] == 'ORDER_COMPLETED':
        return {i['product_id'] for i in order['items']} if order else set()
    return {event['product_id']}


def transition(event, snapshot):
    """Возвращает абсолютные значения строк, не операции вида quantity += n."""
    validate(event)
    state = deepcopy(snapshot)
    kind, at, eid = event['event_type'], event_time(event), event['event_id']
    order = state.get('order')
    products = involved_products(event, order)
    watermarks = [state['totals'].get(p, {}).get('last_event_at', -1) for p in products]
    if order:
        watermarks.append(order['last_event_at'])
    # Равное время также считается устаревшим: первая принятая операция побеждает.
    if any(at <= watermark for watermark in watermarks):
        return {'outcome': 'STALE', 'inventory': [], 'totals': [], 'zones': [], 'order': None}
    changed = set()
    zone_deltas = {}

    def adjust(product, zone, available=0, reserved=0, manual=0, counted=None):
        if zone not in state['zones']:
            raise InvalidEvent(f'Неизвестная зона: {zone}')
        rows = state['inventory'].setdefault(product, {})
        row = rows.setdefault(zone, {
            'product_id': product, 'zone_id': zone, 'available_quantity': 0,
            'reserved_quantity': 0, 'manual_reserved_quantity': 0, 'supplier_id': None,
            'last_event_at': -1, 'last_event_id': '',
        })
        before = row['available_quantity'] + row['reserved_quantity']
        row['available_quantity'] = (row['available_quantity'] + available if counted is None else counted)
        row['reserved_quantity'] += reserved
        row['manual_reserved_quantity'] += manual
        if min(row['available_quantity'], row['reserved_quantity'], row['manual_reserved_quantity']) < 0:
            raise InvalidEvent(f'Недостаточно доступного товара или ручного резерва: {product} / {zone}')
        if row['manual_reserved_quantity'] > row['reserved_quantity']:
            raise InvalidEvent('Ручной резерв превышает общий резерв')
        if row['available_quantity'] + row['reserved_quantity'] > 2_000_000_000:
            raise InvalidEvent('Превышен допустимый остаток')
        row.update(last_event_at=at, last_event_id=eid)
        if kind == 'PRODUCT_RECEIVED':
            row['supplier_id'] = event.get('supplier_id')
        zone_deltas[zone] = zone_deltas.get(zone, 0) + row['available_quantity'] + row['reserved_quantity'] - before
        changed.add((product, zone))

    result_order = None
    if kind == 'ORDER_CREATED':
        if order:
            raise InvalidEvent(f"Заказ {event['order_id']} уже существует")
        for item in event['items']:
            adjust(item['product_id'], item['zone_id'], -item['quantity'], item['quantity'])
        result_order = {'order_id': event['order_id'], 'status': 'CREATED',
                        'items': event['items'], 'last_event_at': at, 'last_event_id': eid}
    elif kind == 'ORDER_COMPLETED':
        if not order or order['status'] != 'CREATED':
            raise InvalidEvent('Для завершения нужен существующий заказ со статусом CREATED')
        for item in order['items']:
            adjust(item['product_id'], item['zone_id'], reserved=-item['quantity'])
        result_order = dict(order, status='COMPLETED', last_event_at=at, last_event_id=eid)
    else:
        p, z, q = event['product_id'], event.get('zone_id'), event.get('quantity')
        if kind == 'PRODUCT_RECEIVED':
            adjust(p, z, available=q)
        elif kind == 'PRODUCT_SHIPPED':
            adjust(p, z, available=-q)
        elif kind == 'PRODUCT_RESERVED':
            adjust(p, z, -q, q, q)
        elif kind == 'PRODUCT_RELEASED':
            adjust(p, z, q, -q, -q)
        elif kind == 'PRODUCT_MOVED':
            adjust(p, event['from_zone_id'], available=-q)
            adjust(p, event['to_zone_id'], available=q)
        elif kind == 'INVENTORY_COUNTED':
            adjust(p, z, counted=event['counted_quantity'])
    zone_rows = []
    for zone, delta in sorted(zone_deltas.items()):
        row = state['zones'][zone]
        row['occupied'] += delta
        if not 0 <= row['occupied'] <= row['capacity']:
            raise InvalidEvent(f'Превышена вместимость зоны {zone}: {row["occupied"]}/{row["capacity"]}')
        # Эта метка нужна для аудита; порядок событий проверяется по товару и заказу.
        row['last_event_at'] = max(row.get('last_event_at') or -1, at)
        zone_rows.append(row)
    totals = []
    for p in sorted(products):
        rows = state['inventory'].get(p, {}).values()
        available = sum(r['available_quantity'] for r in rows)
        reserved = sum(r['reserved_quantity'] for r in rows)
        if max(available, reserved) > 2_000_000_000:
            raise InvalidEvent('Превышен допустимый суммарный остаток')
        totals.append({'product_id': p, 'total_available': available, 'total_reserved': reserved,
                       'last_event_at': at, 'last_event_id': eid})
    return {'outcome': 'APPLIED', 'inventory': [state['inventory'][p][z] for p, z in sorted(changed)],
            'totals': totals, 'zones': zone_rows, 'order': result_order}
