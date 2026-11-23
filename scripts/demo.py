#!/usr/bin/env python3
"""Демонстрация через настоящий HTTP API, Kafka и Cassandra. Нужен только Python 3."""
import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
BASE = 'http://localhost:8080'
PREFIX = datetime.now(timezone.utc).strftime('%H%M%S') + '-' + uuid4().hex[:4]


def http(path, body=None, base=BASE):
    request = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                     headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=30) as response:
        data = response.read().decode()
        return json.loads(data) if response.headers.get_content_type().endswith('json') else data


def command(*args, check=True):
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    if check and result.returncode:
        raise RuntimeError(f'{" ".join(args)}\n{result.stdout}\n{result.stderr}')
    return result


def eventually(fn, timeout=90, description='ожидание результата'):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except (urllib.error.URLError, TimeoutError, AssertionError, ConnectionError) as exc:
            last = exc
        time.sleep(1)
    raise AssertionError(f'Не завершилось: {description}. Последний ответ: {last}')


def send(kind, version=2, wait=True, **kwargs):
    event = {'event_id': str(uuid4()), 'event_type': kind,
             'timestamp': datetime.now(timezone.utc).isoformat(), **kwargs}
    ack = http(f'/events?schema_version={version}', event)
    if wait:
        eventually(lambda: http('/events/' + event['event_id'])['status'] == 'APPLIED',
                   description=f'{kind} {event["event_id"]}')
    return event, ack


def check_stock(product, zone, available, reserved=0):
    view = http('/inventory/products/' + product)
    row = next(r for r in view['zones'] if r['zone_id'] == zone)
    assert (row['available_quantity'], row['reserved_quantity']) == (available, reserved), row
    total = view['total']
    assert total['total_available'] == sum(r['available_quantity'] for r in view['zones'])
    assert total['total_reserved'] == sum(r['reserved_quantity'] for r in view['zones'])
    inverse = next(r for r in http('/inventory/zones/' + zone)['products'] if r['product_id'] == product)
    assert inverse == row, (inverse, row)
    print(f'  {product} / {zone}: available={available}, reserved={reserved}; три проекции согласованы', flush=True)
    return row


def step(title):
    print('\n' + title, flush=True)


def basic():
    step('1. Базовый цикл склада и заказ')
    p, order = f'SKU-001-{PREFIX}', f'ORDER-{PREFIX}'
    send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=100)
    check_stock(p, 'ZONE-A', 100)
    send('PRODUCT_RESERVED', product_id=p, zone_id='ZONE-A', quantity=30)
    check_stock(p, 'ZONE-A', 70, 30)
    send('PRODUCT_MOVED', product_id=p, from_zone_id='ZONE-A', to_zone_id='ZONE-B', quantity=20)
    check_stock(p, 'ZONE-A', 50, 30)
    check_stock(p, 'ZONE-B', 20)
    send('PRODUCT_SHIPPED', product_id=p, zone_id='ZONE-A', quantity=10)
    check_stock(p, 'ZONE-A', 40, 30)
    send('ORDER_CREATED', order_id=order, items=[{'product_id': p, 'zone_id': 'ZONE-A', 'quantity': 15}])
    check_stock(p, 'ZONE-A', 25, 45)
    assert http('/orders/' + order)['status'] == 'CREATED'
    send('ORDER_COMPLETED', order_id=order)
    check_stock(p, 'ZONE-A', 25, 30)
    assert http('/orders/' + order)['status'] == 'COMPLETED'
    send('PRODUCT_RELEASED', product_id=p, zone_id='ZONE-A', quantity=5)
    check_stock(p, 'ZONE-A', 30, 25)
    send('INVENTORY_COUNTED', product_id=p, zone_id='ZONE-A', counted_quantity=28)
    check_stock(p, 'ZONE-A', 28, 25)

    step('2. Повторная доставка одного event_id')
    p = f'SKU-002-{PREFIX}'
    event, _ = send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=50)
    duplicate = http('/events', event)
    await_offset(duplicate['offset'])
    check_stock(p, 'ZONE-A', 50)
    print('  Повтор принят Kafka, количество осталось 50.', flush=True)

    step('3. Денормализованные таблицы')
    p = f'SKU-003-{PREFIX}'
    send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=100)
    check_stock(p, 'ZONE-A', 100)
    cql = command('docker', 'exec', 'cassandra-1', 'cqlsh', '-e',
                  f"CONSISTENCY QUORUM; SELECT * FROM warehouse.inventory_by_product WHERE product_id = '{p}';")
    print(cql.stdout.strip(), flush=True)

    step('4. Нарушение порядка: 12:00 → 12:05 → 12:02')
    p = f'SKU-004-{PREFIX}'
    send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=100, timestamp='2026-04-01T12:00:00Z')
    send('PRODUCT_SHIPPED', product_id=p, zone_id='ZONE-A', quantity=20, timestamp='2026-04-01T12:05:00Z')
    stale, _ = send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=50, timestamp='2026-04-01T12:02:00Z')
    assert http('/events/' + stale['event_id'])['outcome'] == 'STALE'
    check_stock(p, 'ZONE-A', 80)

    step('5. Невалидное событие попадает в DLQ, следующее проходит')
    p = f'SKU-005-{PREFIX}'
    bad, ack = send('PRODUCT_SHIPPED', product_id=p, zone_id='ZONE-A', quantity=-5, wait=False)
    await_offset(ack['offset'])
    dlq = json.loads(command('docker', 'compose', 'exec', '-T', 'wms', 'python', '-m',
                             'scripts.read_dlq', '--event-id', bad['event_id']).stdout)
    assert len(dlq) >= 1
    assert dlq[0]['original_event']['quantity'] == -5
    assert dlq[0]['error_code'] == 'VALIDATION_ERROR'
    assert dlq[0]['kafka_metadata']['offset'] == ack['offset']
    print(json.dumps(dlq[0], ensure_ascii=False, indent=2), flush=True)
    send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=10)
    check_stock(p, 'ZONE-A', 10)

    step('8. Схемы V1 и V2 в одном топике')
    p1, p2 = f'SKU-V1-{PREFIX}', f'SKU-V2-{PREFIX}'
    send('PRODUCT_RECEIVED', version=1, product_id=p1, zone_id='ZONE-A', quantity=11)
    send('PRODUCT_RECEIVED', version=2, product_id=p2, zone_id='ZONE-A', quantity=12, supplier_id='SUP-001')
    assert check_stock(p1, 'ZONE-A', 11)['supplier_id'] is None
    assert check_stock(p2, 'ZONE-A', 12)['supplier_id'] == 'SUP-001'
    versions = http('/subjects/warehouse-events-value/versions', base='http://localhost:8081')
    assert versions == [1, 2], versions
    print(f'  Schema Registry: версии {versions}; V1 supplier_id=null, V2 supplier_id=SUP-001.', flush=True)


def await_offset(offset):
    def committed():
        text = http('/metrics', base='http://localhost:8002')
        values = [float(line.rsplit(' ', 1)[1]) for line in text.splitlines()
                  if line.startswith('consumer_committed_offset{')]
        return values and min(values) >= offset + 1
    eventually(committed, timeout=90, description=f'commit offset >= {offset + 1}')


def lag():
    text = http('/metrics', base='http://localhost:8002')
    values = [float(line.rsplit(' ', 1)[1]) for line in text.splitlines() if line.startswith('consumer_lag{')]
    return sum(values) if values else None


def cluster_un(count):
    output = command('docker', 'exec', 'cassandra-1', 'nodetool', 'status').stdout
    return output if sum(line.startswith('UN ') for line in output.splitlines()) == count else None


def cluster_demo():
    step('6. Отказ одной ноды Cassandra и уровни согласованности')
    print(eventually(lambda: cluster_un(3), timeout=180), flush=True)
    p = f'SKU-006-{PREFIX}'
    send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=200)
    command('docker', 'stop', 'cassandra-2')
    try:
        eventually(lambda: cluster_un(2), timeout=90)
        send('PRODUCT_SHIPPED', product_id=p, zone_id='ZONE-A', quantity=50)
        check_stock(p, 'ZONE-A', 150)
        assert http('/health', base='http://localhost:8001')['cassandra']
        for level in ('ONE', 'QUORUM', 'ALL'):
            result = command('docker', 'exec', 'cassandra-1', 'cqlsh', '-e',
                f"CONSISTENCY {level}; SELECT total_available FROM warehouse.inventory_by_product WHERE product_id = '{p}';", check=False)
            if level == 'ALL':
                assert result.returncode != 0 and 'Unavailable' in result.stderr, result
                print('  ALL: ожидаемый отказ — нужны 3 реплики, доступны 2.', flush=True)
                print('  ' + result.stderr.strip(), flush=True)
            else:
                assert result.returncode == 0 and '150' in result.stdout, result
                print(f'  {level}: запрос выполнен, остаток 150.', flush=True)
    finally:
        command('docker', 'start', 'cassandra-2')
        print(eventually(lambda: cluster_un(3), timeout=240), flush=True)


def monitoring_demo():
    step('7. Метрики, накопление lag и срабатывание алерта')
    assert http('/health', base='http://localhost:8001')['worker']
    metrics = http('/metrics', base='http://localhost:8001')
    for name in ('consumer_lag{', 'events_processed_total{', 'event_processing_duration_seconds_bucket{',
                 'cassandra_write_errors_total'):
        assert name in metrics, name
    for line in metrics.splitlines():
        if line.startswith(('events_processed_total{', 'consumer_lag{', 'cassandra_write_errors_total')):
            print('  ' + line, flush=True)
    # Дашборд действительно загружен Grafana, а не только лежит на диске.
    request = urllib.request.Request('http://localhost:3000/api/dashboards/uid/smart-warehouse')
    import base64
    request.add_header('Authorization', 'Basic ' + base64.b64encode(b'admin:warehouse-demo').decode())
    with urllib.request.urlopen(request) as response:
        dashboard = json.load(response)
    assert len(dashboard['dashboard']['panels']) >= 3
    command('docker', 'compose', 'stop', 'consumer')
    try:
        p = f'SKU-LAG-{PREFIX}'
        for _ in range(12):
            send('PRODUCT_RECEIVED', product_id=p, zone_id='ZONE-A', quantity=1, wait=False)
        eventually(lambda: lag() == 12, description='lag = 12 при остановленном consumer')
        print('  Consumer остановлен; отдельный exporter показывает lag=12.', flush=True)

        def firing():
            alerts = http('/api/v1/alerts', base='http://localhost:9090')['data']['alerts']
            return next((a for a in alerts if a['labels']['alertname'] == 'WarehouseConsumerLagHigh'
                         and a['state'] == 'firing'), None)
        alert = eventually(firing, timeout=90, description='WarehouseConsumerLagHigh = firing')
        print(f'  Алерт {alert["labels"]["alertname"]}: {alert["state"]}.', flush=True)
    finally:
        command('docker', 'compose', 'start', 'consumer')
    eventually(lambda: lag() == 0, description='lag = 0 после запуска consumer')
    check_stock(p, 'ZONE-A', 12)
    print('  Очередь обработана, lag=0. Grafana: http://localhost:3000/d/smart-warehouse', flush=True)


def crash_demo():
    step('9. Дополнительная проверка: авария после Cassandra, до Kafka commit')
    command('docker', 'compose', 'stop', 'consumer')
    try:
        eid, p = f'crash-{PREFIX}', f'SKU-CRASH-{PREFIX}'
        event, ack = send('PRODUCT_RECEIVED', event_id=eid, product_id=p, zone_id='ZONE-A', quantity=50, wait=False)
        result = subprocess.run(['docker', 'compose', 'run', '--rm', '--no-deps', '-e',
                                  f'FAIL_AFTER_WRITE_EVENT_ID={eid}', 'consumer'],
                                 cwd=ROOT, capture_output=True, text=True, timeout=120)
        assert result.returncode == 86, result.stdout + result.stderr
        check_stock(p, 'ZONE-A', 50)
        print('  Процесс завершился с кодом 86 после записи. Offset ещё не подтверждён.', flush=True)
    finally:
        command('docker', 'compose', 'start', 'consumer')
    await_offset(ack['offset'])
    check_stock(p, 'ZONE-A', 50)
    print('  После восстановления осталось 50, повторного прибавления нет.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--full', action='store_true', help='Также остановка ноды, consumer, алерт и авария до commit')
    args = parser.parse_args()
    print(f'Smart Warehouse — демонстрация {PREFIX}\nВремя UTC: {datetime.now(timezone.utc).isoformat()}', flush=True)
    eventually(lambda: http('/health'), timeout=240, description='запуск WMS')
    eventually(lambda: http('/health', base='http://localhost:8001'), timeout=120, description='запуск consumer')
    basic()
    if args.full:
        cluster_demo()
        monitoring_demo()
        crash_demo()
    else:
        print('\nДля отказоустойчивости и алертов: python3 scripts/demo.py --full', flush=True)
    print('\nВсе выбранные сценарии прошли. Проверки выполнены на работающей инфраструктуре.', flush=True)


if __name__ == '__main__':
    main()
