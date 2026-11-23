#!/usr/bin/env python3
"""Проверка HTTP 503 и сохранения события при потере quorum Cassandra."""
import json
import urllib.error
from demo import PREFIX, http, command, eventually, cluster_un, send, check_stock, lag, await_offset


def unavailable():
    try:
        http('/health', base='http://localhost:8001')
    except urllib.error.HTTPError as exc:
        if exc.code == 503:
            value = json.loads(exc.read())
            return value if not value['cassandra'] else None
    return None


def main():
    print('Проверка потери quorum Cassandra', flush=True)
    eventually(lambda: http('/health'), timeout=120, description='готовность WMS')
    eventually(lambda: http('/health', base='http://localhost:8001'), timeout=120, description='готовность consumer')
    eventually(lambda: cluster_un(3), timeout=180)
    product = f'SKU-QUORUM-{PREFIX}'
    send('PRODUCT_RECEIVED', product_id=product, zone_id='ZONE-A', quantity=20)
    try:
        command('docker', 'stop', 'cassandra-2', 'cassandra-3')
        eventually(lambda: cluster_un(1), timeout=90)
        health = eventually(unavailable, timeout=90, description='/health = 503')
        print('HTTP 503:', health, flush=True)
        event, ack = send('PRODUCT_SHIPPED', product_id=product, zone_id='ZONE-A', quantity=5, wait=False)
        eventually(lambda: lag() is not None and lag() >= 1, description='событие остаётся в очереди')
        dlq = json.loads(command('docker', 'compose', 'exec', '-T', 'wms', 'python', '-m',
                                'scripts.read_dlq', '--event-id', event['event_id']).stdout)
        assert dlq == [], dlq
        print('Корректное событие осталось без commit и не было отправлено в DLQ.', flush=True)
    finally:
        command('docker', 'start', 'cassandra-2', 'cassandra-3')
        eventually(lambda: cluster_un(3), timeout=240)
    await_offset(ack['offset'])
    eventually(lambda: http('/health', base='http://localhost:8001'), description='возврат HTTP 200')
    check_stock(product, 'ZONE-A', 15)
    print('Quorum восстановлен: HTTP 200, lag=0, отгрузка применена ровно один раз.', flush=True)


if __name__ == '__main__':
    main()
