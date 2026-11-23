import hashlib
import json
import re
import time
from datetime import datetime, timezone

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, FallthroughRetryPolicy
from cassandra.query import BatchStatement, BatchType, dict_factory

from warehouse.config import HOSTS, KEYSPACE
from warehouse.domain import EventConflict, involved_products, transition, validate


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


class Store:
    def __init__(self, keyspace=KEYSPACE):
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='dc1')),
            consistency_level=ConsistencyLevel.QUORUM,
            retry_policy=FallthroughRetryPolicy(), request_timeout=8,
            row_factory=dict_factory,
        )
        self.cluster = Cluster(HOSTS, execution_profiles={EXEC_PROFILE_DEFAULT: profile}, protocol_version=4)
        self.session = self.cluster.connect()
        self.keyspace = keyspace
        self.prepared = {}
        self.on_write_error = lambda: None

    def close(self):
        self.cluster.shutdown()

    def prepare(self, cql):
        if self.keyspace:
            cql = re.sub(r'\b(FROM|INTO|UPDATE) ([a-z_]+)(?=\s|\()',
                         lambda m: f'{m[1]} {self.keyspace}.{m[2]}', cql)
        return self.session.prepare(cql.replace('%s', '?'))

    def query(self, cql, params=()):
        if cql not in self.prepared:
            self.prepared[cql] = self.prepare(cql)
        if cql.startswith(('INSERT', 'UPDATE', 'DELETE')):
            return self.write(self.prepared[cql], params)
        return self.session.execute(self.prepared[cql], params)

    def write(self, statement, params=None):
        try:
            return self.session.execute(statement, params)
        except Exception:
            self.on_write_error()
            raise

    def health(self):
        self.query("SELECT version FROM schema_migrations WHERE version = %s", ('002_supplier.cql',)).one()

    def product(self, product):
        return list(self.query('SELECT * FROM inventory_by_product_zone WHERE product_id = %s', (product,)))

    def total(self, product):
        return self.query('SELECT * FROM inventory_by_product WHERE product_id = %s', (product,)).one()

    def zone(self, zone):
        return list(self.query('SELECT * FROM inventory_by_zone WHERE zone_id = %s', (zone,)))

    def order(self, order_id):
        row = self.query('SELECT * FROM orders WHERE order_id = %s', (order_id,)).one()
        if row:
            row['items'] = json.loads(row.pop('items_json'))
        return row

    def processed(self, event_id):
        return self.query('SELECT * FROM processed_events WHERE event_id = %s', (event_id,)).one()

    def make_plan(self, event, partition, offset):
        validate(event)
        order = self.order(event['order_id']) if event['event_type'].startswith('ORDER_') else None
        products = involved_products(event, order)
        inventory = {p: {r['zone_id']: r for r in self.product(p)} for p in products}
        totals = {p: self.total(p) or {} for p in products}
        zones = {z for rows in inventory.values() for z in rows}
        zones.update(i['zone_id'] for i in event.get('items', []))
        if order:
            zones.update(i['zone_id'] for i in order['items'])
        zones.update(event[z] for z in ('zone_id', 'from_zone_id', 'to_zone_id') if event.get(z))
        zone_rows = {}
        for z in zones:
            row = self.query('SELECT * FROM zones WHERE zone_id = %s', (z,)).one()
            if row:
                zone_rows[z] = row
        plan = transition(event, {'inventory': inventory, 'totals': totals, 'order': order, 'zones': zone_rows})
        clock = self.query('SELECT write_timestamp FROM runtime_state WHERE name = %s', ('consumer',)).one()
        plan['write_timestamp'] = max(time.time_ns() // 1000, (clock or {}).get('write_timestamp', 0) + 1)
        plan['event'] = event
        plan['partition'] = partition
        plan['offset'] = offset
        return plan

    def save_intent(self, event, plan, partition, offset):
        digest = hashlib.sha256(dumps(event).encode()).hexdigest()
        cql = ('INSERT INTO processed_events (event_id, payload_hash, plan_json, source_partition, '
               'source_offset, outcome, status) VALUES (%s,%s,%s,%s,%s,%s,%s) IF NOT EXISTS')
        if cql not in self.prepared:
            self.prepared[cql] = self.prepare(cql)
            self.prepared[cql].serial_consistency_level = ConsistencyLevel.SERIAL
        self.write(self.prepared[cql], (event['event_id'], digest, dumps(plan), partition, offset,
                                                   plan['outcome'], 'PLANNED'))
        # После неизвестного результата LWT следующий вызов прочитает тот же сохранённый план.
        return self.processed(event['event_id'])

    def apply_plan(self, plan):
        batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=ConsistencyLevel.QUORUM)
        timestamp = plan['write_timestamp']

        def insert(table, row):
            columns = sorted(row)
            cql = (f'INSERT INTO {table} ({", ".join(columns)}) '
                   f'VALUES ({", ".join(["%s"] * len(columns))}) USING TIMESTAMP %s')
            if cql not in self.prepared:
                self.prepared[cql] = self.prepare(cql)
            batch.add(self.prepared[cql], tuple(row[c] for c in columns) + (timestamp,))

        for row in plan['inventory']:
            insert('inventory_by_product_zone', row)
            insert('inventory_by_zone', row)
        for row in plan['totals']:
            insert('inventory_by_product', row)
        for row in plan['zones']:
            insert('zones', row)
        if plan['order']:
            row = dict(plan['order'])
            row['items_json'] = dumps(row.pop('items'))
            insert('orders', row)
        event = plan['event']
        insert('event_history', {
            'event_day': datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).astimezone(timezone.utc).date(),
            'event_id': event['event_id'], 'event_type': event['event_type'], 'event_at': event['timestamp'],
            'source_partition': plan['partition'], 'source_offset': plan['offset'],
            'outcome': plan['outcome'], 'payload_json': dumps(event), 'supplier_id': event.get('supplier_id'),
        })
        insert('runtime_state', {'name': 'consumer', 'write_timestamp': timestamp})
        self.write(batch)
        # Статус меняется только после подтверждённого BATCH. При сбое до этой строки
        # повторяется сохранённый план с теми же значениями и timestamp.
        self.query('UPDATE processed_events SET status = %s WHERE event_id = %s', ('APPLIED', event['event_id']))

    def process(self, event, partition, offset, after_write=None):
        saved = self.processed(event['event_id'])
        if not saved:
            plan = self.make_plan(event, partition, offset)
            saved = self.save_intent(event, plan, partition, offset)
        digest = hashlib.sha256(dumps(event).encode()).hexdigest()
        if saved['payload_hash'] != digest:
            raise EventConflict('event_id уже использован для другого содержимого')
        original_position = (saved['source_partition'], saved['source_offset']) == (partition, offset)
        if saved['status'] == 'APPLIED' and not original_position:
            return 'DUPLICATE'
        plan = json.loads(saved['plan_json'])
        self.apply_plan(plan)
        if after_write:
            after_write()
        return plan['outcome']
