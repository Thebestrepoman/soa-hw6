import json
import time
from pathlib import Path

from cassandra import InvalidRequest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from warehouse.codec import AvroCodec, SCHEMAS
from warehouse.config import BOOTSTRAP, TOPIC, DLQ, SUBJECT
from warehouse.store import Store


def main():
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    for name, future in admin.create_topics([NewTopic(TOPIC, 1, 1), NewTopic(DLQ, 1, 1)]).items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].name() != 'TOPIC_ALREADY_EXISTS':
                raise
        print(f'Kafka topic готов: {name}', flush=True)
    metadata = admin.list_topics(TOPIC, timeout=10)
    if len(metadata.topics[TOPIC].partitions) != 1:
        raise RuntimeError('Для последовательной обработки склада нужна ровно одна партиция')
    codec = AvroCodec()
    codec.request('PUT', f'/config/{SUBJECT}', json={'compatibility': 'BACKWARD'})
    for version in (1, 2):
        body = {'schemaType': 'AVRO', 'schema': (SCHEMAS / f'warehouse-event-v{version}.avsc').read_text()}
        if version == 2:
            result = codec.request('POST', f'/compatibility/subjects/{SUBJECT}/versions/latest', json=body)
            if not result['is_compatible']:
                raise RuntimeError('Схема V2 несовместима с предыдущей')
        result = codec.request('POST', f'/subjects/{SUBJECT}/versions', json=body)
        print(f'Avro V{version}: schema id={result["id"]}', flush=True)
    store = Store(keyspace=None)
    try:
        for path in sorted((Path(__file__).resolve().parent.parent / 'migrations').glob('*.cql')):
            if path.name != '001_initial.cql':
                applied = store.query('SELECT version FROM warehouse.schema_migrations WHERE version = %s', (path.name,)).one()
                if applied:
                    continue
            for statement in path.read_text().split(';'):
                if not statement.strip():
                    continue
                try:
                    store.session.execute(statement)
                except InvalidRequest as exc:
                    if not (statement.strip().startswith('ALTER TABLE') and any(text in str(exc) for text in ('already exists', 'conflicts with an existing column'))):
                        raise
            store.query('INSERT INTO warehouse.schema_migrations (version) VALUES (%s)', (path.name,))
            print(f'Миграция применена: {path.name}', flush=True)
        for zone in ('ZONE-A', 'ZONE-B', 'ZONE-C'):
            store.query('INSERT INTO warehouse.zones (zone_id, capacity, occupied, last_event_at) '
                        'VALUES (%s,%s,%s,%s) IF NOT EXISTS', (zone, 1_000_000, 0, -1))
    finally:
        store.close()
    print('Инфраструктура готова', flush=True)


if __name__ == '__main__':
    for attempt in range(30):
        try:
            main()
            break
        except Exception as exc:
            print(f'Инициализация пока не завершена ({attempt + 1}/30): {exc}', flush=True)
            if attempt == 29:
                raise
            time.sleep(5)
