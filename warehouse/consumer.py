import base64
import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from warehouse.codec import AvroCodec
from warehouse.config import BOOTSTRAP, GROUP, TOPIC, DLQ
from warehouse.domain import InvalidEvent
from warehouse.kafka_io import Publisher
from warehouse.lag_exporter import LagProbe
from warehouse.metrics import PROCESSED, RESULTS, DURATION, WRITE_ERRORS, DEPENDENCY
from warehouse.store import Store

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('warehouse.consumer')
STOP = threading.Event()
HEALTH = {'kafka': False, 'cassandra': False, 'worker': False}


def b64(value):
    return base64.b64encode(value).decode() if value is not None else None


def dead_letter(message, original, error):
    return {
        'original_event': original,
        'original_bytes_base64': b64(message.value()),
        'error_reason': str(error),
        'error_code': getattr(error, 'code', 'VALIDATION_ERROR'),
        'failed_at': datetime.now(timezone.utc).isoformat(),
        'kafka_metadata': {
            'topic': message.topic(), 'partition': message.partition(), 'offset': message.offset(),
            'timestamp': message.timestamp()[1], 'key_base64': b64(message.key()),
            'headers': [(k, b64(v)) for k, v in (message.headers() or [])],
        },
    }


def handle(message, codec, store, publisher, commit):
    original = None
    event = {}
    try:
        event, original, version = codec.decode(message.value())
        # Для воспроизводимой проверки окна «Cassandra записала, Kafka ещё не подтверждена».
        def crash_after_write():
            target = os.getenv('FAIL_AFTER_WRITE_EVENT_ID')
            marker = '/tmp/warehouse-crash-once'
            if target and event['event_id'] == target and not os.path.exists(marker):
                with open(marker, 'w') as f:
                    f.write(target)
                os._exit(86)

        outcome = store.process(event, message.partition(), message.offset(), after_write=crash_after_write)
    except InvalidEvent as exc:
        publisher.send(DLQ, f'{message.topic()}:{message.partition()}:{message.offset()}',
                       json.dumps(dead_letter(message, original, exc), ensure_ascii=False).encode())
        outcome, version = 'DLQ', None
        log.warning('DLQ event_id=%s reason=%s', event.get('event_id'), exc)
    # Любая инфраструктурная ошибка выше прерывает вызов до commit.
    commit(message)
    RESULTS.labels(outcome).inc()
    if outcome == 'APPLIED':
        PROCESSED.labels(event['event_type']).inc()
    log.info('event_id=%s event_type=%s partition=%s offset=%s outcome=%s schema_version=%s',
             event.get('event_id'), event.get('event_type'), message.partition(), message.offset(), outcome, version)


def consume():
    codec, publisher = AvroCodec(), Publisher()
    while not STOP.is_set():
        client = store = None
        try:
            store = Store()
            store.on_write_error = WRITE_ERRORS.inc
            client = Consumer({
                'bootstrap.servers': BOOTSTRAP, 'group.id': GROUP,
                'enable.auto.commit': False, 'enable.auto.offset.store': False,
                'auto.offset.reset': 'earliest', 'max.poll.interval.ms': 300000,
                'session.timeout.ms': 10000,
            })
            metadata = client.list_topics(TOPIC, timeout=10)
            if len(metadata.topics[TOPIC].partitions) != 1:
                raise RuntimeError('Нужна одна партиция для одного последовательного обработчика склада')
            client.subscribe([TOPIC])
            HEALTH['worker'] = True
            while not STOP.is_set():
                message = client.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                with DURATION.time():
                    handle(message, codec, store, publisher,
                           lambda msg: client.commit(message=msg, asynchronous=False))
        except Exception:
            # Offset остаётся прежним. Переподключение повторит именно незавершённую запись.
            HEALTH['worker'] = False
            log.exception('Ошибка инфраструктуры; повторное подключение через 3 секунды, offset не подтверждён')
            STOP.wait(3)
        finally:
            if client:
                client.close()
            if store:
                store.close()
    HEALTH['worker'] = False


def monitor():
    probe = LagProbe()
    store = None
    while not STOP.is_set():
        for name in ('kafka', 'cassandra'):
            try:
                if name == 'kafka':
                    probe.sample()
                else:
                    if store is None:
                        store = Store()
                    store.health()
                HEALTH[name] = True
            except Exception:
                HEALTH[name] = False
                if name == 'cassandra' and store:
                    store.close()
                    store = None
            DEPENDENCY.labels(name).set(int(HEALTH[name]))
        STOP.wait(3)
    probe.close()
    if store:
        store.close()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            code, content_type, body = 200, CONTENT_TYPE_LATEST, generate_latest()
        elif self.path in ('/health', '/live'):
            healthy = all(HEALTH.values()) if self.path == '/health' else HEALTH['worker']
            code = 200 if healthy else 503
            content_type, body = 'application/json', json.dumps(HEALTH).encode()
        else:
            code, content_type, body = 404, 'text/plain', b'Not found'
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    server = ThreadingHTTPServer(('0.0.0.0', 8001), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    consume()
    watcher.join(timeout=15)
    server.shutdown()


if __name__ == '__main__':
    main()
