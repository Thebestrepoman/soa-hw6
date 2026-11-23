import argparse
import json
import time
from uuid import uuid4
from confluent_kafka import Consumer, TopicPartition
from warehouse.config import BOOTSTRAP, DLQ


def read(event_id=None):
    client = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': f'dlq-inspect-{uuid4()}',
                       'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    try:
        low, high = client.get_watermark_offsets(TopicPartition(DLQ, 0), timeout=10)
        if low == high:
            return []
        client.assign([TopicPartition(DLQ, 0, low)])
        found = []
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            value = json.loads(message.value())
            if event_id is None or (value.get('original_event') or {}).get('event_id') == event_id:
                found.append(value)
            if message.offset() + 1 >= high:
                break
        return found
    finally:
        client.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--event-id')
    args = parser.parse_args()
    print(json.dumps(read(args.event_id), ensure_ascii=False, indent=2))
