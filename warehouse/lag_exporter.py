import logging
import time

from confluent_kafka import Consumer, TopicPartition
from prometheus_client import start_http_server

from warehouse.config import BOOTSTRAP, GROUP, TOPIC
from warehouse.metrics import LAG, COMMITTED, HEAD, DEPENDENCY


class LagProbe:
    def __init__(self):
        # Этот клиент не вызывает subscribe/assign и не вступает в consumer group.
        self.client = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': GROUP,
                                'enable.auto.commit': False, 'enable.auto.offset.store': False,
                                'socket.timeout.ms': 5000})

    def sample(self):
        metadata = self.client.list_topics(TOPIC, timeout=4)
        topic = metadata.topics[TOPIC]
        if topic.error:
            raise RuntimeError(str(topic.error))
        partitions = [TopicPartition(TOPIC, p) for p in topic.partitions]
        committed = self.client.committed(partitions, timeout=4)
        for part in committed:
            low, high = self.client.get_watermark_offsets(part, timeout=4, cached=False)
            offset = part.offset if part.offset >= 0 else low
            COMMITTED.labels(TOPIC, str(part.partition), GROUP).set(offset)
            HEAD.labels(TOPIC, str(part.partition), GROUP).set(high)
            LAG.labels(TOPIC, str(part.partition), GROUP).set(max(0, high - offset))

    def close(self):
        self.client.close()


def main():
    start_http_server(8002)
    probe = LagProbe()
    while True:
        try:
            probe.sample()
            DEPENDENCY.labels('kafka').set(1)
        except Exception:
            DEPENDENCY.labels('kafka').set(0)
            LAG.clear()
            COMMITTED.clear()
            HEAD.clear()
            logging.exception('Не удалось измерить lag')
        time.sleep(5)


if __name__ == '__main__':
    main()
