import threading

from confluent_kafka import Producer

from warehouse.config import BOOTSTRAP


class Publisher:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': BOOTSTRAP, 'enable.idempotence': True,
                                  'acks': 'all', 'delivery.timeout.ms': 15000})
        self.lock = threading.Lock()

    def send(self, topic, key, value):
        result = {}

        def delivered(error, message):
            result['error'] = error
            result['message'] = message

        with self.lock:
            self.producer.produce(topic, key=key, value=value, on_delivery=delivered)
            remaining = self.producer.flush(20)
            if remaining or not result or result['error']:
                raise RuntimeError(f'Kafka не подтвердила публикацию: {result.get("error")}')
        message = result['message']
        return {'topic': message.topic(), 'partition': message.partition(), 'offset': message.offset()}
