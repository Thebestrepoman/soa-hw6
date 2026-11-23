import io
import json
import struct
from pathlib import Path

import requests
from fastavro import parse_schema, schemaless_reader, schemaless_writer

from warehouse.config import REGISTRY, SUBJECT
from warehouse.domain import InvalidEvent

SCHEMAS = Path(__file__).resolve().parent.parent / 'schemas'


class RegistryUnavailable(RuntimeError):
    pass


class AvroCodec:
    def __init__(self):
        self.versions = {}
        self.writers = {}
        self.reader = parse_schema(json.loads((SCHEMAS / 'warehouse-event-v2.avsc').read_text()))

    def request(self, method, path, **kwargs):
        try:
            response = requests.request(method, REGISTRY + path, timeout=5, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            raise RegistryUnavailable(str(exc)) from exc

    def version(self, version):
        if version not in (1, 2):
            raise InvalidEvent('Поддерживаются schema_version=1 и schema_version=2')
        if version not in self.versions:
            data = self.request('GET', f'/subjects/{SUBJECT}/versions/{version}')
            schema = json.loads(data['schema'])
            parsed = parse_schema(schema)
            self.versions[version] = (data['id'], parsed)
            self.writers[data['id']] = (version, parsed)
        return self.versions[version]

    def encode(self, event, version):
        schema_id, schema = self.version(version)
        buffer = io.BytesIO()
        buffer.write(struct.pack('>bI', 0, schema_id))
        schemaless_writer(buffer, schema, event)
        return buffer.getvalue()

    def decode(self, payload):
        if not payload or len(payload) < 6 or payload[0] != 0:
            raise InvalidEvent('Сообщение не соответствует Confluent Avro wire format')
        schema_id = struct.unpack('>I', payload[1:5])[0]
        # Версии узнаём по регистрации в subject, не по предположению id == version.
        for version in (1, 2):
            self.version(version)
        if schema_id not in self.writers:
            raise InvalidEvent(f'Неизвестная схема {schema_id} для {SUBJECT}')
        version, writer = self.writers[schema_id]
        try:
            original = schemaless_reader(io.BytesIO(payload[5:]), writer)
            event = schemaless_reader(io.BytesIO(payload[5:]), writer, self.reader)
        except Exception as exc:
            raise InvalidEvent(f'Повреждено Avro-сообщение: {exc}') from exc
        if version == 1:
            event['supplier_id'] = None
        elif version == 2:
            event['supplier_id'] = original.get('supplier_id')
        return event, original, version
