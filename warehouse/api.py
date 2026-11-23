from contextlib import asynccontextmanager
from datetime import datetime, timezone
from threading import Lock
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from warehouse.codec import AvroCodec
from warehouse.config import TOPIC
from warehouse.kafka_io import Publisher
from warehouse.store import Store


class Item(BaseModel):
    model_config = ConfigDict(extra='forbid')
    product_id: str
    zone_id: str
    quantity: int


class Event(BaseModel):
    model_config = ConfigDict(extra='forbid')
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    product_id: str | None = None
    zone_id: str | None = None
    from_zone_id: str | None = None
    to_zone_id: str | None = None
    order_id: str | None = None
    quantity: int | None = None
    counted_quantity: int | None = None
    items: list[Item] = Field(default_factory=list, max_length=10)
    supplier_id: str | None = None


@asynccontextmanager
async def lifespan(app):
    app.state.store = Store()
    app.state.publisher = Publisher()
    app.state.codec = AvroCodec()
    app.state.lock = Lock()
    yield
    app.state.store.close()


app = FastAPI(title='Smart Warehouse — WMS', version='2.0', lifespan=lifespan,
              description='Приём складских событий и чтение состояния из Cassandra. POST возвращает подтверждение Kafka.')


@app.get('/health')
def health():
    try:
        app.state.store.health()
        app.state.publisher.producer.list_topics(TOPIC, timeout=3)
        return {'status': 'ok'}
    except Exception as exc:
        return JSONResponse(status_code=503, content={'status': 'unavailable', 'reason': str(exc)})


@app.post('/events', status_code=202)
def publish(event: Event, schema_version: int = Query(2, ge=1, le=2)):
    if schema_version == 1 and event.supplier_id is not None:
        raise HTTPException(422, 'В схеме V1 нет supplier_id')
    try:
        payload = event.model_dump()
        with app.state.lock:
            data = app.state.codec.encode(payload, schema_version)
        metadata = app.state.publisher.send(TOPIC, 'warehouse-main', data)
        return {'event_id': event.event_id, 'schema_version': schema_version, **metadata}
    except (ValueError, TypeError, OverflowError) as exc:
        raise HTTPException(422, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(503, str(exc)) from exc


@app.get('/inventory/products/{product_id}')
def inventory(product_id: str):
    store = app.state.store
    return {'product_id': product_id, 'total': store.total(product_id), 'zones': store.product(product_id)}


@app.get('/inventory/zones/{zone_id}')
def inventory_zone(zone_id: str):
    return {'zone_id': zone_id, 'products': app.state.store.zone(zone_id)}


@app.get('/zones/{zone_id}')
def zone(zone_id: str):
    row = app.state.store.query('SELECT * FROM zones WHERE zone_id = %s', (zone_id,)).one()
    if not row:
        raise HTTPException(404, 'Зона не найдена')
    return row


@app.get('/orders/{order_id}')
def order(order_id: str):
    result = app.state.store.order(order_id)
    if not result:
        raise HTTPException(404, 'Заказ не найден')
    return result


@app.get('/events/{event_id}')
def event_status(event_id: str):
    row = app.state.store.processed(event_id)
    if not row:
        raise HTTPException(404, 'Событие ещё не обработано или отправлено в DLQ')
    return {k: v for k, v in row.items() if k not in ('plan_json', 'payload_hash')}
