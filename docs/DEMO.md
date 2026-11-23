# Демонстрация Smart Warehouse

## Самый быстрый способ

```bash
cd /home/gsluzh/soa-hw6
docker compose up -d --build
python3 scripts/demo.py --full
```

Скрипт сам дождётся готовности WMS и consumer. Каждый прогон получает собственный суффикс SKU, поэтому старые данные не мешают проверке. Полный сценарий временно останавливает одну ноду Cassandra и consumer; после проверки они запускаются обратно. Обычно показ занимает несколько минут после готовности стенда.

Для показа без остановок:

```bash
python3 scripts/demo.py
```

Результат запуска можно сохранить:

```bash
python3 -u scripts/demo.py --full | tee artifacts/demo-full.txt
```

Для записи с сохранением времени вывода есть `python3 scripts/record_demo.py --full`. Он создаёт текстовый протокол и `artifacts/demo.cast` в формате asciicast v2. Запись можно воспроизвести командой `asciinema play artifacts/demo.cast`, если установлен проигрыватель asciinema.

Файл `artifacts/demo-full.txt` в проекте содержит результат выполненной демонстрации. Вывод `artifacts/unit-tests.txt` относится к модульным тестам. Эти файлы удобно открыть рядом с кодом, но на защите лучше запустить сценарий снова.

## Что открыть перед показом

В одном терминале оставить `docker compose logs -f consumer`. Во втором запускать скрипт или запросы. В браузере открыть:

- [Swagger](http://localhost:8080/docs);
- [Grafana](http://localhost:3000/d/smart-warehouse), `admin` / `warehouse-demo`;
- [Prometheus Alerts](http://localhost:9090/alerts).

Проверить состояние кластера:

```bash
docker exec cassandra-1 nodetool status
```

Должны быть три строки `UN`. Отдельно показать `curl http://localhost:8001/health`: ожидается HTTP 200 и три значения `true`.

## 1. Базовый складской цикл

В `examples/requests.http` есть последовательность всех восьми событий. Её можно выполнять через HTTP-клиент редактора или переносить JSON в Swagger. Для ручного показа используются `SKU-001` и `ORDER-001`; они должны быть новыми либо уже находиться в ожидаемом состоянии от этой же последовательности.

Начать с приёмки:

```bash
curl -sS -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"receive-001","event_type":"PRODUCT_RECEIVED","timestamp":"2026-04-01T12:00:00Z","product_id":"SKU-001","zone_id":"ZONE-A","quantity":100}'
```

Запомнить возвращённый offset и сопоставить его с записью в логе. Дождаться `GET /events/receive-001` со статусом `APPLIED`, затем проверить Cassandra:

```bash
docker exec cassandra-1 cqlsh -e "CONSISTENCY QUORUM; SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001'; SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-001';"
```

Продолжить запросами из `requests.http`:

| Действие | Ожидаемый результат в зоне A |
|---|---|
| Приёмка 100 | доступно 100, резерв 0 |
| Резерв 30 | доступно 70, резерв 30 |
| Перемещение 20 в B | доступно 50, резерв 30; в B доступно 20 |
| Отгрузка 10 | доступно 40, резерв 30 |
| Создание заказа на 15 | доступно 25, резерв 45; заказ CREATED |
| Завершение заказа | доступно 25, резерв 30; заказ COMPLETED |
| Снятие ручного резерва 5 | доступно 30, резерв 25 |
| Инвентаризация до 28 | доступно 28, резерв 25 |

Объяснить, почему завершение заказа не уменьшает доступный остаток: товар был вычтен из него на шаге резервирования.

## 2. Идемпотентность

Повторить первый запрос с теми же `event_id`, timestamp и остальными полями. Не генерировать новый timestamp для того же события: это уже другое содержимое с конфликтующим идентификатором.

После обработки в логе будет `outcome=DUPLICATE`, остаток не увеличится. В автоматическом показе для этого используется отдельный товар `SKU-002-...`: он остаётся равным 50 после двух публикаций.

Показать строку `processed_events`: в ней есть исходная позиция Kafka и рассчитанный план. Если поменять количество, сохранив `event_id`, получится `EVENT_ID_CONFLICT` в DLQ.

## 3. Согласованность таблиц

После отдельной приёмки 100 проверить три представления:

```sql
SELECT * FROM warehouse.inventory_by_product_zone
WHERE product_id='SKU-003' AND zone_id='ZONE-A';

SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-003';

SELECT * FROM warehouse.inventory_by_zone
WHERE zone_id='ZONE-A' AND product_id='SKU-003';
```

Две зональные строки должны содержать `available_quantity=100`, агрегат — `total_available=100`. В автоматической демонстрации эта проверка выполняется после каждого изменения склада, а не только один раз.

Открыть `Store.apply_plan` в `warehouse/store.py`: все проекции входят в один `BatchStatement` с `BatchType.LOGGED`. Отдельно пояснить, что batch не даёт изоляцию чтения между разными партициями.

## 4. События вне порядка

На новом товаре отправить три события:

```text
PRODUCT_RECEIVED, quantity=100, timestamp=2026-04-01T12:00:00Z
PRODUCT_SHIPPED,  quantity=20,  timestamp=2026-04-01T12:05:00Z
PRODUCT_RECEIVED, quantity=50,  timestamp=2026-04-01T12:02:00Z
```

После второго события доступно 80. После третьего остаётся 80, в истории — `STALE`. Это проверка времени сущности, а не сравнение с текущим временем сервера. Поэтому исторические даты в примере допустимы.

## 5. DLQ

Отправить `PRODUCT_SHIPPED` с `quantity=-5`. Показать содержимое DLQ:

```bash
docker compose exec -T wms python -m scripts.read_dlq --event-id bad-shipment
```

В записи должны быть исходные поля, причина ошибки, код `VALIDATION_ERROR`, timestamp ошибки, partition, offset и исходные байты. Затем отправить корректную приёмку и убедиться, что consumer продолжил работу.

Для просмотра всех записей убрать `--event-id`. Утилита читает снимок DLQ до текущего HEAD и завершает работу; она не меняет позицию основного consumer.

## 6. Отказ одной ноды

На новом `SKU-006` принять 200 единиц. Остановить `cassandra-2`:

```bash
docker stop cassandra-2
docker exec cassandra-1 nodetool status
```

Дождаться `DN`, затем отгрузить 50 единиц. При чтении с QUORUM получится 150. Проверить уровни согласованности:

```bash
docker exec cassandra-1 cqlsh -e "CONSISTENCY ONE; SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-006';"
docker exec cassandra-1 cqlsh -e "CONSISTENCY QUORUM; SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-006';"
docker exec cassandra-1 cqlsh -e "CONSISTENCY ALL; SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-006';"
```

ONE и QUORUM выполнятся. ALL вернёт `Unavailable`: при RF=3 ему нужны все три реплики. Вернуть ноду:

```bash
docker start cassandra-2
docker exec cassandra-1 nodetool status
```

Автоматический вариант делает эти шаги самостоятельно и печатает состояние кластера до и после отказа.

## 7. Мониторинг и lag

Показать `/metrics`, затем панели Grafana. После базового сценария на endpoint уже есть счётчики разных типов событий и histogram времени обработки.

Остановить consumer и опубликовать 12 событий. В полном скрипте это выполняется автоматически. В Grafana lag должен вырасти до 12. Показать, что данные приходят из отдельного `lag-exporter`, хотя сам consumer выключен.

Через 20 секунд превышения порога в Prometheus правило `WarehouseConsumerLagHigh` станет `firing`. Реальное ожидание может быть немного длиннее из-за интервала опроса. После запуска consumer lag уменьшается до нуля:

```bash
docker compose start consumer
```

В скрипте проверяется именно состояние `firing` через API Prometheus, а не наличие текста правила в конфигурации.

## 8. Schema Evolution

Отправить две приёмки на разные SKU:

```bash
curl -sS -X POST 'http://localhost:8080/events?schema_version=1' \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-V1","zone_id":"ZONE-A","quantity":11}'

curl -sS -X POST 'http://localhost:8080/events?schema_version=2' \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"PRODUCT_RECEIVED","product_id":"SKU-V2","zone_id":"ZONE-A","quantity":12,"supplier_id":"SUP-001"}'
```

В Cassandra у V1 `supplier_id=null`, у V2 `SUP-001`. Затем показать:

```bash
curl -sS http://localhost:8081/subjects/warehouse-events-value/versions
curl -sS http://localhost:8081/subjects/warehouse-events-value/versions/1
curl -sS http://localhost:8081/subjects/warehouse-events-value/versions/2
```

В ответах видны обе версии одного record. В коде `warehouse/codec.py` показать явные ветки V1/V2 и чтение с reader schema V2.

## Дополнительно: авария между Cassandra и commit

Последняя проверка `--full` останавливает основной consumer и запускает временный с `FAIL_AFTER_WRITE_EVENT_ID`. Он принимает 50 единиц, записывает их в Cassandra и аварийно завершается с кодом 86 до подтверждения Kafka.

Затем запускается основной consumer. Он получает то же сообщение, применяет сохранённый план и подтверждает offset. Остаток остаётся 50. Этот пример показывает, зачем кроме таблицы идентификаторов нужен план абсолютных записей.

## Дополнительно: потеря quorum и HTTP 503

```bash
python3 scripts/quorum_outage.py
```

Этот сценарий временно останавливает две ноды. Он проверяет HTTP 503, публикацию корректного события в Kafka, отсутствие его в DLQ и накопление lag. После запуска нод событие применяется один раз: остаток меняется с 20 на 15, health возвращает HTTP 200. Результат выполненной проверки — `artifacts/quorum-outage.txt`.

## Если запуск не завершился

```bash
docker compose ps -a
docker compose logs --tail=80 init consumer
docker exec cassandra-1 nodetool status
```

При первом запуске дольше всего инициализируется Cassandra. Если на компьютере мало памяти, нужно дать Docker больше ресурсов. Если порт занят, следует изменить соответствующее отображение в Compose и адреса в демонстрационном скрипте. Для перезапуска сервисов с сохранением склада достаточно `docker compose down` и `docker compose up -d`.
