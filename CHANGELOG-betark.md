# Изменения Betark

## 2026-10-07 — packaging candidate 2.0.21+betark.1

- Distribution version отделена от base catalogue 2.0.10/feature 1.
- Wheel включает packaged feature SQL и исходный BSD LICENSE.txt.
- Явно сохранена dependency setuptools<81 для существующего pkg_resources;
  остальные upstream dependencies, включая Sphinx, не удалялись.
- Build/runtime dependency locks фиксируют реальные версии/SHA256.
- Native SQL runner использует -I: checkout не подменяет установленный wheel.
  Wheel admission дополнительно отвергает import из source checkout.
- Build/wheel native/crash gate, publication/download и full/HA admission
  пока не выполнены; номер версии не является подтверждением release.

## 2026-10-07 — crash/restart и потеря commit response (candidate)

- Сохранены реальные PG16 тесты остановки только собственного worker до/после
  COPY, после queue, до commit и после commit. Restart публикует ровно один batch.
- Capture и replay lost-ACK вызывают настоящий commit, затем закрывают connection:
  durable GTID/frontier предотвращают повтор, в том числе после batch retention.
- READ COMMITTED observer не видит частичные DML/checkpoint; termination точного
  fixture backend на строке 1001 откатывает все строки и сохраняет очередь.
- Реальный двухсторонний PostgreSQL deadlock не подтверждает/не пропускает
  неуспешную транзакцию. Retry не требует удаления queue или правки позиции.
- 42 synthetic/native tests PASS. Fault hooks существуют только в tests,
  production API не изменён. Release/full rehearsal/HA admission ещё не выполнены.

## 2026-10-07 — native Timescale/bytea gate (candidate, не release)

- COPY/CDC сохраняют SQL NULL, empty bytea, NUL/0xff и реальные bytes, включая
  отличие ASCII `0102` от `0x0102`. Literal text `NULL` больше не становится NULL.
- Initial COPY error немедленно останавливает load; fallback/discard запрещён.
- Source ID разрешается до guard/frontier entrypoints; CLI lifecycle проверен.
- Начальный GTID heartbeat не скрывает backlog; non-blocking EOF определяет idle.
- Native replay определяет реальные partition dimensions Timescale и переносит
  FULL images через DELETE ровно одной old row и typed INSERT, атомарно.
  Missing old row и неверное число затронутых строк не подтверждаются.
- Candidate gate на PostgreSQL 16/TimescaleDB 2.14.2 прошёл без test-only SQL
  adapters: девять chunk moves, COPY/CDC binary, rollback/resume, failed move
  retry, failed batch retry, source transaction 1001 rows rollback/retry.
- Fixture-only read-only binlog probe сохранён в tests/probe-transaction-stream.py.
- Crash/HA, hashed wheel/release и полный 203-table rehearsal ещё не допущены.

## 2026-10-07 — атомарный replay source transaction (не release)

- Native replay wrapper переиспользует upstream SQL generator, но применяет
  полный выбранный batch, queue mutations и applied frontier одним commit.
- Ошибка строки 1001 откатывает все предыдущие 1000 строк. Снятие CHECK и
  повтор применяют 1001 строку без manual queue edits; соседний batch не обгоняет.
- Порядок определяется batch/event IDs; SQL join metadata ограничен source ID.
- Empty transactions, два replayer, frontier после batch retention и legacy
  однократный replay budget проверяются native PostgreSQL тестами.
- Отключение transactional mode при существующем frontier запрещено до DML.
- Nine-hypertable/crash/HA admission и release ещё не выполнены.

## 2026-10-07 — capture и атомарная публикация (не release)

- Форк от upstream v2.0.21 / `5458575165565b4593d33af912e7fdbeb4db49f8`.
- Явный `transactional_capture` требует новый каталог, GTID ON, ROW/FULL,
  fail-closed и отключённый auto-maintenance. Legacy mode остаётся default.
- Буфер отдаёт только COMMIT/XID, сохраняет GTID holes/SID, ограничивает
  размер до append. Reader не режет транзакцию по replica_batch_size.
- COPY, готовая очередь, capture frontier и metadata публикуются одним commit;
  strict COPY не удаляет NUL и не использует fallback/discard. Начальный
  frontier берётся только из успешного initial load, повтор bootstrap запрещён.
- JSON before image теперь декодируется из before, не after.
- Повтор initial load блокируется до cleanup/truncate, не после копирования.
- Проверены 14 synthetic capture/reader и 10 native PostgreSQL 16 publication
  тестов, включая 1001 строку, 22P05, два publisher и повтор GTID.
- Upstream tokenizer test на неизменённой baseline имеет 9 старых assertion
  failures (не учитывает дополнительные поля parser); parser не менялся.
- Replay, nine-hypertable gate, crash/HA, wheel/release ещё не допущены.

## Проверки

Из корня форка, после установки editable candidate в отдельный Python 3.11 venv:

```bash
/var/tmp/betark-cdc/candidate/bin/python -m unittest discover -s tests -p 'test_transaction_capture.py' -v
/var/tmp/betark-cdc/candidate/bin/python tests/run-transaction-sql.py
```

SQL runner создаёт только disposable Podman PostgreSQL на случайном loopback
порту, требует отдельную БД `betark_cdc_gate_*`, marker/owner/private DSN;
после проверки удаляет только собственный контейнер. Production не используется.
