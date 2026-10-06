# Изменения Betark

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
