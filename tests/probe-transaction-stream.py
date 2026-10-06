"""Read-only synthetic-fixture binlog diagnostics, never source/target row data."""
import json
import logging
import os
from pathlib import Path
import sys

import psycopg2
import yaml
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent, HeartbeatLogEvent, QueryEvent, RotateEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from pg_chameleon.lib.mysql_lib import mysql_source
from pg_chameleon.lib.pg_lib import pg_engine


def main():
    path = Path(sys.argv[1])
    if path.stat().st_mode & 0o077:
        raise ValueError('Private fixture config required')
    config = yaml.safe_load(path.read_text())
    source_config = config['sources']['mysql']
    if source_config['db_conn']['host'] != '127.0.0.1':
        raise ValueError('Only loopback synthetic source allowed')
    pg = config['pg_conn']
    if not pg['database'].startswith('betark_cdc_gate_') or pg['user'] != pg['database']:
        raise ValueError('Only isolated fixture target allowed')
    engine = pg_engine()
    engine.sources, engine.source, engine.type_override = config['sources'], 'mysql', config['type_override']
    engine.logger = logging.getLogger('probe')
    engine.dest_conn = pg
    engine.pgsql_conn = psycopg2.connect(host=pg['host'], port=pg['port'], dbname=pg['database'],
                                        user=pg['user'], password=pg['password'])
    engine.pgsql_conn.autocommit = True
    engine.pgsql_cur = engine.pgsql_conn.cursor()
    source = mysql_source()
    source.sources, source.source, source.pg_engine = config['sources'], 'mysql', engine
    source.logger = engine.logger
    source.tables, source.net_read_timeout = '*', source_config['net_read_timeout']
    source._mysql_source__init_read_replica()
    try:
        state = engine.get_transaction_state()
        stream = BinLogStreamReader(connection_settings=source.replica_conn, server_id=source.my_server_id,
            only_events=[GtidEvent, HeartbeatLogEvent, QueryEvent, RotateEvent, XidEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
            log_file=state['capture_file'], log_pos=state['capture_position'], auto_position=state['capture_gtid_set'],
            resume_stream=True, only_schemas=source.schema_replica, slave_heartbeat=source.sleep_loop)
        headers = []
        try:
            for event in stream:
                headers.append({'event': type(event).__name__, 'position': event.packet.log_pos,
                                'rows': len(event.rows) if isinstance(event, (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)) else 0})
                if len(headers) >= 40:
                    break
        finally:
            stream.close()
        transactions = list(source._mysql_source__read_transaction_stream(state))
        print(json.dumps({'headers': headers, 'complete_transactions': len(transactions),
                          'selected_rows': sum(len(tx['rows']) for tx in transactions)}))
    finally:
        source.disconnect_db_buffered()
        engine.disconnect_db()


if __name__ == '__main__':
    main()
