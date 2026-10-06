"""Native PostgreSQL tests. Run via run-transaction-sql.py, never production."""
import logging
import os
from pathlib import Path
import re
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import psycopg2
from psycopg2.extensions import parse_dsn

from pg_chameleon.lib.pg_lib import pg_engine

SID = '00000000-0000-0000-0000-000000000001'
CONFIG = {'transactional_capture': True, 'gtid_enable': True, 'on_error_read': 'exit',
          'on_error_replay': 'exit', 'auto_maintenance': 'disabled',
          'schema_mappings': {'source': 'public'}, 'limit_tables': [], 'skip_tables': []}


def make_engine(dsn):
    engine = pg_engine()
    engine.logger = logging.getLogger('transaction-tests')
    engine.sources = {'fixture': dict(CONFIG)}
    engine.source = 'fixture'
    engine.type_override = None
    engine.pgsql_conn = psycopg2.connect(dsn)
    engine.pgsql_conn.autocommit = True
    engine.pgsql_cur = engine.pgsql_conn.cursor()
    engine.dest_conn = {'database': parse_dsn(dsn)['dbname']}
    return engine


class PublicationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(os.environ['CDC_TEST_DSN_FILE'])
        if path.stat().st_mode & 0o077:
            raise ValueError('DSN file must be private')
        cls.dsn = path.read_text().strip()
        if not re.fullmatch(r'betark_cdc_gate_[a-z0-9_]+', parse_dsn(cls.dsn).get('dbname', '')):
            raise ValueError('Refusing non-fixture database')
        cls.engine = make_engine(cls.dsn)
        cur = cls.engine.pgsql_cur
        cur.execute('SELECT current_user = pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()')
        if cur.fetchone() != (True,):
            raise ValueError('Fixture owner required')
        cur.execute('SELECT purpose FROM public.betark_fixture_marker')
        if cur.fetchone() != ('transactional-cdc-tests',):
            raise ValueError('Fixture marker required')
        cls.engine.create_replica_schema()
        cur.execute("INSERT INTO sch_chameleon.t_sources(t_source,jsb_schema_mappings,enm_source_type,v_log_table) "
                    "VALUES ('fixture','{\"source\":\"public\"}','mysql',ARRAY['t_log_replica','t_log_replica']) RETURNING i_id_source")
        cls.engine.i_id_source = cur.fetchone()[0]
        cls.engine.insert_source_timings()

    def setUp(self):
        cur = self.engine.pgsql_cur
        cur.execute('TRUNCATE sch_chameleon.t_replica_batch CASCADE')
        cur.execute('DELETE FROM sch_chameleon.t_transaction_state')
        self.engine.bootstrap_transaction_state(SID, {'File': 'mysql-bin.000001', 'Position': 4,
                                                      'Executed_Gtid_Set': SID + ':1-3:7'})

    def transaction(self, number=9, rows=2):
        return {'gtid': SID + ':' + str(number),
                'commit': {'File': 'mysql-bin.000001', 'Position': number * 100, 'Time': 1},
                'rows': [{'global_data': {'schema': 'public', 'table': 'items', 'action': 'insert',
                                         'binlog': 'mysql-bin.000001', 'logpos': number * 100 - 1, 'event_time': 1},
                          'event_after': {'id': i}, 'event_before': {}} for i in range(rows)]}

    def queue(self):
        cur = self.engine.pgsql_cur
        cur.execute('SELECT i_id_batch,i_id_event FROM sch_chameleon.t_batch_events ORDER BY i_id_batch')
        return cur.fetchall()

    def test_atomic_publish_and_duplicate(self):
        tx = self.transaction(rows=1001)
        self.assertTrue(self.engine.publish_transaction(SID, tx))
        self.assertFalse(self.engine.publish_transaction(SID, tx))
        queue = self.queue()
        self.assertEqual(len(queue), 1)
        self.assertEqual(len(queue[0][1]), 1001)
        self.assertEqual(queue[0][1], sorted(queue[0][1]))
        self.assertEqual(self.engine.get_transaction_state()['capture_gtid_set'], SID + ':1-3:7:9')
        self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7')

    def test_failure_does_not_advance(self):
        before = self.engine.get_transaction_state()
        with mock.patch.object(self.engine, 'write_batch', side_effect=RuntimeError('synthetic publication failure')):
            with self.assertRaises(RuntimeError):
                self.engine.publish_transaction(SID, self.transaction())
        self.assertEqual(self.engine.get_transaction_state(), before)
        self.assertEqual(self.queue(), [])

    def test_invalid_json_nul_never_discarded(self):
        tx = self.transaction()
        tx['rows'][-1]['event_after']['bad'] = '\x00'
        before = self.engine.get_transaction_state()
        with self.assertRaises(psycopg2.Error) as error:
            self.engine.publish_transaction(SID, tx)
        self.assertEqual(error.exception.pgcode, '22P05')
        self.assertEqual(self.engine.get_transaction_state(), before)
        self.assertEqual(self.queue(), [])
        self.engine.pgsql_cur.execute('SELECT count(*) FROM sch_chameleon.t_discarded_rows')
        self.assertEqual(self.engine.pgsql_cur.fetchone(), (0,))

    def test_empty_transaction_is_ready_not_applied(self):
        self.assertTrue(self.engine.publish_transaction(SID, self.transaction(rows=0)))
        self.assertEqual(self.queue()[0][1], [])
        self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7')

    def test_uuid_and_selection_change_fail_closed(self):
        with self.assertRaises(ValueError):
            self.engine.publish_transaction('00000000-0000-0000-0000-000000000002', self.transaction())
        self.engine.sources['fixture']['skip_tables'] = ['source.items']
        try:
            with self.assertRaises(ValueError):
                self.engine.get_transaction_state()
        finally:
            self.engine.sources['fixture']['skip_tables'] = []
        self.assertEqual(self.queue(), [])

    def test_bootstrap_never_overwrites_frontier(self):
        self.engine.publish_transaction(SID, self.transaction())
        before = self.engine.get_transaction_state()
        with self.assertRaises(ValueError):
            self.engine.bootstrap_transaction_state(SID, {'File': 'mysql-bin.999999', 'Position': 4,
                                                          'Executed_Gtid_Set': SID + ':1-100'})
        self.assertEqual(self.engine.get_transaction_state(), before)

    def test_existing_frontier_blocks_initial_load(self):
        with self.assertRaises(ValueError):
            self.engine.assert_new_transaction_source()

    def test_two_publishers_deliver_once(self):
        def publish():
            engine = make_engine(self.dsn)
            engine.i_id_source = self.engine.i_id_source
            try:
                return engine.publish_transaction(SID, self.transaction())
            finally:
                engine.disconnect_db()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: publish(), range(2)))
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(len(self.queue()), 1)

    def test_identical_timestamps_keep_event_id_order(self):
        self.engine.publish_transaction(SID, self.transaction())
        self.engine.publish_transaction(SID, self.transaction(number=10))
        self.engine.pgsql_cur.execute("UPDATE sch_chameleon.t_replica_batch SET ts_created='2000-01-01'")
        queue = self.queue()
        self.assertEqual(len(queue), 2)
        self.assertLess(max(queue[0][1]), min(queue[1][1]))

    def test_legacy_catalogue_is_not_silently_upgraded(self):
        self.engine.pgsql_cur.execute('ALTER VIEW sch_chameleon.v_betark_transactional_version RENAME TO v_feature_hidden')
        try:
            self.engine.create_replica_schema()
            with self.assertRaises(psycopg2.Error):
                self.engine.get_transaction_state()
        finally:
            self.engine.pgsql_cur.execute('ALTER VIEW sch_chameleon.v_feature_hidden RENAME TO v_betark_transactional_version')


if __name__ == '__main__':
    unittest.main()
