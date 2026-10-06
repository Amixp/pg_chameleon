"""Native PostgreSQL tests. Run via run-transaction-sql.py, never production."""
import logging
import multiprocessing
import os
from pathlib import Path
import re
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import psycopg2
from psycopg2.extensions import parse_dsn

from pg_chameleon.lib.pg_lib import pg_engine

SID = '00000000-0000-0000-0000-000000000001'
CONFIG = {'transactional_capture': True, 'gtid_enable': True, 'on_error_read': 'exit',
          'on_error_replay': 'exit', 'auto_maintenance': 'disabled',
          'schema_mappings': {'source': 'public'}, 'limit_tables': [], 'skip_tables': [], 'replay_max_rows': 1000}


def make_engine(dsn, connection_factory=None):
    engine = pg_engine()
    engine.logger = logging.getLogger('transaction-tests')
    engine.sources = {'fixture': dict(CONFIG)}
    engine.source = 'fixture'
    engine.type_override = None
    engine.pgsql_conn = psycopg2.connect(dsn, connection_factory=connection_factory)
    engine.pgsql_conn.autocommit = True
    engine.pgsql_cur = engine.pgsql_conn.cursor()
    engine.dest_conn = {'database': parse_dsn(dsn)['dbname']}
    return engine


class FaultConnection(psycopg2.extensions.connection):
    """Test-only barrier/lost response, always using the real PostgreSQL commit."""
    phase = None
    pipe = None
    lose_ack = False

    def pause(self, phase):
        if self.phase == phase:
            self.pipe.send(('barrier', phase))
            self.pipe.recv()

    def commit(self):
        self.pause('before_commit')
        super().commit()
        self.pause('after_commit')
        if self.lose_ack:
            self.close()
            raise psycopg2.OperationalError('synthetic lost commit response')


class FaultCursor(psycopg2.extensions.cursor):
    def execute(self, query, vars=None):
        result = super().execute(query, vars)
        if isinstance(query, str) and query.startswith('INSERT INTO sch_chameleon.t_batch_events '):
            self.connection.pause('after_queue')
        return result


def publication_worker(dsn, transaction, phase, pipe):
    engine = make_engine(dsn, FaultConnection)
    engine.pgsql_cur.close()
    engine.pgsql_cur = engine.pgsql_conn.cursor(cursor_factory=FaultCursor)
    engine.pgsql_conn.phase, engine.pgsql_conn.pipe = phase, pipe
    write = engine.write_batch

    def paused_write(*args, **kwargs):
        engine.pgsql_conn.pause('before_copy')
        write(*args, **kwargs)
        engine.pgsql_conn.pause('after_copy')

    with mock.patch.object(engine, 'write_batch', side_effect=paused_write):
        engine.publish_transaction(SID, transaction)
    engine.disconnect_db()


def replay_worker(dsn, source_id, pipe):
    conn = psycopg2.connect(dsn)
    try:
        pipe.send(('backend', conn.get_backend_pid()))
        pipe.recv()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM sch_chameleon.fn_replay_mysql(1000,%s,true)', (source_id,))
            status = cur.fetchone()
        conn.commit()
        pipe.send(('done', status))
    except psycopg2.Error as error:
        pipe.send(('error', error.pgcode))
    finally:
        conn.close()


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
        cur.execute('CREATE TABLE public.items(id integer PRIMARY KEY)')
        cur.execute("INSERT INTO sch_chameleon.t_replica_tables(i_id_source,v_table_name,v_schema_name,v_table_pkey) "
                    "VALUES (%s,'items','public',ARRAY['id'])", (cls.engine.i_id_source,))

    def setUp(self):
        cur = self.engine.pgsql_cur
        cur.execute('TRUNCATE sch_chameleon.t_replica_batch CASCADE')
        cur.execute('TRUNCATE public.items,sch_chameleon.t_error_log')
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

    def test_source_id_is_resolved_on_fresh_cli_engine(self):
        saved = self.engine.i_id_source
        del self.engine.i_id_source
        try:
            with self.assertRaises(ValueError):
                self.engine.assert_new_transaction_source()
            self.assertEqual(self.engine.get_transaction_state()['i_id_source'], saved)
        finally:
            self.engine.i_id_source = saved

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
            with self.assertRaises(psycopg2.Error):
                self.engine.replay_replica()
        finally:
            self.engine.pgsql_cur.execute('ALTER VIEW sch_chameleon.v_feature_hidden RENAME TO v_betark_transactional_version')

    def replay(self, max_rows=1000):
        self.engine.pgsql_cur.execute('SELECT * FROM sch_chameleon.fn_replay_mysql(%s,%s,true)', (max_rows, self.engine.i_id_source))
        return self.engine.pgsql_cur.fetchone()

    def count(self):
        self.engine.pgsql_cur.execute('SELECT count(*) FROM public.items')
        return self.engine.pgsql_cur.fetchone()[0]

    def test_complete_replay_rolls_back_and_retries(self):
        cur = self.engine.pgsql_cur
        cur.execute('ALTER TABLE public.items ADD CONSTRAINT reject_last CHECK(id<>1000)')
        self.engine.publish_transaction(SID, self.transaction(rows=1001))
        next_tx = self.transaction(number=10, rows=1)
        next_tx['rows'][0]['event_after']['id'] = 2000
        self.engine.publish_transaction(SID, next_tx)
        queued = self.queue()
        before = self.engine.get_transaction_state()
        try:
            status = self.replay()
            self.assertEqual(self.count(), 0, 'Partial source transaction was committed')
            self.assertTrue(status[1], 'Replay did not reach failing last row')
            self.assertEqual(self.queue(), queued)
            self.assertEqual(self.engine.get_transaction_state(), before)
        finally:
            cur.execute('ALTER TABLE public.items DROP CONSTRAINT reject_last')
        self.assertFalse(self.replay()[1])
        self.assertEqual(self.count(), 1001)
        self.assertEqual(len(self.queue()), 1)  # Neighbour not replayed by this call.
        self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7:9')
        self.assertFalse(self.replay()[1])
        self.assertEqual(self.count(), 1002)
        self.assertEqual(self.queue(), [])
        self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7:9-10')

    def test_empty_replay_advances_applied_frontier(self):
        self.engine.publish_transaction(SID, self.transaction(rows=0))
        self.assertFalse(self.replay()[1])
        self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7:9')
        self.assertEqual(self.queue(), [])

    def test_applied_gtid_survives_batch_retention(self):
        self.engine.publish_transaction(SID, self.transaction())
        self.assertFalse(self.replay()[1])
        self.engine.pgsql_cur.execute('DELETE FROM sch_chameleon.t_replica_batch WHERE b_replayed')
        self.assertFalse(self.engine.publish_transaction(SID, self.transaction()))
        self.assertEqual(self.count(), 2)
        self.assertEqual(self.queue(), [])
        self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7:9')

    def test_publication_process_crashes_and_restart(self):
        context = multiprocessing.get_context('spawn')
        for phase in ('before_copy', 'after_copy', 'after_queue', 'before_commit', 'after_commit'):
            with self.subTest(phase=phase):
                self.setUp()
                before = self.engine.get_transaction_state()
                tx = self.transaction(rows=1001)
                parent, child = context.Pipe()
                process = context.Process(target=publication_worker, args=(self.dsn, tx, phase, child))
                process.start()
                child.close()
                try:
                    self.assertTrue(parent.poll(15), 'Worker did not reach barrier')
                    self.assertEqual(parent.recv(), ('barrier', phase))
                    if phase == 'after_commit':
                        self.assertEqual(len(self.queue()), 1)
                        self.assertEqual(len(self.queue()[0][1]), 1001)
                    else:
                        self.assertEqual(self.queue(), [])
                        self.assertEqual(self.engine.get_transaction_state(), before)
                    process.kill()  # Only the worker created by this test.
                    process.join(10)
                    self.assertFalse(process.is_alive())
                    restarted = make_engine(self.dsn)
                    try:
                        self.assertEqual(restarted.publish_transaction(SID, tx), phase != 'after_commit')
                        self.assertFalse(restarted.publish_transaction(SID, tx))
                    finally:
                        restarted.disconnect_db()
                    self.assertEqual(len(self.queue()), 1)
                    self.assertEqual(len(self.queue()[0][1]), 1001)
                    self.assertEqual(self.engine.get_transaction_state()['capture_gtid_set'], SID + ':1-3:7:9')
                    self.assertFalse(self.replay()[1])
                    self.assertEqual(self.count(), 1001)
                finally:
                    if process.is_alive():
                        process.kill()
                    process.join(10)
                    parent.close()

    def test_publication_lost_commit_response_uses_durable_frontier(self):
        fault = make_engine(self.dsn, FaultConnection)
        fault.pgsql_conn.lose_ack = True
        tx = self.transaction(rows=1001)
        try:
            with self.assertRaisesRegex(psycopg2.OperationalError, 'lost commit response'):
                fault.publish_transaction(SID, tx)
        finally:
            fault.pgsql_conn.close()
        self.assertEqual(len(self.queue()), 1)
        self.assertFalse(self.engine.publish_transaction(SID, tx))
        self.assertFalse(self.replay()[1])
        self.engine.pgsql_cur.execute('DELETE FROM sch_chameleon.t_replica_batch WHERE b_replayed')
        self.assertFalse(self.engine.publish_transaction(SID, tx))
        self.assertEqual(self.count(), 1001)
        self.assertEqual(self.queue(), [])

    def test_replay_lost_commit_response_is_complete_and_not_reapplied(self):
        self.engine.publish_transaction(SID, self.transaction(rows=1001))
        conn = psycopg2.connect(self.dsn, connection_factory=FaultConnection)
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM sch_chameleon.fn_replay_mysql(1000,%s,true)', (self.engine.i_id_source,))
                self.assertFalse(cur.fetchone()[1])
            # A separate READ COMMITTED observer sees no uncommitted rows/checkpoint.
            self.assertEqual(self.count(), 0)
            self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7')
            self.assertEqual(len(self.queue()[0][1]), 1001)
            conn.lose_ack = True
            with self.assertRaisesRegex(psycopg2.OperationalError, 'lost commit response'):
                conn.commit()
        finally:
            conn.close()
        self.assertEqual(self.count(), 1001)
        self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7:9')
        self.assertEqual(self.queue(), [])
        self.assertFalse(self.replay()[1])
        self.assertEqual(self.count(), 1001)

    def wait_backend_lock(self, pid):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            self.engine.pgsql_cur.execute('SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s', (pid,))
            if self.engine.pgsql_cur.fetchone() == ('Lock',):
                return
            time.sleep(0.05)
        self.fail('Replay did not reach native lock barrier')

    def start_replay_worker(self):
        parent, child = multiprocessing.get_context('spawn').Pipe()
        process = multiprocessing.get_context('spawn').Process(
            target=replay_worker, args=(self.dsn, self.engine.i_id_source, child))
        process.start()
        child.close()
        self.assertTrue(parent.poll(15))
        kind, pid = parent.recv()
        self.assertEqual(kind, 'backend')
        parent.send('run')
        self.addCleanup(parent.close)
        def stop():
            if process.is_alive():
                process.kill()
            process.join(10)
        self.addCleanup(stop)
        return process, parent, pid

    def test_replay_connection_failure_rolls_back_partial_work(self):
        cur = self.engine.pgsql_cur
        cur.execute('CREATE FUNCTION public.cdc_test_pause() RETURNS trigger LANGUAGE plpgsql AS $$ '
                    'BEGIN IF NEW.id=1000 THEN PERFORM pg_advisory_xact_lock(823719); END IF; RETURN NEW; END $$')
        cur.execute('CREATE TRIGGER cdc_test_pause BEFORE INSERT ON public.items '
                    'FOR EACH ROW EXECUTE FUNCTION public.cdc_test_pause()')
        blocker = psycopg2.connect(self.dsn)
        try:
            with blocker.cursor() as lock:
                lock.execute('SELECT pg_advisory_xact_lock(823719)')
            self.engine.publish_transaction(SID, self.transaction(rows=1001))
            before, queue = self.engine.get_transaction_state(), self.queue()
            process, pipe, pid = self.start_replay_worker()
            self.wait_backend_lock(pid)
            self.assertEqual(self.count(), 0)
            cur.execute('SELECT pg_terminate_backend(%s)', (pid,))  # Exact fixture worker backend.
            self.assertEqual(cur.fetchone(), (True,))
            self.assertTrue(pipe.poll(15))
            self.assertEqual(pipe.recv()[0], 'error')
            process.join(10)
            self.assertEqual(self.count(), 0)
            self.assertEqual(self.queue(), queue)
            self.assertEqual(self.engine.get_transaction_state(), before)
        finally:
            blocker.close()
            cur.execute('DROP TRIGGER cdc_test_pause ON public.items')
            cur.execute('DROP FUNCTION public.cdc_test_pause()')
        self.assertFalse(self.replay()[1])
        self.assertEqual(self.count(), 1001)

    def test_real_deadlock_never_skips_or_commits_partial_transaction(self):
        cur = self.engine.pgsql_cur
        cur.execute('INSERT INTO public.items VALUES(0),(1)')
        tx = self.transaction()
        for row in tx['rows']:
            row['global_data']['action'] = 'delete'
            # Upstream DeleteRowsEvent.values lives in event_after (not before).
        self.engine.publish_transaction(SID, tx)
        before, queue = self.engine.get_transaction_state(), self.queue()
        writer = psycopg2.connect(self.dsn)
        try:
            with writer.cursor() as lock:
                lock.execute('SET statement_timeout=15000')
                lock.execute('SELECT id FROM public.items WHERE id=1 FOR UPDATE')
                process, pipe, pid = self.start_replay_worker()
                self.wait_backend_lock(pid)
                self.assertEqual(self.count(), 2)  # First DELETE remains invisible.
                def contend():
                    try:
                        lock.execute('SELECT id FROM public.items WHERE id=0 FOR UPDATE')
                    except psycopg2.errors.DeadlockDetected:
                        return 'writer_victim'
                    finally:
                        writer.rollback()
                    return 'replay_victim'
                with ThreadPoolExecutor(max_workers=1) as pool:
                    victim = pool.submit(contend).result(timeout=20)
                self.assertTrue(pipe.poll(15))
                kind, status = pipe.recv()
                self.assertEqual(kind, 'done')
                process.join(10)
                if victim == 'replay_victim':
                    self.assertTrue(status[1])
                    self.assertEqual(self.count(), 2)
                    self.assertEqual(self.queue(), queue)
                    self.assertEqual(self.engine.get_transaction_state(), before)
                    self.assertFalse(self.replay()[1])
                else:
                    self.assertFalse(status[1])
                self.assertEqual(self.count(), 0)
                self.assertEqual(self.queue(), [])
                self.assertEqual(self.engine.get_transaction_state()['applied_gtid_set'], SID + ':1-3:7:9')
        finally:
            writer.close()

    def test_two_replayers_serialize(self):
        self.engine.publish_transaction(SID, self.transaction(rows=1001))
        def replay():
            conn = psycopg2.connect(self.dsn)
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute('SELECT * FROM sch_chameleon.fn_replay_mysql(1000,%s,true)', (self.engine.i_id_source,))
                    return cur.fetchone()
            finally:
                conn.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(lambda _: replay(), range(2)))
        self.assertTrue(all(not status[1] for status in statuses))
        self.assertEqual(self.count(), 1001)
        self.assertEqual(self.queue(), [])

    def test_missing_old_row_does_not_acknowledge_update(self):
        tx = self.transaction(rows=1)
        tx['rows'][0]['global_data']['action'] = 'update'
        tx['rows'][0]['event_before'] = {'id': 99}
        tx['rows'][0]['event_after'] = {'id': 100}
        self.engine.publish_transaction(SID, tx)
        before = self.engine.get_transaction_state()
        queued = self.queue()
        self.assertTrue(self.replay()[1], 'UPDATE of missing row must fail closed')
        self.assertEqual(self.engine.get_transaction_state(), before)
        self.assertEqual(self.queue(), queued)

    def test_migration_replay_forbids_skip_errors(self):
        with self.assertRaises(psycopg2.Error):
            self.engine.pgsql_cur.execute('SELECT * FROM sch_chameleon.fn_replay_mysql(1000,%s,false)', (self.engine.i_id_source,))

    def test_mode_cannot_be_disabled_against_migration_frontier(self):
        self.engine.publish_transaction(SID, self.transaction())
        self.engine.sources['fixture']['transactional_capture'] = False
        try:
            with self.assertRaises(ValueError):
                self.engine.replay_replica()
        finally:
            self.engine.sources['fixture']['transactional_capture'] = True
        self.assertEqual(self.count(), 0)

    def test_legacy_replay_still_uses_one_budget(self):
        cur = self.engine.pgsql_cur
        cur.execute("INSERT INTO sch_chameleon.t_sources(t_source,jsb_schema_mappings,enm_source_type,v_log_table) "
                    "VALUES ('legacy','{}','mysql',ARRAY['t_log_replica','t_log_replica']) RETURNING i_id_source")
        other = cur.fetchone()[0]
        try:
            cur.execute("INSERT INTO sch_chameleon.t_replica_tables(i_id_source,v_table_name,v_schema_name,v_table_pkey) "
                        "VALUES (%s,'items','public',ARRAY['id'])", (other,))
            cur.execute("INSERT INTO sch_chameleon.t_replica_batch(i_id_source,b_started,v_log_table) "
                        "VALUES (%s,true,'t_log_replica') RETURNING i_id_batch", (other,))
            batch = cur.fetchone()[0]
            rows = self.transaction()['rows']
            for row in rows:
                row['global_data'].update(batch_id=batch, log_table='t_log_replica')
            self.engine.write_batch(rows)  # Legacy API, no new strict parameter required.
            self.engine.set_batch_processed(batch)
            cur.execute('SELECT * FROM sch_chameleon.fn_replay_mysql(1,%s,true)', (other,))
            self.assertFalse(cur.fetchone()[1])
            self.assertEqual(self.count(), 1)
            cur.execute('SELECT cardinality(i_id_event) FROM sch_chameleon.t_batch_events WHERE i_id_batch=%s', (batch,))
            self.assertEqual(cur.fetchone(), (1,))
        finally:
            cur.execute('DELETE FROM sch_chameleon.t_sources WHERE i_id_source=%s', (other,))


if __name__ == '__main__':
    unittest.main()
