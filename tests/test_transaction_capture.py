"""Synthetic transaction boundaries; no database or credentials needed."""
import json
import unittest
from types import SimpleNamespace
from unittest import mock

from pymysqlreplication.event import GtidEvent, HeartbeatLogEvent, QueryEvent, RotateEvent, XidEvent
from pymysqlreplication.row_event import UpdateRowsEvent, WriteRowsEvent
from pg_chameleon.lib.mysql_lib import mysql_source

from pg_chameleon.lib.transaction import TransactionBuffer, merge_committed_gtid

SID = '00000000-0000-0000-0000-000000000001'
OTHER_SID = '00000000-0000-0000-0000-000000000002'
COMMIT = {'File': 'mysql-bin.000001', 'Position': 20000, 'Time': 1}
ROW = {'global_data': {'action': 'insert'}, 'event_after': {'id': 1}, 'event_before': {}}


class CaptureTest(unittest.TestCase):
    def test_commit_keeps_all_rows(self):
        buf = TransactionBuffer()
        buf.begin(SID + ':7')
        for item in range(1001):
            buf.append(dict(ROW, event_after={'id': item}))
        self.assertTrue(buf.pending)
        tx = buf.commit(COMMIT)
        self.assertEqual(tx['gtid'], SID + ':7')
        self.assertEqual(tx['commit'], COMMIT)
        self.assertEqual(len(tx['rows']), 1001)
        self.assertFalse(buf.pending)
        self.assertEqual(tx['rows'][1000]['event_after']['id'], 1000)

    def test_limits_fail_before_append(self):
        size = len(json.dumps(ROW, default=str, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
        for options in ({'max_rows': 1}, {'max_bytes': size}):
            with self.subTest(options=options):
                buf = TransactionBuffer(**options)
                buf.begin(SID + ':7')
                buf.append(ROW)
                with self.assertRaises(ValueError):
                    buf.append(ROW)
                self.assertEqual(buf.commit(COMMIT)['rows'], [ROW])
        for value in (0, -1, True, 1.5, '1'):
            for option in ('max_rows', 'max_bytes'):
                with self.assertRaises(ValueError):
                    TransactionBuffer(**{option: value})

    def test_incomplete_and_rollback(self):
        buf = TransactionBuffer()
        with self.assertRaises(ValueError):
            buf.append(ROW)
        with self.assertRaises(ValueError):
            buf.commit(COMMIT)
        buf.begin(SID + ':7')
        buf.append(ROW)
        with self.assertRaises(ValueError):
            buf.begin(SID + ':8')
        self.assertTrue(buf.pending)  # EOF/rotate/heartbeat are NOT commits.
        buf.rollback()
        self.assertFalse(buf.pending)
        buf.begin(SID + ':8')
        self.assertEqual(buf.commit(COMMIT)['rows'], [])

    def test_gtid_validation(self):
        for gtid in ('', 'broken', SID + ':0', SID + ':1-3', SID + ':1:3', SID + ':1,' + OTHER_SID + ':2'):
            with self.subTest(gtid=gtid), self.assertRaises(ValueError):
                TransactionBuffer().begin(gtid)

    def test_gtid_holes_and_multiple_sids(self):
        previous = SID + ':1-3:7,' + OTHER_SID + ':2-4'
        self.assertEqual(merge_committed_gtid(previous, SID + ':9'), SID + ':1-3:7:9,' + OTHER_SID + ':2-4')
        self.assertEqual(merge_committed_gtid(previous, SID + ':7'), previous)
        self.assertEqual(merge_committed_gtid('', SID + ':7'), SID + ':7')

    def test_bad_commit_does_not_clear_pending(self):
        buf = TransactionBuffer()
        buf.begin(SID + ':7')
        for coords in ({}, dict(COMMIT, Position=0), dict(COMMIT, Position=True)):
            with self.assertRaises(ValueError):
                buf.commit(coords)
            self.assertTrue(buf.pending)
        self.assertEqual(buf.commit(COMMIT)['rows'], [])


class ReaderTest(unittest.TestCase):
    def setUp(self):
        self.source = mysql_source()
        self.source.conn_buffered = self.source.conn_unbuffered = None
        self.source.source_config = {'transactional_capture': True}
        self.source.pg_engine = mock.Mock()
        self.source.pg_engine.get_inconsistent_tables.return_value = {}
        self.source.pg_engine.get_tables_disabled.return_value = []
        self.source.replica_conn = {}
        self.source.my_server_id = 901
        self.source.schema_replica = ['source']
        self.source.schema_mappings = {'source': 'public'}
        self.source.limit_tables = self.source.skip_tables = {}
        self.source.skip_events = None
        self.source.replica_batch_size = 1000
        self.source.sleep_loop = 1
        self.source.hexify = self.source.hexify_always
        self.source.get_table_type_map = lambda: {'source': {'items': {'column_type': {'id': 'int', 'j': 'json'}}}}
        self.state = {'capture_file': 'mysql-bin.000001', 'capture_position': 4, 'capture_gtid_set': SID + ':1-3:7'}

    def event(self, cls, position=100, **attributes):
        return mock.Mock(spec=cls, packet=SimpleNamespace(log_pos=position), timestamp=1, **attributes)

    def stream(self, events):
        stream = mock.MagicMock()
        stream.__iter__.return_value = iter(events)
        with mock.patch('pg_chameleon.lib.mysql_lib.BinLogStreamReader', return_value=stream) as factory:
            transactions = list(self.source._mysql_source__read_transaction_stream(self.state))
        self.assertEqual(factory.call_args.kwargs['auto_position'], SID + ':1-3:7')
        self.assertIn(XidEvent, factory.call_args.kwargs['only_events'])
        stream.close.assert_called_once()
        return transactions

    def gtid(self, number=9):
        return self.event(GtidEvent, gtid=SID + ':' + str(number))

    def rows(self, count=1):
        return self.event(WriteRowsEvent, schema='source', table='items', rows=[{'values': {'id': i}} for i in range(count)])

    def test_rows_require_xid_and_are_not_split(self):
        tx = self.stream([self.gtid(), self.event(QueryEvent, query='BEGIN'), self.rows(1001), self.event(XidEvent, position=200)])
        self.assertEqual(len(tx), 1)
        self.assertEqual(len(tx[0]['rows']), 1001)
        self.assertEqual(tx[0]['commit']['Position'], 200)
        self.assertEqual(self.stream([self.gtid(), self.rows(), self.event(HeartbeatLogEvent)]), [])
        self.assertEqual(self.stream([self.gtid(), self.rows(), self.event(RotateEvent, next_binlog='mysql-bin.000002')]), [])

    def test_multiple_commits_before_soft_limit(self):
        tx = self.stream([self.gtid(), self.rows(), self.event(XidEvent), self.gtid(10), self.rows(), self.event(XidEvent, position=300)])
        self.assertEqual([t['gtid'] for t in tx], [SID + ':9', SID + ':10'])

    def test_filtered_and_rollback(self):
        self.source.skip_tables = {'source': ['items']}
        tx = self.stream([self.gtid(), self.rows(), self.event(XidEvent)])
        self.assertEqual(tx[0]['rows'], [])
        self.assertEqual(self.stream([self.gtid(), self.rows(), self.event(QueryEvent, query='ROLLBACK')]), [])

    def test_ddl_and_missing_metadata_fail_closed(self):
        with self.assertRaises(ValueError):
            self.stream([self.gtid(), self.event(QueryEvent, query='ALTER TABLE items ADD x int')])
        self.source.get_table_type_map = lambda: {'source': {'items': {'column_type': {}}}}
        with self.assertRaises((ValueError, KeyError)):
            self.stream([self.gtid(), self.rows(), self.event(XidEvent)])

    def test_json_before_is_decoded_from_before(self):
        event = self.event(UpdateRowsEvent, schema='source', table='items', rows=[{
            'before_values': {'id': 1, 'j': {b'k': b'old'}},
            'after_values': {'id': 1, 'j': {b'k': b'new'}}}])
        tx = self.stream([self.gtid(), event, self.event(XidEvent)])
        self.assertEqual(tx[0]['rows'][0]['event_before']['j'], {'k': 'old'})
        self.assertEqual(tx[0]['rows'][0]['event_after']['j'], {'k': 'new'})

    def test_source_row_full_gtid_required(self):
        good = {'source_uuid': SID, 'gtid_mode': 'ON', 'log_bin': 1, 'binlog_format': 'ROW', 'binlog_row_image': 'FULL'}
        self.source.cursor_buffered = mock.Mock()
        self.source.cursor_buffered.fetchone.return_value = good
        self.source._mysql_source__check_mysql_config()
        self.assertEqual(self.source.source_uuid, SID)
        for key, value in (('gtid_mode', 'OFF'), ('log_bin', 0), ('binlog_format', 'STATEMENT'), ('binlog_row_image', 'MINIMAL')):
            self.source.cursor_buffered.fetchone.return_value = dict(good, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.source._mysql_source__check_mysql_config()

    def test_purged_or_missing_gtid_rejected_before_stream(self):
        self.source._mysql_source__init_read_replica = lambda: None
        self.source.source_uuid = SID
        self.source.cursor_buffered = mock.Mock()
        self.source.pg_engine.get_replica_paused.return_value = False
        self.source.pg_engine.get_transaction_state.return_value = dict(self.state, source_uuid=SID)
        with mock.patch.object(self.source, '_mysql_source__read_transaction_stream') as stream:
            for available in ({'purged': SID + ':1-9', 'executed': SID + ':1-100'},
                              {'purged': SID + ':1-3', 'executed': SID + ':1-3'}):
                self.source.cursor_buffered.fetchone.return_value = available
                with self.assertRaises(ValueError):
                    self.source.read_replica()
            stream.assert_not_called()

    def test_initial_load_guard_precedes_cleanup_and_copy(self):
        self.source.source = 'fixture'
        self.source.logger = mock.Mock()
        self.source._mysql_source__init_sync = lambda: None
        self.source._mysql_source__check_mysql_config = lambda: None
        self.source.pg_engine.assert_new_transaction_source = mock.Mock(side_effect=ValueError('Existing frontier'))
        self.source.get_master_coordinates = mock.Mock(side_effect=AssertionError('Guard must precede coordinate access'))
        with self.assertRaisesRegex(ValueError, 'Existing frontier'):
            self.source.init_replica()
        self.source.pg_engine.cleanup_source_tables.assert_not_called()


if __name__ == '__main__':
    unittest.main()
