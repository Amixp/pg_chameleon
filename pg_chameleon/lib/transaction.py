"""Commit-boundary buffering for the explicitly enabled migration mode."""
import json
import re

from pymysqlreplication.gtid import Gtid, GtidSet


def _single_gtid(value):
    if not isinstance(value, str) or not re.fullmatch(
        r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}:[1-9][0-9]*', value
    ):
        raise ValueError('Expected a single positive MySQL GTID')
    return Gtid(value.lower())


def merge_committed_gtid(previous, committed):
    gtid = _single_gtid(committed)
    saved = GtidSet(previous)
    # Gtid.__add__ rejects overlapping intervals, including duplicate delivery.
    return str(saved if gtid in saved else saved + gtid)


class TransactionBuffer:
    def __init__(self, max_rows=100000, max_bytes=268435456):
        for value in (max_rows, max_bytes):
            if type(value) is not int or value <= 0:
                raise ValueError('Transaction limits must be positive integers')
        self.max_rows = max_rows
        self.max_bytes = max_bytes
        self.rollback()

    @property
    def pending(self):
        return self.gtid is not None

    def begin(self, gtid):
        if self.pending:
            raise ValueError('New GTID before the previous transaction committed')
        self.gtid = str(_single_gtid(gtid))

    def append(self, row):
        if not self.pending:
            raise ValueError('Row event outside a GTID transaction')
        size = len(json.dumps(row, default=str, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
        if len(self.rows) >= self.max_rows or self.size + size > self.max_bytes:
            raise ValueError('Source transaction exceeds its configured limits')
        self.rows.append(row)
        self.size += size

    def commit(self, coordinates):
        if not self.pending:
            raise ValueError('Commit outside a GTID transaction')
        if (not isinstance(coordinates.get('File'), str) or not coordinates['File']
                or type(coordinates.get('Position')) is not int or coordinates['Position'] <= 0
                or type(coordinates.get('Time')) is not int or coordinates['Time'] < 0):
            raise ValueError('Invalid commit coordinates')
        transaction = {'gtid': self.gtid, 'commit': dict(coordinates), 'rows': self.rows}
        self.rollback()
        return transaction

    def rollback(self):
        self.gtid = None
        self.rows = []
        self.size = 0
