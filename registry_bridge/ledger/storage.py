import contextlib
import datetime as dt
import sqlite3
from pathlib import Path

from registry_bridge.errors import IntegrityFailure, InvalidMessage

SCHEMA_VERSION = 1
DDL = '''
CREATE TABLE IF NOT EXISTS issuers(
 issuer TEXT PRIMARY KEY, display_name TEXT NOT NULL, enabled INTEGER NOT NULL,
 last_sequence INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS events(
 position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
 issuer TEXT NOT NULL REFERENCES issuers(issuer), sequence INTEGER NOT NULL,
 asset_id TEXT NOT NULL, revision INTEGER NOT NULL, operation TEXT NOT NULL,
 effective_at TEXT NOT NULL, received_at TEXT NOT NULL, payload TEXT NOT NULL,
 fingerprint TEXT NOT NULL, disposition TEXT NOT NULL DEFAULT 'pending',
 rejection TEXT, applied_at TEXT, commit_position INTEGER UNIQUE, UNIQUE(issuer,sequence));
CREATE INDEX IF NOT EXISTS events_asset ON events(issuer,asset_id,revision);
CREATE INDEX IF NOT EXISTS events_pending ON events(issuer,disposition,sequence);
CREATE TABLE IF NOT EXISTS assets(
 issuer TEXT NOT NULL, asset_id TEXT NOT NULL, revision INTEGER NOT NULL,
 state TEXT NOT NULL, title TEXT NOT NULL, holder TEXT NOT NULL,
 scope TEXT NOT NULL, evidence TEXT NOT NULL, dataset_fingerprint TEXT NOT NULL,
 effective_at TEXT NOT NULL, event_id TEXT NOT NULL,
 PRIMARY KEY(issuer,asset_id));
CREATE INDEX IF NOT EXISTS assets_dataset ON assets(dataset_fingerprint);
CREATE TABLE IF NOT EXISTS history(
 event_id TEXT PRIMARY KEY, issuer TEXT NOT NULL, asset_id TEXT NOT NULL,
 revision INTEGER NOT NULL, effective_at TEXT NOT NULL, snapshot TEXT NOT NULL,
 UNIQUE(issuer,asset_id,revision));
CREATE INDEX IF NOT EXISTS history_time ON history(issuer,asset_id,effective_at,revision);
CREATE TABLE IF NOT EXISTS conflicts(
 conflict_id TEXT PRIMARY KEY, kind TEXT NOT NULL, issuer TEXT NOT NULL,
 sequence INTEGER, existing_event TEXT, incoming TEXT NOT NULL,
 reason TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
 resolution TEXT, created_at TEXT NOT NULL, resolved_at TEXT,
 revision INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS dead_letters(
 dead_id TEXT PRIMARY KEY, source TEXT NOT NULL, line_number INTEGER NOT NULL,
 raw TEXT NOT NULL, error TEXT NOT NULL, created_at TEXT NOT NULL,
 replayed_as TEXT, UNIQUE(source,line_number,raw));
CREATE TABLE IF NOT EXISTS leases(
 name TEXT PRIMARY KEY, owner TEXT NOT NULL, generation INTEGER NOT NULL,
 expires_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoints(
 consumer TEXT PRIMARY KEY, position INTEGER NOT NULL,
 revision INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit(
 audit_id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL,
 actor TEXT NOT NULL, details TEXT NOT NULL, at TEXT NOT NULL,
 previous_hash TEXT NOT NULL, entry_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS imports(
 import_id TEXT PRIMARY KEY, source TEXT NOT NULL, content_hash TEXT NOT NULL,
 state TEXT NOT NULL, total INTEGER NOT NULL, accepted INTEGER NOT NULL,
 duplicate INTEGER NOT NULL, rejected INTEGER NOT NULL, conflicts INTEGER NOT NULL,
 started_at TEXT NOT NULL, completed_at TEXT, UNIQUE(source,content_hash));
CREATE TABLE IF NOT EXISTS decisions(
 event_id TEXT PRIMARY KEY REFERENCES events(event_id),
 action TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS export_jobs(
 export_id TEXT PRIMARY KEY, consumer TEXT NOT NULL, lower_position INTEGER NOT NULL,
 upper_position INTEGER NOT NULL, digest TEXT NOT NULL, row_count INTEGER NOT NULL,
 created_at TEXT NOT NULL, acknowledged_at TEXT);
'''


def utcnow():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


class Store:
    def __init__(self, path, *, timeout=5.0, clock=utcnow):
        self.path = str(path)
        self.clock = clock
        if self.path != ':memory:':
            parent = Path(self.path).parent
            if not parent.is_dir():
                raise InvalidMessage('Database parent directory does not exist')
        self.connection = sqlite3.connect(
            self.path, timeout=timeout, isolation_level=None,
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA busy_timeout=5000')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.execute('PRAGMA synchronous=FULL')
        self._depth = 0
        self._initialize()

    def _initialize(self):
        version = self.connection.execute('PRAGMA user_version').fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            self.close()
            raise IntegrityFailure('Unsupported database schema', context={'version': version})
        self.connection.executescript(DDL)
        self.connection.execute(f'PRAGMA user_version={SCHEMA_VERSION}')

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()

    @contextlib.contextmanager
    def transaction(self, *, immediate=True):
        depth = self._depth
        savepoint = f'nested_{depth}'
        if depth:
            self.connection.execute(f'SAVEPOINT {savepoint}')
        else:
            self.connection.execute('BEGIN IMMEDIATE' if immediate else 'BEGIN')
        self._depth += 1
        try:
            yield self
        except BaseException:
            if depth:
                self.connection.execute(f'ROLLBACK TO {savepoint}')
                self.connection.execute(f'RELEASE {savepoint}')
            else:
                self.connection.rollback()
            raise
        else:
            if depth:
                self.connection.execute(f'RELEASE {savepoint}')
            else:
                self.connection.commit()
        finally:
            self._depth -= 1

    def execute(self, sql, values=()):
        return self.connection.execute(sql, values)

    def one(self, sql, values=()):
        row = self.execute(sql, values).fetchone()
        return dict(row) if row is not None else None

    def all(self, sql, values=()):
        return [dict(row) for row in self.execute(sql, values).fetchall()]

    def scalar(self, sql, values=()):
        row = self.execute(sql, values).fetchone()
        return row[0] if row is not None else None

    def integrity(self):
        checks = [row[0] for row in self.execute('PRAGMA integrity_check')]
        foreign = self.all('PRAGMA foreign_key_check')
        return {'ok': checks == ['ok'] and not foreign, 'checks': checks, 'foreign_keys': foreign}

    def backup(self, destination):
        target = Path(destination)
        if self._depth:
            raise InvalidMessage('Cannot backup inside a transaction')
        if target.exists():
            raise InvalidMessage('Backup destination already exists')
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with sqlite3.connect(str(target)) as other:
                self.connection.backup(other)
                check = other.execute('PRAGMA integrity_check').fetchone()[0]
                if check != 'ok':
                    raise IntegrityFailure('Backup failed integrity check')
        except BaseException:
            if target.exists():
                target.unlink()
            raise
        return {'path': str(target), 'bytes': target.stat().st_size}
