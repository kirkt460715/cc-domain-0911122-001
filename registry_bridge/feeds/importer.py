import hashlib
import io
import json
from pathlib import Path

from registry_bridge.errors import Conflict, InvalidMessage, NotFound, RegistryError
from registry_bridge.feeds.adapters import csv_records, ndjson_records
from registry_bridge.feeds.message import canonical, digest, identifier, integer, parse_message, text
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import audit


def dead_letter(store, source, line_number, raw, error):
    source = text(source, 'source', 512)
    integer(line_number, 'line_number', 1)
    error_value = error.as_dict() if isinstance(error, RegistryError) else error
    dead_id = digest({'source': source, 'line': line_number, 'raw': raw})
    store.execute(
        'INSERT OR IGNORE INTO dead_letters(dead_id,source,line_number,raw,error,created_at) VALUES(?,?,?,?,?,?)',
        (dead_id, source, line_number, raw, canonical(error_value), store.clock()),
    )
    return dead_id


def inspect_file(path, *, maximum=64 * 1024 * 1024):
    file = Path(path)
    if not file.is_file() or file.is_symlink():
        raise InvalidMessage('Import must be a regular non-symlink file')
    size = file.stat().st_size
    if size > maximum:
        raise InvalidMessage('Import file exceeds size limit')
    data = file.read_bytes()
    if len(data) > maximum:
        raise InvalidMessage('Import changed beyond size limit while reading')
    return data, hashlib.sha256(data).hexdigest()


def import_bytes(store, data, source, *, format='ndjson', atomic=False):
    source = text(source, 'source', 512)
    if not isinstance(data, bytes) or len(data) > 64 * 1024 * 1024:
        raise InvalidMessage('Import requires at most 64 MiB of bytes')
    if format not in {'ndjson', 'csv'}:
        raise InvalidMessage('Unsupported feed format')
    content_hash = hashlib.sha256(data).hexdigest()
    import_id = digest({'source': source, 'content_hash': content_hash, 'format': format})
    with store.transaction():
        previous = store.one(
            'SELECT * FROM imports WHERE source=? AND content_hash=?', (source, content_hash),
        )
        if previous:
            if previous['import_id'] != import_id:
                raise Conflict('Same source bytes were imported under another format')
            return {**previous, 'reused': True}
        store.execute(
            'INSERT INTO imports(import_id,source,content_hash,state,total,accepted,duplicate,rejected,conflicts,started_at) '
            "VALUES(?,?,?,'running',0,0,0,0,0,?)",
            (import_id, source, content_hash, store.clock()),
        )
        counts = {'total': 0, 'accepted': 0, 'duplicate': 0, 'rejected': 0, 'conflicts': 0}
        if format == 'ndjson':
            records = ndjson_records(io.BytesIO(data))
        else:
            try:
                decoded = data.decode('utf-8')
            except UnicodeDecodeError as exc:
                raise InvalidMessage('CSV must be UTF-8') from exc
            records = csv_records(io.StringIO(decoded, newline=''))
        for line_number, message, error, raw in records:
            counts['total'] += 1
            if error is not None:
                if atomic:
                    raise error
                dead_letter(store, source, line_number, raw, error)
                counts['rejected'] += 1
                continue
            try:
                result = receive(store, message)
            except RegistryError as exc:
                if atomic:
                    raise
                dead_letter(store, source, line_number, raw, exc)
                counts['rejected'] += 1
                continue
            status = result['status']
            if status == 'conflict':
                if atomic:
                    raise Conflict('Atomic import encountered an identity conflict')
                counts['conflicts'] += 1
            else:
                counts[status] += 1
        state = 'partial' if counts['rejected'] or counts['conflicts'] else 'complete'
        store.execute(
            'UPDATE imports SET state=?,total=?,accepted=?,duplicate=?,rejected=?,conflicts=?,completed_at=? WHERE import_id=?',
            (state, counts['total'], counts['accepted'], counts['duplicate'], counts['rejected'],
             counts['conflicts'], store.clock(), import_id),
        )
        audit.append(store, 'import.completed', 'importer', {
            'import_id': import_id, 'state': state, 'counts': counts, 'atomic': atomic,
        })
        return store.one('SELECT * FROM imports WHERE import_id=?', (import_id,))


def import_file(store, path, source, **options):
    data, _ = inspect_file(path)
    return import_bytes(store, data, source, **options)


def list_dead_letters(store, *, unreplayed=True, limit=100):
    integer(limit, 'limit', 1, 10000)
    clause = 'WHERE replayed_as IS NULL' if unreplayed else ''
    rows = store.all(f'SELECT * FROM dead_letters {clause} ORDER BY source,line_number,dead_id LIMIT ?', (limit,))
    for row in rows:
        row['error'] = json.loads(row['error'])
    return rows


def replay_dead_letter(store, dead_id, corrected, *, actor='operator'):
    message = parse_message(corrected)
    with store.transaction():
        row = store.one('SELECT * FROM dead_letters WHERE dead_id=?', (dead_id,))
        if row is None:
            raise NotFound('Dead letter does not exist')
        if row['replayed_as'] is not None:
            if row['replayed_as'] == message.event_id:
                existing = store.one('SELECT fingerprint FROM events WHERE event_id=?', (message.event_id,))
                if existing and existing['fingerprint'] == message.fingerprint:
                    return {'status': 'already_replayed', 'event_id': message.event_id}
            raise Conflict('Dead letter already replayed differently')
        result = receive(store, message)
        if result['status'] == 'conflict':
            return result
        store.execute('UPDATE dead_letters SET replayed_as=? WHERE dead_id=?', (message.event_id, dead_id))
        audit.append(store, 'deadletter.replayed', actor, {'dead_id': dead_id, 'event_id': message.event_id})
        return result


def import_history(store, limit=100):
    integer(limit, 'limit', 1, 10000)
    return store.all('SELECT * FROM imports ORDER BY started_at DESC,import_id LIMIT ?', (limit,))
