import json

from registry_bridge.feeds.message import digest
from registry_bridge.feeds.receiver import from_row
from registry_bridge.ledger import audit


def inspect_events(store):
    errors = []
    count = 0
    for row in store.execute('SELECT * FROM events ORDER BY position'):
        count += 1
        try:
            message = from_row(dict(row))
        except Exception as exc:
            errors.append({'kind': 'malformed_event', 'event_id': row['event_id'], 'message': str(exc)})
            continue
        if message.fingerprint != row['fingerprint']:
            errors.append({'kind': 'fingerprint_mismatch', 'event_id': row['event_id']})
        projected = store.scalar('SELECT COUNT(*) FROM history WHERE event_id=?', (row['event_id'],))
        decision = store.one('SELECT action FROM decisions WHERE event_id=?', (row['event_id'],))
        if row['disposition'] == 'applied' and projected != 1:
            errors.append({'kind': 'missing_history', 'event_id': row['event_id']})
        if row['disposition'] != 'applied' and projected:
            errors.append({'kind': 'unexpected_history', 'event_id': row['event_id']})
        if row['disposition'] == 'skipped' and (not decision or decision['action'] != 'skip'):
            errors.append({'kind': 'unexplained_skip', 'event_id': row['event_id']})
        if row['disposition'] in {'applied', 'skipped'} and not row['applied_at']:
            errors.append({'kind': 'missing_commit_time', 'event_id': row['event_id']})
        committed = row['disposition'] in {'applied', 'skipped'}
        if committed != (row['commit_position'] is not None):
            errors.append({'kind': 'invalid_commit_position', 'event_id': row['event_id']})
        if row['disposition'] == 'rejected' and not row['rejection']:
            errors.append({'kind': 'missing_rejection', 'event_id': row['event_id']})
    return {'ok': not errors, 'checked': count, 'errors': errors}


def inspect_history(store):
    errors = []
    count = 0
    for row in store.execute('SELECT * FROM history ORDER BY issuer,asset_id,revision'):
        count += 1
        try:
            snapshot = json.loads(row['snapshot'])
        except ValueError:
            errors.append({'kind': 'malformed_snapshot', 'event_id': row['event_id']})
            continue
        for key in ('event_id', 'issuer', 'asset_id', 'revision', 'effective_at'):
            if snapshot.get(key) != row[key]:
                errors.append({'kind': 'history_index_mismatch', 'event_id': row['event_id'], 'field': key})
        event = store.one('SELECT event_id FROM events WHERE event_id=?', (row['event_id'],))
        if event is None:
            errors.append({'kind': 'orphan_history', 'event_id': row['event_id']})
    return {'ok': not errors, 'checked': count, 'errors': errors}


def inspect_consumers(store):
    from registry_bridge.query.consumer import stable_ceiling
    ceiling = stable_ceiling(store)
    errors = []
    rows = store.all('SELECT * FROM checkpoints ORDER BY consumer')
    for row in rows:
        if row['position'] < 0 or row['position'] > ceiling:
            errors.append({'kind': 'invalid_checkpoint', 'consumer': row['consumer'], 'position': row['position']})
        if row['revision'] < 1:
            errors.append({'kind': 'invalid_checkpoint_revision', 'consumer': row['consumer']})
    exports = store.all('SELECT * FROM export_jobs ORDER BY export_id')
    for row in exports:
        if row['lower_position'] > row['upper_position']:
            errors.append({'kind': 'reversed_export', 'export_id': row['export_id']})
        if row['upper_position'] > ceiling:
            errors.append({'kind': 'unstable_export', 'export_id': row['export_id']})
        if row['acknowledged_at']:
            cursor = store.scalar('SELECT position FROM checkpoints WHERE consumer=?', (row['consumer'],))
            if (cursor or 0) < row['upper_position']:
                errors.append({'kind': 'acknowledgement_ahead', 'export_id': row['export_id']})
    return {'ok': not errors, 'consumers': len(rows), 'exports': len(exports), 'errors': errors}


def inspect_all(store):
    with store.transaction(immediate=False):
        events = inspect_events(store)
        history = inspect_history(store)
        consumers = inspect_consumers(store)
        chain = audit.verify(store)
        database = store.integrity()
        return {
            'ok': all(section['ok'] for section in (events, history, consumers, chain, database)),
            'events': events,
            'history': history,
            'consumers': consumers,
            'audit': chain,
            'database': database,
        }


def incident_bundle(store, event_ids):
    from registry_bridge.errors import InvalidMessage
    from registry_bridge.feeds.message import identifier
    from registry_bridge.reconcile.inspection import event_trace
    if not isinstance(event_ids, list) or not 1 <= len(event_ids) <= 100:
        raise InvalidMessage('Incident selection requires one to one hundred event identifiers')
    normalized = sorted({identifier(value, 'event_id') for value in event_ids})
    with store.transaction(immediate=False):
        traces = [event_trace(store, event_id) for event_id in normalized]
        audit_state = audit.verify(store)
        payload = {'version': 1, 'traces': traces, 'audit': audit_state}
        return {'payload': payload, 'sha256': digest(payload)}
