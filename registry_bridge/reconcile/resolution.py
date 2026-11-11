import json

from registry_bridge.errors import Conflict, NotFound, StaleRevision
from registry_bridge.feeds.message import identifier, integer, text
from registry_bridge.feeds.receiver import drain_stream
from registry_bridge.ledger import audit, issuers


def skip_rejected(store, event_id, reason, *, actor='operator'):
    identifier(event_id, 'event_id')
    reason = text(reason, 'reason', 2000)
    with store.transaction():
        row = store.one('SELECT * FROM events WHERE event_id=?', (event_id,))
        if row is None:
            raise NotFound('Event does not exist')
        previous = store.one('SELECT * FROM decisions WHERE event_id=?', (event_id,))
        if previous:
            if previous['action'] == 'skip' and previous['reason'] == reason and previous['actor'] == actor:
                return {'event_id': event_id, 'status': 'already_skipped'}
            raise Conflict('Event already has an operator decision')
        issuer = issuers.get(store, row['issuer'])
        if row['disposition'] != 'rejected' or row['sequence'] != issuer['last_sequence'] + 1:
            raise Conflict('Only the rejected event immediately after cursor can be skipped')
        store.execute(
            'INSERT INTO decisions(event_id,action,actor,reason,at) VALUES(?,?,?,?,?)',
            (event_id, 'skip', actor, reason, store.clock()),
        )
        store.execute(
            "UPDATE events SET disposition='skipped',commit_position=(SELECT COALESCE(MAX(commit_position),0)+1 FROM events),applied_at=? WHERE event_id=?",
            (store.clock(), event_id),
        )
        audit.append(store, 'event.skipped', actor, {'event_id': event_id, 'reason': reason})
        advanced = drain_stream(store, row['issuer'])
        return {'event_id': event_id, 'status': 'skipped', 'advanced': advanced}


def conflict(store, conflict_id):
    row = store.one('SELECT * FROM conflicts WHERE conflict_id=?', (conflict_id,))
    if row is None:
        raise NotFound('Conflict does not exist')
    row['incoming'] = json.loads(row['incoming'])
    return row


def close_conflict(store, conflict_id, revision, reason, *, actor='operator'):
    integer(revision, 'revision')
    reason = text(reason, 'reason', 2000)
    with store.transaction():
        previous = conflict(store, conflict_id)
        if previous['revision'] != revision:
            raise StaleRevision('Conflict changed since review')
        if previous['status'] == 'closed':
            raise Conflict('Conflict is already closed')
        store.execute(
            "UPDATE conflicts SET status='closed',resolution=?,resolved_at=?,revision=revision+1 WHERE conflict_id=?",
            (reason, store.clock(), conflict_id),
        )
        audit.append(store, 'conflict.closed', actor, {
            'conflict_id': conflict_id, 'resolution': reason,
            'policy': 'retain_original_event',
        })
        return conflict(store, conflict_id)


def open_conflicts(store, issuer=None, limit=100):
    integer(limit, 'limit', 1, 10000)
    params = []
    clause = "status='open'"
    if issuer is not None:
        identifier(issuer, 'issuer')
        clause += ' AND issuer=?'
        params.append(issuer)
    params.append(limit)
    rows = store.all(f'SELECT * FROM conflicts WHERE {clause} ORDER BY created_at,conflict_id LIMIT ?', params)
    for row in rows:
        row['incoming'] = json.loads(row['incoming'])
    return rows


def gaps(store, issuer, *, ceiling=None):
    config = issuers.get(store, issuer)
    last = config['last_sequence']
    highest = store.scalar('SELECT MAX(sequence) FROM events WHERE issuer=?', (issuer,)) or last
    if ceiling is not None:
        integer(ceiling, 'ceiling', 0)
        highest = min(highest, ceiling)
    rows = store.all(
        'SELECT sequence FROM events WHERE issuer=? AND sequence>? AND sequence<=? ORDER BY sequence',
        (issuer, last, highest),
    )
    missing = []
    expected = last + 1
    for row in rows:
        sequence = row['sequence']
        if sequence > expected:
            missing.append({'from': expected, 'to': sequence - 1, 'count': sequence - expected})
        expected = sequence + 1
    if expected <= highest:
        missing.append({'from': expected, 'to': highest, 'count': highest - expected + 1})
    return {'issuer': issuer, 'cursor': last, 'ceiling': highest, 'gaps': missing}


def explain_blockage(store, issuer):
    config = issuers.get(store, issuer)
    expected = config['last_sequence'] + 1
    if not config['enabled']:
        return {'kind': 'disabled', 'issuer': issuer, 'sequence': expected}
    row = store.one(
        'SELECT event_id,disposition,rejection FROM events WHERE issuer=? AND sequence=?',
        (issuer, expected),
    )
    if row is None:
        later = store.scalar('SELECT MIN(sequence) FROM events WHERE issuer=? AND sequence>?', (issuer, expected))
        return {'kind': 'gap' if later else 'caught_up', 'sequence': expected, 'next_received': later}
    if row['disposition'] == 'rejected':
        return {'kind': 'rejected', 'event_id': row['event_id'], 'error': json.loads(row['rejection'])}
    return {'kind': 'pending', 'event_id': row['event_id'], 'sequence': expected}
