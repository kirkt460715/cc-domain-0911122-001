import hashlib
import json

from registry_bridge.errors import Conflict, NotFound, StaleRevision
from registry_bridge.feeds.message import canonical, digest, identifier, integer
from registry_bridge.ledger import audit, leases


def checkpoint(store, consumer):
    identifier(consumer, 'consumer')
    row = store.one('SELECT * FROM checkpoints WHERE consumer=?', (consumer,))
    return row or {'consumer': consumer, 'position': 0, 'revision': 0, 'updated_at': None}


def stable_ceiling(store):
    return store.scalar('SELECT MAX(commit_position) FROM events') or 0


def changes(store, after=0, limit=100):
    integer(after, 'after', 0)
    integer(limit, 'limit', 1, 10000)
    ceiling = stable_ceiling(store)
    rows = store.all(
        'SELECT commit_position AS position,event_id,issuer,sequence,disposition,applied_at FROM events '
        'WHERE commit_position>? AND commit_position<=? ORDER BY commit_position LIMIT ?',
        (after, ceiling, limit),
    )
    for row in rows:
        history = store.one('SELECT snapshot FROM history WHERE event_id=?', (row['event_id'],))
        row['asset'] = json.loads(history['snapshot']) if history else None
        if row['disposition'] == 'skipped':
            row['decision'] = store.one('SELECT action,actor,reason,at FROM decisions WHERE event_id=?', (row['event_id'],))
    return {
        'after': after, 'stable_ceiling': ceiling, 'rows': rows,
        'next_position': rows[-1]['position'] if rows else after,
        'has_more': bool(rows and rows[-1]['position'] < ceiling),
    }


def advance(store, consumer, position, revision, *, token=None):
    identifier(consumer, 'consumer')
    integer(position, 'position', 0)
    integer(revision, 'revision', 0)
    with store.transaction():
        if token is not None:
            held = leases.assert_held(store, token)
            if held['name'] != 'consumer:' + consumer:
                raise Conflict('Lease belongs to another consumer')
        previous = checkpoint(store, consumer)
        if previous['revision'] != revision:
            raise StaleRevision('Consumer checkpoint changed')
        if position < previous['position']:
            raise Conflict('Consumer checkpoint cannot move backwards')
        if position > stable_ceiling(store):
            raise Conflict('Cannot acknowledge pending changes')
        if position == previous['position']:
            return previous
        store.execute(
            'INSERT INTO checkpoints(consumer,position,revision,updated_at) VALUES(?,?,?,?) '
            'ON CONFLICT(consumer) DO UPDATE SET position=excluded.position,revision=excluded.revision,updated_at=excluded.updated_at',
            (consumer, position, revision + 1, store.clock()),
        )
        audit.append(store, 'consumer.advanced', consumer, {'from': previous['position'], 'to': position})
        return checkpoint(store, consumer)


def create_export(store, consumer, limit=100):
    with store.transaction():
        previous = checkpoint(store, consumer)
        batch = changes(store, previous['position'], limit)
        body = ''.join(canonical(row) + '\n' for row in batch['rows'])
        sha = hashlib.sha256(body.encode()).hexdigest()
        export_id = digest({
            'consumer': consumer, 'lower': previous['position'],
            'upper': batch['next_position'], 'digest': sha,
        })
        store.execute(
            'INSERT OR IGNORE INTO export_jobs(export_id,consumer,lower_position,upper_position,digest,row_count,created_at) '
            'VALUES(?,?,?,?,?,?,?)',
            (export_id, consumer, previous['position'], batch['next_position'], sha, len(batch['rows']), store.clock()),
        )
        return {
            'export_id': export_id, 'content': body, 'sha256': sha,
            'checkpoint_revision': previous['revision'], 'count': len(batch['rows']),
            'lower': previous['position'], 'upper': batch['next_position'],
        }


def acknowledge_export(store, export_id, sha256, revision, *, token=None):
    with store.transaction():
        row = store.one('SELECT * FROM export_jobs WHERE export_id=?', (export_id,))
        if row is None:
            raise NotFound('Export does not exist')
        if row['digest'] != sha256:
            raise Conflict('Export acknowledgement digest mismatch')
        if row['acknowledged_at']:
            return {'status': 'already_acknowledged', 'export_id': export_id}
        current = checkpoint(store, row['consumer'])
        if current['position'] != row['lower_position']:
            raise Conflict('Export no longer follows current checkpoint')
        updated = advance(store, row['consumer'], row['upper_position'], revision, token=token)
        store.execute('UPDATE export_jobs SET acknowledged_at=? WHERE export_id=?', (store.clock(), export_id))
        audit.append(store, 'export.acknowledged', row['consumer'], {'export_id': export_id})
        return {'status': 'acknowledged', 'checkpoint': updated}


def lag(store):
    ceiling = stable_ceiling(store)
    rows = store.all('SELECT * FROM checkpoints ORDER BY consumer')
    for row in rows:
        row['stable_ceiling'] = ceiling
        row['lag'] = ceiling - row['position']
    return rows
