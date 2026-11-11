import hashlib
import json

from registry_bridge.errors import IntegrityFailure
from registry_bridge.feeds.message import canonical, identifier, integer

GENESIS = '0' * 64


def append(store, action, actor, details):
    identifier(action, 'action')
    identifier(actor, 'actor')
    previous = store.scalar('SELECT entry_hash FROM audit ORDER BY audit_id DESC LIMIT 1')
    previous = previous or GENESIS
    at = store.clock()
    payload = canonical(details)
    material = canonical([previous, action, actor, payload, at])
    entry_hash = hashlib.sha256(material.encode()).hexdigest()
    cursor = store.execute(
        'INSERT INTO audit(action,actor,details,at,previous_hash,entry_hash) VALUES(?,?,?,?,?,?)',
        (action, actor, payload, at, previous, entry_hash),
    )
    return cursor.lastrowid


def verify(store):
    previous = GENESIS
    count = 0
    for row in store.execute('SELECT * FROM audit ORDER BY audit_id'):
        material = canonical([previous, row['action'], row['actor'], row['details'], row['at']])
        expected = hashlib.sha256(material.encode()).hexdigest()
        if row['previous_hash'] != previous or row['entry_hash'] != expected:
            return {'ok': False, 'entries': count, 'broken_at': row['audit_id']}
        previous = expected
        count += 1
    return {'ok': True, 'entries': count, 'head': previous}


def page(store, after=0, limit=100):
    integer(after, 'after', 0)
    integer(limit, 'limit', 1, 1000)
    rows = store.all('SELECT * FROM audit WHERE audit_id>? ORDER BY audit_id LIMIT ?', (after, limit))
    for row in rows:
        row['details'] = json.loads(row['details'])
    return rows


def require_valid(store):
    result = verify(store)
    if not result['ok']:
        raise IntegrityFailure('Audit chain is broken', context=result)
    return result
