from registry_bridge.errors import Conflict, NotFound, StaleRevision
from registry_bridge.feeds.message import identifier, integer, text
from registry_bridge.ledger import audit


def add(store, issuer, display_name, *, actor='operator'):
    issuer = identifier(issuer, 'issuer')
    display_name = text(display_name, 'display_name', 200)
    with store.transaction():
        existing = get(store, issuer, required=False)
        if existing:
            if existing['display_name'] == display_name:
                return existing
            raise Conflict('Issuer already registered under another display name')
        store.execute(
            'INSERT INTO issuers(issuer,display_name,enabled,created_at) VALUES(?,?,1,?)',
            (issuer, display_name, store.clock()),
        )
        audit.append(store, 'issuer.added', actor, {'issuer': issuer, 'display_name': display_name})
        return get(store, issuer)


def get(store, issuer, *, required=True):
    identifier(issuer, 'issuer')
    result = store.one('SELECT * FROM issuers WHERE issuer=?', (issuer,))
    if result is None and required:
        raise NotFound('Issuer is not registered', context={'issuer': issuer})
    return result


def set_enabled(store, issuer, enabled, revision, *, actor='operator'):
    integer(revision, 'revision')
    if not isinstance(enabled, bool):
        raise Conflict('Enabled flag must be boolean')
    with store.transaction():
        previous = get(store, issuer)
        if previous['revision'] != revision:
            raise StaleRevision('Issuer configuration changed')
        if bool(previous['enabled']) == enabled:
            return previous
        store.execute(
            'UPDATE issuers SET enabled=?,revision=revision+1 WHERE issuer=?',
            (int(enabled), issuer),
        )
        audit.append(store, 'issuer.enabled', actor, {'issuer': issuer, 'enabled': enabled})
        return get(store, issuer)


def listing(store):
    return store.all('SELECT * FROM issuers ORDER BY issuer')


def progress(store, issuer):
    row = get(store, issuer)
    aggregates = store.one(
        'SELECT COUNT(*) AS received,MAX(sequence) AS highest,MIN(sequence) AS first FROM events WHERE issuer=?',
        (issuer,),
    )
    pending = store.scalar(
        "SELECT COUNT(*) FROM events WHERE issuer=? AND disposition='pending'", (issuer,),
    )
    rejected = store.scalar(
        "SELECT COUNT(*) FROM events WHERE issuer=? AND disposition='rejected'", (issuer,),
    )
    return {
        'issuer': issuer,
        'enabled': bool(row['enabled']),
        'last_sequence': row['last_sequence'],
        'highest_received': aggregates['highest'] or 0,
        'received': aggregates['received'],
        'pending': pending,
        'rejected': rejected,
        'next_sequence': row['last_sequence'] + 1,
    }
