import datetime as dt

from registry_bridge.errors import Conflict, LeaseLost
from registry_bridge.feeds.message import identifier, integer, timestamp
from registry_bridge.ledger import audit


def expires(now, ttl):
    integer(ttl, 'ttl', 1, 86400)
    point = dt.datetime.fromisoformat(now.replace('Z', '+00:00'))
    return (point + dt.timedelta(seconds=ttl)).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def acquire(store, name, owner, ttl=30):
    identifier(name, 'lease')
    identifier(owner, 'owner')
    with store.transaction():
        now = timestamp(store.clock())
        expiration = expires(now, ttl)
        row = store.one('SELECT * FROM leases WHERE name=?', (name,))
        if row and row['expires_at'] > now:
            raise Conflict('Lease is held', context={'owner': row['owner'], 'expires_at': row['expires_at']})
        generation = row['generation'] + 1 if row else 1
        store.execute(
            'INSERT INTO leases(name,owner,generation,expires_at) VALUES(?,?,?,?) '
            'ON CONFLICT(name) DO UPDATE SET owner=excluded.owner,generation=excluded.generation,expires_at=excluded.expires_at',
            (name, owner, generation, expiration),
        )
        audit.append(store, 'lease.acquired', owner, {'name': name, 'generation': generation})
        return {'name': name, 'owner': owner, 'generation': generation, 'expires_at': expiration}


def assert_held(store, token):
    if not isinstance(token, dict):
        raise LeaseLost('Invalid lease token')
    row = store.one('SELECT * FROM leases WHERE name=?', (token.get('name'),))
    if row is None:
        raise LeaseLost('Lease does not exist')
    if row['owner'] != token.get('owner') or row['generation'] != token.get('generation'):
        raise LeaseLost('Lease fencing token is stale')
    if row['expires_at'] <= timestamp(store.clock()):
        raise LeaseLost('Lease expired')
    return row


def renew(store, token, ttl=30):
    with store.transaction():
        row = assert_held(store, token)
        expiration = expires(timestamp(store.clock()), ttl)
        store.execute('UPDATE leases SET expires_at=? WHERE name=?', (expiration, row['name']))
        return {**row, 'expires_at': expiration}


def release(store, token):
    with store.transaction():
        row = assert_held(store, token)
        store.execute('UPDATE leases SET expires_at=? WHERE name=?', (timestamp(store.clock()), row['name']))
        audit.append(store, 'lease.released', row['owner'], {'name': row['name'], 'generation': row['generation']})
        return {'released': True, 'generation': row['generation']}


def list_leases(store):
    now = timestamp(store.clock())
    rows = store.all('SELECT * FROM leases ORDER BY name')
    for row in rows:
        row['active'] = row['expires_at'] > now
    return rows
