import json

from registry_bridge.errors import InvalidTransition, StaleRevision
from registry_bridge.feeds.message import canonical


def decode_asset(row):
    if row is None:
        return None
    result = dict(row)
    result['scope'] = json.loads(result['scope'])
    result['evidence'] = json.loads(result['evidence'])
    return result


def current(store, issuer, asset_id):
    return decode_asset(store.one(
        'SELECT * FROM assets WHERE issuer=? AND asset_id=?', (issuer, asset_id),
    ))


def evolve(previous, message):
    payload = message.payload
    operation = message.operation
    if previous is None:
        if operation != 'register':
            raise InvalidTransition('Asset must be registered before any other event')
        state = {
            'issuer': message.issuer,
            'asset_id': message.asset_id,
            'state': 'active',
            **payload,
        }
    else:
        if operation == 'register':
            raise InvalidTransition('Asset is already registered')
        if message.revision != previous['revision'] + 1:
            raise StaleRevision('Asset revision must advance exactly once', context={
                'expected': previous['revision'] + 1, 'received': message.revision,
            })
        if message.effective_at < previous['effective_at']:
            raise InvalidTransition('Effective time cannot move backwards within an asset')
        if previous['state'] == 'revoked':
            raise InvalidTransition('Revocation is terminal')
        state = dict(previous)
        if operation == 'amend':
            state.update(payload['changes'])
        elif operation == 'suspend':
            if state['state'] != 'active':
                raise InvalidTransition('Only active registrations can be suspended')
            state['state'] = 'suspended'
        elif operation == 'restore':
            if state['state'] != 'suspended':
                raise InvalidTransition('Only suspended registrations can be restored')
            state['state'] = 'active'
        elif operation == 'revoke':
            state['state'] = 'revoked'
    if operation in {'register', 'amend', 'restore'}:
        if state['scope']['expires_at'] <= message.effective_at:
            raise InvalidTransition('Scope is expired at the event effective time')
    state['revision'] = message.revision
    state['effective_at'] = message.effective_at
    state['event_id'] = message.event_id
    return state


def persist(store, state):
    columns = [
        'issuer', 'asset_id', 'revision', 'state', 'title', 'holder',
        'scope', 'evidence', 'dataset_fingerprint', 'effective_at', 'event_id',
    ]
    values = [canonical(state[key]) if key in {'scope', 'evidence'} else state[key] for key in columns]
    updates = ','.join(f'{column}=excluded.{column}' for column in columns[2:])
    store.execute(
        f"INSERT INTO assets({','.join(columns)}) VALUES({','.join('?' for _ in columns)}) "
        f'ON CONFLICT(issuer,asset_id) DO UPDATE SET {updates}',
        values,
    )
    store.execute(
        'INSERT INTO history(event_id,issuer,asset_id,revision,effective_at,snapshot) VALUES(?,?,?,?,?,?)',
        (state['event_id'], state['issuer'], state['asset_id'], state['revision'],
         state['effective_at'], canonical(state)),
    )


def at_time(store, issuer, asset_id, instant):
    row = store.one(
        'SELECT snapshot FROM history WHERE issuer=? AND asset_id=? AND effective_at<=? '
        'ORDER BY effective_at DESC,revision DESC LIMIT 1',
        (issuer, asset_id, instant),
    )
    return json.loads(row['snapshot']) if row else None


def at_revision(store, issuer, asset_id, revision):
    row = store.one(
        'SELECT snapshot FROM history WHERE issuer=? AND asset_id=? AND revision=?',
        (issuer, asset_id, revision),
    )
    return json.loads(row['snapshot']) if row else None


def history(store, issuer, asset_id):
    return [json.loads(row['snapshot']) for row in store.all(
        'SELECT snapshot FROM history WHERE issuer=? AND asset_id=? ORDER BY revision',
        (issuer, asset_id),
    )]


def diff(before, after):
    keys = sorted(set(before or {}) | set(after or {}))
    changes = []
    for key in keys:
        left = (before or {}).get(key)
        right = (after or {}).get(key)
        if left != right:
            changes.append({'field': key, 'before': left, 'after': right})
    return changes
