import json

from registry_bridge.errors import Conflict, RegistryError, StreamBlocked
from registry_bridge.feeds.message import Message, canonical, digest, integer, parse_message
from registry_bridge.ledger import audit, issuers, projection


def from_row(row):
    return parse_message({
        'version': 1,
        'event_id': row['event_id'],
        'issuer': row['issuer'],
        'sequence': row['sequence'],
        'asset_id': row['asset_id'],
        'revision': row['revision'],
        'operation': row['operation'],
        'effective_at': row['effective_at'],
        'payload': json.loads(row['payload']),
    })


def record_conflict(store, message, kind, existing, reason):
    value = message.as_dict()
    key = digest({'kind': kind, 'incoming': value, 'existing': existing})
    created = store.execute(
        'INSERT OR IGNORE INTO conflicts(conflict_id,kind,issuer,sequence,existing_event,incoming,reason,created_at) '
        'VALUES(?,?,?,?,?,?,?,?)',
        (key, kind, message.issuer, message.sequence, existing, canonical(value), reason, store.clock()),
    ).rowcount
    if created:
        audit.append(store, 'feed.conflict', 'receiver', {'conflict_id': key, 'kind': kind})
    return key


def receive(store, value, *, drain=True, limit=1000):
    message = value if isinstance(value, Message) else parse_message(value)
    with store.transaction():
        issuer = issuers.get(store, message.issuer)
        if not issuer['enabled']:
            raise StreamBlocked('Issuer is disabled', context={'issuer': message.issuer})
        existing = store.one('SELECT * FROM events WHERE event_id=?', (message.event_id,))
        if existing:
            if existing['fingerprint'] == message.fingerprint:
                return {'status': 'duplicate', 'event_id': message.event_id, 'position': existing['position']}
            conflict = record_conflict(store, message, 'event_identity', existing['event_id'], 'Event ID reused with different content')
            return {'status': 'conflict', 'conflict_id': conflict, 'event_id': message.event_id}
        occupied = store.one(
            'SELECT * FROM events WHERE issuer=? AND sequence=?',
            (message.issuer, message.sequence),
        )
        if occupied:
            conflict = record_conflict(store, message, 'sequence', occupied['event_id'], 'Issuer sequence already occupied')
            return {'status': 'conflict', 'conflict_id': conflict, 'event_id': message.event_id}
        if message.sequence <= issuer['last_sequence']:
            raise Conflict('Sequence is behind committed cursor but has no stored event')
        cursor = store.execute(
            'INSERT INTO events(event_id,issuer,sequence,asset_id,revision,operation,effective_at,received_at,payload,fingerprint) '
            'VALUES(?,?,?,?,?,?,?,?,?,?)',
            (message.event_id, message.issuer, message.sequence, message.asset_id, message.revision,
             message.operation, message.effective_at, store.clock(), message.payload_json, message.fingerprint),
        )
        position = cursor.lastrowid
        audit.append(store, 'feed.received', 'receiver', {
            'event_id': message.event_id, 'position': position, 'fingerprint': message.fingerprint,
        })
        applied = drain_stream(store, message.issuer, limit=limit) if drain else []
        row = store.one('SELECT disposition,rejection FROM events WHERE event_id=?', (message.event_id,))
        return {
            'status': 'accepted', 'event_id': message.event_id, 'position': position,
            'disposition': row['disposition'], 'rejection': row['rejection'],
            'advanced': applied,
        }


def drain_stream(store, issuer, *, limit=1000):
    integer(limit, 'limit', 1, 100000)
    advanced = []
    with store.transaction():
        config = issuers.get(store, issuer)
        if not config['enabled']:
            raise StreamBlocked('Issuer is disabled')
        sequence = config['last_sequence']
        for _ in range(limit):
            row = store.one(
                'SELECT * FROM events WHERE issuer=? AND sequence=?', (issuer, sequence + 1),
            )
            if row is None:
                break
            if row['disposition'] == 'rejected':
                break
            if row['disposition'] == 'skipped':
                sequence += 1
                advanced.append({'event_id': row['event_id'], 'status': 'skipped'})
                store.execute('UPDATE issuers SET last_sequence=? WHERE issuer=?', (sequence, issuer))
                continue
            if row['disposition'] != 'pending':
                raise Conflict('Cursor points at an already applied event')
            message = from_row(row)
            previous = projection.current(store, issuer, message.asset_id)
            try:
                state = projection.evolve(previous, message)
            except RegistryError as exc:
                rejection = canonical(exc.as_dict())
                store.execute(
                    "UPDATE events SET disposition='rejected',rejection=? WHERE event_id=?",
                    (rejection, message.event_id),
                )
                audit.append(store, 'feed.rejected', 'projector', {
                    'event_id': message.event_id, 'error': exc.as_dict(),
                })
                advanced.append({'event_id': message.event_id, 'status': 'rejected', 'error': exc.as_dict()})
                break
            projection.persist(store, state)
            store.execute(
                "UPDATE events SET disposition='applied',commit_position=(SELECT COALESCE(MAX(commit_position),0)+1 FROM events),applied_at=? WHERE event_id=?",
                (store.clock(), message.event_id),
            )
            sequence += 1
            store.execute('UPDATE issuers SET last_sequence=? WHERE issuer=?', (sequence, issuer))
            audit.append(store, 'asset.projected', 'projector', {
                'event_id': message.event_id, 'issuer': issuer, 'sequence': sequence,
                'changes': projection.diff(previous, state),
            })
            advanced.append({'event_id': message.event_id, 'status': 'applied'})
    return advanced


def event(store, event_id):
    row = store.one('SELECT * FROM events WHERE event_id=?', (event_id,))
    if row:
        row['payload'] = json.loads(row['payload'])
        row['rejection'] = json.loads(row['rejection']) if row['rejection'] else None
    return row


def pending(store, issuer, limit=100):
    integer(limit, 'limit', 1, 10000)
    return store.all(
        "SELECT event_id,sequence,asset_id,disposition,rejection FROM events "
        "WHERE issuer=? AND disposition IN ('pending','rejected') ORDER BY sequence LIMIT ?",
        (issuer, limit),
    )
