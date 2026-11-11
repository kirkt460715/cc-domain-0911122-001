import hashlib
import json

from registry_bridge.errors import Conflict, IntegrityFailure, InvalidMessage
from registry_bridge.feeds.message import canonical, digest, identifier, integer, parse_message, strict_object
from registry_bridge.feeds.receiver import from_row, receive
from registry_bridge.ledger import audit, issuers


def create_bundle(store, issuer, lower, upper):
    identifier(issuer, 'issuer')
    integer(lower, 'lower', 1)
    integer(upper, 'upper', lower, lower + 9999)
    with store.transaction(immediate=False):
        config = issuers.get(store, issuer)
        rows = store.all(
            'SELECT * FROM events WHERE issuer=? AND sequence>=? AND sequence<=? ORDER BY sequence',
            (issuer, lower, upper),
        )
        if len(rows) != upper - lower + 1:
            raise Conflict('Cannot export a bundle with sequence gaps')
        messages = [from_row(row).as_dict() for row in rows]
        body = {
            'format': 'registry-event-bundle',
            'version': 1,
            'issuer': issuer,
            'display_name': config['display_name'],
            'lower': lower,
            'upper': upper,
            'messages': messages,
        }
        return {'body': body, 'sha256': digest(body)}


def verify_bundle(value):
    strict_object(value, {'body', 'sha256'}, {'body', 'sha256'}, 'bundle')
    body = value['body']
    fields = {'format', 'version', 'issuer', 'display_name', 'lower', 'upper', 'messages'}
    strict_object(body, fields, fields, 'bundle.body')
    if body['format'] != 'registry-event-bundle' or type(body['version']) is not int or body['version'] != 1:
        raise InvalidMessage('Unsupported event bundle format')
    issuer = identifier(body['issuer'], 'issuer')
    lower = integer(body['lower'], 'lower', 1)
    upper = integer(body['upper'], 'upper', lower, lower + 9999)
    if not isinstance(body['messages'], list) or len(body['messages']) != upper - lower + 1:
        raise IntegrityFailure('Bundle sequence count mismatch')
    if digest(body) != value['sha256']:
        raise IntegrityFailure('Bundle content digest mismatch')
    seen = set()
    messages = []
    for sequence, raw in enumerate(body['messages'], lower):
        message = parse_message(raw)
        if message.issuer != issuer or message.sequence != sequence:
            raise IntegrityFailure('Bundle sequence or issuer mismatch')
        if message.event_id in seen:
            raise IntegrityFailure('Bundle repeats an event identifier')
        seen.add(message.event_id)
        messages.append(message)
    return messages


def import_bundle(store, value, *, allow_register=False, actor='operator'):
    messages = verify_bundle(value)
    body = value['body']
    with store.transaction():
        existing = issuers.get(store, body['issuer'], required=False)
        if existing is None:
            if not allow_register:
                raise Conflict('Bundle issuer must be registered explicitly')
            issuers.add(store, body['issuer'], body['display_name'], actor=actor)
        elif existing['display_name'] != body['display_name']:
            raise Conflict('Bundle issuer description disagrees with local registry')
        results = []
        for message in messages:
            result = receive(store, message)
            if result['status'] == 'conflict':
                raise Conflict('Bundle conflicts with existing immutable events', context=result)
            results.append(result)
        audit.append(store, 'bundle.imported', actor, {
            'sha256': value['sha256'],
            'issuer': body['issuer'],
            'lower': body['lower'],
            'upper': body['upper'],
        })
        return {
            'accepted': sum(row['status'] == 'accepted' for row in results),
            'duplicate': sum(row['status'] == 'duplicate' for row in results),
            'sha256': value['sha256'],
            'progress': issuers.progress(store, body['issuer']),
        }


def verify_remote_summary(store, issuer, summary):
    fields = {'last_sequence', 'event_count', 'event_digest'}
    strict_object(summary, fields, fields, 'remote_summary')
    integer(summary['last_sequence'], 'last_sequence', 0)
    integer(summary['event_count'], 'event_count', 0)
    local = stream_summary(store, issuer, ceiling=summary['last_sequence'])
    differences = {}
    for key in sorted(fields):
        if local[key] != summary[key]:
            differences[key] = {'local': local[key], 'remote': summary[key]}
    return {'matches': not differences, 'differences': differences, 'local': local}


def stream_summary(store, issuer, *, ceiling=None):
    issuers.get(store, issuer)
    if ceiling is None:
        ceiling = store.scalar('SELECT MAX(sequence) FROM events WHERE issuer=?', (issuer,)) or 0
    integer(ceiling, 'ceiling', 0)
    rows = store.all(
        'SELECT sequence,fingerprint FROM events WHERE issuer=? AND sequence<=? ORDER BY sequence',
        (issuer, ceiling),
    )
    accumulator = hashlib.sha256()
    for row in rows:
        accumulator.update(canonical([row['sequence'], row['fingerprint']]).encode())
        accumulator.update(b'\n')
    return {
        'last_sequence': rows[-1]['sequence'] if rows else 0,
        'event_count': len(rows),
        'event_digest': accumulator.hexdigest(),
    }


def differing_ranges(store, issuer, remote_segments):
    if not isinstance(remote_segments, list) or len(remote_segments) > 1000:
        raise InvalidMessage('Segment summary must be a bounded list')
    result = []
    previous = 0
    for segment in remote_segments:
        strict_object(segment, {'lower', 'upper', 'sha256'}, {'lower', 'upper', 'sha256'}, 'segment')
        lower = integer(segment['lower'], 'lower', previous + 1)
        upper = integer(segment['upper'], 'upper', lower, lower + 9999)
        previous = upper
        rows = store.all(
            'SELECT sequence,fingerprint FROM events WHERE issuer=? AND sequence BETWEEN ? AND ? ORDER BY sequence',
            (issuer, lower, upper),
        )
        checksum = digest([[row['sequence'], row['fingerprint']] for row in rows])
        if checksum != segment['sha256']:
            result.append({'lower': lower, 'upper': upper, 'local_count': len(rows), 'local_sha256': checksum})
    return result


def segments(store, issuer, width=1000):
    integer(width, 'width', 1, 10000)
    issuers.get(store, issuer)
    highest = store.scalar('SELECT MAX(sequence) FROM events WHERE issuer=?', (issuer,)) or 0
    result = []
    for lower in range(1, highest + 1, width):
        upper = min(highest, lower + width - 1)
        rows = store.all(
            'SELECT sequence,fingerprint FROM events WHERE issuer=? AND sequence BETWEEN ? AND ? ORDER BY sequence',
            (issuer, lower, upper),
        )
        result.append({
            'lower': lower,
            'upper': upper,
            'sha256': digest([[row['sequence'], row['fingerprint']] for row in rows]),
        })
    return result
