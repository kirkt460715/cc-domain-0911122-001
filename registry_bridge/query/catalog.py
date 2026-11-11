import base64
import json

from registry_bridge.errors import InvalidMessage, NotFound
from registry_bridge.feeds.message import canonical, identifier, integer, timestamp
from registry_bridge.ledger.projection import at_revision, at_time, current, decode_asset, diff, history


def encode_cursor(issuer, asset_id):
    return base64.urlsafe_b64encode(canonical([issuer, asset_id]).encode()).decode().rstrip('=')


def decode_cursor(value):
    if not isinstance(value, str) or len(value) > 1024:
        raise InvalidMessage('Invalid pagination cursor')
    try:
        raw = base64.b64decode(value + '=' * (-len(value) % 4), altchars=b'-_', validate=True)
        decoded = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise InvalidMessage('Malformed pagination cursor') from exc
    if not isinstance(decoded, list) or len(decoded) != 2:
        raise InvalidMessage('Cursor must contain an issuer and asset key')
    return identifier(decoded[0], 'cursor.issuer'), identifier(decoded[1], 'cursor.asset_id')


def asset(store, issuer, asset_id, *, instant=None, revision=None):
    identifier(issuer, 'issuer')
    identifier(asset_id, 'asset_id')
    if instant is not None and revision is not None:
        raise InvalidMessage('Choose effective time or revision, not both')
    if instant is not None:
        result = at_time(store, issuer, asset_id, timestamp(instant))
    elif revision is not None:
        result = at_revision(store, issuer, asset_id, integer(revision, 'revision'))
    else:
        result = current(store, issuer, asset_id)
    if result is None:
        raise NotFound('Asset view does not exist')
    return result


def catalog(store, *, issuer=None, holder=None, state=None, limit=100, cursor=None, instant=None):
    integer(limit, 'limit', 1, 1000)
    filters = []
    params = []
    if issuer is not None:
        filters.append('issuer=?')
        params.append(identifier(issuer, 'issuer'))
    if holder is not None:
        filters.append('holder=?')
        params.append(identifier(holder, 'holder'))
    if state is not None:
        if state not in {'active', 'suspended', 'revoked'}:
            raise InvalidMessage('Unknown registration state')
        filters.append('state=?')
        params.append(state)
    if cursor:
        lower_issuer, lower_asset = decode_cursor(cursor)
        filters.append('(issuer>? OR (issuer=? AND asset_id>?))')
        params.extend([lower_issuer, lower_issuer, lower_asset])
    if instant is not None:
        instant = timestamp(instant)
        rows = store.all(
            'SELECT h.snapshot FROM history h WHERE h.effective_at<=? AND NOT EXISTS('
            'SELECT 1 FROM history n WHERE n.issuer=h.issuer AND n.asset_id=h.asset_id '
            'AND n.effective_at<=? AND n.revision>h.revision) ORDER BY h.issuer,h.asset_id',
            (instant, instant),
        )
        candidates = []
        for row in rows:
            item = json.loads(row['snapshot'])
            if issuer and item['issuer'] != issuer:
                continue
            if holder and item['holder'] != holder:
                continue
            if state and item['state'] != state:
                continue
            if cursor and (item['issuer'], item['asset_id']) <= decode_cursor(cursor):
                continue
            candidates.append(item)
            if len(candidates) > limit:
                break
    else:
        where = ' WHERE ' + ' AND '.join(filters) if filters else ''
        candidates = [decode_asset(row) for row in store.all(
            f'SELECT * FROM assets{where} ORDER BY issuer,asset_id LIMIT ?',
            [*params, limit + 1],
        )]
    has_more = len(candidates) > limit
    result = candidates[:limit]
    next_cursor = None
    if has_more and result:
        next_cursor = encode_cursor(result[-1]['issuer'], result[-1]['asset_id'])
    return {'items': result, 'next_cursor': next_cursor, 'count': len(result)}


def authorize(store, issuer, asset_id, *, purpose, region, right, instant):
    instant = timestamp(instant)
    snapshot = asset(store, issuer, asset_id, instant=instant)
    reasons = []
    scope = snapshot['scope']
    if snapshot['state'] != 'active':
        reasons.append({'code': 'inactive', 'state': snapshot['state']})
    if scope['expires_at'] <= instant:
        reasons.append({'code': 'expired', 'expires_at': scope['expires_at']})
    if purpose not in scope['purposes']:
        reasons.append({'code': 'purpose_denied', 'requested': purpose})
    if region not in scope['regions']:
        reasons.append({'code': 'region_denied', 'requested': region})
    if right not in scope['rights']:
        reasons.append({'code': 'right_denied', 'requested': right})
    return {
        'allowed': not reasons,
        'issuer': issuer,
        'asset_id': asset_id,
        'revision': snapshot['revision'],
        'event_id': snapshot['event_id'],
        'instant': instant,
        'reasons': reasons,
        'interpretation': 'local_declared_scope_only',
    }


def compare_revisions(store, issuer, asset_id, before, after):
    left = asset(store, issuer, asset_id, revision=before)
    right = asset(store, issuer, asset_id, revision=after)
    return {
        'issuer': issuer,
        'asset_id': asset_id,
        'before_revision': before,
        'after_revision': after,
        'changes': diff(left, right),
    }


def timeline(store, issuer, asset_id):
    versions = history(store, identifier(issuer, 'issuer'), identifier(asset_id, 'asset_id'))
    if not versions:
        raise NotFound('Asset does not exist')
    result = []
    previous = None
    for index, snapshot in enumerate(versions):
        following = versions[index + 1] if index + 1 < len(versions) else None
        result.append({
            'revision': snapshot['revision'],
            'valid_from': snapshot['effective_at'],
            'valid_until': following['effective_at'] if following else None,
            'state': snapshot['state'],
            'event_id': snapshot['event_id'],
            'changes': diff(previous, snapshot),
        })
        previous = snapshot
    return result


def metrics(store):
    states = store.all('SELECT state,COUNT(*) AS count FROM assets GROUP BY state ORDER BY state')
    dispositions = store.all('SELECT disposition,COUNT(*) AS count FROM events GROUP BY disposition ORDER BY disposition')
    return {
        'issuers': store.scalar('SELECT COUNT(*) FROM issuers'),
        'assets': store.scalar('SELECT COUNT(*) FROM assets'),
        'events': store.scalar('SELECT COUNT(*) FROM events'),
        'states': {row['state']: row['count'] for row in states},
        'dispositions': {row['disposition']: row['count'] for row in dispositions},
        'open_conflicts': store.scalar("SELECT COUNT(*) FROM conflicts WHERE status='open'"),
        'unreplayed_dead_letters': store.scalar('SELECT COUNT(*) FROM dead_letters WHERE replayed_as IS NULL'),
    }
