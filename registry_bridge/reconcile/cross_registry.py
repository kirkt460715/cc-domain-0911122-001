from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds.message import DIGEST, integer, timestamp
from registry_bridge.ledger.projection import decode_asset


def intersect_scope(left, right):
    return {
        'purposes': sorted(set(left['purposes']) & set(right['purposes'])),
        'regions': sorted(set(left['regions']) & set(right['regions'])),
        'rights': sorted(set(left['rights']) & set(right['rights'])),
        'expires_at': min(left['expires_at'], right['expires_at']),
    }


def scope_contains(outer, inner):
    return (
        set(outer['purposes']) >= set(inner['purposes'])
        and set(outer['regions']) >= set(inner['regions'])
        and set(outer['rights']) >= set(inner['rights'])
        and outer['expires_at'] >= inner['expires_at']
    )


def compare(left, right):
    common = intersect_scope(left['scope'], right['scope'])
    mismatches = []
    if left['holder'] != right['holder']:
        mismatches.append('holder')
    if left['state'] != right['state']:
        mismatches.append('state')
    if left['scope'] != right['scope']:
        mismatches.append('scope')
    if left['evidence'] != right['evidence']:
        mismatches.append('evidence')
    overlapping = bool(common['purposes'] and common['regions'] and common['rights'])
    if left['holder'] != right['holder'] and overlapping:
        classification = 'overlapping_claims'
    elif left['holder'] != right['holder']:
        classification = 'disjoint_claims'
    elif not mismatches:
        classification = 'equivalent'
    elif scope_contains(left['scope'], right['scope']):
        classification = 'left_contains_right'
    elif scope_contains(right['scope'], left['scope']):
        classification = 'right_contains_left'
    else:
        classification = 'divergent'
    return {
        'left': {'issuer': left['issuer'], 'asset_id': left['asset_id'], 'revision': left['revision']},
        'right': {'issuer': right['issuer'], 'asset_id': right['asset_id'], 'revision': right['revision']},
        'classification': classification,
        'mismatches': mismatches,
        'intersection': common,
        'requires_review': classification not in {'equivalent', 'disjoint_claims'},
    }


def dataset_claims(store, fingerprint, *, instant=None):
    if not isinstance(fingerprint, str) or not DIGEST.fullmatch(fingerprint):
        raise InvalidMessage('Invalid dataset fingerprint')
    rows = store.all(
        'SELECT * FROM assets WHERE dataset_fingerprint=? ORDER BY issuer,asset_id',
        (fingerprint,),
    )
    claims = [decode_asset(row) for row in rows]
    if instant is not None:
        import json
        instant = timestamp(instant)
        histories = store.all(
            'SELECT h.snapshot FROM history h WHERE h.effective_at<=? AND NOT EXISTS('
            'SELECT 1 FROM history n WHERE n.issuer=h.issuer AND n.asset_id=h.asset_id '
            'AND n.effective_at<=? AND n.revision>h.revision) ORDER BY h.issuer,h.asset_id',
            (instant, instant),
        )
        claims = [json.loads(row['snapshot']) for row in histories]
        claims = [row for row in claims if row['dataset_fingerprint'] == fingerprint]
    comparisons = []
    for index, left in enumerate(claims):
        for right in claims[index + 1:]:
            if left['issuer'] != right['issuer']:
                comparisons.append(compare(left, right))
    return {
        'fingerprint': fingerprint,
        'claims': claims,
        'comparisons': comparisons,
        'review_count': sum(item['requires_review'] for item in comparisons),
        'authoritative': False,
    }


def scan(store, *, limit=100, after=None):
    integer(limit, 'limit', 1, 1000)
    params = []
    clause = ''
    if after is not None:
        if not isinstance(after, str) or not DIGEST.fullmatch(after):
            raise InvalidMessage('Invalid scan cursor')
        clause = 'WHERE dataset_fingerprint>?'
        params.append(after)
    params.append(limit + 1)
    rows = store.all(
        f'SELECT dataset_fingerprint,COUNT(DISTINCT issuer) AS issuers FROM assets {clause} '
        'GROUP BY dataset_fingerprint HAVING COUNT(DISTINCT issuer)>1 '
        'ORDER BY dataset_fingerprint LIMIT ?',
        params,
    )
    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        'items': [dataset_claims(store, row['dataset_fingerprint']) for row in rows],
        'next_cursor': rows[-1]['dataset_fingerprint'] if has_more else None,
    }
