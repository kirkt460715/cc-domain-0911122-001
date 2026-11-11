import json

from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds.message import integer, timestamp
from registry_bridge.ledger import issuers
from registry_bridge.query import catalog, consumer
from registry_bridge.reconcile import cross_registry, resolution


def health_report(store, *, instant=None):
    instant = timestamp(instant or store.clock())
    streams = []
    warnings = []
    for config in issuers.listing(store):
        progress = issuers.progress(store, config['issuer'])
        blockage = resolution.explain_blockage(store, config['issuer'])
        missing = resolution.gaps(store, config['issuer'])
        streams.append({'progress': progress, 'blockage': blockage, 'gaps': missing['gaps']})
        if blockage['kind'] not in {'caught_up', 'pending'}:
            warnings.append({'issuer': config['issuer'], **blockage})
    expiring = []
    for row in store.all("SELECT issuer,asset_id,scope,state FROM assets WHERE state='active' ORDER BY issuer,asset_id"):
        scope = json.loads(row['scope'])
        if scope['expires_at'] <= instant:
            expiring.append({'issuer': row['issuer'], 'asset_id': row['asset_id'], 'expired_at': scope['expires_at']})
    return {
        'instant': instant,
        'metrics': catalog.metrics(store),
        'streams': streams,
        'warnings': warnings,
        'expired_active_registrations': expiring,
        'consumers': consumer.lag(store),
        'healthy': not warnings,
    }


def evidence_index(store, sha256):
    from registry_bridge.feeds.message import DIGEST
    if not isinstance(sha256, str) or not DIGEST.fullmatch(sha256):
        raise InvalidMessage('Evidence lookup requires lowercase SHA256')
    matches = []
    for row in store.all('SELECT issuer,asset_id,revision,evidence,state FROM assets ORDER BY issuer,asset_id'):
        evidence = json.loads(row['evidence'])
        kinds = [item['kind'] for item in evidence if item['sha256'] == sha256]
        if kinds:
            matches.append({
                'issuer': row['issuer'], 'asset_id': row['asset_id'],
                'revision': row['revision'], 'state': row['state'], 'kinds': kinds,
            })
    return {'sha256': sha256, 'references': matches}


def holder_inventory(store, holder, *, instant=None):
    from registry_bridge.feeds.message import identifier
    identifier(holder, 'holder')
    rows = []
    cursor = None
    while True:
        page = catalog.catalog(store, holder=holder, limit=1000, cursor=cursor, instant=instant)
        rows.extend(page['items'])
        cursor = page['next_cursor']
        if cursor is None:
            break
    state_counts = {'active': 0, 'suspended': 0, 'revoked': 0}
    unique_datasets = set()
    purposes = {}
    regions = {}
    for row in rows:
        state_counts[row['state']] += 1
        unique_datasets.add(row['dataset_fingerprint'])
        for purpose in row['scope']['purposes']:
            purposes[purpose] = purposes.get(purpose, 0) + 1
        for region in row['scope']['regions']:
            regions[region] = regions.get(region, 0) + 1
    return {
        'holder': holder,
        'registrations': len(rows),
        'datasets': len(unique_datasets),
        'states': state_counts,
        'purposes': dict(sorted(purposes.items())),
        'regions': dict(sorted(regions.items())),
        'assets': rows,
    }


def revision_gaps(store, issuer, asset_id):
    from registry_bridge.feeds.message import identifier
    identifier(issuer, 'issuer')
    identifier(asset_id, 'asset_id')
    rows = store.all(
        'SELECT revision,event_id,sequence,disposition FROM events WHERE issuer=? AND asset_id=? ORDER BY revision,sequence',
        (issuer, asset_id),
    )
    by_revision = {}
    for row in rows:
        by_revision.setdefault(row['revision'], []).append(row)
    duplicates = []
    missing = []
    previous = 0
    for revision, events in sorted(by_revision.items()):
        if revision > previous + 1:
            missing.append({'from': previous + 1, 'to': revision - 1})
        if len(events) > 1:
            duplicates.append({'revision': revision, 'events': events})
        previous = revision
    return {'issuer': issuer, 'asset_id': asset_id, 'missing': missing, 'duplicates': duplicates, 'events': rows}


def event_trace(store, event_id):
    from registry_bridge.feeds.receiver import event
    from registry_bridge.errors import NotFound
    row = event(store, event_id)
    if row is None:
        raise NotFound('Event does not exist')
    snapshot = store.one('SELECT snapshot FROM history WHERE event_id=?', (event_id,))
    relevant_audit = []
    for entry in store.all('SELECT audit_id,action,details,at FROM audit ORDER BY audit_id'):
        details = json.loads(entry['details'])
        if details.get('event_id') == event_id:
            relevant_audit.append({**entry, 'details': details})
    conflicts = store.all('SELECT conflict_id,kind,status,reason FROM conflicts WHERE existing_event=? ORDER BY conflict_id', (event_id,))
    return {
        'event': row,
        'snapshot': json.loads(snapshot['snapshot']) if snapshot else None,
        'audit': relevant_audit,
        'conflicts': conflicts,
        'decision': store.one('SELECT * FROM decisions WHERE event_id=?', (event_id,)),
    }


def expiry_schedule(store, start, end, *, limit=1000):
    start = timestamp(start, 'start')
    end = timestamp(end, 'end')
    integer(limit, 'limit', 1, 10000)
    if start >= end:
        raise InvalidMessage('Expiry interval must be nonempty')
    due = []
    for row in store.all("SELECT issuer,asset_id,holder,scope FROM assets WHERE state='active'"):
        expiration = json.loads(row['scope'])['expires_at']
        if start <= expiration < end:
            due.append({
                'issuer': row['issuer'], 'asset_id': row['asset_id'],
                'holder': row['holder'], 'expires_at': expiration,
            })
    due.sort(key=lambda item: (item['expires_at'], item['issuer'], item['asset_id']))
    return {'items': due[:limit], 'total': len(due), 'truncated': len(due) > limit, 'start': start, 'end': end}


def scope_change_impact(store, issuer, asset_id, before, after):
    left = catalog.asset(store, issuer, asset_id, revision=before)
    right = catalog.asset(store, issuer, asset_id, revision=after)
    removed = {}
    added = {}
    for dimension in ('purposes', 'regions', 'rights'):
        removed[dimension] = sorted(set(left['scope'][dimension]) - set(right['scope'][dimension]))
        added[dimension] = sorted(set(right['scope'][dimension]) - set(left['scope'][dimension]))
    return {
        'issuer': issuer,
        'asset_id': asset_id,
        'removed': removed,
        'added': added,
        'expiry_shortened': right['scope']['expires_at'] < left['scope']['expires_at'],
        'holder_changed': left['holder'] != right['holder'],
        'state_changed': left['state'] != right['state'],
        'requires_consumer_review': (
            any(removed.values())
            or right['scope']['expires_at'] < left['scope']['expires_at']
            or left['holder'] != right['holder']
            or right['state'] != 'active'
        ),
    }
