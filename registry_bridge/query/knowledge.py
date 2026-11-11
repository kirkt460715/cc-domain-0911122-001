import json

from registry_bridge.errors import InvalidMessage, NotFound
from registry_bridge.feeds.message import identifier, integer, timestamp
from registry_bridge.ledger.projection import evolve
from registry_bridge.feeds.receiver import from_row


def view_at_knowledge(store, issuer, asset_id, known_at, effective_at):
    identifier(issuer, 'issuer')
    identifier(asset_id, 'asset_id')
    known_at = timestamp(known_at, 'known_at')
    effective_at = timestamp(effective_at, 'effective_at')
    rows = store.all(
        "SELECT * FROM events WHERE issuer=? AND asset_id=? AND disposition='applied' "
        'AND applied_at<=? AND effective_at<=? ORDER BY revision',
        (issuer, asset_id, known_at, effective_at),
    )
    state = None
    included = []
    for row in rows:
        state = evolve(state, from_row(row))
        included.append(row['event_id'])
    if state is None:
        raise NotFound('No registration known at the requested times')
    return {'asset': state, 'known_at': known_at, 'effective_at': effective_at, 'included_events': included}


def late_arrivals(store, start, end, *, threshold_seconds=0, limit=100):
    import datetime as dt
    start = timestamp(start, 'start')
    end = timestamp(end, 'end')
    integer(threshold_seconds, 'threshold_seconds', 0, 10**10)
    integer(limit, 'limit', 1, 10000)
    if start >= end:
        raise InvalidMessage('Receipt interval must be nonempty')
    rows = store.all(
        'SELECT event_id,issuer,asset_id,effective_at,received_at,applied_at,disposition '
        'FROM events WHERE received_at>=? AND received_at<? ORDER BY received_at,position',
        (start, end),
    )
    result = []
    for row in rows:
        received = dt.datetime.fromisoformat(row['received_at'].replace('Z', '+00:00'))
        effective = dt.datetime.fromisoformat(row['effective_at'].replace('Z', '+00:00'))
        delay = (received - effective).total_seconds()
        if delay >= threshold_seconds:
            result.append({**row, 'delay_seconds': delay})
    result.sort(key=lambda row: (-row['delay_seconds'], row['event_id']))
    return {'items': result[:limit], 'total': len(result), 'threshold_seconds': threshold_seconds}


def changed_between(store, before, after, *, issuer=None):
    before = timestamp(before, 'before')
    after = timestamp(after, 'after')
    if before >= after:
        raise InvalidMessage('Knowledge interval must be increasing')
    clause = "disposition='applied' AND applied_at>? AND applied_at<=?"
    params = [before, after]
    if issuer is not None:
        clause += ' AND issuer=?'
        params.append(identifier(issuer, 'issuer'))
    rows = store.all(
        f'SELECT issuer,asset_id,MIN(revision) AS first_revision,MAX(revision) AS last_revision,COUNT(*) AS changes '
        f'FROM events WHERE {clause} GROUP BY issuer,asset_id ORDER BY issuer,asset_id', params,
    )
    return {'before': before, 'after': after, 'assets': rows, 'count': len(rows)}


def observed_lifecycle(store, issuer, asset_id):
    identifier(issuer, 'issuer')
    identifier(asset_id, 'asset_id')
    rows = store.all(
        'SELECT event_id,revision,operation,effective_at,received_at,applied_at,disposition,rejection '
        'FROM events WHERE issuer=? AND asset_id=? ORDER BY sequence',
        (issuer, asset_id),
    )
    if not rows:
        raise NotFound('No events for requested asset')
    for row in rows:
        row['rejection'] = json.loads(row['rejection']) if row['rejection'] else None
        row['visible'] = row['disposition'] == 'applied'
    return {'issuer': issuer, 'asset_id': asset_id, 'events': rows}
