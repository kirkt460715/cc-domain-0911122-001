import json

from registry_bridge.errors import IntegrityFailure
from registry_bridge.feeds.message import canonical, digest
from registry_bridge.feeds.receiver import from_row
from registry_bridge.ledger import audit, projection


def expected_views(store):
    assets = {}
    history = {}
    errors = []
    cursors = {}
    for issuer in store.all('SELECT issuer,last_sequence FROM issuers ORDER BY issuer'):
        expected_sequence = 1
        rows = store.all('SELECT * FROM events WHERE issuer=? ORDER BY sequence', (issuer['issuer'],))
        for row in rows:
            if row['sequence'] > issuer['last_sequence']:
                break
            if row['sequence'] != expected_sequence:
                errors.append({'kind': 'cursor_gap', 'issuer': issuer['issuer'], 'sequence': expected_sequence})
                break
            expected_sequence += 1
            if row['disposition'] == 'skipped':
                decision = store.one('SELECT action FROM decisions WHERE event_id=?', (row['event_id'],))
                if decision is None or decision['action'] != 'skip':
                    errors.append({'kind': 'missing_skip_decision', 'event_id': row['event_id']})
                continue
            if row['disposition'] != 'applied':
                errors.append({'kind': 'cursor_disposition', 'event_id': row['event_id']})
                continue
            message = from_row(row)
            if message.fingerprint != row['fingerprint']:
                errors.append({'kind': 'event_digest', 'event_id': row['event_id']})
                continue
            key = (message.issuer, message.asset_id)
            try:
                state = projection.evolve(assets.get(key), message)
            except Exception as exc:
                errors.append({'kind': 'replay_failure', 'event_id': row['event_id'], 'message': str(exc)})
                continue
            assets[key] = state
            history[message.event_id] = state
        cursors[issuer['issuer']] = expected_sequence - 1
        if expected_sequence - 1 != issuer['last_sequence']:
            errors.append({'kind': 'cursor_mismatch', 'issuer': issuer['issuer']})
    return {'assets': assets, 'history': history, 'errors': errors, 'cursors': cursors}


def check(store):
    with store.transaction(immediate=False):
        expected = expected_views(store)
        actual_assets = {
            (row['issuer'], row['asset_id']): projection.decode_asset(row)
            for row in store.all('SELECT * FROM assets')
        }
        actual_history = {
            row['event_id']: json.loads(row['snapshot'])
            for row in store.all('SELECT * FROM history')
        }
        errors = list(expected['errors'])
        for key in sorted(set(expected['assets']) | set(actual_assets)):
            if expected['assets'].get(key) != actual_assets.get(key):
                errors.append({'kind': 'asset_projection', 'issuer': key[0], 'asset_id': key[1]})
        for event_id in sorted(set(expected['history']) | set(actual_history)):
            if expected['history'].get(event_id) != actual_history.get(event_id):
                errors.append({'kind': 'history_projection', 'event_id': event_id})
        audit_result = audit.verify(store)
        if not audit_result['ok']:
            errors.append({'kind': 'audit', **audit_result})
        integrity = store.integrity()
        if not integrity['ok']:
            errors.append({'kind': 'sqlite_integrity', **integrity})
        return {
            'ok': not errors,
            'errors': errors,
            'assets_checked': len(expected['assets']),
            'history_checked': len(expected['history']),
            'audit': audit_result,
        }


def rebuild(store, *, actor='operator'):
    with store.transaction():
        audit.require_valid(store)
        expected = expected_views(store)
        if expected['errors']:
            raise IntegrityFailure('Source ledger cannot be replayed', context={'errors': expected['errors']})
        before = check(store)
        store.execute('DELETE FROM history')
        store.execute('DELETE FROM assets')
        ordered = sorted(expected['history'].values(), key=lambda row: (row['issuer'], row['asset_id'], row['revision']))
        for state in ordered:
            projection.persist(store, state)
        after = check(store)
        if not after['ok']:
            raise IntegrityFailure('Rebuilt projection does not match source ledger', context=after)
        audit.append(store, 'projection.rebuilt', actor, {
            'assets': len(expected['assets']),
            'history': len(expected['history']),
            'repaired_errors': before['errors'],
        })
        return {'ok': True, 'assets': len(expected['assets']), 'history': len(expected['history']), 'repaired': len(before['errors'])}


def fingerprint(store):
    with store.transaction(immediate=False):
        assets = [projection.decode_asset(row) for row in store.all('SELECT * FROM assets ORDER BY issuer,asset_id')]
        cursors = store.all('SELECT issuer,last_sequence FROM issuers ORDER BY issuer')
        return {'sha256': digest({'assets': assets, 'cursors': cursors}), 'assets': len(assets), 'issuers': len(cursors)}
