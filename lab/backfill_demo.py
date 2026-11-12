"""End-to-end backfill demonstration against the local exchange service.

The script drives the real ``registry_bridge`` CLI as subprocesses against an
in-process lab provider, so every request, bundle import and ledger projection
goes through the same code path as production use.  It prints one JSON evidence
line per step and covers:

1. a sparse gap (segments already held are matched by digest, only missing
   segments are downloaded);
2. an immutable conflict (a locally held event at the same sequence is kept,
   both fingerprints are retained as evidence and the conflicting range is not
   marked repaired);
3. view expiry (the frozen revision returns 410, the run stays unfinished and a
   new run is planned against the new view);
4. request-budget exhaustion with a restart, proving the persisted work list
   resumes and imported bundles are not applied twice.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from lab.http import serving
from lab.provider import Provider


def run_cli(db, *args):
    process = subprocess.run(
        [sys.executable, '-m', 'registry_bridge', '--db', str(db), *args],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True,
    )
    if process.returncode:
        raise RuntimeError(f'CLI failed: {process.stderr.strip()}')
    return json.loads(process.stdout)['result']


def call(db, operation, **parameters):
    return run_cli(db, 'call', operation, '--json', json.dumps(
        parameters, ensure_ascii=False))


def emit(stage, payload):
    print(json.dumps({'stage': stage, **payload}, ensure_ascii=False, sort_keys=True), flush=True)


def prepare_issuer(db):
    run_cli(db, 'init')
    call(db, 'issuer.add', issuer='north', display_name='合成北方')


def main():
    with tempfile.TemporaryDirectory(prefix='backfill-demo-') as directory:
        root = Path(directory)
        sparse_db = root / 'sparse.sqlite3'
        conflict_db = root / 'conflict.sqlite3'
        expiry_db = root / 'expiry.sqlite3'
        budget_db = root / 'budget.sqlite3'
        for db in (sparse_db, conflict_db, expiry_db, budget_db):
            prepare_issuer(db)

        with serving(Provider()) as base:
            # ------------------------------------------------ 1. sparse gaps
            call(sparse_db, 'bundle.import', value=_fetch(base, 1, 8))
            call(sparse_db, 'bundle.import', value=_fetch(base, 25, 32))
            plan = run_cli(sparse_db, 'backfill', 'plan', '--json', json.dumps(
                {'issuer': 'north', 'base_url': base, 'segment_width': 8}))
            status = run_cli(sparse_db, 'backfill', 'execute', '--json',
                             json.dumps({'run_id': plan['run_id']}))
            emit('sparse_gap', {
                'run_id': plan['run_id'], 'state': status['state'],
                'requests_used': status['requests_used'],
                'ranges': status['range_counts'], 'sequences': status['sequences'],
                'gaps': status['ledger']['gaps'],
                'blockage': status['ledger']['blockage']['kind'],
            })

        with serving(Provider()) as base:
            # ----------------------------------------- 2. immutable conflict
            call(conflict_db, 'event.receive', message=_local_conflicting_event(5))
            plan = run_cli(conflict_db, 'backfill', 'plan', '--json', json.dumps(
                {'issuer': 'north', 'base_url': base, 'segment_width': 8,
                 'run_id': 'demo-conflict'}))
            status = run_cli(conflict_db, 'backfill', 'execute', '--json',
                             json.dumps({'run_id': plan['run_id']}))
            conflicted = [r for r in status['ranges'] if r['state'] == 'conflicted']
            retained = call(conflict_db, 'event.get', event_id='north:event5')
            emit('immutable_conflict', {
                'state': status['state'], 'requests_used': status['requests_used'],
                'sequences': status['sequences'],
                'conflicted_range': {
                    'lower': conflicted[0]['lower'], 'upper': conflicted[0]['upper'],
                    'local_digest': conflicted[0]['detail']['local_digest'],
                    'remote_digest': conflicted[0]['detail']['remote_digest'],
                    'conflict_id': conflicted[0]['detail']['conflicts'][0]['conflict_id'],
                },
                'local_event_retained': {
                    'event_id': retained['event_id'], 'asset_id': retained['asset_id'],
                    'fingerprint': retained['fingerprint'],
                },
                'open_conflicts': [c['kind'] for c in call(conflict_db, 'conflict.list')],
            })

        with serving(Provider()) as base:
            # --------------------------------------------- 3. view expiry
            plan = run_cli(expiry_db, 'backfill', 'plan', '--json', json.dumps(
                {'issuer': 'north', 'base_url': base, 'segment_width': 8,
                 'run_id': 'demo-old-view'}))
            frozen_revision = plan['view']['revision']
            _expire(base)
            stale = run_cli(expiry_db, 'backfill', 'execute', '--json',
                            json.dumps({'run_id': plan['run_id']}))
            replanned = run_cli(expiry_db, 'backfill', 'plan', '--json', json.dumps(
                {'issuer': 'north', 'base_url': base, 'segment_width': 8,
                 'run_id': 'demo-new-view'}))
            finished = run_cli(expiry_db, 'backfill', 'execute', '--json',
                               json.dumps({'run_id': replanned['run_id']}))
            emit('view_expiry', {
                'frozen_revision': frozen_revision,
                'stale_run': {'state': stale['state'], 'reason': stale['pause_reason'],
                              'unfinished': stale['sequences']['unfinished']},
                'new_revision': replanned['view']['revision'],
                'new_run': {'state': finished['state'],
                            'requests_used': finished['requests_used'],
                            'sequences': finished['sequences']},
            })

        with serving(Provider()) as base:
            # ----------------------------- 4. budget exhaustion and restart
            plan = run_cli(budget_db, 'backfill', 'plan', '--json', json.dumps(
                {'issuer': 'north', 'base_url': base, 'segment_width': 8,
                 'run_id': 'demo-budget', 'request_budget': 3}))
            paused = run_cli(budget_db, 'backfill', 'execute', '--json',
                             json.dumps({'run_id': plan['run_id']}))
            # The operator extends the budget in a follow-up process while the
            # exchange keeps serving the same frozen view.
            run_cli(budget_db, 'backfill', 'extend-budget', '--json', json.dumps(
                {'run_id': 'demo-budget', 'request_budget': 100}))
            resumed = run_cli(budget_db, 'backfill', 'execute', '--json',
                              json.dumps({'run_id': 'demo-budget'}))
            runs = run_cli(budget_db, 'backfill', 'list', '--json', '{}')
            emit('budget_restart', {
                'paused': {'state': paused['state'], 'reason': paused['pause_reason'],
                           'requests_used': paused['requests_used']},
                'resumed': {'state': resumed['state'],
                            'requests_used': resumed['requests_used'],
                            'ranges': resumed['range_counts']},
                'recorded_runs': len(runs),
            })

        with serving(Provider()) as base:
            # --------------------------- final business-ledger reconciliation
            for label, db in (('sparse', sparse_db), ('conflict', conflict_db),
                              ('expiry', expiry_db), ('budget', budget_db)):
                health = run_cli(db, 'report')
                check = run_cli(db, 'check')
                runs = run_cli(db, 'backfill', 'list', '--json', '{}')
                stream = call(db, 'stream.gaps', issuer='north')
                conflicts = call(db, 'conflict.list')
                progress = call(db, 'issuer.progress', issuer='north')
                emit('ledger_reconciliation', {
                    'ledger': label, 'integrity_ok': check['ok'],
                    'healthy': health['healthy'],
                    'cursor': progress['last_sequence'],
                    'highest_received': progress['highest_received'],
                    'open_gaps': stream['gaps'],
                    'retained_conflicts': [
                        {'conflict_id': c['conflict_id'], 'kind': c['kind'],
                         'sequence': c['sequence'], 'status': c['status']}
                        for c in conflicts],
                    'runs': [(r['run_id'], r['state']) for r in runs],
                })


def _fetch(base, lower, upper):
    from lab.demo import request
    return request(base, f'/bundle?epoch=exchange-1&revision=1&lower={lower}&upper={upper}')


def _expire(base):
    from lab.demo import request
    request(base, '/control', {'expire': True})


def _local_conflicting_event(sequence):
    # Same event id/sequence as the exchange event but different immutable
    # content: the bridge must keep the local copy and record a conflict.
    return {
        'version': 1,
        'event_id': f'north:event{sequence}',
        'issuer': 'north',
        'sequence': sequence,
        'asset_id': f'local-conflict-asset-{sequence}',
        'revision': 1,
        'operation': 'register',
        'effective_at': '2026-11-01T00:00:00Z',
        'payload': {
            'title': 'Locally retained immutable registration',
            'holder': 'holder-local',
            'dataset_fingerprint': 'c' * 64,
            'scope': {
                'purposes': ['research'],
                'regions': ['CN'],
                'rights': ['hold', 'process'],
                'expires_at': '2027-11-01T00:00:00Z',
            },
            'evidence': [{'kind': 'local_agreement', 'sha256': 'd' * 64}],
        },
    }


if __name__ == '__main__':
    main()
